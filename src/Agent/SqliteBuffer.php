<?php

namespace NightOwl\Agent;

use PDO;
use RuntimeException;

final class SqliteBuffer
{
    private PDO $pdo;
    private \PDOStatement $appendStmt;
    private string $path;

    public function __construct(string $path)
    {
        $this->path = $path;
        $dir = dirname($path);
        if (! is_dir($dir) && ! mkdir($dir, 0755, true)) {
            throw new RuntimeException("Cannot create directory: {$dir}");
        }

        $this->pdo = new PDO("sqlite:{$path}");
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        $this->pdo->exec('PRAGMA busy_timeout=5000');

        // journal_mode=WAL can fail with "database is locked" when another
        // process (e.g. forked drain worker) is simultaneously opening the same
        // file. Retry with backoff since busy_timeout doesn't cover all PRAGMA
        // lock scenarios.
        for ($attempt = 0; $attempt < 10; $attempt++) {
            try {
                $this->pdo->exec('PRAGMA journal_mode=WAL');
                break;
            } catch (\PDOException $e) {
                if ($attempt === 9 || ! str_contains($e->getMessage(), 'locked')) {
                    throw $e;
                }
                usleep(100_000 * ($attempt + 1)); // 100ms, 200ms, 300ms...
            }
        }
        $this->pdo->exec('PRAGMA synchronous=NORMAL');
        $this->pdo->exec('PRAGMA cache_size=-64000');
        $this->pdo->exec('PRAGMA temp_store=MEMORY');
        $this->pdo->exec('PRAGMA mmap_size=268435456');

        $this->pdo->exec('
            CREATE TABLE IF NOT EXISTS buffer (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload TEXT NOT NULL,
                record_count INTEGER NOT NULL,
                created_at REAL NOT NULL,
                synced INTEGER NOT NULL DEFAULT 0
            )
        ');

        $this->pdo->exec('
            CREATE INDEX IF NOT EXISTS idx_buffer_synced_id ON buffer (synced, id)
        ');

        $this->appendStmt = $this->pdo->prepare(
            'INSERT INTO buffer (payload, record_count, created_at) VALUES (:payload, :count, :created_at)'
        );
    }

    /**
     * Append a pre-validated JSON payload string directly to the buffer.
     * Skips json_encode — the raw string from the wire format is stored as-is.
     * This eliminates ~30-50us of json_encode overhead per payload on the hot path.
     */
    public function appendRaw(string $json): void
    {
        $this->appendStmt->execute([
            'payload' => $json,
            'count' => 0,
            'created_at' => microtime(true),
        ]);
    }

    /**
     * Append a payload (array of records) to the buffer.
     * Used by the sync driver path where records are already decoded.
     *
     * @throws RuntimeException If the payload cannot be JSON-encoded (e.g. non-UTF-8 data).
     */
    public function append(array $records): void
    {
        $json = json_encode($records, JSON_INVALID_UTF8_SUBSTITUTE);

        if ($json === false) {
            throw new RuntimeException('Failed to JSON-encode payload: ' . json_last_error_msg());
        }

        $this->appendStmt->execute([
            'payload' => $json,
            'count' => count($records),
            'created_at' => microtime(true),
        ]);
    }

    /**
     * Fetch up to $limit unsynced rows, oldest first.
     * Safe for single-worker mode. For multi-worker, use claimBatch() instead.
     *
     * @return array<int, array{id: int, payload: string, record_count: int}>
     */
    public function fetchPending(int $limit = 1000): array
    {
        $stmt = $this->pdo->prepare(
            'SELECT id, payload, record_count FROM buffer WHERE synced = 0 ORDER BY id ASC LIMIT :limit'
        );
        $stmt->bindValue('limit', $limit, PDO::PARAM_INT);
        $stmt->execute();

        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * Atomically claim a batch of rows for a specific worker.
     *
     * Uses a two-step atomic claim: UPDATE a batch of unclaimed rows to the
     * worker's ID, then SELECT those claimed rows. This ensures no two workers
     * ever process the same row, even under high concurrency.
     *
     * Worker IDs use range 100+ (100 = worker 0, 101 = worker 1, etc.)
     * to avoid collision with 0 (pending) and 1 (synced).
     *
     * @return array<int, array{id: int, payload: string, record_count: int}>
     */
    public function claimBatch(int $workerId, int $limit = 1000): array
    {
        $claimValue = 100 + $workerId;

        // Atomic claim: mark unclaimed rows for this worker
        $this->pdo->exec(
            "UPDATE buffer SET synced = {$claimValue} WHERE id IN ("
            . "SELECT id FROM buffer WHERE synced = 0 ORDER BY id ASC LIMIT {$limit}"
            . ')'
        );

        // Fetch claimed rows
        $stmt = $this->pdo->prepare(
            'SELECT id, payload, record_count FROM buffer WHERE synced = :claim ORDER BY id ASC'
        );
        $stmt->execute(['claim' => $claimValue]);

        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * Mark the given row IDs as synced (fully drained).
     *
     * @param int[] $ids
     */
    public function markSynced(array $ids): void
    {
        if (empty($ids)) {
            return;
        }

        $placeholders = implode(',', array_fill(0, count($ids), '?'));
        $stmt = $this->pdo->prepare("UPDATE buffer SET synced = 1 WHERE id IN ({$placeholders})");
        $stmt->execute(array_values($ids));
    }

    /**
     * Release rows claimed by a worker back to pending state.
     * Called on worker crash recovery so rows aren't permanently stuck.
     */
    public function releaseClaimed(int $workerId): void
    {
        $claimValue = 100 + $workerId;
        $this->pdo->exec("UPDATE buffer SET synced = 0 WHERE synced = {$claimValue}");
    }

    /**
     * Delete synced rows older than $maxAge seconds.
     */
    public function cleanup(int $maxAge = 300): int
    {
        $cutoff = microtime(true) - $maxAge;
        $stmt = $this->pdo->prepare('DELETE FROM buffer WHERE synced = 1 AND created_at < :cutoff');
        $stmt->execute(['cutoff' => $cutoff]);

        return $stmt->rowCount();
    }

    /**
     * Count of rows not yet synced.
     */
    public function pendingCount(): int
    {
        return (int) $this->pdo->query('SELECT COUNT(*) FROM buffer WHERE synced = 0')->fetchColumn();
    }

    /**
     * Size of the WAL file in bytes. Returns 0 if the file doesn't exist.
     *
     * Clears PHP's stat cache for this file first — without this, filesize()
     * returns a stale cached value for the entire lifetime of a long-running
     * CLI process.
     */
    public function walSize(): int
    {
        $walPath = $this->path . '-wal';
        clearstatcache(true, $walPath);

        return file_exists($walPath) ? (int) filesize($walPath) : 0;
    }

    /**
     * Run a passive WAL checkpoint. Moves committed pages from the WAL back
     * to the main database without blocking concurrent writers.
     */
    public function checkpoint(): void
    {
        $this->pdo->exec('PRAGMA wal_checkpoint(PASSIVE)');
    }

    /**
     * Run a TRUNCATE checkpoint — checkpoints all frames, waits for readers
     * and writers to finish, then truncates the WAL file to zero bytes.
     *
     * This BLOCKS concurrent writers for the duration of the checkpoint.
     * The parent's busy_timeout (5s) absorbs the wait. Use only when the
     * WAL has grown large enough that PASSIVE can't keep up.
     *
     * A 200MB WAL (~50k pages) takes ~100-500ms to checkpoint.
     */
    public function checkpointTruncate(): void
    {
        $this->pdo->exec('PRAGMA wal_checkpoint(TRUNCATE)');
    }
}
