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
     * Verify the buffer file can actually be written, throwing a clear error if not.
     *
     * The buffer's whole job is to record drain progress (markSynced). If the file
     * is readable but not writable — most often because the agent is running as a
     * different user than owns agent-buffer.sqlite, or the disk/quota is full, or
     * inodes are exhausted — the drain would commit rows to Postgres and then fail
     * to mark them done, re-COPYing the same rows every loop: silent, unbounded
     * telemetry duplication. The agent calls this at startup and refuses to run when
     * it fails, turning that failure mode into an obvious error instead.
     *
     * The probe writes a real row to the on-disk table inside a rolled-back
     * transaction (a TEMP table would not do — temp_store is MEMORY here, so it
     * would never touch the file). INSERT is what surfaces a read-only file, wrong
     * owner, full disk/quota, or exhausted inodes.
     *
     * @throws RuntimeException if the buffer cannot be written.
     */
    public function assertWritable(): void
    {
        try {
            $this->pdo->beginTransaction();
            $this->pdo->exec(
                "INSERT INTO buffer (payload, record_count, created_at) VALUES ('__nightowl_write_probe__', 0, 0)"
            );
            $this->pdo->rollBack();
        } catch (\Throwable $e) {
            if ($this->pdo->inTransaction()) {
                try {
                    $this->pdo->rollBack();
                } catch (\Throwable) {
                    // The rollback can itself fail on an unwritable file; the
                    // original error below is the one that matters.
                }
            }

            throw new RuntimeException(
                "SQLite buffer at {$this->path} is not writable: ".$e->getMessage(),
                0,
                $e
            );
        }
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
            throw new RuntimeException('Failed to JSON-encode payload: '.json_last_error_msg());
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

        // Atomic claim: mark unclaimed rows for this worker.
        // Values are internally-supplied ints but we bind them anyway — matches
        // the codebase's prepared-statement pattern and defends against future
        // callers that might pass user input.
        $claimStmt = $this->pdo->prepare(
            'UPDATE buffer SET synced = :claim WHERE id IN ('
            .'SELECT id FROM buffer WHERE synced = 0 ORDER BY id ASC LIMIT :lim'
            .')'
        );
        $claimStmt->bindValue('claim', $claimValue, PDO::PARAM_INT);
        $claimStmt->bindValue('lim', $limit, PDO::PARAM_INT);
        $claimStmt->execute();

        $stmt = $this->pdo->prepare(
            'SELECT id, payload, record_count FROM buffer WHERE synced = :claim ORDER BY id ASC'
        );
        $stmt->bindValue('claim', $claimValue, PDO::PARAM_INT);
        $stmt->execute();

        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * SQLite caps host parameters per statement (SQLITE_MAX_VARIABLE_NUMBER — 999
     * on builds before 3.32). markSynced()/quarantine() can carry a whole drain
     * batch of ids (NIGHTOWL_DRAIN_BATCH_SIZE, default 5000), so their IN() lists
     * are chunked well under that cap. Without it, a large batch's UPDATE throws
     * "too many SQL variables", the mark never lands, and the committed batch
     * re-drains every loop — silent, unbounded telemetry duplication.
     */
    private const ID_IN_CHUNK = 500;

    /**
     * Mark the given row IDs as synced (fully drained).
     *
     * @param  int[]  $ids
     */
    public function markSynced(array $ids): void
    {
        if (empty($ids)) {
            return;
        }

        $this->chunkedIdUpdate('UPDATE buffer SET synced = 1 WHERE id IN (%s)', array_values($ids));
    }

    /**
     * Run an UPDATE whose `id IN (...)` list is chunked under SQLite's host-parameter
     * cap (ID_IN_CHUNK). Leading bound params (e.g. a created_at re-stamp) precede the
     * id placeholders in the template's `?`s and are re-bound per chunk. When more than
     * one chunk is needed they run in a single transaction so the update stays
     * all-or-nothing (a half-marked batch would otherwise re-drain its unmarked tail).
     *
     * @param  string  $sqlTemplate  `UPDATE ... IN (%s)` — %s becomes the id placeholders
     * @param  int[]  $ids
     * @param  array<int, scalar>  $leadingParams  bound before the id placeholders
     */
    private function chunkedIdUpdate(string $sqlTemplate, array $ids, array $leadingParams = []): void
    {
        $chunks = array_chunk($ids, self::ID_IN_CHUNK);
        $ownsTransaction = count($chunks) > 1 && ! $this->pdo->inTransaction();

        if ($ownsTransaction) {
            $this->pdo->beginTransaction();
        }

        try {
            foreach ($chunks as $chunk) {
                $placeholders = implode(',', array_fill(0, count($chunk), '?'));
                $stmt = $this->pdo->prepare(sprintf($sqlTemplate, $placeholders));
                $stmt->execute([...$leadingParams, ...$chunk]);
            }

            if ($ownsTransaction) {
                $this->pdo->commit();
            }
        } catch (\Throwable $e) {
            if ($ownsTransaction && $this->pdo->inTransaction()) {
                $this->pdo->rollBack();
            }

            throw $e;
        }
    }

    /**
     * Release rows claimed by a worker back to pending state.
     * Called on worker crash recovery so rows aren't permanently stuck.
     */
    public function releaseClaimed(int $workerId): void
    {
        $claimValue = 100 + $workerId;
        $stmt = $this->pdo->prepare('UPDATE buffer SET synced = 0 WHERE synced = :claim');
        $stmt->bindValue('claim', $claimValue, PDO::PARAM_INT);
        $stmt->execute();
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
     * Move rows to the quarantine / dead-letter state (synced = -1): a terminal
     * marker the drain never re-fetches (fetchPending/claimBatch select synced=0),
     * cleanup() never deletes (it targets synced=1), releaseClaimed() never resets
     * (it targets synced=100+workerId), and pendingCount() never counts. Used for
     * "poison" payloads a write keeps rejecting with a row-level data error, so one
     * bad row can't head-of-line block the whole drain. Bounded by pruneQuarantined().
     *
     * @param  int[]  $ids
     */
    public function quarantine(array $ids): void
    {
        if (empty($ids)) {
            return;
        }

        // Re-stamp created_at to NOW so pruneQuarantined()'s retention is measured
        // from quarantine time, not original ingest time — a long-buffered poison
        // row would otherwise be eligible for pruning immediately. (created_at is
        // only read by cleanup (synced=1) and pruneQuarantined (synced=-1), and a
        // row is only ever one of those, so re-stamping here is side-effect-free.)
        $this->chunkedIdUpdate(
            'UPDATE buffer SET synced = -1, created_at = ? WHERE id IN (%s)',
            array_values($ids),
            [microtime(true)],
        );
    }

    /**
     * Count of quarantined (dead-lettered) rows currently in the buffer.
     */
    public function quarantinedCount(): int
    {
        return (int) $this->pdo->query('SELECT COUNT(*) FROM buffer WHERE synced = -1')->fetchColumn();
    }

    /**
     * Delete quarantined rows older than $maxAge seconds, bounding the dead-letter's
     * disk footprint under a systemic poison (e.g. wrong DB / schema drift). The loss
     * is not silent — the drain surfaces a DRAIN_QUARANTINE health diagnosis with the
     * count + SQLSTATE before rows age out.
     */
    public function pruneQuarantined(int $maxAge): int
    {
        $cutoff = microtime(true) - $maxAge;
        $stmt = $this->pdo->prepare('DELETE FROM buffer WHERE synced = -1 AND created_at < :cutoff');
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
        $walPath = $this->path.'-wal';
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
