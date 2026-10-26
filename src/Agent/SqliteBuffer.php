<?php

namespace NightOwl\Agent;

use PDO;
use RuntimeException;

final class SqliteBuffer
{
    private PDO $pdo;

    private \PDOStatement $appendStmt;

    private string $path;

    /** @var resource|null Handle on the checkpoint lock file, see checkpointLockPath(). */
    private $checkpointLock = null;

    /** @var resource|null Handle on the gate in front of it, see checkpointGatePath(). */
    private $checkpointGate = null;

    // Nesting depth of shared holds, so a write inside another write keeps the hold.
    private int $sharedDepth = 0;

    // What changedSinceCheckpoint() compares: this connection's writes since its
    // last checkpoint, and PRAGMA data_version (other connections' commits) then.
    private int $writesSinceCheckpoint = 0;

    private ?int $dataVersionAtCheckpoint = null;

    // When this connection last checkpointed (microtime), 0.0 if never.
    private float $lastCheckpointAt = 0.0;

    // Bytes per WAL frame (page size plus the 24-byte frame header), for
    // uncheckpointedWalBytes().
    private ?int $walFrameBytes = null;

    // A lock file could not be opened: writes and checkpoints run unguarded.
    private bool $checkpointLockUnavailable = false;

    private float $lastUnguardedWriteLogAt = 0.0;

    /**
     * How long a write waits for a checkpoint to finish before going ahead
     * without the lock. Same budget as busy_timeout, which a TRUNCATE
     * checkpoint already made writers wait on.
     */
    private const WRITE_LOCK_WAIT_MS = 5000;

    /**
     * How long a checkpoint waits for its turn before it is skipped until the
     * next pass. It holds the gate while it waits (checkpointGatePath()), so
     * this only has to outlast the writes already running.
     */
    private const CHECKPOINT_LOCK_WAIT_MS = 250;

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

        // Without this the buffer file is high-water-mark sized: drained rows are
        // DELETEd but their pages stay on the freelist forever, so one traffic
        // spike leaves a permanently larger file on the customer's disk. (Measured:
        // 3.8GB of file holding 126k pending rows.) INCREMENTAL lets the drain
        // worker hand pages back a bounded chunk at a time via reclaim().
        //
        // This MUST run before journal_mode=WAL. auto_vacuum lives in the database
        // header, and it can only be changed while that header does not exist yet
        // — setting journal_mode writes the file, after which the pragma is
        // silently a NO-OP (it does not error, and `PRAGMA auto_vacuum` keeps
        // answering 0). Ordering it after WAL is what made the first version of
        // this fix do nothing at all. busy_timeout still has to come first; it is
        // the only one of the three that must precede WAL for locking reasons.
        //
        // On a buffer that already has tables the pragma is a no-op by design.
        // Those are the already-deployed files: needsCompaction() detects them and
        // compact()'s full VACUUM is what converts them over.
        $this->pdo->exec('PRAGMA auto_vacuum=INCREMENTAL');

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

        // No checkpoint SQLite runs by itself. It would run one on whichever
        // connection's commit pushes the WAL past 1000 pages, without the
        // checkpoint lock, and that is half of the WAL-reset bug (sqlite.org/
        // wal.html section 11, every version 3.7.0 to 3.51.2): a checkpoint
        // starting while another process's commit resets the WAL can mark that
        // commit's frames as already copied, and the next checkpoint skips them.
        // The database file loses part of a committed transaction. The agent's
        // design has two processes writing one WAL, and Yomoney's buffer went
        // corrupt twice in a week on Ubuntu 24.04's 3.45.1 (2026-10-15 and
        // 2026-10-21; the second time a buffer index had the wrong number of
        // entries). Every checkpoint now goes through checkpoint(), which holds
        // the lock exclusively, and the agent's parent process keeps the WAL
        // small instead (AsyncServer::checkpointIfWalLarge()).
        $this->pdo->exec('PRAGMA wal_autocheckpoint=0');

        $this->holdShared();
        try {
            $this->createSchema();
        } finally {
            $this->releaseShared();
        }

        $this->appendStmt = $this->pdo->prepare(
            'INSERT INTO buffer (payload, record_count, created_at) VALUES (:payload, :count, :created_at)'
        );
    }

    private function createSchema(): void
    {
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
        $this->holdShared();
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
        } finally {
            $this->releaseShared();
        }
    }

    /**
     * Append a pre-validated JSON payload string directly to the buffer.
     * Skips json_encode — the raw string from the wire format is stored as-is.
     * This eliminates ~30-50us of json_encode overhead per payload on the hot path.
     */
    public function appendRaw(string $json): void
    {
        $this->writing(fn () => $this->appendStmt->execute([
            'payload' => $json,
            'count' => 0,
            'created_at' => microtime(true),
        ]));
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

        $this->writing(fn () => $this->appendStmt->execute([
            'payload' => $json,
            'count' => count($records),
            'created_at' => microtime(true),
        ]));
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
     * Rows the worker ALREADY holds are returned first, and nothing new is
     * claimed while it holds any. A batch whose write failed stays claimed, and
     * this used to claim a fresh batch on top of it every retry: through a
     * Postgres outage one worker's claim grew by a batch per attempt until it
     * held the whole buffer, back-pressure (which counted only unclaimed rows)
     * never engaged, and the first write after recovery was the entire backlog
     * in one transaction.
     *
     * @return array<int, array{id: int, payload: string, record_count: int}>
     */
    public function claimBatch(int $workerId, int $limit = 1000): array
    {
        $claimValue = 100 + $workerId;

        $held = $this->claimedBy($claimValue, $limit);
        if ($held !== []) {
            return $held;
        }

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
        $this->writing(fn () => $claimStmt->execute());

        return $this->claimedBy($claimValue, $limit);
    }

    /**
     * @return array<int, array{id: int, payload: string, record_count: int}>
     */
    private function claimedBy(int $claimValue, int $limit): array
    {
        $stmt = $this->pdo->prepare(
            'SELECT id, payload, record_count FROM buffer WHERE synced = :claim ORDER BY id ASC LIMIT :lim'
        );
        $stmt->bindValue('claim', $claimValue, PDO::PARAM_INT);
        $stmt->bindValue('lim', $limit, PDO::PARAM_INT);
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
        $this->writing(fn () => $this->chunkedIdUpdateLocked($sqlTemplate, $ids, $leadingParams));
    }

    /**
     * @param  int[]  $ids
     * @param  array<int, scalar>  $leadingParams
     */
    private function chunkedIdUpdateLocked(string $sqlTemplate, array $ids, array $leadingParams): void
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
        $this->writing(fn () => $stmt->execute());
    }

    /**
     * Release every worker's claims back to pending. Returns the rows released.
     *
     * For agent startup only, before any drain worker exists. A claim outlives
     * the agent that made it when the agent dies without reaping its workers,
     * and nothing else would ever free it: releaseClaimed() runs only for a
     * worker the running agent saw die, a claim value above the new worker
     * count is never claimed again, and a single worker reads with
     * fetchPending(), which skips claimed rows. Those rows were never counted,
     * drained or cleaned up. Safe only because the agent holds the buffer's
     * owner lock (acquireOwnerLock()), so no other agent's workers are live on
     * this file.
     */
    public function releaseAllClaimed(): int
    {
        return $this->writing(fn () => $this->pdo->exec('UPDATE buffer SET synced = 0 WHERE synced >= 100'));
    }

    /**
     * Take the exclusive lock that makes one agent the owner of the buffer at
     * $path: an flock on a sidecar file next to it.
     *
     * Two agents on one buffer both drain it, and a single drain worker reads
     * without claiming (fetchPending()), so the same rows can reach Postgres
     * twice. The lock turns that into a refused start.
     *
     * Returns the handle, which the caller must keep open for the life of the
     * process. The kernel drops the lock when the last descriptor on it closes,
     * so a crashed agent never leaves it stale. Forked drain workers share the
     * parent's descriptor and therefore its lock, and a worker exiting does not
     * release it. The handle is close-on-exec, so an exec'd drain worker and the
     * commands the agent spawns (boot migrate, the detached rollup backfill)
     * cannot keep the lock held after the agent is gone.
     *
     * Returns null when another process holds the lock, with that process's pid
     * in $holderPid (0 when the lock file does not say). Throws when the lock
     * could not be taken for any other reason, such as a filesystem without
     * flock support.
     *
     * @param-out int|null $holderPid
     *
     * @return resource|null
     *
     * @throws RuntimeException
     */
    public static function acquireOwnerLock(string $path, ?int &$holderPid = null)
    {
        $holderPid = null;
        $lockPath = self::lockPath($path);

        $dir = dirname($lockPath);
        if (! is_dir($dir) && ! @mkdir($dir, 0755, true) && ! is_dir($dir)) {
            throw new RuntimeException("Cannot create directory: {$dir}");
        }

        // 'c' opens for read/write without truncating (the holder's pid stays
        // readable), 'e' sets close-on-exec.
        $handle = @fopen($lockPath, 'c+e');
        if ($handle === false) {
            throw new RuntimeException("Cannot open {$lockPath}: ".(error_get_last()['message'] ?? 'unknown error'));
        }

        $wouldBlock = 0;
        if (! flock($handle, LOCK_EX | LOCK_NB, $wouldBlock)) {
            if ($wouldBlock) {
                $holderPid = (int) trim((string) stream_get_contents($handle, 32, 0));
                fclose($handle);

                return null;
            }

            fclose($handle);

            throw new RuntimeException("Cannot lock {$lockPath}");
        }

        // The pid is only for the refusal message a second agent prints, so a
        // failed write (full disk) is not a reason to give the lock back.
        @ftruncate($handle, 0);
        @fwrite($handle, (string) getmypid());
        @fflush($handle);

        return $handle;
    }

    public static function lockPath(string $path): string
    {
        return $path.'.lock';
    }

    /**
     * Delete synced rows older than $maxAge seconds.
     */
    public function cleanup(int $maxAge = 300): int
    {
        $cutoff = microtime(true) - $maxAge;
        $stmt = $this->pdo->prepare('DELETE FROM buffer WHERE synced = 1 AND created_at < :cutoff');
        $this->writing(fn () => $stmt->execute(['cutoff' => $cutoff]));

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
        $this->writing(fn () => $stmt->execute(['cutoff' => $cutoff]));

        return $stmt->rowCount();
    }

    /**
     * Count of rows waiting to be drained and not claimed by any worker.
     */
    public function pendingCount(): int
    {
        return (int) $this->pdo->query('SELECT COUNT(*) FROM buffer WHERE synced = 0')->fetchColumn();
    }

    /**
     * Count of rows not yet in Postgres: pending plus claimed by a drain worker.
     *
     * The agent's back-pressure and health report read this rather than
     * pendingCount(). With several drain workers, every in-flight batch is
     * claimed, and leaving those out would hide up to one batch per worker
     * from both. The two subqueries each stay on the (synced, id) index.
     */
    public function backlogCount(): int
    {
        return (int) $this->pdo->query(
            'SELECT (SELECT COUNT(*) FROM buffer WHERE synced = 0) + (SELECT COUNT(*) FROM buffer WHERE synced >= 100)'
        )->fetchColumn();
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
     *
     * Holds the checkpoint lock exclusively, so no write by any process runs
     * while it does (see the wal_autocheckpoint note in the constructor).
     * Returns false when no gap between writes came within $lockWaitMs
     * (CHECKPOINT_LOCK_WAIT_MS by default; 0 tries once): the checkpoint is
     * skipped, and the next pass catches up.
     */
    public function checkpoint(int $lockWaitMs = self::CHECKPOINT_LOCK_WAIT_MS): bool
    {
        return $this->checkpointing('PRAGMA wal_checkpoint(PASSIVE)', $lockWaitMs);
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
    public function checkpointTruncate(): bool
    {
        return $this->checkpointing('PRAGMA wal_checkpoint(TRUNCATE)', self::CHECKPOINT_LOCK_WAIT_MS);
    }

    /**
     * Size of the main database file (excludes the -wal, see walSize()).
     */
    public function fileSize(): int
    {
        clearstatcache(true, $this->path);

        return file_exists($this->path) ? (int) filesize($this->path) : 0;
    }

    /**
     * Bytes currently sitting on the freelist — space the file occupies on disk
     * but no longer uses. This is what reclaim() hands back to the filesystem.
     */
    public function freeBytes(): int
    {
        $pages = (int) $this->pdo->query('PRAGMA freelist_count')->fetchColumn();

        return $pages * $this->pageSize();
    }

    public function pageSize(): int
    {
        return (int) $this->pdo->query('PRAGMA page_size')->fetchColumn();
    }

    /**
     * True when this buffer predates auto_vacuum=INCREMENTAL and therefore can
     * never shrink on its own — only a full compact() converts it.
     */
    public function needsCompaction(): bool
    {
        return (int) $this->pdo->query('PRAGMA auto_vacuum')->fetchColumn() === 0;
    }

    /**
     * Return up to $maxPages freed pages to the filesystem. Bounded on purpose:
     * incremental_vacuum holds the write lock while it runs, so the drain worker
     * trims a slice per cycle rather than stalling ingest with one long pass.
     *
     * Returns the number of bytes actually released.
     */
    public function reclaim(int $maxPages = 2048): int
    {
        if ($maxPages < 1) {
            return 0;
        }

        $before = $this->fileSize();
        $this->writing(fn () => $this->pdo->exec('PRAGMA incremental_vacuum('.$maxPages.')'));
        // The file only shrinks once the vacuum's pages are checkpointed. Back to
        // back checkpoints are safe here: each one holds the lock exclusively.
        $this->checkpoint();

        return max(0, $before - $this->fileSize());
    }

    /**
     * Full VACUUM — rewrites the file, releasing every free page and applying the
     * auto_vacuum mode set in the constructor. BLOCKS for the duration and needs
     * scratch space, so the drain worker only calls this on an idle, legacy buffer.
     *
     * Returns the number of bytes released.
     */
    public function compact(): int
    {
        $before = $this->fileSize();
        $this->writing(fn () => $this->pdo->exec('VACUUM'));
        $this->checkpointTruncate();

        return max(0, $before - $this->fileSize());
    }

    /**
     * The sidecar file whose flock keeps checkpoints and writes apart: writes
     * hold it shared (they still run side by side, SQLite orders them), a
     * checkpoint holds it exclusively. Not the owner lock (lockPath()), which
     * one agent holds exclusively for its whole life.
     */
    public static function checkpointLockPath(string $path): string
    {
        return $path.'.ckpt';
    }

    /**
     * The gate in front of the checkpoint lock. A write passes through it
     * (shared, and let go as soon as the write has the lock); a checkpoint
     * waiting for its turn holds it exclusively, so new writes queue behind
     * the checkpoint. Without it a checkpoint only ever got in on a gap with
     * no write running, and two processes writing flat out leave none: 0 of 10
     * checkpoints got the lock in a test, and a benchmark at ~9k payloads/s
     * took the WAL to 708 MB (2026-10-22). No deadlock: a write holding the
     * lock never waits on the gate.
     */
    public static function checkpointGatePath(string $path): string
    {
        return $path.'.ckpt-gate';
    }

    /**
     * Run a write with the checkpoint lock held shared.
     *
     * @template T
     *
     * @param  \Closure(): T  $write
     * @return T
     */
    private function writing(\Closure $write): mixed
    {
        $this->holdShared();
        try {
            return $write();
        } finally {
            $this->writesSinceCheckpoint++;
            $this->releaseShared();
        }
    }

    /**
     * Whether anything was written to the buffer, by this connection or any
     * other, since this connection's last checkpoint. The -wal file keeps its
     * size after a checkpoint, so its size alone cannot say.
     */
    public function changedSinceCheckpoint(): bool
    {
        return $this->writesSinceCheckpoint > 0
            || $this->dataVersionAtCheckpoint === null
            || $this->dataVersion() !== $this->dataVersionAtCheckpoint;
    }

    /**
     * Bytes of WAL that no checkpoint has copied back yet, by every connection
     * on the file, or null when that cannot be read.
     *
     * Read from the wal-index header in the -shm file (sqlite.org/walformat.html):
     * mxFrame, the last valid frame, at offset 16, and nBackfill, the frames
     * already checkpointed, at offset 96, both native-endian u32, in a header
     * whose iVersion at offset 0 is 3007000. The -wal file's size cannot say
     * this: it keeps its high-water size after a checkpoint. Read without
     * SQLite's locks, so a value can be one commit stale; good enough to decide
     * when to checkpoint, and never used for anything else.
     */
    public function uncheckpointedWalBytes(): ?int
    {
        $header = @file_get_contents($this->path.'-shm', false, null, 0, 100);
        if ($header === false || strlen($header) < 100 || unpack('L', $header, 0)[1] !== 3007000) {
            return null;
        }

        $frames = max(0, unpack('L', $header, 16)[1] - unpack('L', $header, 96)[1]);

        return $frames * ($this->walFrameBytes ??= $this->pageSize() + 24);
    }

    /** Writes this connection made since its last checkpoint. */
    public function writesSinceCheckpoint(): int
    {
        return $this->writesSinceCheckpoint;
    }

    /** When this connection last checkpointed (microtime), 0.0 if it never has. */
    public function lastCheckpointAt(): float
    {
        return $this->lastCheckpointAt;
    }

    private function dataVersion(): int
    {
        return (int) $this->pdo->query('PRAGMA data_version')->fetchColumn();
    }

    /** Run a checkpoint statement and note where the buffer stood, for changedSinceCheckpoint(). */
    private function runCheckpoint(string $statement): void
    {
        $this->pdo->exec($statement);
        $this->writesSinceCheckpoint = 0;
        $this->dataVersionAtCheckpoint = $this->dataVersion();
        $this->lastCheckpointAt = microtime(true);
    }

    private function holdShared(): void
    {
        if ($this->sharedDepth++ > 0) {
            return;
        }

        $lock = $this->checkpointLock();
        if ($lock === null) {
            return;
        }

        // Wait out a running or waiting checkpoint, but never longer than
        // busy_timeout: a checkpoint that hangs must not stop ingest. Past that
        // the write goes ahead unguarded, as every write did before this lock
        // existed.
        $deadline = microtime(true) + self::WRITE_LOCK_WAIT_MS / 1000;
        $held = self::lockBy($this->checkpointGate, LOCK_SH, $deadline, 500);
        if ($held) {
            $held = self::lockBy($lock, LOCK_SH, $deadline, 500);
            flock($this->checkpointGate, LOCK_UN);
        }

        if (! $held && microtime(true) - $this->lastUnguardedWriteLogAt >= 60) {
            $this->lastUnguardedWriteLogAt = microtime(true);
            error_log('[NightOwl Agent] A buffer checkpoint held its lock for over 5s; writes are going ahead without it.');
        }
    }

    private function releaseShared(): void
    {
        if (--$this->sharedDepth > 0) {
            return;
        }

        $this->sharedDepth = 0;
        if ($this->checkpointLock !== null) {
            flock($this->checkpointLock, LOCK_UN);
        }
    }

    /** Run one checkpoint statement holding the lock exclusively. */
    private function checkpointing(string $statement, int $lockWaitMs): bool
    {
        $lock = $this->checkpointLock();
        if ($lock === null) {
            $this->runCheckpoint($statement);

            return true;
        }

        // The gate first, so no new write starts; then the lock, once the
        // writes already running are done.
        $deadline = microtime(true) + $lockWaitMs / 1000;
        if (! self::lockBy($this->checkpointGate, LOCK_EX, $deadline, 1000)) {
            return false;
        }

        try {
            if (! self::lockBy($lock, LOCK_EX, $deadline, 1000)) {
                return false;
            }

            try {
                // data_version is read before the lock goes: no write can land
                // between the checkpoint and it.
                $this->runCheckpoint($statement);

                return true;
            } finally {
                flock($lock, LOCK_UN);
            }
        } finally {
            flock($this->checkpointGate, LOCK_UN);
        }
    }

    /**
     * Take an flock, trying again every $pollMicros until $deadline. Always
     * tries once, even with the deadline already past.
     *
     * @param  resource  $handle
     */
    private static function lockBy($handle, int $operation, float $deadline, int $pollMicros): bool
    {
        while (! flock($handle, $operation | LOCK_NB)) {
            if (microtime(true) >= $deadline) {
                return false;
            }
            usleep($pollMicros);
        }

        return true;
    }

    /**
     * The lock handle, opened on first use with the gate's. Every SqliteBuffer
     * opens its own, and the agent only builds one after a fork, so no two
     * processes share an open file description (an flock belongs to that, not
     * to the process). A file that cannot be opened (a directory owned by
     * someone else) leaves writes and checkpoints running unguarded rather
     * than failing them.
     *
     * @return resource|null
     */
    private function checkpointLock()
    {
        if ($this->checkpointLock !== null || $this->checkpointLockUnavailable) {
            return $this->checkpointLock;
        }

        $handles = [];
        foreach ([self::checkpointLockPath($this->path), self::checkpointGatePath($this->path)] as $lockPath) {
            $handle = @fopen($lockPath, 'c+e');
            if ($handle === false) {
                $this->checkpointLockUnavailable = true;
                error_log("[NightOwl Agent] Cannot open {$lockPath}; buffer checkpoints run without their lock: ".(error_get_last()['message'] ?? 'unknown error'));
                foreach ($handles as $opened) {
                    fclose($opened);
                }

                return null;
            }
            $handles[] = $handle;
        }

        [$this->checkpointLock, $this->checkpointGate] = $handles;

        return $this->checkpointLock;
    }
}
