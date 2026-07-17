<?php

namespace NightOwl\Agent;

/**
 * Drain worker that runs in a forked child process.
 *
 * Reads buffered payloads from SQLite and writes them to Postgres in batches.
 * Opens its own database connections — MUST NOT reuse connections from the parent.
 */
final class DrainWorker
{
    private const METRICS_WRITE_INTERVAL = 5; // seconds

    private bool $running = true;

    /**
     * Liveness channel to the parent, stamped at step boundaries inside a batch.
     * Null until run() builds it, and outside run() (the sync fallback path).
     */
    private ?DrainHeartbeat $heartbeat = null;

    // Drain metrics for IPC with parent process
    private int $batchesDrained = 0;

    private int $batchesFailed = 0;

    private int $rowsDrained = 0;

    // App-vitals: cumulative per-app request/5xx/exception counts since worker
    // start, shipped to the platform for the fleet overview. Each drain worker
    // tracks its own cumulative; the parent sums across workers. See impl plan §4.2.
    private int $cumRequests = 0;

    private int $cum5xx = 0;

    private int $cumExceptions = 0;

    // Open-issues gauge (current, not cumulative) for the fleet overview's
    // per-app "issues" count. Refreshed at most once per minute — see run().
    private int $openIssues = 0;

    private const ISSUE_COUNT_INTERVAL = 60; // seconds

    private float $pgLatencyEwma = 0.0; // EWMA in ms

    // Checkpoint observability — answers "is TRUNCATE actually firing under load?"
    private int $truncateAttempts = 0;

    private int $truncateSuccesses = 0;

    private int $truncateFailures = 0;

    private int $walSizeBytes = 0;

    // Last NON-connection write failure (poison row / missing schema / no
    // privilege / wrong db). Only SQLSTATE + table — never the raw libpq message,
    // which can echo customer row values. Surfaced as the DRAIN_WRITE_FAILING
    // diagnosis so a stuck drain shows the real cause instead of the misleading
    // "Postgres may be unreachable". Connection failures are tracked separately in
    // lastConnFailAt (DRAIN_UNREACHABLE), not folded in here.
    private ?string $lastWriteSqlstate = null;

    private ?string $lastWriteTable = null;

    private float $lastWriteAt = 0.0;

    private float $lastWriteOkAt = 0.0;

    // Most recent PG CONNECTION failure (unreachable / refused / connect-time throw),
    // as opposed to a write rejection (lastWriteAt) or a local buffer error. Drives the
    // DRAIN_UNREACHABLE diagnosis and clears the write-rejection latch when newer.
    private float $lastConnFailAt = 0.0;

    // Poison-payload quarantine (Phase 2). Cumulative since worker start — a
    // dropped telemetry row is data loss, so the DRAIN_QUARANTINE diagnosis stays
    // visible. SQLSTATE + table only, never raw row values.
    private int $quarantinedTotal = 0;

    private ?string $lastQuarantineSqlstate = null;

    private ?string $lastQuarantineTable = null;

    private float $lastQuarantineAt = 0.0;

    // Per-table consecutive-quarantine counters for the systematic-poison circuit
    // breaker: physical table name => quarantines of THAT table with no intervening
    // successful write of it. Keyed per-table so a real per-table schema mismatch
    // (e.g. an unmigrated NOT-NULL column on nightowl_requests) trips the breaker
    // even while other tables keep draining — and so sparse poison on a table that
    // otherwise drains fine never trips it. See isolateSinglePayload / onDrainSuccess.
    /** @var array<string, int> */
    private array $consecutiveQuarantines = [];

    // Live count of dead-lettered rows in the buffer (durable across restart),
    // shipped as quarantined_live for observability.
    private int $quarantinedLive = 0;

    // Rows whose write() COMMITTED to Postgres but whose markSynced() then failed
    // (the local SQLite buffer went unwritable — a full disk is the common cause,
    // and the same condition that made the buffer back up in the first place).
    // They are still synced=0, so re-fetching them (fetchPending selects synced=0)
    // and re-writing would re-COPY data already durably in Postgres — an unbounded
    // duplication storm for as long as the buffer stays unwritable. Instead we hold
    // the ids and retry ONLY the mark (flushed at the top of drainBatch) — never
    // the write — until the buffer accepts it again. Held in memory: a hard kill
    // re-drains at most this one batch on restart (the documented
    // at-most-one-batch-duplicate crash-safety tradeoff, unchanged).
    /** @var int[] */
    private array $committedUnmarkedIds = [];

    // Observability for the buffer-unwritable stall: how many post-commit marks
    // have failed, and when the current stall began (0.0 = not stalled). Shipped in
    // the drain metrics so a stuck-but-not-duplicating drain is visible rather than
    // silent. Distinct from lastWriteAt, which is a Postgres write rejection.
    private int $bufferMarkStalls = 0;

    private float $bufferMarkStalledSince = 0.0;

    private const EWMA_ALPHA = 0.3;

    public function __construct(
        private string $sqlitePath,
        private string $pgHost,
        private int $pgPort,
        private string $pgDatabase,
        private string $pgUsername,
        private string $pgPassword,
        private int $batchSize = 1000,
        private int $intervalMs = 100,
        private int $maxWaitMs = 5000,
        private int $workerId = 0,
        private int $totalWorkers = 1,
        private int $thresholdCacheTtl = 86400,
        private string $appName = 'NightOwl',
        private string $environment = 'production',
        private string $sslmode = 'prefer',
        // Knobs the chaos test tunes; defaults match prior hardcoded behavior.
        private int $checkpointIntervalSeconds = 60,
        private int $checkpointTruncateBytes = 100 * 1024 * 1024,
        // Phase 2: when enabled, a batch that fails with a row-level data error is
        // bisected to isolate and quarantine the poison payload so the rest drain,
        // instead of head-of-line-blocking. Off by default — opt-in via
        // NIGHTOWL_DRAIN_QUARANTINE. Quarantined rows are dead-lettered for
        // $quarantineRetentionSeconds, then pruned (the loss is surfaced as the
        // DRAIN_QUARANTINE health diagnosis with count + SQLSTATE).
        private bool $quarantineEnabled = false,
        private int $quarantineRetentionSeconds = 86400,
        // Circuit breaker: if this many payloads quarantine in a row WITHOUT an
        // intervening successful drain, treat it as a systematic schema mismatch
        // (e.g. an unmigrated NOT-NULL column) rather than per-row poison — stop
        // dropping the stream and head-of-line block + surface DRAIN_WRITE_FAILING.
        private int $quarantineBreakerThreshold = 50,
    ) {}

    /**
     * Set worker identity for multi-worker mode.
     * Called after cloning the prototype before run().
     */
    public function setWorkerConfig(int $workerId, int $totalWorkers): void
    {
        $this->workerId = $workerId;
        $this->totalWorkers = $totalWorkers;
    }

    /**
     * Run the drain loop. Called in the forked child process.
     * This method never returns — it exits the process.
     */
    public function run(): never
    {
        // If the host app runs Octane/Swoole, Swoole's coroutine runtime hooks are
        // enabled process-wide and inherited here. The PDO-pgsql hook reimplements
        // pgsqlCopyFromArray() — the COPY protocol this worker uses for 10 tables —
        // and busy-loops (100% CPU, zero rows drained) instead of completing the
        // COPY. The drain worker is plain synchronous code with no use for coroutine
        // hooks, so turn them off here; PDO then uses the native COPY implementation.
        $this->disableSwooleRuntimeHooks();

        // Own signal handlers (child process, independent of parent's ReactPHP loop)
        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, fn () => $this->running = false);
        pcntl_signal(SIGINT, fn () => $this->running = false);

        // Liveness channel to the parent. Stamped at every step boundary inside a
        // batch, unlike the drain metrics file, which only advances BETWEEN batches
        // and so freezes for the whole of any batch — slow or wedged alike.
        $heartbeat = new DrainHeartbeat($this->sqlitePath, $this->workerId, $this->totalWorkers);
        $heartbeat->enter('starting');
        $this->heartbeat = $heartbeat;

        // Create own connections — NOT inherited from parent
        $buffer = new SqliteBuffer($this->sqlitePath);
        $writer = new RecordWriter(
            host: $this->pgHost,
            port: $this->pgPort,
            database: $this->pgDatabase,
            username: $this->pgUsername,
            password: $this->pgPassword,
            thresholdCacheTtl: $this->thresholdCacheTtl,
            notifier: AlertNotifier::fromConfig(),
            appName: $this->appName,
            environment: $this->environment,
            sslmode: $this->sslmode,
            // Top-level 'drain_connection' — a published config's 'database' array
            // wholly replaces the package's under mergeConfigFrom's shallow merge, so
            // a nested knob (and its env var) would be invisible to most installs.
            timeoutsEnabled: (bool) config('nightowl.drain_connection.timeouts_enabled', true),
            connectTimeout: (int) config('nightowl.drain_connection.connect_timeout', 10),
            tcpUserTimeoutMs: (int) config('nightowl.drain_connection.tcp_user_timeout_ms', 20000),
            keepalivesIdle: (int) config('nightowl.drain_connection.keepalives_idle', 10),
            keepalivesInterval: (int) config('nightowl.drain_connection.keepalives_interval', 5),
            keepalivesCount: (int) config('nightowl.drain_connection.keepalives_count', 3),
            lockTimeoutMs: (int) config('nightowl.drain_connection.lock_timeout_ms', 10000),
            heartbeat: $heartbeat,
            idleTxnTimeoutMs: (int) config('nightowl.drain_connection.idle_txn_timeout_ms', 30000),
        );

        $workerLabel = $this->totalWorkers > 1
            ? "Worker #{$this->workerId}"
            : 'Worker';
        error_log("[NightOwl Drain] {$workerLabel} started (pid: ".getmypid().')');

        $lastCleanup = time();
        $lastFlushTime = microtime(true);
        $lastMetricsWrite = 0.0;
        $lastIssueCount = 0.0;
        // 0 = run partition maintenance on the FIRST cleanup tick after start,
        // so a freshly-partitioned tenant doesn't wait an hour for children.
        $lastPartitionCheck = 0;

        while ($this->running) {
            $drained = $this->drainBatch($buffer, $writer);

            if ($drained) {
                $lastFlushTime = microtime(true);
            }

            // Cleanup + WAL checkpoint on the configured cadence (default 60s)
            if (time() - $lastCleanup >= $this->checkpointIntervalSeconds) {
                try {
                    $buffer->cleanup(300);
                    // Prune dead-letter rows regardless of whether quarantine is
                    // currently enabled. Rows dead-lettered while it was on must still
                    // be reclaimed after the retention window if the flag is later
                    // turned off — otherwise they are orphaned forever (never
                    // re-drained: fetchPending selects synced=0; never cleaned:
                    // cleanup targets synced=1). A no-op when no synced=-1 rows exist.
                    $buffer->pruneQuarantined($this->quarantineRetentionSeconds);

                    // Hourly: keep future daily partitions pre-created so a
                    // day rollover can never route rows to the DEFAULT child.
                    // (No-op on unpartitioned tenants; advisory-locked.)
                    if (time() - $lastPartitionCheck >= 3600) {
                        $writer->maintainRawPartitions();
                        $lastPartitionCheck = time();
                    }
                    $this->checkpointWithEscalation($buffer);
                } catch (\Throwable $e) {
                    error_log("[NightOwl Drain] Cleanup error: {$e->getMessage()}");
                }
                $lastCleanup = time();
            }

            // Refresh the open-issues gauge for the fleet overview at most once
            // a minute — a cheap indexed COUNT on the tenant DB, off the ingest
            // path. Keep the last good value if the query fails (null).
            $now = microtime(true);
            if ($now - $lastIssueCount >= self::ISSUE_COUNT_INTERVAL) {
                // An unguarded Postgres round trip in the run loop — labelled so a
                // stall here is not misreported as a wedge in the previous phase.
                $this->heartbeat?->enter('pg:count-open-issues');
                $issues = $writer->countOpenIssues();
                if ($issues !== null) {
                    $this->openIssues = $issues;
                }
                $lastIssueCount = $now;
            }

            // Write drain metrics for parent process every 5 seconds
            if ($now - $lastMetricsWrite >= self::METRICS_WRITE_INTERVAL) {
                // Refresh the live dead-letter count off the buffer (durable across
                // restart) so the DRAIN_QUARANTINE diagnosis survives a worker respawn.
                if ($this->quarantineEnabled) {
                    $this->quarantinedLive = $buffer->quarantinedCount();
                }
                $this->writeDrainMetrics();
                $lastMetricsWrite = $now;
            }

            // Only sleep when idle — under load, drain as fast as possible.
            // When approaching the max wait deadline, reduce sleep to ensure
            // data doesn't sit in SQLite longer than drain_max_wait_ms.
            if (! $drained) {
                $sinceLastFlush = (microtime(true) - $lastFlushTime) * 1000;
                $remaining = $this->maxWaitMs - $sinceLastFlush;
                $sleepMs = ($remaining > 0 && $remaining < $this->intervalMs)
                    ? max(10, (int) $remaining)
                    : $this->intervalMs;
                usleep($sleepMs * 1000);
            }
        }

        // Drain remaining before exit (5s deadline)
        $pending = $buffer->pendingCount();
        if ($pending > 0) {
            error_log("[NightOwl Drain] Shutting down, draining {$pending} remaining rows...");
            $deadline = microtime(true) + 5.0;
            while (microtime(true) < $deadline) {
                if (! $this->drainBatch($buffer, $writer)) {
                    break;
                }
            }

            $remaining = $buffer->pendingCount();
            if ($remaining > 0) {
                error_log("[NightOwl Drain] Exiting with {$remaining} rows still pending (safe in SQLite)");
            } else {
                error_log('[NightOwl Drain] All rows drained successfully');
            }
        }

        exit(0);
    }

    /**
     * Disable Swoole's coroutine runtime hooks for this process.
     *
     * Swoole's PDO-pgsql hook busy-loops inside pgsqlCopyFromArray() when driven
     * by the drain worker, pegging a core at 100% with no progress. Clearing the
     * hook flags restores PHP's native PDO COPY implementation. No-op when Swoole
     * isn't loaded (the common case).
     */
    private function disableSwooleRuntimeHooks(): void
    {
        if (class_exists(\Swoole\Runtime::class) && method_exists(\Swoole\Runtime::class, 'setHookFlags')) {
            \Swoole\Runtime::setHookFlags(0);
        }
    }

    /**
     * Checkpoint escalation: PASSIVE by default, TRUNCATE when the WAL is large.
     *
     * At high write throughput (10k+ writes/s), PASSIVE checkpoints can't fully
     * complete because the parent holds the write lock most of the time. The WAL
     * grows without bound. TRUNCATE blocks writers briefly but resets the WAL to
     * zero bytes, preventing disk exhaustion.
     *
     * The parent's busy_timeout=5000ms absorbs the block. A 200MB WAL takes
     * ~100-500ms to checkpoint — well within the 5s budget.
     */
    private function checkpointWithEscalation(SqliteBuffer $buffer): void
    {
        // Always run PASSIVE first — it's non-blocking and makes incremental progress
        $buffer->checkpoint();

        $walSize = $buffer->walSize();
        $this->walSizeBytes = $walSize;

        // Escalate to TRUNCATE when WAL exceeds the configured threshold —
        // smaller, more frequent checkpoints (50-200ms each) beat one rare
        // 200-500ms stall.
        if ($walSize > $this->checkpointTruncateBytes) {
            $walMb = round($walSize / 1024 / 1024);
            error_log("[NightOwl Drain] WAL is {$walMb}MB, running TRUNCATE checkpoint to reset...");

            $this->truncateAttempts++;
            try {
                $start = microtime(true);
                $buffer->checkpointTruncate();
                $elapsed = (int) round((microtime(true) - $start) * 1000);
                $this->truncateSuccesses++;
                $this->walSizeBytes = $buffer->walSize();
                error_log("[NightOwl Drain] TRUNCATE checkpoint complete in {$elapsed}ms. WAL reset to zero.");
            } catch (\Throwable $e) {
                // TRUNCATE can fail if a reader/writer can't be interrupted within busy_timeout.
                // Not fatal — PASSIVE already made partial progress. We'll try again next cycle.
                $this->truncateFailures++;
                error_log("[NightOwl Drain] TRUNCATE checkpoint failed (will retry): {$e->getMessage()}");
            }
        }
    }

    /**
     * Drain one batch. Returns true if rows were processed, false if empty or error.
     */
    private function drainBatch(SqliteBuffer $buffer, RecordWriter $writer): bool
    {
        // Clear any prior batch's write error so a failure BEFORE write() runs
        // (claimBatch/fetchPending under SQLite lock contention, disk-full) isn't
        // misattributed to the previous batch's stale Postgres SQLSTATE. write()
        // re-clears this on entry, so this only matters for pre-write throws.
        $writer->lastWriteError = null;

        $this->heartbeat?->enter('buffer:claim');

        try {
            // A prior batch committed to Postgres but its markSynced() failed (the
            // local buffer went unwritable, typically a full disk). Those rows are
            // still synced=0, so fetching now would re-COPY data already durably in
            // Postgres. Re-mark them FIRST, and never re-fetch/re-write until the
            // buffer accepts the mark again — this is the guard against a
            // duplication storm while the disk is full. If the mark still fails it
            // throws to the catch below, the ids stay held, and the loop backs off.
            if (! empty($this->committedUnmarkedIds)) {
                $buffer->markSynced($this->committedUnmarkedIds);
                $this->committedUnmarkedIds = [];
                $this->bufferMarkStalledSince = 0.0;

                return true;
            }

            $rows = $this->totalWorkers > 1
                ? $buffer->claimBatch($this->workerId, $this->batchSize)
                : $buffer->fetchPending($this->batchSize);

            if (empty($rows)) {
                return false;
            }

            // Decode into per-payload units, preserving the buffer id ↔ records
            // link so isolation can quarantine a single offending payload (records
            // are flattened only at write time). Unparseable rows are marked done
            // immediately so they can't head-of-line block.
            $units = [];
            $corruptIds = [];
            foreach ($rows as $row) {
                try {
                    $records = json_decode($row['payload'], true, 512, JSON_THROW_ON_ERROR);
                } catch (\JsonException) {
                    $corruptIds[] = $row['id'];

                    continue;
                }
                if (is_array($records)) {
                    $units[] = ['id' => $row['id'], 'records' => $records];
                } else {
                    $corruptIds[] = $row['id'];
                }
            }
            if (! empty($corruptIds)) {
                $buffer->markSynced($corruptIds);
            }
            if (empty($units)) {
                return true;
            }

            // Write to Postgres first, then mark synced. If the process is
            // hard-killed between write and mark, the rows stay unsynced and
            // are re-written on restart (at-most-one-batch duplicates). The
            // alternative (mark-first) loses data on hard kill — unacceptable
            // for a zero-data-loss requirement.
            $pgStart = microtime(true);
            if ($this->quarantineEnabled) {
                $result = $this->drainUnits($buffer, $writer, $units);
            } else {
                // Phase-1 behavior: all-or-nothing. A non-connection failure throws
                // out to the catch (recorded as DRAIN_WRITE_FAILING) and the whole
                // batch is retried — no quarantine.
                $records = [];
                foreach ($units as $u) {
                    array_push($records, ...$u['records']);
                }
                $writer->write($records);
                $this->markCommitted($buffer, array_column($units, 'id'));
                $this->onDrainSuccess($writer);
                $result = ['drained' => count($units), 'quarantined' => 0, 'deferred' => 0];
            }
            $pgElapsed = (microtime(true) - $pgStart) * 1000; // ms

            $this->batchesDrained++;
            $this->rowsDrained += $result['drained'];
            $this->pgLatencyEwma = $this->pgLatencyEwma === 0.0
                ? $pgElapsed
                : (self::EWMA_ALPHA * $pgElapsed) + ((1 - self::EWMA_ALPHA) * $this->pgLatencyEwma);

            // Treat a batch that only deferred (transient failures, nothing committed
            // or quarantined) as idle so the loop backs off instead of busy-looping.
            return ($result['drained'] + $result['quarantined']) > 0;
        } catch (\Throwable $e) {
            $this->batchesFailed++;
            error_log("[NightOwl Drain] Error: {$e->getMessage()}");

            // Classify the failure for the health report:
            //  - non-connection write rejection (PG reachable, refusing): capture
            //    SQLSTATE + table → DRAIN_WRITE_FAILING.
            //  - PG connection failure (unreachable / refused / connect-time throw,
            //    surfaced as connection=true by write()): stamp the connection clock →
            //    DRAIN_UNREACHABLE, independent of backlog size / lifetime drain volume.
            //  - null $err: a local buffer error (SQLite claim/fetch) — NOT a PG problem,
            //    so it stamps neither clock.
            $err = $writer->lastWriteError;
            if (is_array($err) && empty($err['connection'])) {
                $this->lastWriteSqlstate = is_string($err['sqlstate'] ?? null) ? $err['sqlstate'] : null;
                $this->lastWriteTable = is_string($err['table'] ?? null) ? $err['table'] : null;
                $this->lastWriteAt = microtime(true);
            } elseif (is_array($err) && ! empty($err['connection'])) {
                $this->lastConnFailAt = microtime(true);
            }

            return false;
        }
    }

    /**
     * Drain a set of payload units with poison-row isolation (Phase 2).
     *
     * Each subset that writes cleanly is markSynced immediately — an atomic
     * write→mark that preserves the at-most-one-batch-duplicate guarantee at
     * sub-batch granularity (a crash replays at most the in-flight subset, never
     * an already-committed one, so additive rollups can't double-count). On a
     * row-level data error the set is bisected to isolate the offending payload(s),
     * which — after a forceInsert retry — are quarantined so the rest drain.
     *
     * Connection failures and whole-target failures (missing schema / no privilege
     * / bad credentials / exhausted resources) are NOT the payload's fault: they
     * re-throw to abort isolation, leaving every unit pending for the next attempt
     * and surfacing DRAIN_WRITE_FAILING instead of quarantining good data.
     *
     * @param  array<int, array{id: int, records: array<int, mixed>}>  $units
     * @return array{drained: int, quarantined: int}
     */
    private function drainUnits(SqliteBuffer $buffer, RecordWriter $writer, array $units): array
    {
        // Heartbeat so a long isolation isn't mistaken for a stalled/crashed worker
        // (the run loop's periodic writeDrainMetrics is blocked while we recurse).
        $this->writeDrainMetrics();

        $records = [];
        foreach ($units as $u) {
            array_push($records, ...$u['records']);
        }
        $ids = array_column($units, 'id');

        try {
            $writer->write($records);
        } catch (\Throwable $e) {
            // Connection / whole-target failures hit every payload equally — abort
            // isolation and retry the whole batch (surfaces DRAIN_WRITE_FAILING).
            if ($this->isWholeTargetFailure($writer->lastWriteError)) {
                throw $e;
            }

            // Transient failures (serialization 40001, deadlock 40P01, lock 55xxx)
            // abort the whole COPY transaction, not one payload — defer the entire
            // batch for the next loop instead of recursively bisecting to the leaf.
            // Bisection would re-attempt ~2N-1 doomed writes (plus N leaf
            // force-inserts) before every singleton simply defers anyway: pure
            // wasted load with no progress, every loop the condition persists. The
            // leaf-level isTransientFailure check remains as a backstop.
            if ($this->isTransientFailure($writer->lastWriteError)) {
                return ['drained' => 0, 'quarantined' => 0, 'deferred' => count($units)];
            }

            // Deterministic per-row rejection — bisect to isolate the poison payload.
            if (count($units) > 1) {
                $mid = intdiv(count($units), 2);
                $left = $this->drainUnits($buffer, $writer, array_slice($units, 0, $mid));
                $right = $this->drainUnits($buffer, $writer, array_slice($units, $mid));

                return [
                    'drained' => $left['drained'] + $right['drained'],
                    'quarantined' => $left['quarantined'] + $right['quarantined'],
                    'deferred' => $left['deferred'] + $right['deferred'],
                ];
            }

            return $this->isolateSinglePayload($buffer, $writer, $units[0]);
        }

        // write() committed to Postgres. markSynced() is a local SQLite UPDATE; a
        // failure here (SQLITE_BUSY past busy_timeout, disk-full, I/O) is NOT a
        // payload rejection — the rows are already durably committed. The old code
        // wrapped markSynced() in the same try, so a post-commit SQLite fault left
        // lastWriteError === null, isWholeTargetFailure(null) returned false, and the
        // batch bisected — re-COPYing committed rows (duplicate telemetry +
        // double-counted additive rollups). Keeping markSynced() outside the
        // write() try lets the fault propagate to drainBatch's catch, which sees
        // lastWriteError === null and records it as a local buffer error (no PG
        // diagnosis). markCommitted holds the committed ids so they are re-marked,
        // never re-written, next loop — the documented at-most-one-batch-duplicate
        // crash-safety tradeoff, never a bisection storm and never a re-COPY loop.
        $this->markCommitted($buffer, $ids);
        $this->onDrainSuccess($writer);

        return ['drained' => count($units), 'quarantined' => 0, 'deferred' => 0];
    }

    /**
     * One isolated payload that failed the batch write. Re-run it alone via INSERT
     * (clears a COPY-hostile target and forces a definitive SQLSTATE), then:
     *   - whole-target failure (connection/schema/auth) → re-throw, retry whole batch;
     *   - transient failure (deadlock/serialization/lock) → DEFER: leave it pending,
     *     don't abort its siblings, retry next loop (it is not deterministically bad);
     *   - circuit breaker open (sustained ~100% quarantine rate ⇒ systematic schema
     *     mismatch, not per-row poison) → re-throw so the stream head-of-line blocks
     *     and surfaces DRAIN_WRITE_FAILING instead of silently dropping everything;
     *   - otherwise the database DETERMINISTICALLY rejects just this payload (22/23,
     *     54000 index-row-too-large, …) → quarantine it so the rest of the stream drains.
     *
     * @param  array{id: int, records: array<int, mixed>}  $unit
     * @return array{drained: int, quarantined: int, deferred: int}
     */
    private function isolateSinglePayload(SqliteBuffer $buffer, RecordWriter $writer, array $unit): array
    {
        $ids = [$unit['id']];

        try {
            $writer->writeForceInsert($unit['records']);
            $this->markCommitted($buffer, $ids);
            $this->onDrainSuccess($writer);

            return ['drained' => 1, 'quarantined' => 0, 'deferred' => 0];
        } catch (\Throwable $e) {
            $err = $writer->lastWriteError;

            if ($this->isWholeTargetFailure($err)) {
                throw $e;
            }
            if ($this->isTransientFailure($err)) {
                return ['drained' => 0, 'quarantined' => 0, 'deferred' => 1];
            }
            $table = is_array($err) && is_string($err['table'] ?? null) ? $err['table'] : '?';
            if (($this->consecutiveQuarantines[$table] ?? 0) >= $this->quarantineBreakerThreshold) {
                // Systematic for THIS table — every payload touching it rejects with no
                // successful write of it in between — not per-row poison. Stop dropping
                // the stream; re-throw to head-of-line block and surface DRAIN_WRITE_FAILING.
                throw $e;
            }

            $sqlstate = is_array($err) && is_string($err['sqlstate'] ?? null) ? $err['sqlstate'] : null;
            $buffer->quarantine($ids);
            $this->quarantinedTotal++;
            $this->consecutiveQuarantines[$table] = ($this->consecutiveQuarantines[$table] ?? 0) + 1;
            $this->lastQuarantineSqlstate = $sqlstate;
            $this->lastQuarantineTable = $table === '?' ? null : $table;
            $this->lastQuarantineAt = microtime(true);
            error_log(sprintf(
                '[NightOwl Drain] Quarantined poison payload id=%d (SQLSTATE %s on %s)',
                $unit['id'],
                $sqlstate ?? 'unknown',
                $this->lastQuarantineTable ?? '?'
            ));

            return ['drained' => 0, 'quarantined' => 1, 'deferred' => 0];
        }
    }

    /**
     * A successful write: record drain progress and roll up app vitals. Clears the
     * systematic-poison breaker streak for every table this write actually landed —
     * a table that just drained is not systematically broken. Tables NOT written
     * here (the one whose poison is being isolated) keep their streak, so a genuine
     * per-table schema mismatch still climbs to the threshold even while bisection
     * drains clean siblings of other tables (the bug the old blanket reset caused).
     */
    /**
     * Mark rows synced after their write() committed to Postgres, holding them for
     * a mark-only retry if the local buffer write fails.
     *
     * markSynced() is a SQLite UPDATE. When the buffer is unwritable — a full local
     * disk being the common cause, and the very condition that made the buffer back
     * up — that UPDATE throws even though the rows are already durably in Postgres.
     * Re-throwing WITHOUT remembering the ids lets the run loop re-fetch them (they
     * are still synced=0) and re-COPY them next tick, and the next, duplicating
     * committed telemetry without bound for as long as the disk stays full. So we
     * stash the ids; drainBatch re-marks them (never re-writes) before fetching
     * anything new, and only advances once the buffer accepts the mark again.
     *
     * @param  int[]  $ids
     */
    private function markCommitted(SqliteBuffer $buffer, array $ids): void
    {
        try {
            $buffer->markSynced($ids);
        } catch (\Throwable $e) {
            $this->committedUnmarkedIds = array_values(
                array_unique([...$this->committedUnmarkedIds, ...$ids])
            );
            $this->bufferMarkStalls++;
            if ($this->bufferMarkStalledSince === 0.0) {
                $this->bufferMarkStalledSince = microtime(true);
            }
            error_log(
                '[NightOwl Drain] Buffer unwritable — '.count($ids).' rows committed to '
                .'Postgres could not be marked synced (likely a full local disk). Draining '
                .'paused to avoid duplicating committed data; resumes when the buffer is writable.'
            );
            throw $e;
        }
    }

    private function onDrainSuccess(RecordWriter $writer): void
    {
        $this->lastWriteOkAt = microtime(true);
        foreach ($writer->lastWrittenTables as $table) {
            unset($this->consecutiveQuarantines[$table]);
        }
        $this->cumRequests += $writer->lastRequestCount;
        $this->cum5xx += $writer->last5xxCount;
        $this->cumExceptions += $writer->lastExceptionCount;
    }

    /**
     * A failure that hits the whole target rather than one payload — re-throw to
     * abort isolation and retry the whole batch (surfacing DRAIN_WRITE_FAILING):
     * connection drops, plus schema/syntax/privilege (42), auth (28), catalog (3D),
     * resource (53), operator (57), system (58), and read-only mode (25006 — a
     * managed DB flipped read-only, usually a full disk; every write fails, so
     * quarantining rows would drop good data for a transient whole-target state).
     * An EMPTY SQLSTATE is NOT treated as whole-target — isolation proceeds so the
     * per-payload INSERT can surface a code.
     *
     * @param  mixed  $err  RecordWriter::$lastWriteError
     */
    private function isWholeTargetFailure($err): bool
    {
        if (! is_array($err)) {
            return false;
        }
        if (! empty($err['connection'])) {
            return true;
        }
        $sqlstate = is_string($err['sqlstate'] ?? null) ? $err['sqlstate'] : '';
        if ($sqlstate === '') {
            return false;
        }

        return str_starts_with($sqlstate, '08')
            || $sqlstate === '25006'
            || str_starts_with($sqlstate, '42')
            || str_starts_with($sqlstate, '28')
            || str_starts_with($sqlstate, '3D')
            || str_starts_with($sqlstate, '53')
            || str_starts_with($sqlstate, '57')
            || str_starts_with($sqlstate, '58');
    }

    /**
     * Transient, non-deterministic failures — serialization (40001), deadlock
     * (40P01), lock-not-available / object-in-use (55xxx). Not the payload's fault
     * and likely to succeed on retry, so DEFER rather than quarantine.
     *
     * @param  mixed  $err  RecordWriter::$lastWriteError
     */
    private function isTransientFailure($err): bool
    {
        if (! is_array($err)) {
            return false;
        }
        $sqlstate = is_string($err['sqlstate'] ?? null) ? $err['sqlstate'] : '';

        return str_starts_with($sqlstate, '40') || str_starts_with($sqlstate, '55');
    }

    /**
     * Write drain metrics to a temp file for IPC with the parent process.
     * Uses atomic write (tmp + rename) to prevent partial reads.
     */
    private function writeDrainMetrics(): void
    {
        $metricsPath = $this->totalWorkers > 1
            ? $this->sqlitePath.".drain-metrics-{$this->workerId}.json"
            : $this->sqlitePath.'.drain-metrics.json';
        $tmpPath = $metricsPath.'.tmp';

        // JSON_INVALID_UTF8_SUBSTITUTE (and no JSON_THROW_ON_ERROR): the write
        // error fields are ASCII (SQLSTATE + table identifier), but harden anyway
        // so a stray byte can never throw out of the run loop and crash the worker.
        $data = json_encode([
            'batches_drained' => $this->batchesDrained,
            'batches_failed' => $this->batchesFailed,
            'rows_drained' => $this->rowsDrained,
            'app_requests_total' => $this->cumRequests,
            'app_requests_5xx' => $this->cum5xx,
            'app_exceptions_total' => $this->cumExceptions,
            'app_open_issues' => $this->openIssues,
            'pg_latency_ms' => round($this->pgLatencyEwma, 2),
            'wal_size_bytes' => $this->walSizeBytes,
            'truncate_attempts' => $this->truncateAttempts,
            'truncate_successes' => $this->truncateSuccesses,
            'truncate_failures' => $this->truncateFailures,
            'last_write_sqlstate' => $this->lastWriteSqlstate,
            'last_write_table' => $this->lastWriteTable,
            'last_write_at' => $this->lastWriteAt,
            'last_write_ok_at' => $this->lastWriteOkAt,
            'last_conn_fail_at' => $this->lastConnFailAt,
            // Buffer-unwritable stall (full local disk): the drain is paused, NOT
            // duplicating. buffer_mark_stalled_since > 0 means a stall is in progress.
            'buffer_mark_stalls' => $this->bufferMarkStalls,
            'buffer_mark_stalled_since' => $this->bufferMarkStalledSince,
            'committed_unmarked' => count($this->committedUnmarkedIds),
            'quarantined_live' => $this->quarantinedLive,
            // Cumulative since worker start (never decays). The DRAIN_QUARANTINE
            // diagnosis reads THIS, not quarantined_live (the prunable buffer gauge
            // that pruneQuarantined() resets to 0 after the retention window — which
            // would silently clear a critical that dropped real telemetry).
            'quarantined_total' => $this->quarantinedTotal,
            'last_quarantine_sqlstate' => $this->lastQuarantineSqlstate,
            'last_quarantine_table' => $this->lastQuarantineTable,
            'last_quarantine_at' => $this->lastQuarantineAt,
            'updated_at' => microtime(true),
        ], JSON_INVALID_UTF8_SUBSTITUTE);

        if ($data !== false && @file_put_contents($tmpPath, $data) !== false) {
            @rename($tmpPath, $metricsPath);
        }
    }
}
