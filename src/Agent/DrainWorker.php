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
    // "Postgres may be unreachable". Connection failures are excluded — those are
    // handled by reconnect/DRAIN_STOPPED.
    private ?string $lastWriteSqlstate = null;

    private ?string $lastWriteTable = null;

    private float $lastWriteAt = 0.0;

    private float $lastWriteOkAt = 0.0;

    // Poison-payload quarantine (Phase 2). Cumulative since worker start — a
    // dropped telemetry row is data loss, so the DRAIN_QUARANTINE diagnosis stays
    // visible. SQLSTATE + table only, never raw row values.
    private int $quarantinedTotal = 0;

    private ?string $lastQuarantineSqlstate = null;

    private ?string $lastQuarantineTable = null;

    private float $lastQuarantineAt = 0.0;

    // Consecutive quarantines with no intervening successful drain (systematic-poison
    // circuit breaker). Live count of dead-lettered rows in the buffer (durable
    // across restart — drives the DRAIN_QUARANTINE diagnosis).
    private int $consecutiveQuarantines = 0;

    private int $quarantinedLive = 0;

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
        );

        $workerLabel = $this->totalWorkers > 1
            ? "Worker #{$this->workerId}"
            : 'Worker';
        error_log("[NightOwl Drain] {$workerLabel} started (pid: ".getmypid().')');

        $lastCleanup = time();
        $lastFlushTime = microtime(true);
        $lastMetricsWrite = 0.0;
        $lastIssueCount = 0.0;

        while ($this->running) {
            $drained = $this->drainBatch($buffer, $writer);

            if ($drained) {
                $lastFlushTime = microtime(true);
            }

            // Cleanup + WAL checkpoint on the configured cadence (default 60s)
            if (time() - $lastCleanup >= $this->checkpointIntervalSeconds) {
                try {
                    $buffer->cleanup(300);
                    if ($this->quarantineEnabled) {
                        $buffer->pruneQuarantined($this->quarantineRetentionSeconds);
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
        try {
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
                $buffer->markSynced(array_column($units, 'id'));
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

            // Capture the SQLSTATE + table for the health report, but only for
            // NON-connection failures (PG reachable, rejecting the write). True
            // connection failures are handled by reconnect + DRAIN_STOPPED; folding
            // them in here would mislabel "unreachable" as "rejecting writes".
            $err = $writer->lastWriteError;
            if (is_array($err) && empty($err['connection'])) {
                $this->lastWriteSqlstate = is_string($err['sqlstate'] ?? null) ? $err['sqlstate'] : null;
                $this->lastWriteTable = is_string($err['table'] ?? null) ? $err['table'] : null;
                $this->lastWriteAt = microtime(true);
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
            $buffer->markSynced($ids);
            $this->onDrainSuccess($writer);

            return ['drained' => count($units), 'quarantined' => 0, 'deferred' => 0];
        } catch (\Throwable $e) {
            // Connection / whole-target failures hit every payload equally — abort
            // isolation and retry the whole batch (surfaces DRAIN_WRITE_FAILING).
            if ($this->isWholeTargetFailure($writer->lastWriteError)) {
                throw $e;
            }

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
            $buffer->markSynced($ids);
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
            if ($this->consecutiveQuarantines >= $this->quarantineBreakerThreshold) {
                // Systematic, not per-row — stop dropping the stream; surface it.
                throw $e;
            }

            $sqlstate = is_array($err) && is_string($err['sqlstate'] ?? null) ? $err['sqlstate'] : null;
            $buffer->quarantine($ids);
            $this->quarantinedTotal++;
            $this->consecutiveQuarantines++;
            $this->lastQuarantineSqlstate = $sqlstate;
            $this->lastQuarantineTable = is_array($err) && is_string($err['table'] ?? null) ? $err['table'] : null;
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

    /** A successful write: record progress and reset the systematic-poison breaker. */
    private function onDrainSuccess(RecordWriter $writer): void
    {
        $this->lastWriteOkAt = microtime(true);
        $this->consecutiveQuarantines = 0;
        $this->cumRequests += $writer->lastRequestCount;
        $this->cum5xx += $writer->last5xxCount;
        $this->cumExceptions += $writer->lastExceptionCount;
    }

    /**
     * A failure that hits the whole target rather than one payload — re-throw to
     * abort isolation and retry the whole batch (surfacing DRAIN_WRITE_FAILING):
     * connection drops, plus schema/syntax/privilege (42), auth (28), catalog (3D),
     * resource (53), operator (57), system (58). An EMPTY SQLSTATE is NOT treated as
     * whole-target — isolation proceeds so the per-payload INSERT can surface a code.
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
            'quarantined_live' => $this->quarantinedLive,
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
