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

            $allRecords = [];
            $ids = [];

            foreach ($rows as $row) {
                $ids[] = $row['id'];
                try {
                    $records = json_decode($row['payload'], true, 512, JSON_THROW_ON_ERROR);
                } catch (\JsonException) {
                    // Row is corrupt — skip it rather than crash the drain worker.
                    continue;
                }
                if (is_array($records)) {
                    array_push($allRecords, ...$records);
                }
            }

            // Write to Postgres first, then mark synced. If the process is
            // hard-killed between write and mark, the rows stay unsynced and
            // are re-written on restart (at-most-one-batch duplicates). The
            // alternative (mark-first) loses data on hard kill — unacceptable
            // for a zero-data-loss requirement.
            $pgStart = microtime(true);
            $writer->write($allRecords);
            $pgElapsed = (microtime(true) - $pgStart) * 1000; // ms

            $buffer->markSynced($ids);

            // Update drain metrics
            $this->batchesDrained++;
            $this->rowsDrained += count($rows);

            // Accumulate app-vitals from the batch the writer just persisted.
            $this->cumRequests += $writer->lastRequestCount;
            $this->cum5xx += $writer->last5xxCount;
            $this->cumExceptions += $writer->lastExceptionCount;
            $this->pgLatencyEwma = $this->pgLatencyEwma === 0.0
                ? $pgElapsed
                : (self::EWMA_ALPHA * $pgElapsed) + ((1 - self::EWMA_ALPHA) * $this->pgLatencyEwma);

            return true;
        } catch (\Throwable $e) {
            $this->batchesFailed++;
            error_log("[NightOwl Drain] Error: {$e->getMessage()}");

            return false;
        }
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
            'updated_at' => microtime(true),
        ], JSON_THROW_ON_ERROR);

        if (@file_put_contents($tmpPath, $data) !== false) {
            @rename($tmpPath, $metricsPath);
        }
    }
}
