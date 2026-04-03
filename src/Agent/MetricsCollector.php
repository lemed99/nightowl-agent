<?php

namespace NightOwl\Agent;

/**
 * In-memory metrics collector for the agent parent process.
 *
 * Tracks ingest/drain counters, 60-slot ring buffers for 1-minute rolling
 * averages, event loop lag, and runs diagnosis rules every 10s to produce
 * a health score and actionable diagnoses.
 */
final class MetricsCollector
{
    public const AGENT_VERSION = '1.0.0';

    // Ring buffer for 1-minute rolling averages (1 slot per second)
    private const RING_SIZE = 60;

    // Diagnosis thresholds
    private const BACKLOG_HIGH_PCT = 0.5;
    private const BACKLOG_CRITICAL_PCT = 0.8;
    private const PG_LATENCY_WARNING_MS = 500;
    private const PG_LATENCY_CRITICAL_MS = 2000;
    private const DRAIN_ERROR_RATE_PCT = 10;
    private const LOOP_LAG_WARNING_MS = 50;
    private const LOOP_LAG_CRITICAL_MS = 200;
    private const WAL_LARGE_BYTES = 100 * 1024 * 1024; // 100MB
    private const DRAIN_METRICS_STALE_SECONDS = 15;
    private const MEMORY_HIGH_PCT = 0.7;

    // System metrics thresholds
    private const CPU_HIGH_PCT = 70.0;
    private const CPU_CRITICAL_PCT = 90.0;
    private const SYS_MEMORY_HIGH_PCT = 75.0;
    private const SYS_MEMORY_CRITICAL_PCT = 90.0;
    private const REJECT_RATE_HIGH_PCT = 2.0;
    private const REJECT_RATE_CRITICAL_PCT = 10.0;

    // Ingest counters
    private int $ingestTotal = 0;
    private int $ingestRejected = 0;
    private int $ingestThisSecond = 0;
    private int $rejectsThisSecond = 0;

    // Drain metrics (read from temp file written by DrainWorker)
    private int $drainTotal = 0;
    private int $drainBatchesFailed = 0;
    private float $drainPgLatencyMs = 0.0;
    private float $drainMetricsUpdatedAt = 0.0;

    // Previous drain totals for rate calculation
    private int $prevDrainTotal = 0;

    // Ring buffers
    /** @var int[] */
    private array $ingestRing = [];
    private int $ingestRingIdx = 0;

    /** @var int[] */
    private array $drainRing = [];
    private int $drainRingIdx = 0;

    /** @var int[] */
    private array $rejectRing = [];
    private int $rejectRingIdx = 0;

    // Loop lag tracking
    private float $lastTickTime = 0.0;
    /** @var float[] */
    private array $lagRing = [];
    private int $lagRingIdx = 0;
    private float $lagMax = 0.0;

    // System metrics (Linux /proc, updated every 10s diagnosis tick)
    private float $cpuUsagePct = 0.0;
    private float $sysMemoryUsedPct = 0.0;
    private int $sysMemoryTotalBytes = 0;
    private int $sysMemoryAvailableBytes = 0;
    /** @var float[] */
    private array $loadAvg = [0.0, 0.0, 0.0];
    // Previous CPU jiffies for delta calculation
    private int $prevCpuTotal = 0;
    private int $prevCpuIdle = 0;

    // Diagnosis results
    /** @var array<int, array{code: string, level: string, message: string, recommendation: string, value: float|int}> */
    private array $diagnoses = [];
    private int $healthScore = 100;
    private string $status = 'healthy';

    // Diagnosis lifecycle tracking
    /** @var array<string, array{first_seen_at: string, last_seen_at: string, status: string, consecutive_ticks: int, level: string, message: string, recommendation: string, value: float|int}> */
    private array $diagnosisLifecycle = [];
    /** @var array<int, array{code: string, level: string, message: string, recommendation: string, value: float|int}> Diagnoses that resolved on the current tick */
    private array $newlyResolved = [];

    // Anti-flapping: minimum consecutive ticks before a diagnosis is reported
    private const DEBOUNCE_TICKS = 2;
    // Minimum ticks of active state before a diagnosis can be marked as resolved (vs transient)
    private const MIN_TICKS_FOR_RESOLVE = 3;
    // How long to keep resolved diagnoses before garbage collection (seconds)
    private const RESOLVED_RETENTION_SECONDS = 300;

    public function __construct(
        private int $maxPendingRows = 100_000,
        private int $maxBufferMemory = 256 * 1024 * 1024,
    ) {
        $this->ingestRing = array_fill(0, self::RING_SIZE, 0);
        $this->rejectRing = array_fill(0, self::RING_SIZE, 0);
        $this->drainRing = array_fill(0, self::RING_SIZE, 0);
        $this->lagRing = array_fill(0, self::RING_SIZE, 0.0);
    }

    /**
     * Called after a successful payload is buffered to SQLite.
     */
    public function recordIngest(): void
    {
        $this->ingestTotal++;
        $this->ingestThisSecond++;
    }

    /**
     * Called when a payload is rejected (back-pressure, token failure, etc.).
     */
    public function recordReject(): void
    {
        $this->ingestRejected++;
        $this->rejectsThisSecond++;
    }

    /**
     * Called every 1 second from the event loop.
     * Advances ring buffers and measures event loop lag.
     */
    public function tick(): void
    {
        $now = microtime(true);

        // Event loop lag: difference between expected (1s) and actual elapsed time
        if ($this->lastTickTime > 0) {
            $elapsed = ($now - $this->lastTickTime) * 1000; // ms
            $lag = max(0, $elapsed - 1000);
            $this->lagRing[$this->lagRingIdx] = $lag;
            $this->lagRingIdx = ($this->lagRingIdx + 1) % self::RING_SIZE;
            if ($lag > $this->lagMax) {
                $this->lagMax = $lag;
            }
        }
        $this->lastTickTime = $now;

        // Push ingest and reject counts for this second into ring buffers
        $this->ingestRing[$this->ingestRingIdx] = $this->ingestThisSecond;
        $this->ingestRingIdx = ($this->ingestRingIdx + 1) % self::RING_SIZE;
        $this->ingestThisSecond = 0;

        $this->rejectRing[$this->rejectRingIdx] = $this->rejectsThisSecond;
        $this->rejectRingIdx = ($this->rejectRingIdx + 1) % self::RING_SIZE;
        $this->rejectsThisSecond = 0;

        // Zero out the next drain ring slot so stale data doesn't persist.
        // Actual drain values are backfilled by readDrainMetrics() every 5s.
        $this->drainRing[$this->drainRingIdx] = 0;
        $this->drainRingIdx = ($this->drainRingIdx + 1) % self::RING_SIZE;
    }

    /**
     * Called every 5 seconds to read drain metrics from the temp file(s)
     * written by DrainWorker(s).
     *
     * In multi-worker mode, aggregates metrics across all workers:
     * - rows_drained: summed
     * - batches_failed: summed
     * - pg_latency_ms: averaged
     * - updated_at: earliest (most conservative for staleness detection)
     */
    public function readDrainMetrics(string $path, int $workerCount = 1): void
    {
        $totalRows = 0;
        $totalFailed = 0;
        $latencySum = 0.0;
        $latencyCount = 0;
        $oldestUpdate = PHP_FLOAT_MAX;
        $anyFound = false;

        for ($w = 0; $w < $workerCount; $w++) {
            $metricsPath = $workerCount > 1
                ? $path . ".drain-metrics-{$w}.json"
                : $path . '.drain-metrics.json';

            if (! file_exists($metricsPath)) {
                continue;
            }

            $json = @file_get_contents($metricsPath);
            if ($json === false) {
                continue;
            }

            $data = json_decode($json, true);
            if (! is_array($data)) {
                continue;
            }

            $anyFound = true;
            $totalRows += (int) ($data['rows_drained'] ?? 0);
            $totalFailed += (int) ($data['batches_failed'] ?? 0);

            $lat = (float) ($data['pg_latency_ms'] ?? 0.0);
            if ($lat > 0) {
                $latencySum += $lat;
                $latencyCount++;
            }

            $updatedAt = (float) ($data['updated_at'] ?? 0.0);
            if ($updatedAt > 0 && $updatedAt < $oldestUpdate) {
                $oldestUpdate = $updatedAt;
            }
        }

        if (! $anyFound) {
            return;
        }

        // Calculate rows drained since last read for per-second rate tracking
        $delta = max(0, $totalRows - $this->prevDrainTotal);
        $this->prevDrainTotal = $totalRows;

        // Distribute the delta across the drain ring buffer slots
        // (5 seconds worth spread into remaining slots, with remainder distribution)
        if ($delta > 0) {
            $perSecond = intdiv($delta, 5);
            $remainder = $delta % 5;
            for ($i = 0; $i < 5; $i++) {
                $idx = ($this->drainRingIdx - 1 - $i + self::RING_SIZE) % self::RING_SIZE;
                $this->drainRing[$idx] += $perSecond + ($i < $remainder ? 1 : 0);
            }
        }

        $this->drainTotal = $totalRows;
        $this->drainBatchesFailed = $totalFailed;
        $this->drainPgLatencyMs = $latencyCount > 0 ? $latencySum / $latencyCount : 0.0;
        $this->drainMetricsUpdatedAt = $oldestUpdate < PHP_FLOAT_MAX ? $oldestUpdate : 0.0;
    }

    /**
     * Run diagnosis rules every 10 seconds.
     */
    public function runDiagnosis(bool $backPressure, int $pendingRows, int $walSize, int $rss): void
    {
        // Collect system metrics before running rules
        $this->collectSystemMetrics();

        $diagnoses = [];

        $ingestRate = $this->ringAvg($this->ingestRing);
        $drainRate = $this->ringAvg($this->drainRing);
        $rejectRate = $this->ringAvg($this->rejectRing);
        $lagAvg = $this->ringAvgFloat($this->lagRing);
        $drainMetricsStale = $this->drainMetricsUpdatedAt > 0
            && (microtime(true) - $this->drainMetricsUpdatedAt) > self::DRAIN_METRICS_STALE_SECONDS;

        // Compute reject rate as percentage of total traffic
        $totalTraffic = $ingestRate + $rejectRate;
        $rejectPct = $totalTraffic > 0 ? ($rejectRate / $totalTraffic) * 100 : 0.0;

        // DRAIN_STOPPED
        if ($drainRate == 0 && $pendingRows > 100) {
            $diagnoses[] = [
                'code' => 'DRAIN_STOPPED',
                'level' => 'critical',
                'message' => 'Drain worker is not processing rows.',
                'recommendation' => 'Check PostgreSQL connectivity and drain worker logs. The drain process may have crashed or Postgres may be unreachable.',
                'value' => $pendingRows,
            ];
        }

        // DRAIN_FALLING_BEHIND
        if ($ingestRate > $drainRate * 1.5 && $pendingRows > 5000) {
            $diagnoses[] = [
                'code' => 'DRAIN_FALLING_BEHIND',
                'level' => 'warning',
                'message' => 'Ingest rate exceeds drain rate.',
                'recommendation' => 'Consider increasing drain_batch_size, optimizing PostgreSQL write performance, or scaling horizontally.',
                'value' => round($ingestRate, 1),
            ];
        }

        // BACKLOG_CRITICAL (check before BACKLOG_HIGH)
        if ($pendingRows > $this->maxPendingRows * self::BACKLOG_CRITICAL_PCT) {
            $diagnoses[] = [
                'code' => 'BACKLOG_CRITICAL',
                'level' => 'critical',
                'message' => 'Buffer near capacity, back-pressure imminent.',
                'recommendation' => 'Investigate drain bottleneck immediately. PostgreSQL may be slow or unreachable.',
                'value' => $pendingRows,
            ];
        } elseif ($pendingRows > $this->maxPendingRows * self::BACKLOG_HIGH_PCT) {
            $diagnoses[] = [
                'code' => 'BACKLOG_HIGH',
                'level' => 'warning',
                'message' => 'Buffer is over 50% capacity.',
                'recommendation' => 'Monitor drain rate. If backlog continues growing, check PostgreSQL performance.',
                'value' => $pendingRows,
            ];
        }

        // BACK_PRESSURE_ACTIVE
        if ($backPressure) {
            $diagnoses[] = [
                'code' => 'BACK_PRESSURE_ACTIVE',
                'level' => 'critical',
                'message' => 'Agent is rejecting payloads.',
                'recommendation' => 'The buffer is full or memory limit reached. Investigate drain bottleneck and PostgreSQL connectivity.',
                'value' => $pendingRows,
            ];
        }

        // PG_LATENCY
        if ($this->drainPgLatencyMs > self::PG_LATENCY_CRITICAL_MS) {
            $diagnoses[] = [
                'code' => 'PG_LATENCY_CRITICAL',
                'level' => 'critical',
                'message' => 'Postgres write latency critically high.',
                'recommendation' => 'Check PostgreSQL server load, connection pool, and network latency.',
                'value' => round($this->drainPgLatencyMs, 1),
            ];
        } elseif ($this->drainPgLatencyMs > self::PG_LATENCY_WARNING_MS) {
            $diagnoses[] = [
                'code' => 'PG_LATENCY_HIGH',
                'level' => 'warning',
                'message' => 'Postgres write latency is elevated.',
                'recommendation' => 'Monitor PostgreSQL performance. Consider checking for lock contention or resource exhaustion.',
                'value' => round($this->drainPgLatencyMs, 1),
            ];
        }

        // DRAIN_ERRORS
        $totalBatches = $this->drainTotal > 0 ? ($this->drainTotal / max(1, 1000)) : 0; // rough batch estimate
        if ($this->drainBatchesFailed > 0 && $totalBatches > 0) {
            $failRate = ($this->drainBatchesFailed / ($this->drainBatchesFailed + $totalBatches)) * 100;
            if ($failRate > self::DRAIN_ERROR_RATE_PCT) {
                $diagnoses[] = [
                    'code' => 'DRAIN_ERRORS',
                    'level' => 'warning',
                    'message' => 'Drain worker experiencing batch failures.',
                    'recommendation' => 'Check drain worker logs for PostgreSQL connection or write errors.',
                    'value' => round($failRate, 1),
                ];
            }
        }

        // LOOP_LAG
        if ($lagAvg > self::LOOP_LAG_CRITICAL_MS) {
            $diagnoses[] = [
                'code' => 'LOOP_LAG_CRITICAL',
                'level' => 'critical',
                'message' => 'Event loop severely lagging.',
                'recommendation' => 'The agent process is overloaded. Consider reducing traffic or scaling horizontally.',
                'value' => round($lagAvg, 1),
            ];
        } elseif ($lagAvg > self::LOOP_LAG_WARNING_MS) {
            $diagnoses[] = [
                'code' => 'LOOP_LAG_HIGH',
                'level' => 'warning',
                'message' => 'Event loop is lagging.',
                'recommendation' => 'The agent may be under heavy load. Monitor for degradation.',
                'value' => round($lagAvg, 1),
            ];
        }

        // WAL_LARGE
        if ($walSize > self::WAL_LARGE_BYTES) {
            $diagnoses[] = [
                'code' => 'WAL_LARGE',
                'level' => 'warning',
                'message' => 'SQLite WAL file is large.',
                'recommendation' => 'The drain worker may be falling behind. WAL will be checkpointed automatically when the drain catches up.',
                'value' => $walSize,
            ];
        }

        // DRAIN_METRICS_STALE
        if ($drainMetricsStale) {
            $diagnoses[] = [
                'code' => 'DRAIN_METRICS_STALE',
                'level' => 'warning',
                'message' => 'No recent drain worker metrics.',
                'recommendation' => 'The drain worker may have crashed. Check process status and logs.',
                'value' => round(microtime(true) - $this->drainMetricsUpdatedAt, 1),
            ];
        }

        // MEMORY_HIGH
        if ($rss > $this->maxBufferMemory * self::MEMORY_HIGH_PCT) {
            $diagnoses[] = [
                'code' => 'MEMORY_HIGH',
                'level' => 'warning',
                'message' => 'Memory usage is high.',
                'recommendation' => 'The agent is using over 70% of its memory limit. Consider increasing max_buffer_memory or reducing traffic.',
                'value' => $rss,
            ];
        }

        // REJECT_RATE
        if ($rejectPct > self::REJECT_RATE_CRITICAL_PCT) {
            $diagnoses[] = [
                'code' => 'REJECT_RATE_CRITICAL',
                'level' => 'critical',
                'message' => sprintf('Rejecting %.1f%% of incoming payloads.', $rejectPct),
                'recommendation' => 'Agent is dropping significant traffic. Check buffer capacity, drain performance, and consider scaling.',
                'value' => round($rejectPct, 1),
            ];
        } elseif ($rejectPct > self::REJECT_RATE_HIGH_PCT) {
            $diagnoses[] = [
                'code' => 'REJECT_RATE_HIGH',
                'level' => 'warning',
                'message' => sprintf('Rejecting %.1f%% of incoming payloads.', $rejectPct),
                'recommendation' => 'Some payloads are being dropped. Monitor buffer utilization and drain rate.',
                'value' => round($rejectPct, 1),
            ];
        }

        // CPU_SATURATION (Linux only — /proc/stat)
        if ($this->cpuUsagePct > self::CPU_CRITICAL_PCT) {
            $diagnoses[] = [
                'code' => 'CPU_SATURATION',
                'level' => 'critical',
                'message' => sprintf('Host CPU usage at %.1f%%.', $this->cpuUsagePct),
                'recommendation' => 'Server is CPU-saturated. Reduce traffic, add agent instances, or upgrade hardware.',
                'value' => round($this->cpuUsagePct, 1),
            ];
        } elseif ($this->cpuUsagePct > self::CPU_HIGH_PCT) {
            $diagnoses[] = [
                'code' => 'CPU_HIGH',
                'level' => 'warning',
                'message' => sprintf('Host CPU usage at %.1f%%.', $this->cpuUsagePct),
                'recommendation' => 'CPU usage is elevated. Monitor for further degradation.',
                'value' => round($this->cpuUsagePct, 1),
            ];
        }

        // SYSTEM_MEMORY (Linux only — /proc/meminfo)
        if ($this->sysMemoryUsedPct > self::SYS_MEMORY_CRITICAL_PCT) {
            $diagnoses[] = [
                'code' => 'SYSTEM_MEMORY_CRITICAL',
                'level' => 'critical',
                'message' => sprintf('System memory %.1f%% used, only %s available.', $this->sysMemoryUsedPct, $this->formatBytes($this->sysMemoryAvailableBytes)),
                'recommendation' => 'Server is running out of memory. Risk of OOM killer. Reduce load or add memory.',
                'value' => round($this->sysMemoryUsedPct, 1),
            ];
        } elseif ($this->sysMemoryUsedPct > self::SYS_MEMORY_HIGH_PCT) {
            $diagnoses[] = [
                'code' => 'SYSTEM_MEMORY_HIGH',
                'level' => 'warning',
                'message' => sprintf('System memory %.1f%% used, %s available.', $this->sysMemoryUsedPct, $this->formatBytes($this->sysMemoryAvailableBytes)),
                'recommendation' => 'System memory usage is elevated. Monitor for pressure.',
                'value' => round($this->sysMemoryUsedPct, 1),
            ];
        }

        // Update diagnosis lifecycle
        $now = gmdate('Y-m-d\TH:i:s\Z');
        $activeCodes = [];

        foreach ($diagnoses as $d) {
            $code = $d['code'];
            $activeCodes[$code] = true;

            if (isset($this->diagnosisLifecycle[$code]) && $this->diagnosisLifecycle[$code]['status'] === 'active') {
                // Existing active diagnosis — update
                $this->diagnosisLifecycle[$code]['last_seen_at'] = $now;
                $this->diagnosisLifecycle[$code]['consecutive_ticks']++;
                $this->diagnosisLifecycle[$code]['level'] = $d['level'];
                $this->diagnosisLifecycle[$code]['message'] = $d['message'];
                $this->diagnosisLifecycle[$code]['recommendation'] = $d['recommendation'];
                $this->diagnosisLifecycle[$code]['value'] = $d['value'];
            } else {
                // New diagnosis (or was previously resolved and re-appeared)
                $this->diagnosisLifecycle[$code] = [
                    'first_seen_at' => $now,
                    'last_seen_at' => $now,
                    'status' => 'active',
                    'consecutive_ticks' => 1,
                    'level' => $d['level'],
                    'message' => $d['message'],
                    'recommendation' => $d['recommendation'],
                    'value' => $d['value'],
                ];
            }
        }

        // Handle disappearing diagnoses — collect removals separately to avoid
        // modifying array during iteration (PHP foreach with references)
        $toRemove = [];
        $this->newlyResolved = [];
        foreach ($this->diagnosisLifecycle as $code => &$entry) {
            if (isset($activeCodes[$code]) || $entry['status'] !== 'active') {
                continue;
            }
            // Diagnosis disappeared
            if ($entry['consecutive_ticks'] < self::MIN_TICKS_FOR_RESOLVE) {
                // Transient — remove silently (anti-flapping)
                $toRemove[] = $code;
            } else {
                // Genuine resolution
                $entry['status'] = 'resolved';
                $entry['resolved_at'] = $now;
                $this->newlyResolved[] = [
                    'code' => $code,
                    'level' => $entry['level'],
                    'message' => $entry['message'],
                    'recommendation' => $entry['recommendation'],
                    'value' => $entry['value'],
                ];
            }
        }
        unset($entry);

        foreach ($toRemove as $code) {
            unset($this->diagnosisLifecycle[$code]);
        }

        // Garbage collect old resolved diagnoses
        $nowTs = time();
        $toGc = [];
        foreach ($this->diagnosisLifecycle as $code => $entry) {
            if ($entry['status'] === 'resolved' && isset($entry['resolved_at'])) {
                $resolvedTs = strtotime($entry['resolved_at']);
                if ($resolvedTs !== false && ($nowTs - $resolvedTs) > self::RESOLVED_RETENTION_SECONDS) {
                    $toGc[] = $code;
                }
            }
        }
        foreach ($toGc as $code) {
            unset($this->diagnosisLifecycle[$code]);
        }

        // Build filtered diagnoses (only active with debounce threshold met)
        $filteredDiagnoses = [];
        foreach ($diagnoses as $d) {
            $code = $d['code'];
            if (isset($this->diagnosisLifecycle[$code])
                && $this->diagnosisLifecycle[$code]['consecutive_ticks'] >= self::DEBOUNCE_TICKS
            ) {
                $filteredDiagnoses[] = $d;
            }
        }

        $this->diagnoses = $filteredDiagnoses;

        // Health score calculation (uses filtered diagnoses)
        $score = 100;
        foreach ($filteredDiagnoses as $d) {
            match ($d['level']) {
                'critical' => $score -= 25,
                'warning' => $score -= 10,
                'info' => $score -= 2,
                default => null,
            };
        }
        $this->healthScore = max(0, $score);
        $this->status = match (true) {
            $this->healthScore >= 80 => 'healthy',
            $this->healthScore >= 40 => 'degraded',
            default => 'critical',
        };
    }

    /**
     * Build the complete status payload for the health API and remote reporting.
     */
    public function getFullStatus(
        float $startTime,
        bool $backPressure,
        int $pendingRows,
        int $walSize,
        int $drainWorkerPid,
    ): array {
        $rss = memory_get_usage(true);
        $lagAvg = $this->ringAvgFloat($this->lagRing);

        return [
            'version' => 1,
            'schema_version' => '1.0',
            'agent_version' => self::AGENT_VERSION,
            'status' => $this->status,
            'health_score' => $this->healthScore,
            'uptime_seconds' => (int) (microtime(true) - $startTime),
            'driver' => 'async',
            'ingest' => [
                'total' => $this->ingestTotal,
                'rejected' => $this->ingestRejected,
                'rate_1m' => round($this->ringAvg($this->ingestRing), 2),
                'reject_rate_1m' => round($this->ringAvg($this->rejectRing), 2),
                'reject_pct' => $this->computeRejectPct(),
            ],
            'drain' => [
                'total' => $this->drainTotal,
                'rate_1m' => round($this->ringAvg($this->drainRing), 2),
                'batches_failed' => $this->drainBatchesFailed,
                'pg_latency_ms' => round($this->drainPgLatencyMs, 2),
                'metrics_stale' => $this->drainMetricsUpdatedAt > 0
                    && (microtime(true) - $this->drainMetricsUpdatedAt) > self::DRAIN_METRICS_STALE_SECONDS,
            ],
            'buffer' => [
                'pending_rows' => $pendingRows,
                'max_pending_rows' => $this->maxPendingRows,
                'utilization_pct' => $this->maxPendingRows > 0
                    ? round(($pendingRows / $this->maxPendingRows) * 100, 2)
                    : 0,
                'wal_size_bytes' => $walSize,
                'back_pressure_active' => $backPressure,
            ],
            'process' => [
                'loop_lag_avg_ms' => round($lagAvg, 2),
                'loop_lag_max_ms' => round($this->lagMax, 2),
                'memory_rss_bytes' => $rss,
                'memory_limit_bytes' => $this->maxBufferMemory,
                'drain_worker_pid' => $drainWorkerPid,
            ],
            'system' => [
                'cpu_usage_pct' => $this->cpuUsagePct,
                'memory_used_pct' => $this->sysMemoryUsedPct,
                'memory_total_bytes' => $this->sysMemoryTotalBytes,
                'memory_available_bytes' => $this->sysMemoryAvailableBytes,
                'load_avg' => $this->loadAvg,
            ],
            'diagnoses' => $this->getEnrichedDiagnoses(),
            'resolved_diagnoses' => $this->getResolvedDiagnoses(),
            'reported_at' => gmdate('Y-m-d\TH:i:s\Z'),
        ];
    }

    /**
     * Enrich active diagnoses with lifecycle data.
     */
    private function getEnrichedDiagnoses(): array
    {
        $enriched = [];
        foreach ($this->diagnoses as $d) {
            $code = $d['code'];
            if (isset($this->diagnosisLifecycle[$code])) {
                $lc = $this->diagnosisLifecycle[$code];
                $d['first_seen_at'] = $lc['first_seen_at'];
                $d['last_seen_at'] = $lc['last_seen_at'];
                $d['status'] = 'active';
            }
            $enriched[] = $d;
        }
        return $enriched;
    }

    /**
     * Get recently resolved diagnoses for inclusion in reports.
     */
    private function getResolvedDiagnoses(): array
    {
        $resolved = [];
        foreach ($this->diagnosisLifecycle as $code => $entry) {
            if ($entry['status'] !== 'resolved') {
                continue;
            }
            $resolved[] = [
                'code' => $code,
                'level' => $entry['level'],
                'message' => $entry['message'],
                'recommendation' => $entry['recommendation'],
                'value' => $entry['value'],
                'first_seen_at' => $entry['first_seen_at'],
                'last_seen_at' => $entry['last_seen_at'],
                'resolved_at' => $entry['resolved_at'] ?? $entry['last_seen_at'],
                'status' => 'resolved',
            ];
        }
        return $resolved;
    }

    /**
     * Get diagnoses that just crossed the debounce threshold on this tick.
     * Returns an empty array on most ticks — only non-empty when a new issue appears.
     *
     * @return array<int, array{code: string, level: string, message: string, recommendation: string, value: float|int}>
     */
    public function getNewlyActiveDiagnoses(): array
    {
        $newly = [];
        foreach ($this->diagnoses as $d) {
            $code = $d['code'];
            if (isset($this->diagnosisLifecycle[$code])
                && $this->diagnosisLifecycle[$code]['consecutive_ticks'] === self::DEBOUNCE_TICKS
            ) {
                $newly[] = $d;
            }
        }

        return $newly;
    }

    /**
     * Get diagnoses that resolved on this tick (genuine resolutions only, not transient).
     *
     * @return array<int, array{code: string, level: string, message: string, recommendation: string, value: float|int}>
     */
    public function getNewlyResolvedDiagnoses(): array
    {
        return $this->newlyResolved;
    }

    /**
     * Get current status string (used by HealthReporter for adaptive intervals).
     */
    public function getStatus(): string
    {
        return $this->status;
    }

    /**
     * Collect system-level metrics from /proc (Linux) or fallbacks.
     * Called every 10s during diagnosis tick. Sub-millisecond overhead.
     */
    private function collectSystemMetrics(): void
    {
        // CPU usage from /proc/stat (Linux only)
        if (is_readable('/proc/stat')) {
            $line = @file_get_contents('/proc/stat', false, null, 0, 256);
            if ($line !== false && preg_match('/^cpu\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)/m', $line, $m)) {
                $idle = (int) $m[4];
                $total = (int) $m[1] + (int) $m[2] + (int) $m[3] + $idle + (int) $m[5] + (int) $m[6] + (int) $m[7];

                if ($this->prevCpuTotal > 0) {
                    $totalDelta = $total - $this->prevCpuTotal;
                    $idleDelta = $idle - $this->prevCpuIdle;
                    $this->cpuUsagePct = $totalDelta > 0
                        ? round((1 - $idleDelta / $totalDelta) * 100, 1)
                        : 0.0;
                }

                $this->prevCpuTotal = $total;
                $this->prevCpuIdle = $idle;
            }
        }

        // System memory from /proc/meminfo (Linux only)
        if (is_readable('/proc/meminfo')) {
            $meminfo = @file_get_contents('/proc/meminfo', false, null, 0, 512);
            if ($meminfo !== false) {
                $memTotal = 0;
                $memAvailable = 0;

                if (preg_match('/MemTotal:\s+(\d+)\s+kB/', $meminfo, $m)) {
                    $memTotal = (int) $m[1] * 1024; // bytes
                }
                if (preg_match('/MemAvailable:\s+(\d+)\s+kB/', $meminfo, $m)) {
                    $memAvailable = (int) $m[1] * 1024;
                }

                $this->sysMemoryTotalBytes = $memTotal;
                $this->sysMemoryAvailableBytes = $memAvailable;
                $this->sysMemoryUsedPct = $memTotal > 0
                    ? round((1 - $memAvailable / $memTotal) * 100, 1)
                    : 0.0;
            }
        }

        // Load average (always available on Linux/macOS)
        $load = sys_getloadavg();
        if ($load !== false) {
            $this->loadAvg = $load;
        }
    }

    private function computeRejectPct(): float
    {
        $ingest = $this->ringAvg($this->ingestRing);
        $reject = $this->ringAvg($this->rejectRing);
        $total = $ingest + $reject;
        return $total > 0 ? round(($reject / $total) * 100, 2) : 0.0;
    }

    private function formatBytes(int $bytes): string
    {
        if ($bytes < 1024 * 1024) {
            return round($bytes / 1024, 1) . ' KB';
        }
        if ($bytes < 1024 * 1024 * 1024) {
            return round($bytes / 1024 / 1024, 1) . ' MB';
        }
        return round($bytes / 1024 / 1024 / 1024, 1) . ' GB';
    }

    private function ringAvg(array $ring): float
    {
        $sum = array_sum($ring);
        return $sum / self::RING_SIZE;
    }

    private function ringAvgFloat(array $ring): float
    {
        $sum = 0.0;
        foreach ($ring as $v) {
            $sum += $v;
        }
        return $sum / self::RING_SIZE;
    }
}
