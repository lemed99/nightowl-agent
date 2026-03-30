<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\MetricsCollector;
use PHPUnit\Framework\TestCase;

class MetricsCollectorTest extends TestCase
{
    private MetricsCollector $collector;

    protected function setUp(): void
    {
        $this->collector = new MetricsCollector(
            maxPendingRows: 100_000,
            maxBufferMemory: 256 * 1024 * 1024,
        );
    }

    // --- Ring buffer & tick tests ---

    public function testInitialStatusIsHealthy(): void
    {
        $this->assertSame('healthy', $this->collector->getStatus());
    }

    public function testRecordIngestAndTick(): void
    {
        // Record some ingests then tick to push to ring buffer
        $this->collector->recordIngest();
        $this->collector->recordIngest();
        $this->collector->recordIngest();
        $this->collector->tick();

        // After tick, ingest ring should have 3 in one slot
        // Run diagnosis to update status
        $this->collector->runDiagnosis(false, 0, 0, 0);

        // With 3 ingests and no issues, should still be healthy
        $this->assertSame('healthy', $this->collector->getStatus());
    }

    public function testRecordReject(): void
    {
        $this->collector->recordReject();
        $this->collector->tick();

        // No crash, state is valid
        $this->collector->runDiagnosis(false, 0, 0, 0);
        $this->assertSame('healthy', $this->collector->getStatus());
    }

    // --- Diagnosis rule tests ---

    public function testDrainStoppedDiagnosis(): void
    {
        // Simulate: no drain activity, pending rows > 100
        // Need multiple ticks to build up ring data
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }

        // Run diagnosis twice for debounce (DEBOUNCE_TICKS = 2)
        $this->collector->runDiagnosis(false, 500, 0, 0);
        $this->collector->runDiagnosis(false, 500, 0, 0);

        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 500, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertContains('DRAIN_STOPPED', $codes);
    }

    public function testBackPressureActiveDiagnosis(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->collector->tick();
        }

        // Back pressure active triggers critical diagnosis
        $this->collector->runDiagnosis(true, 50000, 0, 0);
        $this->collector->runDiagnosis(true, 50000, 0, 0);

        $status = $this->collector->getFullStatus(microtime(true) - 60, true, 50000, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertContains('BACK_PRESSURE_ACTIVE', $codes);
    }

    public function testBacklogCriticalDiagnosis(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->collector->tick();
        }

        // 80%+ of max pending = critical
        $this->collector->runDiagnosis(false, 85000, 0, 0);
        $this->collector->runDiagnosis(false, 85000, 0, 0);

        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 85000, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertContains('BACKLOG_CRITICAL', $codes);
    }

    public function testBacklogHighDiagnosis(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->collector->tick();
        }

        // 50-80% of max = warning
        $this->collector->runDiagnosis(false, 60000, 0, 0);
        $this->collector->runDiagnosis(false, 60000, 0, 0);

        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 60000, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertContains('BACKLOG_HIGH', $codes);
    }

    public function testWalLargeDiagnosis(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->collector->tick();
        }

        // WAL > 100MB triggers warning
        $walSize = 150 * 1024 * 1024;
        $this->collector->runDiagnosis(false, 0, $walSize, 0);
        $this->collector->runDiagnosis(false, 0, $walSize, 0);

        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, $walSize, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertContains('WAL_LARGE', $codes);
    }

    public function testMemoryHighDiagnosis(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->collector->tick();
        }

        // RSS > 70% of max buffer memory
        $rss = (int) (256 * 1024 * 1024 * 0.8); // 80%
        $this->collector->runDiagnosis(false, 0, 0, $rss);
        $this->collector->runDiagnosis(false, 0, 0, $rss);

        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertContains('MEMORY_HIGH', $codes);
    }

    // --- Health score tests ---

    public function testHealthyScoreWithNoDiagnoses(): void
    {
        $this->collector->runDiagnosis(false, 0, 0, 0);

        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);

        $this->assertSame(100, $status['health_score']);
        $this->assertSame('healthy', $status['status']);
    }

    public function testCriticalScoreWithMultipleDiagnoses(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->collector->tick();
        }

        // Trigger multiple critical diagnoses
        $rss = (int) (256 * 1024 * 1024 * 0.8);
        $this->collector->runDiagnosis(true, 90000, 150 * 1024 * 1024, $rss);
        $this->collector->runDiagnosis(true, 90000, 150 * 1024 * 1024, $rss);

        $status = $this->collector->getFullStatus(microtime(true) - 60, true, 90000, 150 * 1024 * 1024, 0);

        // Multiple criticals should bring score well below 40
        $this->assertLessThan(40, $status['health_score']);
        $this->assertSame('critical', $status['status']);
    }

    public function testDegradedStatusRange(): void
    {
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }

        // Trigger one warning (WAL_LARGE) and one critical (DRAIN_STOPPED with pending)
        // Score = 100 - 25 (critical) - 10 (warning) = 65 → degraded
        $this->collector->runDiagnosis(false, 500, 150 * 1024 * 1024, 0);
        $this->collector->runDiagnosis(false, 500, 150 * 1024 * 1024, 0);

        $this->assertSame('degraded', $this->collector->getStatus());
    }

    // --- Debounce tests ---

    public function testDiagnosisRequiresDebounce(): void
    {
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }

        // First tick — diagnosis exists but not yet debounced
        $this->collector->runDiagnosis(false, 500, 0, 0);

        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 500, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        // After 1 tick, DRAIN_STOPPED should NOT appear (needs 2 ticks)
        $this->assertNotContains('DRAIN_STOPPED', $codes);
    }

    public function testTransientDiagnosisRemovedSilently(): void
    {
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }

        // Trigger DRAIN_STOPPED for 1 tick only
        $this->collector->runDiagnosis(false, 500, 0, 0);

        // Then condition clears
        $this->collector->runDiagnosis(false, 0, 0, 0);

        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);

        // No diagnoses and no resolved diagnoses (transient < MIN_TICKS_FOR_RESOLVE)
        $this->assertEmpty($status['diagnoses']);
        $this->assertEmpty($status['resolved_diagnoses']);
    }

    public function testGenuineResolution(): void
    {
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }

        // Trigger for 4 ticks (> MIN_TICKS_FOR_RESOLVE = 3)
        for ($i = 0; $i < 4; $i++) {
            $this->collector->runDiagnosis(false, 500, 0, 0);
        }

        // Now condition clears
        $this->collector->runDiagnosis(false, 0, 0, 0);

        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);

        // Should appear in resolved_diagnoses
        $resolvedCodes = array_column($status['resolved_diagnoses'], 'code');
        $this->assertContains('DRAIN_STOPPED', $resolvedCodes);
    }

    // --- Full status payload structure ---

    public function testGetFullStatusStructure(): void
    {
        $status = $this->collector->getFullStatus(
            startTime: microtime(true) - 120,
            backPressure: false,
            pendingRows: 42,
            walSize: 1024,
            drainWorkerPid: 12345,
        );

        // Top-level keys
        $this->assertArrayHasKey('version', $status);
        $this->assertArrayHasKey('status', $status);
        $this->assertArrayHasKey('health_score', $status);
        $this->assertArrayHasKey('uptime_seconds', $status);
        $this->assertArrayHasKey('ingest', $status);
        $this->assertArrayHasKey('drain', $status);
        $this->assertArrayHasKey('buffer', $status);
        $this->assertArrayHasKey('process', $status);
        $this->assertArrayHasKey('system', $status);
        $this->assertArrayHasKey('diagnoses', $status);
        $this->assertArrayHasKey('resolved_diagnoses', $status);
        $this->assertArrayHasKey('reported_at', $status);

        // Nested structure
        $this->assertArrayHasKey('total', $status['ingest']);
        $this->assertArrayHasKey('rate_1m', $status['ingest']);
        $this->assertArrayHasKey('pending_rows', $status['buffer']);
        $this->assertSame(42, $status['buffer']['pending_rows']);
        $this->assertSame(12345, $status['process']['drain_worker_pid']);
        $this->assertGreaterThan(100, $status['uptime_seconds']);
    }

    public function testAgentVersion(): void
    {
        $status = $this->collector->getFullStatus(microtime(true), false, 0, 0, 0);
        $this->assertSame('1.0.0', $status['agent_version']);
    }

    // --- Multi-worker metrics aggregation ---

    public function testReadDrainMetricsAggregatesMultipleWorkers(): void
    {
        $tmpDir = sys_get_temp_dir();
        $basePath = $tmpDir . '/nightowl_metrics_test_' . uniqid();

        // Write metrics for 3 workers
        $workers = [
            ['batches_drained' => 10, 'batches_failed' => 1, 'rows_drained' => 5000, 'pg_latency_ms' => 100.0, 'updated_at' => microtime(true)],
            ['batches_drained' => 8, 'batches_failed' => 0, 'rows_drained' => 4000, 'pg_latency_ms' => 200.0, 'updated_at' => microtime(true)],
            ['batches_drained' => 12, 'batches_failed' => 2, 'rows_drained' => 6000, 'pg_latency_ms' => 150.0, 'updated_at' => microtime(true)],
        ];

        foreach ($workers as $i => $data) {
            file_put_contents("{$basePath}.drain-metrics-{$i}.json", json_encode($data));
        }

        try {
            // First read establishes baseline
            $this->collector->readDrainMetrics($basePath, 3);
            // Tick to advance ring buffer
            $this->collector->tick();

            // Second read with increased totals
            $workers[0]['rows_drained'] = 6000;
            $workers[1]['rows_drained'] = 5000;
            $workers[2]['rows_drained'] = 7000;
            foreach ($workers as $i => $data) {
                file_put_contents("{$basePath}.drain-metrics-{$i}.json", json_encode($data));
            }
            $this->collector->readDrainMetrics($basePath, 3);

            $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);

            // batches_failed should be summed: 1 + 0 + 2 = 3
            $this->assertSame(3, $status['drain']['batches_failed']);

            // pg_latency_ms should be averaged: (100 + 200 + 150) / 3 = 150
            $this->assertEqualsWithDelta(150.0, $status['drain']['pg_latency_ms'], 1.0);

            // drain total should reflect sum across all workers
            $this->assertSame(18000, $status['drain']['total']); // 6000 + 5000 + 7000
        } finally {
            for ($i = 0; $i < 3; $i++) {
                @unlink("{$basePath}.drain-metrics-{$i}.json");
            }
        }
    }

    public function testReadDrainMetricsSingleWorkerFallback(): void
    {
        $tmpDir = sys_get_temp_dir();
        $basePath = $tmpDir . '/nightowl_metrics_single_' . uniqid();

        file_put_contents("{$basePath}.drain-metrics.json", json_encode([
            'batches_drained' => 5,
            'batches_failed' => 0,
            'rows_drained' => 2500,
            'pg_latency_ms' => 50.0,
            'updated_at' => microtime(true),
        ]));

        try {
            $this->collector->readDrainMetrics($basePath, 1);

            $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);

            $this->assertSame(2500, $status['drain']['total']);
            $this->assertSame(0, $status['drain']['batches_failed']);
            $this->assertEqualsWithDelta(50.0, $status['drain']['pg_latency_ms'], 1.0);
        } finally {
            @unlink("{$basePath}.drain-metrics.json");
        }
    }

    public function testReadDrainMetricsHandlesMissingWorkerFiles(): void
    {
        $tmpDir = sys_get_temp_dir();
        $basePath = $tmpDir . '/nightowl_metrics_partial_' . uniqid();

        // Only worker 0 has metrics, workers 1 and 2 have no files
        file_put_contents("{$basePath}.drain-metrics-0.json", json_encode([
            'batches_drained' => 5,
            'batches_failed' => 0,
            'rows_drained' => 2500,
            'pg_latency_ms' => 80.0,
            'updated_at' => microtime(true),
        ]));

        try {
            $this->collector->readDrainMetrics($basePath, 3);

            $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);

            // Should still report worker 0's metrics
            $this->assertSame(2500, $status['drain']['total']);
        } finally {
            @unlink("{$basePath}.drain-metrics-0.json");
        }
    }

    // --- Ring buffer remainder distribution ---

    public function testDrainRingDistributesRemainderAccurately(): void
    {
        $tmpDir = sys_get_temp_dir();
        $basePath = $tmpDir . '/nightowl_ring_test_' . uniqid();

        // Write initial metrics
        file_put_contents("{$basePath}.drain-metrics.json", json_encode([
            'batches_drained' => 0, 'batches_failed' => 0,
            'rows_drained' => 0, 'pg_latency_ms' => 0,
            'updated_at' => microtime(true),
        ]));

        try {
            // Establish baseline
            $this->collector->readDrainMetrics($basePath, 1);

            // Advance ring buffer to have clean slots
            for ($i = 0; $i < 10; $i++) {
                $this->collector->tick();
            }

            // Now report 13 rows drained (13 / 5 = 2 remainder 3)
            file_put_contents("{$basePath}.drain-metrics.json", json_encode([
                'batches_drained' => 1, 'batches_failed' => 0,
                'rows_drained' => 13, 'pg_latency_ms' => 10,
                'updated_at' => microtime(true),
            ]));

            $this->collector->readDrainMetrics($basePath, 1);

            // The drain rate should reflect all 13 rows distributed across 5 slots
            // (2 per slot + 1 extra for first 3 slots = 2+3+2+3+3 = 13 total)
            $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);
            $this->assertSame(13, $status['drain']['total']);
        } finally {
            @unlink("{$basePath}.drain-metrics.json");
        }
    }

    // --- Drain metrics staleness with multi-worker ---

    public function testDrainMetricsStalenessUsesOldestWorker(): void
    {
        $tmpDir = sys_get_temp_dir();
        $basePath = $tmpDir . '/nightowl_stale_test_' . uniqid();

        $now = microtime(true);

        // Worker 0 reported recently, worker 1 is stale
        file_put_contents("{$basePath}.drain-metrics-0.json", json_encode([
            'batches_drained' => 5, 'batches_failed' => 0,
            'rows_drained' => 1000, 'pg_latency_ms' => 50,
            'updated_at' => $now, // fresh
        ]));
        file_put_contents("{$basePath}.drain-metrics-1.json", json_encode([
            'batches_drained' => 3, 'batches_failed' => 0,
            'rows_drained' => 500, 'pg_latency_ms' => 60,
            'updated_at' => $now - 30, // 30s ago = stale
        ]));

        try {
            $this->collector->readDrainMetrics($basePath, 2);

            for ($i = 0; $i < 5; $i++) {
                $this->collector->tick();
            }

            // Run diagnosis — staleness should be based on oldest worker
            $this->collector->runDiagnosis(false, 0, 0, 0);
            $this->collector->runDiagnosis(false, 0, 0, 0);

            $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);

            // metrics_stale should be true because worker 1 is >15s old
            $this->assertTrue($status['drain']['metrics_stale']);
        } finally {
            @unlink("{$basePath}.drain-metrics-0.json");
            @unlink("{$basePath}.drain-metrics-1.json");
        }
    }
}
