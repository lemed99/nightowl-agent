<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\MetricsCollector;
use PHPUnit\Framework\TestCase;

/**
 * A rollup table that stops being written must reach the health payload.
 *
 * Yomoney, 2026-07-31: prune's v1-EOL dropped `nightowl_requests` under a
 * running daemon, so the concurrency recompute named a relation that no longer
 * existed and 42P01'd. Every tick after that aborted the whole statement, the
 * table froze at the minute the drop landed, and the ONLY trace was one
 * `error_log` line per minute in a log nobody tails. The customer found out
 * when four 14-day chart requests 504'd — the API's coverage gate saw the stale
 * bound, abandoned the rollup, swept raw, and hit a statement timeout.
 *
 * The detection rule itself is pinned in RollupStalenessTest; this file pins
 * the wiring, which is where a signal like this actually dies: the verdict is
 * computed in a drain child and has to survive the IPC file, the cross-worker
 * merge, and the debounce to become a diagnosis anyone sees.
 */
final class RollupStaleHealthSignalTest extends TestCase
{
    private MetricsCollector $collector;

    protected function setUp(): void
    {
        $this->collector = new MetricsCollector(
            maxPendingRows: 100_000,
            maxBufferMemory: 256 * 1024 * 1024,
        );
    }

    public function test_a_frozen_rollup_becomes_a_critical_naming_the_table(): void
    {
        $this->loadDrainMetrics([
            'rollup_stale' => ['nightowl_request_concurrency_rollups' => 11 * 3600],
        ]);

        $d = $this->diagnose('ROLLUP_STALE');

        $this->assertNotFalse($d, 'a frozen rollup produced no diagnosis — the failure stays log-only');
        $this->assertSame('critical', $d['level']);
        $this->assertStringContainsString('nightowl_request_concurrency_rollups', $d['message']);
        $this->assertStringContainsString('11 hours behind', $d['message']);
        $this->assertSame(11 * 3600, $d['value']);
        // The operator's first move is the agent log — that is where the
        // repeating maintenance error names the actual cause.
        $this->assertStringContainsString('NightOwl Drain', $d['recommendation']);
    }

    public function test_healthy_rollups_produce_nothing(): void
    {
        $this->loadDrainMetrics(['rollup_stale' => []]);

        $this->assertFalse($this->diagnose('ROLLUP_STALE'));
    }

    public function test_an_agent_with_no_stats_pass_yet_produces_nothing(): void
    {
        // The key is absent entirely for the first hour of a worker's life, and
        // on every agent running with table stats disabled.
        $this->loadDrainMetrics([]);

        $this->assertFalse($this->diagnose('ROLLUP_STALE'));
    }

    public function test_the_worst_lag_across_workers_wins(): void
    {
        // Workers sample on independent hourly clocks. Worker 1's verdict
        // predates the freeze; taking anything but the max would let a stale
        // all-clear mask a freeze another worker has already measured.
        $this->loadDrainMetrics([
            0 => ['rollup_stale' => ['nightowl_request_rollups' => 7200]],
            1 => ['rollup_stale' => []],
        ], workers: 2);

        $d = $this->diagnose('ROLLUP_STALE');

        $this->assertNotFalse($d);
        $this->assertSame(7200, $d['value']);
    }

    /**
     * Cross-repo contract with nightowl-api: `diagnoses.*.code` is validated
     * max:32 and message/recommendation max:1024, and ONE over-long field 422s
     * the entire report — every other diagnosis lost with it. The message
     * interpolates table names, so the worst case is every rollup at once.
     */
    public function test_it_fits_the_platform_wire_limits_at_its_worst(): void
    {
        $stale = [];
        foreach (\NightOwl\Support\RollupSpecs::all() as $spec) {
            foreach (['', '_hourly', '_daily'] as $tier) {
                $stale[str_replace('_rollups', $tier.'_rollups', $spec->table)] = 999_999;
            }
        }
        $this->assertGreaterThan(20, count($stale), 'Worst case means every rollup table and tier at once.');

        $this->loadDrainMetrics(['rollup_stale' => $stale]);
        $d = $this->diagnose('ROLLUP_STALE');

        $this->assertNotFalse($d);
        $this->assertLessThanOrEqual(32, mb_strlen($d['code'], 'UTF-8'));
        $this->assertLessThanOrEqual(1024, mb_strlen($d['message'], 'UTF-8'), 'message overruns the 1KB cap');
        $this->assertLessThanOrEqual(1024, mb_strlen($d['recommendation'], 'UTF-8'), 'recommendation overruns the 1KB cap');

        // The count stays exact even though the list is truncated — the number
        // is what tells an operator the enumeration was cut short.
        $this->assertStringStartsWith(count($stale).' rollup table(s)', $d['message']);
        $this->assertStringContainsString('and '.(count($stale) - 6).' more', $d['message']);
    }

    /**
     * The seam the rest of this file cannot see: the verdict is computed in a
     * drain child, and the parent only ever reads what writeDrainMetrics() puts
     * in the IPC file. Drop the key there and every test above stays green
     * while the health payload says nothing.
     */
    public function test_the_worker_actually_ships_the_verdict_to_the_parent(): void
    {
        $base = tempnam(sys_get_temp_dir(), 'nightowl-stale-seam');
        $worker = new DrainWorker(
            sqlitePath: $base,
            pgHost: '127.0.0.1',
            pgPort: 5432,
            pgDatabase: 'test',
            pgUsername: 'test',
            pgPassword: 'test',
        );

        (new \ReflectionProperty($worker, 'rollupStale'))
            ->setValue($worker, ['nightowl_query_daily_rollups' => 4 * 86400]);
        (new \ReflectionMethod($worker, 'writeDrainMetrics'))->invoke($worker);

        $file = $base.'.drain-metrics.json';
        $this->assertFileExists($file);

        try {
            $this->collector->readDrainMetrics($base, 1);
            $d = $this->diagnose('ROLLUP_STALE');
        } finally {
            @unlink($file);
            @unlink($base);
        }

        $this->assertNotFalse($d, 'the verdict must survive the trip through the IPC file');
        $this->assertStringContainsString('nightowl_query_daily_rollups (4 days behind)', $d['message']);
    }

    // --- helpers ---

    /**
     * @return array<string, mixed>|false
     */
    private function diagnose(string $code): array|false
    {
        // DEBOUNCE_TICKS is 2 — nothing is reported until it has held for two
        // consecutive ticks.
        $this->collector->runDiagnosis(false, 0, 0, 0);
        $this->collector->runDiagnosis(false, 0, 0, 0);
        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);

        foreach ($status['diagnoses'] ?? [] as $d) {
            if (($d['code'] ?? null) === $code) {
                return $d;
            }
        }

        return false;
    }

    /**
     * @param  array<string, mixed>  $fields  one worker's fields, or workerId => fields when $workers > 1
     */
    private function loadDrainMetrics(array $fields, int $workers = 1): void
    {
        $base = tempnam(sys_get_temp_dir(), 'nightowl-stale-health');
        $written = [];

        for ($w = 0; $w < $workers; $w++) {
            $file = $workers > 1 ? $base.".drain-metrics-{$w}.json" : $base.'.drain-metrics.json';
            $own = $workers > 1 ? ($fields[$w] ?? []) : $fields;
            file_put_contents($file, json_encode(array_merge([
                'rows_drained' => 0,
                'batches_failed' => 0,
                'pg_latency_ms' => 0,
                'updated_at' => microtime(true),
                'last_write_sqlstate' => null,
                'last_write_table' => null,
                'last_write_at' => 0.0,
                'last_write_ok_at' => microtime(true),
                'last_conn_fail_at' => 0.0,
            ], $own)));
            $written[] = $file;
        }

        $this->collector->readDrainMetrics($base, $workers);

        foreach ($written as $file) {
            @unlink($file);
        }

        @unlink($base);
    }
}
