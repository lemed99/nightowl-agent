<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\HealthAlertNotifier;
use NightOwl\Agent\MetricsCollector;
use PHPUnit\Framework\TestCase;

/**
 * The 2.0.0 field incident was not only a wedge — it was an INVISIBLE wedge.
 * Two rollup states that distort what the dashboard draws had no representation
 * anywhere except the local agent log:
 *
 *  - a boot backfill still owed (`storage/nightowl/backfill-pending`), during
 *    which any rollup table the upgrade created exists but is EMPTY, and the API
 *    read path prefers an existing rollup over raw — so wide-range charts read
 *    ZERO rather than falling back;
 *  - a rollup repair debt (`nightowl_settings.rollup_repair_from`), written when
 *    a contended rollup table forced a drain to land raw rows without their
 *    summaries. Nothing errors; the numbers are just short.
 *
 * Both now reach the health payload. Both are graded so the grading itself
 * carries information: the repair debt is actionable (warning), and the backfill
 * is informational while it is plausibly still running and only becomes a
 * warning once it is old enough that nobody is going to run it.
 */
final class RollupBackfillHealthSignalTest extends TestCase
{
    private MetricsCollector $collector;

    protected function setUp(): void
    {
        $this->collector = new MetricsCollector(
            maxPendingRows: 100_000,
            maxBufferMemory: 256 * 1024 * 1024,
        );
    }

    public function test_a_rollup_repair_debt_reaches_the_payload_with_the_command_that_clears_it(): void
    {
        $this->loadDrainMetrics([
            'rollup_repair_debt' => [
                'nightowl_query_rollups' => '2026-07-29 11:00:00',
            ],
        ]);

        $d = $this->diagnose('ROLLUP_REPAIR_PENDING');

        $this->assertNotFalse($d, 'A recorded repair debt must be visible to the operator, not only in the agent log.');
        $this->assertSame('warning', $d['level'], 'Actionable but lossless — no telemetry was dropped.');
        $this->assertStringContainsString('nightowl_query_rollups (from 2026-07-29 11:00:00 UTC)', $d['message']);
        $this->assertStringContainsString(
            '2026-07-29 11:00:00',
            $d['recommendation'],
            'The floor is the whole point: it is the --since a backfill has to start from.',
        );
        $this->assertStringContainsString('nightowl:migrate', $d['recommendation']);
        $this->assertSame(1, $d['value']);
    }

    /**
     * Every drain worker reads the SAME tenant marker, so the per-worker values
     * are a gauge to merge — not a counter to sum — and per table the EARLIEST
     * floor has to win, matching the SQL-side minimum the drain converges on.
     * Taking the later floor would backfill from the wrong place and leave the
     * front of the hole open.
     */
    public function test_the_earliest_floor_wins_when_workers_disagree(): void
    {
        // Deliberately crossed, so neither "first worker read wins" nor "last
        // worker read wins" can pass: query's earliest is in worker 0, request's
        // is in worker 1.
        $this->loadDrainMetrics([
            0 => ['rollup_repair_debt' => [
                'nightowl_query_rollups' => '2026-07-29 08:00:00',
                'nightowl_request_rollups' => '2026-07-29 15:00:00',
            ]],
            1 => ['rollup_repair_debt' => [
                'nightowl_query_rollups' => '2026-07-29 12:00:00',
                'nightowl_request_rollups' => '2026-07-29 09:00:00',
            ]],
        ], workers: 2);

        $d = $this->diagnose('ROLLUP_REPAIR_PENDING');

        $this->assertNotFalse($d);
        $this->assertSame(2, $d['value'], 'Two distinct tables owe a repair, not four entries.');
        $this->assertStringContainsString(
            'nightowl_query_rollups (from 2026-07-29 08:00:00 UTC)',
            $d['message'],
            'Backfilling from the LATER floor would leave the front of the hole open.',
        );
        $this->assertStringContainsString('nightowl_request_rollups (from 2026-07-29 09:00:00 UTC)', $d['message']);
        $this->assertStringContainsString(
            '2026-07-29 08:00:00',
            $d['recommendation'],
            'The single --since must be the earliest floor across every worker and table.',
        );
    }

    public function test_no_debt_says_nothing(): void
    {
        $this->loadDrainMetrics(['rollup_repair_debt' => []]);

        $this->assertFalse($this->diagnose('ROLLUP_REPAIR_PENDING'));
    }

    /**
     * A pre-2.0.0 worker (or one that has not reached its first cleanup tick)
     * ships no such key at all. That must read as "nothing owed", not warn, and
     * above all not raise a PHP error by iterating a missing value.
     */
    public function test_a_metrics_file_without_the_key_is_not_a_debt(): void
    {
        $this->loadDrainMetrics([]);

        $this->assertFalse($this->diagnose('ROLLUP_REPAIR_PENDING'));
    }

    public function test_a_backfill_that_just_started_is_informational_not_an_alarm(): void
    {
        $this->collector->setBackfillPendingSince(microtime(true) - 120);

        $d = $this->diagnose('ROLLUP_BACKFILL_PENDING');

        $this->assertNotFalse($d, 'An in-flight reconciliation is exactly the state the field incident hid.');
        $this->assertSame('info', $d['level']);
        $this->assertStringContainsString('Nothing to do', $d['recommendation']);
    }

    public function test_an_info_diagnosis_does_not_drag_the_agent_out_of_healthy(): void
    {
        $this->collector->setBackfillPendingSince(microtime(true) - 120);
        $status = $this->diagnoseTwice();

        // info costs 2 of the 20 points 'healthy' allows. A routine upgrade must
        // not report the customer's agent as degraded on the platform.
        $this->assertSame('healthy', $status['status']);
        $this->assertSame(98, $status['health_score']);
    }

    public function test_a_backfill_nobody_is_running_becomes_a_warning(): void
    {
        $this->collector->setBackfillPendingSince(microtime(true) - (7 * 3600));

        $d = $this->diagnose('ROLLUP_BACKFILL_PENDING');

        $this->assertNotFalse($d);
        $this->assertSame('warning', $d['level'], 'Seven hours in, this is neglect, not progress.');
        $this->assertStringContainsString('7 hours', $d['message']);
        $this->assertStringContainsString('ZERO', $d['recommendation'], 'Say what an empty rollup actually costs.');
        $this->assertStringContainsString('nightowl:migrate', $d['recommendation']);
    }

    public function test_no_marker_says_nothing(): void
    {
        $this->collector->setBackfillPendingSince(0.0);

        $this->assertFalse($this->diagnose('ROLLUP_BACKFILL_PENDING'));
    }

    /**
     * A completed backfill deletes its marker from ANOTHER process, so the signal
     * has to be able to go away — the whole reason the parent re-stats it every
     * tick and the drain re-reads the marker rather than remembering it.
     */
    public function test_both_signals_clear_once_the_state_does(): void
    {
        $this->collector->setBackfillPendingSince(microtime(true) - 120);
        $this->loadDrainMetrics(['rollup_repair_debt' => ['nightowl_query_rollups' => '2026-07-29 11:00:00']]);
        $this->assertNotFalse($this->diagnose('ROLLUP_BACKFILL_PENDING'));
        $this->assertNotFalse($this->diagnose('ROLLUP_REPAIR_PENDING'));

        $this->collector->setBackfillPendingSince(0.0);
        $this->loadDrainMetrics(['rollup_repair_debt' => []]);

        // MIN_TICKS_FOR_RESOLVE (3) before a diagnosis is considered genuinely
        // resolved rather than flapping.
        $status = $this->diagnoseTwice();
        $status = $this->diagnoseTwice();

        $this->assertFalse($this->activeDiagnosis($status, 'ROLLUP_BACKFILL_PENDING'));
        $this->assertFalse($this->activeDiagnosis($status, 'ROLLUP_REPAIR_PENDING'));
    }

    /**
     * Cross-repo contract with nightowl-api: POST /agent/health validates
     * `diagnoses.*.code` as max:32 and message/recommendation as max:1024, and one
     * over-long field 422s the WHOLE report — every diagnosis lost to say one of
     * them too verbosely. The repair message interpolates every owing table name,
     * so its worst case is all 14 rollup tables at once.
     */
    public function test_both_diagnoses_fit_the_platform_wire_limits_at_their_worst(): void
    {
        $debt = [];
        foreach (\NightOwl\Support\RollupSpecs::all() as $spec) {
            $debt[$spec->table] = '2026-07-29 11:00:00';
        }
        $this->assertGreaterThan(10, count($debt), 'Worst case means every rollup table owing at once.');

        $this->collector->setBackfillPendingSince(microtime(true) - (7 * 3600));
        $this->loadDrainMetrics(['rollup_repair_debt' => $debt]);
        $status = $this->diagnoseTwice();

        foreach (['ROLLUP_REPAIR_PENDING', 'ROLLUP_BACKFILL_PENDING'] as $code) {
            $d = $this->activeDiagnosis($status, $code);
            $this->assertNotFalse($d, $code.' must be present to be measured.');
            $this->assertLessThanOrEqual(32, mb_strlen($d['code'], 'UTF-8'), $code.' code overruns varchar(32)');
            $this->assertLessThanOrEqual(1024, mb_strlen($d['message'], 'UTF-8'), $code.' message overruns the 1KB cap');
            $this->assertLessThanOrEqual(1024, mb_strlen($d['recommendation'], 'UTF-8'), $code.' recommendation overruns the 1KB cap');
            $this->assertIsNumeric($d['value']);
        }

        // The count is exact even though the enumeration is capped — the number is
        // what tells an operator the list was truncated.
        $repair = $this->activeDiagnosis($status, 'ROLLUP_REPAIR_PENDING');
        $this->assertSame(count($debt), $repair['value']);
        $this->assertStringContainsString('and '.(count($debt) - 6).' more', $repair['message']);
    }

    /**
     * The counterpart to grading a diagnosis `info`: it must not page anyone. An
     * upgrade sending "agent health degraded" to a customer's Slack for an
     * expected, self-clearing state would train them to ignore the channel that
     * carries DRAIN_QUARANTINE.
     */
    public function test_info_diagnoses_are_not_alertable_and_neither_are_their_recoveries(): void
    {
        $diagnoses = [
            ['code' => 'ROLLUP_BACKFILL_PENDING', 'level' => 'info'],
            ['code' => 'ROLLUP_REPAIR_PENDING', 'level' => 'warning'],
            ['code' => 'DRAIN_QUARANTINE', 'level' => 'critical'],
        ];

        $codes = array_column(HealthAlertNotifier::alertable($diagnoses), 'code');

        $this->assertSame(['ROLLUP_REPAIR_PENDING', 'DRAIN_QUARANTINE'], $codes);
        $this->assertSame([], HealthAlertNotifier::alertable([$diagnoses[0]]), 'An all-info batch dispatches nothing at all.');
    }

    /**
     * The seam between the two halves above: the drain worker holds the debt it
     * read off the tenant marker, and the parent process only ever sees what
     * writeDrainMetrics() puts in the IPC file. Dropping the key there would leave
     * every test in this class green and the health payload silent — so assert the
     * real writer feeds the real reader.
     */
    public function test_the_worker_actually_ships_the_debt_to_the_parent(): void
    {
        $base = tempnam(sys_get_temp_dir(), 'nightowl-ipc-seam');
        $worker = new DrainWorker(
            sqlitePath: $base,
            pgHost: '127.0.0.1',
            pgPort: 5432,
            pgDatabase: 'test',
            pgUsername: 'test',
            pgPassword: 'test',
        );

        (new \ReflectionProperty($worker, 'rollupRepairDebt'))
            ->setValue($worker, ['nightowl_job_rollups' => '2026-07-29 04:00:00']);
        (new \ReflectionMethod($worker, 'writeDrainMetrics'))->invoke($worker);

        $file = $base.'.drain-metrics.json';
        $this->assertFileExists($file);

        try {
            $this->collector->readDrainMetrics($base, 1);
            $d = $this->diagnose('ROLLUP_REPAIR_PENDING');
        } finally {
            @unlink($file);
            @unlink($base);
        }

        $this->assertNotFalse($d, 'The debt must survive the trip through the IPC file.');
        $this->assertStringContainsString('nightowl_job_rollups (from 2026-07-29 04:00:00 UTC)', $d['message']);
    }

    // --- helpers ---

    /**
     * Run the debounce out and return the graded diagnosis, or false if absent.
     *
     * @return array<string, mixed>|false
     */
    private function diagnose(string $code): array|false
    {
        return $this->activeDiagnosis($this->diagnoseTwice(), $code);
    }

    /**
     * DEBOUNCE_TICKS is 2 — a diagnosis is not reported until it has been true on
     * two consecutive ticks (anti-flapping).
     *
     * @return array<string, mixed>
     */
    private function diagnoseTwice(): array
    {
        $this->collector->runDiagnosis(false, 0, 0, 0);
        $this->collector->runDiagnosis(false, 0, 0, 0);

        return $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);
    }

    /**
     * Write per-worker drain-metrics IPC files and read them, exactly as the
     * parent's back-pressure tick does.
     *
     * @param  array<string, mixed>  $fields  one worker's fields, or workerId => fields when $workers > 1
     */
    private function loadDrainMetrics(array $fields, int $workers = 1): void
    {
        $base = tempnam(sys_get_temp_dir(), 'nightowl-rollup-health');
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
                // Non-zero so the absence of a drain never manufactures an
                // unrelated critical that changes the health score under us.
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

    /**
     * @param  array<string, mixed>  $status
     * @return array<string, mixed>|false
     */
    private function activeDiagnosis(array $status, string $code): array|false
    {
        return current(array_filter($status['diagnoses'], fn ($d) => $d['code'] === $code));
    }
}
