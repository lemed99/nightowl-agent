<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\RecordWriter;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * Worker saturation alerting: checkWorkerSaturation() reads per-minute
 * occupancy back from nightowl_request_rollups and fires a performance issue
 * when it holds at/above percent% of the environment's http_workers count for
 * sustained_minutes consecutive COMPLETED minutes.
 *
 * The tests seed the rollup table and settings directly — the arithmetic is
 * the subject here, and each scenario needs exact per-minute occupancy that
 * simulated traffic can't pin down. The end-to-end simulator pass lives in the
 * verification checklist, not here.
 *
 * Requires PostgreSQL (same env vars as RecordWriterTest).
 */
class WorkerSaturationTest extends TestCase
{
    private static ?PDO $pdo = null;

    private static string $host;

    private static int $port;

    private static string $database;

    private static string $username;

    private static string $password;

    private RecordWriter $writer;

    public static function setUpBeforeClass(): void
    {
        self::$host = getenv('NIGHTOWL_TEST_DB_HOST') ?: '127.0.0.1';
        self::$port = (int) (getenv('NIGHTOWL_TEST_DB_PORT') ?: 5432);
        self::$database = getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test';
        self::$username = getenv('NIGHTOWL_TEST_DB_USERNAME') ?: 'nightowl_test';
        self::$password = getenv('NIGHTOWL_TEST_DB_PASSWORD') ?: 'test123';

        try {
            $dsn = sprintf('pgsql:host=%s;port=%d;dbname=%s', self::$host, self::$port, self::$database);
            self::$pdo = new PDO($dsn, self::$username, self::$password);
            self::$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (\Exception) {
            self::$pdo = null;
        }

        if (self::$pdo) {
            MigrationRunner::migrate(self::$host, self::$port, self::$database, self::$username, self::$password);
        }
    }

    protected function setUp(): void
    {
        if (self::$pdo === null) {
            $this->markTestSkipped('PostgreSQL not available. Set NIGHTOWL_TEST_DB_* env vars.');
        }

        $this->writer = new RecordWriter(
            self::$host, self::$port, self::$database, self::$username, self::$password,
            environment: 'production',
        );

        self::$pdo->exec('TRUNCATE nightowl_request_rollups');
        self::$pdo->exec('TRUNCATE nightowl_issues CASCADE');
        self::$pdo->exec('TRUNCATE nightowl_issue_activity');
        self::$pdo->exec("DELETE FROM nightowl_settings WHERE key IN ('http_workers', 'worker_saturation')");
    }

    public static function tearDownAfterClass(): void
    {
        self::$pdo = null;
    }

    // ─── Fixture helpers ───────────────────────────────────

    private function configure(int $workers, int $percent, int $sustainedMinutes, bool $enabled = true): void
    {
        $stmt = self::$pdo->prepare(
            "INSERT INTO nightowl_settings (key, value, updated_at) VALUES (?, ?, now())
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()"
        );
        $stmt->execute(['http_workers', json_encode(['production' => $workers])]);
        $stmt->execute(['worker_saturation', json_encode([
            'enabled' => $enabled,
            'percent' => $percent,
            'sustained_minutes' => $sustainedMinutes,
        ])]);
    }

    /**
     * Seed one rollup row: $occupancySeconds of total request duration landing
     * in the completed minute $minutesAgo back from the current minute.
     */
    private function seedMinute(int $minutesAgo, float $occupancySeconds, string $environment = 'production'): void
    {
        $bucket = gmdate('Y-m-d H:i:00', (intdiv(time(), 60) - $minutesAgo) * 60);
        $stmt = self::$pdo->prepare(
            'INSERT INTO nightowl_request_rollups
                (group_hash, bucket_start, environment, call_count, success_count,
                 client_error_count, server_error_count, total_duration, min_duration, max_duration)
             VALUES (?, ?, ?, 1, 1, 0, 0, ?, 1000, 1000)
             ON CONFLICT (group_hash, bucket_start, environment) DO UPDATE
                SET total_duration = nightowl_request_rollups.total_duration + EXCLUDED.total_duration'
        );
        $stmt->execute([md5('GET /load-'.$minutesAgo), $bucket, $environment, (int) ($occupancySeconds * 1_000_000)]);
    }

    private function issueRow(): array|false
    {
        return self::$pdo->query(
            "SELECT * FROM nightowl_issues WHERE group_hash = 'worker_saturation'"
        )->fetch(PDO::FETCH_ASSOC);
    }

    // ─── Detection arithmetic ──────────────────────────────

    public function test_sustained_saturation_creates_issue(): void
    {
        // 2 workers, 90% => trigger at 108 occupancy-seconds/min. Seed 120 (2.0 busy).
        $this->configure(workers: 2, percent: 90, sustainedMinutes: 3);
        foreach ([1, 2, 3] as $m) {
            $this->seedMinute($m, 120.0);
        }

        $this->writer->checkWorkerSaturation();

        $issue = $this->issueRow();
        $this->assertNotFalse($issue, 'Sustained saturation must create an issue');
        $this->assertSame('performance', $issue['type']);
        $this->assertSame('worker_saturation', $issue['subtype']);
        $this->assertSame('production', $issue['environment']);
        $this->assertSame('open', $issue['status']);
        $this->assertSame('Worker saturation', $issue['exception_class']);
        $this->assertStringContainsString('90% of 2 workers', $issue['exception_message']);
        $this->assertStringContainsString('3 consecutive minutes', $issue['exception_message']);
        // The ms-suffixed scalars must stay NULL — alert formatters would render
        // a worker count as "2ms".
        $this->assertNull($issue['threshold_ms']);
        $this->assertNull($issue['triggered_duration_ms']);
    }

    public function test_streak_below_sustained_minutes_does_not_fire(): void
    {
        $this->configure(workers: 2, percent: 90, sustainedMinutes: 3);
        // Only 2 of the required 3 minutes breach.
        $this->seedMinute(1, 120.0);
        $this->seedMinute(2, 120.0);
        $this->seedMinute(3, 10.0);

        $this->writer->checkWorkerSaturation();

        $this->assertFalse($this->issueRow(), 'Two breaching minutes of three must not fire');
    }

    public function test_gap_minute_breaks_the_streak(): void
    {
        $this->configure(workers: 2, percent: 90, sustainedMinutes: 3);
        // Minutes 1 and 3 breach; minute 2 has NO rollup row at all (no traffic).
        $this->seedMinute(1, 120.0);
        $this->seedMinute(3, 120.0);

        $this->writer->checkWorkerSaturation();

        $this->assertFalse($this->issueRow(), 'A missing minute is zero occupancy and must break the streak');
    }

    public function test_percent_boundary_is_inclusive(): void
    {
        // 2 workers at 90% = exactly 108 occupancy-seconds — fires (>=).
        $this->configure(workers: 2, percent: 90, sustainedMinutes: 1);
        $this->seedMinute(1, 108.0);

        $this->writer->checkWorkerSaturation();

        $this->assertNotFalse($this->issueRow(), 'Occupancy exactly at the threshold must fire');
    }

    public function test_just_under_the_boundary_does_not_fire(): void
    {
        $this->configure(workers: 2, percent: 90, sustainedMinutes: 1);
        $this->seedMinute(1, 107.9);

        $this->writer->checkWorkerSaturation();

        $this->assertFalse($this->issueRow(), 'Occupancy just under the threshold must not fire');
    }

    public function test_other_environment_traffic_is_ignored(): void
    {
        $this->configure(workers: 2, percent: 90, sustainedMinutes: 1);
        // Saturating load, but in staging — the agent's env is production.
        $this->seedMinute(1, 300.0, environment: 'staging');

        $this->writer->checkWorkerSaturation();

        $this->assertFalse($this->issueRow(), 'Occupancy in another environment must not fire for this agent');
    }

    // ─── Config gates ──────────────────────────────────────

    public function test_disabled_config_is_a_noop(): void
    {
        $this->configure(workers: 2, percent: 90, sustainedMinutes: 1, enabled: false);
        $this->seedMinute(1, 300.0);

        $this->writer->checkWorkerSaturation();

        $this->assertFalse($this->issueRow());
    }

    public function test_missing_worker_count_is_a_noop(): void
    {
        // Saturation config present and enabled, but no http_workers entry for
        // this environment — opt-in by construction.
        $stmt = self::$pdo->prepare(
            'INSERT INTO nightowl_settings (key, value, updated_at) VALUES (?, ?, now())'
        );
        $stmt->execute(['worker_saturation', json_encode(['enabled' => true, 'percent' => 90, 'sustained_minutes' => 1])]);
        $stmt->execute(['http_workers', json_encode(['staging' => 4])]);
        $this->seedMinute(1, 300.0);

        $this->writer->checkWorkerSaturation();

        $this->assertFalse($this->issueRow());
    }

    public function test_out_of_bounds_config_is_treated_as_disabled(): void
    {
        $this->configure(workers: 2, percent: 500, sustainedMinutes: 1);
        $this->seedMinute(1, 300.0);

        $this->writer->checkWorkerSaturation();

        $this->assertFalse($this->issueRow(), 'percent outside 10-200 must disable rather than misfire');
    }

    // ─── Real drain path ───────────────────────────────────

    /**
     * Closes the loop the seeded tests skip: request records flow through the
     * REAL drain write path (writer->write() → rollup upsert), and the check
     * reads occupancy back from what the drain itself wrote. Catches drift
     * between the rollup writer's bucketing and the check's read-back.
     */
    public function test_fires_from_rollups_written_by_the_real_drain_path(): void
    {
        $this->configure(workers: 1, percent: 50, sustainedMinutes: 1);

        $sim = new \NightOwl\Simulator\NightwatchSimulator('test-token');
        $previousMinute = (intdiv(time(), 60) - 1) * 60;

        // 40 occupancy-seconds inside the previous (completed) minute — above
        // the 30s trigger (1 worker at 50%).
        $records = [];
        foreach (range(0, 7) as $i) {
            $records[] = $sim->makeRequest([
                'timestamp' => $previousMinute + 1 + $i,
                'duration' => 5_000_000,
                'environment' => 'production',
            ]);
        }
        $this->writer->write($records);

        $this->writer->checkWorkerSaturation();

        $issue = $this->issueRow();
        $this->assertNotFalse($issue, 'Drain-written rollups must feed the saturation check');
        $this->assertSame('worker_saturation', $issue['subtype']);
    }

    // ─── Lifecycle ─────────────────────────────────────────

    public function test_continued_saturation_increments_occurrences_without_new_issue(): void
    {
        $this->configure(workers: 2, percent: 90, sustainedMinutes: 1);
        $this->seedMinute(1, 120.0);
        $this->writer->checkWorkerSaturation();

        // Next minute, still saturated. Fresh writer = fresh in-process minute
        // stamp, so the once-per-minute gate doesn't block the second pass.
        $writer2 = new RecordWriter(
            self::$host, self::$port, self::$database, self::$username, self::$password,
            environment: 'production',
        );
        $this->seedMinute(0, 120.0); // becomes irrelevant; minute 1 still breaches
        $writer2->checkWorkerSaturation();

        $rows = self::$pdo->query(
            "SELECT COUNT(*) FROM nightowl_issues WHERE group_hash = 'worker_saturation'"
        )->fetchColumn();
        $this->assertSame(1, (int) $rows, 'Continued saturation must dedup onto one issue');

        $issue = $this->issueRow();
        $this->assertSame('open', $issue['status']);
        $this->assertGreaterThanOrEqual(2, (int) $issue['occurrences_count'], 'Each saturated evaluation increments occurrences');
    }

    public function test_resolved_issue_reopens_on_recurrence(): void
    {
        $this->configure(workers: 2, percent: 90, sustainedMinutes: 1);
        $this->seedMinute(1, 120.0);
        $this->writer->checkWorkerSaturation();

        self::$pdo->exec("UPDATE nightowl_issues SET status = 'resolved' WHERE group_hash = 'worker_saturation'");

        $writer2 = new RecordWriter(
            self::$host, self::$port, self::$database, self::$username, self::$password,
            environment: 'production',
        );
        $writer2->checkWorkerSaturation();

        $issue = $this->issueRow();
        $this->assertSame('open', $issue['status'], 'Recurrence must auto-reopen a resolved saturation issue');
    }

    public function test_ignored_issue_is_never_reopened(): void
    {
        $this->configure(workers: 2, percent: 90, sustainedMinutes: 1);
        $this->seedMinute(1, 120.0);
        $this->writer->checkWorkerSaturation();

        self::$pdo->exec("UPDATE nightowl_issues SET status = 'ignored' WHERE group_hash = 'worker_saturation'");

        $writer2 = new RecordWriter(
            self::$host, self::$port, self::$database, self::$username, self::$password,
            environment: 'production',
        );
        $writer2->checkWorkerSaturation();

        $issue = $this->issueRow();
        $this->assertSame('ignored', $issue['status'], "'ignored' means silenced — must never auto-reopen");
    }
}
