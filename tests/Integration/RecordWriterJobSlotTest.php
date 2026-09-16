<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\RecordWriter;
use NightOwl\Simulator\NightwatchSimulator;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * github#9 — `queued-job` and `job-attempt` share one write slot.
 *
 * Merging them is what stops the job writer from running at two positions in
 * the batch, which is what let a dispatching host and a running host invert
 * nightowl_job_rollups against each other. This test covers the other half of
 * that claim: that merging changes NOTHING about what gets written.
 *
 * It compares one merged batch against the same records written as two batches,
 * which is how the drain used to process them.
 */
class RecordWriterJobSlotTest extends TestCase
{
    private static ?PDO $pdo = null;

    private static string $host;

    private static int $port;

    private static string $database;

    private static string $username;

    private static string $password;

    private NightwatchSimulator $sim;

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
        $this->sim = new NightwatchSimulator('test-token');
        $this->truncate();
    }

    public static function tearDownAfterClass(): void
    {
        self::$pdo = null;
    }

    private function truncate(): void
    {
        foreach (['nightowl_jobs', 'nightowl_job_rollups', 'nightowl_user_job_rollups'] as $t) {
            self::$pdo->exec("TRUNCATE {$t}");
        }
    }

    private function writer(): RecordWriter
    {
        return new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password, storageV2Config: false);
    }

    /**
     * BOTH rollup tables the job writer touches, ordered, so a difference anywhere
     * shows up. Selecting only nightowl_job_rollups left the per-user rollup
     * truncated-but-unasserted, so "must not change a single rollup counter" was
     * half a claim (found by review).
     */
    private function rollups(): array
    {
        $jobs = self::$pdo->query(
            'SELECT group_hash, call_count, attempts_count, queued_count, processed_count,
                    released_count, failed_count, total_duration, min_duration, max_duration
             FROM nightowl_job_rollups ORDER BY group_hash, bucket_start'
        )->fetchAll(PDO::FETCH_ASSOC);

        $users = self::$pdo->query(
            'SELECT user_id, call_count FROM nightowl_user_job_rollups ORDER BY user_id, bucket_start'
        )->fetchAll(PDO::FETCH_ASSOC);

        return ['jobs' => $jobs, 'users' => $users];
    }

    /** @return list<array<string, mixed>> */
    private function lifecycle(): array
    {
        // Two jobs, each with a dispatch and an attempt — the shape a web host
        // and a queue host produce between them.
        $ts = (string) time();

        return [
            $this->sim->makeJob(['trace_id' => '88888888-8888-4888-8888-000000000001', 'job_id' => 'j-1', 'timestamp' => $ts]),
            $this->sim->makeJob(['trace_id' => '88888888-8888-4888-8888-000000000002', 'job_id' => 'j-2', 'timestamp' => $ts]),
            $this->sim->makeJobAttempt(['trace_id' => '88888888-8888-4888-8888-000000000001', 'job_id' => 'j-1', 'timestamp' => $ts]),
            $this->sim->makeJobAttempt(['trace_id' => '88888888-8888-4888-8888-000000000002', 'job_id' => 'j-2', 'timestamp' => $ts]),
        ];
    }

    public function test_one_merged_slot_writes_exactly_what_two_passes_wrote(): void
    {
        [$job1, $job2, $att1, $att2] = $this->lifecycle();

        // How the drain processes them now: one batch, one slot, one pass.
        $this->writer()->write([$job1, $att1, $job2, $att2]);
        $mergedJobs = (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_jobs')->fetchColumn();
        $mergedRollups = $this->rollups();

        $this->truncate();

        // How it used to: the dispatch rows and the attempt rows in separate
        // passes, each with its own rollup upsert accumulating additively.
        $this->writer()->write([$job1, $job2]);
        $this->writer()->write([$att1, $att2]);
        $splitJobs = (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_jobs')->fetchColumn();
        $splitRollups = $this->rollups();

        $this->assertSame(4, $mergedJobs, 'every dispatch and every attempt must be stored');
        $this->assertSame($splitJobs, $mergedJobs, 'merging must not drop or duplicate a row');
        $this->assertNotEmpty($mergedRollups['jobs'], 'the job rollup must actually be exercised');
        $this->assertNotEmpty($mergedRollups['users'], 'and so must the per-user job rollup');
        $this->assertEquals($splitRollups, $mergedRollups, 'merging must not change a single rollup counter');
    }

    public function test_arrival_order_within_the_slot_does_not_change_what_is_written(): void
    {
        // The two hosts' records interleave arbitrarily in the buffer. Whatever
        // order they arrive in, one batch must produce one result.
        [$job1, $job2, $att1, $att2] = $this->lifecycle();

        $this->writer()->write([$att2, $job1, $att1, $job2]);
        $first = $this->rollups();
        $firstJobs = (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_jobs')->fetchColumn();

        $this->truncate();

        $this->writer()->write([$job1, $job2, $att1, $att2]);

        $this->assertSame($firstJobs, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_jobs')->fetchColumn());
        $this->assertEquals($first, $this->rollups());
    }
}
