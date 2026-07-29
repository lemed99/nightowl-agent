<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\RecordWriter;
use NightOwl\Agent\SqliteBuffer;
use NightOwl\Simulator\NightwatchSimulator;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * The reconnect streak's bookkeeping, against real connections.
 *
 * DrainWorkerBackoffTest pins the arithmetic; this pins what drives it. The
 * distinction that matters here is REACHABLE vs REFUSING: an unreachable
 * Postgres should be retried slower and slower, but one that answers and then
 * rejects the write is a schema/permission problem the backoff must not touch —
 * sleeping 10s between attempts would delay the recovery drain for no reason,
 * and it would do it while Postgres was healthy the whole time.
 *
 * The rejecting-Postgres case runs against its own throwaway database rather
 * than the shared test one. Renaming a table out from under the suite to
 * provoke a 42P01 is exactly the shared-state trick that made PruneV1EolTest
 * order-dependent.
 */
class DrainWorkerBackoffBehaviourTest extends TestCase
{
    /** A port nothing listens on — every connect fails instantly. */
    private const DEAD_PORT = 1;

    private const EMPTY_DB = 'nightowl_backoff_empty_test';

    private static ?PDO $pdo = null;

    private static string $host;

    private static int $port;

    private static string $username;

    private static string $password;

    private static bool $emptyDbReady = false;

    private string $bufferPath;

    private ?string $priorErrorLog = null;

    public static function setUpBeforeClass(): void
    {
        self::$host = getenv('NIGHTOWL_TEST_DB_HOST') ?: '127.0.0.1';
        self::$port = (int) (getenv('NIGHTOWL_TEST_DB_PORT') ?: 5432);
        self::$username = getenv('NIGHTOWL_TEST_DB_USERNAME') ?: 'nightowl_test';
        self::$password = getenv('NIGHTOWL_TEST_DB_PASSWORD') ?: 'test123';
        $database = getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test';

        try {
            self::$pdo = new PDO(
                sprintf('pgsql:host=%s;port=%d;dbname=%s', self::$host, self::$port, $database),
                self::$username,
                self::$password,
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            );
        } catch (\Throwable) {
            self::$pdo = null;

            return;
        }

        // A schema-less database: connecting succeeds, every write raises 42P01.
        try {
            self::$pdo->exec('DROP DATABASE IF EXISTS '.self::EMPTY_DB);
            self::$pdo->exec('CREATE DATABASE '.self::EMPTY_DB);
            self::$emptyDbReady = true;
        } catch (\Throwable) {
            // No CREATEDB right — the rejection test skips rather than falling
            // back to mutating the shared database.
            self::$emptyDbReady = false;
        }
    }

    public static function tearDownAfterClass(): void
    {
        if (self::$pdo !== null && self::$emptyDbReady) {
            try {
                self::$pdo->exec('DROP DATABASE IF EXISTS '.self::EMPTY_DB);
            } catch (\Throwable) {
                // Best effort — a leftover empty database breaks nothing.
            }
        }

        self::$pdo = null;
        self::$emptyDbReady = false;
    }

    protected function setUp(): void
    {
        $this->bufferPath = sys_get_temp_dir().'/nightowl-backoff-'.getmypid().'-'.uniqid().'.sqlite';
    }

    protected function tearDown(): void
    {
        if ($this->priorErrorLog !== null) {
            ini_set('error_log', $this->priorErrorLog);
            $this->priorErrorLog = null;
        }

        foreach (['', '-wal', '-shm'] as $suffix) {
            $f = $this->bufferPath.$suffix;
            if (file_exists($f)) {
                @unlink($f);
            }
        }
    }

    // ─── Helpers ───────────────────────────────────────────────

    /**
     * Real simulator fixtures, not hand-rolled arrays. RecordWriter dispatches on
     * `t` and silently ignores a record without it, so an invented payload shape
     * drains as a successful no-op — which would have made the rejection test below
     * assert against a write that never happened.
     */
    private function seedBuffer(int $payloads = 3): void
    {
        $buffer = new SqliteBuffer($this->bufferPath);
        $sim = new NightwatchSimulator('backoff-test-token', '127.0.0.1', 2499, timeout: 1.0);

        for ($i = 0; $i < $payloads; $i++) {
            $buffer->appendRaw(json_encode([
                $sim->makeRequest([
                    'trace_id' => sprintf('00000000-0000-4000-8000-%012d', $i + 1),
                    'method' => 'GET',
                    'status_code' => 200,
                ]),
            ], JSON_THROW_ON_ERROR));
        }
    }

    private function worker(int $pgPort, string $database = 'nightowl_test'): DrainWorker
    {
        return new DrainWorker(
            sqlitePath: $this->bufferPath,
            pgHost: self::$host,
            pgPort: $pgPort,
            pgDatabase: $database,
            pgUsername: self::$username,
            pgPassword: self::$password,
            intervalMs: 100,
        );
    }

    private function writer(int $pgPort, string $database = 'nightowl_test'): RecordWriter
    {
        return new RecordWriter(
            host: self::$host,
            port: $pgPort,
            database: $database,
            username: self::$username,
            password: self::$password,
        );
    }

    /** Run one drain batch through the private path the run loop uses. */
    private function drainOnce(DrainWorker $worker, RecordWriter $writer): void
    {
        (new \ReflectionMethod($worker, 'drainBatch'))
            ->invoke($worker, new SqliteBuffer($this->bufferPath), $writer);
    }

    private function streak(DrainWorker $worker): int
    {
        return (new \ReflectionProperty($worker, 'connFailStreak'))->getValue($worker);
    }

    // ─── Tests ─────────────────────────────────────────────────

    /**
     * Needs no PostgreSQL: the point is that there isn't one.
     */
    public function test_each_unreachable_batch_increments_the_streak(): void
    {
        $this->seedBuffer();
        $worker = $this->worker(self::DEAD_PORT);
        $writer = $this->writer(self::DEAD_PORT);

        $this->assertSame(0, $this->streak($worker), 'a fresh worker starts unthrottled');

        for ($n = 1; $n <= 4; $n++) {
            $this->drainOnce($worker, $writer);
            $this->assertSame($n, $this->streak($worker), "after {$n} failed batches");
        }
    }

    /**
     * An empty buffer must not look like a healthy Postgres. drainBatch returns
     * early when there is nothing to claim — if that early return also cleared the
     * streak, an outage that outlasted the backlog would reset the backoff to zero
     * on every idle tick and silently restore the busy-loop this fix removed.
     */
    public function test_an_idle_tick_neither_raises_nor_clears_the_streak(): void
    {
        // No seedBuffer() — the buffer is created empty.
        new SqliteBuffer($this->bufferPath);

        $worker = $this->worker(self::DEAD_PORT);
        $writer = $this->writer(self::DEAD_PORT);
        (new \ReflectionProperty($worker, 'connFailStreak'))->setValue($worker, 5);

        $this->drainOnce($worker, $writer);

        $this->assertSame(5, $this->streak($worker), 'an idle tick must leave the streak alone');
    }

    /**
     * Recovery: the first batch that completes a round trip clears the backoff, so
     * the drain returns to its normal cadence immediately rather than serving out
     * whatever sleep the outage had escalated to.
     */
    public function test_a_successful_batch_clears_the_streak(): void
    {
        if (self::$pdo === null) {
            $this->markTestSkipped('PostgreSQL not available. Set NIGHTOWL_TEST_DB_* env vars.');
        }

        MigrationRunner::migrate(
            self::$host,
            self::$port,
            getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test',
            self::$username,
            self::$password,
        );

        $this->seedBuffer();
        $worker = $this->worker(self::$port);
        (new \ReflectionProperty($worker, 'connFailStreak'))->setValue($worker, 7);

        $this->drainOnce($worker, $this->writer(self::$port));

        $this->assertSame(0, $this->streak($worker), 'a completed round trip must clear the backoff');
    }

    /**
     * REACHABLE BUT REFUSING. Postgres is up and answering; the write fails on
     * 42P01 because the schema is not there. That is not a reachability problem,
     * so the reconnect backoff must stay at zero — an operator fixing a migration
     * should not also be waiting out a 10s sleep between retries.
     */
    public function test_a_write_rejection_does_not_engage_the_reconnect_backoff(): void
    {
        if (self::$pdo === null) {
            $this->markTestSkipped('PostgreSQL not available. Set NIGHTOWL_TEST_DB_* env vars.');
        }
        if (! self::$emptyDbReady) {
            $this->markTestSkipped('could not create a scratch database (CREATEDB right required).');
        }

        $this->seedBuffer();
        $worker = $this->worker(self::$port, self::EMPTY_DB);
        $writer = $this->writer(self::$port, self::EMPTY_DB);
        (new \ReflectionProperty($worker, 'connFailStreak'))->setValue($worker, 6);

        $this->drainOnce($worker, $writer);

        $this->assertNotEmpty($writer->lastWriteError, 'the write should have been rejected');
        $this->assertEmpty(
            $writer->lastWriteError['connection'] ?? null,
            'a missing table is not a connection failure — got: '.json_encode($writer->lastWriteError)
        );
        $this->assertSame(
            0,
            $this->streak($worker),
            'a reachable-but-refusing Postgres must not be backed off'
        );
    }

    /**
     * The flood itself. Twenty consecutive unreachable batches used to mean twenty
     * identical SQLSTATE[08006] lines; they should now collapse to one per doubling.
     *
     * Asserted through the real error_log destination rather than by counting
     * isBackoffEscalation() calls — the suppression lives in an `if` beside the
     * error_log() call in drainBatch, and a unit test of the predicate alone would
     * still pass if that `if` were deleted.
     */
    public function test_repeat_connection_failures_are_logged_once_per_doubling(): void
    {
        $logPath = sys_get_temp_dir().'/nightowl-backoff-log-'.getmypid().'-'.uniqid().'.log';
        $this->priorErrorLog = ini_get('error_log') ?: '';
        ini_set('error_log', $logPath);

        try {
            $this->seedBuffer();
            $worker = $this->worker(self::DEAD_PORT);
            $writer = $this->writer(self::DEAD_PORT);

            for ($i = 0; $i < 20; $i++) {
                $this->drainOnce($worker, $writer);
            }

            $log = file_exists($logPath) ? (string) file_get_contents($logPath) : '';
            $drainErrors = substr_count($log, '[NightOwl Drain] Error:');

            $this->assertSame(20, $this->streak($worker), 'all 20 batches should have failed to connect');

            // The guard reads the streak BEFORE the classification block increments
            // it, so batch N sees N-1. Over 20 batches that is 0..19, and a line is
            // emitted for the leading 0 (first failure is always reported) plus the
            // powers of two 1,2,4,8,16 — six lines, fourteen suppressed.
            $this->assertSame(
                6,
                $drainErrors,
                "expected the first failure plus one line per doubling over 20 retries, got {$drainErrors}:\n".$log
            );
        } finally {
            if (file_exists($logPath)) {
                @unlink($logPath);
            }
        }
    }
}
