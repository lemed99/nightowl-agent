<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\RecordWriter;
use NightOwl\Agent\SqliteBuffer;
use NightOwl\Simulator\NightwatchSimulator;
use PDO;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

/**
 * Two drain workers on one buffer, through a Postgres outage and after it.
 *
 * With more than one worker the drain claims rows (claimBatch) instead of
 * reading them (fetchPending). NIGHTOWL_DRAIN_WORKERS used to be dropped before
 * it reached the agent, so no customer had ever run this path. Drives the real
 * DrainWorker::drainBatch over a real buffer, as DrainWorkerTransientFailureTest
 * does. The outage test needs no Postgres; the recovery test does.
 */
class DrainWorkerMultiWorkerTest extends TestCase
{
    private const BATCH = 5;

    private static ?PDO $pdo = null;

    private static string $host;

    private static int $port;

    private static string $database;

    private static string $username;

    private static string $password;

    private string $bufferPath;

    private SqliteBuffer $buffer;

    private NightwatchSimulator $sim;

    private string|false $previousErrorLog = false;

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
        $this->bufferPath = sys_get_temp_dir().'/nightowl_multiworker_'.uniqid().'.sqlite';
        $this->buffer = new SqliteBuffer($this->bufferPath);
        $this->sim = new NightwatchSimulator('test-token');
        // The drain logs every failed connect.
        $this->previousErrorLog = ini_set('error_log', '/dev/null');
    }

    protected function tearDown(): void
    {
        ini_set('error_log', (string) $this->previousErrorLog);
        unset($this->buffer);
        foreach ([$this->bufferPath, $this->bufferPath.'-wal', $this->bufferPath.'-shm', $this->bufferPath.'.ckpt', $this->bufferPath.'.ckpt-gate'] as $f) {
            @unlink($f);
        }
    }

    public static function tearDownAfterClass(): void
    {
        self::$pdo = null;
    }

    public function test_through_an_outage_each_worker_holds_one_batch(): void
    {
        $this->appendRequests(30);
        $down = $this->unreachableWriter();
        $workers = [$this->worker(0), $this->worker(1)];

        for ($attempt = 0; $attempt < 10; $attempt++) {
            foreach ($workers as $worker) {
                $this->assertFalse($this->drainOnce($worker, $down));
            }
        }

        // Before, every failed attempt claimed another batch: after ten each,
        // the two workers held the whole buffer and back-pressure (which read
        // unclaimed rows only) saw an empty one.
        $this->assertSame(30 - 2 * self::BATCH, $this->buffer->pendingCount(), 'each worker must hold one batch, not one per attempt');
        $this->assertSame(30, $this->buffer->backlogCount(), 'nothing reached Postgres, so all of it is still owed');
    }

    public function test_after_an_outage_two_workers_drain_every_row_exactly_once(): void
    {
        if (self::$pdo === null) {
            $this->markTestSkipped('PostgreSQL not available. Set NIGHTOWL_TEST_DB_* env vars.');
        }
        self::$pdo->exec('TRUNCATE nightowl_requests');

        $traces = $this->appendRequests(23);
        $workers = [$this->worker(0), $this->worker(1)];

        $down = $this->unreachableWriter();
        for ($attempt = 0; $attempt < 3; $attempt++) {
            foreach ($workers as $worker) {
                $this->drainOnce($worker, $down);
            }
        }

        $up = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password, storageV2Config: false);
        for ($round = 0; $round < 20 && $this->buffer->backlogCount() > 0; $round++) {
            foreach ($workers as $worker) {
                $this->drainOnce($worker, $up);
            }
        }

        $this->assertSame(0, $this->buffer->backlogCount(), 'every row must drain once Postgres is back');

        $written = self::$pdo->query('SELECT trace_id FROM nightowl_requests ORDER BY trace_id')->fetchAll(PDO::FETCH_COLUMN);
        sort($traces);
        $this->assertSame($traces, $written, 'each request exactly once: none lost, none written by both workers');
    }

    /** @return list<string> the trace ids appended, one request per payload */
    private function appendRequests(int $count): array
    {
        $traces = [];
        for ($i = 0; $i < $count; $i++) {
            $trace = sprintf('44444444-aaaa-4aaa-8aaa-%012d', $i);
            $this->buffer->appendRaw(json_encode([$this->sim->makeRequest(['trace_id' => $trace])]));
            $traces[] = $trace;
        }

        return $traces;
    }

    private function worker(int $workerId): DrainWorker
    {
        return new DrainWorker(
            sqlitePath: $this->bufferPath,
            pgHost: self::$host,
            pgPort: self::$port,
            pgDatabase: self::$database,
            pgUsername: self::$username,
            pgPassword: self::$password,
            batchSize: self::BATCH,
            workerId: $workerId,
            totalWorkers: 2,
        );
    }

    private function unreachableWriter(): RecordWriter
    {
        return new RecordWriter('127.0.0.1', 1, 'x', 'x', 'x', storageV2Config: false);
    }

    private function drainOnce(DrainWorker $worker, RecordWriter $writer): bool
    {
        return (bool) (new ReflectionMethod($worker, 'drainBatch'))->invoke($worker, $this->buffer, $writer);
    }
}
