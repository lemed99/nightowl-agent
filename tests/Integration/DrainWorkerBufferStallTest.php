<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\RecordWriter;
use NightOwl\Agent\SqliteBuffer;
use NightOwl\Simulator\NightwatchSimulator;
use PDO;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use ReflectionProperty;

/**
 * Regression for the buffer-unwritable duplication storm — requires live PostgreSQL.
 *
 *   NIGHTOWL_TEST_DB_PORT=5433 vendor/bin/phpunit tests/Integration/DrainWorkerBufferStallTest.php
 *
 * Reproduces the production incident where a customer's app-server disk filled up:
 * fetchPending() (a SELECT) still works, write() commits to Postgres, but the
 * follow-up markSynced() (a SQLite UPDATE) fails on the full disk. Pre-fix, the
 * still-unmarked rows were re-fetched and re-COPYed on every drain loop, minting a
 * fresh duplicate copy each tick (11 GB in an hour, request counts 10-85x reality).
 * Post-fix, the committed rows land exactly once and are held for a mark-only retry.
 *
 * The full disk is simulated with `PRAGMA query_only = ON` on the buffer's own
 * connection: reads keep working, writes throw — the exact split a full disk
 * produces (fetchPending succeeds, markSynced fails).
 */
class DrainWorkerBufferStallTest extends TestCase
{
    private static ?PDO $pdo = null;

    private static string $host;

    private static int $port;

    private static string $database;

    private static string $username;

    private static string $password;

    private string $bufferPath;

    private SqliteBuffer $buffer;

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
        } catch (\Exception $e) {
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

        self::$pdo->exec('TRUNCATE nightowl_requests');
        self::$pdo->exec('TRUNCATE nightowl_request_rollups');

        $this->bufferPath = sys_get_temp_dir().'/nightowl_stall_'.uniqid().'.sqlite';
        $this->buffer = new SqliteBuffer($this->bufferPath);
        $this->sim = new NightwatchSimulator('test-token');
    }

    protected function tearDown(): void
    {
        if (! isset($this->bufferPath)) {
            return;
        }
        unset($this->buffer);
        foreach ([$this->bufferPath, $this->bufferPath.'-wal', $this->bufferPath.'-shm', $this->bufferPath.'.ckpt', $this->bufferPath.'.ckpt-gate'] as $f) {
            if (file_exists($f)) {
                @unlink($f);
            }
        }
    }

    public static function tearDownAfterClass(): void
    {
        self::$pdo = null;
    }

    private function appendRequest(string $traceId): void
    {
        $record = $this->sim->makeRequest(['trace_id' => $traceId]);
        $this->buffer->appendRaw(json_encode([$record]));
    }

    private function worker(): DrainWorker
    {
        return new DrainWorker(
            sqlitePath: $this->bufferPath,
            pgHost: self::$host,
            pgPort: self::$port,
            pgDatabase: self::$database,
            pgUsername: self::$username,
            pgPassword: self::$password,
            batchSize: 5000,
        );
    }

    private function drainOnce(DrainWorker $worker, RecordWriter $writer): bool
    {
        $m = new ReflectionMethod($worker, 'drainBatch');

        return (bool) $m->invoke($worker, $this->buffer, $writer);
    }

    /**
     * Flip the buffer's own connection between writable and read-only, standing in
     * for a full local disk: SELECTs keep working, the markSynced() UPDATE throws.
     */
    private function setBufferWritable(bool $writable): void
    {
        $prop = new ReflectionProperty(SqliteBuffer::class, 'pdo');
        /** @var PDO $pdo */
        $pdo = $prop->getValue($this->buffer);
        $pdo->exec('PRAGMA query_only = '.($writable ? 'OFF' : 'ON'));
    }

    private function requestCount(): int
    {
        return (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_requests')->fetchColumn();
    }

    private function rollupCallCount(): int
    {
        return (int) self::$pdo
            ->query('SELECT COALESCE(SUM(call_count), 0) FROM nightowl_request_rollups')
            ->fetchColumn();
    }

    public function test_unwritable_buffer_never_duplicates_committed_rows(): void
    {
        $this->appendRequest('r1');
        $this->appendRequest('r2');
        $this->appendRequest('r3');

        $writer = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password, storageV2Config: false);
        $worker = $this->worker();

        // Disk fills: writes to the buffer now fail, reads still work.
        $this->setBufferWritable(false);

        // Drain repeatedly while the buffer can't record progress. Pre-fix each loop
        // re-fetched the still-unmarked rows and re-COPYed them → 3, 6, 9, ... rows.
        for ($i = 0; $i < 10; $i++) {
            $this->drainOnce($worker, $writer);
        }

        $this->assertSame(
            3,
            $this->requestCount(),
            'committed to Postgres exactly once despite 10 drain loops on an unwritable buffer'
        );
        $this->assertSame(3, $this->rollupCallCount(), 'additive rollups not inflated by re-COPY');
        $this->assertSame(3, $this->buffer->pendingCount(), 'rows stay pending until the buffer can mark them');

        // The stall is visible: BUFFER_UNWRITABLE reads buffer_error_at > buffer_ok_at.
        $errAt = new ReflectionProperty($worker, 'bufferErrorAt');
        $okAt = new ReflectionProperty($worker, 'bufferOkAt');
        $this->assertGreaterThan($okAt->getValue($worker), $errAt->getValue($worker), 'the failed mark is recorded as a buffer error');
        $this->assertSame(8, (new ReflectionProperty($worker, 'bufferErrorCode'))->getValue($worker), 'SQLITE_READONLY');

        // Disk is freed: the held ids get marked, and still nothing re-writes.
        $this->setBufferWritable(true);
        $this->assertTrue($this->drainOnce($worker, $writer), 'recovery tick flushes the held marks');
        $this->assertGreaterThan($errAt->getValue($worker), $okAt->getValue($worker), 'the accepted mark clears it');

        $this->assertSame(3, $this->requestCount(), 'still exactly once after recovery');
        $this->assertSame(3, $this->rollupCallCount());
        $this->assertSame(0, $this->buffer->pendingCount(), 'all rows marked synced after recovery');

        // Steady state: no work left, no further writes.
        $this->assertFalse($this->drainOnce($worker, $writer));
        $this->assertSame(3, $this->requestCount());
    }

    /**
     * Found by review: during poison isolation, a payload's force-insert COMMITTED
     * and then its mark failed — and because the mark sat inside the same try, the
     * failure fell into the quarantine branch. The row already in Postgres was
     * dead-lettered and counted into the never-decaying DRAIN_QUARANTINE total
     * ("telemetry dropped"), and the batch's buffer-failure flag stayed set for
     * whatever failed next. A committed row is held for a mark-only retry instead.
     *
     * The trigger refuses the mark (synced = 1) but would allow the quarantine
     * (synced = -1) — a SQLITE_BUSY on the mark followed by a quarantine that gets
     * the lock, which is what made the old path reachable.
     */
    public function test_isolation_holds_a_committed_row_whose_mark_fails_instead_of_quarantining_it(): void
    {
        $this->appendRequest('iso-1');
        $row = $this->buffer->fetchPending(1)[0];
        $id = (int) $row['id'];

        /** @var PDO $sqlite */
        $sqlite = (new ReflectionProperty(SqliteBuffer::class, 'pdo'))->getValue($this->buffer);
        $sqlite->exec("CREATE TRIGGER refuse_mark BEFORE UPDATE OF synced ON buffer WHEN NEW.synced = 1 BEGIN SELECT RAISE(ABORT, 'mark refused'); END");

        $worker = new DrainWorker(
            sqlitePath: $this->bufferPath,
            pgHost: self::$host,
            pgPort: self::$port,
            pgDatabase: self::$database,
            pgUsername: self::$username,
            pgPassword: self::$password,
            quarantineEnabled: true,
        );
        $writer = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password, storageV2Config: false);

        try {
            (new ReflectionMethod($worker, 'isolateSinglePayload'))
                ->invoke($worker, $this->buffer, $writer, ['id' => $id, 'records' => json_decode($row['payload'], true)]);
            $this->fail('a failed mark must propagate to drainBatch, not be swallowed');
        } catch (\PDOException) {
            // Expected.
        }

        $prop = fn (string $name) => (new ReflectionProperty($worker, $name))->getValue($worker);
        $this->assertSame(1, $this->requestCount(), 'committed exactly once');
        $this->assertSame(0, $prop('quarantinedTotal'), 'a committed row is not poison');
        $this->assertSame([$id], $prop('committedUnmarkedIds'), 'held for a mark-only retry');
        $this->assertSame(0, (int) $sqlite->query("SELECT synced FROM buffer WHERE id = {$id}")->fetchColumn(), 'not dead-lettered');
    }
}
