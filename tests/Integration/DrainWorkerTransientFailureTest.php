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
 * github#9 — a transient abort is not a failed batch. Requires live PostgreSQL.
 *
 *   NIGHTOWL_TEST_DB_PORT=5432 vendor/bin/phpunit tests/Integration/DrainWorkerTransientFailureTest.php
 *
 * Drives the REAL DrainWorker::drainBatch over a real buffer and RecordWriter.
 * A BEFORE INSERT trigger on nightowl_request_rollups raises SQLSTATE 40P01, so
 * the deadlock reaches the drain the way Postgres delivers a real one — same
 * SQLSTATE, same abort of the whole batch transaction, same CONTEXT naming a
 * rollup relation — without needing two racing sessions to produce it.
 *
 * Quarantine is OFF, which is the default (drain_quarantine_enabled) and so the
 * path every reported deadlock actually took. drainUnits' transient handling is
 * unreachable there; drainBatch's catch is what has to get this right.
 */
class DrainWorkerTransientFailureTest extends TestCase
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

        self::$pdo->exec('TRUNCATE nightowl_requests');
        self::$pdo->exec('TRUNCATE nightowl_request_rollups');
        $this->dropDeadlockTrigger();

        $this->bufferPath = sys_get_temp_dir().'/nightowl_transient_'.uniqid().'.sqlite';
        $this->buffer = new SqliteBuffer($this->bufferPath);
        $this->sim = new NightwatchSimulator('test-token');
    }

    protected function tearDown(): void
    {
        if (self::$pdo !== null) {
            $this->dropDeadlockTrigger();
        }
        if (! isset($this->bufferPath)) {
            return;
        }
        unset($this->buffer);
        foreach ([$this->bufferPath, $this->bufferPath.'-wal', $this->bufferPath.'-shm'] as $f) {
            if (file_exists($f)) {
                @unlink($f);
            }
        }
    }

    public static function tearDownAfterClass(): void
    {
        self::$pdo = null;
    }

    /** Raise a genuine 40P01 out of the rollup upsert, the relation the reports named. */
    private function installDeadlockTrigger(): void
    {
        self::$pdo->exec(<<<'SQL'
            CREATE OR REPLACE FUNCTION nightowl_test_raise_deadlock() RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION 'deadlock detected'
                    USING ERRCODE = '40P01',
                          DETAIL  = 'Process 414992 waits for ShareLock on transaction 845884; blocked by process 1243572.';
            END;
            $$ LANGUAGE plpgsql;
            CREATE TRIGGER nightowl_test_deadlock
                BEFORE INSERT ON nightowl_request_rollups
                FOR EACH ROW EXECUTE FUNCTION nightowl_test_raise_deadlock();
        SQL);
    }

    private function dropDeadlockTrigger(): void
    {
        self::$pdo->exec('DROP TRIGGER IF EXISTS nightowl_test_deadlock ON nightowl_request_rollups');
        self::$pdo->exec('DROP FUNCTION IF EXISTS nightowl_test_raise_deadlock()');
    }

    private function appendRequest(string $traceId): void
    {
        $this->buffer->appendRaw(json_encode([$this->sim->makeRequest(['trace_id' => $traceId])]));
    }

    private function worker(bool $quarantine = false): DrainWorker
    {
        return new DrainWorker(
            sqlitePath: $this->bufferPath,
            pgHost: self::$host,
            pgPort: self::$port,
            pgDatabase: self::$database,
            pgUsername: self::$username,
            pgPassword: self::$password,
            batchSize: 5000,
            quarantineEnabled: $quarantine,
        );
    }

    private function writer(): RecordWriter
    {
        return new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password, storageV2Config: false);
    }

    private function drainOnce(DrainWorker $worker, RecordWriter $writer): bool
    {
        return (bool) (new ReflectionMethod($worker, 'drainBatch'))->invoke($worker, $this->buffer, $writer);
    }

    private function prop(DrainWorker $worker, string $name): mixed
    {
        return (new ReflectionProperty($worker, $name))->getValue($worker);
    }

    private function requestCount(): int
    {
        return (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_requests')->fetchColumn();
    }

    public function test_a_deadlock_is_deferred_not_counted_as_a_failed_batch(): void
    {
        $this->appendRequest('22222222-aaaa-4aaa-8aaa-000000000001');
        $this->appendRequest('22222222-aaaa-4aaa-8aaa-000000000002');

        $worker = $this->worker();
        $writer = $this->writer();
        $this->installDeadlockTrigger();

        $this->drainOnce($worker, $writer);

        // The database really did abort the batch with a deadlock.
        $this->assertSame('40P01', $writer->lastWriteError['sqlstate'] ?? null);
        $this->assertSame(0, $this->requestCount(), 'the batch transaction must have rolled back');

        // Pre-fix this was 1, and stayed 1 for the life of the process.
        $this->assertSame(0, $this->prop($worker, 'batchesFailed'), 'a deadlock is not a failed batch');
        $this->assertSame(1, $this->prop($worker, 'transientFailures'), 'but it is counted as what it is');
        $this->assertSame(0, $this->prop($worker, 'batchesDrained'), 'a deferred batch committed nothing');

        // It must not claim Postgres is refusing our writes — that clock raises
        // DRAIN_WRITE_FAILING against a database that did nothing wrong.
        $this->assertSame(0.0, $this->prop($worker, 'lastWriteAt'));
        $this->assertNull($this->prop($worker, 'lastWriteSqlstate'));
        $this->assertNull($this->prop($worker, 'lastWriteTable'));

        // PG answered, so reachability is not in doubt.
        $this->assertSame(0, $this->prop($worker, 'connFailStreak'));
        $this->assertSame(0.0, $this->prop($worker, 'lastConnFailAt'));
    }

    public function test_the_deferred_rows_drain_on_the_next_loop(): void
    {
        // The payload is fine; only the timing was unlucky. Nothing may be lost or
        // quarantined, and the retry must land the rows.
        $this->appendRequest('33333333-aaaa-4aaa-8aaa-000000000001');
        $this->appendRequest('33333333-aaaa-4aaa-8aaa-000000000002');

        $worker = $this->worker();
        $writer = $this->writer();

        $this->installDeadlockTrigger();
        $this->drainOnce($worker, $writer);
        $this->assertSame(0, $this->requestCount());

        $this->dropDeadlockTrigger();
        $this->drainOnce($worker, $writer);

        $this->assertSame(2, $this->requestCount(), 'the deferred batch must drain once the conflict clears');
        $this->assertSame(0, $this->prop($worker, 'batchesFailed'));
        $this->assertSame(0, $this->prop($worker, 'quarantinedTotal'));
        $this->assertSame(1, $this->prop($worker, 'transientFailures'));
    }

    public function test_users_are_upserted_in_a_global_id_order_whatever_the_batch_order(): void
    {
        // nightowl_users is upserted one row per user_id. Unsorted, two agents
        // carrying the same signed-in users in opposite orders take those row
        // locks in opposite orders and deadlock — the 2.1.1 hazard in the one
        // upsert that never got the fix. Assert on what Postgres sees: the order
        // the rows were actually inserted in (ctid ascending on a fresh table).
        self::$pdo->exec('TRUNCATE nightowl_users');

        $ids = ['9', '10', 'zeta', 'alpha', '100'];
        $records = [];
        foreach (array_reverse($ids) as $id) {
            $records[] = ['t' => 'user', 'v' => 1, 'id' => $id, 'name' => "u{$id}", 'username' => "u{$id}@x.test", 'timestamp' => (string) time()];
        }
        $this->buffer->appendRaw(json_encode($records));

        $this->drainOnce($this->worker(), $this->writer());

        $got = self::$pdo->query('SELECT user_id FROM nightowl_users ORDER BY ctid')->fetchAll(PDO::FETCH_COLUMN);

        $expected = $ids;
        usort($expected, static fn ($a, $b) => strcmp($a, $b));
        $this->assertSame($expected, $got, 'users must be written in a byte order every agent agrees on');
        // Byte order, not PHP's: '10' and '100' sort before '9'.
        $this->assertSame(['10', '100', '9', 'alpha', 'zeta'], $got);
    }

    /**
     * Found by review: with quarantine ON the deadlock never reaches drainBatch's
     * catch, because drainUnits handles it and RETURNS a deferred result. So
     * transientFailures stayed 0 and batchesDrained++ still ran — a batch that
     * committed nothing counted as a drained one, inflating the denominator of the
     * very rate meant to catch it. Both paths must agree.
     */
    public function test_a_deferred_batch_is_never_counted_as_drained_on_either_path(): void
    {
        foreach ([false, true] as $quarantine) {
            self::$pdo->exec('TRUNCATE nightowl_requests');
            self::$pdo->exec('TRUNCATE nightowl_request_rollups');
            unset($this->buffer);
            @unlink($this->bufferPath);
            $this->buffer = new SqliteBuffer($this->bufferPath);

            $this->appendRequest('66666666-aaaa-4aaa-8aaa-00000000000'.($quarantine ? '1' : '2'));

            $worker = $this->worker($quarantine);
            $this->installDeadlockTrigger();
            $this->drainOnce($worker, $this->writer());
            $this->dropDeadlockTrigger();

            $label = $quarantine ? 'quarantine on' : 'quarantine off';
            $this->assertSame(0, $this->prop($worker, 'batchesDrained'), "{$label}: nothing committed, nothing drained");
            $this->assertSame(1, $this->prop($worker, 'transientFailures'), "{$label}: the contention must be counted");
            $this->assertSame(0, $this->prop($worker, 'batchesFailed'), "{$label}: but not as a failure");
            $this->assertSame(0, $this->requestCount(), "{$label}: the batch rolled back");
        }
    }

    /**
     * Review round 2: with quarantine ON, drainUnits defers 55xxx as well as class
     * 40, and the pure-defer branch counted every deferral as contention. A full
     * disk would then have raised DRAIN_CONTENTION ("no telemetry lost, check agent
     * versions") in place of DRAIN_WRITE_FAILING naming 55P03.
     */
    public function test_with_quarantine_on_a_lock_conflict_is_still_a_named_failure(): void
    {
        $this->appendRequest('77777777-aaaa-4aaa-8aaa-000000000001');

        $worker = $this->worker(quarantine: true);
        $writer = $this->writer();

        self::$pdo->exec(<<<'SQL'
            CREATE OR REPLACE FUNCTION nightowl_test_raise_lock_conflict() RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION 'lock not available' USING ERRCODE = '55P03';
            END;
            $$ LANGUAGE plpgsql;
            CREATE TRIGGER nightowl_test_lock_conflict
                BEFORE INSERT ON nightowl_request_rollups
                FOR EACH ROW EXECUTE FUNCTION nightowl_test_raise_lock_conflict();
        SQL);

        try {
            $this->drainOnce($worker, $writer);
        } finally {
            self::$pdo->exec('DROP TRIGGER IF EXISTS nightowl_test_lock_conflict ON nightowl_request_rollups');
            self::$pdo->exec('DROP FUNCTION IF EXISTS nightowl_test_raise_lock_conflict()');
        }

        $this->assertSame(0, $this->prop($worker, 'transientFailures'), '55P03 is not contention');
        $this->assertSame(1, $this->prop($worker, 'batchesFailed'));
        $this->assertSame('55P03', $this->prop($worker, 'lastWriteSqlstate'), 'the diagnosis must be able to name it');
        $this->assertSame(0, $this->prop($worker, 'batchesDrained'));
    }

    /**
     * Review round 3: DRAIN_CONTENTION tells the operator to look for "Transient
     * conflict, batch deferred" in the agent log. With quarantine on, drainUnits
     * swallows the abort and only the counter moved — a host stuck deadlocking
     * every batch had a critical diagnosis pointing at a line it never wrote.
     */
    public function test_the_log_line_the_diagnosis_names_is_written_on_both_paths(): void
    {
        foreach ([false, true] as $quarantine) {
            self::$pdo->exec('TRUNCATE nightowl_requests');
            self::$pdo->exec('TRUNCATE nightowl_request_rollups');
            unset($this->buffer);
            @unlink($this->bufferPath);
            $this->buffer = new SqliteBuffer($this->bufferPath);
            $this->appendRequest('99999999-aaaa-4aaa-8aaa-00000000000'.($quarantine ? '1' : '2'));

            $log = tempnam(sys_get_temp_dir(), 'nightowl-log');
            $previous = ini_set('error_log', $log);
            try {
                $this->installDeadlockTrigger();
                $this->drainOnce($this->worker($quarantine), $this->writer());
            } finally {
                $this->dropDeadlockTrigger();
                ini_set('error_log', $previous === false ? '' : $previous);
            }

            $label = $quarantine ? 'quarantine on' : 'quarantine off';
            $this->assertStringContainsString(
                'Transient conflict, batch deferred',
                (string) file_get_contents($log),
                "{$label}: the line DRAIN_CONTENTION points at must exist",
            );
            @unlink($log);
        }
    }

    public function test_a_lock_conflict_is_still_a_failed_batch_and_still_names_itself(): void
    {
        // 55P03 must NOT ride along with the deadlock narrowing. In this package's
        // field history it has never been momentary: a full tenant disk and an
        // orphaned idle-in-transaction session behind a pooler both surface as a
        // persistent 55P03, and both were diagnosed from the SQLSTATE that
        // DRAIN_WRITE_FAILING carries. A drain deferring every batch forever with
        // batches_failed at 0 would look idle rather than stuck.
        $this->appendRequest('55555555-aaaa-4aaa-8aaa-000000000001');

        $worker = $this->worker();
        $writer = $this->writer();

        self::$pdo->exec(<<<'SQL'
            CREATE OR REPLACE FUNCTION nightowl_test_raise_lock_conflict() RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION 'lock not available' USING ERRCODE = '55P03';
            END;
            $$ LANGUAGE plpgsql;
            CREATE TRIGGER nightowl_test_lock_conflict
                BEFORE INSERT ON nightowl_request_rollups
                FOR EACH ROW EXECUTE FUNCTION nightowl_test_raise_lock_conflict();
        SQL);

        try {
            $this->drainOnce($worker, $writer);
        } finally {
            self::$pdo->exec('DROP TRIGGER IF EXISTS nightowl_test_lock_conflict ON nightowl_request_rollups');
            self::$pdo->exec('DROP FUNCTION IF EXISTS nightowl_test_raise_lock_conflict()');
        }

        $this->assertSame('55P03', $writer->lastWriteError['sqlstate'] ?? null);
        $this->assertSame(1, $this->prop($worker, 'batchesFailed'), '55P03 must still count');
        $this->assertSame(0, $this->prop($worker, 'transientFailures'));
        $this->assertGreaterThan(0.0, $this->prop($worker, 'lastWriteAt'), 'and must still stamp the write clock');
        $this->assertSame('55P03', $this->prop($worker, 'lastWriteSqlstate'), 'so the diagnosis can name it');
    }

    public function test_a_real_write_rejection_is_still_a_failed_batch(): void
    {
        // The narrowing must not swallow the failures DRAIN_ERRORS exists for. A
        // 42703 (missing column) is deterministic, not transient, and still counts.
        $this->appendRequest('44444444-aaaa-4aaa-8aaa-000000000001');

        $worker = $this->worker();
        $writer = $this->writer();

        self::$pdo->exec('ALTER TABLE nightowl_requests RENAME COLUMN duration TO duration_moved');
        try {
            $this->drainOnce($worker, $writer);
        } finally {
            self::$pdo->exec('ALTER TABLE nightowl_requests RENAME COLUMN duration_moved TO duration');
        }

        $this->assertSame(1, $this->prop($worker, 'batchesFailed'));
        $this->assertSame(0, $this->prop($worker, 'transientFailures'));
        $this->assertGreaterThan(0.0, $this->prop($worker, 'lastWriteAt'));
    }
}
