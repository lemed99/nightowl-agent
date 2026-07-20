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
 * Integration tests for Phase-2 poison-row isolation — requires live PostgreSQL.
 *
 *   NIGHTOWL_TEST_DB_PORT=5433 vendor/bin/phpunit tests/Integration/DrainWorkerIsolationTest.php
 *
 * Drives the REAL DrainWorker::drainBatch over a real SQLite buffer + RecordWriter
 * against a migrated tenant DB. The poison payload carries a non-numeric
 * status_code, which COPY/INSERT reject with SQLSTATE 22P02 on the integer column.
 */
class DrainWorkerIsolationTest extends TestCase
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

        $this->bufferPath = sys_get_temp_dir().'/nightowl_iso_'.uniqid().'.sqlite';
        $this->buffer = new SqliteBuffer($this->bufferPath);
        $this->sim = new NightwatchSimulator('test-token');
    }

    protected function tearDown(): void
    {
        if (! isset($this->bufferPath)) {
            return; // setUp skipped before initializing the buffer
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

    private function appendRequest(string $traceId, array $overrides = []): void
    {
        $record = $this->sim->makeRequest(array_merge(['trace_id' => $traceId], $overrides));
        $this->buffer->appendRaw(json_encode([$record]));
    }

    private function appendPoison(string $traceId): void
    {
        // Set the int column to a non-numeric string after the simulator builds a
        // valid record, so it survives into the COPY/INSERT as 22P02 poison.
        $record = $this->sim->makeRequest(['trace_id' => $traceId]);
        $record['status_code'] = 'NOTANUMBER';
        $this->buffer->appendRaw(json_encode([$record]));
    }

    private function worker(bool $quarantine, int $breakerThreshold = 50): DrainWorker
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
            quarantineBreakerThreshold: $breakerThreshold,
        );
    }

    private function drainOnce(DrainWorker $worker, RecordWriter $writer): bool
    {
        $m = new ReflectionMethod($worker, 'drainBatch');
        $m->setAccessible(true);

        return (bool) $m->invoke($worker, $this->buffer, $writer);
    }

    private function requestCount(): int
    {
        return (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_requests')->fetchColumn();
    }

    public function test_poison_payload_is_quarantined_and_good_rows_drain(): void
    {
        $this->appendRequest('good-1');
        $this->appendRequest('good-2');
        $this->appendPoison('poison');
        $this->appendRequest('good-3');
        $this->appendRequest('good-4');
        $this->appendRequest('good-5');

        $writer = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password);
        $worker = $this->worker(quarantine: true);

        $this->assertTrue($this->drainOnce($worker, $writer));

        // 5 good rows landed; the 1 poison payload is quarantined; queue advanced.
        $this->assertSame(5, $this->requestCount(), 'all good rows should drain');
        $this->assertSame(1, $this->buffer->quarantinedCount(), 'poison payload quarantined');
        $this->assertSame(0, $this->buffer->pendingCount(), 'nothing left pending');

        // Forward progress: a second drain finds nothing to do (no infinite loop).
        $this->assertFalse($this->drainOnce($worker, $writer));

        // No duplicates and no additive-rollup inflation from bisection re-attempts.
        $this->assertSame(5, $this->requestCount());
        $rollupCalls = (int) self::$pdo->query('SELECT COALESCE(SUM(call_count), 0) FROM nightowl_request_rollups')->fetchColumn();
        $this->assertSame(5, $rollupCalls, 'rollup call_count must equal good rows, not inflated');

        $traces = self::$pdo->query('SELECT trace_id FROM nightowl_requests')->fetchAll(PDO::FETCH_COLUMN);
        $this->assertNotContains('poison', $traces);
    }

    public function test_systematic_poison_trips_breaker_and_stops_dropping(): void
    {
        // Every payload is poison (same SQLSTATE) — a systematic schema mismatch, not
        // per-row poison. The breaker must stop quarantining after the threshold and
        // head-of-line block the rest (surfaced) instead of silently dropping the stream.
        for ($i = 0; $i < 6; $i++) {
            $this->appendPoison('poison-'.$i);
        }

        $writer = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password);
        $worker = $this->worker(quarantine: true, breakerThreshold: 3);

        $this->assertFalse($this->drainOnce($worker, $writer), 'breaker trips → batch reported as not-progressed');
        $this->assertSame(3, $this->buffer->quarantinedCount(), 'no more than the threshold are dropped');
        $this->assertSame(3, $this->buffer->pendingCount(), 'the rest head-of-line block, awaiting a fix');
        $this->assertSame(0, $this->requestCount());

        // Breaker stays tripped — subsequent drains drop nothing further.
        $this->assertFalse($this->drainOnce($worker, $writer));
        $this->assertSame(3, $this->buffer->quarantinedCount());
    }

    public function test_flag_off_blocks_on_poison_and_quarantines_nothing(): void
    {
        $this->appendRequest('good-a');
        $this->appendPoison('poison');

        $writer = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password);
        $worker = $this->worker(quarantine: false);

        // Phase-1 behavior: the whole batch fails (all-or-nothing), nothing is
        // committed or quarantined, rows stay pending (the head-of-line block).
        $this->assertFalse($this->drainOnce($worker, $writer));
        $this->assertSame(0, $this->requestCount());
        $this->assertSame(0, $this->buffer->quarantinedCount());
        $this->assertSame(2, $this->buffer->pendingCount());
    }

    /**
     * F12. The hourly gate must advance ONLY when the sweep actually ran.
     *
     * It used to advance unconditionally, so a worker that failed to take
     * nightowl_partition_maintenance created zero children and did not look again
     * for an hour. Contention on that key stopped being rare when the per-tick
     * leftover heal was split out and took the SAME key 60x more often; with two
     * workers whose gates keep landing inside a contended window, no child sweep
     * completes for RawPartitions::DAYS_AHEAD days — after which every drained row
     * routes to {t}_pdefault, which nightowl:prune can only row-DELETE, never DROP
     * PARTITION. (The shared key is gone now; a peer worker's own hourly sweep
     * still contends for it, which is what this drives.)
     */
    public function test_a_skipped_partition_sweep_does_not_spend_the_hour(): void
    {
        $writer = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password);
        $worker = $this->worker(quarantine: false);

        $gate = new ReflectionMethod($worker, 'maintainPartitionsIfDue');

        // Due (0 = never swept), but a peer owns the maintenance key.
        self::$pdo->query("SELECT pg_advisory_lock(hashtext('nightowl_partition_maintenance'))");

        try {
            $this->assertSame(0, $gate->invoke($worker, $writer, 0),
                'a sweep that could not take the lock must leave the gate where it was, so the next ~60s '
                .'cleanup tick retries instead of forfeiting the hour');
        } finally {
            self::$pdo->query("SELECT pg_advisory_unlock(hashtext('nightowl_partition_maintenance'))");
        }

        $this->assertGreaterThan(0, $gate->invoke($worker, $writer, 0),
            'a sweep that ran must spend the hour');

        // Not due: returned untouched, and no sweep attempted.
        $recent = time();
        $this->assertSame($recent, $gate->invoke($worker, $writer, $recent));
    }
}
