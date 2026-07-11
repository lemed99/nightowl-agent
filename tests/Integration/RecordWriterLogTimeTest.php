<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\RecordWriter;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * nightowl_logs.created_at is a text column, unlike every other telemetry table where
 * it is a real timestamp. Postgres therefore accepts an out-of-range date instead of
 * rejecting it (22008), and a year > 9999 sorts lexicographically above every
 * PruneCommand cutoff — so the row can never be deleted. writeLogs must route the
 * event timestamp through the same range guard the other writers use.
 *
 * Requires PostgreSQL (see RecordWriterTest for env vars / docker one-liner).
 */
class RecordWriterLogTimeTest extends TestCase
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

        $this->writer = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password);
        self::$pdo->exec('TRUNCATE nightowl_logs');
    }

    public static function tearDownAfterClass(): void
    {
        self::$pdo = null;
    }

    private function log(string $traceId, mixed $timestamp): array
    {
        return [
            't' => 'log',
            'v' => 1,
            'trace_id' => $traceId,
            'timestamp' => $timestamp,
            'level' => 'warning',
            'message' => 'disk is filling up',
            'channel' => 'stack',
        ];
    }

    private function createdAt(string $traceId): string
    {
        $stmt = self::$pdo->prepare('SELECT created_at FROM nightowl_logs WHERE trace_id = ?');
        $stmt->execute([$traceId]);

        return (string) $stmt->fetchColumn();
    }

    public function test_valid_log_timestamp_is_preserved(): void
    {
        $eventTs = time() - 3600;
        $this->writer->write([$this->log('log-valid', $eventTs)]);

        $this->assertSame(gmdate('Y-m-d H:i:s', $eventTs), $this->createdAt('log-valid'));
    }

    /**
     * A millisecond-scaled timestamp lands ~50,000 years out. Unguarded, gmdate() stamps
     * a year-33658 string that no prune cutoff can ever match.
     */
    public function test_millisecond_scaled_log_timestamp_falls_back_to_drain_clock(): void
    {
        $now = time();
        $this->writer->write([$this->log('log-ms', $now * 1000)]);

        $createdAt = $this->createdAt('log-ms');

        $this->assertMatchesRegularExpression('/^\d{4}-/', $createdAt, "created_at year overflowed four digits: {$createdAt}");
        $this->assertEqualsWithDelta($now, strtotime($createdAt.' UTC'), 5.0);
    }

    /**
     * The bug that matters: an unprunable row. PruneCommand issues
     * `WHERE created_at < :cutoff` against this text column.
     */
    public function test_malformed_log_timestamps_remain_prunable(): void
    {
        $now = time();
        $this->writer->write([
            $this->log('log-ms', $now * 1000),          // ms-scaled -> year 33658 unguarded
            $this->log('log-garbage', 'not-a-number'),  // non-numeric
            $this->log('log-far', 99999999999999),      // far future
        ]);

        // Same comparison PruneCommand makes, with a cutoff in the future so every
        // well-formed row must match. An unguarded year-33658 string sorts above it.
        $cutoff = gmdate('Y-m-d H:i:s', $now + 60);

        $stmt = self::$pdo->prepare('SELECT count(*) FROM nightowl_logs WHERE created_at < ?');
        $stmt->execute([$cutoff]);

        $this->assertSame(3, (int) $stmt->fetchColumn(), 'a log row was written with a created_at no prune cutoff can match');
    }
}
