<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\RecordWriter;
use NightOwl\Simulator\NightwatchSimulator;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * Captured response bodies (NIGHTOWL_CAPTURE_RESPONSE_BODY) on the drain side:
 * they land deflated in nightowl_requests_v2.response_z, and a tenant that has
 * not run migration 000075 still drains its requests — without the bodies —
 * instead of 42703-ing the batch and retrying it forever.
 */
class ResponseBodyStorageTest extends TestCase
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

        self::$pdo->exec('DELETE FROM nightowl_requests_v2');
        self::$pdo->exec('DELETE FROM nightowl_requests');

        $this->sim = new NightwatchSimulator('test-token');
    }

    public function test_a_captured_body_round_trips_and_an_uncaptured_one_stays_null(): void
    {
        $body = '{"ok":true,"access_token":"[6 bytes redacted]"}';

        $this->writer()->write([
            $this->sim->makeRequest(['response_body' => $body]),
            $this->sim->makeRequest(),
        ]);

        $rows = self::$pdo->query('SELECT response_z FROM nightowl_requests_v2 ORDER BY response_z IS NULL')->fetchAll(PDO::FETCH_COLUMN);

        $this->assertCount(2, $rows);
        $z = is_resource($rows[0]) ? stream_get_contents($rows[0]) : $rows[0];
        $this->assertSame($body, gzinflate($z));
        $this->assertNull($rows[1]);
    }

    public function test_a_tenant_without_the_column_drains_the_request_and_drops_the_body(): void
    {
        self::$pdo->exec('ALTER TABLE nightowl_requests_v2 DROP COLUMN response_z');

        try {
            $this->writer()->write([$this->sim->makeRequest(['response_body' => '{"a":1}'])]);

            $this->assertSame(1, (int) self::$pdo->query('SELECT count(*) FROM nightowl_requests_v2')->fetchColumn());
        } finally {
            self::$pdo->exec('ALTER TABLE nightowl_requests_v2 ADD COLUMN IF NOT EXISTS response_z bytea');
        }
    }

    private function writer(): RecordWriter
    {
        return new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password);
    }
}
