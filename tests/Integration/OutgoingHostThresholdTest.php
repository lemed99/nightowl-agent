<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\RecordWriter;
use NightOwl\Simulator\NightwatchSimulator;
use NightOwl\Support\RollupTiers;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * A host threshold fires whichever way its host was written.
 *
 * The Thresholds dialog saves the host the dashboard lists, scheme included
 * ("https://api.kettlepay.test"), while the SDK records the bare host
 * ("api.kettlepay.test"). Compared strictly, no threshold saved from the dialog
 * ever matched a call, so slow calls opened no issue and sent no alert.
 *
 * Requires PostgreSQL; see RecordWriterTest for the NIGHTOWL_TEST_DB_* vars.
 */
class OutgoingHostThresholdTest extends TestCase
{
    private const HOST = 'api.kettlepay.test';

    private static ?PDO $pdo = null;

    private static string $host;

    private static int $port;

    private static string $database;

    private static string $username;

    private static string $password;

    private RecordWriter $writer;

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

        $this->writer = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password, storageV2Config: false);
        $this->sim = new NightwatchSimulator('test-token');

        $tables = [
            'nightowl_issue_activity', 'nightowl_issue_comments', 'nightowl_issues',
            'nightowl_outgoing_requests', 'nightowl_settings', 'nightowl_alert_channels',
            'nightowl_outgoing_request_rollups', ...RollupTiers::tierTables('nightowl_outgoing_request_rollups'),
        ];
        foreach ($tables as $table) {
            self::$pdo->exec("TRUNCATE TABLE {$table} CASCADE");
        }
    }

    public static function tearDownAfterClass(): void
    {
        self::$pdo = null;
    }

    /** @return array<string, array{string}> */
    public static function spellingsOfTheSameHost(): array
    {
        return [
            'as the dialog saves it' => ['https://api.kettlepay.test'],
            'bare host' => ['api.kettlepay.test'],
            'mixed case' => ['HTTPS://API.KettlePay.TEST'],
            'trailing slash' => ['https://api.kettlepay.test/'],
            'bare host, trailing slash' => ['api.kettlepay.test/'],
            'with the port the dialog lists' => ['https://api.kettlepay.test:9446'],
        ];
    }

    #[DataProvider('spellingsOfTheSameHost')]
    public function test_a_slow_call_opens_an_issue_whichever_way_the_host_was_saved(string $target): void
    {
        $this->storeThresholds([['type' => 'outgoing_request', 'target' => $target, 'duration_ms' => 1000]]);

        $this->writer->write([$this->slowCall()]);

        $issues = $this->performanceIssues();
        $this->assertCount(1, $issues, "A 3s call to ".self::HOST." must trip a 1000ms threshold saved as \"{$target}\"");
        $this->assertSame('outgoing_request', $issues[0]['subtype']);
        $this->assertSame(self::HOST, $issues[0]['exception_class']);
        $this->assertSame(1000, (int) $issues[0]['threshold_ms']);
    }

    public function test_the_host_threshold_beats_the_global_one(): void
    {
        // Specific beats global. Before the fix the dialog's spelling never
        // matched, so the call fell through to the 10s global and passed.
        $this->storeThresholds([
            ['type' => 'outgoing_request', 'duration_ms' => 10000],
            ['type' => 'outgoing_request', 'target' => 'https://api.kettlepay.test', 'duration_ms' => 1000],
        ]);

        $this->writer->write([$this->slowCall()]);

        $issues = $this->performanceIssues();
        $this->assertCount(1, $issues);
        $this->assertSame(1000, (int) $issues[0]['threshold_ms']);
    }

    public function test_a_threshold_on_another_host_does_not_fire(): void
    {
        $this->storeThresholds([
            ['type' => 'outgoing_request', 'target' => 'https://api.stripe.com', 'duration_ms' => 1000],
            ['type' => 'outgoing_request', 'target' => 'kettlepay.test', 'duration_ms' => 1000],
            ['type' => 'outgoing_request', 'target' => 'https://api.kettlepay.test.example.com', 'duration_ms' => 1000],
            // Same spelling under another type: a route threshold is not a host one.
            ['type' => 'route', 'target' => 'https://api.kettlepay.test', 'duration_ms' => 1000],
        ]);

        $this->writer->write([$this->slowCall()]);

        $this->assertSame([], $this->performanceIssues());
    }

    public function test_a_call_under_the_host_threshold_does_not_fire(): void
    {
        $this->storeThresholds([['type' => 'outgoing_request', 'target' => 'https://api.kettlepay.test', 'duration_ms' => 5000]]);

        $this->writer->write([$this->slowCall()]);

        $this->assertSame([], $this->performanceIssues());
    }

    /** A 3s call shaped like the SDK's record: host is $uri->getHost(). */
    private function slowCall(): array
    {
        return $this->sim->makeOutgoingRequest([
            '_group' => hash('xxh128', self::HOST),
            'host' => self::HOST,
            'method' => 'POST',
            'url' => 'https://api.kettlepay.test/v1/charges',
            'status_code' => 201,
            'duration' => 3_000_000,
        ]);
    }

    /** @param  list<array<string, mixed>>  $thresholds */
    private function storeThresholds(array $thresholds): void
    {
        $stmt = self::$pdo->prepare(
            "INSERT INTO nightowl_settings (key, value, updated_at) VALUES ('thresholds', :value, NOW())
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at"
        );
        $stmt->execute(['value' => json_encode($thresholds)]);
    }

    /** @return list<array<string, mixed>> */
    private function performanceIssues(): array
    {
        return self::$pdo->query(
            "SELECT subtype, exception_class, threshold_ms FROM nightowl_issues WHERE type = 'performance'"
        )->fetchAll(PDO::FETCH_ASSOC);
    }
}
