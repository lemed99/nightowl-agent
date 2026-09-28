<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\HealthAlertNotifier;
use NightOwl\Agent\WebhookClient;
use NightOwl\Tests\Fixtures\ScriptedServer;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * The health alerts' channel query against a database that takes the
 * connection and then does not answer: here, the query waits on a lock that
 * another session holds on the channel table, as a migration would.
 *
 * The query had no statement timeout, so the round waited as long as the
 * lock was held, and AsyncServer skipped every later transition as still in
 * flight. It now gives up after HealthAlertNotifier::STATEMENT_TIMEOUT_MS and
 * the round alerts from the saved list, as it does when the connect fails.
 *
 * The session holding the lock ends itself after 15s, so without the timeout
 * these tests fail instead of hanging. Runs in a schema of its own; skips
 * when PostgreSQL is unavailable.
 */
final class HealthAlertStatementTimeoutTest extends TestCase
{
    private const SCHEMA = 'nightowl_alert_stmt_timeout';

    private static ?PDO $pdo = null;

    private ?PDO $locker = null;

    private string $dir;

    private string|false $previousErrorLog = false;

    /** @var list<ScriptedServer> */
    private array $servers = [];

    private static function db(): array
    {
        return [
            'host' => getenv('NIGHTOWL_TEST_DB_HOST') ?: '127.0.0.1',
            'port' => (int) (getenv('NIGHTOWL_TEST_DB_PORT') ?: 5432),
            'database' => getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test',
            'username' => getenv('NIGHTOWL_TEST_DB_USERNAME') ?: 'nightowl_test',
            'password' => getenv('NIGHTOWL_TEST_DB_PASSWORD') ?: 'test123',
        ];
    }

    private static function connect(): PDO
    {
        $c = self::db();

        return new PDO(
            sprintf('pgsql:host=%s;port=%d;dbname=%s', $c['host'], $c['port'], $c['database']),
            $c['username'],
            $c['password'],
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public static function setUpBeforeClass(): void
    {
        try {
            self::$pdo = self::connect();
        } catch (\Throwable) {
            self::$pdo = null;
        }
    }

    protected function setUp(): void
    {
        if (self::$pdo === null) {
            $this->markTestSkipped('PostgreSQL unavailable.');
        }

        self::$pdo->exec('DROP SCHEMA IF EXISTS '.self::SCHEMA.' CASCADE');
        self::$pdo->exec('CREATE SCHEMA '.self::SCHEMA);
        // The shape from migration 000018.
        self::$pdo->exec('CREATE TABLE '.self::SCHEMA.'.nightowl_alert_channels (
            id bigserial primary key,
            name varchar(255) NOT NULL,
            type varchar(50) NOT NULL,
            config text NOT NULL,
            enabled boolean NOT NULL DEFAULT true,
            created_at timestamp NULL,
            updated_at timestamp NULL
        )');

        $this->dir = sys_get_temp_dir().'/nightowl-alert-stmt-timeout-'.getmypid().'-'.bin2hex(random_bytes(4));
        mkdir($this->dir, 0777, true);

        $this->previousErrorLog = (string) ini_get('error_log');
        ini_set('error_log', $this->dir.'/error.log');
    }

    protected function tearDown(): void
    {
        if ($this->locker !== null) {
            try {
                $this->locker->rollBack();
            } catch (\Throwable) {
                // Already ended by its idle timeout.
            }
            $this->locker = null;
        }

        foreach ($this->servers as $server) {
            $server->cleanup();
        }
        $this->servers = [];

        if ($this->previousErrorLog !== false) {
            ini_set('error_log', $this->previousErrorLog);
            $this->previousErrorLog = false;
        }

        if (isset($this->dir)) {
            foreach (glob($this->dir.'/*') ?: [] as $file) {
                @unlink($file);
            }
            @rmdir($this->dir);
        }

        self::$pdo?->exec('DROP SCHEMA IF EXISTS '.self::SCHEMA.' CASCADE');
    }

    public function test_a_channel_query_held_by_a_lock_gives_up_at_the_statement_timeout(): void
    {
        $this->addWebhook('ops-hook', 'http://127.0.0.1:1/hook');
        $this->assertTrue($this->notifier()->primeChannels(), 'the notifier cannot read the table even unlocked, so this proves nothing');

        $this->lockChannelTable();

        $start = microtime(true);
        $read = $this->notifier()->primeChannels();
        $elapsed = microtime(true) - $start;

        $this->assertFalse($read, 'the query waited for the lock to be released');
        $this->assertGreaterThan(4.0, $elapsed, 'the query failed at once, not at the timeout');
        $this->assertLessThan(10.0, $elapsed);
        $this->assertStringContainsString('statement timeout', (string) file_get_contents($this->dir.'/error.log'));
    }

    public function test_a_round_held_by_a_lock_alerts_from_the_saved_list(): void
    {
        $http = ScriptedServer::start('fake-http-server.php', ['status' => 200, 'requests' => 1]);
        $this->servers[] = $http;
        $this->addWebhook('ops-hook', "http://127.0.0.1:{$http->port}/hook");
        $this->assertTrue($this->notifier()->primeChannels());

        $this->lockChannelTable();

        $start = microtime(true);
        $this->notifier()->dispatch([[
            'code' => 'PG_LATENCY_CRITICAL',
            'level' => 'critical',
            'message' => 'PostgreSQL latency is 4200ms',
            'recommendation' => 'Check the database',
            'value' => 4200,
        ]]);
        $elapsed = microtime(true) - $start;

        $this->assertLessThan(10.0, $elapsed, 'the round waited for the lock to be released');
        $this->assertStringContainsString('Using the saved list', (string) file_get_contents($this->dir.'/error.log'));
        $this->assertNotEmpty(
            array_filter($http->transcript(), static fn (string $line): bool => str_contains($line, 'PG_LATENCY_CRITICAL')),
            'the alert was not delivered',
        );
    }

    // ─── Helpers ─────────────────────────────────────────────────────

    /** A notifier on this test's schema, with a saved list next to it. */
    private function notifier(): HealthAlertNotifier
    {
        $c = self::db();

        return new HealthAlertNotifier(
            sprintf("pgsql:host=%s;port=%d;dbname=%s;options='-c search_path=%s'", $c['host'], $c['port'], $c['database'], self::SCHEMA),
            $c['username'],
            $c['password'],
            appName: 'AcmeApp',
            instanceId: 'web-1:2407',
            webhook: new WebhookClient(5.0),
            channelsFile: $this->dir.'/agent-buffer.sqlite.alert-channels.json',
        );
    }

    private function addWebhook(string $name, string $url): void
    {
        self::$pdo->prepare('INSERT INTO '.self::SCHEMA.".nightowl_alert_channels (name, type, config, enabled, created_at, updated_at) VALUES (?, 'webhook', ?, true, now(), now())")
            ->execute([$name, json_encode(['url' => $url])]);
    }

    /**
     * Hold ACCESS EXCLUSIVE on the channel table from another session, as a
     * migration altering it would. The session ends itself after 15s idle in
     * the transaction, which releases the lock.
     */
    private function lockChannelTable(): void
    {
        $this->locker = self::connect();
        $this->locker->exec("SET idle_in_transaction_session_timeout = '15s'");
        $this->locker->beginTransaction();
        $this->locker->exec('LOCK TABLE '.self::SCHEMA.'.nightowl_alert_channels IN ACCESS EXCLUSIVE MODE');
    }
}
