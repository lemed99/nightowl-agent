<?php

namespace NightOwl\Tests\Integration;

use Illuminate\Config\Repository;
use Illuminate\Database\DatabaseServiceProvider;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use NightOwl\Commands\TestAlertCommand;
use NightOwl\Tests\Fixtures\ScriptedServer;
use PDO;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * `nightowl:test-alert` end to end — the command as a customer runs it, against
 * real channel rows in Postgres and real servers on the other end of the socket.
 *
 * The bug this command exists for: nightowl-api dispatches the dashboard's "Send
 * test" button and every triage alert (issue.resolved, issue.ignored) through
 * Symfony Mailer, while `issue.new` and `issue.reopened` go out from the drain
 * through the agent's own raw SMTP and HTTP. Two transports, one config row. A
 * customer can test green in the dashboard, receive triage mail all day, and
 * never once be told about a new exception.
 *
 * So the assertions here are about REPORTING, not just delivery. A dispatcher
 * that swallows its outcome turns this command into a second false green — which
 * would be worse than not having it, because the customer would then have a
 * "PASS" to point at. Every case below is a way the old code reported success
 * while nothing was delivered.
 *
 * Runs inside a dedicated Postgres schema; skips when Postgres is unavailable.
 */
final class TestAlertCommandTest extends TestCase
{
    private const SCHEMA = 'nightowl_test_alert_cmd';

    private static ?PDO $pdo = null;

    private Application $app;

    /** @var list<ScriptedServer> */
    private array $servers = [];

    private static function dbConfig(): array
    {
        return [
            'driver' => 'pgsql',
            'host' => getenv('NIGHTOWL_TEST_DB_HOST') ?: '127.0.0.1',
            'port' => (int) (getenv('NIGHTOWL_TEST_DB_PORT') ?: 5432),
            'database' => getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test',
            'username' => getenv('NIGHTOWL_TEST_DB_USERNAME') ?: 'nightowl_test',
            'password' => getenv('NIGHTOWL_TEST_DB_PASSWORD') ?: 'test123',
            'charset' => 'utf8',
            'search_path' => self::SCHEMA,
        ];
    }

    public static function setUpBeforeClass(): void
    {
        $c = self::dbConfig();

        try {
            self::$pdo = new PDO(
                sprintf('pgsql:host=%s;port=%d;dbname=%s', $c['host'], $c['port'], $c['database']),
                $c['username'],
                $c['password'],
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            );
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
        self::$pdo->exec('SET search_path TO '.self::SCHEMA);

        // The shape from migration 000018. The command reads only these rows;
        // it never touches telemetry.
        self::$pdo->exec('CREATE TABLE '.self::SCHEMA.'.nightowl_alert_channels (
            id bigserial primary key,
            name varchar(255) NOT NULL,
            type varchar(50) NOT NULL,
            config text NOT NULL,
            enabled boolean NOT NULL DEFAULT true,
            created_at timestamp NULL,
            updated_at timestamp NULL
        )');

        $this->app = new Application(sys_get_temp_dir().'/nightowl-test-alert-cmd');
        $this->app->singleton('config', fn () => new Repository([
            'app' => ['name' => 'AcmeApp', 'env' => 'production'],
            'database' => [
                'default' => 'nightowl',
                'connections' => ['nightowl' => self::dbConfig()],
            ],
            'nightowl' => [
                'agent' => ['app_id' => null, 'dashboard_url' => 'https://app.usenightowl.com'],
                'threshold_cache_ttl' => 86400,
                'reopen_cooldown_hours' => 0,
                'environment' => null,
                // Short, so a test that deliberately points at a dead port fails
                // in a second rather than ten.
                'smtp' => ['helo' => null, 'connect_timeout' => 1, 'timeout' => 2],
            ],
        ]));
        (new DatabaseServiceProvider($this->app))->register();

        Facade::clearResolvedInstances();
        Facade::setFacadeApplication($this->app);
    }

    protected function tearDown(): void
    {
        foreach ($this->servers as $server) {
            $server->cleanup();
        }
        $this->servers = [];

        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);

        self::$pdo?->exec('DROP SCHEMA IF EXISTS '.self::SCHEMA.' CASCADE');
    }

    // ─── Harness ─────────────────────────────────────────────────────

    private function addChannel(string $name, string $type, array $config, bool $enabled = true): void
    {
        $stmt = self::$pdo->prepare('INSERT INTO '.self::SCHEMA.'.nightowl_alert_channels
            (name, type, config, enabled, created_at, updated_at)
            VALUES (?, ?, ?, ?, now(), now())');

        $stmt->execute([$name, $type, json_encode($config), $enabled ? 'true' : 'false']);
    }

    private function startServer(string $fixture, array $script = []): ScriptedServer
    {
        $server = ScriptedServer::start($fixture, $script);
        $this->servers[] = $server;

        return $server;
    }

    /** @return array{exit: int, output: string} */
    private function runCommand(array $options = []): array
    {
        $command = new TestAlertCommand;
        $command->setLaravel($this->app);
        $output = new BufferedOutput;

        return ['exit' => $command->run(new ArrayInput($options), $output), 'output' => $output->fetch()];
    }

    private function emailConfig(int $port, array $overrides = []): array
    {
        return array_merge([
            'host' => '127.0.0.1',
            'port' => $port,
            'username' => 'alerts@acme.test',
            'password' => 's3cr3t',
            'encryption' => 'none',
            'from_address' => 'alerts@acme.test',
            'from_name' => 'NightOwl',
            'to_addresses' => ['ops@acme.test'],
        ], $overrides);
    }

    // ─── Email ───────────────────────────────────────────────────────

    public function test_a_working_email_channel_passes_and_the_relay_receives_the_alert(): void
    {
        $smtp = $this->startServer('fake-smtp-server.php');
        $this->addChannel('Ops mail', 'email', $this->emailConfig($smtp->port));

        $result = $this->runCommand();

        $this->assertSame(0, $result['exit'], $result['output']);
        $this->assertStringContainsString('PASS', $result['output']);
        $this->assertStringContainsString('Ops mail (email)', $result['output']);

        // Delivered, not merely "not errored".
        $transcript = $smtp->transcript();
        $this->assertContains('RCPT TO:<ops@acme.test>', $transcript);
        $this->assertContains('DATA', $transcript);
        $this->assertContains('.', $transcript, 'the message body was never terminated');

        $subject = null;
        foreach ($transcript as $line) {
            if (str_starts_with($line, 'Subject: ')) {
                $subject = $line;
            }
        }
        $this->assertNotNull($subject);
        $this->assertStringContainsString('AcmeApp', $subject, 'the app name should identify which app alerted');
    }

    /**
     * The whole point. A relay that refuses the connection used to leave one
     * error_log line and nothing else; now it is a FAIL carrying the reason.
     */
    public function test_an_unreachable_relay_fails_with_the_reason(): void
    {
        // Port 1 — nothing listens there, and connect fails immediately.
        $this->addChannel('Broken mail', 'email', $this->emailConfig(1));

        $result = $this->runCommand();

        $this->assertSame(1, $result['exit']);
        $this->assertStringContainsString('FAIL', $result['output']);
        $this->assertStringContainsString('Broken mail (email)', $result['output']);
        $this->assertStringContainsString('SMTP connect to 127.0.0.1:1 failed', $result['output']);
    }

    public function test_a_relay_that_rejects_the_credentials_fails_with_the_servers_own_words(): void
    {
        $smtp = $this->startServer('fake-smtp-server.php', [
            'auth' => "535 5.7.8 Authentication credentials invalid\r\n",
        ]);
        $this->addChannel('Bad password', 'email', $this->emailConfig($smtp->port));

        $result = $this->runCommand();

        $this->assertSame(1, $result['exit']);
        $this->assertStringContainsString('535 5.7.8 Authentication credentials invalid', $result['output']);

        // The AUTH command line carries the base64'd password; the report must
        // be built from the context label, never the command.
        $this->assertStringNotContainsString('s3cr3t', $result['output']);
    }

    public function test_an_email_channel_missing_its_recipients_fails_instead_of_passing_silently(): void
    {
        $smtp = $this->startServer('fake-smtp-server.php');
        $this->addChannel('No recipients', 'email', $this->emailConfig($smtp->port, ['to_addresses' => []]));

        $result = $this->runCommand();

        $this->assertSame(1, $result['exit']);
        $this->assertStringContainsString('incomplete', $result['output']);
    }

    // ─── Webhook / Slack / Discord ───────────────────────────────────

    public function test_a_webhook_channel_passes_and_the_receiver_gets_the_payload(): void
    {
        $http = $this->startServer('fake-http-server.php', ['status' => 200]);
        $this->addChannel('Ops hook', 'webhook', ['url' => "http://127.0.0.1:{$http->port}/hook"]);

        $result = $this->runCommand();

        $this->assertSame(0, $result['exit'], $result['output']);
        $this->assertStringContainsString('PASS', $result['output']);

        $request = implode("\n", $http->transcript());
        $this->assertStringContainsString('POST /hook HTTP/1.1', $request);
        $this->assertStringContainsString('issue.new', $request, 'the payload should be a real issue.new event');
        $this->assertStringContainsString('TestNotification', $request);
    }

    /**
     * `httpPost` used to `@file_get_contents` and discard the result entirely —
     * so a webhook returning 500, 401 or 404 on every alert reported nothing at
     * all, and this command would have called it PASS.
     */
    public function test_a_webhook_returning_an_error_status_fails(): void
    {
        $http = $this->startServer('fake-http-server.php', [
            'status' => 500,
            'body' => 'upstream exploded',
        ]);
        $this->addChannel('Ops hook', 'webhook', ['url' => "http://127.0.0.1:{$http->port}/hook"]);

        $result = $this->runCommand();

        $this->assertSame(1, $result['exit']);
        $this->assertStringContainsString('returned 500', $result['output']);
        $this->assertStringContainsString('upstream exploded', $result['output'], 'the receiver\'s own words are the diagnosis');
    }

    /**
     * A Slack/Discord webhook path IS the credential — anyone holding the full
     * URL can post as that app. It must not land in console output a customer
     * will paste into a support thread.
     */
    public function test_a_failing_slack_webhook_is_reported_without_leaking_its_url(): void
    {
        $http = $this->startServer('fake-http-server.php', ['status' => 404, 'body' => 'no_service']);
        $secret = 'T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX';
        $this->addChannel('Team Slack', 'slack', [
            'webhook_url' => "http://127.0.0.1:{$http->port}/services/{$secret}",
        ]);

        $result = $this->runCommand();

        $this->assertSame(1, $result['exit']);
        $this->assertStringContainsString('returned 404', $result['output']);
        $this->assertStringNotContainsString($secret, $result['output']);
        $this->assertStringNotContainsString('XXXXXXXXXXXXXXXXXXXXXXXX', $result['output']);
    }

    public function test_a_channel_with_no_url_at_all_fails_rather_than_reporting_success(): void
    {
        $this->addChannel('Empty hook', 'webhook', ['url' => '']);

        $result = $this->runCommand();

        $this->assertSame(1, $result['exit']);
        $this->assertStringContainsString('no url configured', $result['output']);
    }

    // ─── Channel selection and reporting ─────────────────────────────

    public function test_disabled_channels_are_not_tested(): void
    {
        $this->addChannel('Off', 'webhook', ['url' => 'http://127.0.0.1:1/hook'], enabled: false);

        $result = $this->runCommand();

        $this->assertSame(1, $result['exit']);
        $this->assertStringContainsString('No enabled alert channels', $result['output']);
        $this->assertStringNotContainsString('Off', $result['output']);
    }

    public function test_the_channel_option_tests_only_that_channel(): void
    {
        $http = $this->startServer('fake-http-server.php', ['status' => 200]);
        $this->addChannel('Ops hook', 'webhook', ['url' => "http://127.0.0.1:{$http->port}/hook"]);
        // Would fail if it were tested — proving the filter, not just the pass.
        $this->addChannel('Broken mail', 'email', $this->emailConfig(1));

        $result = $this->runCommand(['--channel' => 'Ops hook']);

        $this->assertSame(0, $result['exit'], $result['output']);
        $this->assertStringContainsString('Ops hook', $result['output']);
        $this->assertStringNotContainsString('Broken mail', $result['output']);
    }

    public function test_an_unknown_channel_name_is_distinguished_from_having_no_channels(): void
    {
        $http = $this->startServer('fake-http-server.php', ['status' => 200]);
        $this->addChannel('Ops hook', 'webhook', ['url' => "http://127.0.0.1:{$http->port}/hook"]);

        $result = $this->runCommand(['--channel' => 'Typo']);

        $this->assertSame(1, $result['exit']);
        $this->assertStringContainsString("No enabled channel named 'Typo'", $result['output']);
    }

    /**
     * The test deliberately ignores notify_events so a transport failure can't
     * hide behind a filter — which means a channel can PASS here and still be
     * silent in production. Saying so is the difference between a useful pass
     * and a misleading one.
     */
    public function test_a_channel_muting_issue_events_passes_but_is_flagged(): void
    {
        $http = $this->startServer('fake-http-server.php', ['status' => 200]);
        $this->addChannel('Resolved only', 'webhook', [
            'url' => "http://127.0.0.1:{$http->port}/hook",
            'notify_events' => ['issue.resolved'],
        ]);

        $result = $this->runCommand();

        $this->assertSame(0, $result['exit'], $result['output']);
        $this->assertStringContainsString('PASS', $result['output']);
        $this->assertStringContainsString('issue.new', $result['output']);
        $this->assertStringContainsString('issue.reopened', $result['output']);
        $this->assertStringContainsString('will not alert', $result['output']);
    }

    public function test_one_failing_channel_does_not_stop_the_others_being_tested(): void
    {
        $http = $this->startServer('fake-http-server.php', ['status' => 200]);
        $this->addChannel('Broken mail', 'email', $this->emailConfig(1));
        $this->addChannel('Ops hook', 'webhook', ['url' => "http://127.0.0.1:{$http->port}/hook"]);

        $result = $this->runCommand();

        $this->assertSame(1, $result['exit']);
        $this->assertStringContainsString('FAIL', $result['output']);
        $this->assertStringContainsString('PASS', $result['output']);
        $this->assertStringContainsString('1 of 2 channel(s) failed', $result['output']);

        // The healthy channel was genuinely dispatched to, not just listed.
        $this->assertStringContainsString('POST /hook', implode("\n", $http->transcript()));
    }
}
