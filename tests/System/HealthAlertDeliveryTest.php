<?php

namespace NightOwl\Tests\System;

use NightOwl\Agent\HealthAlertNotifier;
use NightOwl\Agent\SmtpClient;
use NightOwl\Agent\WebhookClient;
use NightOwl\Tests\Fixtures\ScriptedServer;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * Health alerts on the wire: what the receiving server actually got.
 *
 * Two things are under test. A round that cannot reach the database still
 * delivers, to the channel list the last healthy round saved (each round is a
 * fresh forked child, so nothing in memory survives between them). And every
 * channel carries a link to the app's Agent Health page, with the email sent
 * as HTML in the issue emails' template.
 *
 * The channel table is a SQLite file, "down" means its directory is gone, and
 * the receivers are the scripted servers in tests/Fixtures. No PostgreSQL and
 * no pcntl.
 */
class HealthAlertDeliveryTest extends TestCase
{
    private const HEALTH_URL = 'https://dash.example/dashboard/app_123/health';

    private string $dir;

    private string|false $previousErrorLog = false;

    /** @var list<ScriptedServer> */
    private array $servers = [];

    protected function setUp(): void
    {
        $this->dir = sys_get_temp_dir().'/nightowl-health-delivery-'.getmypid().'-'.bin2hex(random_bytes(4));
        mkdir($this->dir.'/db', 0777, true);
        mkdir($this->dir.'/buffer', 0777, true);

        // Failed sends log; keep them out of the test output.
        $this->previousErrorLog = (string) ini_get('error_log');
        ini_set('error_log', $this->dir.'/error.log');
    }

    protected function tearDown(): void
    {
        foreach ($this->servers as $server) {
            $server->cleanup();
        }
        $this->servers = [];

        if ($this->previousErrorLog !== false) {
            ini_set('error_log', $this->previousErrorLog);
        }

        foreach ([$this->dir.'/db', $this->dir.'/db-gone'] as $dbDir) {
            foreach (glob($dbDir.'/*') ?: [] as $file) {
                @unlink($file);
            }
            @rmdir($dbDir);
        }
        foreach (glob($this->dir.'/buffer/*') ?: [] as $file) {
            @unlink($file);
        }
        @rmdir($this->dir.'/buffer');
        @unlink($this->dir.'/error.log');
        @rmdir($this->dir);
    }

    // ─── Saved channel list ──────────────────────────────────────────

    public function test_an_alert_is_delivered_from_the_saved_list_while_the_database_is_down(): void
    {
        $http = $this->startServer('fake-http-server.php', ['status' => 200, 'requests' => 2]);
        $db = $this->channelDb();
        $this->addChannel($db, 'ops-hook', 'webhook', ['url' => "http://127.0.0.1:{$http->port}/hook"]);

        $this->notifier()->dispatch([$this->diagnosis('PG_LATENCY_WARNING', 'warning')]);

        rename($this->dir.'/db', $this->dir.'/db-gone');

        // A fresh instance, as each forked round is.
        $this->notifier()->dispatch([$this->diagnosis('DRAIN_STOPPED')]);

        $payloads = $this->jsonBodies($http->transcript());

        $this->assertCount(2, $payloads, 'the round without the database delivered nothing');
        $this->assertSame('PG_LATENCY_WARNING', $payloads[0]['diagnosis']['code']);
        $this->assertSame('DRAIN_STOPPED', $payloads[1]['diagnosis']['code']);
    }

    /**
     * An agent that has never sent an alert: only the prime AsyncServer runs
     * after startup has touched the database. Its first alert, sent with the
     * database already down, still arrives.
     */
    public function test_the_first_alert_of_an_outage_is_delivered_after_a_prime(): void
    {
        $http = $this->startServer('fake-http-server.php', ['status' => 200, 'requests' => 1]);
        $db = $this->channelDb();
        $this->addChannel($db, 'ops-hook', 'webhook', ['url' => "http://127.0.0.1:{$http->port}/hook"]);

        $this->assertTrue($this->notifier()->primeChannels());

        rename($this->dir.'/db', $this->dir.'/db-gone');

        $this->notifier()->dispatch([$this->diagnosis('DRAIN_STOPPED')]);

        $payloads = $this->jsonBodies($http->transcript());

        $this->assertCount(1, $payloads, 'the first alert of the outage was not delivered');
        $this->assertSame('DRAIN_STOPPED', $payloads[0]['diagnosis']['code']);
    }

    // ─── Links ───────────────────────────────────────────────────────

    /**
     * The email went out as text/plain with no URL in it. It is now the HTML
     * template with a button to Agent Health. The channel has no username,
     * which the API now allows, so no AUTH may be sent either.
     */
    public function test_the_health_email_is_html_and_links_to_agent_health(): void
    {
        $smtp = $this->startServer('fake-smtp-server.php');
        $db = $this->channelDb();
        $this->addChannel($db, 'ops-mail', 'email', [
            'host' => '127.0.0.1',
            'port' => $smtp->port,
            'username' => null,
            'password' => null,
            'encryption' => 'none',
            'from_address' => 'alerts@acme.test',
            'from_name' => 'NightOwl',
            'to_addresses' => ['ops@acme.test'],
        ]);

        $this->notifier()->dispatch([$this->diagnosis()]);

        $transcript = $smtp->transcript();
        $html = $this->decodedBody($transcript);

        $this->assertContains('Subject: [AcmeApp] Agent Health: DRAIN_STOPPED', $transcript);
        $this->assertContains('Content-Type: text/html; charset=UTF-8', $transcript);
        $this->assertStringContainsString('<a href="'.self::HEALTH_URL.'"', $html);
        $this->assertStringContainsString('>View agent health</a>', $html);
        $this->assertStringContainsString('Drain has not written for 120s', $html);
        $this->assertSame([], array_values(array_filter($transcript, static fn (string $l): bool => str_starts_with($l, 'AUTH'))));
    }

    public function test_slack_discord_and_webhook_alerts_link_to_agent_health(): void
    {
        $http = $this->startServer('fake-http-server.php', ['status' => 200, 'requests' => 3]);
        $url = "http://127.0.0.1:{$http->port}";
        $db = $this->channelDb();
        $this->addChannel($db, 'slack', 'slack', ['webhook_url' => "{$url}/slack"]);
        $this->addChannel($db, 'discord', 'discord', ['webhook_url' => "{$url}/discord"]);
        $this->addChannel($db, 'hook', 'webhook', ['url' => "{$url}/hook"]);

        $this->notifier()->dispatch([$this->diagnosis()]);

        $bodies = $this->bodiesByChannel($http->transcript());

        $this->assertStringEndsWith("\n<".self::HEALTH_URL.'|View agent health>', $bodies['slack']['text']);
        $this->assertStringEndsWith("\n[View agent health](<".self::HEALTH_URL.'>)', $bodies['discord']['content']);

        // New keys only: every existing key is still there, same shape.
        $hook = $bodies['webhook'];
        $this->assertSame(self::HEALTH_URL, $hook['view_url']);
        $this->assertSame('app_123', $hook['app_id']);
        $this->assertSame('health.degraded', $hook['event']);
        $this->assertSame('AcmeApp', $hook['app']);
        $this->assertSame('web-1:2407', $hook['instance']);
        $this->assertSame('DRAIN_STOPPED', $hook['diagnosis']['code']);
        $this->assertSame('active', $hook['diagnosis']['status']);
        $this->assertArrayHasKey('timestamp', $hook);
    }

    public function test_no_link_is_sent_without_a_dashboard_url(): void
    {
        $http = $this->startServer('fake-http-server.php', ['status' => 200, 'requests' => 2]);
        $url = "http://127.0.0.1:{$http->port}";
        $db = $this->channelDb();
        $this->addChannel($db, 'slack', 'slack', ['webhook_url' => "{$url}/slack"]);
        $this->addChannel($db, 'hook', 'webhook', ['url' => "{$url}/hook"]);

        $this->notifier(dashboardUrl: '')->dispatchRecovered([$this->diagnosis()]);

        $bodies = $this->bodiesByChannel($http->transcript());

        $this->assertStringNotContainsString('View agent health', $bodies['slack']['text']);
        $this->assertStringEndsWith('(resolved)', $bodies['slack']['text']);
        $this->assertArrayHasKey('view_url', $bodies['webhook']);
        $this->assertNull($bodies['webhook']['view_url']);
    }

    /** Same fallback as AlertNotifier's view_url: no app id, the dashboard itself. */
    public function test_without_an_app_id_the_link_goes_to_the_dashboard(): void
    {
        $http = $this->startServer('fake-http-server.php', ['status' => 200]);
        $db = $this->channelDb();
        $this->addChannel($db, 'hook', 'webhook', ['url' => "http://127.0.0.1:{$http->port}/hook"]);

        $this->notifier(appId: null)->dispatch([$this->diagnosis()]);

        $this->assertSame('https://dash.example/dashboard', $this->jsonBodies($http->transcript())[0]['view_url']);
    }

    // ─── Helpers ─────────────────────────────────────────────────────

    private function startServer(string $fixture, array $script = []): ScriptedServer
    {
        $server = ScriptedServer::start($fixture, $script);
        $this->servers[] = $server;

        return $server;
    }

    private function channelDb(): PDO
    {
        $db = new PDO('sqlite:'.$this->dir.'/db/channels.sqlite');
        $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $db->exec('CREATE TABLE nightowl_alert_channels (
            id integer primary key,
            name text NOT NULL,
            type text NOT NULL,
            config text NOT NULL,
            enabled boolean NOT NULL DEFAULT 1,
            updated_at text NULL
        )');

        return $db;
    }

    private function addChannel(PDO $db, string $name, string $type, array $config): void
    {
        $db->prepare("INSERT INTO nightowl_alert_channels (name, type, config, enabled, updated_at) VALUES (?, ?, ?, 1, datetime('now'))")
            ->execute([$name, $type, json_encode($config)]);
    }

    private function notifier(string $dashboardUrl = 'https://dash.example/', ?string $appId = 'app_123'): HealthAlertNotifier
    {
        return new HealthAlertNotifier(
            'sqlite:'.$this->dir.'/db/channels.sqlite',
            '',
            '',
            'AcmeApp',
            'web-1:2407',
            new SmtpClient(null, 2.0, 2.0),
            new WebhookClient(2.0),
            $dashboardUrl,
            $appId,
            $this->dir.'/buffer/agent-buffer.sqlite.alert-channels.json',
        );
    }

    private function diagnosis(string $code = 'DRAIN_STOPPED', string $level = 'critical'): array
    {
        return [
            'code' => $code,
            'level' => $level,
            'message' => 'Drain has not written for 120s',
            'recommendation' => 'Check PostgreSQL connectivity',
            'value' => 120,
        ];
    }

    /**
     * The message body as the recipient's mail client reads it: the lines
     * between the headers and the terminator, un-dot-stuffed, QP-decoded.
     */
    private function decodedBody(array $transcript): string
    {
        $data = array_search('DATA', $transcript, true);
        $lines = array_slice($transcript, $data === false ? 0 : $data + 1);
        $start = array_search('', $lines, true);
        $end = array_search('.', $lines, true);
        $body = array_slice($lines, $start + 1, ($end === false ? count($lines) : $end) - $start - 1);

        return quoted_printable_decode(implode("\r\n", array_map(
            static fn (string $line): string => str_starts_with($line, '..') ? substr($line, 1) : $line,
            $body,
        )));
    }

    /**
     * The request bodies the fake HTTP server recorded, in order.
     *
     * @return list<array<string, mixed>>
     */
    private function jsonBodies(array $transcript): array
    {
        $bodies = [];
        foreach ($transcript as $line) {
            if (str_starts_with($line, '{')) {
                $bodies[] = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
            }
        }

        return $bodies;
    }

    /** @return array<string, array<string, mixed>> keyed slack / discord / webhook */
    private function bodiesByChannel(array $transcript): array
    {
        $byChannel = [];
        foreach ($this->jsonBodies($transcript) as $body) {
            $channel = match (true) {
                isset($body['text']) => 'slack',
                isset($body['content']) => 'discord',
                default => 'webhook',
            };
            $byChannel[$channel] = $body;
        }

        return $byChannel;
    }
}
