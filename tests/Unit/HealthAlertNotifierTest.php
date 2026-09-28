<?php

namespace NightOwl\Tests\Unit;

use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use NightOwl\Agent\HealthAlertNotifier;
use NightOwl\Agent\WebhookClient;
use NightOwl\Tests\Fixtures\ScriptedServer;
use PDO;
use PHPUnit\Framework\TestCase;

class HealthAlertNotifierTest extends TestCase
{
    // Header sanitization moved to SmtpClient, and scheme rejection / status
    // checking / URL redaction moved to WebhookClient, when this class and
    // AlertNotifier stopped carrying a private copy of each transport. See
    // SmtpClientTest, WebhookClientTest, and tests/System/*ConversationTest.
    //
    // The saved-channel tests below stand a SQLite file in for the channel
    // table (the query is plain SQL) and take "the database" down by removing
    // its directory, which makes the connect itself fail. Actual delivery from
    // the saved list, on the wire, is in tests/System/HealthAlertDeliveryTest.

    private ?string $dir = null;

    private string|false $previousErrorLog = false;

    protected function tearDown(): void
    {
        if ($this->previousErrorLog !== false) {
            ini_set('error_log', $this->previousErrorLog);
            $this->previousErrorLog = false;
        }

        if ($this->dir !== null) {
            self::removeTree($this->dir);
            $this->dir = null;
        }
    }

    public function test_dispatch_returns_early_on_empty_diagnoses(): void
    {
        $notifier = new HealthAlertNotifier('pgsql:host=127.0.0.1;port=1;dbname=x', 'u', 'p');

        // With empty diagnoses, loadChannels() should never be called, so no PG error.
        $notifier->dispatch([]);
        $notifier->dispatchRecovered([]);

        $this->assertTrue(true); // no exception
    }

    public function test_compute_interval_is_unused(): void
    {
        // Sanity: class can be instantiated with minimal args
        $notifier = new HealthAlertNotifier('dsn', 'u', 'p', 'AppName', 'host:1234');
        $this->assertInstanceOf(HealthAlertNotifier::class, $notifier);
    }

    // ─── Saved channel list ──────────────────────────────────────────

    /**
     * Every round runs in a fresh forked child, so the in-memory cache never
     * survived to the next round and an unreachable database meant no alert
     * at all. A successful load now leaves the list on disk, private.
     */
    public function test_a_successful_load_saves_the_channel_list_privately(): void
    {
        $db = $this->channelDb();
        $this->addChannel($db, 'ops-hook', 'webhook', ['url' => 'http://127.0.0.1:1/hook', 'secret' => 'shh']);

        // A stale file with loose permissions must be replaced, not reused.
        file_put_contents($this->sidecar(), 'garbage');
        chmod($this->sidecar(), 0644);

        $this->notifier()->dispatch([$this->diagnosis()]);

        $this->assertFileExists($this->sidecar());
        $this->assertSame(0600, fileperms($this->sidecar()) & 0777);
        $this->assertSame(['ops-hook'], $this->savedNames());
        $this->assertSame([], glob($this->dir.'/buffer/*.tmp'), 'the tmp file was left behind');
    }

    public function test_a_round_without_the_database_uses_the_saved_list(): void
    {
        $db = $this->channelDb();
        $this->addChannel($db, 'ops-hook', 'webhook', ['url' => 'http://127.0.0.1:1/hook']);
        $this->notifier()->dispatch([$this->diagnosis()]);

        $this->takeDatabaseDown();
        $log = $this->captureErrorLog();

        // A fresh instance, as each forked round is.
        $this->notifier()->dispatch([$this->diagnosis()]);

        $logged = (string) file_get_contents($log);
        $this->assertStringContainsString('Could not read alert channels from the database', $logged);
        $this->assertStringContainsString('Using the saved list at '.$this->sidecar().' (1 channel)', $logged);
        // The send itself was attempted: the channel came from the file.
        $this->assertStringContainsString('Health alert via webhook (ops-hook) failed', $logged);
    }

    /**
     * An outage can outlast the one-hour cache. The saved list has no expiry
     * of its own, and an instance whose cache has expired (the inline, no-fork
     * path) falls back to it too.
     */
    public function test_the_saved_list_outlives_the_cache_ttl(): void
    {
        $db = $this->channelDb();
        $this->addChannel($db, 'ops-hook', 'webhook', ['url' => 'http://127.0.0.1:1/hook']);

        $notifier = $this->notifier();
        $notifier->dispatch([$this->diagnosis()]);

        // Three days old, past the TTL, and the connection gone with the
        // database (SQLite keeps an open handle working across the rename).
        touch($this->sidecar(), time() - 3 * 86400);
        (new \ReflectionProperty(HealthAlertNotifier::class, 'channelCacheExpiry'))->setValue($notifier, 0.0);
        (new \ReflectionProperty(HealthAlertNotifier::class, 'pdo'))->setValue($notifier, null);
        $this->takeDatabaseDown();
        $log = $this->captureErrorLog();

        $notifier->dispatch([$this->diagnosis()]);

        $this->assertStringContainsString('Using the saved list at '.$this->sidecar().' (1 channel)', (string) file_get_contents($log));
    }

    public function test_a_channel_disabled_or_deleted_while_healthy_leaves_the_saved_list(): void
    {
        $db = $this->channelDb();
        $this->addChannel($db, 'keep', 'webhook', ['url' => 'http://127.0.0.1:1/a']);
        $this->addChannel($db, 'disable-me', 'webhook', ['url' => 'http://127.0.0.1:1/b']);
        $this->notifier()->dispatch([$this->diagnosis()]);
        $this->assertSame(['keep', 'disable-me'], $this->savedNames());

        $db->exec("UPDATE nightowl_alert_channels SET enabled = 0 WHERE name = 'disable-me'");
        $this->notifier()->dispatch([$this->diagnosis()]);
        $this->assertSame(['keep'], $this->savedNames());

        // An empty list is saved too; skipping it would leave 'keep' behind.
        $db->exec('DELETE FROM nightowl_alert_channels');
        $this->notifier()->dispatch([$this->diagnosis()]);
        $this->assertSame([], $this->savedNames());

        $this->takeDatabaseDown();
        $log = $this->captureErrorLog();
        $this->notifier()->dispatch([$this->diagnosis()]);

        $logged = (string) file_get_contents($log);
        $this->assertStringContainsString('(0 channels)', $logged);
        $this->assertStringNotContainsString('Health alert via webhook', $logged);
    }

    /** A list saved for one database must never route another's alerts. */
    public function test_a_saved_list_from_a_different_database_is_not_used(): void
    {
        $db = $this->channelDb();
        $this->addChannel($db, 'ops-hook', 'webhook', ['url' => 'http://127.0.0.1:1/hook']);
        $this->notifier()->dispatch([$this->diagnosis()]);

        $log = $this->captureErrorLog();
        $this->notifier('pgsql:host=127.0.0.1;port=1;dbname=other')->dispatch([$this->diagnosis()]);

        $logged = (string) file_get_contents($log);
        $this->assertStringContainsString('is for a different database, so it is not used', $logged);
        $this->assertStringNotContainsString('Health alert via webhook', $logged);
    }

    /** It used to return without a word. */
    public function test_an_unreachable_database_with_no_saved_list_is_logged(): void
    {
        $this->channelDb();
        $this->takeDatabaseDown();
        $log = $this->captureErrorLog();

        $this->notifier()->dispatch([$this->diagnosis()]);

        $this->assertStringContainsString('no saved list exists', (string) file_get_contents($log));
        $this->assertFileDoesNotExist($this->sidecar());
    }

    public function test_the_recovered_pass_in_the_same_round_does_not_retry_the_database(): void
    {
        $this->channelDb();
        $this->takeDatabaseDown();
        $log = $this->captureErrorLog();

        $notifier = $this->notifier();
        $notifier->dispatch([$this->diagnosis()]);
        $notifier->dispatchRecovered([$this->diagnosis('PG_LATENCY_CRITICAL')]);

        $this->assertSame(1, substr_count((string) file_get_contents($log), 'Could not read alert channels'));
    }

    // ─── Priming ─────────────────────────────────────────────────────

    /**
     * The list used to be saved only by an alert round that reached the
     * database. A healthy agent never runs one, so its first alert during an
     * outage had no list. AsyncServer now primes it while healthy.
     */
    public function test_a_prime_saves_the_list_without_any_alert_round(): void
    {
        $db = $this->channelDb();
        $this->addChannel($db, 'ops-hook', 'webhook', ['url' => 'http://127.0.0.1:1/hook', 'secret' => 'shh']);

        $this->assertTrue($this->notifier()->primeChannels());

        $this->assertSame(['ops-hook'], $this->savedNames());
        $this->assertSame(0600, fileperms($this->sidecar()) & 0777);

        // The first round of an outage now has somewhere to send.
        $this->takeDatabaseDown();
        $log = $this->captureErrorLog();
        $this->notifier()->dispatch([$this->diagnosis()]);

        $logged = (string) file_get_contents($log);
        $this->assertStringContainsString('Using the saved list at '.$this->sidecar().' (1 channel)', $logged);
        $this->assertStringContainsString('Health alert via webhook (ops-hook) failed', $logged);
    }

    public function test_a_prime_that_cannot_reach_the_database_keeps_the_saved_list(): void
    {
        $db = $this->channelDb();
        $this->addChannel($db, 'ops-hook', 'webhook', ['url' => 'http://127.0.0.1:1/hook']);
        $this->assertTrue($this->notifier()->primeChannels());
        $saved = (string) file_get_contents($this->sidecar());

        $this->takeDatabaseDown();
        $log = $this->captureErrorLog();

        $this->assertFalse($this->notifier()->primeChannels());

        $this->assertSame($saved, (string) file_get_contents($this->sidecar()));
        $this->assertStringContainsString('The last saved list is kept.', (string) file_get_contents($log));
    }

    public function test_a_prime_with_nowhere_to_save_does_not_connect(): void
    {
        $this->channelDb();
        $log = $this->captureErrorLog();

        // Unroutable: a connect attempt would take the whole connect bound.
        $notifier = new HealthAlertNotifier('pgsql:host=192.0.2.1;port=5432;dbname=x', 'u', 'p');

        $start = microtime(true);
        $this->assertFalse($notifier->primeChannels());
        $this->assertLessThan(1.0, microtime(true) - $start);
        $this->assertSame('', (string) file_get_contents($log));
    }

    // ─── Connection ──────────────────────────────────────────────────

    /**
     * PDO_PGSQL gives libpq a 30s connect_timeout by default, so a round
     * against an unreachable host waited 30s before falling back to the saved
     * list, and AsyncServer skipped every transition in the meantime.
     */
    public function test_an_unreachable_database_is_given_up_on_within_seconds(): void
    {
        $db = $this->channelDb();
        $this->addChannel($db, 'ops-hook', 'webhook', ['url' => 'http://127.0.0.1:1/hook']);
        $this->assertTrue($this->notifier()->primeChannels());
        $log = $this->captureErrorLog();

        // 192.0.2.1 is TEST-NET-1: unroutable, so the connect hangs until the
        // bound rather than failing fast.
        $start = microtime(true);
        $this->notifier('pgsql:host=192.0.2.1;port=5432;dbname=x')->dispatch([$this->diagnosis()]);
        $elapsed = microtime(true) - $start;

        $this->assertLessThan(10.0, $elapsed, 'the connect is still bounded by PDO\'s 30s default');
        $this->assertStringContainsString('timeout expired', (string) file_get_contents($log));
    }

    /**
     * The notifier connected without an sslmode, so libpq used its default
     * whatever NIGHTOWL_DB_SSLMODE said. A fake server records whether the
     * client opened with an SSLRequest, which libpq skips only for 'disable'
     * and which is all 'require' sends before it gives up on a server
     * without SSL.
     */
    public function test_from_config_connects_with_the_configured_sslmode(): void
    {
        $this->channelDb();

        $this->assertSame(['STARTUP'], $this->openingPackets('disable'));
        $this->assertSame(['SSL'], $this->openingPackets('require'));
    }

    /**
     * Unset, NIGHTOWL_DB_SSLMODE leaves the mode to libpq, so a PGSSLMODE
     * environment variable applies, as it did before the notifier took the
     * setting. 'database.sslmode' reads 'prefer' then, which is also what
     * passing it would send, overriding PGSSLMODE.
     */
    public function test_from_config_leaves_the_mode_to_pgsslmode_when_none_is_set(): void
    {
        $this->channelDb();
        $previous = getenv('PGSSLMODE');

        try {
            putenv('PGSSLMODE=require');
            $this->assertSame(['SSL'], $this->openingPackets('prefer'));

            putenv('PGSSLMODE=disable');
            $this->assertSame(['STARTUP'], $this->openingPackets('prefer'));

            // Set explicitly, 'prefer' wins over PGSSLMODE like any other mode.
            putenv('PGSSLMODE=require');
            $this->assertSame(['SSL', 'STARTUP'], $this->openingPackets('prefer', explicit: 'prefer'));
        } finally {
            putenv($previous === false ? 'PGSSLMODE' : "PGSSLMODE={$previous}");
        }
    }

    /**
     * @param  string|null  $explicit  NIGHTOWL_DB_SSLMODE as the top-level
     *                                 'explicit_sslmode' key reads it
     * @return list<string>
     */
    private function openingPackets(string $sslmode, ?string $explicit = null): array
    {
        $server = ScriptedServer::start('fake-pg-startup.php');

        try {
            $previous = Container::getInstance();
            $container = new Container;
            $container->instance('config', new Repository(['nightowl' => [
                'agent' => ['sqlite_path' => $this->dir.'/buffer/agent-buffer.sqlite'],
                'explicit_sslmode' => $explicit,
                'database' => [
                    'host' => '127.0.0.1',
                    'port' => $server->port,
                    'database' => 'x',
                    'username' => 'u',
                    'password' => 'p',
                    'sslmode' => $sslmode,
                ],
            ]]));
            Container::setInstance($container);

            try {
                $notifier = HealthAlertNotifier::fromConfig('web-1:2407');
            } finally {
                Container::setInstance($previous);
            }

            $this->assertFalse($notifier->primeChannels());

            return array_values(array_filter($server->transcript(), static fn (string $line): bool => $line !== 'GSSENC'));
        } finally {
            $server->cleanup();
        }
    }

    // ─── fromConfig ──────────────────────────────────────────────────

    /**
     * The link and the saved list come from the same config the issue alerts
     * and the buffer use. Built under a bare container, as the agent harness
     * runs it, which has no storage_path().
     */
    public function test_from_config_derives_the_link_and_the_saved_list_path(): void
    {
        $notifier = $this->fromConfig([
            'sqlite_path' => '/var/nightowl/agent-buffer.sqlite',
            'dashboard_url' => 'https://dash.example',
            'app_id' => 'app_123',
        ]);

        $this->assertSame('/var/nightowl/agent-buffer.sqlite.alert-channels.json', $this->property($notifier, 'channelsFile'));
        $this->assertSame('https://dash.example', $this->property($notifier, 'dashboardUrl'));
        $this->assertSame('app_123', $this->property($notifier, 'appId'));
        $this->assertSame(
            'https://dash.example/dashboard/app_123/health',
            (new \ReflectionMethod(HealthAlertNotifier::class, 'healthUrl'))->invoke($notifier),
        );
    }

    public function test_from_config_without_a_buffer_path_saves_nothing_and_does_not_throw(): void
    {
        $notifier = $this->fromConfig(['dashboard_url' => '', 'app_id' => null]);

        $this->assertNull($this->property($notifier, 'channelsFile'));
        $this->assertNull((new \ReflectionMethod(HealthAlertNotifier::class, 'healthUrl'))->invoke($notifier));
    }

    private function fromConfig(array $agent): HealthAlertNotifier
    {
        $previous = Container::getInstance();
        $container = new Container;
        $container->instance('config', new Repository(['nightowl' => ['agent' => $agent]]));
        Container::setInstance($container);

        try {
            return HealthAlertNotifier::fromConfig('web-1:2407');
        } finally {
            Container::setInstance($previous);
        }
    }

    private function property(HealthAlertNotifier $notifier, string $name): mixed
    {
        return (new \ReflectionProperty(HealthAlertNotifier::class, $name))->getValue($notifier);
    }

    // ─── Helpers ─────────────────────────────────────────────────────

    private function channelDb(): PDO
    {
        $this->dir = sys_get_temp_dir().'/nightowl-health-alert-'.getmypid().'-'.bin2hex(random_bytes(4));
        mkdir($this->dir.'/db', 0777, true);
        mkdir($this->dir.'/buffer', 0777, true);

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

        // Keeps the failed-send lines of every round out of the test output.
        $this->captureErrorLog();

        return $db;
    }

    private function addChannel(PDO $db, string $name, string $type, array $config): void
    {
        $db->prepare("INSERT INTO nightowl_alert_channels (name, type, config, enabled, updated_at) VALUES (?, ?, ?, 1, datetime('now'))")
            ->execute([$name, $type, json_encode($config)]);
    }

    private function dsn(): string
    {
        return 'sqlite:'.$this->dir.'/db/channels.sqlite';
    }

    private function sidecar(): string
    {
        return $this->dir.'/buffer/agent-buffer.sqlite.alert-channels.json';
    }

    private function notifier(?string $dsn = null): HealthAlertNotifier
    {
        return new HealthAlertNotifier(
            $dsn ?? $this->dsn(),
            '',
            '',
            'AcmeApp',
            'web-1:2407',
            null,
            new WebhookClient(1.0),
            'https://dash.example',
            'app_123',
            $this->sidecar(),
        );
    }

    /** The directory goes, so PDO cannot even open the file. */
    private function takeDatabaseDown(): void
    {
        rename($this->dir.'/db', $this->dir.'/db-gone');
    }

    /** @return list<string> */
    private function savedNames(): array
    {
        $saved = json_decode((string) file_get_contents($this->sidecar()), true);

        return array_column($saved['channels'] ?? [], 'name');
    }

    private function captureErrorLog(): string
    {
        $log = $this->dir.'/error.log';
        file_put_contents($log, '');

        if ($this->previousErrorLog === false) {
            $this->previousErrorLog = (string) ini_get('error_log');
        }
        ini_set('error_log', $log);

        return $log;
    }

    private function diagnosis(string $code = 'DRAIN_STOPPED'): array
    {
        return [
            'code' => $code,
            'level' => 'critical',
            'message' => 'Drain has not written for 120s',
            'recommendation' => 'Check PostgreSQL connectivity',
            'value' => 120,
        ];
    }

    private static function removeTree(string $path): void
    {
        if (is_dir($path) && ! is_link($path)) {
            foreach (scandir($path) ?: [] as $entry) {
                if ($entry !== '.' && $entry !== '..') {
                    self::removeTree($path.'/'.$entry);
                }
            }
            @rmdir($path);

            return;
        }

        @unlink($path);
    }
}
