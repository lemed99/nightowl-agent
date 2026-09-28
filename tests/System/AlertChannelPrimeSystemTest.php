<?php

namespace NightOwl\Tests\System;

use NightOwl\Tests\Integration\MigrationRunner;
use NightOwl\Tests\System\Concerns\SystemEnvironment;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * A running agent saves its alert-channel list without ever sending an alert.
 *
 * Health alerts fall back to the saved list (<buffer>.alert-channels.json)
 * when the database is down, and the list used to be written only by an
 * alert round that reached the database. A healthy agent never runs one, so
 * the first alert of its first outage was lost. The agent now refreshes the
 * list from a forked child a few seconds after it starts.
 *
 * A real AsyncServer in a subprocess, stub drain workers, the channel table in
 * the test PostgreSQL. No diagnosis ever fires here.
 */
final class AlertChannelPrimeSystemTest extends TestCase
{
    private static ?PDO $pdo = null;

    private static ?\Throwable $pdoError = null;

    private static string $host;

    private static int $port;

    private static string $database;

    private static string $username;

    private static string $password;

    private string $workDir;

    private string $channelName;

    /** @var resource|null */
    private $proc = null;

    /** @var array<int, resource> */
    private array $pipes = [];

    private string $output = '';

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
            self::$pdoError = $e;
        }

        if (self::$pdo) {
            MigrationRunner::migrate(self::$host, self::$port, self::$database, self::$username, self::$password);
        }
    }

    protected function setUp(): void
    {
        if (self::$pdo === null) {
            SystemEnvironment::postgresUnavailable(self::$pdoError ?? new \RuntimeException('no connection attempt recorded'));
        }

        foreach (['pcntl_fork', 'posix_kill', 'pcntl_exec'] as $fn) {
            if (! function_exists($fn)) {
                $this->markTestSkipped("{$fn} required to run the agent.");
            }
        }

        $this->workDir = sys_get_temp_dir().'/nightowl-channel-prime-'.getmypid().'-'.uniqid();
        mkdir($this->workDir, 0755, true);

        $this->channelName = 'prime-test-'.bin2hex(random_bytes(4));
        self::$pdo->prepare(
            "INSERT INTO nightowl_alert_channels (name, type, config, enabled, created_at, updated_at)
             VALUES (?, 'webhook', ?, true, now(), now())"
        )->execute([$this->channelName, json_encode(['url' => 'http://127.0.0.1:1/hook'])]);
    }

    protected function tearDown(): void
    {
        if (is_resource($this->proc)) {
            $status = proc_get_status($this->proc);
            if ($status['running'] ?? false) {
                @posix_kill($status['pid'], SIGTERM);
                $deadline = microtime(true) + 5.0;
                while ((proc_get_status($this->proc)['running'] ?? false) && microtime(true) < $deadline) {
                    usleep(50_000);
                }
                if (proc_get_status($this->proc)['running'] ?? false) {
                    @posix_kill($status['pid'], SIGKILL);
                }
            }
            foreach ($this->pipes as $pipe) {
                if (is_resource($pipe)) {
                    @fclose($pipe);
                }
            }
            @proc_close($this->proc);
        }

        foreach (glob($this->workDir.'/worker-*.pid') ?: [] as $marker) {
            $pid = (int) trim((string) @file_get_contents($marker));
            if ($pid > 0) {
                @posix_kill($pid, SIGKILL);
            }
        }

        self::$pdo?->prepare('DELETE FROM nightowl_alert_channels WHERE name = ?')->execute([$this->channelName]);

        foreach (glob($this->workDir.'/*') ?: [] as $file) {
            @unlink($file);
        }
        @rmdir($this->workDir);
    }

    public function test_a_healthy_agent_saves_its_alert_channels_soon_after_starting(): void
    {
        $sidecar = $this->workDir.'/buffer.sqlite.alert-channels.json';

        $this->startAgent();

        $saved = $this->waitUntil(function () use ($sidecar): bool {
            $list = json_decode((string) @file_get_contents($sidecar), true);

            return in_array($this->channelName, array_column($list['channels'] ?? [], 'name'), true);
        }, 20.0);

        $this->assertTrue($saved, "the channel list was not saved after startup.\n".$this->agentOutput());
        $this->assertSame(0600, fileperms($sidecar) & 0777);
        $this->assertTrue($this->agentRunning(), "the agent stopped.\n".$this->agentOutput());
    }

    // ─── Harness ─────────────────────────────────────────────────────

    private function startAgent(): void
    {
        $autoload = var_export(realpath(__DIR__.'/../../vendor/autoload.php'), true);
        $sqlite = var_export($this->workDir.'/buffer.sqlite', true);
        $markerPrefix = var_export($this->workDir.'/worker-', true);
        $config = var_export([
            'nightowl' => [
                'agent' => ['sqlite_path' => $this->workDir.'/buffer.sqlite'],
                'database' => [
                    'host' => self::$host,
                    'port' => self::$port,
                    'database' => self::$database,
                    'username' => self::$username,
                    'password' => self::$password,
                ],
            ],
        ], true);
        $ingestPort = $this->freePort();

        $script = <<<PHP
        <?php
        require {$autoload};

        use Illuminate\\Config\\Repository;
        use Illuminate\\Container\\Container;
        use NightOwl\\Agent\\AsyncServer;
        use NightOwl\\Agent\\DrainWorker;
        use NightOwl\\Agent\\PayloadParser;

        \$container = new Container();
        \$container->instance('config', new Repository({$config}));
        Container::setInstance(\$container);

        // Stub workers: record a pid and sleep, so nothing drains.
        \$spawner = function (int \$workerId, int \$totalWorkers, string \$sqlitePath) {
            \$marker = {$markerPrefix} . \$workerId . '.pid';
            pcntl_exec(PHP_BINARY, ['-r', 'file_put_contents(' . var_export(\$marker, true) . ', getmypid()); usleep(60000000);']);
            exit(1);
        };

        \$server = new AsyncServer(
            parser: new PayloadParser(),
            sqlitePath: {$sqlite},
            drainWorker: new DrainWorker(
                sqlitePath: {$sqlite},
                pgHost: '127.0.0.1',
                pgPort: 1,
                pgDatabase: 'x',
                pgUsername: 'x',
                pgPassword: 'x',
            ),
            healthEnabled: false,
            healthReportEnabled: false,
            drainSpawner: \$spawner,
        );

        \$server->listen('127.0.0.1', {$ingestPort});
        PHP;

        $harness = $this->workDir.'/agent-harness.php';
        file_put_contents($harness, $script);

        $cmd = sprintf('exec %s %s 2>&1', escapeshellarg(PHP_BINARY), escapeshellarg($harness));
        $this->proc = proc_open($cmd, [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $this->pipes);

        $this->assertIsResource($this->proc, 'Failed to start the agent harness.');
        stream_set_blocking($this->pipes[1], false);
    }

    private function agentOutput(): string
    {
        if (isset($this->pipes[1]) && is_resource($this->pipes[1])) {
            $this->output .= (string) stream_get_contents($this->pipes[1]);
        }

        return $this->output;
    }

    private function agentRunning(): bool
    {
        return is_resource($this->proc) && (proc_get_status($this->proc)['running'] ?? false);
    }

    private function waitUntil(callable $condition, float $seconds): bool
    {
        $deadline = microtime(true) + $seconds;

        while (microtime(true) < $deadline) {
            if ($condition()) {
                return true;
            }
            usleep(100_000);
        }

        return (bool) $condition();
    }

    private function freePort(): int
    {
        $sock = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        if (! $sock) {
            $this->markTestSkipped("Could not allocate a free port: {$errstr}");
        }
        $name = (string) stream_socket_get_name($sock, false);
        fclose($sock);

        return (int) substr($name, strrpos($name, ':') + 1);
    }
}
