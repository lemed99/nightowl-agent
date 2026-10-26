<?php

namespace NightOwl\Tests\System;

use NightOwl\Agent\BufferRecovery;
use NightOwl\Tests\Support\CorruptBufferFixture;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * A running agent repairs its own corrupt buffer with its drain workers live.
 *
 * The real AsyncServer boots on a buffer whose index is broken the way
 * Yomoney's was on 2026-10-21. Its two forked drain workers fail with SQLite
 * error 11 on their first claim; the parent reads that from their metrics
 * files, stops them, rebuilds the index and starts them again, all without
 * restarting itself. Then a SIGTERM shuts it down cleanly.
 *
 * No PostgreSQL required: the workers point at a closed port, and the failure
 * under test happens in the buffer before they ever reach it.
 */
class BufferRecoverySystemTest extends TestCase
{
    private string $workDir;

    private string $sqlitePath;

    /** @var resource|null */
    private $proc = null;

    /** @var array<int, resource> */
    private array $pipes = [];

    private string $output = '';

    protected function setUp(): void
    {
        foreach (['pcntl_fork', 'posix_kill'] as $fn) {
            if (! function_exists($fn)) {
                $this->markTestSkipped("{$fn} required for the buffer recovery system test.");
            }
        }

        $this->workDir = sys_get_temp_dir().'/nightowl-recovery-sys-'.getmypid().'-'.uniqid();
        mkdir($this->workDir, 0755, true);
        $this->sqlitePath = $this->workDir.'/agent-buffer.sqlite';
    }

    protected function tearDown(): void
    {
        if (is_resource($this->proc)) {
            $status = proc_get_status($this->proc);
            if ($status['running'] ?? false) {
                // The whole process group: the agent and any drain worker it left.
                @posix_kill(-$status['pid'], SIGKILL);
                @posix_kill($status['pid'], SIGKILL);
            }
            foreach ($this->pipes as $pipe) {
                if (is_resource($pipe)) {
                    @fclose($pipe);
                }
            }
            @proc_close($this->proc);
        }

        foreach (glob($this->workDir.'/*') ?: [] as $f) {
            @unlink($f);
        }
        @rmdir($this->workDir);
    }

    public function test_a_running_agent_reindexes_its_buffer_and_restarts_its_workers(): void
    {
        CorruptBufferFixture::indexOnly($this->sqlitePath);
        $this->assertContains('wrong # of entries in index idx_buffer_synced_id', BufferRecovery::integrityProblems($this->sqlitePath));

        $harness = $this->workDir.'/harness.php';
        file_put_contents($harness, $this->harnessScript($this->freePort()));
        $this->proc = proc_open(
            sprintf('exec %s %s 2>&1', escapeshellarg(PHP_BINARY), escapeshellarg($harness)),
            [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $this->pipes,
        );
        $this->assertIsResource($this->proc);
        stream_set_blocking($this->pipes[1], false);

        // Metrics are read every 5s and recovery runs on the 10s tick.
        $this->waitForOutput('Rebuilt the buffer\'s indexes', 40.0);
        // Both workers come back after the rebuild.
        $this->waitFor(fn () => substr_count($this->afterRebuild(), 'forked (pid') >= 2, 10.0,
            'the drain workers were not restarted after the rebuild');

        $this->assertTrue(proc_get_status($this->proc)['running'], "the agent itself must keep running.\n{$this->output}");
        $this->assertStringContainsString('Stopping the drain to check it', $this->output);

        // The file is whole again and every waiting row is still in it.
        $this->assertSame([], BufferRecovery::integrityProblems($this->sqlitePath));
        $pdo = new PDO('sqlite:'.$this->sqlitePath);
        $waiting = (int) $pdo->query('SELECT COUNT(*) FROM buffer WHERE synced = 0 OR synced >= 100')->fetchColumn();
        $this->assertSame(180, $waiting);
        unset($pdo);

        // The restarted workers get past the buffer: their next failure is the
        // closed Postgres port, not the index.
        $this->waitForOutput('port 1 failed', 15.0);
        $this->assertSame(1, substr_count($this->output, 'Rebuilt the buffer\'s indexes'), 'one recovery, not a loop');

        // SIGTERM stops the agent and its workers through the shared stop path.
        $pid = proc_get_status($this->proc)['pid'];
        posix_kill($pid, SIGTERM);
        $this->waitFor(fn () => ! proc_get_status($this->proc)['running'], 15.0, 'the agent did not exit on SIGTERM');
    }

    private function afterRebuild(): string
    {
        $at = strpos($this->output, 'Rebuilt the buffer\'s indexes');

        return $at === false ? '' : substr($this->output, $at);
    }

    private function waitForOutput(string $needle, float $seconds): void
    {
        $this->waitFor(fn () => str_contains($this->output, $needle), $seconds, "never logged \"{$needle}\"");
    }

    private function waitFor(callable $condition, float $seconds, string $message): void
    {
        $deadline = microtime(true) + $seconds;
        while (microtime(true) < $deadline) {
            $this->output .= stream_get_contents($this->pipes[1]) ?: '';
            if ($condition()) {
                return;
            }
            usleep(100_000);
        }
        $this->output .= stream_get_contents($this->pipes[1]) ?: '';
        if (! $condition()) {
            $this->fail("{$message}. Agent output:\n{$this->output}");
        }
    }

    private function harnessScript(int $port): string
    {
        $autoload = var_export(realpath(__DIR__.'/../../vendor/autoload.php'), true);
        $sqlite = var_export($this->sqlitePath, true);

        return <<<PHP
        <?php
        require {$autoload};

        use Illuminate\\Config\\Repository;
        use Illuminate\\Container\\Container;
        use NightOwl\\Agent\\AsyncServer;
        use NightOwl\\Agent\\DrainWorker;
        use NightOwl\\Agent\\PayloadParser;

        // listen() reads config() once its workers are forked.
        \$container = new Container();
        \$container->instance('config', new Repository([]));
        Container::setInstance(\$container);

        // As the real command does: SIGCHLD runs the reaper as soon as it
        // arrives, including in the middle of a recovery.
        pcntl_async_signals(true);

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
            // Two workers claim with an UPDATE, which is what trips over the
            // broken index (a lone worker reads with a SELECT).
            drainWorkerCount: 2,
        );

        \$server->listen('127.0.0.1', {$port});
        PHP;
    }

    private function freePort(): int
    {
        $sock = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        if (! $sock) {
            $this->markTestSkipped("Could not allocate a free port: {$errstr}");
        }
        $name = stream_socket_get_name($sock, false);
        fclose($sock);

        return (int) substr($name, strrpos($name, ':') + 1);
    }
}
