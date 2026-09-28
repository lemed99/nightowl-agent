<?php

namespace NightOwl\Tests\System;

use NightOwl\Agent\SqliteBuffer;
use PHPUnit\Framework\TestCase;

/**
 * Drain workers must never outlive the agent that forked them.
 *
 * An orphaned worker keeps draining the buffer, and when a supervisor starts a
 * replacement agent its workers read the same rows: each row can reach Postgres
 * twice. Three ways an agent used to leave workers behind are covered here:
 *
 * - a health port clash (a second agent on a host, both on 2409), which threw
 *   after the workers were forked;
 * - any other error after the fork, such as an exception out of a loop timer;
 * - the agent being killed outright (SIGKILL, the OOM killer), which no code in
 *   the agent can react to, so the worker has to notice on its own.
 *
 * A worker that dies that way can leave rows claimed, and the next agent has to
 * free them.
 *
 * Each test boots a real AsyncServer in a subprocess with two workers. No
 * PostgreSQL: the workers never write.
 */
final class AgentProcessLifecycleSystemTest extends TestCase
{
    private const WORKERS = 2;

    private string $workDir;

    private string $sqlitePath;

    /** @var resource|null */
    private $proc = null;

    /** @var array<int, resource> */
    private array $pipes = [];

    /** @var resource|null a socket the test holds to occupy a port */
    private $occupied = null;

    protected function setUp(): void
    {
        foreach (['pcntl_fork', 'posix_kill', 'pcntl_exec', 'posix_getppid'] as $fn) {
            if (! function_exists($fn)) {
                $this->markTestSkipped("{$fn} required for the agent lifecycle system test.");
            }
        }

        $this->workDir = sys_get_temp_dir().'/nightowl-lifecycle-'.getmypid().'-'.uniqid();
        mkdir($this->workDir, 0755, true);
        $this->sqlitePath = $this->workDir.'/buffer.sqlite';
    }

    protected function tearDown(): void
    {
        // Both kinds, so a failing run does not leave the orphans it found.
        foreach ([...$this->workerPids(), ...$this->forkedWorkerPids()] as $pid) {
            @posix_kill($pid, SIGKILL);
        }

        if (is_resource($this->proc)) {
            $status = proc_get_status($this->proc);
            if ($status['running'] ?? false) {
                @posix_kill($status['pid'], SIGKILL);
            }
            foreach ($this->pipes as $pipe) {
                if (is_resource($pipe)) {
                    @fclose($pipe);
                }
            }
            @proc_close($this->proc);
        }

        if (is_resource($this->occupied)) {
            fclose($this->occupied);
        }

        foreach (glob($this->workDir.'/*') ?: [] as $f) {
            @unlink($f);
        }
        @rmdir($this->workDir);
    }

    public function test_a_health_port_clash_fails_before_any_drain_worker_is_forked(): void
    {
        $this->occupied = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        $this->assertIsResource($this->occupied, "could not occupy a port: {$errstr}");
        $healthPort = $this->portOf($this->occupied);

        $this->startAgent($this->harnessScript(healthPort: $healthPort, stubWorkers: true));

        $exited = $this->waitUntil(fn (): bool => ! $this->agentRunning(), 10.0);
        $output = $this->agentOutput();

        $this->assertTrue($exited, "the agent must stop when its health port is taken.\n{$output}");
        $this->assertStringContainsString('PortInUseException', $output, "a port clash must be reported as one.\n{$output}");
        $this->assertStringContainsString("health port 127.0.0.1:{$healthPort} is already in use", $output);
        $this->assertStringContainsString('NIGHTOWL_HEALTH_PORT', $output);
        $this->assertStringContainsString('NIGHTOWL_HEALTH_ENABLED=false', $output);

        // Give a worker forked in error the time the stub needs to announce itself.
        usleep(500_000);
        $this->assertSame(
            [],
            $this->workerPids(),
            "no drain worker may be started before the health port is bound: these were left running.\n{$output}",
        );
    }

    public function test_an_error_after_the_fork_stops_the_drain_workers(): void
    {
        $this->startAgent($this->harnessScript(healthPort: null, stubWorkers: true, throwAfter: 1.5));

        $this->assertTrue(
            $this->waitUntil(fn (): bool => count($this->workerPids()) === self::WORKERS, 5.0),
            "both drain workers must start.\n".$this->agentOutput(),
        );
        $workers = $this->workerPids();

        $exited = $this->waitUntil(fn (): bool => ! $this->agentRunning(), 15.0);
        $output = $this->agentOutput();

        $this->assertTrue($exited, "the agent must exit on the error.\n{$output}");
        $this->assertStringContainsString('boom from a timer', $output);
        $this->assertStringContainsString('Stopping after an unexpected error', $output);
        $this->assertSame(
            [],
            array_values(array_filter($workers, fn (int $pid): bool => $this->alive($pid))),
            "the agent exited and left its drain workers running.\n{$output}",
        );
    }

    public function test_a_drain_worker_stops_when_its_agent_is_killed(): void
    {
        // In-process drain workers: the real DrainWorker::run() loop, which is
        // where the parent check lives, on both the in-process and exec paths.
        $this->startAgent($this->harnessScript(healthPort: null, stubWorkers: false));

        $this->assertTrue(
            $this->waitUntil(fn (): bool => count($this->forkedWorkerPids()) === self::WORKERS && $this->heartbeatsWritten(), 10.0),
            "both drain workers must start.\n".$this->agentOutput(),
        );
        $workers = $this->forkedWorkerPids();

        // Nothing in the agent runs on SIGKILL.
        posix_kill(proc_get_status($this->proc)['pid'], SIGKILL);

        $stopped = $this->waitUntil(
            fn (): bool => array_filter($workers, fn (int $pid): bool => $this->alive($pid)) === [],
            10.0,
        );

        $this->assertTrue(
            $stopped,
            "drain workers kept running after their agent was killed.\n".$this->agentOutput(),
        );
        $this->assertStringContainsString('is gone', $this->agentOutput());
    }

    public function test_a_new_agent_frees_the_rows_a_dead_agents_workers_had_claimed(): void
    {
        // Claimed by worker #5, as by an agent that ran more workers than this
        // one. No worker here uses that id, and fetchPending() skips claimed
        // rows, so without the release at startup they would never drain.
        $buffer = new SqliteBuffer($this->sqlitePath);
        for ($i = 0; $i < 6; $i++) {
            $buffer->appendRaw((string) json_encode([['t' => 'log', 'i' => $i]]));
        }
        $buffer->claimBatch(5, 4);
        unset($buffer);

        // Stub workers never touch the buffer, so what it holds afterwards is
        // the agent's doing alone.
        $this->startAgent($this->harnessScript(healthPort: null, stubWorkers: true));

        $this->assertTrue(
            $this->waitUntil(fn (): bool => count($this->workerPids()) === self::WORKERS, 5.0),
            "both drain workers must start.\n".$this->agentOutput(),
        );

        $buffer = new SqliteBuffer($this->sqlitePath);
        $pending = $buffer->pendingCount();
        unset($buffer);

        $this->assertSame(6, $pending, "the stale claims must be back in the queue.\n".$this->agentOutput());
        $this->assertStringContainsString('Released 4 buffer rows', $this->agentOutput());
    }

    // ─── Harness ───────────────────────────────────────────────

    private function startAgent(string $script): void
    {
        $harness = $this->workDir.'/agent-harness.php';
        file_put_contents($harness, $script);

        $cmd = sprintf('exec %s %s 2>&1', escapeshellarg(PHP_BINARY), escapeshellarg($harness));
        $this->proc = proc_open($cmd, [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $this->pipes);

        $this->assertIsResource($this->proc, 'Failed to start the agent harness.');
        stream_set_blocking($this->pipes[1], false);
    }

    /**
     * A real AsyncServer on an unreachable Postgres. With $stubWorkers each
     * worker is exec'd as a stub that records its pid and sleeps (it has no
     * parent check of its own, so only the agent can stop it); without, the
     * workers run DrainWorker::run() in-process.
     */
    private function harnessScript(?int $healthPort, bool $stubWorkers, ?float $throwAfter = null): string
    {
        $autoload = var_export(realpath(__DIR__.'/../../vendor/autoload.php'), true);
        $sqlite = var_export($this->sqlitePath, true);
        $markerPrefix = var_export($this->workDir.'/worker-', true);
        $ingestPort = $this->freePort();
        $healthEnabled = $healthPort !== null ? 'true' : 'false';
        $healthPortArg = $healthPort ?? 2409;
        $workers = self::WORKERS;

        $spawner = $stubWorkers ? <<<PHP
            function (int \$workerId, int \$totalWorkers, string \$sqlitePath) {
                \$marker = {$markerPrefix} . \$workerId . '.pid';
                \$code = 'file_put_contents(' . var_export(\$marker . '.tmp', true) . ', getmypid()); '
                    . 'rename(' . var_export(\$marker . '.tmp', true) . ', ' . var_export(\$marker, true) . '); usleep(30000000);';
                pcntl_exec(PHP_BINARY, ['-r', \$code]);
                exit(1);
            }
            PHP : 'null';

        $throw = $throwAfter === null ? '' : <<<PHP
            React\\EventLoop\\Loop::get()->addTimer({$throwAfter}, function () {
                throw new RuntimeException('boom from a timer');
            });
            PHP;

        return <<<PHP
        <?php
        require {$autoload};

        use Illuminate\\Config\\Repository;
        use Illuminate\\Container\\Container;
        use NightOwl\\Agent\\AsyncServer;
        use NightOwl\\Agent\\DrainWorker;
        use NightOwl\\Agent\\PayloadParser;

        // DrainWorker::run() reads config(); every key falls to its default.
        \$container = new Container();
        \$container->instance('config', new Repository([]));
        Container::setInstance(\$container);

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
            healthEnabled: {$healthEnabled},
            healthPort: {$healthPortArg},
            healthReportEnabled: false,
            drainWorkerCount: {$workers},
            drainSpawner: {$spawner},
        );

        {$throw}

        \$server->listen('127.0.0.1', {$ingestPort});
        PHP;
    }

    /** Pids the exec'd stub workers recorded. */
    private function workerPids(): array
    {
        if (! isset($this->workDir)) {
            return [];
        }

        $pids = [];
        foreach (glob($this->workDir.'/worker-*.pid') ?: [] as $marker) {
            $pid = (int) trim((string) @file_get_contents($marker));
            if ($pid > 0) {
                $pids[] = $pid;
            }
        }

        return $pids;
    }

    /** Pids the agent logged for its in-process workers. */
    private function forkedWorkerPids(): array
    {
        preg_match_all('/Drain worker #\d+ forked \(pid: (\d+)\)/', $this->agentOutput(), $matches);

        return array_values(array_unique(array_map('intval', $matches[1])));
    }

    private function heartbeatsWritten(): bool
    {
        for ($i = 0; $i < self::WORKERS; $i++) {
            if (! is_file($this->sqlitePath.".drain-alive-{$i}.json")) {
                return false;
            }
        }

        return true;
    }

    private string $output = '';

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

    private function alive(int $pid): bool
    {
        return posix_kill($pid, 0);
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

    /** @param resource $socket */
    private function portOf($socket): int
    {
        $name = (string) stream_socket_get_name($socket, false);

        return (int) substr($name, strrpos($name, ':') + 1);
    }

    private function freePort(): int
    {
        $sock = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        if (! $sock) {
            $this->markTestSkipped("Could not allocate a free port: {$errstr}");
        }
        $port = $this->portOf($sock);
        fclose($sock);

        return $port;
    }
}
