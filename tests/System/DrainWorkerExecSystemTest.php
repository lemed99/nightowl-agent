<?php

namespace NightOwl\Tests\System;

use PHPUnit\Framework\TestCase;

/**
 * Proves the drain worker takes the exec path, not the in-process fork.
 *
 * When AsyncServer is given a drainSpawner, the forked child must pcntl_exec()
 * a fresh interpreter (a clean process isolated from the parent agent's
 * long-lived ReactPHP state) instead of cloning the DrainWorker and running the
 * drain loop in-process.
 *
 * We boot a real AsyncServer in a subprocess with a spawner that execs a tiny
 * stub which writes its PID to a marker file. The marker can ONLY appear via the
 * spawner→exec branch: the in-process fallback would instead call
 * DrainWorker::run(), which connects to the (deliberately unreachable) Postgres
 * and never writes the marker. So marker-present == exec path exercised.
 *
 * No PostgreSQL required — the worker is forked at listen() time, before any
 * payload is ingested or drained.
 */
class DrainWorkerExecSystemTest extends TestCase
{
    private string $workDir;
    private string $markerPath;
    private string $harnessPath;
    private string $sqlitePath;

    /** @var resource|null */
    private $proc = null;
    /** @var array<int, resource> */
    private array $pipes = [];

    protected function setUp(): void
    {
        foreach (['pcntl_fork', 'posix_kill', 'pcntl_exec'] as $fn) {
            if (! function_exists($fn)) {
                $this->markTestSkipped("{$fn} required for the exec-path system test.");
            }
        }

        $this->workDir = sys_get_temp_dir().'/nightowl-exec-'.getmypid().'-'.uniqid();
        mkdir($this->workDir, 0755, true);
        $this->markerPath = $this->workDir.'/worker-exec.marker';
        $this->harnessPath = $this->workDir.'/exec-harness.php';
        $this->sqlitePath = $this->workDir.'/buffer.sqlite';
    }

    protected function tearDown(): void
    {
        // Kill the execed stub child first (it sleeps), then the agent process.
        if (is_file($this->markerPath)) {
            $stubPid = (int) trim((string) @file_get_contents($this->markerPath));
            if ($stubPid > 0) {
                @posix_kill($stubPid, SIGKILL);
            }
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

        foreach (glob($this->workDir.'/*') ?: [] as $f) {
            @unlink($f);
        }
        @rmdir($this->workDir);
    }

    public function testForkedWorkerExecsFreshProcessViaSpawner(): void
    {
        $port = $this->freePort();
        file_put_contents($this->harnessPath, $this->harnessScript($port));

        $cmd = sprintf('exec %s %s 2>&1', escapeshellarg(PHP_BINARY), escapeshellarg($this->harnessPath));
        $descriptors = [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']];
        $this->proc = proc_open($cmd, $descriptors, $this->pipes);

        $this->assertIsResource($this->proc, 'Failed to start exec-path harness.');
        stream_set_blocking($this->pipes[1], false);

        // Poll for the marker — the execed stub writes it within a second of boot.
        $deadline = microtime(true) + 5.0;
        while (microtime(true) < $deadline && ! is_file($this->markerPath)) {
            usleep(100_000);
        }

        $output = stream_get_contents($this->pipes[1]) ?: '';

        $this->assertFileDoesNotExist(
            $this->markerPath.'.execfail',
            'Spawner reported pcntl_exec failure: '.@file_get_contents($this->markerPath.'.execfail'),
        );
        $this->assertFileExists(
            $this->markerPath,
            "Forked worker did not exec via the spawner. Harness output:\n{$output}",
        );

        $stubPid = (int) trim((string) file_get_contents($this->markerPath));
        $this->assertGreaterThan(0, $stubPid, 'Marker did not contain a valid stub PID.');

        $agentPid = proc_get_status($this->proc)['pid'] ?? 0;
        $this->assertNotSame(
            $agentPid,
            $stubPid,
            'Stub PID equals the agent PID — the worker did not run in a separate execed process.',
        );
    }

    /**
     * A standalone harness that builds a real AsyncServer with a spawner which
     * execs a stub that records its PID. Heredoc kept dependency-free except for
     * the package autoloader.
     */
    private function harnessScript(int $port): string
    {
        $autoload = realpath(__DIR__.'/../../vendor/autoload.php');
        $marker = var_export($this->markerPath, true);
        $sqlite = var_export($this->sqlitePath, true);
        $autoloadExport = var_export($autoload, true);

        return <<<PHP
        <?php
        require {$autoloadExport};

        use NightOwl\\Agent\\AsyncServer;
        use NightOwl\\Agent\\DrainWorker;
        use NightOwl\\Agent\\PayloadParser;

        \$marker = {$marker};

        \$spawner = function (int \$workerId, int \$totalWorkers, string \$sqlitePath) use (\$marker) {
            // Fresh interpreter that proves it ran, then idles so the parent's
            // SIGCHLD restart logic doesn't fire during the test window.
            \$code = 'file_put_contents(' . var_export(\$marker, true) . ', getmypid()); usleep(30000000);';
            pcntl_exec(PHP_BINARY, ['-r', \$code]);
            // Only reached if exec failed.
            file_put_contents(\$marker . '.execfail', (string) (error_get_last()['message'] ?? 'unknown'));
            exit(1);
        };

        \$server = new AsyncServer(
            parser: new PayloadParser(),
            sqlitePath: {$sqlite},
            // Unreachable Postgres on purpose: if the worker ran in-process instead
            // of execing, DrainWorker::run() would hang/fail here and never write
            // the marker.
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
            drainWorkerCount: 1,
            drainSpawner: \$spawner,
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
