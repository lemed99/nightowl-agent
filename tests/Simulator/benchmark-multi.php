#!/usr/bin/env php
<?php

/**
 * Multi-instance agent benchmark.
 *
 * Spawns N agent instances on the same port (SO_REUSEPORT) with separate SQLite buffers,
 * then hammers them with concurrent client workers to find the throughput ceiling.
 *
 * Usage:
 *   php tests/Simulator/benchmark-multi.php --token=test-token --instances=4 --workers=8
 *
 * Requires: Docker PostgreSQL running on port 5433
 *   docker run -d --name nightowl-test-pg -p 5433:5432 \
 *     -e POSTGRES_DB=nightowl_test -e POSTGRES_USER=nightowl_test \
 *     -e POSTGRES_PASSWORD=test123 postgres:15-alpine
 */

require_once __DIR__.'/../../vendor/autoload.php';

use NightOwl\Agent\AsyncServer;
use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\PayloadParser;
use NightOwl\Tests\Integration\MigrationRunner;
use NightOwl\Tests\Simulator\NightwatchSimulator;

if (! function_exists('pcntl_fork')) {
    fwrite(STDERR, "Error: pcntl extension required.\n");
    exit(1);
}

$options = getopt('', ['token:', 'host:', 'port:', 'instances:', 'workers:', 'duration:', 'payload:', 'db-port:']);

$token = $options['token'] ?? null;
if (! $token) {
    fwrite(STDERR, <<<'HELP'

    NightOwl Multi-Instance Benchmark
    ──────────────────────────────────

    Spawns N agent instances on the same port, then benchmarks with concurrent clients.

    Usage:
      php tests/Simulator/benchmark-multi.php --token=<token> [options]

    Options:
      --instances   Agent instances (default: 2)
      --workers     Client workers (default: 8)
      --duration    Seconds (default: 10)
      --port        Port (default: 2410)
      --payload     small / medium / large (default: medium)
      --db-port     PostgreSQL port (default: 5433)

    Requires PostgreSQL running:
      docker run -d --name nightowl-test-pg -p 5433:5432 \
        -e POSTGRES_DB=nightowl_test -e POSTGRES_USER=nightowl_test \
        -e POSTGRES_PASSWORD=test123 postgres:15-alpine

    HELP);
    exit(1);
}

$host = $options['host'] ?? '127.0.0.1';
$port = (int) ($options['port'] ?? 2410);
$instances = (int) ($options['instances'] ?? 2);
$workers = (int) ($options['workers'] ?? 8);
$duration = (int) ($options['duration'] ?? 10);
$payloadType = $options['payload'] ?? 'medium';
$dbPort = (int) ($options['db-port'] ?? 5433);

$dbHost = '127.0.0.1';
$dbName = 'nightowl_test';
$dbUser = 'nightowl_test';
$dbPass = 'test123';

// ─── Setup database ────────────────────────────────────────

fwrite(STDOUT, "\nSetting up database...\n");

try {
    $pdo = new PDO("pgsql:host={$dbHost};port={$dbPort};dbname={$dbName}", $dbUser, $dbPass);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    MigrationRunner::migrate($dbHost, (int) $dbPort, $dbName, $dbUser, $dbPass);
    // Truncate all tables
    $tables = ['nightowl_issue_activity', 'nightowl_issue_comments', 'nightowl_issues',
        'nightowl_requests', 'nightowl_queries', 'nightowl_exceptions', 'nightowl_commands',
        'nightowl_jobs', 'nightowl_cache_events', 'nightowl_mail', 'nightowl_notifications',
        'nightowl_outgoing_requests', 'nightowl_scheduled_tasks', 'nightowl_logs', 'nightowl_users'];
    foreach ($tables as $t) {
        $pdo->exec("TRUNCATE TABLE {$t} CASCADE");
    }
} catch (Exception $e) {
    fwrite(STDERR, "PostgreSQL error: {$e->getMessage()}\n");
    fwrite(STDERR, "Is PostgreSQL running? docker run -d --name nightowl-test-pg -p {$dbPort}:5432 ...\n");
    exit(1);
}
unset($pdo);

// ─── Spawn agent instances ─────────────────────────────────

fwrite(STDOUT, "Spawning {$instances} agent instances on port {$port}...\n");

$agentPids = [];
$tmpFiles = [];

for ($i = 0; $i < $instances; $i++) {
    $sqlitePath = sys_get_temp_dir()."/nightowl-bench-inst-{$i}-".getmypid().'.sqlite';
    $tmpFiles[] = $sqlitePath;

    $pid = pcntl_fork();

    if ($pid === -1) {
        fwrite(STDERR, "Fork failed for instance {$i}\n");
        exit(1);
    }

    if ($pid === 0) {
        // === Agent instance child ===
        $server = new AsyncServer(
            parser: new PayloadParser(gzipEnabled: true),
            sqlitePath: $sqlitePath,
            drainWorker: new DrainWorker(
                sqlitePath: $sqlitePath,
                pgHost: $dbHost,
                pgPort: $dbPort,
                pgDatabase: $dbName,
                pgUsername: $dbUser,
                pgPassword: $dbPass,
                batchSize: 1000,
                intervalMs: 50,
            ),
            token: $token,
            maxPendingRows: 100_000,
            maxBufferMemory: 256 * 1024 * 1024,
            enableUdp: false,
            healthEnabled: false,
            healthReportEnabled: false,
        );

        $server->listen($host, $port);
        exit(0);
    }

    $agentPids[] = $pid;
    usleep(200_000); // 200ms stagger to avoid SQLite race
}

// Wait for all instances to start
sleep(2);

// Verify connectivity
$sim = new NightwatchSimulator($token, $host, $port, timeout: 2.0);
$resp = $sim->ping();
if (! $resp || ! str_starts_with($resp, '2:')) {
    fwrite(STDERR, "Agent not responding on port {$port}\n");
    goto cleanup;
}

fwrite(STDOUT, "All {$instances} instances running. Agent responds to PING.\n");

// ─── Build payloads ────────────────────────────────────────

$payloadBuilders = [
    'small' => fn () => [$sim->makeRequest()],
    'medium' => fn () => [
        $sim->makeRequest(),
        $sim->makeQuery(),
        $sim->makeQuery(),
        $sim->makeCacheEvent(),
        $sim->makeLog(),
    ],
    'large' => fn () => array_merge(
        [$sim->makeRequest()],
        array_map(fn () => $sim->makeQuery(), range(1, 8)),
        array_map(fn () => $sim->makeCacheEvent(), range(1, 3)),
        [$sim->makeOutgoingRequest(), $sim->makeMail(), $sim->makeUser('bench_'.mt_rand(1, 100))],
    ),
];

$buildPayload = $payloadBuilders[$payloadType] ?? $payloadBuilders['medium'];
$recordsPerPayload = count($buildPayload());

$tokenHash = substr(hash('xxh128', $token), 0, 7);

function buildWire(array $records, string $tokenHash): string
{
    $json = json_encode($records, JSON_THROW_ON_ERROR);
    $body = "v1:{$tokenHash}:{$json}";

    return strlen($body).':'.$body;
}

// ─── Run benchmark workers ─────────────────────────────────

fwrite(STDOUT, "\n");
fwrite(STDOUT, "NightOwl Multi-Instance Benchmark\n");
fwrite(STDOUT, "─────────────────────────────────\n");
fwrite(STDOUT, "Instances:  {$instances} (SO_REUSEPORT on port {$port})\n");
fwrite(STDOUT, "Workers:    {$workers} concurrent clients\n");
fwrite(STDOUT, "Duration:   {$duration}s\n");
fwrite(STDOUT, "Payload:    {$payloadType} ({$recordsPerPayload} records/payload)\n");
fwrite(STDOUT, "\nRunning...\n\n");

$resultsFile = sys_get_temp_dir().'/nightowl-mbench-'.getmypid().'.json';
$startTime = microtime(true) + 0.5;
$endTime = $startTime + $duration;

$workerPids = [];

for ($w = 0; $w < $workers; $w++) {
    $pid = pcntl_fork();

    if ($pid === -1) {
        fwrite(STDERR, "Worker fork failed\n");
        break;
    }

    if ($pid === 0) {
        // === Benchmark worker child ===
        $sent = 0;
        $failed = 0;
        $bytes = 0;

        while (microtime(true) < $startTime) {
            usleep(1000);
        }

        while (microtime(true) < $endTime) {
            $records = $buildPayload();
            $wire = buildWire($records, $tokenHash);

            $sock = @stream_socket_client("tcp://{$host}:{$port}", $errno, $errstr, 1.0);
            if (! $sock) {
                $failed++;

                continue;
            }

            stream_set_timeout($sock, 2);
            fwrite($sock, $wire);
            $resp = fread($sock, 16);
            fclose($sock);

            if ($resp !== false && str_starts_with($resp, '2:')) {
                $sent++;
                $bytes += strlen($wire);
            } else {
                $failed++;
            }
        }

        $fp = fopen($resultsFile, 'a');
        flock($fp, LOCK_EX);
        fwrite($fp, json_encode(['worker' => $w, 'sent' => $sent, 'failed' => $failed, 'bytes' => $bytes])."\n");
        flock($fp, LOCK_UN);
        fclose($fp);
        exit(0);
    }

    $workerPids[] = $pid;
}

// Wait for workers
foreach ($workerPids as $pid) {
    pcntl_waitpid($pid, $status);
}

$actualDuration = microtime(true) - $startTime;

// ─── Aggregate results ─────────────────────────────────────

$totalSent = 0;
$totalFailed = 0;
$totalBytes = 0;

if (file_exists($resultsFile)) {
    $lines = array_filter(explode("\n", file_get_contents($resultsFile)));
    foreach ($lines as $line) {
        $r = json_decode($line, true);
        if ($r) {
            $totalSent += $r['sent'];
            $totalFailed += $r['failed'];
            $totalBytes += $r['bytes'];
        }
    }
    @unlink($resultsFile);
}

$totalRecords = $totalSent * $recordsPerPayload;
$payloadsPerSec = $actualDuration > 0 ? round($totalSent / $actualDuration) : 0;
$recordsPerSec = $actualDuration > 0 ? round($totalRecords / $actualDuration) : 0;
$mbPerSec = $actualDuration > 0 ? round($totalBytes / 1024 / 1024 / $actualDuration, 2) : 0;
$failRate = ($totalSent + $totalFailed) > 0 ? round($totalFailed / ($totalSent + $totalFailed) * 100, 1) : 0;

fwrite(STDOUT, "Results\n");
fwrite(STDOUT, "───────\n");
fwrite(STDOUT, sprintf("  Payloads:    %s sent, %s failed (%.1f%% failure rate)\n", number_format($totalSent), number_format($totalFailed), $failRate));
fwrite(STDOUT, sprintf("  Records:     %s (%d per payload)\n", number_format($totalRecords), $recordsPerPayload));
fwrite(STDOUT, sprintf("  Data:        %s MB\n", number_format($totalBytes / 1024 / 1024, 1)));
fwrite(STDOUT, sprintf("  Duration:    %.1fs\n", $actualDuration));
fwrite(STDOUT, "\n");
fwrite(STDOUT, sprintf("  ► Payloads/s:  %s\n", number_format($payloadsPerSec)));
fwrite(STDOUT, sprintf("  ► Records/s:   %s\n", number_format($recordsPerSec)));
fwrite(STDOUT, sprintf("  ► Throughput:   %s MB/s\n", $mbPerSec));
fwrite(STDOUT, "\n");

// Wait a bit for drain workers to flush, then count DB rows
fwrite(STDOUT, "Waiting for drain workers to flush...\n");
sleep(8);

try {
    $pdo = new PDO("pgsql:host={$dbHost};port={$dbPort};dbname={$dbName}", $dbUser, $dbPass);
    $total = (int) $pdo->query('SELECT COUNT(*) FROM nightowl_requests')->fetchColumn();
    fwrite(STDOUT, sprintf("  PostgreSQL:  %s requests drained\n\n", number_format($total)));
} catch (Exception $e) {
    fwrite(STDOUT, "  (Could not query PostgreSQL: {$e->getMessage()})\n\n");
}

// ─── Cleanup ───────────────────────────────────────────────

cleanup:

fwrite(STDOUT, "Stopping agents...\n");

foreach ($agentPids as $pid) {
    posix_kill($pid, SIGTERM);
}

// Wait for agents to stop
foreach ($agentPids as $pid) {
    pcntl_waitpid($pid, $status, WNOHANG);
}

sleep(2);

// Force kill any remaining
foreach ($agentPids as $pid) {
    if (pcntl_waitpid($pid, $status, WNOHANG) === 0) {
        posix_kill($pid, SIGKILL);
        pcntl_waitpid($pid, $status);
    }
}

// Clean up temp files
foreach ($tmpFiles as $f) {
    foreach ([$f, "{$f}-wal", "{$f}-shm", "{$f}.drain-metrics.json", "{$f}.drain-metrics.json.tmp"] as $file) {
        if (file_exists($file)) {
            @unlink($file);
        }
    }
}

fwrite(STDOUT, "Done.\n");
