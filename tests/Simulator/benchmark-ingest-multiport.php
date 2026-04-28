#!/usr/bin/env php
<?php

/**
 * Multi-port ingest benchmark for macOS — each instance on its own port,
 * workers distributed evenly across ports.
 *
 * On Linux, SO_REUSEPORT handles this automatically on one port.
 * On macOS, we simulate the same by giving each instance a separate port.
 *
 * Usage:
 *   php tests/Simulator/benchmark-ingest-multiport.php --token=test-token --instances=4 --workers=16
 */

require_once __DIR__ . '/../../vendor/autoload.php';

use NightOwl\Agent\AsyncServer;
use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\PayloadParser;
use NightOwl\Tests\Simulator\NightwatchSimulator;

if (! function_exists('pcntl_fork')) {
    fwrite(STDERR, "Error: pcntl extension required.\n");
    exit(1);
}

$options = getopt('', ['token:', 'instances:', 'workers:', 'duration:', 'payload:', 'base-port:']);

$token = $options['token'] ?? null;
if (! $token) {
    fwrite(STDERR, <<<HELP

    NightOwl Multi-Port Ingest Benchmark (macOS compatible)
    ───────────────────────────────────────────────────────

    Usage:
      php tests/Simulator/benchmark-ingest-multiport.php --token=<token> [options]

    Options:
      --instances    Agent instances, each on its own port (default: 1)
      --workers      Total client workers, distributed across instances (default: 4)
      --duration     Seconds (default: 10)
      --payload      small / medium / large (default: medium)
      --base-port    Starting port (default: 2420)

    HELP);
    exit(1);
}

$host = '127.0.0.1';
$instanceCount = (int) ($options['instances'] ?? 1);
$totalWorkers = (int) ($options['workers'] ?? 4);
$duration = (int) ($options['duration'] ?? 10);
$payloadType = $options['payload'] ?? 'medium';
$basePort = (int) ($options['base-port'] ?? 2420);

$tokenHash = substr(hash('xxh128', $token), 0, 7);
$sim = new NightwatchSimulator($token);

$payloadBuilders = [
    'small' => fn () => [$sim->makeRequest()],
    'medium' => fn () => [
        $sim->makeRequest(), $sim->makeQuery(), $sim->makeQuery(),
        $sim->makeCacheEvent(), $sim->makeLog(),
    ],
    'large' => fn () => array_merge(
        [$sim->makeRequest()],
        array_map(fn () => $sim->makeQuery(), range(1, 8)),
        array_map(fn () => $sim->makeCacheEvent(), range(1, 3)),
        [$sim->makeOutgoingRequest(), $sim->makeMail(), $sim->makeUser('b_' . mt_rand(1, 100))],
    ),
];

$buildPayload = $payloadBuilders[$payloadType] ?? $payloadBuilders['medium'];
$recordsPerPayload = count($buildPayload());

function buildWire(array $records, string $tokenHash): string
{
    $json = json_encode($records, JSON_THROW_ON_ERROR);
    $body = "v1:{$tokenHash}:{$json}";
    return strlen($body) . ':' . $body;
}

// ─── Spawn instances on separate ports ──────────────────────

$ports = [];
$agentPids = [];
$sqlitePaths = [];

for ($i = 0; $i < $instanceCount; $i++) {
    $instancePort = $basePort + $i;
    $ports[] = $instancePort;
    $sqlitePath = sys_get_temp_dir() . "/nightowl-mp-{$i}-" . getmypid() . '.sqlite';
    $sqlitePaths[] = $sqlitePath;

    $pid = pcntl_fork();
    if ($pid === -1) { exit(1); }

    if ($pid === 0) {
        error_reporting(0);
        $server = new AsyncServer(
            parser: new PayloadParser(gzipEnabled: true),
            sqlitePath: $sqlitePath,
            drainWorker: new DrainWorker(
                sqlitePath: $sqlitePath,
                pgHost: '127.0.0.1', pgPort: 1, pgDatabase: 'x', pgUsername: 'x', pgPassword: 'x',
                batchSize: 1000, intervalMs: 2000,
            ),
            token: $token,
            maxPendingRows: 10_000_000,
            maxBufferMemory: 1024 * 1024 * 1024,
            enableUdp: false, healthEnabled: false, healthReportEnabled: false,
        );
        @$server->listen($host, $instancePort);
        exit(0);
    }

    $agentPids[] = $pid;
    usleep(300_000);
}

sleep(2);

// Verify all instances
foreach ($ports as $p) {
    $s = new NightwatchSimulator($token, $host, $p, 2.0);
    $r = $s->ping();
    if (! $r || ! str_starts_with($r, '2:')) {
        fwrite(STDERR, "Instance on port {$p} not responding\n");
        goto cleanup;
    }
}

$portsStr = implode(', ', $ports);
$workersPerInstance = max(1, intdiv($totalWorkers, $instanceCount));
$actualWorkers = $workersPerInstance * $instanceCount;

fwrite(STDOUT, "\n");
fwrite(STDOUT, "NightOwl Multi-Port Ingest Benchmark\n");
fwrite(STDOUT, "────────────────────────────────────\n");
fwrite(STDOUT, "Instances:  {$instanceCount} (ports: {$portsStr})\n");
fwrite(STDOUT, "Workers:    {$actualWorkers} total ({$workersPerInstance} per instance)\n");
fwrite(STDOUT, "Duration:   {$duration}s\n");
fwrite(STDOUT, "Payload:    {$payloadType} ({$recordsPerPayload} records)\n");
fwrite(STDOUT, "Drain:      DISABLED\n");
fwrite(STDOUT, "\nRunning...\n\n");

// ─── Benchmark workers ──────────────────────────────────────

$resultsFile = sys_get_temp_dir() . '/nightowl-mp-res-' . getmypid() . '.json';
$startTime = microtime(true) + 0.5;
$endTime = $startTime + $duration;

$workerPids = [];

for ($i = 0; $i < $instanceCount; $i++) {
    $targetPort = $ports[$i];

    for ($w = 0; $w < $workersPerInstance; $w++) {
        $workerId = ($i * $workersPerInstance) + $w;
        $pid = pcntl_fork();
        if ($pid === -1) { break; }

        if ($pid === 0) {
            $sent = 0; $failed = 0; $bytes = 0;

            while (microtime(true) < $startTime) { usleep(1000); }

            while (microtime(true) < $endTime) {
                $wire = buildWire($buildPayload(), $tokenHash);
                $sock = @stream_socket_client("tcp://{$host}:{$targetPort}", $en, $es, 1.0);
                if (! $sock) { $failed++; continue; }
                stream_set_timeout($sock, 2);
                fwrite($sock, $wire);
                $r = fread($sock, 16);
                fclose($sock);
                if ($r !== false && str_starts_with($r, '2:')) { $sent++; $bytes += strlen($wire); }
                else { $failed++; }
            }

            $fp = fopen($resultsFile, 'a');
            flock($fp, LOCK_EX);
            fwrite($fp, json_encode(['i' => $i, 'w' => $workerId, 's' => $sent, 'f' => $failed, 'b' => $bytes]) . "\n");
            flock($fp, LOCK_UN);
            fclose($fp);
            exit(0);
        }

        $workerPids[] = $pid;
    }
}

foreach ($workerPids as $pid) { pcntl_waitpid($pid, $st); }

$actualDuration = microtime(true) - $startTime;

// ─── Results ────────────────────────────────────────────────

$totalSent = 0; $totalFailed = 0; $totalBytes = 0;
$perInstance = array_fill(0, $instanceCount, ['sent' => 0, 'failed' => 0]);

if (file_exists($resultsFile)) {
    foreach (array_filter(explode("\n", file_get_contents($resultsFile))) as $line) {
        $r = json_decode($line, true);
        if ($r) {
            $totalSent += $r['s'];
            $totalFailed += $r['f'];
            $totalBytes += $r['b'];
            $perInstance[$r['i']]['sent'] += $r['s'];
            $perInstance[$r['i']]['failed'] += $r['f'];
        }
    }
    @unlink($resultsFile);
}

// SQLite row counts
$totalBuffered = 0;
foreach ($sqlitePaths as $idx => $sp) {
    if (file_exists($sp)) {
        try {
            $db = new \PDO("sqlite:{$sp}");
            $db->exec('PRAGMA busy_timeout=5000');
            $count = (int) $db->query('SELECT COUNT(*) FROM buffer')->fetchColumn();
            $totalBuffered += $count;
            $ips = round($perInstance[$idx]['sent'] / $actualDuration);
            fwrite(STDOUT, sprintf("  Instance %d (port %d):  %s buffered  (%s/s)\n", $idx, $ports[$idx], number_format($count), number_format($ips)));
        } catch (\Exception $e) {
            fwrite(STDOUT, sprintf("  Instance %d: error %s\n", $idx, $e->getMessage()));
        }
    }
}

$totalRecords = $totalSent * $recordsPerPayload;
$pps = $actualDuration > 0 ? round($totalSent / $actualDuration) : 0;
$rps = $actualDuration > 0 ? round($totalRecords / $actualDuration) : 0;
$mbps = $actualDuration > 0 ? round($totalBytes / 1024 / 1024 / $actualDuration, 2) : 0;
$failRate = ($totalSent + $totalFailed) > 0 ? round($totalFailed / ($totalSent + $totalFailed) * 100, 1) : 0;

fwrite(STDOUT, "\n");
fwrite(STDOUT, sprintf("  Total:     %s sent, %s failed (%.1f%%)\n", number_format($totalSent), number_format($totalFailed), $failRate));
fwrite(STDOUT, sprintf("  Records:   %s (%d per payload)\n", number_format($totalRecords), $recordsPerPayload));
fwrite(STDOUT, sprintf("  Duration:  %.1fs\n\n", $actualDuration));
fwrite(STDOUT, sprintf("  ► Payloads/s:  %s\n", number_format($pps)));
fwrite(STDOUT, sprintf("  ► Records/s:   %s\n", number_format($rps)));
fwrite(STDOUT, sprintf("  ► Throughput:   %s MB/s\n\n", $mbps));

// ─── Cleanup ───────────────────────────────────────────────

cleanup:

foreach ($agentPids as $pid) { posix_kill($pid, SIGTERM); }
sleep(1);
foreach ($agentPids as $pid) {
    if (pcntl_waitpid($pid, $st, WNOHANG) === 0) {
        posix_kill($pid, SIGKILL); pcntl_waitpid($pid, $st);
    }
}
foreach ($sqlitePaths as $f) {
    foreach ([$f, "{$f}-wal", "{$f}-shm", "{$f}.drain-metrics.json", "{$f}.drain-metrics.json.tmp"] as $file) {
        if (file_exists($file)) { @unlink($file); }
    }
}

fwrite(STDOUT, "Done.\n");
