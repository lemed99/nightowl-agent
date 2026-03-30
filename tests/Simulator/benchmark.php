#!/usr/bin/env php
<?php

/**
 * Agent throughput benchmark — measures peak payloads/s with concurrent connections.
 *
 * Uses pcntl_fork to spawn N worker processes, each sending payloads as fast as possible
 * for a fixed duration. Results are aggregated via shared temp file.
 *
 * Usage:
 *   php tests/Simulator/benchmark.php --token=test-token --port=2410
 *   php tests/Simulator/benchmark.php --token=test-token --port=2410 --workers=8 --duration=10
 *   php tests/Simulator/benchmark.php --token=test-token --port=2410 --workers=1 --duration=5   # single-threaded baseline
 */

require_once __DIR__ . '/../../vendor/autoload.php';

use NightOwl\Tests\Simulator\NightwatchSimulator;

if (! function_exists('pcntl_fork')) {
    fwrite(STDERR, "Error: pcntl extension required for benchmark.\n");
    exit(1);
}

$options = getopt('', ['token:', 'host:', 'port:', 'workers:', 'duration:', 'payload:']);

$token = $options['token'] ?? null;
if (! $token) {
    fwrite(STDERR, <<<HELP

    NightOwl Agent Benchmark
    ────────────────────────

    Usage:
      php tests/Simulator/benchmark.php --token=<token> [options]

    Options:
      --host       Agent host (default: 127.0.0.1)
      --port       Agent port (default: 2410)
      --workers    Concurrent worker processes (default: 4)
      --duration   Test duration in seconds (default: 5)
      --payload    Payload type: small (1 record), medium (5 records), large (20 records)
                   Default: medium

    HELP);
    exit(1);
}

$host = $options['host'] ?? '127.0.0.1';
$port = (int) ($options['port'] ?? 2410);
$workers = (int) ($options['workers'] ?? 4);
$duration = (int) ($options['duration'] ?? 5);
$payloadType = $options['payload'] ?? 'medium';

// Results file for IPC
$resultsFile = sys_get_temp_dir() . '/nightowl-bench-' . getmypid() . '.json';

// Pre-build payloads
$sim = new NightwatchSimulator($token, $host, $port, timeout: 2.0);

$payloadBuilders = [
    'small' => fn () => [$sim->makeRequest()],                                    // 1 record
    'medium' => fn () => [                                                         // ~5 records
        $sim->makeRequest(),
        $sim->makeQuery(),
        $sim->makeQuery(),
        $sim->makeCacheEvent(),
        $sim->makeLog(),
    ],
    'large' => fn () => array_merge(                                              // ~20 records
        [$sim->makeRequest()],
        array_map(fn () => $sim->makeQuery(), range(1, 8)),
        array_map(fn () => $sim->makeCacheEvent(), range(1, 3)),
        array_map(fn () => $sim->makeLog(), range(1, 2)),
        [$sim->makeOutgoingRequest()],
        [$sim->makeMail()],
        [$sim->makeNotification()],
        [$sim->makeUser('bench_' . mt_rand(1, 100))],
    ),
];

if (! isset($payloadBuilders[$payloadType])) {
    fwrite(STDERR, "Unknown payload type: {$payloadType}\n");
    exit(1);
}

$buildPayload = $payloadBuilders[$payloadType];
$sampleRecords = $buildPayload();
$recordsPerPayload = count($sampleRecords);

// Pre-encode wire format for maximum send speed
$tokenHash = substr(hash('xxh128', $token), 0, 7);

function buildWire(array $records, string $tokenHash): string
{
    $json = json_encode($records, JSON_THROW_ON_ERROR);
    $body = "v1:{$tokenHash}:{$json}";
    return strlen($body) . ':' . $body;
}

fwrite(STDOUT, "\n");
fwrite(STDOUT, "NightOwl Agent Benchmark\n");
fwrite(STDOUT, "────────────────────────\n");
fwrite(STDOUT, "Target:     tcp://{$host}:{$port}\n");
fwrite(STDOUT, "Workers:    {$workers}\n");
fwrite(STDOUT, "Duration:   {$duration}s\n");
fwrite(STDOUT, "Payload:    {$payloadType} ({$recordsPerPayload} records/payload)\n");
fwrite(STDOUT, "\nWarming up...\n");

// Warm-up: 1 payload to ensure connection works
$warmup = buildWire($buildPayload(), $tokenHash);
$sock = @stream_socket_client("tcp://{$host}:{$port}", $errno, $errstr, 2.0);
if (! $sock) {
    fwrite(STDERR, "Cannot connect to agent: {$errstr}\n");
    exit(1);
}
fwrite($sock, $warmup);
$resp = fread($sock, 16);
fclose($sock);
if (! str_starts_with($resp, '2:')) {
    fwrite(STDERR, "Agent rejected warm-up payload: {$resp}\n");
    exit(1);
}

fwrite(STDOUT, "Running benchmark...\n\n");

// Fork workers
$childPids = [];
$startTime = microtime(true) + 0.5; // Synchronize start across workers
$endTime = $startTime + $duration;

for ($w = 0; $w < $workers; $w++) {
    $pid = pcntl_fork();

    if ($pid === -1) {
        fwrite(STDERR, "Fork failed\n");
        exit(1);
    }

    if ($pid === 0) {
        // === Child worker ===
        $sent = 0;
        $failed = 0;
        $bytes = 0;

        // Wait for synchronized start
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

        // Write results
        $workerResult = json_encode([
            'worker' => $w,
            'sent' => $sent,
            'failed' => $failed,
            'bytes' => $bytes,
        ]);

        // Append to results file (atomic via file locking)
        $fp = fopen($resultsFile, 'a');
        flock($fp, LOCK_EX);
        fwrite($fp, $workerResult . "\n");
        flock($fp, LOCK_UN);
        fclose($fp);

        exit(0);
    }

    $childPids[] = $pid;
}

// === Parent: wait for all workers ===
foreach ($childPids as $pid) {
    pcntl_waitpid($pid, $status);
}

$actualDuration = microtime(true) - $startTime;

// Aggregate results
$totalSent = 0;
$totalFailed = 0;
$totalBytes = 0;
$workerResults = [];

if (file_exists($resultsFile)) {
    $lines = array_filter(explode("\n", file_get_contents($resultsFile)));
    foreach ($lines as $line) {
        $r = json_decode($line, true);
        if ($r) {
            $totalSent += $r['sent'];
            $totalFailed += $r['failed'];
            $totalBytes += $r['bytes'];
            $workerResults[] = $r;
        }
    }
    @unlink($resultsFile);
}

$totalRecords = $totalSent * $recordsPerPayload;
$payloadsPerSec = $actualDuration > 0 ? round($totalSent / $actualDuration) : 0;
$recordsPerSec = $actualDuration > 0 ? round($totalRecords / $actualDuration) : 0;
$mbPerSec = $actualDuration > 0 ? round($totalBytes / 1024 / 1024 / $actualDuration, 2) : 0;

fwrite(STDOUT, "Results\n");
fwrite(STDOUT, "───────\n");

// Per-worker breakdown
foreach ($workerResults as $r) {
    $wps = round($r['sent'] / $actualDuration);
    fwrite(STDOUT, sprintf("  Worker %d:  %s sent, %s failed  (%s/s)\n",
        $r['worker'], number_format($r['sent']), number_format($r['failed']), number_format($wps)));
}

fwrite(STDOUT, "\n");
fwrite(STDOUT, sprintf("  Total payloads:  %s sent, %s failed\n", number_format($totalSent), number_format($totalFailed)));
fwrite(STDOUT, sprintf("  Total records:   %s (%d per payload)\n", number_format($totalRecords), $recordsPerPayload));
fwrite(STDOUT, sprintf("  Total data:      %s MB\n", number_format($totalBytes / 1024 / 1024, 1)));
fwrite(STDOUT, sprintf("  Duration:        %.1fs\n", $actualDuration));
fwrite(STDOUT, "\n");
fwrite(STDOUT, sprintf("  Payloads/s:      %s\n", number_format($payloadsPerSec)));
fwrite(STDOUT, sprintf("  Records/s:       %s\n", number_format($recordsPerSec)));
fwrite(STDOUT, sprintf("  Throughput:      %s MB/s\n", $mbPerSec));
fwrite(STDOUT, "\n");
