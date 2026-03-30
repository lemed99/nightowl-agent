#!/usr/bin/env php
<?php

/**
 * Ingest-only benchmark — measures how fast agent instances can accept payloads
 * and buffer them to SQLite, with NO PostgreSQL drain.
 *
 * Uses a fake DrainWorker that does nothing, isolating pure TCP + parse + SQLite performance.
 *
 * Usage:
 *   php tests/Simulator/benchmark-ingest.php --token=test-token --instances=1 --workers=4
 *   php tests/Simulator/benchmark-ingest.php --token=test-token --instances=4 --workers=16
 */

require_once __DIR__ . '/../../vendor/autoload.php';

use NightOwl\Agent\AsyncServer;
use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\PayloadParser;
use NightOwl\Agent\Redactor;
use NightOwl\Agent\Sampler;
use NightOwl\Agent\SqliteBuffer;
use NightOwl\Tests\Simulator\NightwatchSimulator;

if (! function_exists('pcntl_fork')) {
    fwrite(STDERR, "Error: pcntl extension required.\n");
    exit(1);
}

$options = getopt('', ['token:', 'host:', 'port:', 'instances:', 'workers:', 'duration:', 'payload:']);

$token = $options['token'] ?? null;
if (! $token) {
    fwrite(STDERR, <<<HELP

    NightOwl Ingest-Only Benchmark
    ──────────────────────────────

    Measures pure ingestion throughput: TCP → parse → SQLite buffer.
    No PostgreSQL drain — isolates the agent's accept capacity.

    Usage:
      php tests/Simulator/benchmark-ingest.php --token=<token> [options]

    Options:
      --instances   Agent instances (default: 1)
      --workers     Client workers (default: 4)
      --duration    Seconds (default: 10)
      --port        Port (default: 2410)
      --payload     small / medium / large (default: medium)

    HELP);
    exit(1);
}

$host = $options['host'] ?? '127.0.0.1';
$port = (int) ($options['port'] ?? 2410);
$instances = (int) ($options['instances'] ?? 1);
$workers = (int) ($options['workers'] ?? 4);
$duration = (int) ($options['duration'] ?? 10);
$payloadType = $options['payload'] ?? 'medium';

$sim = new NightwatchSimulator($token, $host, $port, timeout: 2.0);
$tokenHash = substr(hash('xxh128', $token), 0, 7);

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

// ─── Spawn agent instances with no-op drain ─────────────────

fwrite(STDOUT, "\nSpawning {$instances} agent instance(s) (ingest-only, no PG drain)...\n");

$agentPids = [];
$sqlitePaths = [];

for ($i = 0; $i < $instances; $i++) {
    $sqlitePath = sys_get_temp_dir() . "/nightowl-ingest-{$i}-" . getmypid() . '.sqlite';
    $sqlitePaths[] = $sqlitePath;

    $pid = pcntl_fork();

    if ($pid === -1) {
        fwrite(STDERR, "Fork failed\n");
        exit(1);
    }

    if ($pid === 0) {
        // === Agent instance ===
        // DrainWorker with impossible PG creds — it will fork but immediately
        // fail to connect and just sleep in a retry loop, effectively doing nothing.
        // The parent still accepts and buffers to SQLite at full speed.
        $server = new AsyncServer(
            parser: new PayloadParser(gzipEnabled: true),
            sqlitePath: $sqlitePath,
            drainWorker: new DrainWorker(
                sqlitePath: $sqlitePath,
                pgHost: '127.0.0.1',
                pgPort: 1, // impossible port — drain worker will fail harmlessly
                pgDatabase: 'none',
                pgUsername: 'none',
                pgPassword: 'none',
                batchSize: 1000,
                intervalMs: 1000, // slow retry so it doesn't spam errors
            ),
            sampler: new Sampler(sampleRate: 1.0),
            redactor: new Redactor(keys: [], enabled: false),
            token: $token,
            maxPendingRows: 10_000_000, // 10M — effectively unlimited for this benchmark
            maxBufferMemory: 1024 * 1024 * 1024, // 1GB — won't trigger
            enableUdp: false,
            healthEnabled: false,
            healthReportEnabled: false,
        );

        // Suppress drain worker error output
        error_reporting(0);
        @$server->listen($host, $port);
        exit(0);
    }

    $agentPids[] = $pid;
    usleep(300_000); // 300ms stagger
}

sleep(2);

// Verify
$resp = $sim->ping();
if (! $resp || ! str_starts_with($resp, '2:')) {
    fwrite(STDERR, "Agent not responding\n");
    goto cleanup;
}

fwrite(STDOUT, "{$instances} instance(s) running.\n");

// ─── Benchmark ──────────────────────────────────────────────

fwrite(STDOUT, "\n");
fwrite(STDOUT, "NightOwl Ingest-Only Benchmark\n");
fwrite(STDOUT, "──────────────────────────────\n");
fwrite(STDOUT, "Instances:  {$instances}\n");
fwrite(STDOUT, "Workers:    {$workers}\n");
fwrite(STDOUT, "Duration:   {$duration}s\n");
fwrite(STDOUT, "Payload:    {$payloadType} ({$recordsPerPayload} records)\n");
fwrite(STDOUT, "Drain:      DISABLED (ingest → SQLite only)\n");
fwrite(STDOUT, "\nRunning...\n\n");

$resultsFile = sys_get_temp_dir() . '/nightowl-ingest-res-' . getmypid() . '.json';
$startTime = microtime(true) + 0.5;
$endTime = $startTime + $duration;

$workerPids = [];

for ($w = 0; $w < $workers; $w++) {
    $pid = pcntl_fork();
    if ($pid === -1) { break; }

    if ($pid === 0) {
        $sent = 0;
        $failed = 0;
        $bytes = 0;

        while (microtime(true) < $startTime) { usleep(1000); }

        while (microtime(true) < $endTime) {
            $wire = buildWire($buildPayload(), $tokenHash);
            $sock = @stream_socket_client("tcp://{$host}:{$port}", $en, $es, 1.0);
            if (! $sock) { $failed++; continue; }

            stream_set_timeout($sock, 2);
            fwrite($sock, $wire);
            $r = fread($sock, 16);
            fclose($sock);

            if ($r !== false && str_starts_with($r, '2:')) {
                $sent++;
                $bytes += strlen($wire);
            } else {
                $failed++;
            }
        }

        $fp = fopen($resultsFile, 'a');
        flock($fp, LOCK_EX);
        fwrite($fp, json_encode(['w' => $w, 's' => $sent, 'f' => $failed, 'b' => $bytes]) . "\n");
        flock($fp, LOCK_UN);
        fclose($fp);
        exit(0);
    }

    $workerPids[] = $pid;
}

foreach ($workerPids as $pid) { pcntl_waitpid($pid, $st); }

$actualDuration = microtime(true) - $startTime;

// Aggregate
$totalSent = 0; $totalFailed = 0; $totalBytes = 0;
$perWorker = [];

if (file_exists($resultsFile)) {
    foreach (array_filter(explode("\n", file_get_contents($resultsFile))) as $line) {
        $r = json_decode($line, true);
        if ($r) {
            $totalSent += $r['s'];
            $totalFailed += $r['f'];
            $totalBytes += $r['b'];
            $perWorker[] = $r;
        }
    }
    @unlink($resultsFile);
}

// Count SQLite rows
$totalBuffered = 0;
foreach ($sqlitePaths as $idx => $sp) {
    if (file_exists($sp)) {
        try {
            $db = new PDO("sqlite:{$sp}");
            $db->exec('PRAGMA busy_timeout=5000');
            $count = (int) $db->query('SELECT COUNT(*) FROM buffer')->fetchColumn();
            $totalBuffered += $count;
            fwrite(STDOUT, sprintf("  Instance %d SQLite: %s rows buffered\n", $idx, number_format($count)));
        } catch (\Exception $e) {
            fwrite(STDOUT, sprintf("  Instance %d SQLite: (error: %s)\n", $idx, $e->getMessage()));
        }
    }
}

$totalRecords = $totalSent * $recordsPerPayload;
$pps = $actualDuration > 0 ? round($totalSent / $actualDuration) : 0;
$rps = $actualDuration > 0 ? round($totalRecords / $actualDuration) : 0;
$mbps = $actualDuration > 0 ? round($totalBytes / 1024 / 1024 / $actualDuration, 2) : 0;
$failRate = ($totalSent + $totalFailed) > 0 ? round($totalFailed / ($totalSent + $totalFailed) * 100, 1) : 0;

fwrite(STDOUT, "\n");
fwrite(STDOUT, "Results\n");
fwrite(STDOUT, "───────\n");

foreach ($perWorker as $r) {
    $wps = round($r['s'] / $actualDuration);
    fwrite(STDOUT, sprintf("  Worker %d:  %s sent, %s failed  (%s/s)\n",
        $r['w'], number_format($r['s']), number_format($r['f']), number_format($wps)));
}

fwrite(STDOUT, "\n");
fwrite(STDOUT, sprintf("  Payloads:     %s sent, %s failed (%.1f%%)\n", number_format($totalSent), number_format($totalFailed), $failRate));
fwrite(STDOUT, sprintf("  Records:      %s (%d per payload)\n", number_format($totalRecords), $recordsPerPayload));
fwrite(STDOUT, sprintf("  Buffered:     %s rows in SQLite\n", number_format($totalBuffered)));
fwrite(STDOUT, sprintf("  Data:         %s MB\n", number_format($totalBytes / 1024 / 1024, 1)));
fwrite(STDOUT, sprintf("  Duration:     %.1fs\n", $actualDuration));
fwrite(STDOUT, "\n");
fwrite(STDOUT, sprintf("  ► Payloads/s:  %s\n", number_format($pps)));
fwrite(STDOUT, sprintf("  ► Records/s:   %s\n", number_format($rps)));
fwrite(STDOUT, sprintf("  ► Throughput:   %s MB/s\n", $mbps));
fwrite(STDOUT, "\n");

// ─── Cleanup ───────────────────────────────────────────────

cleanup:

foreach ($agentPids as $pid) { posix_kill($pid, SIGTERM); }
sleep(1);
foreach ($agentPids as $pid) {
    if (pcntl_waitpid($pid, $st, WNOHANG) === 0) {
        posix_kill($pid, SIGKILL);
        pcntl_waitpid($pid, $st);
    }
}

foreach ($sqlitePaths as $f) {
    foreach ([$f, "{$f}-wal", "{$f}-shm", "{$f}.drain-metrics.json", "{$f}.drain-metrics.json.tmp"] as $file) {
        if (file_exists($file)) { @unlink($file); }
    }
}

fwrite(STDOUT, "Done.\n");
