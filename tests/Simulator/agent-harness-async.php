#!/usr/bin/env php
<?php

/**
 * Standalone ASYNC agent harness — runs the full AsyncServer with fork + drain worker.
 *
 * This is the production-equivalent driver: ReactPHP event loop for non-blocking TCP,
 * SQLite buffer for crash safety, forked DrainWorker for background PostgreSQL writes.
 *
 * Usage:
 *   # Start PostgreSQL first
 *   docker run -d --name nightowl-test-pg -p 5433:5432 \
 *     -e POSTGRES_DB=nightowl_test -e POSTGRES_USER=nightowl_test \
 *     -e POSTGRES_PASSWORD=test123 postgres:15-alpine
 *
 *   # Start async agent
 *   NIGHTOWL_TEST_DB_PORT=5433 php tests/Simulator/agent-harness-async.php --token=test-token
 *
 *   # In another terminal, run the simulator
 *   php tests/Simulator/run.php --token=test-token --scenario=high-throughput --count=500
 */

require_once __DIR__ . '/../../vendor/autoload.php';

use NightOwl\Agent\AsyncServer;
use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\PayloadParser;
use NightOwl\Agent\Redactor;
use NightOwl\Agent\Sampler;

if (! function_exists('pcntl_fork') || ! function_exists('posix_kill')) {
    fwrite(STDERR, "Error: pcntl and posix extensions required for async driver.\n");
    exit(1);
}

$options = getopt('', ['token:', 'host:', 'port:', 'db-host:', 'db-port:', 'db-name:', 'db-user:', 'db-pass:', 'drain-workers:', 'sample-rate:', 'redact-keys:', 'threshold-cache-ttl:', 'max-pending-rows:', 'drain-interval:']);

$token = $options['token'] ?? null;
if (! $token) {
    fwrite(STDERR, "Usage: php tests/Simulator/agent-harness-async.php --token=<token>\n");
    exit(1);
}

$host = $options['host'] ?? '127.0.0.1';
$port = (int) ($options['port'] ?? 2410);

$dbHost = $options['db-host'] ?? getenv('NIGHTOWL_TEST_DB_HOST') ?: '127.0.0.1';
$dbPort = (int) ($options['db-port'] ?? getenv('NIGHTOWL_TEST_DB_PORT') ?: 5432);
$dbName = $options['db-name'] ?? getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test';
$dbUser = $options['db-user'] ?? getenv('NIGHTOWL_TEST_DB_USERNAME') ?: 'nightowl_test';
$dbPass = $options['db-pass'] ?? getenv('NIGHTOWL_TEST_DB_PASSWORD') ?: 'test123';

// Create tables
fwrite(STDOUT, "Connecting to PostgreSQL {$dbHost}:{$dbPort}/{$dbName}...\n");

try {
    $pdo = new PDO("pgsql:host={$dbHost};port={$dbPort};dbname={$dbName}", $dbUser, $dbPass);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $sql = file_get_contents(__DIR__ . '/schema.sql');
    if ($sql) {
        $pdo->exec($sql);
    }
    fwrite(STDOUT, "Tables ready.\n");
} catch (Exception $e) {
    fwrite(STDERR, "Failed to connect to PostgreSQL: {$e->getMessage()}\n");
    exit(1);
}
unset($pdo);

// SQLite buffer path
$sqlitePath = sys_get_temp_dir() . '/nightowl-harness-' . getmypid() . '.sqlite';

$drainWorkers = (int) ($options['drain-workers'] ?? 1);
$sampleRate = (float) ($options['sample-rate'] ?? 1.0);
$redactKeysRaw = $options['redact-keys'] ?? '';
$redactKeys = $redactKeysRaw !== '' ? explode(',', $redactKeysRaw) : [];
$thresholdCacheTtl = (int) ($options['threshold-cache-ttl'] ?? 86400);
$maxPendingRows = (int) ($options['max-pending-rows'] ?? 100_000);
$drainIntervalMs = (int) ($options['drain-interval'] ?? 50);

// Wire up the async server
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
        batchSize: 5000,
        intervalMs: $drainIntervalMs,
        thresholdCacheTtl: $thresholdCacheTtl,
    ),
    sampler: new Sampler(sampleRate: $sampleRate),
    redactor: new Redactor(keys: $redactKeys, enabled: ! empty($redactKeys)),
    token: $token,
    maxPendingRows: $maxPendingRows,
    maxBufferMemory: 256 * 1024 * 1024,
    enableUdp: false,
    healthEnabled: false,
    healthReportEnabled: false,
    drainWorkerCount: $drainWorkers,
);

$tokenHash = substr(hash('xxh128', $token), 0, 7);

fwrite(STDOUT, "\n");
fwrite(STDOUT, "NightOwl Agent Harness (ASYNC)\n");
fwrite(STDOUT, "─────────────────────────────\n");
fwrite(STDOUT, "Listening:  tcp://{$host}:{$port}\n");
fwrite(STDOUT, "Token hash: {$tokenHash}\n");
fwrite(STDOUT, "Database:   {$dbHost}:{$dbPort}/{$dbName}\n");
fwrite(STDOUT, "Buffer:     {$sqlitePath}\n");
fwrite(STDOUT, "Driver:     async (ReactPHP + fork)\n");
fwrite(STDOUT, "Drain:      {$drainWorkers} worker(s), batch 5000, COPY protocol\n");
fwrite(STDOUT, "\nPress Ctrl+C to stop.\n\n");

$server->listen($host, $port);

// Cleanup SQLite files
foreach ([$sqlitePath, $sqlitePath . '-wal', $sqlitePath . '-shm', $sqlitePath . '.drain-metrics.json', $sqlitePath . '.drain-metrics.json.tmp'] as $f) {
    if (file_exists($f)) {
        @unlink($f);
    }
}

fwrite(STDOUT, "Agent stopped.\n");
