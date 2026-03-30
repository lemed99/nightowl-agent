#!/usr/bin/env php
<?php

/**
 * Standalone agent harness for testing — runs the sync Server with a real
 * PostgreSQL-backed RecordWriter, without needing a full Laravel app.
 *
 * Usage:
 *   NIGHTOWL_TEST_DB_PORT=5433 php tests/Simulator/agent-harness.php --token=test-token
 *
 * Then in another terminal:
 *   php tests/Simulator/run.php --token=test-token --scenario=realistic --count=200
 */

require_once __DIR__ . '/../../vendor/autoload.php';

use NightOwl\Agent\ConnectionHandler;
use NightOwl\Agent\PayloadParser;
use NightOwl\Agent\Redactor;
use NightOwl\Agent\RecordWriter;
use NightOwl\Agent\Sampler;
use NightOwl\Agent\Server;

$options = getopt('', ['token:', 'host:', 'port:', 'db-host:', 'db-port:', 'db-name:', 'db-user:', 'db-pass:']);

$token = $options['token'] ?? null;
if (! $token) {
    fwrite(STDERR, "Usage: php tests/Simulator/agent-harness.php --token=<token>\n");
    exit(1);
}

$host = $options['host'] ?? '127.0.0.1';
$port = (int) ($options['port'] ?? 2407);

$dbHost = $options['db-host'] ?? getenv('NIGHTOWL_TEST_DB_HOST') ?: '127.0.0.1';
$dbPort = (int) ($options['db-port'] ?? getenv('NIGHTOWL_TEST_DB_PORT') ?: 5432);
$dbName = $options['db-name'] ?? getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test';
$dbUser = $options['db-user'] ?? getenv('NIGHTOWL_TEST_DB_USERNAME') ?: 'nightowl_test';
$dbPass = $options['db-pass'] ?? getenv('NIGHTOWL_TEST_DB_PASSWORD') ?: 'test123';

// Create tables if they don't exist
fwrite(STDOUT, "Connecting to PostgreSQL {$dbHost}:{$dbPort}/{$dbName}...\n");

try {
    $pdo = new PDO("pgsql:host={$dbHost};port={$dbPort};dbname={$dbName}", $dbUser, $dbPass);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
} catch (Exception $e) {
    fwrite(STDERR, "Failed to connect to PostgreSQL: {$e->getMessage()}\n");
    exit(1);
}

// Create all tables
$sql = file_get_contents(__DIR__ . '/schema.sql');
if ($sql) {
    $pdo->exec($sql);
    fwrite(STDOUT, "Tables ready.\n");
} else {
    fwrite(STDERR, "Warning: schema.sql not found, assuming tables exist.\n");
}

unset($pdo); // Close setup connection

// Wire up the agent pipeline
$writer = new RecordWriter($dbHost, $dbPort, $dbName, $dbUser, $dbPass);
$parser = new PayloadParser(gzipEnabled: true);
$sampler = new Sampler(sampleRate: 1.0);
$redactor = new Redactor(keys: [], enabled: false);

$handler = new ConnectionHandler(
    parser: $parser,
    writer: $writer,
    sampler: $sampler,
    redactor: $redactor,
    token: $token,
);

$server = new Server($handler);

$tokenHash = substr(hash('xxh128', $token), 0, 7);

fwrite(STDOUT, "\n");
fwrite(STDOUT, "NightOwl Agent Harness\n");
fwrite(STDOUT, "──────────────────────\n");
fwrite(STDOUT, "Listening:  tcp://{$host}:{$port}\n");
fwrite(STDOUT, "Token hash: {$tokenHash}\n");
fwrite(STDOUT, "Database:   {$dbHost}:{$dbPort}/{$dbName}\n");
fwrite(STDOUT, "Driver:     sync (stream_select)\n");
fwrite(STDOUT, "\nPress Ctrl+C to stop.\n\n");

if (function_exists('pcntl_async_signals')) {
    pcntl_async_signals(true);
    pcntl_signal(SIGINT, fn () => $server->stop());
    pcntl_signal(SIGTERM, fn () => $server->stop());
}

$server->listen($host, $port);

fwrite(STDOUT, "Agent stopped.\n");
