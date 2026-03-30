#!/usr/bin/env php
<?php

/**
 * NightOwl Nightwatch Simulator — sends realistic telemetry to the agent.
 *
 * Usage:
 *   php tests/Simulator/run.php --token=your-app-token
 *   php tests/Simulator/run.php --token=your-token --count=200
 *   php tests/Simulator/run.php --token=your-token --scenario=error-storm
 *   php tests/Simulator/run.php --token=your-token --scenario=realistic --count=500
 *   php tests/Simulator/run.php --token=your-token --ping
 *
 * Options:
 *   --token     Required. The NIGHTOWL_TOKEN value from your .env
 *   --host      Agent host (default: 127.0.0.1)
 *   --port      Agent port (default: 2407)
 *   --count     Number of events to send (default: 100)
 *   --scenario  Traffic pattern: mixed, error-storm, high-throughput, jobs, realistic
 *   --ping      Send a single PING and exit
 *   --single    Send a single request event and exit (good for testing)
 */

require_once __DIR__ . '/../../vendor/autoload.php';

use NightOwl\Tests\Simulator\NightwatchSimulator;

// ─── Parse arguments ───────────────────────────────────────

$options = getopt('', ['token:', 'host:', 'port:', 'count:', 'scenario:', 'ping', 'single']);

if (! isset($options['token'])) {
    fwrite(STDERR, <<<HELP

    NightOwl Nightwatch Simulator
    ─────────────────────────────

    Sends realistic telemetry payloads to the NightOwl agent over TCP,
    simulating what laravel/nightwatch would send in production.

    Usage:
      php tests/Simulator/run.php --token=<your-token> [options]

    Required:
      --token        Your NIGHTOWL_TOKEN (same value as in the app's .env)

    Options:
      --host         Agent host (default: 127.0.0.1)
      --port         Agent port (default: 2407)
      --count        Number of events to send (default: 100)
      --scenario     Traffic pattern to simulate:
                       mixed          — 50% requests, 20% jobs, 10% commands, 10% errors, 10% tasks
                       error-storm    — 100% error requests (tests issue creation)
                       high-throughput — 20 requests per batch (tests buffer/drain)
                       jobs           — 60% processed, 20% released, 20% failed
                       realistic      — Production-like mix with mail, notifications, outgoing requests
      --ping         Send a single PING health check and exit
      --single       Send one request lifecycle and exit (good for smoke testing)

    Examples:
      php tests/Simulator/run.php --token=abc123
      php tests/Simulator/run.php --token=abc123 --scenario=realistic --count=500
      php tests/Simulator/run.php --token=abc123 --scenario=error-storm --count=50
      php tests/Simulator/run.php --token=abc123 --ping

    HELP);
    exit(1);
}

$token = $options['token'];
$host = $options['host'] ?? '127.0.0.1';
$port = (int) ($options['port'] ?? 2407);
$count = (int) ($options['count'] ?? 100);
$scenario = $options['scenario'] ?? 'mixed';

$sim = new NightwatchSimulator($token, $host, $port);

// ─── Execute ───────────────────────────────────────────────

$tokenHash = substr(hash('xxh128', $token), 0, 7);
fwrite(STDOUT, "NightOwl Simulator → {$host}:{$port} (token hash: {$tokenHash})\n");

if (isset($options['ping'])) {
    fwrite(STDOUT, "Sending PING... ");
    $response = $sim->ping();
    fwrite(STDOUT, ($response ?? 'no response') . "\n");
    exit($response && str_starts_with($response, '2:') ? 0 : 1);
}

if (isset($options['single'])) {
    fwrite(STDOUT, "Sending single request lifecycle... ");
    $response = $sim->simulateRequest();
    fwrite(STDOUT, ($response ?? 'no response') . "\n");
    $stats = $sim->getStats();
    fwrite(STDOUT, "Bytes sent: {$stats['bytes']}\n");
    exit($response && str_starts_with($response, '2:') ? 0 : 1);
}

fwrite(STDOUT, "Scenario: {$scenario}, Count: {$count}\n\n");
$sim->runScenario($scenario, $count);
