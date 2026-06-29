<?php

namespace NightOwl\Tests\System;

use NightOwl\Simulator\NightwatchSimulator;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * Chaos test: agent under a real PostgreSQL outage and recovery.
 *
 * Drives the failure modes flagged on r/laravel:
 *   - back-pressure activates and WAL plateaus when pg is gone
 *   - drain catches up cleanly on recovery
 *   - TRUNCATE checkpoint actually fires (instead of getting starved
 *     by drain-worker contention) and reclaims WAL disk space
 *   - SQLite integrity_check passes through the whole sequence
 *
 * Requirements:
 *   - Docker, with a running container named `nightowl-test-pg`
 *     (the test stops/starts it to simulate the outage)
 *   - NIGHTOWL_RUN_CHAOS=1 (the test is slow and intentionally
 *     disruptive to local pg state — opt-in only)
 *   - pcntl + posix extensions
 *   - Ports 2413 / 2414 available
 *
 * Run:
 *   NIGHTOWL_RUN_CHAOS=1 NIGHTOWL_TEST_DB_PORT=5433 \
 *     vendor/bin/phpunit --filter=AgentPgOutageSystemTest
 */
class AgentPgOutageSystemTest extends TestCase
{
    private const TOKEN = 'chaos-test-token-2025';

    private const AGENT_HOST = '127.0.0.1';

    private const AGENT_PORT = 2413;

    private const STARTUP_TIMEOUT = 5;

    private const PG_RECOVERY_TIMEOUT = 30;

    private const DRAIN_CATCHUP_TIMEOUT = 30;

    // Small enough that back-pressure trips quickly in test time.
    private const MAX_PENDING_ROWS = 500;

    // Cadence + threshold tuned so we exercise the TRUNCATE path on
    // test-sized WAL volumes (default 100MB threshold + 60s cadence
    // would never trigger within a chaos-test window).
    private const CHECKPOINT_INTERVAL_SECONDS = 3;

    private const CHECKPOINT_TRUNCATE_BYTES = 256 * 1024;

    private static string $containerName;

    private static string $dbHost;

    private static int $dbPort;

    private static string $dbDatabase;

    private static string $dbUsername;

    private static string $dbPassword;

    private static ?PDO $pdo = null;

    /** @var resource|null */
    private static $agentProcess = null;

    /** @var resource[] */
    private static array $agentPipes = [];

    private static string $sqlitePath = '';

    private NightwatchSimulator $sim;

    public static function setUpBeforeClass(): void
    {
        if (getenv('NIGHTOWL_RUN_CHAOS') !== '1') {
            static::markTestSkipped('NIGHTOWL_RUN_CHAOS=1 to enable.');
        }

        if (! function_exists('pcntl_fork') || ! function_exists('posix_kill')) {
            static::markTestSkipped('pcntl + posix required.');
        }

        self::$containerName = getenv('NIGHTOWL_CHAOS_DOCKER_CONTAINER') ?: 'nightowl-test-pg';

        if (! self::dockerAvailable()) {
            static::markTestSkipped('docker CLI not available.');
        }

        if (! self::containerRunning(self::$containerName)) {
            static::markTestSkipped(sprintf('container "%s" not running.', self::$containerName));
        }

        self::$dbHost = getenv('NIGHTOWL_TEST_DB_HOST') ?: '127.0.0.1';
        self::$dbPort = (int) (getenv('NIGHTOWL_TEST_DB_PORT') ?: 5432);
        self::$dbDatabase = getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test';
        self::$dbUsername = getenv('NIGHTOWL_TEST_DB_USERNAME') ?: 'nightowl_test';
        self::$dbPassword = getenv('NIGHTOWL_TEST_DB_PASSWORD') ?: 'test123';

        self::$pdo = self::connectPg();

        // Leftover tables from a prior run break MigrationRunner — it uses
        // Schema::create not createIfNotExists. Drop the agent's tables so
        // the harness subprocess can migrate from a clean slate.
        self::dropAgentTables();

        self::startAgent();
    }

    protected function setUp(): void
    {
        if (self::$pdo === null || self::$agentProcess === null) {
            $this->markTestSkipped('agent or pg unavailable.');
        }
        $this->sim = new NightwatchSimulator(self::TOKEN, self::AGENT_HOST, self::AGENT_PORT, timeout: 3.0);
        self::truncateAllTables();
    }

    public static function tearDownAfterClass(): void
    {
        // Make sure pg is back up no matter what the test left behind.
        if (isset(self::$containerName) && self::containerRunning(self::$containerName) === false) {
            self::dockerExec('start', self::$containerName);
            self::waitForPg(self::PG_RECOVERY_TIMEOUT);
        }

        self::stopAgent();
        self::$pdo = null;
    }

    // ─── The chaos run ─────────────────────────────────────────

    public function test_agent_survives_pg_outage_and_drains_on_recovery(): void
    {
        // 1. Baseline: pg up, a few rows make it through normally.
        $traceA = 'chaos-baseline-'.uniqid();
        $response = $this->sim->send([
            $this->sim->makeRequest(['trace_id' => $traceA, 'method' => 'GET', 'status_code' => 200]),
        ]);
        $this->assertSame('2:OK', $response, 'baseline ingest should succeed before outage');
        $this->waitForRow('nightowl_requests', "trace_id = '{$traceA}'", 15);

        // 2. Stop pg — graceful shutdown so libpq sees a clean refusal
        // on the drain worker's next attempt (paused containers hang
        // indefinitely, which would mask real failure behavior).
        self::dockerExec('stop', self::$containerName);
        $this->assertFalse(self::containerRunning(self::$containerName), 'pg container should be stopped');

        // 3. Burst phase: fill the buffer past MAX_PENDING_ROWS while pg is
        // down. Back-pressure is gated on a 5s periodic monitor (see
        // AsyncServer::BACK_PRESSURE_CHECK_INTERVAL) — the inline guard at
        // accept-time only flips after the monitor sets $backPressure=true.
        // So we burst here without expecting per-payload 5:ERROR, then sleep
        // through at least one monitor tick before probing.
        $burstSize = (int) (self::MAX_PENDING_ROWS * 1.5);
        $okCount = 0;
        $earlyErr = 0;
        for ($i = 0; $i < $burstSize; $i++) {
            $resp = $this->sim->send([
                $this->sim->makeRequest([
                    'trace_id' => 'chaos-outage-'.$i,
                    'method' => 'POST',
                    'status_code' => 500,
                ]),
            ]);
            if ($resp === '2:OK') {
                $okCount++;
            } elseif ($resp === '5:ERROR') {
                $earlyErr++;
            }
        }

        // Give the back-pressure monitor (5s cadence) a clean window to tick.
        // Pending rows in SQLite > MAX_PENDING_ROWS at this point, so the next
        // tick must flip $backPressure=true.
        sleep(7);

        // Probe phase: now expect 5:ERROR on subsequent sends.
        $probeOk = 0;
        $probeErr = 0;
        for ($i = 0; $i < 30; $i++) {
            $resp = $this->sim->send([
                $this->sim->makeRequest([
                    'trace_id' => 'chaos-probe-'.$i,
                    'method' => 'POST',
                    'status_code' => 500,
                ]),
            ]);
            if ($resp === '2:OK') {
                $probeOk++;
            } elseif ($resp === '5:ERROR') {
                $probeErr++;
            }
        }

        $this->assertGreaterThan(
            0,
            $okCount,
            'expected at least some payloads to be buffered before back-pressure kicked in'
        );
        $this->assertGreaterThan(
            0,
            $probeErr,
            sprintf(
                'expected back-pressure 5:ERROR on probe after monitor tick (burst ok=%d earlyErr=%d, probe ok=%d err=%d)',
                $okCount,
                $earlyErr,
                $probeOk,
                $probeErr,
            )
        );

        // 4. Bring pg back. Wait for it to actually accept connections.
        self::dockerExec('start', self::$containerName);
        self::waitForPg(self::PG_RECOVERY_TIMEOUT);

        // 5. Drain should catch up. Pending rows visible via the
        // drain-metrics file once the catch-up settles.
        $this->waitForDrainCatchup(self::DRAIN_CATCHUP_TIMEOUT);

        // 6. Verify ingest is healthy again — back-pressure should lift.
        $traceB = 'chaos-recovery-'.uniqid();
        $recoveryResp = null;
        $deadline = microtime(true) + 10;
        while (microtime(true) < $deadline) {
            $recoveryResp = $this->sim->send([
                $this->sim->makeRequest(['trace_id' => $traceB, 'method' => 'GET', 'status_code' => 200]),
            ]);
            if ($recoveryResp === '2:OK') {
                break;
            }
            usleep(500_000);
        }
        $this->assertSame('2:OK', $recoveryResp, 'agent should accept ingest again after recovery');
        $this->waitForRow('nightowl_requests', "trace_id = '{$traceB}'", 15);

        // 7. Verify the checkpoint path actually ran. With CHECKPOINT_INTERVAL_SECONDS=3
        // + CHECKPOINT_TRUNCATE_BYTES=256KB, TRUNCATE should have fired multiple times
        // during the outage and the catch-up phase.
        $metrics = $this->readDrainMetrics();
        $this->assertGreaterThan(
            0,
            $metrics['truncate_attempts'] ?? 0,
            'TRUNCATE checkpoint should have been attempted during the outage / catch-up — got: '.json_encode($metrics)
        );
        $this->assertGreaterThan(
            0,
            $metrics['truncate_successes'] ?? 0,
            sprintf(
                'expected at least one successful TRUNCATE (attempts=%d, failures=%d) — checkpoint may be getting starved by drain contention',
                $metrics['truncate_attempts'] ?? 0,
                $metrics['truncate_failures'] ?? 0,
            )
        );

        // Failures > successes would suggest the commenter's concern was right.
        // Don't hard-fail on a non-zero failure count (TRUNCATE retrying is
        // expected under heavy contention), but surface the ratio for review.
        $failures = $metrics['truncate_failures'] ?? 0;
        $successes = $metrics['truncate_successes'] ?? 0;
        $this->assertLessThanOrEqual(
            $successes,
            $failures,
            sprintf(
                'TRUNCATE failures (%d) exceeded successes (%d) — checkpoint is getting starved under contention',
                $failures,
                $successes,
            )
        );

        // 8. Final WAL integrity check on the buffer file the agent is using.
        $integrity = $this->probeIntegrityCheck();
        $this->assertSame('ok', $integrity, "PRAGMA integrity_check returned: {$integrity}");
    }

    // ─── Helpers ───────────────────────────────────────────────

    private static function dockerAvailable(): bool
    {
        exec('docker version --format "{{.Server.Version}}" 2>/dev/null', $out, $rc);

        return $rc === 0;
    }

    private static function containerRunning(string $name): bool
    {
        $cmd = sprintf(
            'docker ps --filter %s --format "{{.Names}}" 2>/dev/null',
            escapeshellarg('name=^'.preg_quote($name, '/').'$'),
        );
        exec($cmd, $out, $rc);

        return $rc === 0 && in_array($name, $out, true);
    }

    private static function dockerExec(string $verb, string $name): void
    {
        $cmd = sprintf('docker %s %s 2>&1', escapeshellarg($verb), escapeshellarg($name));
        exec($cmd, $out, $rc);
        if ($rc !== 0) {
            throw new \RuntimeException("docker {$verb} {$name} failed: ".implode("\n", $out));
        }
    }

    private static function waitForPg(float $timeout): void
    {
        $deadline = microtime(true) + $timeout;
        while (microtime(true) < $deadline) {
            try {
                self::$pdo = self::connectPg();

                return;
            } catch (\Throwable) {
                usleep(500_000);
            }
        }
        throw new \RuntimeException('pg did not come back within '.$timeout.'s');
    }

    private static function connectPg(): PDO
    {
        $dsn = sprintf('pgsql:host=%s;port=%d;dbname=%s', self::$dbHost, self::$dbPort, self::$dbDatabase);
        $pdo = new PDO($dsn, self::$dbUsername, self::$dbPassword);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        return $pdo;
    }

    private static function startAgent(): void
    {
        self::$sqlitePath = sys_get_temp_dir().'/nightowl-chaos-'.getmypid().'.sqlite';

        // Wipe any leftover from a prior failed run before launching.
        foreach ([self::$sqlitePath, self::$sqlitePath.'-wal', self::$sqlitePath.'-shm', self::$sqlitePath.'.drain-metrics.json'] as $f) {
            if (file_exists($f)) {
                @unlink($f);
            }
        }

        $harness = realpath(__DIR__.'/../Simulator/agent-harness-async.php');
        if (! $harness) {
            static::markTestSkipped('agent-harness-async.php not found.');
        }

        $cmd = sprintf(
            'exec php %s --token=%s --host=%s --port=%d --db-host=%s --db-port=%d --db-name=%s --db-user=%s --db-pass=%s --max-pending-rows=%d --drain-interval=50 --checkpoint-interval=%d --checkpoint-truncate-bytes=%d --sqlite-path=%s 2>&1',
            escapeshellarg($harness),
            escapeshellarg(self::TOKEN),
            escapeshellarg(self::AGENT_HOST),
            self::AGENT_PORT,
            escapeshellarg(self::$dbHost),
            self::$dbPort,
            escapeshellarg(self::$dbDatabase),
            escapeshellarg(self::$dbUsername),
            escapeshellarg(self::$dbPassword),
            self::MAX_PENDING_ROWS,
            self::CHECKPOINT_INTERVAL_SECONDS,
            self::CHECKPOINT_TRUNCATE_BYTES,
            escapeshellarg(self::$sqlitePath),
        );

        $descriptors = [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']];
        self::$agentProcess = proc_open($cmd, $descriptors, self::$agentPipes);

        if (! is_resource(self::$agentProcess)) {
            static::markTestSkipped('failed to start agent.');
        }

        stream_set_blocking(self::$agentPipes[1], false);

        $deadline = microtime(true) + self::STARTUP_TIMEOUT;
        $ready = false;
        while (microtime(true) < $deadline) {
            $sock = @stream_socket_client('tcp://'.self::AGENT_HOST.':'.self::AGENT_PORT, $errno, $errstr, 0.5);
            if ($sock) {
                fclose($sock);
                $ready = true;
                break;
            }
            usleep(100_000);
        }
        if (! $ready) {
            $output = stream_get_contents(self::$agentPipes[1]);
            self::stopAgent();
            static::markTestSkipped('agent did not start: '.$output);
        }
    }

    private static function stopAgent(): void
    {
        if (self::$agentProcess === null) {
            return;
        }

        $status = proc_get_status(self::$agentProcess);
        if ($status['running']) {
            posix_kill($status['pid'], SIGTERM);
            $deadline = microtime(true) + 10;
            while (microtime(true) < $deadline) {
                $check = proc_get_status(self::$agentProcess);
                if (! $check['running']) {
                    break;
                }
                usleep(100_000);
            }
            $check = proc_get_status(self::$agentProcess);
            if ($check['running']) {
                posix_kill($status['pid'], SIGKILL);
                usleep(200_000);
            }
        }

        foreach (self::$agentPipes as $p) {
            if (is_resource($p)) {
                fclose($p);
            }
        }
        proc_close(self::$agentProcess);
        self::$agentProcess = null;
        self::$agentPipes = [];

        foreach ([self::$sqlitePath, self::$sqlitePath.'-wal', self::$sqlitePath.'-shm', self::$sqlitePath.'.drain-metrics.json', self::$sqlitePath.'.drain-metrics.json.tmp'] as $f) {
            if (file_exists($f)) {
                @unlink($f);
            }
        }
    }

    private static function dropAgentTables(): void
    {
        $rows = self::$pdo->query(
            "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'nightowl_%'"
        )->fetchAll(PDO::FETCH_COLUMN);

        foreach ($rows as $table) {
            self::$pdo->exec("DROP TABLE IF EXISTS \"{$table}\" CASCADE");
        }

        // Also drop the migrations table so MigrationRunner starts clean.
        self::$pdo->exec('DROP TABLE IF EXISTS "migrations" CASCADE');
    }

    private static function truncateAllTables(): void
    {
        $tables = [
            'nightowl_issue_activity', 'nightowl_issue_comments', 'nightowl_issues',
            'nightowl_requests', 'nightowl_queries', 'nightowl_exceptions',
            'nightowl_commands', 'nightowl_jobs', 'nightowl_cache_events',
            'nightowl_mail', 'nightowl_notifications', 'nightowl_outgoing_requests',
            'nightowl_scheduled_tasks', 'nightowl_logs', 'nightowl_users',
            'nightowl_settings', 'nightowl_alert_channels',
        ];
        foreach ($tables as $t) {
            self::$pdo->exec("TRUNCATE TABLE {$t} CASCADE");
        }
    }

    private function waitForRow(string $table, string $where, float $timeout): void
    {
        $deadline = microtime(true) + $timeout;
        while (microtime(true) < $deadline) {
            $n = (int) self::$pdo->query("SELECT COUNT(*) FROM {$table} WHERE {$where}")->fetchColumn();
            if ($n >= 1) {
                return;
            }
            usleep(200_000);
        }
        $this->fail("drain timeout: no row in {$table} WHERE {$where}");
    }

    private function waitForDrainCatchup(float $timeout): void
    {
        // Catch-up is "no more pending rows" — easiest signal is that
        // the buffer count goes to zero. Read directly from the sqlite
        // file rather than racing the drain-metrics IPC.
        $deadline = microtime(true) + $timeout;
        $lastPending = PHP_INT_MAX;

        while (microtime(true) < $deadline) {
            $pending = $this->bufferPending();
            if ($pending === 0) {
                return;
            }
            $lastPending = $pending;
            usleep(500_000);
        }

        $this->fail("drain did not catch up within {$timeout}s — {$lastPending} rows still pending");
    }

    private function bufferPending(): int
    {
        try {
            $pdo = new PDO('sqlite:'.self::$sqlitePath);
            $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            $pdo->exec('PRAGMA busy_timeout=2000');

            return (int) $pdo->query('SELECT COUNT(*) FROM buffer WHERE synced = 0')->fetchColumn();
        } catch (\Throwable) {
            return PHP_INT_MAX;
        }
    }

    /** @return array<string, int|float> */
    private function readDrainMetrics(): array
    {
        $path = self::$sqlitePath.'.drain-metrics.json';

        // The drain worker writes metrics every 5s. Give it a beat to flush
        // the post-recovery state before reading.
        $deadline = microtime(true) + 8;
        while (microtime(true) < $deadline) {
            if (file_exists($path)) {
                $raw = @file_get_contents($path);
                if ($raw !== false) {
                    $decoded = json_decode($raw, true);
                    if (is_array($decoded) && isset($decoded['updated_at'])) {
                        return $decoded;
                    }
                }
            }
            usleep(500_000);
        }

        $this->fail("drain metrics file never appeared at: {$path}");
    }

    private function probeIntegrityCheck(): string
    {
        $pdo = new PDO('sqlite:'.self::$sqlitePath);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        return (string) $pdo->query('PRAGMA integrity_check')->fetchColumn();
    }
}
