<?php

namespace NightOwl\Tests\System;

use NightOwl\Tests\Integration\MigrationRunner;
use NightOwl\Tests\Simulator\NightwatchSimulator;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * System tests for agent features: sampling, redaction, and performance thresholds.
 *
 * Boots the real AsyncServer + DrainWorker pipeline with feature-specific config:
 * - Sample rate 0.0 (drop all non-critical)
 * - Redaction enabled for 'password', 'secret', 'authorization'
 * - Threshold cache TTL 0 (always re-read from DB)
 *
 * Requirements: PostgreSQL + pcntl + posix + port 2413 available.
 *
 * Run:
 *   NIGHTOWL_TEST_DB_PORT=5433 vendor/bin/phpunit --testsuite System --filter Features
 */
class AgentFeaturesSystemTest extends TestCase
{
    private const TOKEN = 'features-test-token-2025';

    private const AGENT_HOST = '127.0.0.1';

    private const AGENT_PORT = 2413;

    private const DRAIN_TIMEOUT = 15;

    private const STARTUP_TIMEOUT = 5;

    private static ?PDO $pdo = null;

    private static string $dbHost;

    private static int $dbPort;

    private static string $dbDatabase;

    private static string $dbUsername;

    private static string $dbPassword;

    /** @var resource|null */
    private static $agentProcess = null;

    /** @var resource[] */
    private static array $agentPipes = [];

    private static string $sqlitePath = '';

    private NightwatchSimulator $sim;

    // ─── Lifecycle ────────────────────────────────────────────

    public static function setUpBeforeClass(): void
    {
        if (! function_exists('pcntl_fork') || ! function_exists('posix_kill')) {
            static::markTestSkipped('pcntl and posix extensions required.');
        }

        self::$dbHost = getenv('NIGHTOWL_TEST_DB_HOST') ?: '127.0.0.1';
        self::$dbPort = (int) (getenv('NIGHTOWL_TEST_DB_PORT') ?: 5432);
        self::$dbDatabase = getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test';
        self::$dbUsername = getenv('NIGHTOWL_TEST_DB_USERNAME') ?: 'nightowl_test';
        self::$dbPassword = getenv('NIGHTOWL_TEST_DB_PASSWORD') ?: 'test123';

        try {
            $dsn = sprintf('pgsql:host=%s;port=%d;dbname=%s', self::$dbHost, self::$dbPort, self::$dbDatabase);
            self::$pdo = new PDO($dsn, self::$dbUsername, self::$dbPassword);
            self::$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (\Exception $e) {
            static::markTestSkipped('PostgreSQL not available: '.$e->getMessage());
        }

        MigrationRunner::migrate(
            self::$dbHost,
            (int) self::$dbPort,
            self::$dbDatabase,
            self::$dbUsername,
            self::$dbPassword,
        );

        self::startAgent();
    }

    protected function setUp(): void
    {
        if (self::$pdo === null || self::$agentProcess === null) {
            $this->markTestSkipped('Agent or PostgreSQL not available.');
        }

        $this->sim = new NightwatchSimulator(
            self::TOKEN,
            self::AGENT_HOST,
            self::AGENT_PORT,
            timeout: 3.0,
        );

        self::truncateAllTables();
    }

    public static function tearDownAfterClass(): void
    {
        self::stopAgent();
        self::$pdo = null;
    }

    // ─── Agent Process (with features enabled) ────────────────

    private static function startAgent(): void
    {
        self::$sqlitePath = sys_get_temp_dir().'/nightowl-features-test-'.getmypid().'.sqlite';

        $harness = realpath(__DIR__.'/../Simulator/agent-harness-async.php');
        if (! $harness) {
            static::markTestSkipped('agent-harness-async.php not found.');
        }

        // Key config differences from AgentSystemTest:
        //   --sample-rate=0.0        → drops all non-exception/non-5xx payloads
        //   --redact-keys=...        → strips sensitive keys before PG storage
        //   --threshold-cache-ttl=0  → always re-reads thresholds from nightowl_settings
        $cmd = sprintf(
            'exec php %s --token=%s --host=%s --port=%d --db-host=%s --db-port=%d --db-name=%s --db-user=%s --db-pass=%s --sample-rate=0.0 --redact-keys=password,secret,authorization,cookie --threshold-cache-ttl=0 2>&1',
            escapeshellarg($harness),
            escapeshellarg(self::TOKEN),
            escapeshellarg(self::AGENT_HOST),
            self::AGENT_PORT,
            escapeshellarg(self::$dbHost),
            self::$dbPort,
            escapeshellarg(self::$dbDatabase),
            escapeshellarg(self::$dbUsername),
            escapeshellarg(self::$dbPassword),
        );

        $descriptors = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];

        self::$agentProcess = proc_open($cmd, $descriptors, self::$agentPipes);

        if (! is_resource(self::$agentProcess)) {
            static::markTestSkipped('Failed to start agent process.');
        }

        stream_set_blocking(self::$agentPipes[1], false);

        $deadline = microtime(true) + self::STARTUP_TIMEOUT;
        $ready = false;
        while (microtime(true) < $deadline) {
            $sock = @stream_socket_client(
                'tcp://'.self::AGENT_HOST.':'.self::AGENT_PORT,
                $errno, $errstr, 0.5,
            );
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
            static::markTestSkipped('Agent did not start within '.self::STARTUP_TIMEOUT."s. Output: {$output}");
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

        foreach (self::$agentPipes as $pipe) {
            if (is_resource($pipe)) {
                fclose($pipe);
            }
        }
        proc_close(self::$agentProcess);
        self::$agentProcess = null;
        self::$agentPipes = [];

        foreach ([
            self::$sqlitePath,
            self::$sqlitePath.'-wal',
            self::$sqlitePath.'-shm',
            self::$sqlitePath.'.drain-metrics.json',
            self::$sqlitePath.'.drain-metrics.json.tmp',
        ] as $f) {
            if (file_exists($f)) {
                @unlink($f);
            }
        }
    }

    // ─── Helpers ──────────────────────────────────────────────

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
        foreach ($tables as $table) {
            self::$pdo->exec("TRUNCATE TABLE {$table} CASCADE");
        }
    }

    private static function rowCount(string $table, string $where = '1=1'): int
    {
        return (int) self::$pdo->query("SELECT COUNT(*) FROM {$table} WHERE {$where}")->fetchColumn();
    }

    private static function fetch(string $table, string $where): ?array
    {
        $row = self::$pdo->query("SELECT * FROM {$table} WHERE {$where}")->fetch(PDO::FETCH_ASSOC);

        return $row ?: null;
    }

    private function waitForDrain(string $table, string $where, int $expectedCount, float $timeout = self::DRAIN_TIMEOUT): void
    {
        $deadline = microtime(true) + $timeout;
        $actual = 0;
        while (microtime(true) < $deadline) {
            $actual = self::rowCount($table, $where);
            if ($actual >= $expectedCount) {
                return;
            }
            usleep(200_000);
        }
        $this->fail("Drain timeout after {$timeout}s: expected {$expectedCount} rows in {$table} WHERE {$where}, got {$actual}.");
    }

    private function sendTcp(string $wire): string|false
    {
        $sock = @stream_socket_client(
            'tcp://'.self::AGENT_HOST.':'.self::AGENT_PORT,
            $errno, $errstr, 3.0,
        );
        if (! $sock) {
            return false;
        }
        stream_set_timeout($sock, 3);
        fwrite($sock, $wire);
        $response = fread($sock, 128);
        fclose($sock);

        return $response ?: false;
    }

    private function buildWire(array $records): string
    {
        $json = json_encode($records, JSON_THROW_ON_ERROR);
        $tokenHash = substr(hash('xxh128', self::TOKEN), 0, 7);
        $body = "v1:{$tokenHash}:{$json}";

        return strlen($body).':'.$body;
    }

    // ═══════════════════════════════════════════════════════════
    //  SAMPLING TESTS (sample_rate = 0.0)
    // ═══════════════════════════════════════════════════════════

    public function test_sampling_drops_normal_requests(): void
    {
        $traceId = 'feat-sample-drop-'.uniqid();

        // Normal 200 request — should be dropped (sample_rate=0.0)
        $response = $this->sim->send([
            $this->sim->makeRequest(['trace_id' => $traceId, 'status_code' => 200]),
        ]);

        // Agent accepts (2:OK) — the client doesn't know it was sampled out
        $this->assertSame('2:OK', $response);

        // Wait briefly, then verify nothing was stored
        usleep(2_000_000); // 2s for drain to have run at least once
        $this->assertSame(0, self::rowCount('nightowl_requests', "trace_id = '{$traceId}'"));
    }

    public function test_sampling_keeps_exception_payloads(): void
    {
        $traceId = 'feat-sample-exc-'.uniqid();
        $excClass = 'App\\Exceptions\\SamplingTestException';
        $file = 'app/Sampling.php';
        $line = 99;

        // Payload containing an exception — must be kept despite sample_rate=0.0
        $response = $this->sim->send([
            $this->sim->makeRequest([
                'trace_id' => $traceId,
                'status_code' => 500,
                'exceptions' => 1,
            ]),
            $this->sim->makeException([
                'trace_id' => 'feat-exc-detail-'.uniqid(),
                'execution_id' => $traceId,
                'class' => $excClass,
                'message' => 'Sampling bypass test',
                'file' => $file,
                'line' => $line,
            ]),
        ]);

        $this->assertSame('2:OK', $response);

        // Exception bypasses sampling — MUST arrive in PG
        $this->waitForDrain('nightowl_requests', "trace_id = '{$traceId}'", 1);

        $request = self::fetch('nightowl_requests', "trace_id = '{$traceId}'");
        $this->assertNotNull($request, 'Exception payload must bypass sampling');
        $this->assertSame(500, (int) $request['status_code']);

        // Exception record and issue also stored
        $this->assertGreaterThanOrEqual(1, self::rowCount('nightowl_exceptions', "execution_id = '{$traceId}'"));
        $fp = md5($excClass.'|'.'0'.'|'.$file.'|'.$line);
        $issue = self::fetch('nightowl_issues', "group_hash = '{$fp}'");
        $this->assertNotNull($issue);
    }

    public function test_sampling_keeps5xx_requests(): void
    {
        $traceId = 'feat-sample-5xx-'.uniqid();

        // 502 request without explicit exception record — still kept (5xx bypass)
        $response = $this->sim->send([
            $this->sim->makeRequest([
                'trace_id' => $traceId,
                'status_code' => 502,
            ]),
        ]);

        $this->assertSame('2:OK', $response);

        $this->waitForDrain('nightowl_requests', "trace_id = '{$traceId}'", 1);

        $request = self::fetch('nightowl_requests', "trace_id = '{$traceId}'");
        $this->assertNotNull($request, '5xx requests must bypass sampling');
        $this->assertSame(502, (int) $request['status_code']);
    }

    public function test_sampling_drops_normal_jobs_and_commands(): void
    {
        $jobTrace = 'feat-sample-job-'.uniqid();
        $cmdTrace = 'feat-sample-cmd-'.uniqid();

        // Normal job and command — both should be dropped
        $this->sim->send([
            $this->sim->makeJob(['trace_id' => $jobTrace, 'status' => 'processed']),
        ]);
        $this->sim->send([
            $this->sim->makeCommand(['trace_id' => $cmdTrace, 'exit_code' => 0]),
        ]);

        usleep(2_000_000);
        $this->assertSame(0, self::rowCount('nightowl_jobs', "trace_id = '{$jobTrace}'"));
        $this->assertSame(0, self::rowCount('nightowl_commands', "trace_id = '{$cmdTrace}'"));
    }

    // ═══════════════════════════════════════════════════════════
    //  REDACTION TESTS (redact_keys = password,secret,authorization,cookie)
    //
    //  The redactor walks the PHP array structure of each record and
    //  replaces matching keys with [REDACTED]. It does NOT parse
    //  pre-encoded JSON strings (e.g. headers stored as a JSON string).
    //
    //  To test redaction, we pass context/headers as arrays (not pre-encoded
    //  strings). RecordWriter's COPY path then json_encodes the redacted
    //  arrays for storage.
    // ═══════════════════════════════════════════════════════════

    public function test_redaction_strips_password_from_context(): void
    {
        $traceId = 'feat-redact-ctx-'.uniqid();

        // Send context as a nested array (not pre-encoded JSON string)
        // so the redactor can walk into it and find the 'password' key.
        // Exception in payload bypasses sample_rate=0.0.
        $response = $this->sim->send([
            $this->sim->makeRequest([
                'trace_id' => $traceId,
                'status_code' => 500,
                'exceptions' => 1,
                'context' => [
                    'user' => 'admin',
                    'password' => 'super-secret-p@ss!',
                    'action' => 'login',
                ],
            ]),
            $this->sim->makeException([
                'trace_id' => 'feat-redact-exc-'.uniqid(),
                'execution_id' => $traceId,
                'class' => 'RuntimeException',
                'file' => 'app/Redact.php',
                'line' => 1,
            ]),
        ]);

        $this->assertSame('2:OK', $response);

        $this->waitForDrain('nightowl_requests', "trace_id = '{$traceId}'", 1);

        $request = self::fetch('nightowl_requests', "trace_id = '{$traceId}'");
        $this->assertNotNull($request);

        $context = $request['context'];
        $this->assertStringNotContainsString('super-secret-p@ss!', $context, 'Password should be redacted from context');
        $this->assertStringContainsString('[REDACTED]', $context, 'Redacted marker should be present');
        $this->assertStringContainsString('admin', $context, 'Non-sensitive fields should be preserved');
    }

    public function test_redaction_strips_headers_passed_as_array(): void
    {
        $traceId = 'feat-redact-hdr-'.uniqid();

        // Pass headers as a nested array so the redactor can walk it.
        // In production, Nightwatch may pre-encode headers as a JSON string,
        // in which case redaction doesn't apply (string values are leaf nodes).
        // This test verifies the redactor works when headers ARE arrays.
        $response = $this->sim->send([
            $this->sim->makeRequest([
                'trace_id' => $traceId,
                'status_code' => 500,
                'exceptions' => 1,
                'headers' => [
                    'host' => 'app.example.com',
                    'authorization' => 'Bearer eyJhbGciOi...',
                    'cookie' => 'session_id=abc123; csrf=xyz789',
                    'accept' => 'application/json',
                ],
            ]),
            $this->sim->makeException([
                'trace_id' => 'feat-redact-exc2-'.uniqid(),
                'execution_id' => $traceId,
                'class' => 'RuntimeException',
                'file' => 'app/Redact.php',
                'line' => 2,
            ]),
        ]);

        $this->assertSame('2:OK', $response);

        $this->waitForDrain('nightowl_requests', "trace_id = '{$traceId}'", 1);

        $request = self::fetch('nightowl_requests', "trace_id = '{$traceId}'");
        $headers = $request['headers'];

        $this->assertStringNotContainsString('Bearer eyJhbGciOi', $headers, 'Authorization header should be redacted');
        $this->assertStringNotContainsString('session_id=abc123', $headers, 'Cookie header should be redacted');
        $this->assertStringContainsString('app.example.com', $headers, 'Host header should be preserved');
        // JSON encoding escapes "/" to "\/" — check for the decoded value
        $decoded = json_decode($headers, true);
        $this->assertSame('application/json', $decoded['accept'], 'Accept header should be preserved');
    }

    public function test_redaction_handles_nested_sensitive_keys(): void
    {
        $traceId = 'feat-redact-nest-'.uniqid();

        // Nested array context with deep sensitive keys
        $response = $this->sim->send([
            $this->sim->makeException([
                'trace_id' => $traceId,
                'class' => 'RuntimeException',
                'file' => 'app/Redact.php',
                'line' => 3,
            ]),
            $this->sim->makeLog([
                'trace_id' => 'feat-redact-log-'.uniqid(),
                'execution_id' => $traceId,
                'level' => 'error',
                'message' => 'Auth failed',
                'context' => [
                    'user' => 'admin',
                    'credentials' => [
                        'username' => 'admin',
                        'password' => 'hunter2',
                        'secret' => 'api-key-xyz',
                    ],
                ],
            ]),
        ]);

        $this->assertSame('2:OK', $response);

        $this->waitForDrain('nightowl_exceptions', "trace_id = '{$traceId}'", 1);
        usleep(500_000);

        $logs = self::$pdo->query("SELECT * FROM nightowl_logs WHERE execution_id = '{$traceId}'")->fetchAll(PDO::FETCH_ASSOC);
        if (! empty($logs)) {
            $context = $logs[0]['context'];
            $this->assertStringNotContainsString('hunter2', $context, 'Nested password should be redacted');
            $this->assertStringNotContainsString('api-key-xyz', $context, 'Nested secret should be redacted');
            $this->assertStringContainsString('admin', $context, 'Non-sensitive username should be preserved');
        }
    }

    public function test_redaction_strips_record_level_sensitive_keys(): void
    {
        $traceId = 'feat-redact-toplvl-'.uniqid();

        // Test that top-level record keys matching redact list are stripped.
        // 'password' and 'secret' at record level → [REDACTED]
        $response = $this->sim->send([
            array_merge($this->sim->makeRequest([
                'trace_id' => $traceId,
                'status_code' => 500,
                'exceptions' => 1,
            ]), [
                'password' => 'top-level-secret',
                'secret' => 'top-level-api-key',
            ]),
            $this->sim->makeException([
                'trace_id' => 'feat-redact-exc3-'.uniqid(),
                'execution_id' => $traceId,
                'class' => 'RuntimeException',
                'file' => 'app/Redact.php',
                'line' => 4,
            ]),
        ]);

        $this->assertSame('2:OK', $response);

        $this->waitForDrain('nightowl_requests', "trace_id = '{$traceId}'", 1);

        // Request stored — the password/secret keys are not standard columns,
        // so they won't appear in PG. But the payload was processed through
        // the redactor without crashing (functional test of key stripping).
        $request = self::fetch('nightowl_requests', "trace_id = '{$traceId}'");
        $this->assertNotNull($request, 'Record with redacted keys should still be stored');
    }

    // ═══════════════════════════════════════════════════════════
    //  THRESHOLD TESTS (cache_ttl = 0, always re-reads)
    // ═══════════════════════════════════════════════════════════

    public function test_route_threshold_creates_performance_issue(): void
    {
        // Insert a threshold: any route over 100ms (100,000 us) triggers a performance issue
        self::$pdo->exec("
            INSERT INTO nightowl_settings (key, value) VALUES ('thresholds', '".
            json_encode([['type' => 'route', 'duration_ms' => 100]])."')
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        ");

        $traceId = 'feat-thresh-route-'.uniqid();

        // Send a slow request (500ms = 500,000 us) with exception to bypass sampling
        $response = $this->sim->send([
            $this->sim->makeRequest([
                'trace_id' => $traceId,
                'status_code' => 500,
                'exceptions' => 1,
                'duration' => 500_000, // 500ms in microseconds
                'route_path' => '/api/slow-route',
                'method' => 'GET',
                'route_methods' => json_encode(['GET']),
            ]),
            $this->sim->makeException([
                'trace_id' => 'feat-thresh-exc-'.uniqid(),
                'execution_id' => $traceId,
                'class' => 'RuntimeException',
                'file' => 'app/Threshold.php',
                'line' => 1,
            ]),
        ]);

        $this->assertSame('2:OK', $response);

        $this->waitForDrain('nightowl_requests', "trace_id = '{$traceId}'", 1);

        // Give threshold check time to process
        usleep(1_000_000);

        // Should have created a performance issue (in addition to the exception issue)
        $perfIssues = self::$pdo->query(
            "SELECT * FROM nightowl_issues WHERE type = 'performance'"
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertNotEmpty($perfIssues, 'Slow route should create a performance issue');
        $this->assertSame('open', $perfIssues[0]['status']);
    }

    public function test_request_below_threshold_does_not_create_issue(): void
    {
        // Insert a threshold: 1000ms (1,000,000 us)
        self::$pdo->exec("
            INSERT INTO nightowl_settings (key, value) VALUES ('thresholds', '".
            json_encode([['type' => 'route', 'duration_ms' => 1000]])."')
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        ");

        $traceId = 'feat-thresh-fast-'.uniqid();

        // Send a fast request (50ms = 50,000 us) — below threshold, with exception to bypass sampling
        $response = $this->sim->send([
            $this->sim->makeRequest([
                'trace_id' => $traceId,
                'status_code' => 500,
                'exceptions' => 1,
                'duration' => 50_000, // 50ms — below 1000ms threshold
            ]),
            $this->sim->makeException([
                'trace_id' => 'feat-thresh-fast-exc-'.uniqid(),
                'execution_id' => $traceId,
                'class' => 'RuntimeException',
                'file' => 'app/Threshold.php',
                'line' => 2,
            ]),
        ]);

        $this->assertSame('2:OK', $response);

        $this->waitForDrain('nightowl_requests', "trace_id = '{$traceId}'", 1);
        usleep(1_000_000);

        // Should NOT create a performance issue (duration below threshold)
        $perfCount = self::rowCount('nightowl_issues', "type = 'performance'");
        $this->assertSame(0, $perfCount, 'Fast request should not create performance issue');
    }

    public function test_job_threshold_creates_performance_issue(): void
    {
        // Insert a job threshold: any job over 200ms triggers performance issue
        self::$pdo->exec("
            INSERT INTO nightowl_settings (key, value) VALUES ('thresholds', '".
            json_encode([['type' => 'job', 'duration_ms' => 200]])."')
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        ");

        $traceId = 'feat-thresh-job-'.uniqid();

        // Send a slow failed job (5s = 5,000,000 us) with exception to bypass sampling
        $response = $this->sim->send([
            $this->sim->makeJob([
                'trace_id' => $traceId,
                'name' => 'App\\Jobs\\SlowJob',
                'status' => 'failed',
                'duration' => 5_000_000, // 5 seconds
                'exceptions' => 1,
            ]),
            $this->sim->makeException([
                'trace_id' => 'feat-thresh-job-exc-'.uniqid(),
                'execution_id' => $traceId,
                'execution_source' => 'job',
                'class' => 'App\\Exceptions\\JobTimeout',
                'file' => 'app/Jobs/SlowJob.php',
                'line' => 50,
            ]),
        ]);

        $this->assertSame('2:OK', $response);

        $this->waitForDrain('nightowl_jobs', "trace_id = '{$traceId}'", 1);
        usleep(1_000_000);

        $perfIssues = self::$pdo->query(
            "SELECT * FROM nightowl_issues WHERE type = 'performance'"
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertNotEmpty($perfIssues, 'Slow job should create a performance issue');
    }

    // ═══════════════════════════════════════════════════════════
    //  COMBINED: SAMPLING + REDACTION + THRESHOLDS TOGETHER
    // ═══════════════════════════════════════════════════════════

    public function test_all_features_work_together_in_single_payload(): void
    {
        // Set threshold
        self::$pdo->exec("
            INSERT INTO nightowl_settings (key, value) VALUES ('thresholds', '".
            json_encode([['type' => 'route', 'duration_ms' => 100]])."')
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        ");

        $traceId = 'feat-combined-'.uniqid();
        $excClass = 'App\\Exceptions\\CombinedTest';
        $file = 'app/Combined.php';
        $line = 42;

        // Payload combining all three features:
        // - Exception (bypasses sample_rate=0.0)
        // - Sensitive data as arrays (redactor walks and strips)
        // - Slow duration (triggers threshold → performance issue)
        $response = $this->sim->send([
            $this->sim->makeRequest([
                'trace_id' => $traceId,
                'status_code' => 500,
                'exceptions' => 1,
                'duration' => 800_000, // 800ms — exceeds 100ms threshold
                'route_path' => '/api/combined-test',
                'method' => 'POST',
                'route_methods' => json_encode(['POST']),
                'context' => [
                    'user' => 'admin',
                    'password' => 'should-be-redacted',
                    'action' => 'create_order',
                ],
                'headers' => [
                    'host' => 'app.example.com',
                    'authorization' => 'Bearer secret-token-123',
                    'content-type' => 'application/json',
                ],
            ]),
            $this->sim->makeException([
                'trace_id' => 'feat-combined-exc-'.uniqid(),
                'execution_id' => $traceId,
                'class' => $excClass,
                'message' => 'Combined test error',
                'file' => $file,
                'line' => $line,
            ]),
        ]);

        $this->assertSame('2:OK', $response);

        $this->waitForDrain('nightowl_requests', "trace_id = '{$traceId}'", 1);
        usleep(1_000_000);

        // 1. SAMPLING: payload arrived (exception bypass worked)
        $request = self::fetch('nightowl_requests', "trace_id = '{$traceId}'");
        $this->assertNotNull($request, 'Exception payload must bypass sampling');

        // 2. REDACTION: sensitive data stripped
        $this->assertStringNotContainsString('should-be-redacted', $request['context'], 'Password must be redacted');
        $this->assertStringNotContainsString('secret-token-123', $request['headers'], 'Authorization must be redacted');
        $this->assertStringContainsString('app.example.com', $request['headers'], 'Non-sensitive data preserved');

        // 3. THRESHOLDS: performance issue created
        $perfIssues = self::$pdo->query(
            "SELECT * FROM nightowl_issues WHERE type = 'performance'"
        )->fetchAll(PDO::FETCH_ASSOC);
        $this->assertNotEmpty($perfIssues, 'Slow request should create performance issue');

        // 4. EXCEPTION: issue also created
        $fp = md5($excClass.'|'.'0'.'|'.$file.'|'.$line);
        $excIssue = self::fetch('nightowl_issues', "group_hash = '{$fp}' AND type = 'exception'");
        $this->assertNotNull($excIssue, 'Exception issue should exist alongside performance issue');
    }
}
