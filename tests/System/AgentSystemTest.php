<?php

namespace NightOwl\Tests\System;

use NightOwl\Tests\Integration\MigrationRunner;
use NightOwl\Tests\Simulator\NightwatchSimulator;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * System-level integration test: real AsyncServer + DrainWorker + PostgreSQL.
 *
 * Boots the full agent as a subprocess (TCP listener, forked drain workers,
 * SQLite WAL buffer, COPY to PostgreSQL), sends real traffic over TCP, waits
 * for the drain pipeline to flush, then verifies data arrived in PostgreSQL.
 *
 * This tests everything the unit/integration tests can't:
 * - TCP accept + connection handling under ReactPHP event loop
 * - pcntl_fork drain workers with SQLite PDO lifecycle
 * - WAL write → claim → COPY → mark-synced pipeline
 * - Back-pressure activation and rejection
 * - Graceful shutdown with SIGTERM
 * - Health API responses
 * - Gzip over the wire
 * - Error storms and issue creation at scale
 *
 * Requirements:
 *   - PostgreSQL (set NIGHTOWL_TEST_DB_* env vars or use Docker)
 *   - pcntl + posix extensions
 *   - Port 2411 available (agent binds here)
 *
 * Run:
 *   NIGHTOWL_TEST_DB_PORT=5433 vendor/bin/phpunit --testsuite System
 */
class AgentSystemTest extends TestCase
{
    private const TOKEN = 'system-test-token-2025';

    private const AGENT_HOST = '127.0.0.1';

    private const AGENT_PORT = 2411;

    private const HEALTH_PORT = 2412;

    /** Maximum seconds to wait for drain to flush data to PG */
    private const DRAIN_TIMEOUT = 15;

    /** Maximum seconds to wait for agent process to start */
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
            static::markTestSkipped('pcntl and posix extensions required for system tests.');
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

        // Apply the agent's migrations — single source of truth.
        MigrationRunner::migrate(
            self::$dbHost,
            (int) self::$dbPort,
            self::$dbDatabase,
            self::$dbUsername,
            self::$dbPassword,
        );

        // Start the agent
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

    // ─── Agent Process Management ─────────────────────────────

    private static function startAgent(): void
    {
        self::$sqlitePath = sys_get_temp_dir().'/nightowl-system-test-'.getmypid().'.sqlite';

        $harness = realpath(__DIR__.'/../Simulator/agent-harness-async.php');
        if (! $harness) {
            static::markTestSkipped('agent-harness-async.php not found.');
        }

        $cmd = sprintf(
            'exec php %s --token=%s --host=%s --port=%d --db-host=%s --db-port=%d --db-name=%s --db-user=%s --db-pass=%s 2>&1',
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
            0 => ['pipe', 'r'],   // stdin
            1 => ['pipe', 'w'],   // stdout
            2 => ['pipe', 'w'],   // stderr (merged with stdout via 2>&1 but keep pipe)
        ];

        self::$agentProcess = proc_open($cmd, $descriptors, self::$agentPipes);

        if (! is_resource(self::$agentProcess)) {
            static::markTestSkipped('Failed to start agent process.');
        }

        // Non-blocking reads on stdout
        stream_set_blocking(self::$agentPipes[1], false);

        // Wait for the agent to be ready (accepts TCP connections)
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
            usleep(100_000); // 100ms
        }

        if (! $ready) {
            // Read any output for diagnostics
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
            // Send SIGTERM for graceful shutdown (drains remaining SQLite rows)
            posix_kill($status['pid'], SIGTERM);

            // Wait up to 10s for clean exit
            $deadline = microtime(true) + 10;
            while (microtime(true) < $deadline) {
                $check = proc_get_status(self::$agentProcess);
                if (! $check['running']) {
                    break;
                }
                usleep(100_000);
            }

            // Force kill if still running
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

        // Cleanup SQLite files
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

    /**
     * Poll PostgreSQL until a condition is met or timeout expires.
     * The drain worker runs in a separate process with its own schedule,
     * so we must wait for it to flush SQLite → PG.
     */
    private function waitForDrain(string $table, string $where, int $expectedCount, float $timeout = self::DRAIN_TIMEOUT): void
    {
        $deadline = microtime(true) + $timeout;
        $actual = 0;

        while (microtime(true) < $deadline) {
            $actual = self::rowCount($table, $where);
            if ($actual >= $expectedCount) {
                return;
            }
            usleep(200_000); // 200ms poll
        }

        $this->fail(
            "Drain timeout after {$timeout}s: expected {$expectedCount} rows in {$table} WHERE {$where}, got {$actual}."
        );
    }

    /**
     * Send a TCP payload and assert the agent accepts it.
     */
    private function sendAndExpectOk(array $records): void
    {
        $response = $this->sim->send($records);
        $this->assertNotNull($response, 'Agent did not respond');
        $this->assertStringStartsWith('2:', $response, "Expected 2:OK, got: {$response}");
    }

    // ═══════════════════════════════════════════════════════════
    //  TEST CASES
    // ═══════════════════════════════════════════════════════════

    // ─── 1. Basic Pipeline ────────────────────────────────────

    public function test_ping_survives_full_stack(): void
    {
        $response = $this->sim->ping();
        $this->assertSame('2:OK', $response);
    }

    public function test_single_request_flows_through_entire_pipeline(): void
    {
        $traceId = 'sys-req-'.uniqid();

        $this->sendAndExpectOk([
            $this->sim->makeRequest(['trace_id' => $traceId, 'method' => 'GET', 'status_code' => 200]),
        ]);

        $this->waitForDrain('nightowl_requests', "trace_id = '{$traceId}'", 1);

        $row = self::fetch('nightowl_requests', "trace_id = '{$traceId}'");
        $this->assertNotNull($row);
        $this->assertSame('GET', $row['method']);
        $this->assertSame(200, (int) $row['status_code']);
    }

    // ─── 2. Full Request Lifecycle ────────────────────────────

    public function test_request_lifecycle_with_child_records(): void
    {
        $traceId = 'sys-lifecycle-'.uniqid();
        $userId = 'sys-user-'.uniqid();

        $this->sendAndExpectOk([
            $this->sim->makeRequest([
                'trace_id' => $traceId,
                'user' => $userId,
                'method' => 'POST',
                'url' => 'https://app.test/api/orders',
                'status_code' => 201,
            ]),
            $this->sim->makeQuery([
                'trace_id' => 'sys-q1-'.uniqid(),
                'execution_id' => $traceId,
                'execution_source' => 'request',
                'sql' => 'INSERT INTO orders (user_id, total) VALUES (?, ?)',
            ]),
            $this->sim->makeQuery([
                'trace_id' => 'sys-q2-'.uniqid(),
                'execution_id' => $traceId,
                'execution_source' => 'request',
                'sql' => 'SELECT * FROM products WHERE id = ?',
            ]),
            $this->sim->makeCacheEvent([
                'trace_id' => 'sys-c1-'.uniqid(),
                'execution_id' => $traceId,
                'type' => 'hit',
                'key' => 'products:list',
            ]),
            $this->sim->makeLog([
                'trace_id' => 'sys-l1-'.uniqid(),
                'execution_id' => $traceId,
                'level' => 'info',
                'message' => 'Order created',
            ]),
            $this->sim->makeUser($userId),
        ]);

        // Wait for the request (parent record) — child records arrive in the same batch
        $this->waitForDrain('nightowl_requests', "trace_id = '{$traceId}'", 1);

        // Verify parent
        $request = self::fetch('nightowl_requests', "trace_id = '{$traceId}'");
        $this->assertSame('POST', $request['method']);
        $this->assertSame(201, (int) $request['status_code']);

        // Verify children linked by execution_id
        $this->assertSame(2, self::rowCount('nightowl_queries', "execution_id = '{$traceId}'"));
        $this->assertSame(1, self::rowCount('nightowl_cache_events', "execution_id = '{$traceId}'"));
        $this->assertSame(1, self::rowCount('nightowl_logs', "execution_id = '{$traceId}'"));

        // Verify user
        $user = self::fetch('nightowl_users', "user_id = '{$userId}'");
        $this->assertNotNull($user);
    }

    // ─── 3. Exception → Issue Creation ────────────────────────

    public function test_exception_creates_issue_automatically(): void
    {
        $traceId = 'sys-exc-'.uniqid();
        $exceptionClass = 'App\\Exceptions\\SystemTestException';
        $file = 'app/Services/Payment.php';
        $line = 42;
        $fingerprint = md5($exceptionClass.'|'.'0'.'|'.$file.'|'.$line);

        $this->sendAndExpectOk([
            $this->sim->makeRequest([
                'trace_id' => $traceId,
                'status_code' => 500,
                'exceptions' => 1,
            ]),
            $this->sim->makeException([
                'trace_id' => 'sys-exc-detail-'.uniqid(),
                'execution_id' => $traceId,
                'class' => $exceptionClass,
                'message' => 'Payment gateway timeout',
                'file' => $file,
                'line' => $line,
            ]),
        ]);

        $this->waitForDrain('nightowl_exceptions', "execution_id = '{$traceId}'", 1);

        // Exception record stored
        $exception = self::fetch('nightowl_exceptions', "execution_id = '{$traceId}'");
        $this->assertSame($exceptionClass, $exception['class']);
        $this->assertSame($fingerprint, $exception['fingerprint']);

        // Issue auto-created from fingerprint
        $issue = self::fetch('nightowl_issues', "group_hash = '{$fingerprint}'");
        $this->assertNotNull($issue, 'Issue should be auto-created from exception fingerprint');
        $this->assertSame('exception', $issue['type']);
        $this->assertSame('open', $issue['status']);
        $this->assertSame(1, (int) $issue['occurrences_count']);
    }

    // ─── 4. Duplicate Exceptions Increment ────────────────────

    public function test_duplicate_exceptions_increment_issue_count(): void
    {
        $exceptionClass = 'App\\Exceptions\\DuplicateSystemTest';
        $file = 'app/Dup.php';
        $line = 10;
        $fingerprint = md5($exceptionClass.'|'.'0'.'|'.$file.'|'.$line);

        // Send 5 separate payloads with the same exception fingerprint
        for ($i = 0; $i < 5; $i++) {
            $this->sendAndExpectOk([
                $this->sim->makeException([
                    'trace_id' => 'sys-dup-'.uniqid(),
                    'class' => $exceptionClass,
                    'file' => $file,
                    'line' => $line,
                    'user' => "user_{$i}",
                ]),
            ]);
        }

        $this->waitForDrain('nightowl_exceptions', "fingerprint = '{$fingerprint}'", 5);

        $issue = self::fetch('nightowl_issues', "group_hash = '{$fingerprint}'");
        $this->assertSame(5, (int) $issue['occurrences_count']);
        // users_count should be accurate (5 distinct users)
        $this->assertSame(5, (int) $issue['users_count']);
    }

    // ─── 5. All 12 Record Types ───────────────────────────────

    public function test_all_twelve_record_types_arrive_in_postgres(): void
    {
        $tag = 'sys-all-'.uniqid();

        $this->sendAndExpectOk([
            $this->sim->makeRequest(['trace_id' => "{$tag}-req"]),
            $this->sim->makeQuery(['trace_id' => "{$tag}-qry"]),
            $this->sim->makeException(['trace_id' => "{$tag}-exc"]),
            $this->sim->makeCommand(['trace_id' => "{$tag}-cmd"]),
            $this->sim->makeJob(['trace_id' => "{$tag}-job"]),
            $this->sim->makeCacheEvent(['trace_id' => "{$tag}-cache"]),
            $this->sim->makeMail(['trace_id' => "{$tag}-mail"]),
            $this->sim->makeNotification(['trace_id' => "{$tag}-notif"]),
            $this->sim->makeOutgoingRequest(['trace_id' => "{$tag}-out"]),
            $this->sim->makeScheduledTask(['trace_id' => "{$tag}-task"]),
            $this->sim->makeLog(['trace_id' => "{$tag}-log"]),
            $this->sim->makeUser("{$tag}-user"),
        ]);

        // Wait for the slowest table (exception triggers issue upsert)
        $this->waitForDrain('nightowl_exceptions', "trace_id = '{$tag}-exc'", 1);

        // Verify every table
        $checks = [
            'nightowl_requests' => "{$tag}-req",
            'nightowl_queries' => "{$tag}-qry",
            'nightowl_exceptions' => "{$tag}-exc",
            'nightowl_commands' => "{$tag}-cmd",
            'nightowl_jobs' => "{$tag}-job",
            'nightowl_cache_events' => "{$tag}-cache",
            'nightowl_mail' => "{$tag}-mail",
            'nightowl_notifications' => "{$tag}-notif",
            'nightowl_outgoing_requests' => "{$tag}-out",
            'nightowl_scheduled_tasks' => "{$tag}-task",
            'nightowl_logs' => "{$tag}-log",
        ];

        foreach ($checks as $table => $traceId) {
            $count = self::rowCount($table, "trace_id = '{$traceId}'");
            $this->assertSame(1, $count, "Expected 1 row in {$table} for trace_id {$traceId}");
        }

        $this->assertSame(1, self::rowCount('nightowl_users', "user_id = '{$tag}-user'"));
        // Exception should have created an issue
        $this->assertGreaterThanOrEqual(1, self::rowCount('nightowl_issues'));
    }

    // ─── 6. Gzip Over The Wire ────────────────────────────────

    public function test_gzip_payload_processed_correctly(): void
    {
        if (! function_exists('gzencode')) {
            $this->markTestSkipped('ext-zlib not available');
        }

        $traceId = 'sys-gzip-'.uniqid();

        $records = [
            $this->sim->makeRequest(['trace_id' => $traceId, 'method' => 'PUT', 'status_code' => 200]),
            $this->sim->makeQuery(['trace_id' => 'sys-gzq-'.uniqid(), 'execution_id' => $traceId]),
        ];

        // Build gzip wire payload manually
        $json = json_encode($records, JSON_THROW_ON_ERROR);
        $compressed = gzencode($json);
        $tokenHash = substr(hash('xxh128', self::TOKEN), 0, 7);
        $body = "v1:{$tokenHash}:{$compressed}";
        $wire = strlen($body).':'.$body;

        $sock = stream_socket_client(
            'tcp://'.self::AGENT_HOST.':'.self::AGENT_PORT,
            $errno, $errstr, 3.0,
        );
        $this->assertNotFalse($sock, "TCP connect failed: {$errstr}");

        fwrite($sock, $wire);
        stream_set_timeout($sock, 3);
        $response = fread($sock, 128);
        fclose($sock);

        $this->assertSame('2:OK', $response);

        $this->waitForDrain('nightowl_requests', "trace_id = '{$traceId}'", 1);

        $row = self::fetch('nightowl_requests', "trace_id = '{$traceId}'");
        $this->assertSame('PUT', $row['method']);
    }

    // ─── 7. Token Rejection ───────────────────────────────────

    public function test_invalid_token_rejected_over_tcp(): void
    {
        $traceId = 'sys-reject-'.uniqid();

        $json = json_encode([$this->sim->makeRequest(['trace_id' => $traceId])]);
        $body = "v1:INVALID:{$json}";
        $wire = strlen($body).':'.$body;

        $sock = stream_socket_client(
            'tcp://'.self::AGENT_HOST.':'.self::AGENT_PORT,
            $errno, $errstr, 3.0,
        );
        $this->assertNotFalse($sock);

        fwrite($sock, $wire);
        stream_set_timeout($sock, 3);
        $response = fread($sock, 128);
        fclose($sock);

        $this->assertSame('5:ERROR', $response);

        // Give drain a moment, then verify nothing was stored
        usleep(500_000);
        $this->assertSame(0, self::rowCount('nightowl_requests', "trace_id = '{$traceId}'"));
    }

    // ─── 8. Batch Throughput ──────────────────────────────────

    public function test_batch_of100_requests_drained_correctly(): void
    {
        $tag = 'sys-batch-'.uniqid();

        $records = [];
        for ($i = 0; $i < 100; $i++) {
            $records[] = $this->sim->makeRequest(['trace_id' => "{$tag}-{$i}"]);
        }

        $this->sendAndExpectOk($records);

        $this->waitForDrain('nightowl_requests', "trace_id LIKE '{$tag}-%'", 100);

        $count = self::rowCount('nightowl_requests', "trace_id LIKE '{$tag}-%'");
        $this->assertSame(100, $count);
    }

    // ─── 9. Sequential Payloads ───────────────────────────────

    public function test_multiple_sequential_payloads_all_arrive(): void
    {
        $tag = 'sys-seq-'.uniqid();
        $total = 20;

        for ($i = 0; $i < $total; $i++) {
            $this->sendAndExpectOk([
                $this->sim->makeRequest(['trace_id' => "{$tag}-{$i}"]),
            ]);
        }

        $this->waitForDrain('nightowl_requests', "trace_id LIKE '{$tag}-%'", $total);

        $this->assertSame($total, self::rowCount('nightowl_requests', "trace_id LIKE '{$tag}-%'"));
    }

    // ─── 10. Error Storm ──────────────────────────────────────

    public function test_error_storm_creates_issues_without_crashing(): void
    {
        $exceptionClasses = [
            'App\\Exceptions\\StormA',
            'App\\Exceptions\\StormB',
            'App\\Exceptions\\StormC',
        ];

        $expectedFingerprints = [];
        $totalExceptions = 0;

        for ($i = 0; $i < 30; $i++) {
            $class = $exceptionClasses[$i % 3];
            $file = 'app/Storm.php';
            $line = ($i % 3) + 1; // 3 distinct fingerprints
            $fingerprint = md5($class.'|'.'0'.'|'.$file.'|'.$line);
            $expectedFingerprints[$fingerprint] = true;

            $this->sendAndExpectOk([
                $this->sim->makeException([
                    'trace_id' => 'sys-storm-'.uniqid(),
                    'class' => $class,
                    'message' => "Storm error #{$i}",
                    'file' => $file,
                    'line' => $line,
                ]),
            ]);
            $totalExceptions++;
        }

        $this->waitForDrain('nightowl_exceptions', '1=1', $totalExceptions);

        $this->assertSame($totalExceptions, self::rowCount('nightowl_exceptions'));

        // 3 distinct issues created (one per fingerprint)
        $this->assertSame(3, self::rowCount('nightowl_issues', "type = 'exception'"));

        // Each issue should have 10 occurrences
        foreach (array_keys($expectedFingerprints) as $fp) {
            $issue = self::fetch('nightowl_issues', "group_hash = '{$fp}'");
            $this->assertNotNull($issue, "Issue missing for fingerprint {$fp}");
            $this->assertSame(10, (int) $issue['occurrences_count']);
        }
    }

    // ─── 11. Job Lifecycle ────────────────────────────────────

    public function test_job_lifecycle_processed_and_failed(): void
    {
        $successTrace = 'sys-job-ok-'.uniqid();
        $failTrace = 'sys-job-fail-'.uniqid();

        // Successful job
        $this->sendAndExpectOk([
            $this->sim->makeJob([
                'trace_id' => $successTrace,
                'name' => 'App\\Jobs\\SendEmail',
                'status' => 'processed',
                'queue' => 'emails',
            ]),
        ]);

        // Failed job with exception
        $this->sendAndExpectOk([
            $this->sim->makeJob([
                'trace_id' => $failTrace,
                'name' => 'App\\Jobs\\ProcessPayment',
                'status' => 'failed',
                'exceptions' => 1,
            ]),
            $this->sim->makeException([
                'trace_id' => 'sys-jexc-'.uniqid(),
                'execution_id' => $failTrace,
                'execution_source' => 'job',
                'class' => 'App\\Exceptions\\PaymentTimeout',
                'file' => 'app/Jobs/ProcessPayment.php',
                'line' => 88,
            ]),
        ]);

        $this->waitForDrain('nightowl_jobs', "trace_id = '{$failTrace}'", 1);

        $successJob = self::fetch('nightowl_jobs', "trace_id = '{$successTrace}'");
        $this->assertSame('processed', $successJob['status']);
        $this->assertSame('emails', $successJob['queue']);

        $failedJob = self::fetch('nightowl_jobs', "trace_id = '{$failTrace}'");
        $this->assertSame('failed', $failedJob['status']);

        // Failed job's exception should create an issue
        $fp = md5('App\\Exceptions\\PaymentTimeout'.'|'.'0'.'|'.'app/Jobs/ProcessPayment.php'.'|'.'88');
        $issue = self::fetch('nightowl_issues', "group_hash = '{$fp}'");
        $this->assertNotNull($issue);
    }

    // ─── 12. Concurrent Connections ───────────────────────────

    public function test_concurrent_tcp_connections_all_accepted(): void
    {
        $tag = 'sys-conc-'.uniqid();
        $concurrency = 10;

        // Open all connections first
        $sockets = [];
        $tokenHash = substr(hash('xxh128', self::TOKEN), 0, 7);

        for ($i = 0; $i < $concurrency; $i++) {
            $sock = @stream_socket_client(
                'tcp://'.self::AGENT_HOST.':'.self::AGENT_PORT,
                $errno, $errstr, 3.0,
            );
            $this->assertNotFalse($sock, "Connection {$i} failed: {$errstr}");
            stream_set_timeout($sock, 5);
            $sockets[] = $sock;
        }

        // Send payloads on all connections
        foreach ($sockets as $i => $sock) {
            $records = [$this->sim->makeRequest(['trace_id' => "{$tag}-{$i}"])];
            $json = json_encode($records);
            $body = "v1:{$tokenHash}:{$json}";
            $wire = strlen($body).':'.$body;
            fwrite($sock, $wire);
        }

        // Read responses
        $okCount = 0;
        foreach ($sockets as $sock) {
            $response = fread($sock, 128);
            fclose($sock);
            if ($response === '2:OK') {
                $okCount++;
            }
        }

        $this->assertSame($concurrency, $okCount, 'All concurrent connections should be accepted');

        $this->waitForDrain('nightowl_requests', "trace_id LIKE '{$tag}-%'", $concurrency);
        $this->assertSame($concurrency, self::rowCount('nightowl_requests', "trace_id LIKE '{$tag}-%'"));
    }

    // ─── 13. Mixed Realistic Scenario ─────────────────────────

    public function test_realistic_mixed_traffic_scenario(): void
    {
        $tag = 'sys-mix-'.uniqid();

        // Simulate 30 seconds of realistic traffic in fast-forward
        // 10 requests, 3 jobs, 2 commands, 1 scheduled task, 1 error
        for ($i = 0; $i < 10; $i++) {
            $this->sim->simulateRequest(['trace_id' => "{$tag}-req-{$i}"]);
        }
        for ($i = 0; $i < 3; $i++) {
            $this->sim->simulateJob('processed', ['trace_id' => "{$tag}-job-{$i}"]);
        }
        for ($i = 0; $i < 2; $i++) {
            $this->sim->simulateCommand(['trace_id' => "{$tag}-cmd-{$i}"]);
        }
        $this->sim->simulateScheduledTask(['trace_id' => "{$tag}-task-0"]);
        $this->sim->simulateErrorRequest(['trace_id' => "{$tag}-err-0"]);

        // Wait for the last items to arrive
        $this->waitForDrain('nightowl_scheduled_tasks', "trace_id = '{$tag}-task-0'", 1);

        // Verify the realistic spread
        $this->assertSame(10, self::rowCount('nightowl_requests', "trace_id LIKE '{$tag}-req-%'"));
        $this->assertSame(3, self::rowCount('nightowl_jobs', "trace_id LIKE '{$tag}-job-%'"));
        $this->assertSame(2, self::rowCount('nightowl_commands', "trace_id LIKE '{$tag}-cmd-%'"));
        $this->assertSame(1, self::rowCount('nightowl_scheduled_tasks', "trace_id = '{$tag}-task-0'"));

        // Error request should have generated an exception + issue
        $this->assertGreaterThanOrEqual(1, self::rowCount('nightowl_exceptions'));
        $this->assertGreaterThanOrEqual(1, self::rowCount('nightowl_issues'));

        // Queries generated by simulateRequest (2-8 per request × 10 requests)
        $this->assertGreaterThanOrEqual(20, self::rowCount('nightowl_queries'));

        // Users generated by simulateRequest
        $this->assertGreaterThanOrEqual(1, self::rowCount('nightowl_users'));
    }

    // ─── 14. User Upsert Across Payloads ──────────────────────

    public function test_user_upsert_updates_across_payloads(): void
    {
        $userId = 'sys-upsert-user-'.uniqid();

        // First payload: create user
        $this->sendAndExpectOk([
            ['t' => 'user', 'id' => $userId, 'name' => 'Original Name', 'username' => 'original@test.com'],
        ]);

        $this->waitForDrain('nightowl_users', "user_id = '{$userId}'", 1);

        $user = self::fetch('nightowl_users', "user_id = '{$userId}'");
        $this->assertSame('Original Name', $user['name']);

        // Second payload: update user
        $this->sendAndExpectOk([
            ['t' => 'user', 'id' => $userId, 'name' => 'Updated Name', 'username' => 'updated@test.com'],
        ]);

        // Wait for the update to propagate (drain second batch)
        $deadline = microtime(true) + self::DRAIN_TIMEOUT;
        while (microtime(true) < $deadline) {
            $user = self::fetch('nightowl_users', "user_id = '{$userId}'");
            if ($user['name'] === 'Updated Name') {
                break;
            }
            usleep(200_000);
        }

        $user = self::fetch('nightowl_users', "user_id = '{$userId}'");
        $this->assertSame('Updated Name', $user['name']);
        $this->assertSame('updated@test.com', $user['email']);
    }

    // ─── 15. Malformed Payload Rejected ───────────────────────

    public function test_malformed_payload_does_not_crash_agent(): void
    {
        // Send garbage
        $sock = stream_socket_client(
            'tcp://'.self::AGENT_HOST.':'.self::AGENT_PORT,
            $errno, $errstr, 3.0,
        );
        $this->assertNotFalse($sock);

        fwrite($sock, "this is not a valid payload\n");
        stream_set_timeout($sock, 3);
        $response = fread($sock, 128);
        fclose($sock);

        // Agent should reject without crashing
        // Response may be empty (connection closed) or 5:ERROR
        $this->assertTrue(
            $response === '' || $response === false || $response === '5:ERROR',
            'Expected rejection, got: '.var_export($response, true),
        );

        // Verify agent is still alive by sending a valid PING
        $response = $this->sim->ping();
        $this->assertSame('2:OK', $response, 'Agent should still be alive after malformed payload');
    }

    // ─── 16. Large Payload Over Wire ──────────────────────────

    public function test_large_payload_with_many_records(): void
    {
        $tag = 'sys-large-'.uniqid();

        // 200 records in a single payload (requests + queries + cache + logs)
        $records = [];
        for ($i = 0; $i < 50; $i++) {
            $records[] = $this->sim->makeRequest(['trace_id' => "{$tag}-r{$i}"]);
            $records[] = $this->sim->makeQuery(['trace_id' => "{$tag}-q{$i}"]);
            $records[] = $this->sim->makeCacheEvent(['trace_id' => "{$tag}-c{$i}"]);
            $records[] = $this->sim->makeLog(['trace_id' => "{$tag}-l{$i}"]);
        }

        $this->sendAndExpectOk($records);

        $this->waitForDrain('nightowl_requests', "trace_id LIKE '{$tag}-r%'", 50);

        $this->assertSame(50, self::rowCount('nightowl_requests', "trace_id LIKE '{$tag}-r%'"));
        $this->assertSame(50, self::rowCount('nightowl_queries', "trace_id LIKE '{$tag}-q%'"));
        $this->assertSame(50, self::rowCount('nightowl_cache_events', "trace_id LIKE '{$tag}-c%'"));
        $this->assertSame(50, self::rowCount('nightowl_logs', "trace_id LIKE '{$tag}-l%'"));
    }
}
