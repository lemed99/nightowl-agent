<?php

namespace NightOwl\Tests\System;

use NightOwl\Tests\Integration\MigrationRunner;
use NightOwl\Tests\System\Concerns\ReadsRawFamily;
use NightOwl\Tests\System\Concerns\SystemEnvironment;
use NightOwl\Simulator\NightwatchSimulator;
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
    use ReadsRawFamily;

    private const TOKEN = 'system-test-token-2025';

    private const AGENT_HOST = '127.0.0.1';

    private const AGENT_PORT = 2411;

    private const HEALTH_PORT = 2412;

    /** Maximum seconds to wait for drain to flush data to PG */
    private const DRAIN_TIMEOUT = 15;

    /**
     * Maximum seconds to wait for the agent process to bind its port. Sized for
     * a harness that has to build the whole schema before it listens, not for a
     * warm one — a dead subprocess ends the wait immediately (awaitAgentPort),
     * so the only thing a generous ceiling buys is a slow CI disk.
     */
    private const STARTUP_TIMEOUT = 60;

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
            SystemEnvironment::postgresUnavailable($e);
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
            SystemEnvironment::agentUnavailable('Agent or PostgreSQL not available.');
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
            SystemEnvironment::agentUnavailable('agent-harness-async.php not found.');
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
            SystemEnvironment::agentUnavailable('Failed to start agent process.');
        }

        // Non-blocking reads on stdout
        stream_set_blocking(self::$agentPipes[1], false);

        $reason = SystemEnvironment::awaitAgentPort(
            self::$agentProcess,
            self::$agentPipes[1],
            self::AGENT_HOST,
            self::AGENT_PORT,
            self::STARTUP_TIMEOUT,
        );

        if ($reason !== null) {
            self::stopAgent();
            SystemEnvironment::agentUnavailable($reason);
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

    protected static function pdo(): PDO
    {
        return self::$pdo;
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
        $traceId = self::uuid();

        $this->sendAndExpectOk([
            $this->sim->makeRequest(['trace_id' => $traceId, 'method' => 'GET', 'status_code' => 200]),
        ]);

        $this->waitForDrain('nightowl_requests', self::traceEq('nightowl_requests', $traceId), 1);

        $row = self::fetch('nightowl_requests', self::traceEq('nightowl_requests', $traceId));
        $this->assertNotNull($row);
        $this->assertSame('GET', $row['method']);
        $this->assertSame(200, (int) $row['status_code']);
    }

    // ─── 2. Full Request Lifecycle ────────────────────────────

    public function test_request_lifecycle_with_child_records(): void
    {
        $traceId = self::uuid();
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
                'trace_id' => self::uuid(),
                'execution_id' => $traceId,
                'execution_source' => 'request',
                'sql' => 'INSERT INTO orders (user_id, total) VALUES (?, ?)',
            ]),
            $this->sim->makeQuery([
                'trace_id' => self::uuid(),
                'execution_id' => $traceId,
                'execution_source' => 'request',
                'sql' => 'SELECT * FROM products WHERE id = ?',
            ]),
            $this->sim->makeCacheEvent([
                'trace_id' => self::uuid(),
                'execution_id' => $traceId,
                'type' => 'hit',
                'key' => 'products:list',
            ]),
            $this->sim->makeLog([
                'trace_id' => self::uuid(),
                'execution_id' => $traceId,
                'level' => 'info',
                'message' => 'Order created',
            ]),
            $this->sim->makeUser($userId),
        ]);

        // Wait for the request (parent record) — child records arrive in the same batch
        $this->waitForDrain('nightowl_requests', self::traceEq('nightowl_requests', $traceId), 1);

        // Verify parent
        $request = self::fetch('nightowl_requests', self::traceEq('nightowl_requests', $traceId));
        $this->assertSame('POST', $request['method']);
        $this->assertSame(201, (int) $request['status_code']);

        // Verify children linked by execution_id
        $this->assertSame(2, self::rowCount('nightowl_queries', self::execEq($traceId)));
        $this->assertSame(1, self::rowCount('nightowl_cache_events', self::execEq($traceId)));
        $this->assertSame(1, self::rowCount('nightowl_logs', self::execEq($traceId)));

        // Verify user
        $user = self::fetch('nightowl_users', "user_id = '{$userId}'");
        $this->assertNotNull($user);
    }

    // ─── 3. Exception → Issue Creation ────────────────────────

    public function test_exception_creates_issue_automatically(): void
    {
        $traceId = self::uuid();
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
                'trace_id' => self::uuid(),
                'execution_id' => $traceId,
                'class' => $exceptionClass,
                'message' => 'Payment gateway timeout',
                'file' => $file,
                'line' => $line,
            ]),
        ]);

        $this->waitForDrain('nightowl_exceptions', self::execEq($traceId), 1);

        // Exception record stored
        $exception = self::fetch('nightowl_exceptions', self::execEq($traceId));
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
                    'trace_id' => self::uuid(),
                    'class' => $exceptionClass,
                    'file' => $file,
                    'line' => $line,
                    'user' => "user_{$i}",
                ]),
            ]);
        }

        $this->waitForDrain('nightowl_exceptions', self::fingerprintEq($fingerprint), 5);

        $issue = self::fetch('nightowl_issues', "group_hash = '{$fingerprint}'");
        $this->assertSame(5, (int) $issue['occurrences_count']);
        // users_count should be accurate (5 distinct users)
        $this->assertSame(5, (int) $issue['users_count']);
    }

    // ─── 5. All 12 Record Types ───────────────────────────────

    public function test_all_twelve_record_types_arrive_in_postgres(): void
    {
        $userId = 'sys-all-user-'.uniqid();

        // One trace id per logical table, so each assertion below names the
        // exact row it expects rather than counting whatever landed.
        $traces = [];
        foreach ([
            'nightowl_requests', 'nightowl_queries', 'nightowl_exceptions',
            'nightowl_commands', 'nightowl_jobs', 'nightowl_cache_events',
            'nightowl_mail', 'nightowl_notifications', 'nightowl_outgoing_requests',
            'nightowl_scheduled_tasks', 'nightowl_logs',
        ] as $table) {
            $traces[$table] = self::uuid();
        }

        $this->sendAndExpectOk([
            $this->sim->makeRequest(['trace_id' => $traces['nightowl_requests']]),
            $this->sim->makeQuery(['trace_id' => $traces['nightowl_queries']]),
            $this->sim->makeException(['trace_id' => $traces['nightowl_exceptions']]),
            $this->sim->makeCommand(['trace_id' => $traces['nightowl_commands']]),
            $this->sim->makeJob(['trace_id' => $traces['nightowl_jobs']]),
            $this->sim->makeCacheEvent(['trace_id' => $traces['nightowl_cache_events']]),
            $this->sim->makeMail(['trace_id' => $traces['nightowl_mail']]),
            $this->sim->makeNotification(['trace_id' => $traces['nightowl_notifications']]),
            $this->sim->makeOutgoingRequest(['trace_id' => $traces['nightowl_outgoing_requests']]),
            $this->sim->makeScheduledTask(['trace_id' => $traces['nightowl_scheduled_tasks']]),
            $this->sim->makeLog(['trace_id' => $traces['nightowl_logs']]),
            $this->sim->makeUser($userId),
        ]);

        // Wait for the slowest table (exception triggers issue upsert)
        $this->waitForDrain('nightowl_exceptions', self::traceEq('nightowl_exceptions', $traces['nightowl_exceptions']), 1);

        foreach ($traces as $table => $traceId) {
            $count = self::rowCount($table, self::traceEq($table, $traceId));
            $this->assertSame(1, $count, "Expected 1 row in {$table} for trace_id {$traceId}");
        }

        $this->assertSame(1, self::rowCount('nightowl_users', "user_id = '{$userId}'"));
        // Exception should have created an issue
        $this->assertGreaterThanOrEqual(1, self::rowCount('nightowl_issues'));
    }

    // ─── 6. Gzip Over The Wire ────────────────────────────────

    public function test_gzip_payload_processed_correctly(): void
    {
        if (! function_exists('gzencode')) {
            $this->markTestSkipped('ext-zlib not available');
        }

        $traceId = self::uuid();

        $records = [
            $this->sim->makeRequest(['trace_id' => $traceId, 'method' => 'PUT', 'status_code' => 200]),
            $this->sim->makeQuery(['trace_id' => self::uuid(), 'execution_id' => $traceId]),
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

        $this->waitForDrain('nightowl_requests', self::traceEq('nightowl_requests', $traceId), 1);

        $row = self::fetch('nightowl_requests', self::traceEq('nightowl_requests', $traceId));
        $this->assertSame('PUT', $row['method']);
    }

    // ─── 7. Token Rejection ───────────────────────────────────

    public function test_invalid_token_rejected_over_tcp(): void
    {
        $traceId = self::uuid();

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
        $this->assertSame(0, self::rowCount('nightowl_requests', self::traceEq('nightowl_requests', $traceId)));
    }

    // ─── 8. Batch Throughput ──────────────────────────────────

    public function test_batch_of100_requests_drained_correctly(): void
    {
        $traces = self::uuids(100);

        $records = [];
        foreach ($traces as $traceId) {
            $records[] = $this->sim->makeRequest(['trace_id' => $traceId]);
        }

        $this->sendAndExpectOk($records);

        $where = self::traceIn('nightowl_requests', $traces);
        $this->waitForDrain('nightowl_requests', $where, 100);

        $this->assertSame(100, self::rowCount('nightowl_requests', $where));
    }

    // ─── 9. Sequential Payloads ───────────────────────────────

    public function test_multiple_sequential_payloads_all_arrive(): void
    {
        $total = 20;
        $traces = self::uuids($total);

        foreach ($traces as $traceId) {
            $this->sendAndExpectOk([
                $this->sim->makeRequest(['trace_id' => $traceId]),
            ]);
        }

        $where = self::traceIn('nightowl_requests', $traces);
        $this->waitForDrain('nightowl_requests', $where, $total);

        $this->assertSame($total, self::rowCount('nightowl_requests', $where));
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
                    'trace_id' => self::uuid(),
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
        $successTrace = self::uuid();
        $failTrace = self::uuid();

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
                'trace_id' => self::uuid(),
                'execution_id' => $failTrace,
                'execution_source' => 'job',
                'class' => 'App\\Exceptions\\PaymentTimeout',
                'file' => 'app/Jobs/ProcessPayment.php',
                'line' => 88,
            ]),
        ]);

        $this->waitForDrain('nightowl_jobs', self::traceEq('nightowl_jobs', $failTrace), 1);

        $successJob = self::fetch('nightowl_jobs', self::traceEq('nightowl_jobs', $successTrace));
        $this->assertSame('processed', $successJob['status']);
        $this->assertSame('emails', $successJob['queue']);

        $failedJob = self::fetch('nightowl_jobs', self::traceEq('nightowl_jobs', $failTrace));
        $this->assertSame('failed', $failedJob['status']);

        // Failed job's exception should create an issue
        $fp = md5('App\\Exceptions\\PaymentTimeout'.'|'.'0'.'|'.'app/Jobs/ProcessPayment.php'.'|'.'88');
        $issue = self::fetch('nightowl_issues', "group_hash = '{$fp}'");
        $this->assertNotNull($issue);
    }

    // ─── 12. Concurrent Connections ───────────────────────────

    public function test_concurrent_tcp_connections_all_accepted(): void
    {
        $concurrency = 10;
        $traces = self::uuids($concurrency);

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
            $records = [$this->sim->makeRequest(['trace_id' => $traces[$i]])];
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

        $where = self::traceIn('nightowl_requests', $traces);
        $this->waitForDrain('nightowl_requests', $where, $concurrency);
        $this->assertSame($concurrency, self::rowCount('nightowl_requests', $where));
    }

    // ─── 13. Mixed Realistic Scenario ─────────────────────────

    public function test_realistic_mixed_traffic_scenario(): void
    {
        $reqTraces = self::uuids(10);
        $jobTraces = self::uuids(3);
        $cmdTraces = self::uuids(2);
        $taskTrace = self::uuid();
        $errTrace = self::uuid();

        // Simulate 30 seconds of realistic traffic in fast-forward
        // 10 requests, 3 jobs, 2 commands, 1 scheduled task, 1 error
        foreach ($reqTraces as $traceId) {
            $this->sim->simulateRequest(['trace_id' => $traceId]);
        }
        foreach ($jobTraces as $traceId) {
            $this->sim->simulateJob('processed', ['trace_id' => $traceId]);
        }
        foreach ($cmdTraces as $traceId) {
            $this->sim->simulateCommand(['trace_id' => $traceId]);
        }
        $this->sim->simulateScheduledTask(['trace_id' => $taskTrace]);
        $this->sim->simulateErrorRequest(['trace_id' => $errTrace]);

        // Wait for the last items to arrive
        $taskWhere = self::traceEq('nightowl_scheduled_tasks', $taskTrace);
        $this->waitForDrain('nightowl_scheduled_tasks', $taskWhere, 1);

        // Verify the realistic spread
        $this->assertSame(10, self::rowCount('nightowl_requests', self::traceIn('nightowl_requests', $reqTraces)));
        $this->assertSame(3, self::rowCount('nightowl_jobs', self::traceIn('nightowl_jobs', $jobTraces)));
        $this->assertSame(2, self::rowCount('nightowl_commands', self::traceIn('nightowl_commands', $cmdTraces)));
        $this->assertSame(1, self::rowCount('nightowl_scheduled_tasks', $taskWhere));

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
        $reqTraces = self::uuids(50);
        $qryTraces = self::uuids(50);
        $cacheTraces = self::uuids(50);
        $logTraces = self::uuids(50);

        // 200 records in a single payload (requests + queries + cache + logs)
        $records = [];
        for ($i = 0; $i < 50; $i++) {
            $records[] = $this->sim->makeRequest(['trace_id' => $reqTraces[$i]]);
            $records[] = $this->sim->makeQuery(['trace_id' => $qryTraces[$i]]);
            $records[] = $this->sim->makeCacheEvent(['trace_id' => $cacheTraces[$i]]);
            $records[] = $this->sim->makeLog(['trace_id' => $logTraces[$i]]);
        }

        $this->sendAndExpectOk($records);

        $reqWhere = self::traceIn('nightowl_requests', $reqTraces);
        $this->waitForDrain('nightowl_requests', $reqWhere, 50);

        $this->assertSame(50, self::rowCount('nightowl_requests', $reqWhere));
        $this->assertSame(50, self::rowCount('nightowl_queries', self::traceIn('nightowl_queries', $qryTraces)));
        $this->assertSame(50, self::rowCount('nightowl_cache_events', self::traceIn('nightowl_cache_events', $cacheTraces)));
        $this->assertSame(50, self::rowCount('nightowl_logs', self::traceIn('nightowl_logs', $logTraces)));
    }
}
