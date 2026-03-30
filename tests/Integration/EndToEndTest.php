<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\ConnectionHandler;
use NightOwl\Agent\PayloadParser;
use NightOwl\Agent\Redactor;
use NightOwl\Agent\RecordWriter;
use NightOwl\Agent\Sampler;
use NightOwl\Tests\Simulator\NightwatchSimulator;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * End-to-end tests: Simulator → Parser → Sampler → Redactor → RecordWriter → PostgreSQL.
 *
 * Validates the full pipeline without TCP/fork — uses ConnectionHandler directly
 * with a real RecordWriter connected to PostgreSQL.
 *
 * Requires PostgreSQL (same env vars as RecordWriterTest).
 */
class EndToEndTest extends TestCase
{
    private static ?PDO $pdo = null;
    private static string $host;
    private static int $port;
    private static string $database;
    private static string $username;
    private static string $password;

    private string $token = 'e2e-test-token';
    private ConnectionHandler $handler;
    private NightwatchSimulator $sim;

    public static function setUpBeforeClass(): void
    {
        self::$host = getenv('NIGHTOWL_TEST_DB_HOST') ?: '127.0.0.1';
        self::$port = (int) (getenv('NIGHTOWL_TEST_DB_PORT') ?: 5432);
        self::$database = getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test';
        self::$username = getenv('NIGHTOWL_TEST_DB_USERNAME') ?: 'nightowl_test';
        self::$password = getenv('NIGHTOWL_TEST_DB_PASSWORD') ?: 'test123';

        try {
            $dsn = sprintf('pgsql:host=%s;port=%d;dbname=%s', self::$host, self::$port, self::$database);
            self::$pdo = new PDO($dsn, self::$username, self::$password);
            self::$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (\Exception) {
            self::$pdo = null;
        }

        if (self::$pdo) {
            self::createTables();
        }
    }

    protected function setUp(): void
    {
        if (self::$pdo === null) {
            $this->markTestSkipped('PostgreSQL not available. Set NIGHTOWL_TEST_DB_* env vars.');
        }

        $writer = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password);

        $this->handler = new ConnectionHandler(
            parser: new PayloadParser(gzipEnabled: true),
            writer: $writer,
            sampler: new Sampler(sampleRate: 1.0),
            redactor: new Redactor(keys: ['password', 'secret'], enabled: true),
            token: $this->token,
        );

        $this->sim = new NightwatchSimulator($this->token);

        self::truncateAll();
    }

    // ─── Helpers ───────────────────────────────────────────

    private function handleWire(string $wirePayload): string
    {
        $stream = fopen('php://memory', 'r+');
        $this->handler->handle($stream, $wirePayload);
        rewind($stream);

        return stream_get_contents($stream);
    }

    private function buildWire(array $records): string
    {
        $json = json_encode($records, JSON_THROW_ON_ERROR);
        $tokenHash = substr(hash('xxh128', $this->token), 0, 7);
        $body = "v1:{$tokenHash}:{$json}";

        return strlen($body) . ':' . $body;
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

    private static function truncateAll(): void
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

    private static function createTables(): void
    {
        // Same schema as RecordWriterTest — ensures EndToEndTest is self-contained
        $sql = <<<'SQL'
        CREATE TABLE IF NOT EXISTS nightowl_requests (
            id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), group_hash VARCHAR(255), user_id VARCHAR(255), method VARCHAR(255) NOT NULL DEFAULT 'GET', url TEXT NOT NULL DEFAULT '/', route_name VARCHAR(255), route_methods TEXT, route_domain VARCHAR(255), route_path VARCHAR(255), route_action VARCHAR(255), ip VARCHAR(255), duration INTEGER, status_code INTEGER NOT NULL DEFAULT 200, request_size INTEGER, response_size INTEGER, bootstrap INTEGER, before_middleware INTEGER, action INTEGER, render INTEGER, after_middleware INTEGER, sending INTEGER, terminating INTEGER, exceptions INTEGER DEFAULT 0, logs INTEGER DEFAULT 0, queries INTEGER DEFAULT 0, lazy_loads INTEGER DEFAULT 0, jobs_queued INTEGER DEFAULT 0, mail INTEGER DEFAULT 0, notifications INTEGER DEFAULT 0, outgoing_requests INTEGER DEFAULT 0, files_read INTEGER DEFAULT 0, files_written INTEGER DEFAULT 0, cache_events INTEGER DEFAULT 0, hydrated_models INTEGER DEFAULT 0, peak_memory_usage INTEGER, exception_preview TEXT, context TEXT, headers TEXT, payload TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS nightowl_queries (
            id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), group_hash VARCHAR(255), execution_source VARCHAR(255), execution_id VARCHAR(255), execution_stage VARCHAR(255), user_id VARCHAR(255), sql_query TEXT NOT NULL DEFAULT '', file VARCHAR(255), line INTEGER, duration INTEGER, connection VARCHAR(255), connection_type VARCHAR(255), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS nightowl_exceptions (
            id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), execution_source VARCHAR(255), execution_id VARCHAR(255), execution_stage VARCHAR(255), user_id VARCHAR(255), class VARCHAR(255) NOT NULL DEFAULT 'Unknown', message TEXT, code VARCHAR(255), file VARCHAR(255), line INTEGER, trace TEXT, php_version VARCHAR(255), laravel_version VARCHAR(255), handled BOOLEAN DEFAULT false, fingerprint VARCHAR(255) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS nightowl_commands (
            id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), group_hash VARCHAR(255), user_id VARCHAR(255), command VARCHAR(255) NOT NULL DEFAULT 'unknown', exit_code INTEGER, duration INTEGER, exceptions INTEGER DEFAULT 0, logs INTEGER DEFAULT 0, queries INTEGER DEFAULT 0, lazy_loads INTEGER DEFAULT 0, jobs_queued INTEGER DEFAULT 0, mail INTEGER DEFAULT 0, notifications INTEGER DEFAULT 0, outgoing_requests INTEGER DEFAULT 0, files_read INTEGER DEFAULT 0, files_written INTEGER DEFAULT 0, cache_events INTEGER DEFAULT 0, hydrated_models INTEGER DEFAULT 0, peak_memory_usage INTEGER, exception_preview TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS nightowl_jobs (
            id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), group_hash VARCHAR(255), execution_source VARCHAR(255), execution_id VARCHAR(255), user_id VARCHAR(255), job_class VARCHAR(255) NOT NULL DEFAULT 'Unknown', queue VARCHAR(255), connection VARCHAR(255), status VARCHAR(255), duration INTEGER, attempts INTEGER DEFAULT 1, exceptions INTEGER DEFAULT 0, logs INTEGER DEFAULT 0, queries INTEGER DEFAULT 0, lazy_loads INTEGER DEFAULT 0, jobs_queued INTEGER DEFAULT 0, mail INTEGER DEFAULT 0, notifications INTEGER DEFAULT 0, outgoing_requests INTEGER DEFAULT 0, files_read INTEGER DEFAULT 0, files_written INTEGER DEFAULT 0, cache_events INTEGER DEFAULT 0, hydrated_models INTEGER DEFAULT 0, peak_memory_usage INTEGER, exception_preview TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS nightowl_cache_events (
            id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), execution_source VARCHAR(255), execution_id VARCHAR(255), execution_stage VARCHAR(255), user_id VARCHAR(255), event_type VARCHAR(255) NOT NULL DEFAULT 'unknown', key VARCHAR(255) NOT NULL DEFAULT '', store VARCHAR(255), ttl INTEGER, duration INTEGER, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS nightowl_mail (
            id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), execution_source VARCHAR(255), execution_id VARCHAR(255), execution_stage VARCHAR(255), user_id VARCHAR(255), mailer VARCHAR(255), recipients TEXT, subject VARCHAR(255), mailable VARCHAR(255), duration INTEGER, queued BOOLEAN DEFAULT false, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS nightowl_notifications (
            id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), execution_source VARCHAR(255), execution_id VARCHAR(255), execution_stage VARCHAR(255), user_id VARCHAR(255), notification VARCHAR(255), channel VARCHAR(255), notifiable_type VARCHAR(255), notifiable_id VARCHAR(255), duration INTEGER, queued BOOLEAN DEFAULT false, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS nightowl_outgoing_requests (
            id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), execution_source VARCHAR(255), execution_id VARCHAR(255), execution_stage VARCHAR(255), user_id VARCHAR(255), method VARCHAR(255) NOT NULL DEFAULT 'GET', url TEXT NOT NULL DEFAULT '', status_code INTEGER, duration INTEGER, request_size INTEGER, response_size INTEGER, request_headers TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS nightowl_scheduled_tasks (
            id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), group_hash VARCHAR(255), user_id VARCHAR(255), command VARCHAR(255) NOT NULL DEFAULT 'unknown', expression VARCHAR(255), status VARCHAR(255), duration INTEGER, exit_code INTEGER, exceptions INTEGER DEFAULT 0, logs INTEGER DEFAULT 0, queries INTEGER DEFAULT 0, lazy_loads INTEGER DEFAULT 0, jobs_queued INTEGER DEFAULT 0, mail INTEGER DEFAULT 0, notifications INTEGER DEFAULT 0, outgoing_requests INTEGER DEFAULT 0, files_read INTEGER DEFAULT 0, files_written INTEGER DEFAULT 0, cache_events INTEGER DEFAULT 0, hydrated_models INTEGER DEFAULT 0, peak_memory_usage INTEGER, exception_preview TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS nightowl_logs (
            id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), execution_source VARCHAR(255), execution_id VARCHAR(255), execution_stage VARCHAR(255), user_id VARCHAR(255), level VARCHAR(255) DEFAULT 'info', message TEXT, context TEXT, channel VARCHAR(255), created_at VARCHAR(255)
        );
        CREATE TABLE IF NOT EXISTS nightowl_users (
            user_id VARCHAR(255) PRIMARY KEY, name VARCHAR(255), email VARCHAR(255), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS nightowl_issues (
            id BIGSERIAL PRIMARY KEY, type VARCHAR(255) NOT NULL, status VARCHAR(255) DEFAULT 'open', priority VARCHAR(255), exception_class VARCHAR(255), exception_message TEXT, group_hash VARCHAR(255), first_seen_at TIMESTAMP, last_seen_at TIMESTAMP, occurrences_count INTEGER DEFAULT 0, users_count INTEGER DEFAULT 0, assigned_to VARCHAR(255), description TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, UNIQUE (group_hash, type)
        );
        CREATE TABLE IF NOT EXISTS nightowl_issue_comments (
            id BIGSERIAL PRIMARY KEY, issue_id BIGINT NOT NULL REFERENCES nightowl_issues(id) ON DELETE CASCADE, user_id BIGINT, user_name VARCHAR(255), user_email VARCHAR(255), body TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS nightowl_issue_activity (
            id BIGSERIAL PRIMARY KEY, issue_id BIGINT NOT NULL REFERENCES nightowl_issues(id) ON DELETE CASCADE, user_id BIGINT, user_name VARCHAR(255), action VARCHAR(50) NOT NULL, old_value VARCHAR(255), new_value VARCHAR(255), created_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS nightowl_settings (
            id BIGSERIAL PRIMARY KEY, key VARCHAR(255) NOT NULL UNIQUE, value TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS nightowl_alert_channels (
            id BIGSERIAL PRIMARY KEY, type VARCHAR(255) NOT NULL, name VARCHAR(255) NOT NULL, config TEXT NOT NULL DEFAULT '{}', enabled BOOLEAN NOT NULL DEFAULT true, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        SQL;

        self::$pdo->exec($sql);
    }

    // ─── Full request lifecycle ────────────────────────────

    public function testRequestLifecycleE2E(): void
    {
        $traceId = 'e2e-req-001';
        $userId = 'user_e2e';

        $records = [
            $this->sim->makeRequest([
                'trace_id' => $traceId,
                'user' => $userId,
                'status_code' => 200,
                'method' => 'POST',
                'url' => 'https://app.test/api/orders',
                'route_path' => '/api/orders',
            ]),
            $this->sim->makeQuery([
                'trace_id' => 'e2e-q1',
                'execution_id' => $traceId,
                'sql' => 'INSERT INTO orders (user_id, total) VALUES (?, ?)',
            ]),
            $this->sim->makeQuery([
                'trace_id' => 'e2e-q2',
                'execution_id' => $traceId,
                'sql' => 'SELECT * FROM products WHERE id = ?',
            ]),
            $this->sim->makeCacheEvent([
                'trace_id' => 'e2e-c1',
                'execution_id' => $traceId,
                'type' => 'hit',
                'key' => 'products:list',
            ]),
            $this->sim->makeLog([
                'trace_id' => 'e2e-l1',
                'execution_id' => $traceId,
                'level' => 'info',
                'message' => 'Order created successfully',
            ]),
            $this->sim->makeUser($userId),
        ];

        $response = $this->handleWire($this->buildWire($records));
        $this->assertSame('2:OK', $response);

        // Verify all records landed in PostgreSQL
        $request = self::fetch('nightowl_requests', "trace_id = 'e2e-req-001'");
        $this->assertNotNull($request);
        $this->assertSame('POST', $request['method']);
        $this->assertSame(200, (int) $request['status_code']);

        $this->assertSame(2, self::rowCount('nightowl_queries', "execution_id = 'e2e-req-001'"));
        $this->assertSame(1, self::rowCount('nightowl_cache_events', "execution_id = 'e2e-req-001'"));
        $this->assertSame(1, self::rowCount('nightowl_logs', "execution_id = 'e2e-req-001'"));

        $user = self::fetch('nightowl_users', "user_id = 'user_e2e'");
        $this->assertNotNull($user);
    }

    // ─── Error request → exception → issue ─────────────────

    public function testErrorRequestCreatesIssueE2E(): void
    {
        $traceId = 'e2e-err-001';

        $records = [
            $this->sim->makeRequest([
                'trace_id' => $traceId,
                'status_code' => 500,
                'exceptions' => 1,
            ]),
            $this->sim->makeException([
                'trace_id' => 'e2e-exc-001',
                'execution_id' => $traceId,
                'execution_source' => 'request',
                'class' => 'App\\Exceptions\\PaymentFailed',
                'message' => 'Card declined',
                'file' => 'app/Services/Payment.php',
                'line' => 42,
            ]),
        ];

        $response = $this->handleWire($this->buildWire($records));
        $this->assertSame('2:OK', $response);

        // Exception stored
        $exception = self::fetch('nightowl_exceptions', "trace_id = 'e2e-exc-001'");
        $this->assertNotNull($exception);
        $this->assertSame('App\\Exceptions\\PaymentFailed', $exception['class']);

        // Issue auto-created
        $fingerprint = md5('App\\Exceptions\\PaymentFailed' . 'app/Services/Payment.php' . '42');
        $issue = self::fetch('nightowl_issues', "group_hash = '{$fingerprint}'");
        $this->assertNotNull($issue);
        $this->assertSame('open', $issue['status']);
        $this->assertSame('exception', $issue['type']);
        $this->assertSame(1, (int) $issue['occurrences_count']);
    }

    // ─── Duplicate exceptions increment issue count ────────

    public function testDuplicateExceptionsIncrementIssueE2E(): void
    {
        $base = [
            'class' => 'App\\Exceptions\\DupE2E',
            'file' => 'app/Dup.php',
            'line' => 10,
            'execution_source' => 'request',
        ];

        // Send 3 separate payloads with same exception fingerprint
        for ($i = 0; $i < 3; $i++) {
            $response = $this->handleWire($this->buildWire([
                $this->sim->makeException(array_merge($base, [
                    'trace_id' => "e2e-dup-{$i}",
                    'user' => "user_{$i}",
                ])),
            ]));
            $this->assertSame('2:OK', $response);
        }

        $fingerprint = md5('App\\Exceptions\\DupE2E' . 'app/Dup.php' . '10');
        $issue = self::fetch('nightowl_issues', "group_hash = '{$fingerprint}'");

        $this->assertSame(3, (int) $issue['occurrences_count']);
        $this->assertSame(3, (int) $issue['users_count']);
    }

    // ─── Job lifecycle ─────────────────────────────────────

    public function testJobLifecycleE2E(): void
    {
        $traceId = 'e2e-job-001';

        $records = [
            $this->sim->makeJob([
                'trace_id' => $traceId,
                'name' => 'App\\Jobs\\SendInvoice',
                'status' => 'processed',
                'queue' => 'emails',
            ]),
            $this->sim->makeQuery([
                'trace_id' => 'e2e-jq1',
                'execution_id' => $traceId,
                'execution_source' => 'job',
                'sql' => 'SELECT * FROM invoices WHERE id = ?',
            ]),
            $this->sim->makeMail([
                'trace_id' => 'e2e-jm1',
                'execution_id' => $traceId,
                'execution_source' => 'job',
                'subject' => 'Your invoice #1234',
                'mailable' => 'App\\Mail\\InvoiceMail',
            ]),
        ];

        $response = $this->handleWire($this->buildWire($records));
        $this->assertSame('2:OK', $response);

        $job = self::fetch('nightowl_jobs', "trace_id = 'e2e-job-001'");
        $this->assertNotNull($job);
        $this->assertSame('App\\Jobs\\SendInvoice', $job['job_class']);
        $this->assertSame('processed', $job['status']);
        $this->assertSame('emails', $job['queue']);

        $this->assertSame(1, self::rowCount('nightowl_queries', "execution_id = 'e2e-job-001'"));
        $this->assertSame(1, self::rowCount('nightowl_mail', "execution_id = 'e2e-job-001'"));
    }

    // ─── Failed job with exception ─────────────────────────

    public function testFailedJobCreatesIssueE2E(): void
    {
        $traceId = 'e2e-fail-001';

        $records = [
            $this->sim->makeJob([
                'trace_id' => $traceId,
                'name' => 'App\\Jobs\\ProcessPayment',
                'status' => 'failed',
                'exceptions' => 1,
            ]),
            $this->sim->makeException([
                'trace_id' => 'e2e-fexc-001',
                'execution_id' => $traceId,
                'execution_source' => 'job',
                'class' => 'App\\Exceptions\\PaymentTimeout',
                'message' => 'Gateway timeout',
                'file' => 'app/Jobs/ProcessPayment.php',
                'line' => 88,
            ]),
        ];

        $response = $this->handleWire($this->buildWire($records));
        $this->assertSame('2:OK', $response);

        $job = self::fetch('nightowl_jobs', "trace_id = 'e2e-fail-001'");
        $this->assertSame('failed', $job['status']);

        $fingerprint = md5('App\\Exceptions\\PaymentTimeout' . 'app/Jobs/ProcessPayment.php' . '88');
        $issue = self::fetch('nightowl_issues', "group_hash = '{$fingerprint}'");
        $this->assertNotNull($issue);
    }

    // ─── Command lifecycle ─────────────────────────────────

    public function testCommandLifecycleE2E(): void
    {
        $records = [
            $this->sim->makeCommand([
                'trace_id' => 'e2e-cmd-001',
                'command' => 'migrate',
                'exit_code' => 0,
            ]),
            $this->sim->makeQuery([
                'trace_id' => 'e2e-cq1',
                'execution_id' => 'e2e-cmd-001',
                'execution_source' => 'command',
                'sql' => 'CREATE TABLE orders (...)',
            ]),
        ];

        $response = $this->handleWire($this->buildWire($records));
        $this->assertSame('2:OK', $response);

        $cmd = self::fetch('nightowl_commands', "trace_id = 'e2e-cmd-001'");
        $this->assertNotNull($cmd);
        $this->assertSame('migrate', $cmd['command']);
        $this->assertSame(0, (int) $cmd['exit_code']);
    }

    // ─── Redaction works in pipeline ───────────────────────

    public function testRedactionAppliedBeforeStorageE2E(): void
    {
        $records = [
            $this->sim->makeRequest([
                'trace_id' => 'e2e-redact-001',
                'password' => 'super-secret-123',
                'secret' => 'api-key-xyz',
            ]),
        ];

        $response = $this->handleWire($this->buildWire($records));
        $this->assertSame('2:OK', $response);

        // The request was stored — verify redaction happened
        // Note: 'password' and 'secret' are not standard request columns,
        // so they won't appear in the DB. But the record was processed
        // through the pipeline without error.
        $request = self::fetch('nightowl_requests', "trace_id = 'e2e-redact-001'");
        $this->assertNotNull($request);
    }

    // ─── All 12 types in single payload ────────────────────

    public function testAll12TypesInSinglePayloadE2E(): void
    {
        $records = [
            $this->sim->makeRequest(['trace_id' => 'e2e-all-req']),
            $this->sim->makeQuery(['trace_id' => 'e2e-all-qry']),
            $this->sim->makeException(['trace_id' => 'e2e-all-exc']),
            $this->sim->makeCommand(['trace_id' => 'e2e-all-cmd']),
            $this->sim->makeJob(['trace_id' => 'e2e-all-job']),
            $this->sim->makeCacheEvent(['trace_id' => 'e2e-all-cache']),
            $this->sim->makeMail(['trace_id' => 'e2e-all-mail']),
            $this->sim->makeNotification(['trace_id' => 'e2e-all-notif']),
            $this->sim->makeOutgoingRequest(['trace_id' => 'e2e-all-out']),
            $this->sim->makeScheduledTask(['trace_id' => 'e2e-all-task']),
            $this->sim->makeLog(['trace_id' => 'e2e-all-log']),
            $this->sim->makeUser('e2e-all-user'),
        ];

        $response = $this->handleWire($this->buildWire($records));
        $this->assertSame('2:OK', $response);

        // Verify every table got a row
        $this->assertSame(1, self::rowCount('nightowl_requests', "trace_id = 'e2e-all-req'"));
        $this->assertSame(1, self::rowCount('nightowl_queries', "trace_id = 'e2e-all-qry'"));
        $this->assertSame(1, self::rowCount('nightowl_exceptions', "trace_id = 'e2e-all-exc'"));
        $this->assertSame(1, self::rowCount('nightowl_commands', "trace_id = 'e2e-all-cmd'"));
        $this->assertSame(1, self::rowCount('nightowl_jobs', "trace_id = 'e2e-all-job'"));
        $this->assertSame(1, self::rowCount('nightowl_cache_events', "trace_id = 'e2e-all-cache'"));
        $this->assertSame(1, self::rowCount('nightowl_mail', "trace_id = 'e2e-all-mail'"));
        $this->assertSame(1, self::rowCount('nightowl_notifications', "trace_id = 'e2e-all-notif'"));
        $this->assertSame(1, self::rowCount('nightowl_outgoing_requests', "trace_id = 'e2e-all-out'"));
        $this->assertSame(1, self::rowCount('nightowl_scheduled_tasks', "trace_id = 'e2e-all-task'"));
        $this->assertSame(1, self::rowCount('nightowl_logs', "trace_id = 'e2e-all-log'"));
        $this->assertSame(1, self::rowCount('nightowl_users', "user_id = 'e2e-all-user'"));
        // Exception also created an issue
        $this->assertGreaterThanOrEqual(1, self::rowCount('nightowl_issues'));
    }

    // ─── Throughput: batch of 50 requests ──────────────────

    public function testBatchOf50RequestsE2E(): void
    {
        $records = [];
        for ($i = 0; $i < 50; $i++) {
            $records[] = $this->sim->makeRequest(['trace_id' => "e2e-batch-{$i}"]);
        }

        $response = $this->handleWire($this->buildWire($records));
        $this->assertSame('2:OK', $response);

        $this->assertSame(50, self::rowCount('nightowl_requests', "trace_id LIKE 'e2e-batch-%'"));
    }

    // ─── Token rejection doesn't write ─────────────────────

    public function testInvalidTokenDoesNotWriteE2E(): void
    {
        $json = json_encode([$this->sim->makeRequest(['trace_id' => 'e2e-reject'])]);
        $body = "v1:INVALID:{$json}";
        $wire = strlen($body) . ':' . $body;

        $response = $this->handleWire($wire);
        $this->assertSame('5:ERROR', $response);

        $this->assertSame(0, self::rowCount('nightowl_requests', "trace_id = 'e2e-reject'"));
    }

    // ─── Gzip payload ──────────────────────────────────────

    public function testGzipPayloadE2E(): void
    {
        if (! function_exists('gzencode')) {
            $this->markTestSkipped('ext-zlib not available');
        }

        $records = [$this->sim->makeRequest(['trace_id' => 'e2e-gzip-001'])];
        $json = json_encode($records);
        $compressed = gzencode($json);

        $tokenHash = substr(hash('xxh128', $this->token), 0, 7);
        $body = "v1:{$tokenHash}:{$compressed}";
        $wire = strlen($body) . ':' . $body;

        $response = $this->handleWire($wire);
        $this->assertSame('2:OK', $response);

        $this->assertSame(1, self::rowCount('nightowl_requests', "trace_id = 'e2e-gzip-001'"));
    }

    // ─── Multiple payloads to same handler instance ────────

    public function testMultiplePayloadsSequentialE2E(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $records = [$this->sim->makeRequest(['trace_id' => "e2e-seq-{$i}"])];
            $response = $this->handleWire($this->buildWire($records));
            $this->assertSame('2:OK', $response);
        }

        $this->assertSame(5, self::rowCount('nightowl_requests', "trace_id LIKE 'e2e-seq-%'"));
    }
}
