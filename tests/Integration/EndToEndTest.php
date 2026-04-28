<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\ConnectionHandler;
use NightOwl\Agent\PayloadParser;
use NightOwl\Agent\RecordWriter;
use NightOwl\Tests\Simulator\NightwatchSimulator;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * End-to-end tests: Simulator → Parser → RecordWriter → PostgreSQL.
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
            MigrationRunner::migrate(self::$host, self::$port, self::$database, self::$username, self::$password);
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

        return strlen($body).':'.$body;
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

    // ─── Full request lifecycle ────────────────────────────

    public function test_request_lifecycle_e2_e(): void
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

    public function test_error_request_creates_issue_e2_e(): void
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
        $fingerprint = md5('App\\Exceptions\\PaymentFailed'.'|'.'0'.'|'.'app/Services/Payment.php'.'|'.'42');
        $issue = self::fetch('nightowl_issues', "group_hash = '{$fingerprint}'");
        $this->assertNotNull($issue);
        $this->assertSame('open', $issue['status']);
        $this->assertSame('exception', $issue['type']);
        $this->assertSame(1, (int) $issue['occurrences_count']);
    }

    // ─── Duplicate exceptions increment issue count ────────

    public function test_duplicate_exceptions_increment_issue_e2_e(): void
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

        $fingerprint = md5('App\\Exceptions\\DupE2E'.'|'.'0'.'|'.'app/Dup.php'.'|'.'10');
        $issue = self::fetch('nightowl_issues', "group_hash = '{$fingerprint}'");

        $this->assertSame(3, (int) $issue['occurrences_count']);
        $this->assertSame(3, (int) $issue['users_count']);
    }

    // ─── Job lifecycle ─────────────────────────────────────

    public function test_job_lifecycle_e2_e(): void
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

    public function test_failed_job_creates_issue_e2_e(): void
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

        $fingerprint = md5('App\\Exceptions\\PaymentTimeout'.'|'.'0'.'|'.'app/Jobs/ProcessPayment.php'.'|'.'88');
        $issue = self::fetch('nightowl_issues', "group_hash = '{$fingerprint}'");
        $this->assertNotNull($issue);
    }

    // ─── Command lifecycle ─────────────────────────────────

    public function test_command_lifecycle_e2_e(): void
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

    // ─── All 12 types in single payload ────────────────────

    public function test_all12_types_in_single_payload_e2_e(): void
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

    public function test_batch_of50_requests_e2_e(): void
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

    public function test_invalid_token_does_not_write_e2_e(): void
    {
        $json = json_encode([$this->sim->makeRequest(['trace_id' => 'e2e-reject'])]);
        $body = "v1:INVALID:{$json}";
        $wire = strlen($body).':'.$body;

        $response = $this->handleWire($wire);
        $this->assertSame('5:ERROR', $response);

        $this->assertSame(0, self::rowCount('nightowl_requests', "trace_id = 'e2e-reject'"));
    }

    // ─── Gzip payload ──────────────────────────────────────

    public function test_gzip_payload_e2_e(): void
    {
        if (! function_exists('gzencode')) {
            $this->markTestSkipped('ext-zlib not available');
        }

        $records = [$this->sim->makeRequest(['trace_id' => 'e2e-gzip-001'])];
        $json = json_encode($records);
        $compressed = gzencode($json);

        $tokenHash = substr(hash('xxh128', $this->token), 0, 7);
        $body = "v1:{$tokenHash}:{$compressed}";
        $wire = strlen($body).':'.$body;

        $response = $this->handleWire($wire);
        $this->assertSame('2:OK', $response);

        $this->assertSame(1, self::rowCount('nightowl_requests', "trace_id = 'e2e-gzip-001'"));
    }

    // ─── Multiple payloads to same handler instance ────────

    public function test_multiple_payloads_sequential_e2_e(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $records = [$this->sim->makeRequest(['trace_id' => "e2e-seq-{$i}"])];
            $response = $this->handleWire($this->buildWire($records));
            $this->assertSame('2:OK', $response);
        }

        $this->assertSame(5, self::rowCount('nightowl_requests', "trace_id LIKE 'e2e-seq-%'"));
    }
}
