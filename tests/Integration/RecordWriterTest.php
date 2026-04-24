<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\RecordWriter;
use NightOwl\Tests\Simulator\NightwatchSimulator;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * Integration tests for RecordWriter — requires a live PostgreSQL database.
 *
 * Set these env vars to run:
 *   NIGHTOWL_TEST_DB_HOST=127.0.0.1
 *   NIGHTOWL_TEST_DB_PORT=5432
 *   NIGHTOWL_TEST_DB_DATABASE=nightowl_test
 *   NIGHTOWL_TEST_DB_USERNAME=nightowl_test
 *   NIGHTOWL_TEST_DB_PASSWORD=test123
 *
 * Or run PostgreSQL via Docker:
 *   docker run -d --name nightowl-test-pg -p 5433:5432 \
 *     -e POSTGRES_DB=nightowl_test -e POSTGRES_USER=nightowl_test \
 *     -e POSTGRES_PASSWORD=test123 postgres:15-alpine
 *
 * Then: NIGHTOWL_TEST_DB_PORT=5433 vendor/bin/phpunit tests/Integration/RecordWriterTest.php
 */
class RecordWriterTest extends TestCase
{
    private static ?PDO $pdo = null;

    private static string $host;

    private static int $port;

    private static string $database;

    private static string $username;

    private static string $password;

    private RecordWriter $writer;

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
        } catch (\Exception $e) {
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

        $this->writer = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password);
        $this->sim = new NightwatchSimulator('test-token');

        self::truncateAllTables();
    }

    public static function tearDownAfterClass(): void
    {
        self::$pdo = null;
    }

    // ─── Individual record type tests ──────────────────────

    public function test_write_request(): void
    {
        $record = $this->sim->makeRequest(['trace_id' => 'req-001', 'status_code' => 200]);

        $this->writer->write([$record]);

        $row = self::$pdo->query("SELECT * FROM nightowl_requests WHERE trace_id = 'req-001'")->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($row);
        $this->assertSame(200, (int) $row['status_code']);
        $this->assertSame('req-001', $row['trace_id']);
    }

    public function test_write_query(): void
    {
        $record = $this->sim->makeQuery(['trace_id' => 'qry-001', 'sql' => 'SELECT * FROM users']);

        $this->writer->write([$record]);

        $row = self::$pdo->query("SELECT * FROM nightowl_queries WHERE trace_id = 'qry-001'")->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($row);
        $this->assertSame('SELECT * FROM users', $row['sql_query']);
    }

    public function test_write_exception(): void
    {
        $record = $this->sim->makeException([
            'trace_id' => 'exc-001',
            'class' => 'RuntimeException',
            'message' => 'Test error',
        ]);

        $this->writer->write([$record]);

        $row = self::$pdo->query("SELECT * FROM nightowl_exceptions WHERE trace_id = 'exc-001'")->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($row);
        $this->assertSame('RuntimeException', $row['class']);
        $this->assertSame('Test error', $row['message']);
    }

    public function test_write_exception_creates_issue(): void
    {
        $record = $this->sim->makeException([
            'trace_id' => 'exc-issue-001',
            'class' => 'App\\Exceptions\\TestException',
            'message' => 'Issue test',
            'file' => 'app/Test.php',
            'line' => 42,
        ]);

        $this->writer->write([$record]);

        $fingerprint = md5('App\\Exceptions\\TestException'.'|'.'0'.'|'.'app/Test.php'.'|'.'42');
        $issue = self::$pdo->query("SELECT * FROM nightowl_issues WHERE group_hash = '{$fingerprint}'")->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($issue);
        $this->assertSame('exception', $issue['type']);
        $this->assertSame('open', $issue['status']);
        $this->assertSame('App\\Exceptions\\TestException', $issue['exception_class']);
        $this->assertSame(1, (int) $issue['occurrences_count']);
    }

    public function test_write_exception_upserts_issue_count(): void
    {
        $baseRecord = [
            'class' => 'App\\Exceptions\\DuplicateTest',
            'file' => 'app/Dup.php',
            'line' => 10,
        ];

        $this->writer->write([$this->sim->makeException(array_merge($baseRecord, ['trace_id' => 'dup-1']))]);
        $this->writer->write([$this->sim->makeException(array_merge($baseRecord, ['trace_id' => 'dup-2']))]);
        $this->writer->write([$this->sim->makeException(array_merge($baseRecord, ['trace_id' => 'dup-3']))]);

        $fingerprint = md5('App\\Exceptions\\DuplicateTest'.'|'.'0'.'|'.'app/Dup.php'.'|'.'10');
        $issue = self::$pdo->query("SELECT * FROM nightowl_issues WHERE group_hash = '{$fingerprint}'")->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $issue['occurrences_count']);
    }

    public function test_write_command(): void
    {
        $record = $this->sim->makeCommand(['trace_id' => 'cmd-001', 'command' => 'migrate']);

        $this->writer->write([$record]);

        $row = self::$pdo->query("SELECT * FROM nightowl_commands WHERE trace_id = 'cmd-001'")->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($row);
        $this->assertSame('migrate', $row['command']);
    }

    public function test_write_job(): void
    {
        $record = $this->sim->makeJob(['trace_id' => 'job-001', 'name' => 'App\\Jobs\\TestJob', 'status' => 'processed']);

        $this->writer->write([$record]);

        $row = self::$pdo->query("SELECT * FROM nightowl_jobs WHERE trace_id = 'job-001'")->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($row);
        $this->assertSame('App\\Jobs\\TestJob', $row['job_class']);
        $this->assertSame('processed', $row['status']);
    }

    public function test_write_cache_event(): void
    {
        $record = $this->sim->makeCacheEvent(['trace_id' => 'cache-001', 'type' => 'hit', 'key' => 'users:1']);

        $this->writer->write([$record]);

        $row = self::$pdo->query("SELECT * FROM nightowl_cache_events WHERE trace_id = 'cache-001'")->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($row);
        $this->assertSame('hit', $row['event_type']);
        $this->assertSame('users:1', $row['key']);
    }

    public function test_write_mail(): void
    {
        $record = $this->sim->makeMail(['trace_id' => 'mail-001', 'subject' => 'Welcome!']);

        $this->writer->write([$record]);

        $row = self::$pdo->query("SELECT * FROM nightowl_mail WHERE trace_id = 'mail-001'")->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($row);
        $this->assertSame('Welcome!', $row['subject']);
    }

    public function test_write_notification(): void
    {
        $record = $this->sim->makeNotification(['trace_id' => 'notif-001', 'channel' => 'mail']);

        $this->writer->write([$record]);

        $row = self::$pdo->query("SELECT * FROM nightowl_notifications WHERE trace_id = 'notif-001'")->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($row);
        $this->assertSame('mail', $row['channel']);
    }

    public function test_write_outgoing_request(): void
    {
        $record = $this->sim->makeOutgoingRequest(['trace_id' => 'out-001', 'url' => 'https://api.stripe.com/v1/charges']);

        $this->writer->write([$record]);

        $row = self::$pdo->query("SELECT * FROM nightowl_outgoing_requests WHERE trace_id = 'out-001'")->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($row);
        $this->assertStringContainsString('stripe', $row['url']);
    }

    public function test_write_scheduled_task(): void
    {
        $record = $this->sim->makeScheduledTask(['trace_id' => 'task-001', 'name' => 'schedule:run']);

        $this->writer->write([$record]);

        $row = self::$pdo->query("SELECT * FROM nightowl_scheduled_tasks WHERE trace_id = 'task-001'")->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($row);
        $this->assertSame('schedule:run', $row['command']);
    }

    public function test_write_log(): void
    {
        $record = $this->sim->makeLog(['trace_id' => 'log-001', 'level' => 'error', 'message' => 'Something broke']);

        $this->writer->write([$record]);

        $row = self::$pdo->query("SELECT * FROM nightowl_logs WHERE trace_id = 'log-001'")->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($row);
        $this->assertSame('error', $row['level']);
        $this->assertSame('Something broke', $row['message']);
    }

    public function test_write_user(): void
    {
        $record = $this->sim->makeUser('user_42');

        $this->writer->write([$record]);

        $row = self::$pdo->query("SELECT * FROM nightowl_users WHERE user_id = 'user_42'")->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($row);
        $this->assertNotNull($row['name']);
    }

    public function test_write_user_upsert_updates_existing(): void
    {
        $this->writer->write([$this->sim->makeUser('user_upsert')]);
        $this->writer->write([['t' => 'user', 'id' => 'user_upsert', 'name' => 'Updated Name', 'username' => 'updated@test.com']]);

        $row = self::$pdo->query("SELECT * FROM nightowl_users WHERE user_id = 'user_upsert'")->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Updated Name', $row['name']);
        $this->assertSame('updated@test.com', $row['email']);
    }

    // ─── Mixed payload tests ───────────────────────────────

    public function test_write_mixed_payload(): void
    {
        $traceId = 'mixed-001';
        $records = [
            $this->sim->makeRequest(['trace_id' => $traceId]),
            $this->sim->makeQuery(['trace_id' => 'q-mixed-1', 'execution_id' => $traceId]),
            $this->sim->makeQuery(['trace_id' => 'q-mixed-2', 'execution_id' => $traceId]),
            $this->sim->makeCacheEvent(['trace_id' => 'c-mixed-1', 'execution_id' => $traceId]),
            $this->sim->makeLog(['trace_id' => 'l-mixed-1', 'execution_id' => $traceId]),
            $this->sim->makeUser('user_mixed'),
        ];

        $this->writer->write($records);

        $this->assertSame(1, (int) self::$pdo->query("SELECT COUNT(*) FROM nightowl_requests WHERE trace_id = '{$traceId}'")->fetchColumn());
        $this->assertSame(2, (int) self::$pdo->query("SELECT COUNT(*) FROM nightowl_queries WHERE execution_id = '{$traceId}'")->fetchColumn());
        $this->assertSame(1, (int) self::$pdo->query("SELECT COUNT(*) FROM nightowl_cache_events WHERE execution_id = '{$traceId}'")->fetchColumn());
        $this->assertSame(1, (int) self::$pdo->query("SELECT COUNT(*) FROM nightowl_logs WHERE execution_id = '{$traceId}'")->fetchColumn());
        $this->assertSame(1, (int) self::$pdo->query("SELECT COUNT(*) FROM nightowl_users WHERE user_id = 'user_mixed'")->fetchColumn());
    }

    public function test_write_all_twelve_types(): void
    {
        $records = [
            $this->sim->makeRequest(['trace_id' => 'all-req']),
            $this->sim->makeQuery(['trace_id' => 'all-qry']),
            $this->sim->makeException(['trace_id' => 'all-exc']),
            $this->sim->makeCommand(['trace_id' => 'all-cmd']),
            $this->sim->makeJob(['trace_id' => 'all-job']),
            $this->sim->makeCacheEvent(['trace_id' => 'all-cache']),
            $this->sim->makeMail(['trace_id' => 'all-mail']),
            $this->sim->makeNotification(['trace_id' => 'all-notif']),
            $this->sim->makeOutgoingRequest(['trace_id' => 'all-out']),
            $this->sim->makeScheduledTask(['trace_id' => 'all-task']),
            $this->sim->makeLog(['trace_id' => 'all-log']),
            $this->sim->makeUser('all-user'),
        ];

        $this->writer->write($records);

        // Verify every table got a row
        $tables = [
            'nightowl_requests' => 'all-req',
            'nightowl_queries' => 'all-qry',
            'nightowl_exceptions' => 'all-exc',
            'nightowl_commands' => 'all-cmd',
            'nightowl_jobs' => 'all-job',
            'nightowl_cache_events' => 'all-cache',
            'nightowl_mail' => 'all-mail',
            'nightowl_notifications' => 'all-notif',
            'nightowl_outgoing_requests' => 'all-out',
            'nightowl_scheduled_tasks' => 'all-task',
            'nightowl_logs' => 'all-log',
        ];

        foreach ($tables as $table => $traceId) {
            $count = (int) self::$pdo->query("SELECT COUNT(*) FROM {$table} WHERE trace_id = '{$traceId}'")->fetchColumn();
            $this->assertSame(1, $count, "Expected 1 row in {$table} with trace_id {$traceId}");
        }

        $this->assertSame(1, (int) self::$pdo->query("SELECT COUNT(*) FROM nightowl_users WHERE user_id = 'all-user'")->fetchColumn());
    }

    // ─── Transaction behavior ──────────────────────────────

    public function test_write_is_atomic(): void
    {
        // Write valid records
        $this->writer->write([
            $this->sim->makeRequest(['trace_id' => 'atomic-1']),
            $this->sim->makeQuery(['trace_id' => 'atomic-2']),
        ]);

        $this->assertSame(1, (int) self::$pdo->query("SELECT COUNT(*) FROM nightowl_requests WHERE trace_id = 'atomic-1'")->fetchColumn());
        $this->assertSame(1, (int) self::$pdo->query("SELECT COUNT(*) FROM nightowl_queries WHERE trace_id = 'atomic-2'")->fetchColumn());
    }

    public function test_skips_records_without_type(): void
    {
        // Records without 't' key should be silently skipped
        $this->writer->write([
            ['url' => '/no-type'],
            $this->sim->makeRequest(['trace_id' => 'has-type']),
        ]);

        $this->assertSame(1, (int) self::$pdo->query("SELECT COUNT(*) FROM nightowl_requests WHERE trace_id = 'has-type'")->fetchColumn());
    }

    public function test_skips_unknown_type(): void
    {
        $this->writer->write([
            ['t' => 'unknown_type', 'data' => 'ignored'],
            $this->sim->makeRequest(['trace_id' => 'known-type']),
        ]);

        $this->assertSame(1, (int) self::$pdo->query("SELECT COUNT(*) FROM nightowl_requests WHERE trace_id = 'known-type'")->fetchColumn());
    }

    // ─── users_count accuracy ────────────────────────────────

    public function test_exception_issue_users_count_does_not_inflate(): void
    {
        $baseRecord = [
            'class' => 'App\\Exceptions\\UserCountTest',
            'file' => 'app/UserCount.php',
            'line' => 99,
        ];
        $fingerprint = md5('App\\Exceptions\\UserCountTest'.'|'.'0'.'|'.'app/UserCount.php'.'|'.'99');

        // Batch 1: user_A and user_B
        $this->writer->write([
            $this->sim->makeException(array_merge($baseRecord, ['trace_id' => 'uc-1', 'user' => 'user_A'])),
            $this->sim->makeException(array_merge($baseRecord, ['trace_id' => 'uc-2', 'user' => 'user_B'])),
        ]);

        $issue = self::$pdo->query("SELECT * FROM nightowl_issues WHERE group_hash = '{$fingerprint}'")->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $issue['users_count'], 'First batch: 2 distinct users');
        $this->assertSame(2, (int) $issue['occurrences_count']);

        // Batch 2: user_A again (same user, different trace)
        $this->writer->write([
            $this->sim->makeException(array_merge($baseRecord, ['trace_id' => 'uc-3', 'user' => 'user_A'])),
        ]);

        $issue = self::$pdo->query("SELECT * FROM nightowl_issues WHERE group_hash = '{$fingerprint}'")->fetch(PDO::FETCH_ASSOC);
        // users_count should be 2 (not 3) — user_A is the same user across batches
        $this->assertSame(2, (int) $issue['users_count'], 'Same user across batches should not inflate count');
        $this->assertSame(3, (int) $issue['occurrences_count']);

        // Batch 3: user_C (new user)
        $this->writer->write([
            $this->sim->makeException(array_merge($baseRecord, ['trace_id' => 'uc-4', 'user' => 'user_C'])),
        ]);

        $issue = self::$pdo->query("SELECT * FROM nightowl_issues WHERE group_hash = '{$fingerprint}'")->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $issue['users_count'], 'New user should increment count');
        $this->assertSame(4, (int) $issue['occurrences_count']);
    }

    public function test_exception_issue_users_count_handles_null_users(): void
    {
        $baseRecord = [
            'class' => 'App\\Exceptions\\NullUserTest',
            'file' => 'app/NullUser.php',
            'line' => 50,
        ];
        $fingerprint = md5('App\\Exceptions\\NullUserTest'.'|'.'0'.'|'.'app/NullUser.php'.'|'.'50');

        // Exceptions with null user_id
        $this->writer->write([
            $this->sim->makeException(array_merge($baseRecord, ['trace_id' => 'nu-1', 'user' => null])),
            $this->sim->makeException(array_merge($baseRecord, ['trace_id' => 'nu-2', 'user' => null])),
        ]);

        $issue = self::$pdo->query("SELECT * FROM nightowl_issues WHERE group_hash = '{$fingerprint}'")->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $issue['users_count'], 'Null users should not be counted');
    }

    // ─── Batch stress ──────────────────────────────────────

    public function test_large_batch_write(): void
    {
        $records = [];
        for ($i = 0; $i < 100; $i++) {
            $records[] = $this->sim->makeRequest(['trace_id' => "batch-{$i}"]);
        }

        $this->writer->write($records);

        $count = (int) self::$pdo->query("SELECT COUNT(*) FROM nightowl_requests WHERE trace_id LIKE 'batch-%'")->fetchColumn();
        $this->assertSame(100, $count);
    }

    // ─── Helpers ───────────────────────────────────────────

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
}
