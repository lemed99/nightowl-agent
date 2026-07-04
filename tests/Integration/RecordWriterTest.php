<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\RecordWriter;
use NightOwl\Simulator\NightwatchSimulator;
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

    // ─── Swoole COPY-fallback (insertBatch) ────────────────

    /**
     * When Swoole is loaded its pgsqlCopyFromArray hook busy-loops, so copyBatch
     * routes to insertBatch instead. This verifies that fallback writes rows
     * correctly via multi-row INSERT — including values containing tabs/newlines,
     * which would corrupt COPY's TSV but are preserved verbatim by INSERT.
     */
    public function test_insert_batch_fallback_writes_rows(): void
    {
        $pdoMethod = new \ReflectionMethod($this->writer, 'pdo');
        /** @var PDO $wpdo */
        $wpdo = $pdoMethod->invoke($this->writer);

        $wpdo->exec('CREATE TABLE IF NOT EXISTS t_insert_batch_test (a int, b text, c text)');
        $wpdo->exec('TRUNCATE t_insert_batch_test');

        try {
            $insert = new \ReflectionMethod($this->writer, 'insertBatch');
            $insert->invoke($this->writer, 't_insert_batch_test', ['a', 'b', 'c'], [
                [1, 'hello', null],
                [2, "has\ttab\nand newline", 'z'],
            ]);

            $rows = $wpdo->query('SELECT a, b, c FROM t_insert_batch_test ORDER BY a')->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(2, $rows);
            $this->assertSame('hello', $rows[0]['b']);
            $this->assertNull($rows[0]['c']);
            $this->assertSame("has\ttab\nand newline", $rows[1]['b']);
            $this->assertSame('z', $rows[1]['c']);
        } finally {
            $wpdo->exec('DROP TABLE IF EXISTS t_insert_batch_test');
        }
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

    public function test_write_tallies_app_vitals_including_5xx(): void
    {
        $records = [
            $this->sim->makeRequest(['trace_id' => 'av-1', 'status_code' => 200]),
            $this->sim->makeRequest(['trace_id' => 'av-2', 'status_code' => 503]),
            $this->sim->makeRequest(['trace_id' => 'av-3', 'status_code' => 500]),
            $this->sim->makeQuery(['trace_id' => 'av-q', 'sql' => 'SELECT 1']),
            $this->sim->makeException(['trace_id' => 'av-e', 'class' => 'RuntimeException', 'message' => 'boom']),
        ];

        $this->writer->write($records);

        // 3 requests, 2 of which are 5xx, 1 exception — queries don't count.
        $this->assertSame(3, $this->writer->lastRequestCount);
        $this->assertSame(2, $this->writer->last5xxCount);
        $this->assertSame(1, $this->writer->lastExceptionCount);
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

    public function test_count_open_issues_reflects_status(): void
    {
        // No issues yet.
        $this->assertSame(0, $this->writer->countOpenIssues());

        // Two distinct exceptions → two open issues.
        $this->writer->write([$this->sim->makeException([
            'trace_id' => 'oi-1', 'class' => 'App\\Oi\\AException', 'file' => 'app/A.php', 'line' => 1,
        ])]);
        $this->writer->write([$this->sim->makeException([
            'trace_id' => 'oi-2', 'class' => 'App\\Oi\\BException', 'file' => 'app/B.php', 'line' => 2,
        ])]);
        $this->assertSame(2, $this->writer->countOpenIssues());

        // Resolving one drops the open count.
        self::$pdo->exec("UPDATE nightowl_issues SET status = 'resolved' WHERE exception_class = 'App\\Oi\\AException'");
        $this->assertSame(1, $this->writer->countOpenIssues());
    }

    public function test_count_open_issues_returns_null_when_table_missing(): void
    {
        // Older tenant schemas have no nightowl_issues table — the gauge must
        // degrade to null, never throw and break the drain loop. Rename it away
        // for the duration of the assertion, then restore it.
        self::$pdo->exec('ALTER TABLE nightowl_issues RENAME TO nightowl_issues_tmp');
        try {
            $this->assertNull($this->writer->countOpenIssues());
        } finally {
            self::$pdo->exec('ALTER TABLE nightowl_issues_tmp RENAME TO nightowl_issues');
        }
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

    // ─── Query rollups ─────────────────────────────────────

    public function test_write_query_populates_rollup(): void
    {
        $records = [];
        for ($i = 1; $i <= 5; $i++) {
            $records[] = $this->sim->makeQuery([
                'trace_id' => "rollup-{$i}",
                '_group' => 'rollupgrouphash',
                'sql' => 'SELECT * FROM widgets',
                'duration' => $i * 1000, // 1000..5000
                'connection' => 'pgsql',
            ]);
        }

        $this->writer->write($records);

        $rollup = self::$pdo->query(
            "SELECT * FROM nightowl_query_rollups WHERE group_hash = 'rollupgrouphash'"
        )->fetchAll(PDO::FETCH_ASSOC);

        // Same hash + connection + minute bucket collapse to a single row.
        $this->assertCount(1, $rollup);
        $row = $rollup[0];
        $this->assertSame(5, (int) $row['call_count']);
        $this->assertSame(15000, (int) $row['total_duration']);
        $this->assertSame(1000, (int) $row['min_duration']);
        $this->assertSame(5000, (int) $row['max_duration']);
        $this->assertSame('pgsql', $row['connection']);
        $this->assertSame('SELECT * FROM widgets', $row['sql_query']);
    }

    public function test_rollup_accumulates_additively_across_batches(): void
    {
        $make = fn (string $trace, int $duration) => $this->sim->makeQuery([
            'trace_id' => $trace,
            '_group' => 'accumhash',
            'sql' => 'SELECT 1',
            'duration' => $duration,
            'connection' => 'pgsql',
        ]);

        $this->writer->write([$make('a1', 2000), $make('a2', 4000)]);
        $this->writer->write([$make('a3', 1000)]);

        $row = self::$pdo->query(
            "SELECT * FROM nightowl_query_rollups WHERE group_hash = 'accumhash'"
        )->fetch(PDO::FETCH_ASSOC);

        // Both batches land in the same minute bucket and accumulate via the
        // additive ON CONFLICT upsert.
        $this->assertSame(3, (int) $row['call_count']);
        $this->assertSame(7000, (int) $row['total_duration']);
        $this->assertSame(1000, (int) $row['min_duration']);
        $this->assertSame(4000, (int) $row['max_duration']);
    }

    // ─── Per-user rollups (powers the users list) ──────────

    /** @return array<string, array<string, string>> rollup rows indexed by user_id */
    private function fetchUserRollup(string $table): array
    {
        $rows = self::$pdo->query("SELECT * FROM {$table}")->fetchAll(PDO::FETCH_ASSOC);
        $byUser = [];
        foreach ($rows as $r) {
            $u = $r['user_id'];
            // Sum across minute buckets so the assertions are immune to a test
            // straddling a minute boundary.
            foreach ($r as $col => $val) {
                if ($col === 'user_id' || $col === 'bucket_start' || $col === 'environment') {
                    continue;
                }
                $byUser[$u][$col] = (int) ($byUser[$u][$col] ?? 0) + (int) $val;
            }
        }

        return $byUser;
    }

    public function test_drain_populates_per_user_request_rollup(): void
    {
        $this->writer->write([
            $this->sim->makeRequest(['trace_id' => 'ur-1', 'user' => 'user-a', 'status_code' => 200]),
            $this->sim->makeRequest(['trace_id' => 'ur-2', 'user' => 'user-a', 'status_code' => 200]),
            $this->sim->makeRequest(['trace_id' => 'ur-3', 'user' => 'user-a', 'status_code' => 404]),
            $this->sim->makeRequest(['trace_id' => 'ur-4', 'user' => 'user-a', 'status_code' => 500]),
            $this->sim->makeRequest(['trace_id' => 'ur-5', 'user' => 'user-b', 'status_code' => 200]),
            $this->sim->makeRequest(['trace_id' => 'ur-6', 'user' => null, 'status_code' => 200]), // anonymous → '' sentinel
        ]);

        $byUser = $this->fetchUserRollup('nightowl_user_rollups');

        $this->assertSame(4, $byUser['user-a']['call_count']);
        $this->assertSame(2, $byUser['user-a']['success_count']);
        $this->assertSame(1, $byUser['user-a']['client_error_count']);
        $this->assertSame(1, $byUser['user-a']['server_error_count']);
        $this->assertSame(1, $byUser['user-b']['call_count']);
        $this->assertSame(1, $byUser['user-b']['success_count']);
        // Anonymous requests collapse into the '' group, kept out of the users
        // list by the read side's `user_id != ''` filter, not dropped here.
        $this->assertSame(1, $byUser['']['call_count']);
    }

    public function test_drain_populates_per_user_job_and_exception_rollups(): void
    {
        $this->writer->write([
            $this->sim->makeJob(['trace_id' => 'uj-1', 'user' => 'user-a', 'attempt_id' => 'att-1']),
            $this->sim->makeJob(['trace_id' => 'uj-2', 'user' => 'user-a', 'attempt_id' => 'att-2']),
            $this->sim->makeJob(['trace_id' => 'uj-3', 'user' => 'user-a', 'attempt_id' => null]), // queued dispatch — not an attempt
            $this->sim->makeJob(['trace_id' => 'uj-4', 'user' => 'user-b', 'attempt_id' => 'att-4']),
        ]);
        $this->writer->write([
            $this->sim->makeException(['trace_id' => 'ue-1', 'user' => 'user-a']),
            $this->sim->makeException(['trace_id' => 'ue-2', 'user' => 'user-a']),
            $this->sim->makeException(['trace_id' => 'ue-3', 'user' => 'user-b']),
        ]);

        $jobs = $this->fetchUserRollup('nightowl_user_job_rollups');
        // queued_jobs on the users list = attempts_count (attempt_id IS NOT NULL).
        $this->assertSame(3, $jobs['user-a']['call_count']);
        $this->assertSame(2, $jobs['user-a']['attempts_count']);
        $this->assertSame(1, $jobs['user-b']['attempts_count']);

        $exceptions = $this->fetchUserRollup('nightowl_user_exception_rollups');
        // exceptions on the users list = the implicit call_count.
        $this->assertSame(2, $exceptions['user-a']['call_count']);
        $this->assertSame(1, $exceptions['user-b']['call_count']);
    }

    /**
     * Drift guard: the per-user request rollup, summed across its buckets, must
     * exactly reproduce a raw re-aggregation of nightowl_requests grouped by user.
     * This is the single highest-value defense against the drain and the users-
     * list read path diverging — any future change to the status bands trips it.
     */
    public function test_per_user_request_rollup_matches_raw_reaggregation(): void
    {
        $statuses = [200, 201, 404, 500];
        $records = [];
        for ($i = 1; $i <= 20; $i++) {
            $records[] = $this->sim->makeRequest([
                'trace_id' => "drift-u-{$i}",
                'user' => $i % 5 === 0 ? null : 'u'.($i % 4), // mix real users + anonymous
                'status_code' => $statuses[$i % 4],
            ]);
        }

        $this->writer->write($records);

        $rollup = self::$pdo->query(
            "SELECT user_id,
                    SUM(call_count) AS cc,
                    SUM(success_count) AS sc,
                    SUM(client_error_count) AS cec,
                    SUM(server_error_count) AS sec
             FROM nightowl_user_rollups
             GROUP BY user_id ORDER BY user_id"
        )->fetchAll(PDO::FETCH_ASSOC);

        $raw = self::$pdo->query(
            "SELECT COALESCE(user_id, '') AS user_id,
                    COUNT(*) AS cc,
                    SUM(CASE WHEN status_code < 400 THEN 1 ELSE 0 END) AS sc,
                    SUM(CASE WHEN status_code >= 400 AND status_code < 500 THEN 1 ELSE 0 END) AS cec,
                    SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS sec
             FROM nightowl_requests
             GROUP BY COALESCE(user_id, '') ORDER BY user_id"
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertEquals($raw, $rollup, 'per-user rollup must reproduce a raw re-aggregation of nightowl_requests');
    }

    /**
     * Drift guard: nightowl_exception_rollups (keyed by fingerprint) summed across
     * buckets must reproduce a raw re-aggregation of nightowl_exceptions, incl. the
     * handled/unhandled bands ExceptionController's list/overview/chart read.
     */
    public function test_exception_group_rollup_matches_raw_reaggregation(): void
    {
        $records = [];
        for ($i = 1; $i <= 20; $i++) {
            $records[] = $this->sim->makeException([
                'trace_id' => "drift-exc-{$i}",
                'class' => 'App\\Exceptions\\E'.($i % 3),
                'file' => '/app/E'.($i % 3).'.php',
                'line' => 10 + ($i % 3),
                'handled' => $i % 2 === 0, // mix handled / unhandled
            ]);
        }

        $this->writer->write($records);

        $rollup = self::$pdo->query(
            "SELECT fingerprint,
                    SUM(call_count) AS cc,
                    SUM(handled_count) AS hc,
                    SUM(unhandled_count) AS uc
             FROM nightowl_exception_rollups
             GROUP BY fingerprint ORDER BY fingerprint"
        )->fetchAll(PDO::FETCH_ASSOC);

        $raw = self::$pdo->query(
            "SELECT COALESCE(fingerprint, '') AS fingerprint,
                    COUNT(*) AS cc,
                    SUM(CASE WHEN handled = true THEN 1 ELSE 0 END) AS hc,
                    SUM(CASE WHEN handled != true OR handled IS NULL THEN 1 ELSE 0 END) AS uc
             FROM nightowl_exceptions
             GROUP BY COALESCE(fingerprint, '') ORDER BY fingerprint"
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertEquals($raw, $rollup, 'exception fingerprint rollup must reproduce a raw re-aggregation of nightowl_exceptions');
    }

    /**
     * Drift guard: COUNT(DISTINCT server) per fingerprint off nightowl_exception_
     * server_rollups must reproduce the same distinct-server count over raw
     * nightowl_exceptions — the exception detail "servers affected" stat.
     */
    public function test_exception_server_rollup_matches_raw_distinct_servers(): void
    {
        $servers = ['web-1', 'web-2', 'web-1', 'worker-1'];
        $records = [];
        for ($i = 0; $i < 12; $i++) {
            $records[] = $this->sim->makeException([
                'trace_id' => "svr-exc-{$i}",
                'class' => 'App\\Exceptions\\SvrE'.($i % 2),
                'file' => '/app/SvrE'.($i % 2).'.php',
                'line' => 5 + ($i % 2),
                'server' => $servers[$i % count($servers)],
            ]);
        }

        $this->writer->write($records);

        $rollup = self::$pdo->query(
            "SELECT fingerprint, COUNT(DISTINCT server) AS servers
             FROM nightowl_exception_server_rollups WHERE server <> ''
             GROUP BY fingerprint ORDER BY fingerprint"
        )->fetchAll(PDO::FETCH_ASSOC);

        $raw = self::$pdo->query(
            "SELECT COALESCE(fingerprint, '') AS fingerprint, COUNT(DISTINCT server) AS servers
             FROM nightowl_exceptions WHERE server IS NOT NULL AND server <> ''
             GROUP BY COALESCE(fingerprint, '') ORDER BY fingerprint"
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertEquals($raw, $rollup, 'server rollup COUNT(DISTINCT server) per fingerprint must match raw');
    }

    /**
     * Drift guard: SUM(authenticated_count) per fingerprint off nightowl_exception_
     * rollups must equal the raw count of occurrences carrying a user_id — the
     * detail page's authenticated-vs-guest split (guest = call_count - authenticated).
     */
    public function test_exception_rollup_authenticated_count_matches_raw(): void
    {
        $records = [];
        for ($i = 0; $i < 12; $i++) {
            $records[] = $this->sim->makeException([
                'trace_id' => "auth-exc-{$i}",
                'class' => 'App\\Exceptions\\AuthE'.($i % 2),
                'file' => '/app/AuthE'.($i % 2).'.php',
                'line' => 7 + ($i % 2),
                'user' => $i % 3 === 0 ? null : 'user-'.$i, // ~1/3 guest (null user_id)
            ]);
        }

        $this->writer->write($records);

        $rollup = self::$pdo->query(
            "SELECT fingerprint, SUM(call_count) AS cc, SUM(authenticated_count) AS ac
             FROM nightowl_exception_rollups GROUP BY fingerprint ORDER BY fingerprint"
        )->fetchAll(PDO::FETCH_ASSOC);

        $raw = self::$pdo->query(
            "SELECT COALESCE(fingerprint, '') AS fingerprint, COUNT(*) AS cc,
                    SUM(CASE WHEN user_id IS NOT NULL AND user_id <> '' THEN 1 ELSE 0 END) AS ac
             FROM nightowl_exceptions GROUP BY COALESCE(fingerprint, '') ORDER BY fingerprint"
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertEquals($raw, $rollup, 'authenticated_count per fingerprint must match the raw count of user-present occurrences');
    }

    /**
     * Drift guard for nightowl_mail_rollups. Beyond the counters, it asserts the
     * SUM of histogram bins equals COUNT(duration) — writeRollup bins exactly the
     * non-null durations, so that sum is the avg denominator the read path uses
     * (RollupReader::durationCountExpr) to keep mail's average correct when
     * queued/failed rows carry a NULL duration. Half the batch has NULL duration.
     */
    public function test_mail_rollup_matches_raw_reaggregation(): void
    {
        $records = [];
        for ($i = 1; $i <= 20; $i++) {
            $records[] = $this->sim->makeMail([
                'trace_id' => "drift-mail-{$i}",
                '_group' => 'mg'.($i % 3),
                'class' => 'App\\Mail\\M'.($i % 3),
                'queued' => $i % 4 === 0,
                'failed' => $i % 5 === 0,
                'duration' => $i % 2 === 0 ? null : $i * 1000, // half NULL-duration
            ]);
        }

        $this->writer->write($records);

        $histSum = 'SUM('.implode(') + SUM(', \NightOwl\Support\QueryHistogram::columns()).')';

        $rollup = self::$pdo->query(
            "SELECT group_hash,
                    SUM(call_count) AS cc,
                    SUM(queued_count) AS qc,
                    SUM(failed_count) AS fc,
                    SUM(total_duration) AS td,
                    {$histSum} AS dc
             FROM nightowl_mail_rollups
             GROUP BY group_hash ORDER BY group_hash"
        )->fetchAll(PDO::FETCH_ASSOC);

        $raw = self::$pdo->query(
            "SELECT COALESCE(group_hash, '') AS group_hash,
                    COUNT(*) AS cc,
                    SUM(CASE WHEN queued = true THEN 1 ELSE 0 END) AS qc,
                    SUM(CASE WHEN failed = true THEN 1 ELSE 0 END) AS fc,
                    COALESCE(SUM(duration), 0) AS td,
                    COUNT(duration) AS dc
             FROM nightowl_mail
             GROUP BY COALESCE(group_hash, '') ORDER BY group_hash"
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertEquals($raw, $rollup, 'mail rollup must reproduce a raw re-aggregation of nightowl_mail (hist-bin sum == COUNT(duration))');
    }

    /**
     * Drift guard for nightowl_notification_rollups at its (group_hash, channel)
     * grain, incl. the hist-bin sum == COUNT(duration) invariant behind the avg
     * denominator. Half the batch has NULL duration (queued/failed).
     */
    public function test_notification_rollup_matches_raw_reaggregation(): void
    {
        $records = [];
        $channels = ['mail', 'database', 'slack'];
        for ($i = 1; $i <= 24; $i++) {
            $records[] = $this->sim->makeNotification([
                'trace_id' => "drift-notif-{$i}",
                '_group' => 'ng'.($i % 3),
                'class' => 'App\\Notifications\\N'.($i % 3),
                'channel' => $channels[$i % 3],
                'queued' => $i % 4 === 0,
                'failed' => $i % 5 === 0,
                'duration' => $i % 2 === 0 ? null : $i * 500,
            ]);
        }

        $this->writer->write($records);

        $histSum = 'SUM('.implode(') + SUM(', \NightOwl\Support\QueryHistogram::columns()).')';

        $rollup = self::$pdo->query(
            "SELECT group_hash, channel,
                    SUM(call_count) AS cc,
                    SUM(queued_count) AS qc,
                    SUM(failed_count) AS fc,
                    SUM(total_duration) AS td,
                    {$histSum} AS dc
             FROM nightowl_notification_rollups
             GROUP BY group_hash, channel ORDER BY group_hash, channel"
        )->fetchAll(PDO::FETCH_ASSOC);

        $raw = self::$pdo->query(
            "SELECT COALESCE(group_hash, '') AS group_hash, COALESCE(channel, '') AS channel,
                    COUNT(*) AS cc,
                    SUM(CASE WHEN queued = true THEN 1 ELSE 0 END) AS qc,
                    SUM(CASE WHEN failed = true THEN 1 ELSE 0 END) AS fc,
                    COALESCE(SUM(duration), 0) AS td,
                    COUNT(duration) AS dc
             FROM nightowl_notifications
             GROUP BY COALESCE(group_hash, ''), COALESCE(channel, '') ORDER BY group_hash, channel"
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertEquals($raw, $rollup, 'notification (group_hash, channel) rollup must reproduce a raw re-aggregation of nightowl_notifications');
    }

    /**
     * Drift guard: the rollup, summed across its keys, must exactly reproduce a
     * raw re-aggregation of nightowl_queries. This is the single highest-value
     * defense against the rollup and raw paths diverging — any future change to
     * writeQueries that updates one without the other trips this.
     */
    public function test_rollup_sums_match_raw_reaggregation(): void
    {
        $records = [];
        for ($i = 1; $i <= 24; $i++) {
            $records[] = $this->sim->makeQuery([
                'trace_id' => "drift-{$i}",
                '_group' => 'g'.($i % 3),
                'sql' => 'SELECT '.($i % 3),
                'duration' => $i * 100,
                'connection' => $i % 2 === 0 ? 'pgsql' : 'mysql',
            ]);
        }

        $this->writer->write($records);

        $raw = self::$pdo->query(
            "SELECT COALESCE(group_hash, '') AS gh, COALESCE(connection, '') AS conn,
                    COUNT(*) AS c, SUM(duration) AS s, MIN(duration) AS mn, MAX(duration) AS mx
             FROM nightowl_queries
             GROUP BY 1, 2 ORDER BY 1, 2"
        )->fetchAll(PDO::FETCH_ASSOC);

        $rollup = self::$pdo->query(
            "SELECT group_hash AS gh, connection AS conn,
                    SUM(call_count) AS c, SUM(total_duration) AS s,
                    MIN(min_duration) AS mn, MAX(max_duration) AS mx
             FROM nightowl_query_rollups
             GROUP BY 1, 2 ORDER BY 1, 2"
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertEquals($raw, $rollup, 'Rollup aggregates must match a raw re-aggregation exactly');
    }

    public function test_rollup_populates_histogram_bins(): void
    {
        $this->writer->write([
            $this->sim->makeQuery(['trace_id' => 'h-1', '_group' => 'histgroup', 'sql' => 'SELECT 1', 'duration' => 100, 'connection' => 'pgsql']),     // bin 0 (< 128)
            $this->sim->makeQuery(['trace_id' => 'h-2', '_group' => 'histgroup', 'sql' => 'SELECT 1', 'duration' => 150, 'connection' => 'pgsql']),     // bin 1 ([128, 181))
            $this->sim->makeQuery(['trace_id' => 'h-3', '_group' => 'histgroup', 'sql' => 'SELECT 1', 'duration' => 2000000, 'connection' => 'pgsql']), // bin 28 ([1482910, 2097152))
        ]);

        $row = self::$pdo->query(
            "SELECT * FROM nightowl_query_rollups WHERE group_hash = 'histgroup'"
        )->fetch(PDO::FETCH_ASSOC);

        $this->assertSame(3, (int) $row['call_count']);
        $this->assertSame(1, (int) $row['hist_00']);
        $this->assertSame(1, (int) $row['hist_01']);
        $this->assertSame(1, (int) $row['hist_28']);

        $binTotal = 0;
        for ($i = 0; $i < 39; $i++) {
            $binTotal += (int) $row[sprintf('hist_%02d', $i)];
        }
        $this->assertSame(3, $binTotal, 'Histogram bins must sum to call_count');
    }

    /**
     * The drain assigns bins via QueryHistogram::binIndex (PHP); the backfill
     * assigns them via QueryHistogram::caseSql (SQL). This asserts the two agree
     * — drain-written bins equal a CASE re-aggregation of the raw rows.
     */
    public function test_histogram_matches_raw_case_aggregation(): void
    {
        $records = [];
        for ($i = 1; $i <= 30; $i++) {
            $records[] = $this->sim->makeQuery([
                'trace_id' => "hc-{$i}",
                '_group' => 'g'.($i % 2),
                'sql' => 'SELECT '.($i % 2),
                'duration' => $i * $i * 50, // 50µs … 45ms
                'connection' => 'pgsql',
            ]);
        }
        $this->writer->write($records);

        $case = \NightOwl\Support\QueryHistogram::caseSql('duration');
        $rawSelect = [];
        foreach ($case as $col => $expr) {
            $rawSelect[] = "{$expr} as {$col}";
        }
        $rollSelect = array_map(static fn (string $c): string => "SUM({$c}) as {$c}", array_keys($case));

        $raw = self::$pdo->query(
            "SELECT COALESCE(group_hash, '') AS gh, ".implode(', ', $rawSelect).
            ' FROM nightowl_queries GROUP BY 1 ORDER BY 1'
        )->fetchAll(PDO::FETCH_ASSOC);

        $rollup = self::$pdo->query(
            'SELECT group_hash AS gh, '.implode(', ', $rollSelect).
            ' FROM nightowl_query_rollups GROUP BY 1 ORDER BY 1'
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertEquals($raw, $rollup, 'Drain-written histogram must match a CASE re-aggregation of raw');
    }

    public function test_rollup_handles_null_duration(): void
    {
        // A query with no duration still counts toward call_count, but must not
        // touch min/max (stay null) or any histogram bin.
        $this->writer->write([
            $this->sim->makeQuery(['trace_id' => 'nd-1', '_group' => 'nulldur', 'sql' => 'SELECT 1', 'duration' => null, 'connection' => 'pgsql']),
        ]);

        $row = self::$pdo->query(
            "SELECT * FROM nightowl_query_rollups WHERE group_hash = 'nulldur'"
        )->fetch(PDO::FETCH_ASSOC);

        $this->assertSame(1, (int) $row['call_count']);
        $this->assertSame(0, (int) $row['total_duration']);
        $this->assertNull($row['min_duration']);
        $this->assertNull($row['max_duration']);

        $binTotal = 0;
        for ($i = 0; $i < 39; $i++) {
            $binTotal += (int) $row[sprintf('hist_%02d', $i)];
        }
        $this->assertSame(0, $binTotal, 'Null-duration rows must not increment any histogram bin');
    }

    /**
     * Critical availability path: when nightowl_query_rollups is missing (new
     * agent code before nightowl:migrate created it), the rollup write is skipped
     * and the raw query still drains — the upsert shares the COPY's transaction,
     * so a hard failure here would roll the raw write back and trap the drain in
     * a retry loop.
     */
    public function test_drain_succeeds_when_rollup_table_missing(): void
    {
        self::$pdo->exec('DROP TABLE IF EXISTS nightowl_query_rollups');

        try {
            // Fresh writer so the table-exists probe re-runs and observes the drop.
            $writer = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password);
            $writer->write([
                $this->sim->makeQuery(['trace_id' => 'no-rollup-tbl', 'sql' => 'SELECT 1', 'duration' => 1000]),
            ]);

            $raw = self::$pdo->query("SELECT COUNT(*) FROM nightowl_queries WHERE trace_id = 'no-rollup-tbl'")->fetchColumn();
            $this->assertSame(1, (int) $raw, 'Raw query must still drain when the rollup table is missing');
        } finally {
            // Recreate the table for the remaining tests in this class.
            $this->rollupMigration32()->up();
            $this->rollupMigration33()->up();
        }
    }

    public function test_rollup_merges_across_independent_writers(): void
    {
        // Simulates two NIGHTOWL_DRAIN_WORKERS: independent RecordWriter
        // instances (separate connections) hitting the same (hash, bucket). The
        // additive ON CONFLICT upsert must merge both — counts/sums add,
        // histogram bins add.
        $make = fn (string $trace, int $duration) => $this->sim->makeQuery([
            'trace_id' => $trace, '_group' => 'multiworker', 'sql' => 'SELECT 1',
            'duration' => $duration, 'connection' => 'pgsql',
        ]);

        $writerA = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password);
        $writerB = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password);

        $writerA->write([$make('mw-a1', 1000), $make('mw-a2', 200000)]); // bins for 1000 and 200000
        $writerB->write([$make('mw-b1', 1000)]);

        $row = self::$pdo->query(
            "SELECT * FROM nightowl_query_rollups WHERE group_hash = 'multiworker'"
        )->fetch(PDO::FETCH_ASSOC);

        $this->assertSame(3, (int) $row['call_count'], 'Independent writers must accumulate, not overwrite');
        $this->assertSame(202000, (int) $row['total_duration']);
        $this->assertSame(1000, (int) $row['min_duration']);
        $this->assertSame(200000, (int) $row['max_duration']);

        $binTotal = 0;
        for ($i = 0; $i < 39; $i++) {
            $binTotal += (int) $row[sprintf('hist_%02d', $i)];
        }
        $this->assertSame(3, $binTotal, 'Histogram bins must accumulate across independent writers');
    }

    /**
     * Migration round-trip: down() drops the histogram columns (000033) and then
     * the rollup table (000032); up() restores both. Tenant migrations aren't
     * rolled back in production, but down() should still be correct.
     */
    public function test_rollup_migrations_down_and_up_round_trip(): void
    {
        $tableExists = fn (): bool => (bool) self::$pdo->query(
            "SELECT to_regclass('public.nightowl_query_rollups') IS NOT NULL"
        )->fetchColumn();
        $histExists = fn (): bool => (bool) self::$pdo->query(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'nightowl_query_rollups' AND column_name = 'hist_38'"
        )->fetchColumn();

        $this->assertTrue($tableExists());
        $this->assertTrue($histExists());

        try {
            $this->rollupMigration33()->down();
            $this->assertFalse($histExists(), 'down() must drop the histogram columns');
            $this->assertTrue($tableExists(), 'dropping hist columns must leave the table');

            $this->rollupMigration32()->down();
            $this->assertFalse($tableExists(), 'down() must drop the rollup table');
        } finally {
            $this->rollupMigration32()->up();
            $this->rollupMigration33()->up();
        }

        $this->assertTrue($tableExists());
        $this->assertTrue($histExists());
    }

    /**
     * Load a fresh migration instance. `require` caches by path (so a second
     * require returns 1, not the object), and MigrationRunner may already have
     * required these files — so eval the file contents to get a new instance
     * regardless of include state.
     */
    private function rollupMigration32(): object
    {
        return $this->loadMigration(__DIR__.'/../../database/migrations/2024_01_01_000032_create_nightowl_query_rollups_table.php');
    }

    private function rollupMigration33(): object
    {
        return $this->loadMigration(__DIR__.'/../../database/migrations/2024_01_01_000033_add_histogram_to_query_rollups.php');
    }

    private function loadMigration(string $path): object
    {
        return eval('?>'.file_get_contents($path));
    }

    // ─── Request rollups (generic engine) ──────────────────

    public function test_request_write_populates_rollup(): void
    {
        $mk = fn (string $trace, int $status, int $dur): array => $this->sim->makeRequest([
            'trace_id' => $trace, '_group' => 'reqgroup', 'status_code' => $status, 'duration' => $dur,
            'route_methods' => ['GET'], 'route_path' => '/api/widgets',
        ]);

        $this->writer->write([
            $mk('rr-1', 200, 1000), $mk('rr-2', 200, 2000),
            $mk('rr-3', 404, 3000), $mk('rr-4', 500, 4000), $mk('rr-5', 503, 5000),
        ]);

        $row = self::$pdo->query(
            "SELECT * FROM nightowl_request_rollups WHERE group_hash = 'reqgroup'"
        )->fetch(PDO::FETCH_ASSOC);

        $this->assertSame(5, (int) $row['call_count']);
        $this->assertSame(2, (int) $row['success_count']);
        $this->assertSame(1, (int) $row['client_error_count']);
        $this->assertSame(2, (int) $row['server_error_count']);
        $this->assertSame(15000, (int) $row['total_duration']);
        $this->assertSame(1000, (int) $row['min_duration']);
        $this->assertSame(5000, (int) $row['max_duration']);
        $this->assertSame('/api/widgets', $row['route_path']);
        $this->assertSame('["GET"]', $row['route_methods']);

        $bins = 0;
        for ($i = 0; $i < 39; $i++) {
            $bins += (int) $row[sprintf('hist_%02d', $i)];
        }
        $this->assertSame(5, $bins);
    }

    public function test_request_rollup_drift_matches_raw(): void
    {
        $statuses = [200, 201, 404, 500, 503];
        $records = [];
        for ($i = 1; $i <= 20; $i++) {
            $records[] = $this->sim->makeRequest([
                'trace_id' => "rd-{$i}",
                '_group' => 'g'.($i % 3),
                'status_code' => $statuses[$i % 5],
                'duration' => $i * 100,
                'route_path' => '/p/'.($i % 3),
                'route_methods' => ['GET'],
            ]);
        }
        $this->writer->write($records);

        $raw = self::$pdo->query(
            "SELECT COALESCE(group_hash, '') AS gh, COUNT(*) AS c,
                    SUM(CASE WHEN status_code < 400 THEN 1 ELSE 0 END) AS s,
                    SUM(CASE WHEN status_code >= 400 AND status_code < 500 THEN 1 ELSE 0 END) AS ce,
                    SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS se,
                    SUM(duration) AS sd, MIN(duration) AS mn, MAX(duration) AS mx
             FROM nightowl_requests GROUP BY 1 ORDER BY 1"
        )->fetchAll(PDO::FETCH_ASSOC);

        $rollup = self::$pdo->query(
            'SELECT group_hash AS gh, SUM(call_count) AS c, SUM(success_count) AS s,
                    SUM(client_error_count) AS ce, SUM(server_error_count) AS se,
                    SUM(total_duration) AS sd, MIN(min_duration) AS mn, MAX(max_duration) AS mx
             FROM nightowl_request_rollups GROUP BY 1 ORDER BY 1'
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertEquals($raw, $rollup, 'Request rollup must match a raw re-aggregation');
    }

    // ─── Job rollups (generic engine; attempts vs queued) ──

    public function test_job_write_populates_rollup(): void
    {
        // 3 attempts (processed/released/failed) + 1 queued (no attempt_id, no duration).
        $this->writer->write([
            $this->sim->makeJob(['trace_id' => 'jr-1', '_group' => 'jobgroup', 'name' => 'App\\Jobs\\X', 'queue' => 'default', 'attempt_id' => 'a1', 'status' => 'processed', 'duration' => 1000]),
            $this->sim->makeJob(['trace_id' => 'jr-2', '_group' => 'jobgroup', 'name' => 'App\\Jobs\\X', 'queue' => 'default', 'attempt_id' => 'a2', 'status' => 'released', 'duration' => 3000]),
            $this->sim->makeJob(['trace_id' => 'jr-3', '_group' => 'jobgroup', 'name' => 'App\\Jobs\\X', 'queue' => 'default', 'attempt_id' => 'a3', 'status' => 'failed', 'duration' => 5000]),
            $this->sim->makeJob(['trace_id' => 'jr-4', '_group' => 'jobgroup', 'name' => 'App\\Jobs\\X', 'queue' => 'default', 'attempt_id' => null, 'status' => null, 'duration' => null]),
        ]);

        $row = self::$pdo->query("SELECT * FROM nightowl_job_rollups WHERE group_hash = 'jobgroup'")->fetch(PDO::FETCH_ASSOC);

        $this->assertSame(4, (int) $row['call_count']);
        $this->assertSame(3, (int) $row['attempts_count']);
        $this->assertSame(1, (int) $row['queued_count']);
        $this->assertSame(1, (int) $row['processed_count']);
        $this->assertSame(1, (int) $row['released_count']);
        $this->assertSame(1, (int) $row['failed_count']);
        $this->assertSame(9000, (int) $row['total_duration']);
        $this->assertSame(1000, (int) $row['min_duration']);
        $this->assertSame(5000, (int) $row['max_duration']);
        $this->assertSame('App\\Jobs\\X', $row['job_class']);
        $this->assertSame('default', $row['queue']);

        // Only the 3 attempts (non-null duration) enter the histogram.
        $bins = 0;
        for ($i = 0; $i < 39; $i++) {
            $bins += (int) $row[sprintf('hist_%02d', $i)];
        }
        $this->assertSame(3, $bins);
    }

    public function test_job_rollup_drift_matches_raw(): void
    {
        $records = [];
        for ($i = 1; $i <= 18; $i++) {
            $records[] = $this->sim->makeJob([
                'trace_id' => "jd-{$i}",
                '_group' => 'g'.($i % 3),
                'name' => 'App\\Jobs\\Y',
                'attempt_id' => $i % 4 === 0 ? null : "att-{$i}",
                'status' => ['processed', 'released', 'failed'][$i % 3],
                'duration' => $i % 4 === 0 ? null : $i * 100,
            ]);
        }
        $this->writer->write($records);

        $raw = self::$pdo->query(
            "SELECT COALESCE(group_hash, '') AS gh, COUNT(*) AS c,
                    SUM(CASE WHEN attempt_id IS NOT NULL THEN 1 ELSE 0 END) AS att,
                    SUM(CASE WHEN attempt_id IS NULL THEN 1 ELSE 0 END) AS q,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS f,
                    COALESCE(SUM(duration), 0) AS sd, MIN(duration) AS mn, MAX(duration) AS mx
             FROM nightowl_jobs GROUP BY 1 ORDER BY 1"
        )->fetchAll(PDO::FETCH_ASSOC);

        $rollup = self::$pdo->query(
            'SELECT group_hash AS gh, SUM(call_count) AS c, SUM(attempts_count) AS att,
                    SUM(queued_count) AS q, SUM(failed_count) AS f,
                    SUM(total_duration) AS sd, MIN(min_duration) AS mn, MAX(max_duration) AS mx
             FROM nightowl_job_rollups GROUP BY 1 ORDER BY 1'
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertEquals($raw, $rollup, 'Job rollup must match a raw re-aggregation');
    }

    // ─── Outgoing-request rollups ──────────────────────────

    public function test_outgoing_write_populates_rollup(): void
    {
        $mk = fn (string $trace, int $status, int $dur): array => $this->sim->makeOutgoingRequest([
            'trace_id' => $trace, '_group' => 'outgroup', 'status_code' => $status, 'duration' => $dur,
            'url' => 'https://api.stripe.com/v1/charges',
        ]);

        $this->writer->write([
            $mk('og-1', 200, 1000), $mk('og-2', 201, 2000),
            $mk('og-3', 404, 3000), $mk('og-4', 503, 4000),
        ]);

        $row = self::$pdo->query("SELECT * FROM nightowl_outgoing_request_rollups WHERE group_hash = 'outgroup'")->fetch(PDO::FETCH_ASSOC);

        $this->assertSame(4, (int) $row['call_count']);
        $this->assertSame(2, (int) $row['success_count']);
        $this->assertSame(1, (int) $row['client_error_count']);
        $this->assertSame(1, (int) $row['server_error_count']);
        $this->assertSame(10000, (int) $row['total_duration']);
        // host = extractHost(url) = scheme://host
        $this->assertSame('https://api.stripe.com', $row['host']);
    }

    public function test_outgoing_rollup_host_matches_extracthost(): void
    {
        $this->writer->write([
            $this->sim->makeOutgoingRequest(['trace_id' => 'oh-1', '_group' => 'hostgroup', 'url' => 'https://example.com/a/b/c', 'status_code' => 200, 'duration' => 500]),
        ]);

        // The rollup's stored host must equal the SQL extractHost(url) the read
        // path uses, so rollup and raw display the same host string.
        $rollupHost = self::$pdo->query("SELECT host FROM nightowl_outgoing_request_rollups WHERE group_hash = 'hostgroup'")->fetchColumn();
        $rawHost = self::$pdo->query(
            "SELECT SPLIT_PART(url, '/', 1) || '//' || SPLIT_PART(url, '/', 3) FROM nightowl_outgoing_requests WHERE trace_id = 'oh-1'"
        )->fetchColumn();

        $this->assertSame($rawHost, $rollupHost);
        $this->assertSame('https://example.com', $rollupHost);
    }

    // ─── Cache rollups (key/store group, no histogram) ─────

    public function test_cache_write_populates_rollup(): void
    {
        $mk = fn (string $trace, string $type, int $dur): array => $this->sim->makeCacheEvent([
            'trace_id' => $trace, 'type' => $type, 'key' => 'users:1', 'store' => 'redis', 'duration' => $dur,
        ]);

        $this->writer->write([
            $mk('cr-1', 'hit', 100), $mk('cr-2', 'hit', 200), $mk('cr-3', 'miss', 300),
            $mk('cr-4', 'set', 400), $mk('cr-5', 'forget', 500), $mk('cr-6', 'fail', 0),
            $mk('cr-7', 'write_fail', 0), $mk('cr-8', 'delete_fail', 0),
        ]);

        $row = self::$pdo->query(
            "SELECT * FROM nightowl_cache_rollups WHERE \"key\" = 'users:1' AND store = 'redis'"
        )->fetch(PDO::FETCH_ASSOC);

        $this->assertSame(8, (int) $row['call_count']);
        $this->assertSame(2, (int) $row['hits']);
        $this->assertSame(1, (int) $row['misses']);
        $this->assertSame(1, (int) $row['writes']);
        $this->assertSame(1, (int) $row['deletes']);
        $this->assertSame(1, (int) $row['fails']);
        $this->assertSame(1, (int) $row['delete_failures']);
        // write_failures includes 'write_fail' AND 'fail'.
        $this->assertSame(2, (int) $row['write_failures']);
        $this->assertSame(1500, (int) $row['total_duration']);
    }

    public function test_cache_rollup_drift_matches_raw(): void
    {
        $types = ['hit', 'miss', 'set', 'forget', 'fail', 'write_fail'];
        $records = [];
        for ($i = 1; $i <= 24; $i++) {
            $records[] = $this->sim->makeCacheEvent([
                'trace_id' => "cd-{$i}",
                'type' => $types[$i % 6],
                'key' => 'k:'.($i % 3),
                'store' => $i % 2 === 0 ? 'redis' : 'file',
                'duration' => $i * 10,
            ]);
        }
        $this->writer->write($records);

        $raw = self::$pdo->query(
            "SELECT COALESCE(\"key\", '') AS k, COALESCE(store, '') AS s, COUNT(*) AS c,
                    SUM(CASE WHEN event_type = 'hit' THEN 1 ELSE 0 END) AS h,
                    SUM(CASE WHEN event_type IN ('write_fail', 'set_fail', 'put_fail', 'fail') THEN 1 ELSE 0 END) AS wf,
                    COALESCE(SUM(duration), 0) AS sd
             FROM nightowl_cache_events GROUP BY 1, 2 ORDER BY 1, 2"
        )->fetchAll(PDO::FETCH_ASSOC);

        $rollup = self::$pdo->query(
            'SELECT "key" AS k, store AS s, SUM(call_count) AS c, SUM(hits) AS h,
                    SUM(write_failures) AS wf, SUM(total_duration) AS sd
             FROM nightowl_cache_rollups GROUP BY 1, 2 ORDER BY 1, 2'
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertEquals($raw, $rollup, 'Cache rollup must match a raw re-aggregation');
    }

    public function test_query_write_sets_created_at(): void
    {
        $this->writer->write([
            $this->sim->makeQuery(['trace_id' => 'created-at-1', 'sql' => 'SELECT now()']),
        ]);

        $createdAt = self::$pdo->query(
            "SELECT created_at FROM nightowl_queries WHERE trace_id = 'created-at-1'"
        )->fetchColumn();

        $this->assertNotNull($createdAt);
        $this->assertNotFalse($createdAt);
    }

    /**
     * Regression: created_at must be stamped in UTC regardless of the agent
     * host's default timezone. Before this fix the writer used date() (local
     * time), so on a non-UTC host (e.g. America/Bogota, UTC-5) created_at was
     * written hours behind the API's UTC now() and the dashboard's short
     * time-range filters showed no data. See gmdate() in RecordWriter.
     */
    public function test_created_at_is_utc_under_non_utc_host_timezone(): void
    {
        $originalTz = date_default_timezone_get();
        date_default_timezone_set('America/Bogota'); // UTC-5

        try {
            $writer = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password);
            $writer->write([
                $this->sim->makeJob(['trace_id' => 'utc-created-at-1']),
            ]);

            $createdAt = self::$pdo->query(
                "SELECT created_at FROM nightowl_jobs WHERE trace_id = 'utc-created-at-1'"
            )->fetchColumn();

            $this->assertNotFalse($createdAt);

            // Interpret the stored string as UTC and compare to UTC now.
            $stored = \DateTimeImmutable::createFromFormat(
                'Y-m-d H:i:s',
                substr((string) $createdAt, 0, 19),
                new \DateTimeZone('UTC')
            );
            $this->assertNotFalse($stored, "Unparseable created_at: {$createdAt}");

            $skew = abs($stored->getTimestamp() - time());

            // With the local-time bug this skew would be ~5h (18000s); UTC
            // stamping keeps it within drain/test latency.
            $this->assertLessThan(
                300,
                $skew,
                "created_at is {$skew}s from UTC now — not stamped in UTC (host tz leaked in)."
            );
        } finally {
            date_default_timezone_set($originalTz);
        }
    }

    /**
     * Regression for the reported "-17923s ago" bug, swept across *every* write
     * path: created_at must be stamped in UTC even when the tenant PostgreSQL
     * server runs in a non-UTC zone.
     *
     * The exception/command/mail/notification/scheduled_task writers and the
     * users upsert omitted created_at entirely and fell back to the column's
     * useCurrent() default (CURRENT_TIMESTAMP), which resolves in the DB session
     * timezone. On a UTC+6 server (Asia/Dhaka — the real customer was in
     * Bangladesh) rows landed ~6h in the future; the dashboard appended "Z" and
     * rendered "LAST SEEN" hours ahead, and short time-range filters dropped
     * fresh data. This test pins all of them so no writer can regress.
     */
    public function test_created_at_is_utc_for_all_write_paths_under_non_utc_db_timezone(): void
    {
        // DB-level setting only affects sessions opened *after* it's applied,
        // so a fresh writer connection inherits the non-UTC zone.
        self::$pdo->exec('ALTER DATABASE '.self::$database." SET timezone = 'Asia/Dhaka'");

        try {
            $writer = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password);
            $writer->write([
                $this->sim->makeRequest(['trace_id' => 'tz-req']),
                $this->sim->makeQuery(['trace_id' => 'tz-qry']),
                $this->sim->makeException(['trace_id' => 'tz-exc']),
                $this->sim->makeJob(['trace_id' => 'tz-job']),
                $this->sim->makeCommand(['trace_id' => 'tz-cmd']),
                $this->sim->makeScheduledTask(['trace_id' => 'tz-sch']),
                $this->sim->makeCacheEvent(['trace_id' => 'tz-cache']),
                $this->sim->makeMail(['trace_id' => 'tz-mail']),
                $this->sim->makeNotification(['trace_id' => 'tz-notif']),
                $this->sim->makeOutgoingRequest(['trace_id' => 'tz-out']),
                $this->sim->makeLog(['trace_id' => 'tz-log']),
                $this->sim->makeUser('tz-user'),
            ]);

            $cases = [
                ['nightowl_requests', "trace_id = 'tz-req'"],
                ['nightowl_queries', "trace_id = 'tz-qry'"],
                ['nightowl_exceptions', "trace_id = 'tz-exc'"],
                ['nightowl_jobs', "trace_id = 'tz-job'"],
                ['nightowl_commands', "trace_id = 'tz-cmd'"],
                ['nightowl_scheduled_tasks', "trace_id = 'tz-sch'"],
                ['nightowl_cache_events', "trace_id = 'tz-cache'"],
                ['nightowl_mail', "trace_id = 'tz-mail'"],
                ['nightowl_notifications', "trace_id = 'tz-notif'"],
                ['nightowl_outgoing_requests', "trace_id = 'tz-out'"],
                ['nightowl_logs', "trace_id = 'tz-log'"],
                ['nightowl_users', "user_id = 'tz-user'"],
            ];

            foreach ($cases as [$table, $where]) {
                $createdAt = self::$pdo->query("SELECT created_at FROM {$table} WHERE {$where}")->fetchColumn();
                $this->assertNotFalse($createdAt, "No row written to {$table}");

                $stored = \DateTimeImmutable::createFromFormat(
                    'Y-m-d H:i:s',
                    substr((string) $createdAt, 0, 19),
                    new \DateTimeZone('UTC')
                );
                $this->assertNotFalse($stored, "Unparseable created_at in {$table}: {$createdAt}");

                // With the useCurrent() bug this skew would be ~6h (21600s) under
                // Asia/Dhaka; explicit gmdate() stamping keeps it within latency.
                $skew = abs($stored->getTimestamp() - time());
                $this->assertLessThan(
                    300,
                    $skew,
                    "{$table}.created_at is {$skew}s from UTC now — the DB session timezone leaked in (useCurrent regression)."
                );
            }
        } finally {
            self::$pdo->exec('ALTER DATABASE '.self::$database." SET timezone = 'UTC'");
        }
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

    // ─── Auto-reopen on recurrence ─────────────────────────

    public function test_resolved_issue_auto_reopens_on_recurrence(): void
    {
        $base = ['class' => 'App\\Exceptions\\ReopenTest', 'file' => 'app/Reopen.php', 'line' => 7];
        $fingerprint = md5('App\\Exceptions\\ReopenTest|0|app/Reopen.php|7');

        // First occurrence creates the issue (status=open)
        $this->writer->write([$this->sim->makeException(array_merge($base, ['trace_id' => 'r1']))]);

        $issueId = (int) self::$pdo->query("SELECT id FROM nightowl_issues WHERE group_hash = '{$fingerprint}'")->fetchColumn();
        $this->assertGreaterThan(0, $issueId);

        // User resolves it (simulate the IssueController/MCP path)
        self::$pdo->exec("UPDATE nightowl_issues SET status = 'resolved', updated_at = NOW() - INTERVAL '1 hour' WHERE id = {$issueId}");
        self::$pdo->exec("INSERT INTO nightowl_issue_activity (issue_id, user_id, user_name, actor_type, action, old_value, new_value, created_at) VALUES ({$issueId}, NULL, 'tester', 'user', 'status_changed', 'open', 'resolved', NOW() - INTERVAL '1 hour')");

        // Recurrence — should flip back to open and append a status_changed activity row
        $this->writer->write([$this->sim->makeException(array_merge($base, ['trace_id' => 'r2']))]);

        $issue = self::$pdo->query("SELECT * FROM nightowl_issues WHERE id = {$issueId}")->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('open', $issue['status'], 'Resolved issue should auto-reopen on recurrence');
        $this->assertSame(2, (int) $issue['occurrences_count']);

        $reopenLog = self::$pdo->query("SELECT * FROM nightowl_issue_activity WHERE issue_id = {$issueId} AND actor_type = 'agent' AND action = 'status_changed' AND old_value = 'resolved' AND new_value = 'open'")->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($reopenLog, 'Agent should log the auto-reopen in nightowl_issue_activity');
    }

    public function test_ignored_issue_stays_ignored_on_recurrence(): void
    {
        $base = ['class' => 'App\\Exceptions\\IgnoredTest', 'file' => 'app/Ignored.php', 'line' => 9];
        $fingerprint = md5('App\\Exceptions\\IgnoredTest|0|app/Ignored.php|9');

        $this->writer->write([$this->sim->makeException(array_merge($base, ['trace_id' => 'i1']))]);

        $issueId = (int) self::$pdo->query("SELECT id FROM nightowl_issues WHERE group_hash = '{$fingerprint}'")->fetchColumn();
        self::$pdo->exec("UPDATE nightowl_issues SET status = 'ignored' WHERE id = {$issueId}");

        $this->writer->write([$this->sim->makeException(array_merge($base, ['trace_id' => 'i2']))]);

        $issue = self::$pdo->query("SELECT * FROM nightowl_issues WHERE id = {$issueId}")->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('ignored', $issue['status'], 'Ignored issues must never auto-reopen');
        $this->assertSame(2, (int) $issue['occurrences_count']);
    }

    public function test_resolved_issue_within_cooldown_stays_resolved(): void
    {
        $base = ['class' => 'App\\Exceptions\\CooldownTest', 'file' => 'app/Cooldown.php', 'line' => 11];
        $fingerprint = md5('App\\Exceptions\\CooldownTest|0|app/Cooldown.php|11');

        // 24-hour cooldown
        $writer = new RecordWriter(
            self::$host, self::$port, self::$database, self::$username, self::$password,
            86400,
            new \NightOwl\Agent\AlertNotifier(86400, '', null, 24),
        );

        $writer->write([$this->sim->makeException(array_merge($base, ['trace_id' => 'c1']))]);

        $issueId = (int) self::$pdo->query("SELECT id FROM nightowl_issues WHERE group_hash = '{$fingerprint}'")->fetchColumn();

        // Resolved 5 minutes ago — well inside the 24h cooldown
        self::$pdo->exec("UPDATE nightowl_issues SET status = 'resolved' WHERE id = {$issueId}");
        self::$pdo->exec("INSERT INTO nightowl_issue_activity (issue_id, user_id, user_name, actor_type, action, old_value, new_value, created_at) VALUES ({$issueId}, NULL, 'tester', 'user', 'status_changed', 'open', 'resolved', NOW() - INTERVAL '5 minutes')");

        $writer->write([$this->sim->makeException(array_merge($base, ['trace_id' => 'c2']))]);

        $issue = self::$pdo->query("SELECT * FROM nightowl_issues WHERE id = {$issueId}")->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('resolved', $issue['status'], 'Cooldown should suppress the reopen');
        $this->assertSame(2, (int) $issue['occurrences_count'], 'Occurrences still accumulate');

        $reopenLog = self::$pdo->query("SELECT 1 FROM nightowl_issue_activity WHERE issue_id = {$issueId} AND actor_type = 'agent'")->fetchColumn();
        $this->assertFalse($reopenLog, 'No activity row should be written when cooldown suppresses the flip');
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
            'nightowl_settings', 'nightowl_alert_channels', 'nightowl_query_rollups',
            'nightowl_request_rollups', 'nightowl_job_rollups', 'nightowl_outgoing_request_rollups',
            'nightowl_cache_rollups', 'nightowl_user_rollups', 'nightowl_user_job_rollups',
            'nightowl_user_exception_rollups', 'nightowl_exception_rollups',
            'nightowl_exception_server_rollups',
            'nightowl_mail_rollups', 'nightowl_notification_rollups',
        ];

        foreach ($tables as $table) {
            self::$pdo->exec("TRUNCATE TABLE {$table} CASCADE");
        }
    }
}
