<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\RecordWriter;
use NightOwl\Simulator\NightwatchSimulator;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * Storage-format-v2 write path against a live PostgreSQL.
 *
 * The claims proven here are the ones the design mandates as fixtures:
 *  - bytea ('\x'+hex) and uuid-as-text traverse BOTH the COPY TSV escaper and
 *    the insertBatch fallback byte-identically (no in-tree precedent existed
 *    for bytea-through-COPY before this);
 *  - every dictionary-encoded value is byte-recoverable (no-loss rule);
 *  - the v1 and v2 modes produce IDENTICAL rollup rows from the same batch
 *    (rollups read in-memory records, not tables — pinned here);
 *  - malformed ids degrade to NULL + log, never to a drain-blocking error;
 *  - the issues users_count spans both storage families.
 */
class RecordWriterV2Test extends TestCase
{
    private static ?PDO $pdo = null;

    private static string $host;

    private static int $port;

    private static string $database;

    private static string $username;

    private static string $password;

    private RecordWriter $writer;

    private NightwatchSimulator $sim;

    private const V2_TABLES = [
        'nightowl_requests_v2', 'nightowl_queries_v2', 'nightowl_exceptions_v2',
        'nightowl_commands_v2', 'nightowl_jobs_v2', 'nightowl_cache_events_v2',
        'nightowl_mail_v2', 'nightowl_notifications_v2', 'nightowl_outgoing_requests_v2',
        'nightowl_scheduled_tasks_v2', 'nightowl_logs_v2',
    ];

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

        foreach ([...self::V2_TABLES,
            'nightowl_requests', 'nightowl_queries', 'nightowl_exceptions', 'nightowl_jobs',
            'nightowl_dict_string', 'nightowl_dict_sql', 'nightowl_dict_route', 'nightowl_dict_trace',
        ] as $t) {
            self::$pdo->exec("DELETE FROM {$t}");
        }
        self::$pdo->exec('TRUNCATE nightowl_issues CASCADE');
        self::$pdo->exec('TRUNCATE nightowl_query_rollups');

        $this->writer = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password);
        $this->sim = new NightwatchSimulator('test-token');
    }

    // -------------------------------------------------------- full round trip

    public function test_all_twelve_types_land_in_v2_tables(): void
    {
        $this->writer->write([
            $this->sim->makeRequest(),
            $this->sim->makeQuery(),
            $this->sim->makeException(),
            $this->sim->makeCommand(),
            $this->sim->makeJob(),
            $this->sim->makeJobAttempt(),
            $this->sim->makeCacheEvent(),
            $this->sim->makeMail(),
            $this->sim->makeNotification(),
            $this->sim->makeOutgoingRequest(),
            $this->sim->makeScheduledTask(),
            $this->sim->makeLog(),
        ]);

        foreach (self::V2_TABLES as $t) {
            $n = (int) self::$pdo->query("SELECT COUNT(*) FROM {$t}")->fetchColumn();
            $expected = $t === 'nightowl_jobs_v2' ? 2 : 1;
            $this->assertSame($expected, $n, $t);
        }

        // Nothing leaked into v1.
        foreach (['nightowl_requests', 'nightowl_queries', 'nightowl_exceptions'] as $t) {
            $this->assertSame(0, (int) self::$pdo->query("SELECT COUNT(*) FROM {$t}")->fetchColumn(), $t);
        }
    }

    public function test_query_row_is_byte_recoverable(): void
    {
        $execId = '9b2f1c4e-8a3d-4f6b-9c1e-2d5a7b8c9d0e';
        // Relative to now (not a frozen absolute) so the event time never ages
        // out of StorageV2's 366-day past guard and falls back to the drain clock.
        $eventTs = time() - 3600;
        $expectedUs = $eventTs * 1_000_000 + 123456;
        $q = $this->sim->makeQuery([
            'trace_id' => $execId,          // equal → stored NULL
            'execution_id' => $execId,
            'timestamp' => $eventTs.'.123456',
            'sql' => 'select * from "users" where "id" = ?',
            'file' => '/app/Http/Controllers/UserController.php',
            'line' => 42,
        ]);

        $this->writer->write([$q]);

        $row = self::$pdo->query("
            SELECT q.ts_us, q.trace_id, q.execution_id::text AS execution_id,
                   COALESCE(q.trace_id, q.execution_id)::text AS effective_trace,
                   encode(q.group_hash, 'hex') AS group_hash,
                   s.sql, s.file, s.line,
                   env.value AS environment
            FROM nightowl_queries_v2 q
            LEFT JOIN nightowl_dict_sql s ON s.id = q.sql_id
            LEFT JOIN nightowl_dict_string env ON env.id = q.environment_id
        ")->fetch(PDO::FETCH_ASSOC);

        $this->assertSame($expectedUs, (int) $row['ts_us']);
        $this->assertNull($row['trace_id'], 'trace equal to execution must be stored NULL');
        $this->assertSame($execId, $row['execution_id']);
        $this->assertSame($execId, $row['effective_trace'], 'COALESCE must reconstruct the trace id');
        $this->assertSame(strtolower((string) $q['_group']), $row['group_hash']);
        $this->assertSame('select * from "users" where "id" = ?', $row['sql']);
        $this->assertSame('/app/Http/Controllers/UserController.php', $row['file']);
        $this->assertSame(42, (int) $row['line']);
        $this->assertSame('production', $row['environment']);
    }

    public function test_request_blobs_deflate_and_placeholders_null(): void
    {
        $headers = '{"accept":"application/json","x-forwarded-for":"10.1.2.3","user-agent":"Mozilla/5.0 test"}';
        $r = $this->sim->makeRequest([
            'headers' => $headers,
            'payload' => '{}',        // placeholder → NULL
            'context' => 'null',      // placeholder → NULL
        ]);

        $this->writer->write([$r]);

        $row = self::$pdo->query('
            SELECT headers_z, payload_z, context_z, rt.path AS route_path, rt.action AS route_action
            FROM nightowl_requests_v2 req
            LEFT JOIN nightowl_dict_route rt ON rt.id = req.route_id
        ')->fetch(PDO::FETCH_ASSOC);

        $z = is_resource($row['headers_z']) ? stream_get_contents($row['headers_z']) : $row['headers_z'];
        $this->assertSame($headers, gzinflate($z), 'headers must round-trip byte-identical');
        $this->assertNull($row['payload_z']);
        $this->assertNull($row['context_z']);
        $this->assertSame($r['route_path'], $row['route_path']);
        $this->assertSame($r['route_action'], $row['route_action']);
    }

    public function test_exception_trace_dictionary_round_trips(): void
    {
        $trace = "#0 /app/Foo.php(10): boom()\n#1 {main}";
        $e = $this->sim->makeException(['trace' => $trace]);

        $this->writer->write([$e]);

        $row = self::$pdo->query("
            SELECT encode(x.fingerprint, 'hex') AS fingerprint, t.trace_z
            FROM nightowl_exceptions_v2 x
            LEFT JOIN nightowl_dict_trace t ON t.id = x.trace_ref
        ")->fetch(PDO::FETCH_ASSOC);

        $z = is_resource($row['trace_z']) ? stream_get_contents($row['trace_z']) : $row['trace_z'];
        $this->assertSame($trace, gzinflate($z));
        $this->assertSame(strtolower((string) $e['_group']), $row['fingerprint']);

        // The issue upsert keeps its hex-varchar key, matching the bytea via encode().
        $issue = self::$pdo->query('SELECT group_hash, users_count FROM nightowl_issues')->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(strtolower((string) $e['_group']), strtolower($issue['group_hash']));
    }

    // ------------------------------------------- COPY vs INSERT byte identity

    public function test_copy_and_insert_fallback_store_identical_bytes(): void
    {
        $records = [
            // In-window event time (see test_query_row_is_byte_recoverable): a
            // frozen absolute would age out and both paths would fall back to the
            // drain clock — which can differ by a second across the two writes.
            $this->sim->makeQuery(['timestamp' => (time() - 3600).'.500000']),
            $this->sim->makeRequest(['headers' => '{"a":"b"}']),
            $this->sim->makeCacheEvent(),
        ];

        $this->writer->write($records);
        $viaCopy = $this->snapshotV2Rows();

        foreach ([...self::V2_TABLES, 'nightowl_dict_string', 'nightowl_dict_sql', 'nightowl_dict_route', 'nightowl_dict_trace'] as $t) {
            self::$pdo->exec("DELETE FROM {$t}");
        }

        $insertWriter = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password);
        $insertWriter->writeForceInsert($records);
        $viaInsert = $this->snapshotV2Rows();

        $this->assertSame($viaCopy, $viaInsert, 'COPY and INSERT fallback must store identical bytes');
    }

    /** Normalized dump of every v2 row's storage bytes (ids excluded — sequences differ). */
    private function snapshotV2Rows(): array
    {
        $out = [];
        $out['queries'] = self::$pdo->query("
            SELECT ts_us, COALESCE(trace_id::text, ''), COALESCE(execution_id::text, ''),
                   COALESCE(encode(group_hash, 'hex'), ''), s.sql, COALESCE(s.file, ''), COALESCE(s.line, -1)
            FROM nightowl_queries_v2 q LEFT JOIN nightowl_dict_sql s ON s.id = q.sql_id
            ORDER BY ts_us
        ")->fetchAll(PDO::FETCH_NUM);
        $out['requests'] = self::$pdo->query("
            SELECT ts_us, url, COALESCE(encode(headers_z, 'hex'), ''), COALESCE(encode(group_hash, 'hex'), '')
            FROM nightowl_requests_v2 ORDER BY ts_us
        ")->fetchAll(PDO::FETCH_NUM);
        $out['cache'] = self::$pdo->query('
            SELECT ts_us, key, ttl, duration FROM nightowl_cache_events_v2 ORDER BY ts_us
        ')->fetchAll(PDO::FETCH_NUM);

        return $out;
    }

    // ------------------------------------------------------- rollup equality

    public function test_v1_and_v2_modes_produce_identical_query_rollups(): void
    {
        $records = [];
        for ($i = 0; $i < 6; $i++) {
            $records[] = $this->sim->makeQuery(['timestamp' => (string) (time() - 30)]);
        }

        $this->writer->write($records);   // v2 mode
        $v2Rollups = self::$pdo->query('
            SELECT group_hash, connection, call_count, total_duration, min_duration, max_duration
            FROM nightowl_query_rollups ORDER BY group_hash, connection
        ')->fetchAll(PDO::FETCH_ASSOC);

        self::$pdo->exec('TRUNCATE nightowl_query_rollups');
        self::$pdo->exec('DELETE FROM nightowl_queries_v2');

        $v1Writer = new RecordWriter(
            self::$host, self::$port, self::$database, self::$username, self::$password,
            storageV2Config: false,
        );
        $v1Writer->write($records);       // v1 mode, same records
        $v1Rollups = self::$pdo->query('
            SELECT group_hash, connection, call_count, total_duration, min_duration, max_duration
            FROM nightowl_query_rollups ORDER BY group_hash, connection
        ')->fetchAll(PDO::FETCH_ASSOC);

        $this->assertNotEmpty($v2Rollups);
        $this->assertSame($v1Rollups, $v2Rollups, 'rollup rows must be identical across storage modes');
    }

    // ----------------------------------------------------------- degradation

    public function test_malformed_ids_store_null_without_blocking_the_batch(): void
    {
        $q = $this->sim->makeQuery([
            'trace_id' => 'not-a-uuid-at-all',
            'execution_id' => 'also-garbage',
            '_group' => 'zz-not-hex',
        ]);

        $this->writer->write([$q]);

        $row = self::$pdo->query('
            SELECT trace_id, execution_id, group_hash, sql_id FROM nightowl_queries_v2
        ')->fetch(PDO::FETCH_ASSOC);

        $this->assertNotFalse($row, 'the row must land despite malformed ids');
        $this->assertNull($row['trace_id']);
        $this->assertNull($row['execution_id']);
        $this->assertNull($row['group_hash']);
        $this->assertNotNull($row['sql_id'], 'sql dictionary is independent of id validity');
    }

    public function test_users_count_spans_both_storage_families(): void
    {
        // One occurrence written pre-cutover (v1), one post (v2), different
        // users — the issue's users_count must see both. makeException derives
        // _group from class|code|file|line, so pin those and derive the same.
        $base = ['class' => 'App\\SpanError', 'code' => '0', 'file' => '/app/Span.php', 'line' => 7];
        $fingerprint = md5('App\\SpanError|0|/app/Span.php|7');

        $v1Writer = new RecordWriter(
            self::$host, self::$port, self::$database, self::$username, self::$password,
            storageV2Config: false,
        );
        $v1Writer->write([$this->sim->makeException($base + ['user' => 'user-a'])]);

        $this->writer->write([$this->sim->makeException($base + ['user' => 'user-b'])]);

        $count = (int) self::$pdo->query(
            "SELECT users_count FROM nightowl_issues WHERE group_hash = '{$fingerprint}'"
        )->fetchColumn();

        $this->assertSame(2, $count, 'users_count must union v1 and v2 occurrences');
    }

    /**
     * Prune's v1-EOL DROPS the v1 raw parent under a RUNNING daemon, and the
     * concurrency recompute named `nightowl_requests` unconditionally — so from
     * the minute of the drop every cleanup tick raised 42P01, aborted, and was
     * swallowed into error_log. The table froze while every other rollup stayed
     * current; the API's coverage gate then read a stale max(bucket_start),
     * fell back to its raw sweep, and 14d peak-concurrency charts 57014'd.
     *
     * Renamed rather than dropped so the shared test DB survives: the probe the
     * fix added reads `to_regclass`, which answers NULL either way, and the
     * children of a renamed partitioned parent stay attached to it.
     */
    public function test_concurrency_maintenance_outlives_the_v1_raw_parent(): void
    {
        self::$pdo->exec('DELETE FROM nightowl_request_concurrency_rollups');

        $minute = (intdiv(time(), 60) - 10) * 60;

        // Two overlapping requests inside one minute → peak 2 in flight.
        $this->writer->write([
            $this->sim->makeRequest(['timestamp' => (float) $minute, 'duration' => 10_000_000]),
            $this->sim->makeRequest(['timestamp' => (float) ($minute + 2), 'duration' => 4_000_000]),
        ]);

        self::$pdo->exec('ALTER TABLE nightowl_requests RENAME TO nightowl_requests__eol');

        try {
            $this->writer->maintainConcurrencyRollup(time());
        } finally {
            self::$pdo->exec('ALTER TABLE nightowl_requests__eol RENAME TO nightowl_requests');
        }

        $row = self::$pdo->query(
            "SELECT delta_sum, max_prefix FROM nightowl_request_concurrency_rollups
             WHERE bucket_start = to_timestamp({$minute}) AT TIME ZONE 'UTC'"
        )->fetch(PDO::FETCH_ASSOC);

        $this->assertNotFalse($row, 'the v2-only fold must still be written after v1 EOL');
        $this->assertSame(0, (int) $row['delta_sum'], 'both requests start AND end in the minute');
        $this->assertSame(2, (int) $row['max_prefix'], 'the two requests overlapped');
    }
}
