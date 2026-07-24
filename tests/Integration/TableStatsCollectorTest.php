<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\TableStatsCollector;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * The collector's one non-negotiable property, codified: coverage is
 * CATALOG-DRIVEN, so a table nobody registered anywhere still appears in the
 * sample. This is the "we never ask a customer to run SQL, and every future
 * table is covered" mandate as an assertion.
 */
final class TableStatsCollectorTest extends TestCase
{
    private static ?PDO $pdo = null;

    private static string $host;

    private static int $port;

    private static string $database;

    private static string $username;

    private static string $password;

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
    }

    protected function tearDown(): void
    {
        self::$pdo?->exec('DROP TABLE IF EXISTS nightowl_zz_future_feature');
    }

    private function collector(): TableStatsCollector
    {
        return new TableStatsCollector(
            self::$host, self::$port, self::$database, self::$username, self::$password,
            'prefer', 'http://127.0.0.1:1', null, // token null → post() never fires
        );
    }

    /** @return array<string, array<string, mixed>> tables keyed by name */
    private function sample(): array
    {
        $payload = $this->collector()->gather(self::$pdo, time());

        $this->assertSame(2, $payload['v']);
        $this->assertGreaterThan(0, $payload['db_bytes']);

        $byName = [];
        foreach ($payload['tables'] as $t) {
            $byName[$t['name']] = $t;
        }

        return $byName;
    }

    public function test_a_table_registered_nowhere_is_covered_the_moment_it_exists(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_zz_future_feature (id bigserial PRIMARY KEY, payload text)');
        self::$pdo->exec("INSERT INTO nightowl_zz_future_feature (payload) SELECT 'x' FROM generate_series(1, 25)");
        self::$pdo->exec('ANALYZE nightowl_zz_future_feature');

        $tables = $this->sample();

        $this->assertArrayHasKey('nightowl_zz_future_feature', $tables,
            'a brand-new table with ZERO registration anywhere must appear in the sample');
        $this->assertSame(25, $tables['nightowl_zz_future_feature']['live']);
        $this->assertGreaterThan(0, $tables['nightowl_zz_future_feature']['bytes']);
    }

    public function test_partition_children_fold_into_their_logical_parent(): void
    {
        $tables = $this->sample();

        $this->assertArrayHasKey('nightowl_requests', $tables);
        $this->assertGreaterThan(0, $tables['nightowl_requests']['children'],
            'the partitioned raw table must aggregate its children, not list them');

        foreach (array_keys($tables) as $name) {
            $this->assertDoesNotMatchRegularExpression('/_p(\d{8}|default|historic|new)$/', $name,
                "child {$name} leaked into the sample unfolded");
        }
    }

    public function test_rollup_tables_carry_bucket_bounds(): void
    {
        self::$pdo->exec("INSERT INTO nightowl_request_concurrency_rollups (bucket_start, delta_sum, max_prefix)
             VALUES (date_trunc('minute', now() - interval '5 minutes'), 0, 3)
             ON CONFLICT (bucket_start) DO NOTHING");

        $tables = $this->sample();

        $this->assertArrayHasKey('nightowl_request_concurrency_rollups', $tables);
        $this->assertNotNull($tables['nightowl_request_concurrency_rollups']['min_bucket']);
        $this->assertNotNull($tables['nightowl_request_concurrency_rollups']['max_bucket']);

        $this->assertArrayHasKey('nightowl_requests', $tables);
        $this->assertArrayNotHasKey('min_bucket', $tables['nightowl_requests'],
            'raw tables have no bucket concept');
    }

    public function test_diagnostic_sections_cover_the_full_surface(): void
    {
        $payload = $this->collector()->gather(self::$pdo, time());
        $sections = $payload['sections'];

        $this->assertSame(2, $payload['v']);

        // Cluster: I/O + spill + checkpoint + WAL + wraparound.
        $this->assertArrayHasKey('cluster', $sections);
        foreach (['blks_hit', 'blks_read', 'temp_files', 'temp_bytes', 'deadlocks',
            'checkpoints_timed', 'checkpoints_req', 'datfrozenxid_age'] as $k) {
            $this->assertArrayHasKey($k, $sections['cluster'], "cluster.{$k}");
        }

        // Activity: states + waits + worst-case ages, and NEVER query text —
        // the tenant DB can be shared with the customer's own app.
        $this->assertArrayHasKey('activity', $sections);
        foreach (['conns_total', 'conns_active', 'conns_idle_in_txn', 'waiting_on_locks',
            'oldest_txn_s', 'ungranted_locks', 'max_connections'] as $k) {
            $this->assertArrayHasKey($k, $sections['activity'], "activity.{$k}");
        }
        $this->assertStringNotContainsString('query', strtolower(json_encode(array_keys($sections['activity']))),
            'no field may carry or reference query text');

        // Replication: the abandoned-slot disk-filler.
        $this->assertArrayHasKey('replication', $sections);
        $this->assertArrayHasKey('slots_retained_wal_bytes', $sections['replication']);

        // Settings: version + the behavior-explaining knobs.
        $this->assertArrayHasKey('settings', $sections);
        $this->assertArrayHasKey('server_version_num', $sections['settings']);
        $this->assertArrayHasKey('work_mem', $sections['settings']);
    }

    public function test_extended_table_diagnostics_ride_the_extra_bag(): void
    {
        $tables = $this->sample();

        $extra = $tables['nightowl_requests']['extra'] ?? null;
        $this->assertNotNull($extra, 'per-table extended diagnostics must be present');
        foreach (['hot_upd', 'heap_read', 'heap_hit', 'frozen_age', 'autovacuum_count'] as $k) {
            $this->assertArrayHasKey($k, $extra, "extra.{$k}");
        }
    }

    public function test_per_index_rows_answer_which_index_earns_its_keep(): void
    {
        $tables = $this->sample();

        $indexRows = array_filter($tables, fn ($k) => str_starts_with($k, 'index:'), ARRAY_FILTER_USE_KEY);
        $this->assertNotEmpty($indexRows, 'per-index rows must be present');

        // A known index on a real table, with usage + size + validity.
        $match = null;
        foreach ($indexRows as $name => $row) {
            if (str_starts_with($name, 'index:nightowl_requests:')) {
                $match = $row;
                break;
            }
        }
        $this->assertNotNull($match, 'nightowl_requests must expose per-index rows');
        $this->assertGreaterThan(0, $match['bytes']);
        $this->assertArrayHasKey('idx_scan', $match);
        $this->assertSame(0, $match['extra']['invalid'], 'healthy indexes report zero invalid shells');

        // Children fold: no per-index row may carry a partition-child suffix.
        foreach (array_keys($indexRows) as $name) {
            $this->assertDoesNotMatchRegularExpression('/_p\d{8}/', $name,
                "index child {$name} leaked unfolded");
        }
    }

    public function test_agent_side_observations_and_final_tier(): void
    {
        $payload = $this->collector()->gather(self::$pdo, time());
        $sections = $payload['sections'];

        // Connection: what only the agent can measure.
        $this->assertArrayHasKey('connection', $sections);
        $c = $sections['connection'];
        $this->assertEqualsWithDelta(0, $c['clock_skew_s'], 5.0, 'local server: skew ~0');
        $this->assertGreaterThan(0, $c['ping_rtt_ms']);
        $this->assertSame(1, $c['backend_pid_stable'], 'direct connection: stable pid');
        $this->assertSame(0, (int) $c['in_recovery'], 'test DB is a primary');

        // Extensions inventory (plpgsql is always present).
        $this->assertArrayHasKey('extensions', $sections);
        $this->assertArrayHasKey('plpgsql', $sections['extensions']);

        // Cluster additions: archiver + bgwriter cleaning + our functions.
        foreach (['archived_count', 'archive_failed_count', 'buffers_clean',
            'buffers_alloc', 'fn_calls', 'fn_total_time'] as $k) {
            $this->assertArrayHasKey($k, $sections['cluster'], "cluster.{$k}");
        }

        // Activity: per-locktype ungranted breakdown.
        foreach (['ungranted_relation', 'ungranted_tuple', 'ungranted_xid', 'ungranted_advisory'] as $k) {
            $this->assertArrayHasKey($k, $sections['activity'], "activity.{$k}");
        }

        // Settings additions.
        $this->assertArrayHasKey('shared_preload_libraries', $sections['settings']);
        $this->assertArrayHasKey('jit', $sections['settings']);

        // Per-table: toast IO + reloptions surface (rollup tables carry
        // fillfactor=70 from migration 000053, so reloptions must appear).
        $tables = [];
        foreach ($payload['tables'] as $t) {
            $tables[$t['name']] = $t;
        }
        $this->assertArrayHasKey('toast_read', $tables['nightowl_requests']['extra']);
        $this->assertArrayHasKey('reloptions', $tables['nightowl_request_rollups']['extra'],
            'fillfactor tuning on rollup tables must be visible');
        $this->assertStringContainsString('fillfactor', $tables['nightowl_request_rollups']['extra']['reloptions']);
    }

    public function test_payload_is_json_encodable_and_bounded(): void
    {
        $payload = $this->collector()->gather(self::$pdo, time());
        $json = json_encode($payload);

        $this->assertNotFalse($json);
        $this->assertLessThan(131072, strlen($json),
            'a full migrated schema must fit the endpoint cap with headroom');
    }
}
