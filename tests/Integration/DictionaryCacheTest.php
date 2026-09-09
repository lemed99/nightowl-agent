<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Support\DictionaryCache;
use PDO;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * DictionaryCache against a live PostgreSQL — the transaction discipline and
 * concurrent-worker convergence claims are exactly the ones that cannot be
 * proven by unit tests. Requires NIGHTOWL_TEST_DB_* (skips otherwise).
 */
/** Captures execute() parameters so a test can assert what a warm actually bound. */
final class RecordingStatement extends \PDOStatement
{
    /** @var array<int, array|null> */
    public static array $executed = [];

    protected function __construct() {}

    public function execute(?array $params = null): bool
    {
        self::$executed[] = $params;

        return parent::execute($params);
    }
}

class DictionaryCacheTest extends TestCase
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
            self::$pdo = self::connect();
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

        foreach (['nightowl_dict_string', 'nightowl_dict_sql', 'nightowl_dict_route', 'nightowl_dict_trace'] as $t) {
            self::$pdo->exec("DELETE FROM {$t}");
        }
    }

    private static function connect(): PDO
    {
        $dsn = sprintf('pgsql:host=%s;port=%d;dbname=%s', self::$host, self::$port, self::$database);
        $pdo = new PDO($dsn, self::$username, self::$password);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        return $pdo;
    }

    public function test_warm_inserts_and_caches_string_ids(): void
    {
        $cache = new DictionaryCache;

        $cache->warm(self::$pdo, ['string' => [['environment', 'production'], ['queue', 'default']]]);

        $envId = $cache->stringId('environment', 'production');
        $queueId = $cache->stringId('queue', 'default');
        $this->assertNotNull($envId);
        $this->assertNotNull($queueId);
        $this->assertNotSame($envId, $queueId);

        $stored = self::$pdo->query(
            "SELECT id FROM nightowl_dict_string WHERE kind = 'environment' AND value = 'production'"
        )->fetchColumn();
        $this->assertSame((int) $stored, $envId);
    }

    public function test_rewarming_same_values_creates_no_new_rows(): void
    {
        $cache = new DictionaryCache;
        $cache->warm(self::$pdo, ['string' => [['environment', 'production']]]);
        $first = $cache->stringId('environment', 'production');

        $cache->warm(self::$pdo, ['string' => [['environment', 'production']]]);

        $this->assertSame($first, $cache->stringId('environment', 'production'));
        $count = self::$pdo->query("SELECT COUNT(*) FROM nightowl_dict_string")->fetchColumn();
        $this->assertSame(1, (int) $count);
    }

    public function test_hash_keyed_dicts_round_trip_through_warm(): void
    {
        $cache = new DictionaryCache;
        $sqlHash = md5('select * from users where id = ?');
        $traceHash = md5('#0 /app/Foo.php(10): boom()');
        $traceZ = '\x'.bin2hex(gzdeflate('#0 /app/Foo.php(10): boom()', 6));

        $cache->warm(self::$pdo, [
            'sql' => [[$sqlHash, 'select * from users where id = ?', '/app/Foo.php', 10]],
            'route' => [[md5('GET|api.test|/users|users.index|UserController@index|["GET"]'), 'GET', 'api.test', '/users', 'users.index', 'UserController@index', '["GET"]']],
            'trace' => [[$traceHash, $traceZ]],
        ]);

        $this->assertNotNull($cache->sqlId($sqlHash));
        $this->assertNotNull($cache->traceId($traceHash));

        // The stored trace round-trips byte-identically.
        $z = self::$pdo->query('SELECT trace_z FROM nightowl_dict_trace LIMIT 1')->fetchColumn();
        $raw = is_resource($z) ? stream_get_contents($z) : $z;
        $this->assertSame('#0 /app/Foo.php(10): boom()', gzinflate($raw));
    }

    public function test_rewarming_a_trace_bumps_created_at(): void
    {
        // The GC's race-safety rests on the trace warm bumping created_at on
        // every reference (ON CONFLICT DO UPDATE SET created_at = now()), so an
        // actively-used trace can never age into the GC's quarantine window.
        $cache = new DictionaryCache;
        $hash = md5('#0 /app/Boom.php(1): boom()');
        $z = '\x'.bin2hex(gzdeflate('#0 /app/Boom.php(1): boom()', 6));

        $cache->warm(self::$pdo, ['trace' => [[$hash, $z]]]);
        // Age the row well past any quarantine window.
        self::$pdo->exec("UPDATE nightowl_dict_trace SET created_at = now() - interval '30 days'");
        $before = self::$pdo->query('SELECT created_at FROM nightowl_dict_trace LIMIT 1')->fetchColumn();

        // A fresh cache forces the second warm to hit the DB (no LRU hit), the
        // same as the collector's always-re-warm discipline for traces.
        (new DictionaryCache)->warm(self::$pdo, ['trace' => [[$hash, $z]]]);

        $after = self::$pdo->query('SELECT created_at FROM nightowl_dict_trace LIMIT 1')->fetchColumn();
        $this->assertGreaterThan(strtotime($before), strtotime($after));
        // Still exactly one row — the touch is an update, not an insert.
        $this->assertSame(1, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_dict_trace')->fetchColumn());
    }

    public function test_rewarming_after_a_gc_delete_recreates_the_trace_with_a_live_id(): void
    {
        // If the GC won a race and deleted a trace a batch still wants, the
        // warm's INSERT re-creates it (append-only) and hands the write a fresh,
        // live id — so the exception write never lands a dangling trace_ref.
        $hash = md5('#0 /app/Race.php(9): kaboom()');
        $z = '\x'.bin2hex(gzdeflate('#0 /app/Race.php(9): kaboom()', 6));

        (new DictionaryCache)->warm(self::$pdo, ['trace' => [[$hash, $z]]]);
        self::$pdo->exec('DELETE FROM nightowl_dict_trace');

        $cache = new DictionaryCache;
        $cache->warm(self::$pdo, ['trace' => [[$hash, $z]]]);

        $id = $cache->traceId($hash);
        $this->assertNotNull($id);
        $stored = self::$pdo->query('SELECT id FROM nightowl_dict_trace LIMIT 1')->fetchColumn();
        $this->assertSame((int) $stored, $id);
    }

    public function test_concurrent_workers_converge_on_one_id(): void
    {
        // Two independent connections warm the same value — the ON CONFLICT
        // DO NOTHING + fresh-snapshot SELECT contract must give both the same id.
        $a = new DictionaryCache;
        $b = new DictionaryCache;
        $pdoB = self::connect();

        $a->warm(self::$pdo, ['string' => [['job_class', 'App\\Jobs\\SendReceipt']]]);
        $b->warm($pdoB, ['string' => [['job_class', 'App\\Jobs\\SendReceipt']]]);

        $this->assertSame(
            $a->stringId('job_class', 'App\\Jobs\\SendReceipt'),
            $b->stringId('job_class', 'App\\Jobs\\SendReceipt'),
        );
        $count = self::$pdo->query("SELECT COUNT(*) FROM nightowl_dict_string")->fetchColumn();
        $this->assertSame(1, (int) $count);
    }

    public function test_warm_refuses_to_run_inside_a_transaction(): void
    {
        $cache = new DictionaryCache;
        self::$pdo->beginTransaction();

        try {
            $this->expectException(RuntimeException::class);
            $cache->warm(self::$pdo, ['string' => [['environment', 'production']]]);
        } finally {
            self::$pdo->rollBack();
        }
    }

    /**
     * The warm is the one write outside doWrite()'s try, so the classifier that
     * normally names the failing table never runs for it. warmingTable() is how
     * RecordWriter names it instead — without it a dict failure reached the drain
     * worker with a null lastWriteError, which it files as a LOCAL SQLite error:
     * neither health clock stamped, no DRAIN_WRITE_FAILING, nothing for
     * quarantine's per-table breaker to count.
     */
    public function test_warm_names_the_table_it_died_on(): void
    {
        $cache = new DictionaryCache;

        $this->assertNull($cache->warmingTable(), 'no table outside a warm');

        self::$pdo->exec(
            "ALTER TABLE nightowl_dict_route
             ADD CONSTRAINT nightowl_dict_route_test_reject CHECK (path <> 'rejected-by-test')"
        );

        try {
            $cache->warm(self::$pdo, [
                'string' => [['environment', 'production']],
                'route' => [[str_repeat('a', 32), 'GET', '', 'rejected-by-test', 'r', 'A@b', '["GET"]']],
            ]);
            $this->fail('the constrained route should have failed the warm');
        } catch (\PDOException) {
            $this->assertSame('nightowl_dict_route', $cache->warmingTable());
        } finally {
            self::$pdo->exec('ALTER TABLE nightowl_dict_route DROP CONSTRAINT nightowl_dict_route_test_reject');
        }

        // A later clean warm clears it again — a stale name must never be
        // attributed to the next batch's failure.
        $cache->warm(self::$pdo, ['string' => [['queue', 'default']]]);
        $this->assertNull($cache->warmingTable());
    }

    /**
     * The repair replaces a stored value only with a LONGER one for the same hash.
     * Both directions are pinned: a mutation audit found reverting both statements
     * to DO NOTHING passed every test in this file.
     */
    public function test_a_longer_value_repairs_a_clipped_row_and_a_shorter_one_does_not(): void
    {
        $cache = new DictionaryCache;
        $hash = str_repeat('ab', 16);

        // A worker on the old width stored the clipped value.
        $cache->warm(self::$pdo, ['route' => [[$hash, 'GET', null, str_repeat('p', 512), 'r', 'A@b', '["GET"]']]]);
        $id = $cache->routeId($hash);
        // Precondition only (text column, nothing clamps here): the clipped value is stored as given.
        $this->assertSame(512, (int) self::$pdo->query('SELECT length(path) FROM nightowl_dict_route')->fetchColumn());

        // The full value arrives (a different process, or this one after forget()).
        // `action` is the column this repair exists for (the Livewire list), so
        // it is repaired alongside `path` here, not path alone.
        $long = [$hash, 'GET', null, str_repeat('p', 900), 'r', str_repeat('A', 700).'@b', '["GET"]'];
        (new DictionaryCache)->warm(self::$pdo, ['route' => [$long]]);
        $row = self::$pdo->query('SELECT id, length(path) AS p, length(action) AS a FROM nightowl_dict_route')->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(900, (int) $row['p'], 'longer path wins');
        $this->assertSame(702, (int) $row['a'], 'longer action wins');
        $this->assertSame($id, (int) $row['id'], 'id never changes');
        $this->assertSame(1, (int) self::$pdo->query('SELECT count(*) FROM nightowl_dict_route')->fetchColumn());

        // A worker still on the stale width sends the clipped values again.
        (new DictionaryCache)->warm(self::$pdo, ['route' => [[$hash, 'GET', null, str_repeat('p', 512), 'r', 'A@b', '["GET"]']]]);
        $row = self::$pdo->query('SELECT length(path) AS p, length(action) AS a FROM nightowl_dict_route')->fetch(PDO::FETCH_ASSOC);
        $this->assertSame([900, 702], [(int) $row['p'], (int) $row['a']], 'shorter never clobbers');

        // Same two directions for dict_sql.file.
        $sqlHash = str_repeat('cd', 16);
        (new DictionaryCache)->warm(self::$pdo, ['sql' => [[$sqlHash, 'select 1', str_repeat('f', 512), 1]]]);
        (new DictionaryCache)->warm(self::$pdo, ['sql' => [[$sqlHash, 'select 1', str_repeat('f', 650), 1]]]);
        $this->assertSame(650, (int) self::$pdo->query('SELECT length(file) FROM nightowl_dict_sql')->fetchColumn());
        (new DictionaryCache)->warm(self::$pdo, ['sql' => [[$sqlHash, 'select 1', str_repeat('f', 512), 1]]]);
        $this->assertSame(650, (int) self::$pdo->query('SELECT length(file) FROM nightowl_dict_sql')->fetchColumn(), 'sql shorter never clobbers');
    }

    /** forget() empties exactly one map, so its values miss (and re-warm) again. */
    public function test_forget_empties_one_map(): void
    {
        $cache = new DictionaryCache;
        $cache->warm(self::$pdo, [
            'string' => [['environment', 'production']],
            'route' => [[str_repeat('ef', 16), 'GET', null, '/x', 'x', 'A@b', '["GET"]']],
        ]);
        $this->assertNotNull($cache->routeId(str_repeat('ef', 16)));

        $cache->forget('routes');

        $this->assertNull($cache->routeId(str_repeat('ef', 16)));
        $this->assertNotNull($cache->stringId('environment', 'production'), 'other maps untouched');

        $this->expectException(\InvalidArgumentException::class);
        $cache->forget('nope');
    }

    /**
     * dict_string warms are sorted like the other three: two workers inserting an
     * overlapping label set in different orders deadlock under ON CONFLICT — DO
     * NOTHING included. Pinned the same way the trim test pins the hashed dicts:
     * warm order is LRU insertion order, so the coldest entry is the lowest key.
     */
    /**
     * Every warm sorts its rows before the multi-row INSERT — the deadlock
     * avoidance for two workers touching an overlapping set. The property lives
     * in the INSERT's bound parameter order, and ONLY there: the LRU is filled
     * from the SELECT-back's row order, so reading the LRU (as an earlier
     * version of this test did) observed heap order and passed by coincidence.
     * A recording statement class captures what was actually bound.
     */
    public function test_every_warm_binds_its_rows_in_sorted_order(): void
    {
        $recorder = self::connect();
        $recorder->setAttribute(PDO::ATTR_STATEMENT_CLASS, [RecordingStatement::class]);

        $hashes = [str_repeat('ff', 16), str_repeat('00', 16), str_repeat('88', 16)];
        $warms = [
            // name, rows, params per row, key extractor over one row's params
            ['route', array_map(fn ($h) => [$h, 'GET', null, '/'.$h[0], 'n', 'A@b', '["GET"]'], $hashes), 7, fn (array $p) => $p[0]],
            ['sql', array_map(fn ($h) => [$h, 'select '.$h[0], null, 1], $hashes), 4, fn (array $p) => $p[0]],
            ['trace', array_map(fn ($h) => [$h, '\\x'.bin2hex(gzdeflate('t'.$h, 6))], $hashes), 2, fn (array $p) => $p[0]],
            ['string', [['queue', 'zeta'], ['queue', 'alpha'], ['queue', 'mid']], 2, fn (array $p) => $p[0]."\0".$p[1]],
        ];

        foreach ($warms as [$name, $rows, $width, $key]) {
            RecordingStatement::$executed = [];
            (new DictionaryCache)->warm($recorder, [$name => $rows]);

            // The first statement per table is the INSERT; the SELECT-back follows.
            $insertParams = RecordingStatement::$executed[0] ?? null;
            $this->assertIsArray($insertParams, "{$name}: INSERT was recorded");
            $keys = array_map($key, array_chunk($insertParams, $width));
            $sorted = $keys;
            sort($sorted, SORT_STRING);
            $this->assertSame($sorted, $keys, "{$name}: rows must be bound in sorted order");
            $this->assertCount(3, $keys);
        }
    }

    public function test_in_txn_resolution_is_staged_until_promoted(): void
    {
        $cache = new DictionaryCache;

        self::$pdo->beginTransaction();
        $id = $cache->resolveStringInTxn(self::$pdo, 'environment', 'staging');
        $this->assertNotNull($id);
        // Not yet published: a rollback must leave no trace in the LRU.
        $this->assertNull($cache->stringId('environment', 'staging'));
        self::$pdo->rollBack();
        $cache->discardPending();

        $this->assertNull($cache->stringId('environment', 'staging'));

        // The committed path promotes.
        self::$pdo->beginTransaction();
        $id = $cache->resolveStringInTxn(self::$pdo, 'environment', 'staging');
        self::$pdo->commit();
        $cache->promotePending();

        $this->assertSame($id, $cache->stringId('environment', 'staging'));
    }

    public function test_a_single_warm_past_the_sql_cap_evicts_nothing(): void
    {
        // The eviction discipline, stated as the failure it prevents: one drain
        // batch with more distinct sql sites than SQL_CAP (4096) must still hand
        // the write phase an id for EVERY hash. Per-chunk eviction dropped the
        // earliest-warmed ids here, and writeQueriesV2 has no in-txn fallback —
        // those rows COPY in with sql_id NULL against a dict row that exists,
        // which is unrecoverable and logs nothing. The hash is xxh128 over
        // sql+file+line, so a batch this wide is routine (every whereIn arity
        // and every call site is its own entry).
        $cache = new DictionaryCache;
        $misses = [];
        $hashes = [];
        for ($i = 0; $i < 4600; $i++) {
            $hash = md5("sql-site-{$i}");
            $hashes[] = $hash;
            $misses[] = [$hash, "select * from t{$i} where id = ?", "/app/Repo{$i}.php", $i];
        }

        $cache->warm(self::$pdo, ['sql' => $misses]);

        $lost = [];
        foreach ($hashes as $i => $hash) {
            if ($cache->sqlId($hash) === null) {
                $lost[] = $i;
            }
        }
        $this->assertCount(0, $lost, sprintf(
            '%d of %d sql ids were evicted before the write could read them back (first: #%d)',
            count($lost), count($hashes), $lost[0] ?? -1,
        ));
        $this->assertSame(4600, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_dict_sql')->fetchColumn());
    }

    public function test_a_single_warm_past_the_route_and_trace_caps_evicts_nothing(): void
    {
        // Same rule for the other two hash-keyed dicts — route_id (4096) on the
        // request write, trace_ref (2048) on the exception write — and warmed in
        // ONE call, because it is the batch, not the table, that must be atomic.
        $cache = new DictionaryCache;
        $routes = [];
        $routeHashes = [];
        for ($i = 0; $i < 4200; $i++) {
            $hash = md5("GET|/users/{$i}");
            $routeHashes[] = $hash;
            $routes[] = [$hash, 'GET', null, "/users/{$i}", "users.show.{$i}", "UserController@show{$i}", '["GET"]'];
        }
        $traces = [];
        $traceHashes = [];
        for ($i = 0; $i < 2100; $i++) {
            $hash = md5("#0 /app/Boom{$i}.php(1): boom()");
            $traceHashes[] = $hash;
            $traces[] = [$hash, '\x'.bin2hex(gzdeflate("#0 /app/Boom{$i}.php(1): boom()", 6))];
        }

        $cache->warm(self::$pdo, ['route' => $routes, 'trace' => $traces]);

        $lostRoutes = array_filter($routeHashes, fn (string $h): bool => $cache->routeId($h) === null);
        $lostTraces = array_filter($traceHashes, fn (string $h): bool => $cache->traceId($h) === null);
        $this->assertCount(0, $lostRoutes, count($lostRoutes).' of 4200 route ids were evicted mid-batch');
        $this->assertCount(0, $lostTraces, count($lostTraces).' of 2100 trace ids were evicted mid-batch');
    }

    public function test_the_maps_are_trimmed_to_their_caps_once_the_batch_outcome_is_published(): void
    {
        // Deferring eviction is not skipping it: both outcome publishers trim, so
        // an over-cap batch cannot leave the LRU over-cap for the next one. The
        // trim is memory-only — the dict rows it forgets stay in the table and
        // are simply re-warmed when a later batch references them again.
        foreach (['promote', 'discard'] as $outcome) {
            self::$pdo->exec('DELETE FROM nightowl_dict_sql');
            $cache = new DictionaryCache;
            $misses = [];
            $hashes = [];
            for ($i = 0; $i < 4600; $i++) {
                $hash = md5("{$outcome}|select {$i}");
                $hashes[] = $hash;
                $misses[] = [$hash, "select {$i}", null, null];
            }
            $cache->warm(self::$pdo, ['sql' => $misses]);

            if ($outcome === 'promote') {
                $cache->promotePending();
            } else {
                $cache->discardPending();
            }

            // LRU insertion order is the SELECT-back's row order, which is not
            // contractual — so pin the cap, not which specific hash was coldest.
            $survivors = 0;
            foreach ($hashes as $hash) {
                $survivors += $cache->sqlId($hash) === null ? 0 : 1;
            }

            $this->assertSame(4096, $survivors, "{$outcome}: the map must be trimmed to exactly its cap");
            $this->assertSame(4600, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_dict_sql')->fetchColumn());
        }
    }

    public function test_dicts_are_append_only_across_batches(): void
    {
        // A second batch with a "renamed" route tuple must create a NEW row,
        // never mutate the old one (no-loss rule).
        $cache = new DictionaryCache;
        $old = md5('GET|/users|OldController@index');
        $new = md5('GET|/users|NewController@index');

        $cache->warm(self::$pdo, ['route' => [[$old, 'GET', null, '/users', 'users.index', 'OldController@index', '["GET"]']]]);
        $cache->warm(self::$pdo, ['route' => [[$new, 'GET', null, '/users', 'users.index', 'NewController@index', '["GET"]']]]);

        $rows = self::$pdo->query('SELECT action FROM nightowl_dict_route ORDER BY id')->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['OldController@index', 'NewController@index'], $rows);
    }
}
