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
