<?php

namespace NightOwl\Tests\Integration;

use Illuminate\Config\Repository;
use Illuminate\Database\DatabaseServiceProvider;
use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Events\Dispatcher;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use NightOwl\Commands\RepairCacheRollupKeysCommand;
use PDO;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * The contract nightowl:repair-cache-rollup-keys owes REGARDLESS of how it does
 * the work — shared by both strategies, so neither can drift from the other.
 *
 * That contract is narrow and total: literal keys are REGROUPED onto their
 * CacheKeyTemplate pattern, never dropped. Every counter must still sum to what
 * it summed before, min/max must fold, an already-present pattern row must be
 * merged onto rather than replaced, keys that are already their own pattern must
 * not be rewritten at all, and the tier tables get the same treatment as the
 * minute table.
 *
 * The two strategies then add their own cases in their own subclasses:
 * RepairCacheRollupKeysCommandTest (--in-place: safety margin, --since/--until)
 * and RepairCacheRollupKeysRebuildTest (default: mid-rebuild capture, index and
 * grant replication, abort safety).
 *
 * Runs inside a per-subclass Postgres schema so the suite's real nightowl_*
 * tables are untouched. Skips when PostgreSQL is unavailable.
 */
abstract class CacheRollupRepairTestCase extends TestCase
{
    /** Options that select the strategy under test. */
    abstract protected function strategyOptions(): array;


    protected const TABLES = [
        'nightowl_cache_rollups',
        'nightowl_cache_hourly_rollups',
        'nightowl_cache_daily_rollups',
    ];

    protected static ?PDO $pdo = null;

    /** One schema per subclass: the two strategy suites build and drop the same table names. */
    protected static function schema(): string
    {
        return 'nightowl_cache_repair_'.strtolower(str_replace('\\', '_', substr(strrchr(static::class, '\\'), 1)));
    }

    protected Application $app;

    private static function dbConfig(): array
    {
        return [
            'driver' => 'pgsql',
            'host' => getenv('NIGHTOWL_TEST_DB_HOST') ?: '127.0.0.1',
            'port' => (int) (getenv('NIGHTOWL_TEST_DB_PORT') ?: 5432),
            'database' => getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test',
            'username' => getenv('NIGHTOWL_TEST_DB_USERNAME') ?: 'nightowl_test',
            'password' => getenv('NIGHTOWL_TEST_DB_PASSWORD') ?: 'test123',
            'charset' => 'utf8',
            'search_path' => static::schema(),
        ];
    }

    public static function setUpBeforeClass(): void
    {
        $c = self::dbConfig();

        try {
            self::$pdo = new PDO(
                sprintf('pgsql:host=%s;port=%d;dbname=%s', $c['host'], $c['port'], $c['database']),
                $c['username'],
                $c['password'],
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            );
        } catch (\Throwable) {
            self::$pdo = null;
        }
    }

    protected function setUp(): void
    {
        if (self::$pdo === null) {
            $this->markTestSkipped('PostgreSQL unavailable.');
        }

        self::$pdo->exec('DROP SCHEMA IF EXISTS '.static::schema().' CASCADE');
        self::$pdo->exec('CREATE SCHEMA '.static::schema());
        self::$pdo->exec('SET search_path TO '.static::schema());

        // Migration 000037's shape, and its tier siblings the way migration
        // 000054 builds them (LIKE ... INCLUDING ALL — same columns, same PK).
        foreach (self::TABLES as $table) {
            self::$pdo->exec('CREATE TABLE '.static::schema().'.'.$table.' (
                key varchar(255) NOT NULL DEFAULT \'\',
                store varchar(255) NOT NULL DEFAULT \'\',
                bucket_start timestamp NOT NULL,
                environment varchar(255) NOT NULL DEFAULT \'\',
                call_count bigint NOT NULL DEFAULT 0,
                hits bigint NOT NULL DEFAULT 0,
                misses bigint NOT NULL DEFAULT 0,
                writes bigint NOT NULL DEFAULT 0,
                deletes bigint NOT NULL DEFAULT 0,
                fails bigint NOT NULL DEFAULT 0,
                delete_failures bigint NOT NULL DEFAULT 0,
                write_failures bigint NOT NULL DEFAULT 0,
                total_duration bigint NOT NULL DEFAULT 0,
                min_duration bigint,
                max_duration bigint,
                PRIMARY KEY (key, store, bucket_start, environment)
            )');
        }

        $this->app = new Application(sys_get_temp_dir().'/nightowl-cache-repair-test');
        $this->app->singleton('config', fn () => new Repository([
            'database' => [
                'default' => 'nightowl',
                'connections' => ['nightowl' => self::dbConfig()],
            ],
            'nightowl' => [
                'cache_key_template' => true,
            ],
        ]));
        (new DatabaseServiceProvider($this->app))->register();

        Facade::clearResolvedInstances();
        Facade::setFacadeApplication($this->app);
    }

    protected function tearDown(): void
    {
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);

        self::$pdo?->exec('DROP SCHEMA IF EXISTS '.static::schema().' CASCADE');
    }

    protected function runRepair(array $options = []): string
    {
        $command = new RepairCacheRollupKeysCommand;
        $command->setLaravel($this->app);
        $output = new BufferedOutput;
        $exit = $command->run(new ArrayInput($options + $this->strategyOptions()), $output);
        $text = $output->fetch();
        $this->assertSame(0, $exit, $text);

        return $text;
    }

    /** A bucket old enough to clear the daily tier's safety ceiling. */
    protected function oldBucket(int $daysAgo = 3): string
    {
        return gmdate('Y-m-d H:i:s', (int) (floor((time() - $daysAgo * 86400) / 86400) * 86400));
    }

    protected function insert(string $table, string $key, array $overrides = []): void
    {
        $row = array_merge([
            'store' => 'redis',
            'bucket_start' => $this->oldBucket(),
            'environment' => 'production',
            'call_count' => 1,
            'hits' => 1,
            'misses' => 0,
            'writes' => 0,
            'deletes' => 0,
            'fails' => 0,
            'delete_failures' => 0,
            'write_failures' => 0,
            'total_duration' => 100,
            'min_duration' => 100,
            'max_duration' => 100,
        ], $overrides);

        $stmt = self::$pdo->prepare('INSERT INTO '.static::schema().'.'.$table.'
            (key, store, bucket_start, environment, call_count, hits, misses, writes, deletes, fails,
             delete_failures, write_failures, total_duration, min_duration, max_duration)
            VALUES (:key, :store, :bucket_start, :environment, :call_count, :hits, :misses, :writes, :deletes, :fails,
                    :delete_failures, :write_failures, :total_duration, :min_duration, :max_duration)');
        $stmt->execute(['key' => $key] + $row);
    }

    protected function rows(string $table = 'nightowl_cache_rollups'): array
    {
        return self::$pdo->query('SELECT key, store, environment, call_count, hits, total_duration,
                min_duration, max_duration
            FROM '.static::schema().'.'.$table.' ORDER BY key')->fetchAll(PDO::FETCH_ASSOC);
    }

    /** Byte-sorted in PHP: the DB collation orders '{' against digits by locale. */
    protected function keys(string $table = 'nightowl_cache_rollups'): array
    {
        $keys = self::$pdo->query('SELECT key FROM '.static::schema().'.'.$table)
            ->fetchAll(PDO::FETCH_COLUMN);
        sort($keys, SORT_STRING);

        return $keys;
    }

    public function test_the_pass_keeps_its_own_sql_off_the_query_listeners(): void
    {
        // The agent is normally installed beside laravel/nightwatch, whose query
        // sensor keeps a copy of every statement it is handed. A pass issues
        // thousands, and each merge or projection carries a VALUES list of up to
        // MAX_KEY_BATCH key pairs, so a listener holding them exhausts PHP's
        // usual 128MB CLI limit long before a large tenant's table is finished:
        // measured 404MB peak against 92MB detached, on a 4.7M-row hourly table.
        // The pass therefore runs off the dispatcher — and has to hand it back,
        // or every later query in the process goes unrecorded.
        $this->app->singleton('events', fn () => new Dispatcher($this->app));
        $this->app['db']->purge('nightowl');

        $seen = [];
        $this->app['events']->listen(QueryExecuted::class, static function (QueryExecuted $query) use (&$seen): void {
            $seen[] = $query->sql;
        });

        $connection = $this->app['db']->connection('nightowl');
        $this->assertNotNull(
            $connection->getEventDispatcher(),
            'the connection must start ON the dispatcher, or this test proves nothing',
        );

        $this->insert('nightowl_cache_rollups', 'user:1:profile');
        $this->insert('nightowl_cache_rollups', 'user:2:profile');

        $this->runRepair();

        $this->assertSame([], $seen, 'the pass handed its own SQL to a query listener: '
            .implode(' | ', array_slice($seen, 0, 3)));
        $this->assertNotNull(
            $connection->getEventDispatcher(),
            'the pass kept the dispatcher it borrowed',
        );
    }

    public function test_collapses_literal_keys_into_one_pattern_group(): void
    {
        $this->insert('nightowl_cache_rollups', 'user:1:profile', ['min_duration' => 50, 'max_duration' => 50, 'total_duration' => 50]);
        $this->insert('nightowl_cache_rollups', 'user:2:profile', ['min_duration' => 10, 'max_duration' => 10, 'total_duration' => 10]);
        $this->insert('nightowl_cache_rollups', 'user:3:profile', ['min_duration' => 90, 'max_duration' => 90, 'total_duration' => 90]);

        $this->runRepair();

        $rows = $this->rows();
        $this->assertCount(1, $rows);
        $this->assertSame('user:{int}:profile', $rows[0]['key']);
        $this->assertSame(3, (int) $rows[0]['call_count']);
        $this->assertSame(3, (int) $rows[0]['hits']);
        $this->assertSame(150, (int) $rows[0]['total_duration']);
        $this->assertSame(10, (int) $rows[0]['min_duration']);
        $this->assertSame(90, (int) $rows[0]['max_duration']);
    }

    public function test_merges_onto_a_pattern_row_that_already_exists(): void
    {
        // The steady state after an upgrade: the drain already writes patterns,
        // and the legacy literal rows have to ADD to that row, not replace it.
        $this->insert('nightowl_cache_rollups', 'user:{int}:profile', ['call_count' => 7, 'hits' => 7, 'total_duration' => 700, 'min_duration' => 5, 'max_duration' => 200]);
        $this->insert('nightowl_cache_rollups', 'user:42:profile', ['call_count' => 1, 'hits' => 1, 'total_duration' => 100, 'min_duration' => 100, 'max_duration' => 900]);

        $this->runRepair();

        $rows = $this->rows();
        $this->assertCount(1, $rows);
        $this->assertSame('user:{int}:profile', $rows[0]['key']);
        $this->assertSame(8, (int) $rows[0]['call_count']);
        $this->assertSame(800, (int) $rows[0]['total_duration']);
        $this->assertSame(5, (int) $rows[0]['min_duration']);
        $this->assertSame(900, (int) $rows[0]['max_duration']);
    }

    public function test_keeps_store_and_environment_and_bucket_apart(): void
    {
        $other = gmdate('Y-m-d H:i:s', (int) (floor((time() - 5 * 86400) / 86400) * 86400));

        $this->insert('nightowl_cache_rollups', 'user:1:profile');
        $this->insert('nightowl_cache_rollups', 'user:2:profile', ['store' => 'file']);
        $this->insert('nightowl_cache_rollups', 'user:3:profile', ['environment' => 'staging']);
        $this->insert('nightowl_cache_rollups', 'user:4:profile', ['bucket_start' => $other]);

        $this->runRepair();

        // One pattern, four distinct rows — the rest of the conflict key is
        // still the rest of the conflict key.
        $rows = self::$pdo->query('SELECT key, store, environment, bucket_start FROM '.static::schema().'.nightowl_cache_rollups')
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows);
        foreach ($rows as $row) {
            $this->assertSame('user:{int}:profile', $row['key']);
        }
    }

    public function test_is_idempotent(): void
    {
        $this->insert('nightowl_cache_rollups', 'user:1:profile');
        $this->insert('nightowl_cache_rollups', 'user:2:profile');

        $this->runRepair();
        $first = $this->rows();

        $this->runRepair();

        $this->assertSame($first, $this->rows());
    }

    public function test_preserves_totals_across_a_mixed_population(): void
    {
        $this->insert('nightowl_cache_rollups', 'user:1:profile', ['call_count' => 3, 'total_duration' => 30]);
        $this->insert('nightowl_cache_rollups', 'user:2:profile', ['call_count' => 5, 'total_duration' => 50]);
        $this->insert('nightowl_cache_rollups', 'post:16f38194-b403-4016-a8a1-0a43b1fc1034', ['call_count' => 2, 'total_duration' => 20]);
        $this->insert('nightowl_cache_rollups', 'settings:global', ['call_count' => 11, 'total_duration' => 110]);

        $before = self::$pdo->query('SELECT sum(call_count) AS c, sum(total_duration) AS d FROM '.static::schema().'.nightowl_cache_rollups')->fetch(PDO::FETCH_ASSOC);

        $this->runRepair();

        $after = self::$pdo->query('SELECT sum(call_count) AS c, sum(total_duration) AS d FROM '.static::schema().'.nightowl_cache_rollups')->fetch(PDO::FETCH_ASSOC);

        $this->assertSame($before, $after);
        $this->assertSame(
            ['post:{uuid}', 'settings:global', 'user:{int}:profile'],
            $this->keys(),
        );
    }

    public function test_repairs_tier_tables(): void
    {
        foreach (self::TABLES as $table) {
            $this->insert($table, 'user:1:profile');
            $this->insert($table, 'user:2:profile');
        }

        $this->runRepair();

        foreach (self::TABLES as $table) {
            $this->assertSame(['user:{int}:profile'], $this->keys($table), $table);
        }
    }

    public function test_tier_option_restricts_the_pass(): void
    {
        foreach (self::TABLES as $table) {
            $this->insert($table, 'user:1:profile');
            $this->insert($table, 'user:2:profile');
        }

        $this->runRepair(['--tier' => 'hourly']);

        $this->assertSame(['user:1:profile', 'user:2:profile'], $this->keys('nightowl_cache_rollups'));
        $this->assertSame(['user:{int}:profile'], $this->keys('nightowl_cache_hourly_rollups'));
        $this->assertSame(['user:1:profile', 'user:2:profile'], $this->keys('nightowl_cache_daily_rollups'));
    }

    public function test_does_not_cry_wolf_about_the_plan_on_a_small_table(): void
    {
        // A sequential scan IS the right plan below a million rows; the
        // unindexed-key-scan warning must not fire on every small tenant.
        $this->insert('nightowl_cache_rollups', 'user:1:profile');

        $text = $this->runRepair();

        $this->assertStringNotContainsString('not using an index', $text);
    }

    public function test_dry_run_reports_without_writing(): void
    {
        $this->insert('nightowl_cache_rollups', 'user:1:profile');
        $this->insert('nightowl_cache_rollups', 'user:2:profile');

        $text = $this->runRepair(['--dry-run' => true]);

        $this->assertStringContainsString('would collapse', $text);
        $this->assertSame(['user:1:profile', 'user:2:profile'], $this->keys());
    }

    public function test_a_pattern_split_across_batches_still_produces_one_group(): void
    {
        // 150 keys of one shape against the smallest batch the command will
        // accept (MIN_KEY_BATCH = 100) puts the pattern in TWO transactions.
        // That is the case the additive ON CONFLICT exists for, and the reason
        // batch size is free for the pacer to move: the second transaction must
        // add to the row the first one wrote, not overwrite it.
        $expectedCalls = 0;
        for ($i = 1; $i <= 150; $i++) {
            $this->insert('nightowl_cache_rollups', "user:{$i}:profile", [
                'call_count' => $i,
                'total_duration' => $i * 10,
                'min_duration' => $i,
                'max_duration' => $i,
            ]);
            $expectedCalls += $i;
        }

        $this->runRepair(['--key-batch' => '100']);

        $rows = $this->rows();
        $this->assertCount(1, $rows);
        $this->assertSame('user:{int}:profile', $rows[0]['key']);
        $this->assertSame($expectedCalls, (int) $rows[0]['call_count']);
        $this->assertSame($expectedCalls * 10, (int) $rows[0]['total_duration']);
        $this->assertSame(1, (int) $rows[0]['min_duration']);
        $this->assertSame(150, (int) $rows[0]['max_duration']);
    }

    public function test_missing_tier_tables_are_skipped(): void
    {
        self::$pdo->exec('DROP TABLE '.static::schema().'.nightowl_cache_hourly_rollups');
        self::$pdo->exec('DROP TABLE '.static::schema().'.nightowl_cache_daily_rollups');

        $this->insert('nightowl_cache_rollups', 'user:1:profile');
        $this->insert('nightowl_cache_rollups', 'user:2:profile');

        $this->runRepair();

        $this->assertSame(['user:{int}:profile'], $this->keys());
    }

    public function test_warns_when_templating_is_disabled(): void
    {
        $this->app['config']->set('nightowl.cache_key_template', false);
        $this->insert('nightowl_cache_rollups', 'user:1:profile');

        $text = $this->runRepair();

        $this->assertStringContainsString('cache_key_template is OFF', $text);
    }
}
