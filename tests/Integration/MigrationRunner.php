<?php

namespace NightOwl\Tests\Integration;

use Illuminate\Container\Container;
use Illuminate\Database\Capsule\Manager as Capsule;
use Illuminate\Database\Migrations\Migration;
use Illuminate\Events\Dispatcher;
use Illuminate\Support\Facades\Facade;
use Illuminate\Support\Facades\Schema;

/**
 * Runs the agent's package migrations against a test PostgreSQL database.
 *
 * The integration test fixtures used to declare inline CREATE TABLE SQL that
 * drifted as new migrations landed. This runner makes the migrations
 * themselves the single source of truth — any migration added under
 * `database/migrations/` is picked up automatically on the next test run.
 */
final class MigrationRunner
{
    private static bool $booted = false;

    private static bool $migrated = false;

    private static ?Container $container = null;

    /**
     * Migrations that leave NO schema artifact the probe below could assert, so
     * the warm-DB fast path replays them instead of proving them.
     *
     * Only safe for a body that is idempotent AND forward-only by construction,
     * which is verified per entry, not assumed:
     *  - 000069 delegates to V2SequenceFence::apply, which skips any sequence
     *    still holding MIN_HEADROOM and otherwise setvals it to
     *    MAX(v1.id) + GAP — strictly above the value it just read, since it only
     *    acts when that value is below MAX(v1.id) + GAP/2. Replaying it on a DB
     *    already at 000069 is a no-op.
     *
     * @var list<string>
     */
    private const REPLAY_ALWAYS = [
        '2024_01_01_000069_refence_raw_v2_id_sequences.php',
    ];

    public static function migrate(string $host, int $port, string $database, string $username, string $password): void
    {
        self::bootCapsule($host, $port, $database, $username, $password);

        // Migrations are monotonic for a single PHPUnit run — running them
        // again across test classes would hit duplicate-table errors.
        if (self::$migrated) {
            return;
        }

        // Cross-process guard: the harness subprocess re-enters this method
        // with its own static state. Probe the NEWEST migration's observable
        // effect — probing an early artifact would skip every migration added
        // since the test DB was first provisioned. Update this probe whenever
        // a migration is added (high-water 000072 searchable log context; 000071 rollup tier autovacuum floor; 000070 linear ddsketch aggregate;
        // 000069 v2 id-sequence re-fence is artifact-less and rides
        // REPLAY_ALWAYS instead of a clause; before that 000068
        // dict_trace.created_at, 000066 dictionaries + 000067 raw v2 family,
        // 000063 concurrency rollup, 000064 mail/notification composites,
        // 000065 cache key_pattern).
        if (Schema::connection('nightowl')->hasTable('nightowl_dict_string')
            && Schema::connection('nightowl')->hasColumn('nightowl_logs_v2', 'context')
            && Schema::connection('nightowl')->hasColumn('nightowl_dict_trace', 'created_at')
            && Schema::connection('nightowl')->hasTable('nightowl_requests_v2')
            && Schema::connection('nightowl')->hasTable('nightowl_request_hourly_rollups')
            && Schema::connection('nightowl')->hasColumn('nightowl_query_rollups', 'sketch')
            && Schema::connection('nightowl')->hasTable('nightowl_logs_pdefault')
            && Schema::connection('nightowl')->hasColumn('nightowl_mail_rollups', 'duration_count')
            && Schema::connection('nightowl')->hasTable('nightowl_request_concurrency_rollups')
            && Schema::connection('nightowl')->hasColumn('nightowl_cache_events', 'key_pattern')
            && Schema::connection('nightowl')->getConnection()->selectOne(
                "SELECT to_regclass('nightowl_mail_group_hash_created_at_idx') IS NOT NULL AS present"
            )->present
            && Schema::connection('nightowl')->getConnection()->selectOne(
                "SELECT to_regprocedure('nightowl_ddsketch_count(bytea)') IS NOT NULL AS present"
            )->present
            && Schema::connection('nightowl')->getConnection()->selectOne(
                // The aggregate's state type, not the existence of accum():
                // 000057's CREATE OR REPLACE AGGREGATE silently reverts the
                // state to bytea while leaving 000070's functions in place, so
                // probing for the function would call that DB migrated.
                "SELECT (SELECT format_type(a.aggtranstype, NULL) FROM pg_aggregate a
                         JOIN pg_proc p ON p.oid = a.aggfnoid
                         WHERE p.proname = 'nightowl_ddsketch_agg') = 'bigint[]' AS present"
            )->present
            && Schema::connection('nightowl')->getConnection()->selectOne(
                // 000071's only artifact is reloptions, so probe them directly.
                // A tier table is the right probe: the absolute vacuum floor is
                // applied to the coarse tiers and nowhere else.
                "SELECT EXISTS (
                     SELECT 1 FROM pg_class c
                       JOIN pg_namespace n ON n.oid = c.relnamespace
                      WHERE n.nspname = current_schema()
                        AND c.relname = 'nightowl_request_daily_rollups'
                        AND c.reloptions @> ARRAY['autovacuum_vacuum_threshold=50000']
                 ) AS present"
            )->present) {
            // Warm DB: the glob/replay loop below never runs, so an
            // artifact-less migration would never execute in CI once the test DB
            // reached the previous high-water mark. Run those here.
            self::replayArtifactless();

            self::$migrated = true;

            return;
        }

        // Stale schema from an older run: early migrations have no hasTable
        // guards, so the chain can't be re-run over them. This is a throwaway
        // test DB — drop every nightowl_* table and migrate fresh.
        if (Schema::connection('nightowl')->hasTable('nightowl_requests')) {
            self::dropEveryTable();
        }

        self::replayChain();

        self::$migrated = true;
    }

    /**
     * Drop every nightowl_* relation and replay the whole chain, mid-run.
     *
     * For the one test class that RETIRES schema rather than merely writing to
     * it: PruneV1EolTest proves the prune-integrated v1 EOL step by letting it
     * DROP v1 parents, and it cannot put them back. Its `CREATE TABLE (LIKE …)`
     * shape backup copies columns and NOT NULLs, which is all that class needs,
     * but it does not copy PARTITIONING — so the restored table is a plain
     * table, and PartitioningTest asserts exactly that distinction ("no
     * unpartitioned table holds rows"). Whether the suite passed came down to
     * which class PHPUnit ran first, and executionOrder="defects" makes that
     * vary between otherwise identical runs.
     *
     * migrate()'s warm-DB probe cannot rescue it either: every artifact it
     * checks (dictionaries, the v2 family, rollup columns, the log partition,
     * the ddsketch function) survives a v1 parent drop untouched, so the fast
     * path short-circuits and the dropped parents are never recreated for the
     * rest of the process.
     *
     * Only the migrations know the real shape, so replay them. Called from
     * tearDownAfterClass, which bounds the cost to once per run.
     */
    public static function rebuild(string $host, int $port, string $database, string $username, string $password): void
    {
        self::bootCapsule($host, $port, $database, $username, $password);

        self::dropEveryTable();
        self::replayChain();

        self::$migrated = true;
    }

    /**
     * Every nightowl_* table plus the `__{table}_shape_backup` scratch tables
     * PruneV1EolTest leaves behind — those do NOT match the nightowl_ prefix,
     * and leaving one would make the next rebuild's replay collide on a name
     * that is not supposed to exist.
     */
    private static function dropEveryTable(): void
    {
        $conn = Schema::connection('nightowl')->getConnection();
        $tables = $conn->select(
            "SELECT tablename FROM pg_tables
             WHERE schemaname = 'public'
               AND (tablename LIKE 'nightowl\\_%' OR tablename LIKE '\\_\\_nightowl\\_%')"
        );
        foreach ($tables as $t) {
            $conn->statement("DROP TABLE IF EXISTS \"{$t->tablename}\" CASCADE");
        }
    }

    private static function replayChain(): void
    {
        $files = glob(__DIR__.'/../../database/migrations/*.php') ?: [];
        sort($files);

        foreach ($files as $file) {
            $migration = require $file;
            if ($migration instanceof Migration) {
                $migration->up();
            }
        }
    }

    /**
     * Replay the REPLAY_ALWAYS migrations. Reached only from the warm-DB fast
     * path — the cold path's glob loop already includes them, in file order.
     */
    private static function replayArtifactless(): void
    {
        foreach (self::REPLAY_ALWAYS as $name) {
            $migration = require __DIR__.'/../../database/migrations/'.$name;
            if ($migration instanceof Migration) {
                $migration->up();
            }
        }
    }

    private static function bootCapsule(string $host, int $port, string $database, string $username, string $password): void
    {
        if (self::$booted) {
            // Already wired up — but a Laravel-booting test class
            // (PruneCommandTest, BackfillRollupsFailureTest) may have nulled
            // the facade root in its tearDown since, and executionOrder
            // "defects" makes class order vary run to run. Re-point global
            // state at OUR container so the eval'd migrations' Schema:: calls
            // keep resolving whatever ran before, then refresh the connection
            // so subsequent test classes get a clean PDO handle.
            Container::setInstance(self::$container);
            Facade::clearResolvedInstances();
            Facade::setFacadeApplication(self::$container);
            self::$container['db']->purge('nightowl');

            return;
        }

        $container = Container::getInstance() ?: new Container;
        Container::setInstance($container);
        self::$container = $container;

        $capsule = new Capsule($container);
        $capsule->addConnection([
            'driver' => 'pgsql',
            'host' => $host,
            'port' => $port,
            'database' => $database,
            'username' => $username,
            'password' => $password,
            'charset' => 'utf8',
            'prefix' => '',
            'schema' => 'public',
        ], 'nightowl');

        $capsule->setEventDispatcher(new Dispatcher($container));
        $capsule->setAsGlobal();
        $capsule->bootEloquent();

        // Schema facade resolves 'db' from the container — register the
        // DatabaseManager under that key and point facades at our container.
        $container->instance('db', $capsule->getDatabaseManager());
        Facade::setFacadeApplication($container);

        self::$booted = true;
    }
}
