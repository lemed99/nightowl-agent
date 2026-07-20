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
        // a migration is added (currently 000062's sketch sample counter).
        if (Schema::connection('nightowl')->hasTable('nightowl_request_hourly_rollups')
            && Schema::connection('nightowl')->hasColumn('nightowl_query_rollups', 'sketch')
            && Schema::connection('nightowl')->hasTable('nightowl_logs_pdefault')
            && Schema::connection('nightowl')->hasColumn('nightowl_mail_rollups', 'duration_count')
            && Schema::connection('nightowl')->getConnection()->selectOne(
                "SELECT to_regprocedure('nightowl_ddsketch_count(bytea)') IS NOT NULL AS present"
            )->present) {
            self::$migrated = true;

            return;
        }

        // Stale schema from an older run: early migrations have no hasTable
        // guards, so the chain can't be re-run over them. This is a throwaway
        // test DB — drop every nightowl_* table and migrate fresh.
        if (Schema::connection('nightowl')->hasTable('nightowl_requests')) {
            $conn = Schema::connection('nightowl')->getConnection();
            $tables = $conn->select(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'nightowl\\_%'"
            );
            foreach ($tables as $t) {
                $conn->statement("DROP TABLE IF EXISTS {$t->tablename} CASCADE");
            }
        }

        $migrationsDir = __DIR__.'/../../database/migrations';
        $files = glob($migrationsDir.'/*.php') ?: [];
        sort($files);

        foreach ($files as $file) {
            $migration = require $file;
            if ($migration instanceof Migration) {
                $migration->up();
            }
        }

        self::$migrated = true;
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
