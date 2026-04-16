<?php

namespace NightOwl\Tests\Integration;

use Illuminate\Container\Container;
use Illuminate\Database\Capsule\Manager as Capsule;
use Illuminate\Database\Migrations\Migration;
use Illuminate\Events\Dispatcher;
use Illuminate\Support\Facades\Facade;

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

    public static function migrate(string $host, int $port, string $database, string $username, string $password): void
    {
        self::bootCapsule($host, $port, $database, $username, $password);

        // Migrations are monotonic for a single PHPUnit run — running them
        // again across test classes would hit duplicate-table errors.
        if (self::$migrated) {
            return;
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
        $container = Container::getInstance() ?: new Container;
        Container::setInstance($container);

        if (self::$booted) {
            // Already wired up — just refresh the connection so subsequent
            // test classes get a clean PDO handle against the same DB.
            $container['db']->purge('nightowl');

            return;
        }

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
