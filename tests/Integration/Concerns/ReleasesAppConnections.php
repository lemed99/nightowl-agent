<?php

namespace NightOwl\Tests\Integration\Concerns;

use Illuminate\Contracts\Foundation\Application;
use Illuminate\Database\Capsule\Manager as Capsule;
use ReflectionObject;
use Throwable;

/**
 * Closes the database handles a test opened, at the end of that test.
 *
 * Every integration test builds its own container (`new Application`, or a
 * Capsule) and drains through it, so each test that runs opens its own pool of
 * Postgres connections. PHPUnit keeps every test object alive for the length of
 * the run, so those containers are never collected and their handles stay open
 * until the process exits: the suite's connection count only ever climbs. Run
 * all three suites in one process, as CI does, and the run dies part-way
 * through on `FATAL: sorry, too many clients already` in whichever test happens
 * to be executing when the server's max_connections is reached — which is why
 * the failure moves around between runs and never names the test that caused
 * it.
 *
 * Containers held only in a local variable are no better off: the container
 * graph is circular, so dropping the last reference does not refcount it away.
 * Hence the explicit purge for containers this test still holds, and the cycle
 * collection for the ones it has already let go of.
 */
trait ReleasesAppConnections
{
    /**
     * Call first thing in tearDown, before the facade root is torn down —
     * purging needs the container that owns the connections to still resolve.
     */
    protected function releaseAppConnections(): void
    {
        foreach ((new ReflectionObject($this))->getProperties() as $property) {
            if ($property->isStatic() || ! $property->isInitialized($this)) {
                continue;
            }

            $value = $property->getValue($this);

            if ($value instanceof Application) {
                self::purgeContainer($value);
            } elseif ($value instanceof Capsule) {
                self::purgeManager($value->getDatabaseManager());
            }
        }

        // Containers this test built and dropped are unreachable but circular,
        // so only the cycle collector frees them — and freeing them is what
        // closes their PDO handles.
        gc_collect_cycles();
    }

    private static function purgeContainer(Application $app): void
    {
        try {
            if (! $app->bound('db')) {
                return;
            }

            self::purgeManager($app->make('db'));
        } catch (Throwable) {
            // A container that never resolved a database has nothing to release.
        }
    }

    private static function purgeManager(mixed $manager): void
    {
        try {
            // Purge by the manager's own list rather than a hardcoded set, so a
            // connection some test adds later is covered without touching this.
            foreach (array_keys($manager->getConnections()) as $name) {
                $manager->purge($name);
            }
        } catch (Throwable) {
            // Already disconnected, or a manager mid-teardown. Nothing to do.
        }
    }
}
