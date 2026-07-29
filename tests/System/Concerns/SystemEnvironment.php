<?php

namespace NightOwl\Tests\System\Concerns;

use PHPUnit\Framework\Assert;

/**
 * Decides whether a missing System-suite dependency is an ABSENCE or a FAULT.
 *
 * Every System class skips itself when it cannot reach PostgreSQL or cannot boot
 * the agent. That is right on a laptop with no database — the suite should step
 * aside rather than fail work it was never able to do. It is wrong everywhere
 * else, because PHPUnit prints `OK` for a run that skipped, so a class that
 * quietly skipped all of itself is indistinguishable from one that passed.
 *
 * Observed: a full System run reported `48 tests, 311 assertions, Skipped: 5`
 * where five other runs of the same command reported 48 tests / 363 assertions
 * and no skips. Both printed OK. The suite had silently lost a class's worth of
 * coverage — while exercising a drain fix whose entire point was that a failure
 * had been silent.
 *
 * The rule: an EXPLICITLY CONFIGURED dependency that is missing is a fault.
 * Setting any NIGHTOWL_TEST_DB_* variable is the operator saying "the database
 * is there" — if it then is not, that is a broken environment or a test that
 * knocked it over, and the run must say so. With no such variable set we are on
 * defaults nobody promised, and skipping stays correct.
 *
 * pcntl/posix/zlib keep skipping unconditionally: a missing PHP extension is a
 * property of the interpreter, not a fault the run introduced, and CI asserts
 * them separately (`.github/workflows/tests.yml`) so they cannot go missing
 * unnoticed there.
 */
final class SystemEnvironment
{
    /**
     * Did the operator name a database? Any NIGHTOWL_TEST_DB_* variable counts —
     * CI sets the full set, a local run typically sets only the port.
     */
    public static function databaseWasConfigured(): bool
    {
        foreach (['HOST', 'PORT', 'DATABASE', 'USERNAME', 'PASSWORD'] as $suffix) {
            if (getenv('NIGHTOWL_TEST_DB_'.$suffix) !== false) {
                return true;
            }
        }

        return false;
    }

    /**
     * PostgreSQL could not be reached. Fails when it was configured, skips when
     * it was not.
     */
    public static function postgresUnavailable(\Throwable $e): never
    {
        if (self::databaseWasConfigured()) {
            Assert::fail(
                'PostgreSQL was configured via NIGHTOWL_TEST_DB_* but could not be reached, so this '
                .'class would have skipped silently and the run would still have printed OK. If the '
                .'database is up, suspect the chaos leg: AgentPgOutageSystemTest stops the container '
                ."and a class starting before its restart lands sees exactly this. {$e->getMessage()}"
            );
        }

        Assert::markTestSkipped('PostgreSQL not available. Set NIGHTOWL_TEST_DB_* env vars. ('.$e->getMessage().')');
    }

    /**
     * The agent subprocess did not come up. Once PostgreSQL and the fork
     * extensions are present there is nothing left for the agent to be legitimately
     * absent for — a class that skips here has lost real coverage, which is how
     * six scaling tests can vanish from a green run.
     */
    public static function agentUnavailable(string $reason): never
    {
        if (self::databaseWasConfigured()) {
            Assert::fail(
                'the agent subprocess could not be started in a fully configured System environment, '
                ."so this class would have skipped silently: {$reason}"
            );
        }

        Assert::markTestSkipped($reason);
    }
}
