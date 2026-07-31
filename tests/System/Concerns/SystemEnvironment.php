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

    /**
     * Wait for the harness subprocess to accept TCP, and say WHY when it does not.
     *
     * Returns null once the port answers, otherwise a reason string for
     * agentUnavailable(). Three things the four per-class copies this replaces
     * got wrong — none of them caught yet, which is the point: each one turns a
     * specific failure into the same uninformative "did not start within Ns".
     *
     *  - A dead subprocess was waited out to the full deadline and then reported
     *    as a timeout, which reads as slow. proc_get_status() ends the wait the
     *    moment it exits and reports the exit code, so a fatal is never dressed
     *    up as slowness.
     *  - stdout was read ONCE, after the deadline. A pipe buffer is finite
     *    (64KB on Linux): a subprocess that fills it blocks in fwrite() and
     *    never reaches listen(), i.e. the diagnostic could cause the failure it
     *    reports. Drain every iteration.
     *  - The deadline was 5s, which is not a startup budget but a stopwatch on
     *    MigrationRunner: a harness whose warm-schema probe misses builds 69
     *    migrations before it binds, and nothing says a CI disk does that in
     *    five seconds. The ceiling is generous now precisely BECAUSE exit
     *    detection is what ends the wait early — a genuinely broken agent still
     *    fails in the time it takes to die, not in the full timeout.
     */
    public static function awaitAgentPort(
        mixed $process,
        mixed $stdout,
        string $host,
        int $port,
        float $timeout,
    ): ?string {
        $started = microtime(true);
        $deadline = $started + $timeout;
        $output = '';

        $pump = static function () use ($stdout, &$output): void {
            if (is_resource($stdout)) {
                $output .= (string) stream_get_contents($stdout);
            }
        };

        while (microtime(true) < $deadline) {
            $pump();

            $sock = @stream_socket_client('tcp://'.$host.':'.$port, $errno, $errstr, 0.5);
            if ($sock) {
                fclose($sock);

                return null;
            }

            // Only after the connect attempt: a process that bound the port and
            // exited in the same tick should still be reported as bound.
            $status = proc_get_status($process);
            if ($status['running'] === false) {
                $pump();

                return sprintf(
                    'the agent exited (code %s) after %.1fs without binding %s:%d. Output: %s',
                    var_export($status['exitcode'], true),
                    microtime(true) - $started,
                    $host,
                    $port,
                    self::describeOutput($output),
                );
            }

            usleep(100_000);
        }

        $pump();

        return sprintf(
            'the agent was still running but had not bound %s:%d after %.0fs — its boot is either slower '
            .'than the budget (a cold MigrationRunner replay) or blocked (a lock held by another session). '
            .'Output: %s',
            $host,
            $port,
            $timeout,
            self::describeOutput($output),
        );
    }

    private static function describeOutput(string $output): string
    {
        $output = trim($output);

        return $output === '' ? '(nothing on stdout/stderr)' : $output;
    }
}
