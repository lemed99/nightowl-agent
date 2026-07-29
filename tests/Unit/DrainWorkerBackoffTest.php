<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\DrainWorker;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Reconnect backoff while PostgreSQL is unreachable.
 *
 * Without it the drain retries at the idle interval forever: measured under a real
 * outage, 856 identical SQLSTATE[08006] lines and ~60% of a core over 3.5 minutes,
 * and 1369 lines / ~27 CPU-seconds over 90s in the follow-up run. Rows are safe in
 * SQLite the whole time, so the retries buy nothing.
 *
 * These cover the two pure helpers. The streak's own bookkeeping — what increments
 * it and, more importantly, what RESETS it — needs a real connection and lives in
 * DrainWorkerBackoffBehaviourTest.
 */
class DrainWorkerBackoffTest extends TestCase
{
    private function worker(int $intervalMs = 100): DrainWorker
    {
        return new DrainWorker(
            sqlitePath: '/tmp/nightowl-backoff-unit.db',
            pgHost: '127.0.0.1',
            pgPort: 5432,
            pgDatabase: 'test',
            pgUsername: 'test',
            pgPassword: 'test',
            intervalMs: $intervalMs,
        );
    }

    private function backoffAt(DrainWorker $worker, int $streak): int
    {
        (new \ReflectionProperty($worker, 'connFailStreak'))->setValue($worker, $streak);

        return (new \ReflectionMethod($worker, 'reconnectBackoffMs'))->invoke($worker);
    }

    /**
     * The ladder starts at ONE idle interval, not at the ceiling — a single dropped
     * connection has to retry promptly or every blip costs seconds of drain latency.
     */
    public function test_backoff_doubles_from_one_idle_interval(): void
    {
        $worker = $this->worker(intervalMs: 100);

        $this->assertSame(100, $this->backoffAt($worker, 1), 'first retry waits one idle interval');
        $this->assertSame(200, $this->backoffAt($worker, 2));
        $this->assertSame(400, $this->backoffAt($worker, 3));
        $this->assertSame(800, $this->backoffAt($worker, 4));
        $this->assertSame(1600, $this->backoffAt($worker, 5));
    }

    /**
     * The ceiling is what keeps recovery prompt. An uncapped ladder would be at
     * ~28 hours by streak 20, so a five-minute outage would leave the drain asleep
     * long after PG came back — strictly worse than the busy-loop it replaced.
     */
    public function test_backoff_is_capped_at_ten_seconds(): void
    {
        $worker = $this->worker(intervalMs: 100);

        $this->assertSame(10_000, $this->backoffAt($worker, 8));

        foreach ([9, 20, 100, 5000, PHP_INT_MAX] as $streak) {
            $this->assertSame(
                10_000,
                $this->backoffAt($worker, $streak),
                "streak {$streak} must stay at the ceiling"
            );
        }
    }

    /**
     * The exponent is clamped BEFORE 2** is evaluated. Without the clamp a long
     * outage overflows the shift to INF, and (int) INF is undefined — on a 64-bit
     * build it lands on PHP_INT_MIN, i.e. a NEGATIVE sleep. usleep() would then
     * throw, taking the drain worker down at exactly the moment it is supposed to
     * be waiting patiently.
     */
    public function test_a_long_outage_never_produces_a_negative_sleep(): void
    {
        $worker = $this->worker(intervalMs: 100);

        foreach ([21, 64, 1024, PHP_INT_MAX] as $streak) {
            $ms = $this->backoffAt($worker, $streak);
            $this->assertGreaterThan(0, $ms, "streak {$streak} produced a non-positive sleep");
            $this->assertLessThanOrEqual(10_000, $ms);
        }
    }

    /**
     * A larger idle interval must still be honoured as the first step — the backoff
     * multiplies the operator's configured cadence, it does not replace it.
     */
    public function test_backoff_scales_with_the_configured_interval(): void
    {
        $worker = $this->worker(intervalMs: 1000);

        $this->assertSame(1000, $this->backoffAt($worker, 1));
        $this->assertSame(2000, $this->backoffAt($worker, 2));
        $this->assertSame(10_000, $this->backoffAt($worker, 6), 'still clamped to the shared ceiling');
    }

    /**
     * Logging fires on powers of two so an outage leaves a readable trail — roughly
     * one line per doubling instead of one per retry.
     */
    #[DataProvider('escalationStreaks')]
    public function test_escalation_fires_only_on_powers_of_two(int $streak, bool $expected): void
    {
        $worker = $this->worker();
        (new \ReflectionProperty($worker, 'connFailStreak'))->setValue($worker, $streak);

        $this->assertSame(
            $expected,
            (new \ReflectionMethod($worker, 'isBackoffEscalation'))->invoke($worker),
            "streak {$streak}"
        );
    }

    public static function escalationStreaks(): array
    {
        return [
            // Zero means "PG is fine" — there is nothing to escalate, and the
            // bit trick alone would answer true for it (0 & -1 === 0).
            'no failures yet' => [0, false],
            'first failure' => [1, true],
            'second' => [2, true],
            'third' => [3, false],
            'fourth' => [4, true],
            'fifth' => [5, false],
            'sixth' => [6, false],
            'seventh' => [7, false],
            'eighth' => [8, true],
            'ninth' => [9, false],
            'sixteenth' => [16, true],
            'seventeenth' => [17, false],
            'thirty-first' => [31, false],
            'thirty-second' => [32, true],
            'one thousand' => [1000, false],
            'kibi' => [1024, true],
        ];
    }

    /**
     * Over a long outage the predicate must fire logarithmically, not linearly. At
     * the drain's 100ms idle cadence 10k retries is ~17 minutes of downtime, and it
     * used to mean 10k log lines.
     *
     * This counts the PREDICATE only. The drain emits one more line than this — the
     * guard also passes when the streak is still 0, so the first failure of an
     * outage is always reported. DrainWorkerBackoffBehaviourTest pins the real
     * end-to-end count through error_log.
     */
    public function test_the_escalation_predicate_fires_fourteen_times_in_ten_thousand_retries(): void
    {
        $worker = $this->worker();
        $escalation = new \ReflectionMethod($worker, 'isBackoffEscalation');
        $streak = new \ReflectionProperty($worker, 'connFailStreak');

        $lines = 0;
        for ($n = 1; $n <= 10_000; $n++) {
            $streak->setValue($worker, $n);
            if ($escalation->invoke($worker)) {
                $lines++;
            }
        }

        $this->assertSame(14, $lines, 'expected one line per doubling up to 8192');
    }
}
