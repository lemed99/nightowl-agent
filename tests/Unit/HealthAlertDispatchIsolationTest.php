<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\AsyncServer;
use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\HealthAlertNotifier;
use NightOwl\Agent\PayloadParser;
use PHPUnit\Framework\TestCase;

/**
 * The health-alert dispatch must never run on the ingest event loop.
 *
 * A dispatch round is blocking network I/O end to end — loadChannels() opens a
 * Postgres connection, the sends are raw SMTP and HTTP — bounded only by
 * HealthAlertNotifier::MAX_DISPATCH_SECONDS (30s). It used to run inline off
 * AsyncServer's 10s diagnosis timer, so every round was up to 30 seconds during
 * which the agent accepted no telemetry. Worse, it fires on diagnosis
 * TRANSITIONS and the Postgres it must query is the same one whose slowness
 * raises the diagnoses, so a struggling agent flapped and blocked in a loop.
 * (Yomoney, 2026-07-29: loop lag 2ms on v1.4.0 → ~480ms sustained, on a box at
 * 4% CPU.)
 *
 * The unreachable-Postgres DSN below is the customer's condition, reproduced:
 * 192.0.2.1 is RFC 5737 TEST-NET-1, unroutable by definition, so the connect
 * blocks for the full libpq default (measured: 30.0s) rather than failing fast.
 * Anything that waits on the dispatch inherits that 30 seconds.
 */
class HealthAlertDispatchIsolationTest extends TestCase
{
    /** Unroutable — connect() blocks until libpq gives up (~30s). */
    private const BLACKHOLE_DSN = 'pgsql:host=192.0.2.1;port=5432;dbname=x';

    /** Loopback port 1: nothing listens, so connect() fails instantly. */
    private const REFUSED_DSN = 'pgsql:host=127.0.0.1;port=1;dbname=x';

    /** @var int[] children to clean up if a test leaves one running */
    private array $strays = [];

    protected function setUp(): void
    {
        if (! function_exists('pcntl_fork') || ! function_exists('posix_kill')) {
            $this->markTestSkipped('pcntl/posix required');
        }
    }

    protected function tearDown(): void
    {
        foreach ($this->strays as $pid) {
            @posix_kill($pid, SIGKILL);
            @pcntl_waitpid($pid, $status);
        }

        $this->strays = [];
    }

    public function test_dispatch_returns_immediately_instead_of_blocking_on_postgres(): void
    {
        $server = $this->serverWithNotifier(self::BLACKHOLE_DSN);

        $start = microtime(true);
        $this->dispatch($server, [$this->criticalDiagnosis()]);
        $elapsed = microtime(true) - $start;

        // Inline, this call cannot return before the 30s connect gives up. Any
        // bound comfortably under that proves the caller was not made to wait.
        $this->assertLessThan(
            5.0,
            $elapsed,
            'dispatchHealthAlerts() blocked the caller — the event loop stalls for the whole dispatch round'
        );

        $pid = $this->childPid($server);
        $this->assertNotNull($pid, 'no child was forked — the dispatch ran in the ingest process');
        $this->assertNotSame(posix_getpid(), $pid);
        $this->strays[] = $pid;

        // Still connecting to the blackhole, in its own process, while the
        // caller above has already returned. That is the whole property.
        $this->assertTrue(posix_kill($pid, 0), 'dispatch child is gone — the work did not outlive the call');
    }

    public function test_only_one_dispatch_round_is_in_flight_at_a_time(): void
    {
        $server = $this->serverWithNotifier(self::BLACKHOLE_DSN);

        $this->dispatch($server, [$this->criticalDiagnosis()]);
        $first = $this->childPid($server);
        $this->assertNotNull($first);
        $this->strays[] = $first;

        // Diagnoses flap every tick on a struggling agent; without the guard
        // each tick forks another 30-second child on top of the last.
        $this->dispatch($server, [$this->criticalDiagnosis('PG_LATENCY_CRITICAL')]);

        $this->assertSame($first, $this->childPid($server), 'a second child was forked while the first was still running');
    }

    public function test_dispatch_child_is_killed_rather_than_exited(): void
    {
        // exit() runs destructors, which close the SQLite buffer handle the child
        // inherited and corrupt the WAL shared memory
        // (sqlite.org/howtocorrupt.html §2.7). SIGKILL runs none.
        $server = $this->serverWithNotifier(self::REFUSED_DSN);

        $this->dispatch($server, [$this->criticalDiagnosis()]);
        $pid = $this->childPid($server);
        $this->assertNotNull($pid);

        $deadline = microtime(true) + 10.0;
        do {
            $reaped = pcntl_waitpid($pid, $status, WNOHANG);
            if ($reaped !== 0) {
                break;
            }
            usleep(20_000);
        } while (microtime(true) < $deadline);

        $this->assertSame($pid, $reaped, 'dispatch child never terminated');
        $this->assertTrue(pcntl_wifsignaled($status), 'child exited normally — destructors ran on the inherited SQLite handle');
        $this->assertSame(SIGKILL, pcntl_wtermsig($status));
    }

    public function test_nothing_is_forked_when_there_is_nothing_to_dispatch(): void
    {
        $server = $this->serverWithNotifier(self::BLACKHOLE_DSN);

        $this->dispatch($server, []);

        $this->assertNull($this->childPid($server));
    }

    // ─── Helpers ─────────────────────────────────────────────────────

    private function serverWithNotifier(string $dsn): AsyncServer
    {
        $server = new AsyncServer(
            parser: new PayloadParser,
            sqlitePath: sys_get_temp_dir().'/nightowl-alert-isolation-'.getmypid().'.sqlite',
            drainWorker: new DrainWorker(
                sqlitePath: sys_get_temp_dir().'/nightowl-alert-isolation-'.getmypid().'.sqlite',
                pgHost: '127.0.0.1',
                pgPort: 1,
                pgDatabase: 'x',
                pgUsername: 'u',
                pgPassword: 'p',
            ),
            healthEnabled: false,
            healthReportEnabled: false,
        );

        // The property is typed ?HealthAlertNotifier and the class is final, so
        // a real notifier pointed at an unreachable DSN is the only fake there
        // is — and the right one: the connect IS the blocking under test.
        $property = new \ReflectionProperty(AsyncServer::class, 'healthAlertNotifier');
        $property->setValue($server, new HealthAlertNotifier($dsn, 'u', 'p'));

        return $server;
    }

    /**
     * @param  array<int, array<string, mixed>>  $active
     */
    private function dispatch(AsyncServer $server, array $active): void
    {
        $method = new \ReflectionMethod(AsyncServer::class, 'dispatchHealthAlerts');
        $method->invoke($server, $active, []);
    }

    private function childPid(AsyncServer $server): ?int
    {
        $property = new \ReflectionProperty(AsyncServer::class, 'alertChildPid');

        return $property->getValue($server);
    }

    /**
     * @return array<string, mixed>
     */
    private function criticalDiagnosis(string $code = 'LOOP_LAG_CRITICAL'): array
    {
        return [
            'code' => $code,
            'level' => 'critical',
            'message' => 'Event loop lag is 480ms',
            'recommendation' => 'Check for blocking work on the loop',
            'value' => 480,
        ];
    }
}
