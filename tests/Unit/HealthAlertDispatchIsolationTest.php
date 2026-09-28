<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\AsyncServer;
use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\HealthAlertNotifier;
use NightOwl\Agent\PayloadParser;
use NightOwl\Agent\SqliteBuffer;
use NightOwl\Tests\Fixtures\ScriptedServer;
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
 * blocks until its bound (the notifier's 5s, PDO's 30s default before that)
 * rather than failing fast. Anything that waits on the dispatch inherits it.
 *
 * The same child runs the periodic refresh of the saved alert-channel list
 * (primeAlertChannels()), which gives way to an alert round.
 */
class HealthAlertDispatchIsolationTest extends TestCase
{
    /** Unroutable — connect() blocks until the connect bound gives up. */
    private const BLACKHOLE_DSN = 'pgsql:host=192.0.2.1;port=5432;dbname=x';

    /** Loopback port 1: nothing listens, so connect() fails instantly. */
    private const REFUSED_DSN = 'pgsql:host=127.0.0.1;port=1;dbname=x';

    /** @var int[] children to clean up if a test leaves one running */
    private array $strays = [];

    private ?string $dir = null;

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

        if ($this->dir !== null) {
            foreach (['db', 'buffer'] as $sub) {
                foreach (glob($this->dir.'/'.$sub.'/*') ?: [] as $file) {
                    @unlink($file);
                }
                @rmdir($this->dir.'/'.$sub);
            }
            @rmdir($this->dir);
            $this->dir = null;
        }
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

    // ─── Priming the saved channel list ──────────────────────────────

    /**
     * The saved list is what a round falls back to with the database down,
     * and only a round that reached the database wrote it. The prime writes
     * it while healthy, from a child, so the loop never runs the query.
     */
    public function test_a_prime_saves_the_channel_list_from_a_child(): void
    {
        $dsn = $this->channelDb('ops-hook');
        $server = $this->serverWithNotifier($dsn, $this->sidecar());

        $this->prime($server);
        $pid = $this->childPid($server);

        $this->assertNotNull($pid, 'no child was forked — the prime ran in the ingest process');
        $this->assertNotSame(posix_getpid(), $pid);
        $this->assertSame($pid, $this->waitForExit($pid), 'prime child never terminated');
        $this->assertFileExists($this->sidecar());

        $saved = json_decode((string) file_get_contents($this->sidecar()), true);
        $this->assertSame(['ops-hook'], array_column($saved['channels'] ?? [], 'name'));
    }

    /**
     * A prime stuck on the database must not cost an alert: the round kills
     * it and runs, where it would skip for another alert round.
     */
    public function test_an_alert_round_cuts_a_prime_short(): void
    {
        $this->channelDb();
        $server = $this->serverWithNotifier(self::BLACKHOLE_DSN, $this->sidecar());

        $this->prime($server);
        $prime = $this->childPid($server);
        $this->assertNotNull($prime);
        $this->strays[] = $prime;

        $this->dispatch($server, [$this->criticalDiagnosis()]);
        $round = $this->childPid($server);
        $this->assertNotNull($round);
        $this->strays[] = $round;

        $this->assertNotSame($prime, $round, 'the alert round was skipped while a prime was in flight');
        $this->assertFalse(posix_kill($prime, 0), 'the prime was left running next to the alert round');
        $this->assertTrue(posix_kill($round, 0));
    }

    public function test_a_prime_does_not_displace_an_alert_round(): void
    {
        $this->channelDb();
        $server = $this->serverWithNotifier(self::BLACKHOLE_DSN, $this->sidecar());

        $this->dispatch($server, [$this->criticalDiagnosis()]);
        $round = $this->childPid($server);
        $this->assertNotNull($round);
        $this->strays[] = $round;

        $this->prime($server);

        $this->assertSame($round, $this->childPid($server), 'a prime replaced the alert round in flight');
        $this->assertTrue(posix_kill($round, 0), 'the alert round was killed for a prime');
    }

    /** A prime left running on a slow database would outlive the agent. */
    public function test_shutdown_stops_a_prime_in_flight(): void
    {
        $this->channelDb();
        $server = $this->serverWithNotifier(self::BLACKHOLE_DSN, $this->sidecar());

        $this->prime($server);
        $prime = $this->childPid($server);
        $this->assertNotNull($prime);
        $this->strays[] = $prime;

        (new \ReflectionMethod(AsyncServer::class, 'shutdown'))->invoke($server);

        $this->assertFalse(posix_kill($prime, 0), 'the prime outlived the agent');
    }

    /**
     * The buffer's owner lock comes through the fork, and the kernel keeps it
     * while any copy is open. An alert child that kept its copy held the
     * buffer until its round ended, so an agent that died with a round or a
     * prime in flight kept the next agent from starting.
     */
    public function test_an_alert_child_does_not_keep_the_buffer_lock(): void
    {
        $this->channelDb();
        $buffer = $this->dir.'/buffer/agent-buffer.sqlite';
        $server = $this->serverWithNotifier(self::BLACKHOLE_DSN, $this->sidecar());

        $lock = SqliteBuffer::acquireOwnerLock($buffer);
        $this->assertNotNull($lock);
        $server->setBufferLock($lock);

        $this->prime($server);
        $child = $this->childPid($server);
        $this->assertNotNull($child);
        $this->strays[] = $child;

        // The agent's own copy goes, as when the agent dies. The child is
        // still waiting on the unroutable database, for 5s.
        fclose($lock);

        $next = null;
        $deadline = microtime(true) + 2.0;
        do {
            $next = SqliteBuffer::acquireOwnerLock($buffer);
            if ($next !== null) {
                break;
            }
            usleep(20_000);
        } while (microtime(true) < $deadline);

        $this->assertSame(0, pcntl_waitpid($child, $status, WNOHANG), 'the child ended before the lock was checked, so this proves nothing');
        $this->assertNotNull($next, 'the alert child kept the buffer lock after the agent let go of it');
        fclose($next);
    }

    // ─── A database that goes silent ─────────────────────────────────

    /**
     * A server that completes the handshake and never answers a query. The
     * notifier's statement timeout cannot end this, because the server is the
     * one that would enforce it, and nothing in libpq does. The round stayed
     * in flight for good, and every later transition was skipped as still in
     * flight. The child's alarm ends it.
     */
    public function test_an_alert_child_stuck_on_a_silent_database_is_ended_by_its_alarm(): void
    {
        $pg = ScriptedServer::start('fake-pg-startup.php', ['ready' => true, 'hold' => 20]);
        $log = tempnam(sys_get_temp_dir(), 'nightowl-alert-alarm-');
        $previousLog = ini_set('error_log', $log);

        try {
            $server = $this->serverWithNotifier("pgsql:host=127.0.0.1;port={$pg->port};dbname=x");
            (new \ReflectionProperty(AsyncServer::class, 'alertChildMaxSeconds'))->setValue($server, 2);

            $this->dispatch($server, [$this->criticalDiagnosis()]);
            $pid = $this->childPid($server);
            $this->assertNotNull($pid);
            $this->strays[] = $pid;

            // Reaped the way the agent reaps it, through the SIGCHLD handler.
            $reap = new \ReflectionMethod(AsyncServer::class, 'reapChildren');
            $deadline = microtime(true) + 8.0;
            while ($this->childPid($server) !== null && microtime(true) < $deadline) {
                $reap->invoke($server);
                usleep(20_000);
            }

            $this->assertNull($this->childPid($server), 'the alert child was still waiting on the silent database');
            $this->assertStringContainsString('Health alert dispatch was stopped after 2s', (string) file_get_contents($log), 'the child ended, but not by its alarm');
            $this->assertContains('QUERY BEGIN', $pg->transcript(), 'the child never got past the connect, so this proves nothing about a silent query');
        } finally {
            ini_set('error_log', $previousLog === false ? '' : $previousLog);
            @unlink($log);
            $pg->cleanup();
        }
    }

    // ─── Reaping ─────────────────────────────────────────────────────

    /**
     * The SIGCHLD handler reaps the alert child along with everything else.
     * It kept the pid, so once the kernel gave that pid to a new process (a
     * respawned drain worker), the WNOHANG check read it as the alert child
     * still running, and stopAlertChild() would SIGKILL it.
     */
    public function test_reaping_the_alert_child_forgets_its_pid(): void
    {
        $server = $this->serverWithNotifier(self::REFUSED_DSN);

        $this->prime($server);
        $pid = $this->childPid($server);
        $this->assertNotNull($pid);
        $this->strays[] = $pid;
        $this->assertTrue((new \ReflectionProperty(AsyncServer::class, 'alertChildIsPrime'))->getValue($server));

        $reap = new \ReflectionMethod(AsyncServer::class, 'reapChildren');
        $deadline = microtime(true) + 10.0;
        do {
            $reap->invoke($server);
            if ($this->childPid($server) === null) {
                break;
            }
            usleep(20_000);
        } while (microtime(true) < $deadline);

        $this->assertSame(-1, pcntl_waitpid($pid, $status, WNOHANG), 'the handler did not reap the alert child');
        $this->assertNull($this->childPid($server), 'the reaped alert child\'s pid was kept');
        $this->assertFalse((new \ReflectionProperty(AsyncServer::class, 'alertChildIsPrime'))->getValue($server));
    }

    // ─── Helpers ─────────────────────────────────────────────────────

    private function serverWithNotifier(string $dsn, ?string $channelsFile = null): AsyncServer
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
        $property->setValue($server, new HealthAlertNotifier($dsn, 'u', 'p', channelsFile: $channelsFile));

        return $server;
    }

    private function prime(AsyncServer $server): void
    {
        (new \ReflectionMethod(AsyncServer::class, 'primeAlertChannels'))->invoke($server);
    }

    /** A SQLite stand-in for the channel table, as in HealthAlertNotifierTest. Returns its DSN. */
    private function channelDb(string ...$webhooks): string
    {
        $this->dir = sys_get_temp_dir().'/nightowl-alert-prime-'.getmypid().'-'.bin2hex(random_bytes(4));
        mkdir($this->dir.'/db', 0777, true);
        mkdir($this->dir.'/buffer', 0777, true);

        $dsn = 'sqlite:'.$this->dir.'/db/channels.sqlite';
        $db = new \PDO($dsn);
        $db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $db->exec('CREATE TABLE nightowl_alert_channels (
            id integer primary key,
            name text NOT NULL,
            type text NOT NULL,
            config text NOT NULL,
            enabled boolean NOT NULL DEFAULT 1,
            updated_at text NULL
        )');

        foreach ($webhooks as $name) {
            $db->prepare("INSERT INTO nightowl_alert_channels (name, type, config, enabled, updated_at) VALUES (?, 'webhook', ?, 1, datetime('now'))")
                ->execute([$name, json_encode(['url' => 'http://127.0.0.1:1/hook'])]);
        }

        return $dsn;
    }

    private function sidecar(): string
    {
        return $this->dir.'/buffer/agent-buffer.sqlite.alert-channels.json';
    }

    private function waitForExit(int $pid): int
    {
        $deadline = microtime(true) + 10.0;
        do {
            $reaped = pcntl_waitpid($pid, $status, WNOHANG);
            if ($reaped !== 0) {
                return $reaped;
            }
            usleep(20_000);
        } while (microtime(true) < $deadline);

        return 0;
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
