<?php

namespace NightOwl\Tests\System;

use NightOwl\Simulator\NightwatchSimulator;
use NightOwl\Tests\Integration\MigrationRunner;
use NightOwl\Tests\System\Concerns\ReadsRawFamily;
use NightOwl\Tests\System\Concerns\SystemEnvironment;
use PDO;
use PHPUnit\Framework\AssertionFailedError;
use PHPUnit\Framework\TestCase;

/**
 * Chaos tests: agent under real PostgreSQL failure and recovery.
 *
 * Two failure shapes, deliberately kept in one class so they share one booted
 * agent — and deliberately gated differently, because they need different things
 * and only one of them is disruptive.
 *
 * 1. OUTAGE (`test_agent_survives_pg_outage_and_drains_on_recovery`) — stops and
 *    starts the container. Needs Docker, so it stays opt-in behind
 *    NIGHTOWL_RUN_CHAOS=1. Covers:
 *      - back-pressure activates and WAL plateaus when pg is gone
 *      - drain catches up cleanly on recovery
 *      - TRUNCATE checkpoint actually fires (instead of getting starved
 *        by drain-worker contention) and reclaims WAL disk space
 *      - SQLite integrity_check passes through the whole sequence
 *
 * 2. BACKEND KILL (`test_drain_recovers_when_its_backend_dies_mid_transaction`) —
 *    terminates the drain's own backends WHILE THEY HOLD AN OPEN TRANSACTION.
 *    Pure SQL, no Docker, so it runs wherever PostgreSQL is available, including
 *    CI. Ungated on purpose: this is the case that shipped a permanent wedge.
 *
 * Why the outage test was NOT enough. It was written for exactly the failure the
 * wedge caused, and it passed against the wedged build — verified by reverting
 * RecordWriter and re-running. `docker stop` shuts Postgres down cleanly, so the
 * drain mostly sees connection-refused on its NEXT connect, which the ordinary
 * reconnect path has always handled. The wedge needs the backend to die while a
 * transaction is open on it (disk-full, OOM kill, pg_terminate_backend): PDO then
 * leaves inTransaction() answering true forever, and the resulting failure is a
 * RuntimeException from DictionaryCache::warm — not a connection error — so the
 * drain never reconnects. Test 2 reproduces that directly, and asserts it caught
 * a live transaction rather than trusting timing.
 *
 * Requirements:
 *   - PostgreSQL (NIGHTOWL_TEST_DB_*); both tests skip without it
 *   - pcntl + posix extensions
 *   - Ports 2413 / 2414 available
 *   - Test 1 additionally: Docker, a running `nightowl-test-pg`, NIGHTOWL_RUN_CHAOS=1
 *
 * Run:
 *   NIGHTOWL_TEST_DB_PORT=5433 vendor/bin/phpunit --filter=AgentPgOutageSystemTest
 *   NIGHTOWL_RUN_CHAOS=1 NIGHTOWL_TEST_DB_PORT=5433 \
 *     vendor/bin/phpunit --filter=AgentPgOutageSystemTest   # adds the outage leg
 */
class AgentPgOutageSystemTest extends TestCase
{
    use ReadsRawFamily;

    private const TOKEN = 'chaos-test-token-2025';

    private const AGENT_HOST = '127.0.0.1';

    private const AGENT_PORT = 2413;

    /** Ceiling only — awaitAgentPort ends the wait the moment the harness dies. */
    private const STARTUP_TIMEOUT = 60;

    private const PG_RECOVERY_TIMEOUT = 30;

    private const DRAIN_CATCHUP_TIMEOUT = 30;

    /** ReadsRawFamily::waitForDrain's default deadline. */
    private const DRAIN_TIMEOUT = 15;

    // Small enough that back-pressure trips quickly in test time.
    private const MAX_PENDING_ROWS = 500;

    // Cadence + threshold tuned so we exercise the TRUNCATE path on
    // test-sized WAL volumes (default 100MB threshold + 60s cadence
    // would never trigger within a chaos-test window).
    private const CHECKPOINT_INTERVAL_SECONDS = 3;

    private const CHECKPOINT_TRUNCATE_BYTES = 256 * 1024;

    // Long enough to land kills across several drain batches; the loop asserts it
    // actually caught transactions rather than trusting this number.
    private const KILL_WINDOW_SECONDS = 8;

    /**
     * How long ingest may keep refusing after the drain has caught up.
     *
     * Three back-pressure monitor ticks (5s each). Generous on purpose: the point
     * of the wait is to prove the door reopens, and one tick of slack would make
     * the test flaky again on a loaded runner. A drain that is actually wedged
     * never gets here — waitForDrainCatchup fails first.
     */
    private const RECOVERY_ACCEPT_TIMEOUT = 15.0;

    private static string $containerName;

    /**
     * Did THIS class stop the container? Only the outage leg ever does, and only
     * that leg may restart it.
     *
     * The teardown used to restart the container whenever Docker was present and
     * the container was not running — which on CI means every run: the runner has
     * a docker CLI, Postgres arrives as a service container, and nothing named
     * `nightowl-test-pg` has ever existed there. `docker start` on a container
     * that was never created is not a recoverable "it's already up", it is
     * `No such container`, and the class errored out at teardown on both storage
     * legs. "Not running" cannot stand in for "we stopped it".
     */
    private static bool $containerStopped = false;

    private static string $dbHost;

    private static int $dbPort;

    private static string $dbDatabase;

    private static string $dbUsername;

    private static string $dbPassword;

    private static ?PDO $pdo = null;

    /** @var resource|null */
    private static $agentProcess = null;

    /** @var resource[] */
    private static array $agentPipes = [];

    /** Everything the agent has written to stdout, accumulated by pumpAgentOutput(). */
    private static string $agentOutput = '';

    private static string $sqlitePath = '';

    private NightwatchSimulator $sim;

    public static function setUpBeforeClass(): void
    {
        if (! function_exists('pcntl_fork') || ! function_exists('posix_kill')) {
            static::markTestSkipped('pcntl + posix required.');
        }

        self::$containerName = getenv('NIGHTOWL_CHAOS_DOCKER_CONTAINER') ?: 'nightowl-test-pg';

        self::$dbHost = getenv('NIGHTOWL_TEST_DB_HOST') ?: '127.0.0.1';
        self::$dbPort = (int) (getenv('NIGHTOWL_TEST_DB_PORT') ?: 5432);
        self::$dbDatabase = getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test';
        self::$dbUsername = getenv('NIGHTOWL_TEST_DB_USERNAME') ?: 'nightowl_test';
        self::$dbPassword = getenv('NIGHTOWL_TEST_DB_PASSWORD') ?: 'test123';

        // PostgreSQL is the floor for BOTH tests now that the backend-kill leg no
        // longer needs Docker, so an absent database skips rather than fataling.
        try {
            self::$pdo = self::connectPg();
        } catch (\Throwable $e) {
            SystemEnvironment::postgresUnavailable($e);
        }

        // Clean slate, built HERE rather than left for the harness subprocess.
        // Dropping the tables and letting the subprocess re-migrate puts a
        // 69-migration schema build inside the window the test spends waiting
        // for a port to open — and every later System class inherits whatever
        // state that leaves. rebuild() does the same drop-and-replay in-process,
        // where nothing is racing a stopwatch, and leaves MigrationRunner's
        // warm-DB probe satisfied so each harness binds immediately.
        MigrationRunner::rebuild(
            self::$dbHost,
            self::$dbPort,
            self::$dbDatabase,
            self::$dbUsername,
            self::$dbPassword,
        );

        self::startAgent();
    }

    /**
     * Gate for the container stop/start leg only. It is slow, needs Docker, and
     * leaves local pg state disrupted if it dies midway — so it stays opt-in even
     * though the rest of the class no longer is.
     */
    private function requireDockerOutageSupport(): void
    {
        if (getenv('NIGHTOWL_RUN_CHAOS') !== '1') {
            $this->markTestSkipped('NIGHTOWL_RUN_CHAOS=1 to enable the container outage leg.');
        }

        if (! self::dockerAvailable()) {
            $this->markTestSkipped('docker CLI not available.');
        }

        if (! self::containerRunning(self::$containerName)) {
            $this->markTestSkipped(sprintf('container "%s" not running.', self::$containerName));
        }
    }

    protected function setUp(): void
    {
        if (self::$pdo === null || self::$agentProcess === null) {
            SystemEnvironment::agentUnavailable('agent or pg unavailable.');
        }
        $this->sim = new NightwatchSimulator(self::TOKEN, self::AGENT_HOST, self::AGENT_PORT, timeout: 3.0);
        self::truncateAllTables();
    }

    public static function tearDownAfterClass(): void
    {
        // Make sure pg is back up no matter what the test left behind — but only
        // if this class is what took it down. See $containerStopped: a container
        // that is "not running" because it does not exist is not ours to start.
        //
        // Wrapped in try/finally because dockerExec throws, and that exception used
        // to skip stopAgent() entirely — the harness subprocess outlived the class
        // holding port 2413, which is how one teardown error turns into a hung run.
        try {
            if (self::$containerStopped
                && isset(self::$containerName)
                && self::dockerAvailable()
                && self::containerRunning(self::$containerName) === false) {
                self::dockerExec('start', self::$containerName);
                self::waitForPg(self::PG_RECOVERY_TIMEOUT);
            }
        } finally {
            self::stopAgent();
            self::$pdo = null;
        }
    }

    // ─── The chaos run ─────────────────────────────────────────

    public function test_agent_survives_pg_outage_and_drains_on_recovery(): void
    {
        $this->requireDockerOutageSupport();

        // 1. Baseline: pg up, a few rows make it through normally.
        $traceA = self::uuid();
        $response = $this->sim->send([
            $this->sim->makeRequest(['trace_id' => $traceA, 'method' => 'GET', 'status_code' => 200]),
        ]);
        $this->assertSame('2:OK', $response, 'baseline ingest should succeed before outage');
        $this->waitForDrain('nightowl_requests', self::traceEq('nightowl_requests', $traceA), 1);

        // 2. Stop pg — graceful shutdown so libpq sees a clean refusal
        // on the drain worker's next attempt (paused containers hang
        // indefinitely, which would mask real failure behavior).
        self::dockerExec('stop', self::$containerName);
        $this->assertFalse(self::containerRunning(self::$containerName), 'pg container should be stopped');

        // 3. Burst phase: fill the buffer past MAX_PENDING_ROWS while pg is
        // down. Back-pressure is gated on a 5s periodic monitor (see
        // AsyncServer::BACK_PRESSURE_CHECK_INTERVAL) — the inline guard at
        // accept-time only flips after the monitor sets $backPressure=true.
        // So we burst here without expecting per-payload 5:ERROR, then sleep
        // through at least one monitor tick before probing.
        $burstSize = (int) (self::MAX_PENDING_ROWS * 1.5);
        $okCount = 0;
        $earlyErr = 0;
        for ($i = 0; $i < $burstSize; $i++) {
            $resp = $this->sim->send([
                $this->sim->makeRequest([
                    'trace_id' => self::uuid(),
                    'method' => 'POST',
                    'status_code' => 500,
                ]),
            ]);
            if ($resp === '2:OK') {
                $okCount++;
            } elseif ($resp === '5:ERROR') {
                $earlyErr++;
            }
        }

        // Give the back-pressure monitor (5s cadence) a clean window to tick.
        // Pending rows in SQLite > MAX_PENDING_ROWS at this point, so the next
        // tick must flip $backPressure=true.
        sleep(7);

        // Probe phase: now expect 5:ERROR on subsequent sends.
        $probeOk = 0;
        $probeErr = 0;
        for ($i = 0; $i < 30; $i++) {
            $resp = $this->sim->send([
                $this->sim->makeRequest([
                    'trace_id' => self::uuid(),
                    'method' => 'POST',
                    'status_code' => 500,
                ]),
            ]);
            if ($resp === '2:OK') {
                $probeOk++;
            } elseif ($resp === '5:ERROR') {
                $probeErr++;
            }
        }

        $this->assertGreaterThan(
            0,
            $okCount,
            'expected at least some payloads to be buffered before back-pressure kicked in'
        );
        $this->assertGreaterThan(
            0,
            $probeErr,
            sprintf(
                'expected back-pressure 5:ERROR on probe after monitor tick (burst ok=%d earlyErr=%d, probe ok=%d err=%d)',
                $okCount,
                $earlyErr,
                $probeOk,
                $probeErr,
            )
        );

        // 4. Bring pg back. Wait for it to actually accept connections.
        self::dockerExec('start', self::$containerName);
        self::waitForPg(self::PG_RECOVERY_TIMEOUT);

        // 5. Drain should catch up. Pending rows visible via the
        // drain-metrics file once the catch-up settles.
        $this->waitForDrainCatchup(self::DRAIN_CATCHUP_TIMEOUT);

        // 6. Verify ingest is healthy again — back-pressure should lift.
        $traceB = self::uuid();
        $this->sendUntilAccepted($traceB, self::RECOVERY_ACCEPT_TIMEOUT);
        $this->waitForDrain('nightowl_requests', self::traceEq('nightowl_requests', $traceB), 1);

        // 7. Verify the checkpoint path actually ran. With CHECKPOINT_INTERVAL_SECONDS=3
        // + CHECKPOINT_TRUNCATE_BYTES=256KB, TRUNCATE should have fired multiple times
        // during the outage and the catch-up phase.
        $metrics = $this->readDrainMetrics();
        $this->assertGreaterThan(
            0,
            $metrics['truncate_attempts'] ?? 0,
            'TRUNCATE checkpoint should have been attempted during the outage / catch-up — got: '.json_encode($metrics)
        );
        $this->assertGreaterThan(
            0,
            $metrics['truncate_successes'] ?? 0,
            sprintf(
                'expected at least one successful TRUNCATE (attempts=%d, failures=%d) — checkpoint may be getting starved by drain contention',
                $metrics['truncate_attempts'] ?? 0,
                $metrics['truncate_failures'] ?? 0,
            )
        );

        // Failures > successes would suggest the commenter's concern was right.
        // Don't hard-fail on a non-zero failure count (TRUNCATE retrying is
        // expected under heavy contention), but surface the ratio for review.
        $failures = $metrics['truncate_failures'] ?? 0;
        $successes = $metrics['truncate_successes'] ?? 0;
        $this->assertLessThanOrEqual(
            $successes,
            $failures,
            sprintf(
                'TRUNCATE failures (%d) exceeded successes (%d) — checkpoint is getting starved under contention',
                $failures,
                $successes,
            )
        );

        // 8. Final WAL integrity check on the buffer file the agent is using.
        $integrity = $this->probeIntegrityCheck();
        $this->assertSame('ok', $integrity, "PRAGMA integrity_check returned: {$integrity}");
    }

    /**
     * The wedge. Kill the drain's backends WHILE they hold an open transaction and
     * the drain must still recover by itself.
     *
     * PDO's behaviour here is the whole problem, and it was measured rather than
     * assumed: once the backend is gone, rollBack() throws AND inTransaction()
     * keeps answering true, so every later beginTransaction() on that handle raises
     * "There is already an active transaction". That surfaced as a RuntimeException
     * out of DictionaryCache::warm — not a connection error — so the drain's
     * reconnect-on-connection-error path never fired and the wedge was permanent:
     * measured 30 CPU-seconds and 1369 log lines over 90s, with rows still stuck in
     * the buffer long after Postgres was healthy.
     *
     * Unlike the outage leg this needs no Docker — pg_terminate_backend is plain
     * SQL — which is exactly why it can be the one that runs in CI.
     */
    public function test_drain_recovers_when_its_backend_dies_mid_transaction(): void
    {
        // 1. Warm the drain first. This is load-bearing, not tidiness: RecordWriter
        // probes for the v2 tables once and caches only a SUCCESSFUL probe, so a
        // writer whose very first connection is already dead fails open to v1 and
        // never reaches the dictionary warm where the wedge assertion lives. Kill a
        // cold drain and the test passes against the broken build.
        $warm = self::uuid();
        $this->assertSame('2:OK', $this->sim->send([
            $this->sim->makeRequest(['trace_id' => $warm, 'method' => 'GET', 'status_code' => 200]),
        ]));
        $this->waitForDrain('nightowl_requests', self::traceEq('nightowl_requests', $warm), 1);

        // 2. Sustained load while repeatedly terminating any backend that is inside
        // a transaction. Timing-dependent by nature, so the kill count is asserted
        // below — a run that never caught one proves nothing and must not pass.
        $accepted = 0;
        $kills = 0;
        $deadline = microtime(true) + self::KILL_WINDOW_SECONDS;

        while (microtime(true) < $deadline) {
            for ($i = 0; $i < 25; $i++) {
                $resp = $this->sim->send([
                    $this->sim->makeRequest([
                        'trace_id' => self::uuid(),
                        'method' => 'POST',
                        'status_code' => 200,
                    ]),
                ]);
                if ($resp === '2:OK') {
                    $accepted++;
                }
            }

            $kills += $this->terminateBackendsInTransaction();
            $this->pumpAgentOutput();
            usleep(50_000);
        }

        $this->assertGreaterThan(
            0,
            $kills,
            'INCONCLUSIVE: never caught a drain backend inside a transaction, so the wedge was never provoked'
        );
        $this->assertGreaterThan(0, $accepted, 'expected the agent to keep accepting ingest through the kills');

        // 3. Stop killing. Postgres has been healthy the whole time — nothing but
        // the agent's own recovery can move these rows now.
        //
        // A wedged drain fails here first, as a bare "did not catch up", which reads
        // like a slow machine. Translate it: the buffer standing still while Postgres
        // is healthy IS the wedge, and the log says so.
        try {
            $this->waitForDrainCatchup(self::DRAIN_CATCHUP_TIMEOUT);
        } catch (AssertionFailedError $e) {
            if (str_contains($this->pumpAgentOutput(), 'must run outside the batch transaction')) {
                $this->fail(
                    "drain wedged on a stranded transaction after {$kills} mid-transaction kills "
                    ."and never recovered, with Postgres healthy throughout — {$e->getMessage()}"
                );
            }

            throw $e;
        }

        // 4. The wedge's signature must be absent. Checked after catch-up so a
        // recovery that only LOOKED clean (e.g. one worker wedged while another
        // carried the backlog) still fails here.
        $output = $this->pumpAgentOutput();
        $this->assertStringNotContainsString(
            'must run outside the batch transaction',
            $output,
            "drain hit the stranded-transaction wedge after {$kills} mid-transaction kills"
        );

        // 5. And the pipeline is genuinely alive again, not merely idle.
        //
        // Retried rather than asserted outright, because catch-up and back-pressure
        // run off different clocks: waitForDrainCatchup returns the instant the
        // buffer reads zero, while $backPressure is only re-evaluated by a 5s
        // periodic monitor (AsyncServer::BACK_PRESSURE_CHECK_INTERVAL). The kill
        // window does latch it — the agent logs "Back-pressure ON" during this
        // test — which leaves a gap of up to one tick where the buffer is empty
        // and the door is still shut. A single send into that gap is what
        // returned 5:ERROR and failed both CI legs; the same one-sample assertion
        // would equally fail on a transient SQLite-busy from this class's very
        // aggressive TRUNCATE checkpoint cadence. Retrying covers both.
        $after = self::uuid();
        $this->sendUntilAccepted($after, self::RECOVERY_ACCEPT_TIMEOUT);
        $this->waitForDrain('nightowl_requests', self::traceEq('nightowl_requests', $after), 1);

        // Rows accepted before the kills started are already in; the assertion that
        // matters is that the backlog moved rather than sitting in SQLite forever.
        $this->assertGreaterThan(
            1,
            self::rowCount(self::rawTable('nightowl_requests')),
            'the buffered backlog never reached Postgres'
        );
    }

    // ─── Helpers ───────────────────────────────────────────────

    /**
     * Terminate every backend on this database that currently holds an open
     * transaction, and report how many were actually hit.
     *
     * `xact_start IS NOT NULL` is the whole point: a backend killed between
     * transactions just yields a clean reconnect, which was never broken. Our own
     * connection is excluded, and the filter is scoped to this database, so the
     * only backends left to match are the agent's drain workers.
     */
    private function terminateBackendsInTransaction(): int
    {
        $stmt = self::$pdo->prepare(
            'SELECT pg_terminate_backend(pid) AS killed
               FROM pg_stat_activity
              WHERE datname = :db
                AND pid <> pg_backend_pid()
                AND xact_start IS NOT NULL'
        );
        $stmt->execute(['db' => self::$dbDatabase]);

        return count(array_filter($stmt->fetchAll(PDO::FETCH_COLUMN)));
    }

    /**
     * Send one request until the agent accepts it, and say why if it never does.
     *
     * Back-pressure is a latch, not a per-payload verdict: it flips only on the
     * 5s monitor tick, so "the buffer is empty" and "ingest is open" become true
     * at different moments. Every post-recovery liveness check therefore has to
     * wait out a tick rather than sample once.
     */
    private function sendUntilAccepted(string $trace, float $timeout): void
    {
        $deadline = microtime(true) + $timeout;
        $last = null;

        while (microtime(true) < $deadline) {
            $last = $this->sim->send([
                $this->sim->makeRequest(['trace_id' => $trace, 'method' => 'GET', 'status_code' => 200]),
            ]);

            if ($last === '2:OK') {
                return;
            }

            usleep(500_000);
        }

        $this->fail(sprintf(
            'agent still refusing ingest %.0fs after the drain caught up (last response: %s). '
            .'That is three back-pressure ticks with an empty buffer — read the log below for '
            .'"Back-pressure ON" (never lifted) or "SQLite buffer error" (the append itself failed). '
            .'Agent output: %s',
            $timeout,
            var_export($last, true),
            $this->pumpAgentOutput(),
        ));
    }

    /**
     * Read whatever the agent has written to stdout so far, accumulating it.
     *
     * Must be called periodically during a noisy phase, not just at the end: the
     * pipe holds ~64KB and the agent BLOCKS once it fills, which would look like a
     * drain stall caused by the test harness itself.
     */
    private function pumpAgentOutput(): string
    {
        if (isset(self::$agentPipes[1]) && is_resource(self::$agentPipes[1])) {
            $chunk = stream_get_contents(self::$agentPipes[1]);
            if (is_string($chunk) && $chunk !== '') {
                self::$agentOutput .= $chunk;
            }
        }

        return self::$agentOutput;
    }

    private static function dockerAvailable(): bool
    {
        exec('docker version --format "{{.Server.Version}}" 2>/dev/null', $out, $rc);

        return $rc === 0;
    }

    private static function containerRunning(string $name): bool
    {
        $cmd = sprintf(
            'docker ps --filter %s --format "{{.Names}}" 2>/dev/null',
            escapeshellarg('name=^'.preg_quote($name, '/').'$'),
        );
        exec($cmd, $out, $rc);

        return $rc === 0 && in_array($name, $out, true);
    }

    private static function dockerExec(string $verb, string $name): void
    {
        $cmd = sprintf('docker %s %s 2>&1', escapeshellarg($verb), escapeshellarg($name));
        exec($cmd, $out, $rc);
        if ($rc !== 0) {
            throw new \RuntimeException("docker {$verb} {$name} failed: ".implode("\n", $out));
        }

        // Bookkeeping lives here rather than at the call sites so the teardown's
        // "did we stop it?" answer cannot drift from what actually ran.
        if ($verb === 'stop') {
            self::$containerStopped = true;
        } elseif ($verb === 'start') {
            self::$containerStopped = false;
        }
    }

    private static function waitForPg(float $timeout): void
    {
        $deadline = microtime(true) + $timeout;
        while (microtime(true) < $deadline) {
            try {
                self::$pdo = self::connectPg();

                return;
            } catch (\Throwable) {
                usleep(500_000);
            }
        }
        throw new \RuntimeException('pg did not come back within '.$timeout.'s');
    }

    private static function connectPg(): PDO
    {
        $dsn = sprintf('pgsql:host=%s;port=%d;dbname=%s', self::$dbHost, self::$dbPort, self::$dbDatabase);
        $pdo = new PDO($dsn, self::$dbUsername, self::$dbPassword);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        return $pdo;
    }

    private static function startAgent(): void
    {
        self::$agentOutput = '';
        self::$sqlitePath = sys_get_temp_dir().'/nightowl-chaos-'.getmypid().'.sqlite';

        // Wipe any leftover from a prior failed run before launching.
        foreach ([self::$sqlitePath, self::$sqlitePath.'-wal', self::$sqlitePath.'-shm', self::$sqlitePath.'.drain-metrics.json'] as $f) {
            if (file_exists($f)) {
                @unlink($f);
            }
        }

        $harness = realpath(__DIR__.'/../Simulator/agent-harness-async.php');
        if (! $harness) {
            SystemEnvironment::agentUnavailable('agent-harness-async.php not found.');
        }

        $cmd = sprintf(
            'exec php %s --token=%s --host=%s --port=%d --db-host=%s --db-port=%d --db-name=%s --db-user=%s --db-pass=%s --max-pending-rows=%d --drain-interval=50 --checkpoint-interval=%d --checkpoint-truncate-bytes=%d --sqlite-path=%s 2>&1',
            escapeshellarg($harness),
            escapeshellarg(self::TOKEN),
            escapeshellarg(self::AGENT_HOST),
            self::AGENT_PORT,
            escapeshellarg(self::$dbHost),
            self::$dbPort,
            escapeshellarg(self::$dbDatabase),
            escapeshellarg(self::$dbUsername),
            escapeshellarg(self::$dbPassword),
            self::MAX_PENDING_ROWS,
            self::CHECKPOINT_INTERVAL_SECONDS,
            self::CHECKPOINT_TRUNCATE_BYTES,
            escapeshellarg(self::$sqlitePath),
        );

        $descriptors = [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']];
        self::$agentProcess = proc_open($cmd, $descriptors, self::$agentPipes);

        if (! is_resource(self::$agentProcess)) {
            SystemEnvironment::agentUnavailable('failed to start agent.');
        }

        stream_set_blocking(self::$agentPipes[1], false);

        $reason = SystemEnvironment::awaitAgentPort(
            self::$agentProcess,
            self::$agentPipes[1],
            self::AGENT_HOST,
            self::AGENT_PORT,
            self::STARTUP_TIMEOUT,
        );

        if ($reason !== null) {
            self::stopAgent();
            SystemEnvironment::agentUnavailable($reason);
        }
    }

    private static function stopAgent(): void
    {
        if (self::$agentProcess === null) {
            return;
        }

        $status = proc_get_status(self::$agentProcess);
        if ($status['running']) {
            posix_kill($status['pid'], SIGTERM);
            $deadline = microtime(true) + 10;
            while (microtime(true) < $deadline) {
                $check = proc_get_status(self::$agentProcess);
                if (! $check['running']) {
                    break;
                }
                usleep(100_000);
            }
            $check = proc_get_status(self::$agentProcess);
            if ($check['running']) {
                posix_kill($status['pid'], SIGKILL);
                usleep(200_000);
            }
        }

        foreach (self::$agentPipes as $p) {
            if (is_resource($p)) {
                fclose($p);
            }
        }
        proc_close(self::$agentProcess);
        self::$agentProcess = null;
        self::$agentPipes = [];

        foreach ([self::$sqlitePath, self::$sqlitePath.'-wal', self::$sqlitePath.'-shm', self::$sqlitePath.'.drain-metrics.json', self::$sqlitePath.'.drain-metrics.json.tmp'] as $f) {
            if (file_exists($f)) {
                @unlink($f);
            }
        }
    }

    protected static function pdo(): PDO
    {
        return self::$pdo;
    }

    private function waitForDrainCatchup(float $timeout): void
    {
        // Catch-up is "no more pending rows" — easiest signal is that
        // the buffer count goes to zero. Read directly from the sqlite
        // file rather than racing the drain-metrics IPC.
        $deadline = microtime(true) + $timeout;
        $lastPending = PHP_INT_MAX;

        while (microtime(true) < $deadline) {
            $pending = $this->bufferPending();
            if ($pending === 0) {
                return;
            }
            $lastPending = $pending;
            usleep(500_000);
        }

        $this->fail("drain did not catch up within {$timeout}s — {$lastPending} rows still pending");
    }

    private function bufferPending(): int
    {
        try {
            $pdo = new PDO('sqlite:'.self::$sqlitePath);
            $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            $pdo->exec('PRAGMA busy_timeout=2000');

            return (int) $pdo->query('SELECT COUNT(*) FROM buffer WHERE synced = 0')->fetchColumn();
        } catch (\Throwable) {
            return PHP_INT_MAX;
        }
    }

    /** @return array<string, int|float> */
    private function readDrainMetrics(): array
    {
        $path = self::$sqlitePath.'.drain-metrics.json';

        // The drain worker writes metrics every 5s. Give it a beat to flush
        // the post-recovery state before reading.
        $deadline = microtime(true) + 8;
        while (microtime(true) < $deadline) {
            if (file_exists($path)) {
                $raw = @file_get_contents($path);
                if ($raw !== false) {
                    $decoded = json_decode($raw, true);
                    if (is_array($decoded) && isset($decoded['updated_at'])) {
                        return $decoded;
                    }
                }
            }
            usleep(500_000);
        }

        $this->fail("drain metrics file never appeared at: {$path}");
    }

    private function probeIntegrityCheck(): string
    {
        $pdo = new PDO('sqlite:'.self::$sqlitePath);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        return (string) $pdo->query('PRAGMA integrity_check')->fetchColumn();
    }
}
