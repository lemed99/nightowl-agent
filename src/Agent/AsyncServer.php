<?php

namespace NightOwl\Agent;

use NightOwl\Support\AgentInstanceId;
use React\Datagram\Socket as DatagramSocket;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\EventLoop\TimerInterface;
use React\Socket\ConnectionInterface;
use React\Socket\TcpServer;
use RuntimeException;

final class AsyncServer
{
    private const CONNECTION_TIMEOUT = 10.0;
    private const MAX_PAYLOAD_BYTES = 10 * 1024 * 1024; // 10 MB per connection
    private const CHILD_SHUTDOWN_TIMEOUT = 10; // seconds
    private const RESTART_COOLDOWN = 2.0; // seconds — suppress rapid restart loops
    private const BACK_PRESSURE_CHECK_INTERVAL = 5.0; // seconds

    private LoopInterface $loop;
    private ?TcpServer $server = null;
    private ?DatagramSocket $udpSocket = null;
    private ?HealthServer $healthServer = null;
    private ?SqliteBuffer $buffer = null;
    private ?string $expectedTokenHash;
    /** @var int[] Drain worker PIDs indexed by worker ID */
    private array $drainChildPids = [];
    /** In-flight health-alert dispatch child, at most one — see dispatchHealthAlerts(). */
    private ?int $alertChildPid = null;
    private bool $shuttingDown = false;
    /** @var float[] Last spawn time per worker ID */
    private array $lastChildSpawns = [];

    /**
     * workerId => hrtime(true) at fork. Monotonic, unlike lastChildSpawns' wall clock:
     * it is compared against the child's own monotonic heartbeat stamps, so an NTP step
     * can neither manufacture nor mask a wedge. Also the liveness basis for a child that
     * wedges before ever writing a heartbeat.
     */
    private array $childForkedAtMonoNs = [];

    // Back-pressure state
    private bool $backPressure = false;
    private int $totalBufferBytes = 0;

    // Telemetry
    private float $startTime = 0;
    private MetricsCollector $metrics;
    private ?HealthAlertNotifier $healthAlertNotifier = null;

    /** @var array<int, TimerInterface> */
    private array $connectionTimers = [];

    public function __construct(
        private PayloadParser $parser,
        private string $sqlitePath,
        private DrainWorker $drainWorker,
        ?string $token = null,
        private int $maxPendingRows = 100_000,
        private int $maxBufferMemory = 256 * 1024 * 1024, // 256 MB
        private bool $enableUdp = false,
        private int $udpPort = 2408,
        private bool $healthEnabled = true,
        private int $healthPort = 2409,
        private string $apiUrl = '',
        private string $healthReportToken = '',
        private bool $healthReportEnabled = true,
        private int $healthReportInterval = 30,
        private array $healthReportIntervals = [],
        private string $tenantId = '',
        private int $drainWorkerCount = 1,
        // Optional respawn strategy for drain workers. When set, the forked child
        // calls this instead of running the drain loop in-process — it is expected
        // to pcntl_exec() a fresh PHP interpreter and therefore never return.
        // Execing a clean process keeps the background drain isolated from the
        // ingest server: the child doesn't inherit the parent agent's long-lived
        // ReactPHP state (event-loop epoll/kqueue FDs, signal handlers, sockets
        // and handles opened for health reporting), which is sound hygiene for a
        // long-running forking process. When null (standalone harness, tests) the
        // child runs the drain loop in-process — the original fork-only behavior.
        // Signature: fn (int $workerId, int $totalWorkers, string $sqlitePath): void
        private ?\Closure $drainSpawner = null,
        // DRAIN_WEDGED threshold (seconds); 0 disables. Appended LAST on purpose:
        // the service provider passes 16 positional args and only then switches to
        // named, and several harnesses pass drainSpawner positionally, so inserting
        // earlier would silently shift arguments with no type error.
        float $drainWedgeSeconds = 180.0,
        // Update-drift watcher (warn-only; see VersionDriftWatcher). null =
        // feature off — the default keeps every existing harness/test
        // byte-identical in behavior. Appended LAST (see above).
        private ?VersionDriftWatcher $driftWatcher = null,
        // Path of the boot-backfill pending marker, stat'd each diagnosis tick so
        // an unfinished rollup reconciliation shows up in the health payload
        // instead of only in the log. Injected rather than resolved here: this
        // class touches no Laravel helpers, so the standalone harness works
        // without an app. null = feature off. Appended LAST (see above).
        private ?string $backfillMarkerPath = null,
    ) {
        $this->expectedTokenHash = $token !== null
            ? substr(hash('xxh128', $token), 0, 7)
            : null;
        $this->loop = Loop::get();
        $this->metrics = new MetricsCollector($maxPendingRows, $maxBufferMemory);
        $this->metrics->setDrainWedgeSeconds($drainWedgeSeconds);
    }

    /**
     * mtime of the boot-backfill pending marker, or 0.0 when nothing is owed
     * (marker absent, or the feature not wired). The AGE is what the diagnosis
     * grades on — a pass that is legitimately slow reads as in-progress, one
     * nobody is running reads as neglected — so the stamp travels, not a bool.
     */
    private function backfillPendingSince(): float
    {
        if ($this->backfillMarkerPath === null) {
            return 0.0;
        }

        clearstatcache(true, $this->backfillMarkerPath);
        $mtime = @filemtime($this->backfillMarkerPath);

        // false means gone (the usual case) or present-but-unstattable. Only the
        // first is "nothing owed"; the second still owes a backfill, so fall back
        // to "now" and report it as in-progress rather than let it vanish.
        if ($mtime === false) {
            return @file_exists($this->backfillMarkerPath) ? microtime(true) : 0.0;
        }

        return (float) $mtime;
    }

    public function listen(string $host, int $port): void
    {
        $this->startTime = microtime(true);

        // SO_REUSEPORT lets multiple instances bind to the same port.
        // The kernel distributes incoming connections across all listeners.
        // This is how you scale beyond one core: run N agent instances on
        // the same port, each with its own SQLite buffer file.
        try {
            $this->server = new TcpServer("{$host}:{$port}", $this->loop, [
                'so_reuseport' => true,
            ]);
        } catch (RuntimeException $e) {
            $message = strtolower($e->getMessage());
            if (str_contains($message, 'address already in use') || str_contains($message, 'eaddrinuse')) {
                $hint = $port === 2407
                    ? "Port 2407 is also Nightwatch's default ingest port — if the Nightwatch agent is running, NightOwl can't share it. "
                    : '';

                throw new PortInUseException(
                    "Could not start the agent: {$host}:{$port} is already in use.\n\n"
                    .$hint
                    ."To run NightOwl alongside Nightwatch, set NIGHTOWL_AGENT_PORT to a free port (e.g. 2410) and enable parallel mode:\n"
                    ."  https://docs.usenightowl.com/agent/running-alongside-nightwatch\n\n"
                    ."Otherwise, stop whatever already holds {$host}:{$port} (often a second nightowl:agent process or a leftover daemon).",
                    (int) $e->getCode(),
                    $e
                );
            }

            throw $e;
        }

        // forkDrainWorker() closes any existing SQLite buffer before fork
        // and re-creates it after — ensuring the child never inherits an open
        // SQLite PDO (sqlite3_close in child corrupts WAL shared memory).
        for ($i = 0; $i < $this->drainWorkerCount; $i++) {
            $this->forkDrainWorker($i);
        }

        // UDP listener AFTER fork — react/datagram creates the raw stream socket
        // synchronously inside createServer(), but the Promise callback that sets
        // $this->udpSocket fires on the next event loop tick. If we fork between
        // socket creation and promise resolution, the child inherits the fd but
        // $this->udpSocket is null — we can't close it in the child branch.
        if ($this->enableUdp) {
            $this->listenUdp($host, $this->udpPort);
        }

        $this->server->on('connection', function (ConnectionInterface $conn) {
            // Reject immediately if back-pressure is active — don't even
            // buffer data, just tell the client to back off.
            if ($this->backPressure) {
                $this->metrics->recordReject();
                $conn->write('5:ERROR');
                $conn->end();
                return;
            }

            $dataBuffer = '';
            $bufferBytes = 0;
            $connId = spl_object_id($conn);

            // Per-connection timeout
            $this->connectionTimers[$connId] = $this->loop->addTimer(
                self::CONNECTION_TIMEOUT,
                function () use ($conn, $connId) {
                    unset($this->connectionTimers[$connId]);
                    $conn->close();
                }
            );

            $conn->on('data', function (string $chunk) use ($conn, &$dataBuffer, &$bufferBytes, $connId) {
                $chunkLen = strlen($chunk);
                $dataBuffer .= $chunk;
                $bufferBytes += $chunkLen;
                $this->totalBufferBytes += $chunkLen;

                // Guard against oversized payloads (per-connection)
                if ($bufferBytes > self::MAX_PAYLOAD_BYTES) {
                    $conn->write('5:ERROR');
                    $conn->end();
                    return;
                }

                // Guard against global memory pressure
                if ($this->totalBufferBytes > $this->maxBufferMemory) {
                    $conn->write('5:ERROR');
                    $conn->end();
                    return;
                }

                if ($this->hasCompletePayload($dataBuffer)) {
                    $this->handlePayload($conn, $dataBuffer);
                    $dataBuffer = '';
                }
            });

            $conn->on('close', function () use (&$dataBuffer, &$bufferBytes, $connId) {
                $this->totalBufferBytes -= $bufferBytes;
                $dataBuffer = '';
                $bufferBytes = 0;
                if (isset($this->connectionTimers[$connId])) {
                    $this->loop->cancelTimer($this->connectionTimers[$connId]);
                    unset($this->connectionTimers[$connId]);
                }
            });
        });

        // 1-second tick — advance ring buffers, measure event loop lag
        $this->loop->addPeriodicTimer(1.0, function () {
            $this->metrics->tick();
        });

        // Back-pressure monitor — check pending row count and process RSS periodically.
        // Pending count (not WAL file size) because SQLite never shrinks the WAL
        // file during operation — filesize() would never decrease after a surge.
        // RSS check because PHP's Zend Memory Manager doesn't return freed memory
        // to the OS — totalBufferBytes can't detect the RSS staircase.
        $this->loop->addPeriodicTimer(self::BACK_PRESSURE_CHECK_INTERVAL, function () {
            $pending = $this->buffer?->pendingCount() ?? 0;
            $rss = memory_get_usage(true);
            $wasActive = $this->backPressure;

            $this->backPressure = $pending > $this->maxPendingRows
                || $rss > $this->maxBufferMemory;

            if ($this->backPressure && ! $wasActive) {
                $rssMb = round($rss / 1024 / 1024);
                $limitMb = round($this->maxBufferMemory / 1024 / 1024);
                error_log("[NightOwl Agent] Back-pressure ON: {$pending} pending rows (limit {$this->maxPendingRows}), RSS {$rssMb}MB (limit {$limitMb}MB).");
            } elseif (! $this->backPressure && $wasActive) {
                error_log("[NightOwl Agent] Back-pressure OFF: {$pending} pending rows. Accepting payloads.");
            }

            // Read drain worker metrics from IPC temp files
            $this->metrics->readDrainMetrics($this->sqlitePath, $this->drainWorkerCount);
        });

        // Health alert notifier — dispatches to Slack/Discord/Webhook/Email
        // when a new diagnosis crosses the debounce threshold.
        $instanceId = AgentInstanceId::current();
        $this->healthAlertNotifier = HealthAlertNotifier::fromConfig($instanceId);

        // 10-second diagnosis — run health checks and update score
        $this->loop->addPeriodicTimer(10.0, function () {
            $pending = $this->buffer?->pendingCount() ?? 0;
            $walSize = $this->buffer?->walSize() ?? 0;
            $rss = memory_get_usage(true);
            // Read liveness immediately before diagnosing, so read -> diagnose is one
            // ordered pass and DRAIN_WEDGED never fires off a snapshot from a prior tick.
            // Live workers only: a reaped worker's stale stamp must not diagnose.
            $this->metrics->readWorkerLiveness(
                $this->sqlitePath,
                $this->drainWorkerCount,
                array_intersect_key($this->childForkedAtMonoNs, $this->drainChildPids)
            );
            // One stat per tick: is the rollup reconciliation an upgrade owes still
            // outstanding, and since when? clearstatcache() because the detached
            // backfill deletes this file from ANOTHER process — PHP's cached stat
            // would otherwise keep the diagnosis alive after it completed.
            $this->metrics->setBackfillPendingSince($this->backfillPendingSince());

            $this->metrics->runDiagnosis($this->backPressure, $pending, $walSize, $rss);

            // Dispatch alerts for newly active or resolved diagnoses. Off the
            // event loop — see dispatchHealthAlerts().
            if ($this->healthAlertNotifier !== null) {
                $this->dispatchHealthAlerts(
                    $this->metrics->getNewlyActiveDiagnoses(),
                    $this->metrics->getNewlyResolvedDiagnoses(),
                );
            }
        });

        // Update-drift watcher: say so in the log when composer update has put
        // a newer agent on disk while this process keeps running the old one.
        // Warn only — restarting is the operator's call (see the class
        // docblock for why the agent does not make it).
        if ($this->driftWatcher !== null) {
            $this->loop->addPeriodicTimer($this->driftWatcher->pollSeconds(), function () {
                if ($this->shuttingDown) {
                    return;
                }

                $warning = $this->driftWatcher->check();
                if ($warning !== null) {
                    error_log("[NightOwl Agent] Update available: {$warning}");
                }
            });
        }

        // Restart drain workers if they die unexpectedly
        $this->loop->addSignal(SIGCHLD, function () {
            // Reap all terminated children
            while (($result = pcntl_waitpid(-1, $status, WNOHANG)) > 0) {
                // Find which worker this PID belongs to
                $deadWorkerId = array_search($result, $this->drainChildPids, true);
                if ($deadWorkerId === false) {
                    continue;
                }

                unset($this->drainChildPids[$deadWorkerId]);

                // Release any rows claimed by the dead worker back to pending
                if ($this->buffer !== null) {
                    $this->buffer->releaseClaimed($deadWorkerId);
                }

                // Don't restart during shutdown — but keep reaping other dead children
                if ($this->shuttingDown) {
                    continue;
                }

                // Restart cooldown — prevents fork bomb when child crashes on startup
                $elapsed = microtime(true) - ($this->lastChildSpawns[$deadWorkerId] ?? 0);
                if ($elapsed < self::RESTART_COOLDOWN) {
                    $delay = self::RESTART_COOLDOWN - $elapsed;
                    error_log("[NightOwl Agent] Drain worker #{$deadWorkerId} died, restarting in {$delay}s...");
                    $this->loop->addTimer($delay, function () use ($deadWorkerId) {
                        if (! $this->shuttingDown && ! isset($this->drainChildPids[$deadWorkerId])) {
                            $this->forkDrainWorker($deadWorkerId);
                        }
                    });
                    continue;
                }

                $exitCode = pcntl_wifexited($status) ? pcntl_wexitstatus($status) : -1;
                error_log("[NightOwl Agent] Drain worker #{$deadWorkerId} exited (code: {$exitCode}), restarting...");
                $this->forkDrainWorker($deadWorkerId);
            }
        });

        // Graceful shutdown
        $this->loop->addSignal(SIGINT, fn () => $this->shutdown());
        $this->loop->addSignal(SIGTERM, fn () => $this->shutdown());

        // Health API server
        if ($this->healthEnabled) {
            $this->healthServer = new HealthServer($this->loop);
            $this->healthServer->listen($host, $this->healthPort, $this);
        }

        // Remote health reporting to api
        if ($this->healthReportEnabled && $this->apiUrl !== '' && $this->healthReportToken !== '') {
            // Build adaptive intervals — use legacy single interval as fallback
            $intervals = $this->healthReportIntervals;
            if (empty($intervals)) {
                $intervals = [
                    'healthy' => $this->healthReportInterval,
                    'degraded' => $this->healthReportInterval,
                    'critical' => $this->healthReportInterval,
                ];
            }

            $reporter = new HealthReporter(
                $this->apiUrl,
                $this->healthReportToken,
                $this->tenantId,
                $intervals,
            );
            $reporter->start($this->loop, $this);
        }

        $this->loop->run();
    }

    /**
     * Dispatch health alerts in a short-lived forked child, never on the loop.
     *
     * Every step of a dispatch round is BLOCKING network I/O: loadChannels()
     * issues a Postgres query, and the sends are raw SMTP and HTTP with their
     * own socket timeouts, the whole round bounded only by
     * HealthAlertNotifier::MAX_DISPATCH_SECONDS (30s). Run inline off the 10s
     * diagnosis timer — as this did — that is up to 30 seconds during which the
     * agent accepts no telemetry.
     *
     * It degrades exactly when it must not. The dispatch fires on diagnosis
     * TRANSITIONS, so a struggling agent flaps and dispatches constantly, and
     * the Postgres it must query to find its channels is the same slow one that
     * is causing the diagnoses. Worse, it is self-reinforcing: the blocking
     * raises event-loop lag, lag raises LOOP_LAG_CRITICAL, and that transition
     * dispatches again. (Yomoney, 2026-07-29 onward: event-loop lag went from a
     * 2ms median on v1.4.0 to ~480ms and never came back, on a box sitting at
     * 4% CPU with 2-5s Postgres latency and diagnoses flapping every 30s.)
     *
     * The child is SIGKILLed rather than exited. PHP's exit() runs destructors,
     * which would call sqlite3_close() on the buffer handle it inherited and
     * corrupt the WAL shared memory (sqlite.org/howtocorrupt.html §2.7 — the
     * same hazard forkDrainWorker() avoids by nulling the buffer first). SIGKILL
     * runs no destructors at all, so the inherited handle is never touched,
     * which is why this does NOT have to churn the parent's buffer connection on
     * every alert. The notifier's own Postgres handle is created lazily and the
     * parent no longer dispatches, so each child opens its own and takes the
     * abrupt close with it.
     *
     * One round at a time: under flapping these would otherwise pile up. A
     * skipped round is not a lost alert — the diagnosis stays active and the
     * next transition re-reports it.
     *
     * @param  array<int, array<string, mixed>>  $active
     * @param  array<int, array<string, mixed>>  $resolved
     */
    private function dispatchHealthAlerts(array $active, array $resolved): void
    {
        if ($active === [] && $resolved === []) {
            return;
        }

        // No fork available (sync driver, non-POSIX host): inline is all there
        // is. Keeps the pre-existing behaviour rather than dropping the alert.
        if (! function_exists('pcntl_fork') || ! function_exists('posix_kill')) {
            $this->dispatchHealthAlertsInline($active, $resolved);

            return;
        }

        if ($this->alertChildPid !== null) {
            // >0 reaped here, -1 already reaped by the SIGCHLD handler; 0 means
            // it is still running and this round is skipped.
            if (pcntl_waitpid($this->alertChildPid, $status, WNOHANG) === 0) {
                error_log('[NightOwl Agent] Health alert dispatch still in flight, skipping this round.');

                return;
            }

            $this->alertChildPid = null;
        }

        $pid = pcntl_fork();

        if ($pid === -1) {
            error_log('[NightOwl Agent] Could not fork for health alert dispatch — sending inline.');
            $this->dispatchHealthAlertsInline($active, $resolved);

            return;
        }

        if ($pid === 0) {
            // === Child ===
            // The SIGKILL is in a finally, not trailing the body: anything that
            // escapes here would reach PHP's fatal handler, which shuts the
            // process down through the destructors this whole approach exists to
            // avoid. The child never runs the loop, so it is not stopped — only
            // the listeners are dropped, so a slow dispatch cannot hold the
            // ingest port open.
            try {
                $this->server?->close();
                $this->udpSocket?->close();
                $this->healthServer?->close();

                $this->dispatchHealthAlertsInline($active, $resolved);
            } catch (\Throwable $e) {
                error_log("[NightOwl Agent] Health alert dispatch child failed: {$e->getMessage()}");
            } finally {
                posix_kill(posix_getpid(), SIGKILL);
            }
        }

        $this->alertChildPid = $pid;
    }

    /**
     * @param  array<int, array<string, mixed>>  $active
     * @param  array<int, array<string, mixed>>  $resolved
     */
    private function dispatchHealthAlertsInline(array $active, array $resolved): void
    {
        try {
            $this->healthAlertNotifier?->dispatch($active);
            $this->healthAlertNotifier?->dispatchRecovered($resolved);
        } catch (\Throwable $e) {
            error_log("[NightOwl Agent] Health alert dispatch failed: {$e->getMessage()}");
        }
    }

    private function forkDrainWorker(int $workerId = 0): void
    {
        // Close parent's SQLite connection BEFORE fork. If the child inherits
        // an open SQLite PDO, the child's eventual exit() triggers sqlite3_close()
        // on the inherited handle — this corrupts the WAL shared memory.
        // See: https://www.sqlite.org/howtocorrupt.html section 2.7
        //
        // The first fork (before buffer creation) has $this->buffer === null,
        // so this is a no-op. Auto-restart forks close and re-create.
        // The event loop is single-threaded, so no TCP data events can
        // interleave between close and re-create.
        $this->buffer = null;

        $pid = pcntl_fork();

        if ($pid === -1) {
            // Re-create buffer before throwing so the parent can continue
            $this->buffer = new SqliteBuffer($this->sqlitePath);
            throw new RuntimeException('Failed to fork drain worker process');
        }

        if ($pid === 0) {
            // === Child process ===
            // Close inherited sockets so neither the exec'd process nor the
            // in-process drain loop holds the parent's listeners open. The event
            // loop's internal FDs (epoll/kqueue) are also inherited but harmless
            // in a child that never runs the loop.
            $this->server?->close();
            $this->udpSocket?->close();
            $this->healthServer?->close();

            // Preferred path: exec a fresh interpreter via the injected spawner so
            // the drain worker doesn't inherit the parent agent's long-lived
            // runtime state (see $drainSpawner docblock). The spawner pcntl_exec()s
            // and never returns; if it returns, exec failed — fall through to in-process.
            if ($this->drainSpawner !== null) {
                ($this->drainSpawner)($workerId, $this->drainWorkerCount, $this->sqlitePath);
                error_log('[NightOwl Agent] drain spawner exec failed ('.(error_get_last()['message'] ?? 'unknown').') — falling back to in-process drain');
            }

            // In-process drain (standalone harness, tests, or exec fallback).
            // Stop the parent's event loop in this process so it doesn't interfere
            // — we won't call run(), but this cleans up fds/signals.
            $this->loop->stop();
            $worker = clone $this->drainWorker;
            $worker->setWorkerConfig($workerId, $this->drainWorkerCount);
            $worker->run();
        }

        // === Parent process ===
        // Re-create SQLite buffer with a fresh connection
        $this->buffer = new SqliteBuffer($this->sqlitePath);

        $this->drainChildPids[$workerId] = $pid;
        $this->lastChildSpawns[$workerId] = microtime(true);
        $this->childForkedAtMonoNs[$workerId] = hrtime(true);
        error_log("[NightOwl Agent] Drain worker #{$workerId} forked (pid: {$pid})");
    }

    private function handlePayload(ConnectionInterface $conn, string $data): void
    {
        $result = $this->parser->parse($data);

        if ($result === null) {
            $conn->write('5:ERROR');
            $conn->end();
            return;
        }

        if ($result['type'] === 'error') {
            error_log("[NightOwl Agent] {$result['error']}");
            $conn->write('5:ERROR');
            $conn->end();
            return;
        }

        if ($result['type'] === 'text' && $result['payload'] === 'PING') {
            $conn->write('2:OK');
            $conn->end();
            return;
        }

        // Validate token
        if ($this->expectedTokenHash !== null) {
            $receivedHash = $result['tokenHash'] ?? null;
            if ($receivedHash !== $this->expectedTokenHash) {
                $this->metrics->recordReject();
                error_log('[NightOwl Agent] Rejected payload: invalid token hash');
                $conn->write('5:ERROR');
                $conn->end();
                return;
            }
        }

        if ($result['type'] === 'json') {
            try {
                $this->buffer->appendRaw($result['rawPayload']);
                $this->metrics->recordIngest();
                $conn->write('2:OK');
            } catch (\Throwable $e) {
                error_log("[NightOwl Agent] SQLite buffer error: {$e->getMessage()}");
                $conn->write('5:ERROR');
            }
        }

        $conn->end();
    }

    private function shutdown(): void
    {
        if ($this->shuttingDown) {
            return;
        }
        $this->shuttingDown = true;

        error_log('[NightOwl Agent] Shutting down...');

        // Stop accepting new connections
        if ($this->server !== null) {
            $this->server->close();
            $this->server = null;
        }

        if ($this->udpSocket !== null) {
            $this->udpSocket->close();
            $this->udpSocket = null;
        }

        if ($this->healthServer !== null) {
            $this->healthServer->close();
            $this->healthServer = null;
        }

        // Signal all drain workers to finish and wait
        foreach ($this->drainChildPids as $workerId => $pid) {
            posix_kill($pid, SIGTERM);
        }

        $deadline = time() + self::CHILD_SHUTDOWN_TIMEOUT;
        while (! empty($this->drainChildPids) && time() < $deadline) {
            foreach ($this->drainChildPids as $workerId => $pid) {
                $result = pcntl_waitpid($pid, $status, WNOHANG);
                if ($result !== 0) {
                    unset($this->drainChildPids[$workerId]);
                }
            }
            if (! empty($this->drainChildPids)) {
                usleep(100_000);
            }
        }

        // Force kill any remaining
        foreach ($this->drainChildPids as $workerId => $pid) {
            error_log("[NightOwl Agent] Drain worker #{$workerId} did not exit in time, sending SIGKILL");
            posix_kill($pid, SIGKILL);
            pcntl_waitpid($pid, $status);
        }
        $this->drainChildPids = [];

        $this->loop->stop();
    }

    private function listenUdp(string $host, int $udpPort): void
    {
        $factory = new \React\Datagram\Factory($this->loop);

        $factory->createServer("{$host}:{$udpPort}")->then(
            function (DatagramSocket $socket) {
                $this->udpSocket = $socket;

                $socket->on('message', function (string $data) {
                    if ($this->backPressure) {
                        return; // Silently drop — UDP has no ACK
                    }

                    $result = $this->parser->parse($data);

                    if ($result === null || $result['type'] !== 'json') {
                        return; // Silently drop malformed/non-JSON
                    }

                    // Validate token
                    if ($this->expectedTokenHash !== null) {
                        $receivedHash = $result['tokenHash'] ?? null;
                        if ($receivedHash !== $this->expectedTokenHash) {
                            return;
                        }
                    }

                    try {
                        $this->buffer->appendRaw($result['rawPayload']);
                        $this->metrics->recordIngest();
                    } catch (\Throwable $e) {
                        error_log("[NightOwl Agent] UDP buffer error: {$e->getMessage()}");
                    }
                });
            },
            function (\Throwable $e) {
                error_log("[NightOwl Agent] Failed to start UDP server: {$e->getMessage()}");
            }
        );
    }

    /**
     * Get current agent status for the health API.
     */
    public function getStatus(): array
    {
        return $this->metrics->getFullStatus(
            $this->startTime,
            $this->backPressure,
            $this->buffer?->pendingCount() ?? 0,
            $this->buffer?->walSize() ?? 0,
            $this->drainChildPids[0] ?? -1,
        );
    }

    private function hasCompletePayload(string $data): bool
    {
        $colonPos = strpos($data, ':');
        if ($colonPos === false) {
            return false;
        }

        $declaredLength = (int) substr($data, 0, $colonPos);
        $expectedTotal = $colonPos + 1 + $declaredLength;

        return strlen($data) >= $expectedTotal;
    }
}
