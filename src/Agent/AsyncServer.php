<?php

namespace NightOwl\Agent;

use NightOwl\Support\AgentInstanceId;
use NightOwl\Support\SqliteFailure;
use React\Datagram\Socket as DatagramSocket;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\EventLoop\StreamSelectLoop;
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

    // A corruption error older than this does not start a buffer recovery: the
    // drain re-stamps its error every loop while the file stays broken, so an
    // old one means the failure has stopped.
    private const BUFFER_CORRUPTION_FRESH_SECONDS = 60;

    // At most one buffer recovery per this many seconds. It stops the drain and
    // reads the whole file, so a check that finds nothing wrong (a corruption
    // error SQLite raised without the file being damaged) must not repeat every
    // tick.
    private const BUFFER_RECOVERY_COOLDOWN_SECONDS = 300;

    // WAL size past which the parent checkpoints. The buffer runs with
    // wal_autocheckpoint=0 (see SqliteBuffer's constructor), so this takes over
    // from SQLite's own checkpoint, which ran inside the parent's commits at
    // 1000 pages: 4 MB at the 4 KB page size. The parent, not a drain worker:
    // a worker can spend minutes inside one PostgreSQL write, and in a
    // benchmark at ~9k payloads/s the WAL reached 424 MB while worker 0 was
    // busy (2026-10-22).
    private const WAL_CHECKPOINT_BYTES = 4 * 1024 * 1024;

    // How often the parent looks. Each append is its own commit and rewrites
    // whole pages (the table leaf, the index, the AUTOINCREMENT counter), so
    // ~9k payloads/s put 50-60k frames a second into the WAL, ~240 MB. Once a
    // second let the WAL reach 470 MB in that benchmark; SQLite's own
    // checkpoint ran every ~20ms at that rate.
    private const WAL_CHECKPOINT_INTERVAL_SECONDS = 0.1;

    // How long that checkpoint waits for the writes already running in drain
    // workers, so how long it can hold up the event loop. About what an append
    // already waits behind a worker's write.
    private const WAL_CHECKPOINT_LOCK_WAIT_MS = 100;

    // After a pass that did not get its turn, the next waits this long: a
    // worker's long write (a VACUUM holds the lock for its whole length) must
    // not cost the event loop 100ms of every 100ms.
    private const WAL_CHECKPOINT_RETRY_SECONDS = 1.0;

    // Past 4 MB the -wal file stays that size (SQLite reuses it from the start
    // after a checkpoint), so its size cannot say when the next one is due.
    // Keyed on size alone, it ran every pass with anything written: at 5
    // payloads/s that was ten checkpoints a second, each two fsyncs inside the
    // event loop, where SQLite's own ran about every 30s. What says it is the
    // WAL not yet copied back, by any process (SqliteBuffer::
    // uncheckpointedWalBytes()): a drain worker catching up on a backlog writes
    // ~20 MB of WAL per 5,000-row claim and mark, which the parent's own writes
    // never show. When that cannot be read, the parent counts its own writes at
    // this much each (an append commits 3-7 whole pages, 13-29 KB of WAL at the
    // 4 KB page size, measured on payloads of 0.2-8 KB).
    private const WAL_BYTES_PER_WRITE = 16 * 1024;

    // Anything written at all is checkpointed at least this often.
    private const WAL_CHECKPOINT_MAX_INTERVAL_SECONDS = 10.0;
    private const BACK_PRESSURE_CHECK_INTERVAL = 5.0; // seconds

    /**
     * When the saved alert-channel list is refreshed: once this long after
     * startup, then every CHANNEL_PRIME_INTERVAL. See primeAlertChannels().
     */
    private const CHANNEL_PRIME_DELAY = 5.0; // seconds
    private const CHANNEL_PRIME_INTERVAL = 900.0; // seconds

    /**
     * Longest an alert child may live, in seconds (see forkAlertChild()). A
     * round is two passes, alerts then recoveries, each up to
     * HealthAlertNotifier::MAX_DISPATCH_SECONDS plus the one send that runs
     * past it and a channel load: about 100s at worst. The rest is margin for
     * slow DNS.
     */
    private const ALERT_CHILD_MAX_SECONDS = 180;

    /**
     * Ceiling on NIGHTOWL_DRAIN_WORKERS. Every worker holds its own Postgres
     * connection for its whole life (plus short-lived ones for maintenance),
     * and all of them share one SQLite file, which takes one writer at a time
     * for every claim and mark. Well before this many, extra workers mostly
     * wait on that lock, and without a ceiling a mistyped value (100) would
     * open that many connections against a database whose default
     * max_connections is 100.
     */
    public const MAX_DRAIN_WORKERS = 16;

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
    /** Whether that child is only refreshing the saved channel list, which an alert round may cut short. */
    private bool $alertChildIsPrime = false;
    /** ALERT_CHILD_MAX_SECONDS, as a property so a test can shorten it. */
    private int $alertChildMaxSeconds = self::ALERT_CHILD_MAX_SECONDS;
    private bool $shuttingDown = false;

    // A SIGTERM/SIGINT arrived; shutdown() runs on the next tick (see serve()).
    private bool $shutdownRequested = false;
    /** Pid of the process that called listen(). Only it stops the workers; a forked child never does. */
    private int $ownerPid = 0;
    /**
     * The buffer's owner lock, taken and kept open by AgentCommand. Held here
     * only so the alert children can close their copy (forkAlertChild()).
     *
     * @var resource|null
     */
    private $bufferLock = null;
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

    // Last pending-row count the buffer answered — see backlogCount().
    private int $lastBacklogCount = 0;

    /** Why the parent's buffer handle is closed, when reopening it failed. */
    private ?\Throwable $bufferOpenError = null;

    private float $lastBufferOpenAttempt = 0.0;

    // Set while recoverBuffer() has the drain workers stopped, so reapChildren()
    // does not restart them under it.
    private bool $recoveringBuffer = false;

    private float $lastWalCheckpointErrorLogAt = 0.0;

    private float $walCheckpointRetryAt = 0.0;

    private float $lastBufferRecoveryAt = 0.0;

    // The parent's own reads of the buffer failing as corrupt (backlogCount()).
    private float $parentBufferCorruptAt = 0.0;
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
        // CPU_HIGH / CPU_SATURATION thresholds and the window their average is
        // taken over (MetricsCollector::setCpuThresholds()). Appended LAST (see
        // above).
        float $cpuHighPct = 70.0,
        float $cpuCriticalPct = 90.0,
        int $cpuSustainSeconds = 60,
    ) {
        $this->expectedTokenHash = $token !== null
            ? substr(hash('xxh128', $token), 0, 7)
            : null;
        $this->drainWorkerCount = self::clampDrainWorkerCount($drainWorkerCount);
        // Pin the select loop, whatever extensions the host has loaded.
        // Loop::get() auto-picks a kernel-backed loop (ExtUvLoop / ExtEvLoop /
        // ExtEventLoop) whenever its extension is present — ext-uv is common
        // because Reverb's docs recommend it — and those loops cannot coexist
        // with this class's pcntl_fork() architecture: their backend (an epoll
        // fd, plus an io_uring ring on libuv >= 1.45) is a kernel object the
        // fork SHARES, so a child closing an inherited server deregisters the
        // PARENT's listeners and the agent goes permanently deaf at boot while
        // looking healthy to the supervisor (GitHub issue #6). php-uv exposes
        // no uv_loop_fork(), so there is no safe way to fork under it at all.
        // StreamSelectLoop keeps watcher state in process-local PHP arrays —
        // the only loop the forked children can safely drop listeners under —
        // and every published throughput number was measured on it. Replace
        // only when needed so a select loop a harness already registered
        // watchers on is never discarded.
        $loop = Loop::get();
        if (! $loop instanceof StreamSelectLoop) {
            $loop = new StreamSelectLoop;
            Loop::set($loop);
        }
        $this->loop = $loop;
        $this->metrics = new MetricsCollector($maxPendingRows, $maxBufferMemory);
        $this->metrics->setDrainWedgeSeconds($drainWedgeSeconds);
        $this->metrics->setCpuThresholds($cpuHighPct, $cpuCriticalPct, $cpuSustainSeconds);
    }

    /**
     * NIGHTOWL_DRAIN_WORKERS clamped to 1..MAX_DRAIN_WORKERS. Zero, a negative
     * number or a non-number (which casts to 0) means the one worker the agent
     * always ran with.
     */
    public static function clampDrainWorkerCount(int $requested): int
    {
        if ($requested > self::MAX_DRAIN_WORKERS) {
            error_log('[NightOwl Agent] NIGHTOWL_DRAIN_WORKERS='.$requested.' is above the maximum of '
                .self::MAX_DRAIN_WORKERS.'; using '.self::MAX_DRAIN_WORKERS.'.');

            return self::MAX_DRAIN_WORKERS;
        }

        return max(1, $requested);
    }

    public function drainWorkerCount(): int
    {
        return $this->drainWorkerCount;
    }

    /**
     * Hand over the buffer's owner lock (SqliteBuffer::acquireOwnerLock()).
     * The caller keeps it open; this process never releases it.
     *
     * @param  resource|null  $lock
     */
    public function setBufferLock($lock): void
    {
        $this->bufferLock = is_resource($lock) ? $lock : null;
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
        $this->ownerPid = getmypid();

        // SO_REUSEPORT lets multiple instances bind to the same port.
        // The kernel distributes incoming connections across all listeners.
        // This is how you scale beyond one core: run N agent instances on
        // the same port, each with its own SQLite buffer file.
        try {
            $this->server = new TcpServer("{$host}:{$port}", $this->loop, [
                'so_reuseport' => true,
            ]);
        } catch (RuntimeException $e) {
            if (self::isAddressInUse($e)) {
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

        // Health API, bound BEFORE any drain worker is forked. A second agent on
        // the same host fails here on the default port 2409, and failing after
        // the fork left its drain workers running with no parent to stop them.
        if ($this->healthEnabled) {
            $this->listenHealth($host);
        }

        try {
            $this->serve($host);
        } catch (\Throwable $e) {
            // Whatever failed after the fork, the workers must not outlive this
            // process: an orphaned worker keeps draining the buffer next to the
            // workers of the agent a supervisor starts to replace this one. Only
            // the agent process cleans up. A forked child reaches here when its
            // in-process drain throws, and must not signal its siblings.
            if (getmypid() === $this->ownerPid) {
                error_log("[NightOwl Agent] Stopping after an unexpected error: {$e->getMessage()}");
                try {
                    $this->shutdown();
                } catch (\Throwable) {
                    // The original error is the one to report.
                }
            }

            throw $e;
        }
    }

    /**
     * Bind the health API, or fail with the settings that fix a clash.
     */
    private function listenHealth(string $host): void
    {
        $this->healthServer = new HealthServer($this->loop);

        try {
            $this->healthServer->listen($host, $this->healthPort, $this);
        } catch (RuntimeException $e) {
            $this->healthServer = null;
            $this->server?->close();
            $this->server = null;

            $advice = 'Give each agent on a host its own health port with NIGHTOWL_HEALTH_PORT, '
                .'or turn the health API off with NIGHTOWL_HEALTH_ENABLED=false.';

            if (self::isAddressInUse($e)) {
                throw new PortInUseException(
                    "Could not start the agent: health port {$host}:{$this->healthPort} is already in use, "
                    ."often by another nightowl:agent on this host.\n\n".$advice,
                    (int) $e->getCode(),
                    $e
                );
            }

            throw new RuntimeException(
                "Could not start the agent: the health API could not listen on {$host}:{$this->healthPort} "
                ."({$e->getMessage()}).\n\n".$advice,
                (int) $e->getCode(),
                $e
            );
        }
    }

    private static function isAddressInUse(RuntimeException $e): bool
    {
        $message = strtolower($e->getMessage());

        return str_contains($message, 'address already in use') || str_contains($message, 'eaddrinuse');
    }

    /**
     * Everything after the listeners are bound: fork the drain workers, arm the
     * timers and signal handlers, run the loop until shutdown.
     */
    private function serve(string $host): void
    {
        $this->releaseStaleClaims();

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

        $this->loop->addPeriodicTimer(self::WAL_CHECKPOINT_INTERVAL_SECONDS, function () {
            $this->checkpointIfWalLarge();
        });

        // Back-pressure monitor — check pending row count and process RSS periodically.
        // Pending count (not WAL file size) because SQLite never shrinks the WAL
        // file during operation — filesize() would never decrease after a surge.
        // RSS check because PHP's Zend Memory Manager doesn't return freed memory
        // to the OS — totalBufferBytes can't detect the RSS staircase.
        $this->loop->addPeriodicTimer(self::BACK_PRESSURE_CHECK_INTERVAL, function () {
            $pending = $this->backlogCount();
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

        // Keep the saved alert-channel list current while the database is
        // reachable, so the first alert of an outage has somewhere to go.
        $this->loop->addTimer(self::CHANNEL_PRIME_DELAY, fn () => $this->primeAlertChannels());
        $this->loop->addPeriodicTimer(self::CHANNEL_PRIME_INTERVAL, fn () => $this->primeAlertChannels());

        // 10-second diagnosis — run health checks and update score
        $this->loop->addPeriodicTimer(10.0, function () {
            $this->recoverBufferIfCorrupt();

            $pending = $this->backlogCount();
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
        // Reaped on the next loop tick, not inside the handler. ReactPHP turns
        // on async signals, so the handler runs between any two statements,
        // including in the middle of a buffer write or checkpoint. There, the
        // reaper's own buffer write would take the checkpoint lock through the
        // same handle, which converts the checkpoint's exclusive hold to shared
        // and then drops it; and a respawn would fork with the buffer open.
        $this->loop->addSignal(SIGCHLD, fn () => $this->loop->futureTick(fn () => $this->reapChildren()));

        // Graceful shutdown, on the next tick for the same reason as SIGCHLD:
        // run inside the handler, it could start in the middle of a checkpoint
        // and wait up to CHILD_SHUTDOWN_TIMEOUT for the workers while holding
        // the checkpoint lock, which every final write of theirs then waits on.
        $this->loop->addSignal(SIGINT, fn () => $this->requestShutdown());
        $this->loop->addSignal(SIGTERM, fn () => $this->requestShutdown());

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
     * Free rows a previous run's drain workers claimed and never finished.
     *
     * First marks synced the rows those workers had written to PostgreSQL but
     * could not mark (markRowsWorkersCommitted()), so they are not drained a
     * second time, and deletes every such list either way. A list left in
     * place could outlive its file: a buffer deleted by hand and recreated
     * can get the same inode back, and a recovery much later would then mark
     * the new file's rows with those ids synced without draining them. At
     * startup nothing has written to a recreated file yet, so a stale list
     * matches no row.
     *
     * Runs before the first fork, on a connection that is closed again before
     * it (the fork-safety rule in forkDrainWorker()). A failure is logged, not
     * fatal: the rows stay in the buffer and the next start tries again.
     */
    private function releaseStaleClaims(): void
    {
        try {
            $buffer = new SqliteBuffer($this->sqlitePath);
            $this->markRowsWorkersCommitted($buffer, true);
            $released = $buffer->releaseAllClaimed();
            unset($buffer);

            if ($released > 0) {
                error_log("[NightOwl Agent] Released {$released} buffer rows a previous run's drain workers had claimed.");
            }
        } catch (\Throwable $e) {
            error_log("[NightOwl Agent] Could not release stale buffer claims: {$e->getMessage()}");
        }
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

        $forked = $this->forkAlertChild(
            fn () => $this->dispatchHealthAlertsInline($active, $resolved),
            prime: false,
        );

        if ($forked === null) {
            error_log('[NightOwl Agent] Could not fork for health alert dispatch — sending inline.');
            $this->dispatchHealthAlertsInline($active, $resolved);
        }
    }

    /**
     * Refresh the saved alert-channel list (HealthAlertNotifier::primeChannels()).
     *
     * The list is what a round falls back to when the database is down, and a
     * round saves it only when it reaches the database. A healthy agent may
     * never run a round, so without this its first outage alert had no list to
     * go to. The load is a blocking Postgres query, so it runs in the same kind
     * of child as an alert round and never on the loop. Without pcntl there is
     * no such child, and priming is skipped: an inline query here would stall
     * ingest on every tick for a list that may never be needed.
     */
    private function primeAlertChannels(): void
    {
        if ($this->shuttingDown || $this->healthAlertNotifier === null) {
            return;
        }

        if (! function_exists('pcntl_fork') || ! function_exists('posix_kill')) {
            return;
        }

        $this->forkAlertChild(fn () => $this->healthAlertNotifier?->primeChannels(), prime: true);
    }

    /**
     * Run $work in a short-lived forked child, at most one at a time.
     *
     * An alert round in flight makes the next one skip, as before. A prime in
     * flight gives way instead: it is killed, because it only refreshes a file
     * and an alert must not be skipped for it. A prime can also be stuck on a
     * database that accepts the connection and never answers, and this is what
     * ends it.
     *
     * @return bool|null true when forked, false when skipped, null when fork failed
     */
    private function forkAlertChild(\Closure $work, bool $prime): ?bool
    {
        // Read once: the SIGCHLD handler runs between any two statements
        // (ReactPHP turns on async signals) and clears the pid when it reaps
        // the child.
        $inFlight = $this->alertChildPid;
        $inFlightIsPrime = $this->alertChildIsPrime;

        if ($inFlight !== null) {
            // >0 reaped here, -1 already reaped elsewhere; 0 means it is still
            // running.
            if (pcntl_waitpid($inFlight, $status, WNOHANG) === 0) {
                if (! $inFlightIsPrime) {
                    if (! $prime) {
                        error_log('[NightOwl Agent] Health alert dispatch still in flight, skipping this round.');
                    }

                    // A prime has nothing to add: the round in flight loads the
                    // channels and saves them itself.
                    return false;
                }

                $this->stopAlertChild();
            }

            $this->alertChildPid = null;
        }

        // SIGCHLD is held until the pid is recorded below. A child that ended
        // before that would be reaped as an unknown pid, and the pid recorded
        // after it would be stale. The child keeps the mask: it has no
        // children of its own.
        $masked = function_exists('pcntl_sigprocmask')
            && pcntl_sigprocmask(SIG_BLOCK, [SIGCHLD], $previousMask);

        $pid = pcntl_fork();

        if ($pid === -1) {
            if ($masked) {
                pcntl_sigprocmask(SIG_SETMASK, $previousMask);
            }

            return null;
        }

        if ($pid === 0) {
            // === Child ===
            // The SIGKILL is in a finally, not trailing the body: anything that
            // escapes here would reach PHP's fatal handler, which shuts the
            // process down through the destructors this whole approach exists to
            // avoid. The child never runs the loop, so it is not stopped — only
            // the listeners are dropped, so a slow dispatch cannot hold the
            // ingest port open. (Dropping them via close() is fork-safe only
            // under the pinned StreamSelectLoop — see forkDrainWorker.)
            try {
                // A ceiling on this child's life. The notifier's statement
                // timeout covers a database that is waiting on something, but
                // nothing on the client side covers one that took the
                // connection and went silent, and a round stuck there made
                // every later transition skip as still in flight. SIGALRM
                // with its default action ends the process in the kernel,
                // blocked in libpq or not, and like SIGKILL runs no
                // destructors. A PHP handler would not help: it only runs
                // between opcodes, never inside a blocked libpq call.
                pcntl_signal(SIGALRM, SIG_DFL);
                pcntl_alarm($this->alertChildMaxSeconds);

                $this->server?->close();
                $this->udpSocket?->close();
                $this->healthServer?->close();

                // The owner lock came through the fork, and the kernel keeps it
                // while any copy is open. This child never touches the buffer,
                // and if the agent died first, its copy would keep the next
                // agent from starting until the round ended. Closing it here
                // leaves the agent's own lock in place.
                if (is_resource($this->bufferLock)) {
                    fclose($this->bufferLock);
                }

                $work();
            } catch (\Throwable $e) {
                $what = $prime ? 'Alert channel refresh' : 'Health alert dispatch';
                error_log("[NightOwl Agent] {$what} child failed: {$e->getMessage()}");
            } finally {
                posix_kill(posix_getpid(), SIGKILL);
            }
        }

        $this->alertChildPid = $pid;
        $this->alertChildIsPrime = $prime;

        if ($masked) {
            pcntl_sigprocmask(SIG_SETMASK, $previousMask);
        }

        return true;
    }

    /** SIGKILL the in-flight alert child and reap it. */
    private function stopAlertChild(): void
    {
        // A local copy, as in forkAlertChild(): the SIGCHLD handler may clear
        // the property at any point, and a null pid here would turn the wait
        // below into a blocking wait on any child.
        $pid = $this->alertChildPid;
        if ($pid === null) {
            return;
        }

        posix_kill($pid, SIGKILL);
        // Returns at once: the child is dead, or the SIGCHLD handler already
        // reaped it (-1).
        pcntl_waitpid($pid, $status);
        $this->alertChildPid = null;
        $this->alertChildIsPrime = false;
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

    /**
     * Reap every child that has exited (the SIGCHLD handler). A drain worker
     * has its claimed rows released and is forked again.
     */
    private function reapChildren(): void
    {
        // Reap all terminated children
        while (($result = pcntl_waitpid(-1, $status, WNOHANG)) > 0) {
            // The alert child is reaped here too. Forget its pid, or once the
            // kernel hands that pid to a new process (a respawned drain worker,
            // say), forkAlertChild() and shutdown() would take it for the alert
            // child still running, and stopAlertChild() would SIGKILL it.
            if ($result === $this->alertChildPid) {
                if (pcntl_wifsignaled($status) && pcntl_wtermsig($status) === SIGALRM) {
                    $what = $this->alertChildIsPrime ? 'Alert channel refresh' : 'Health alert dispatch';
                    error_log("[NightOwl Agent] {$what} was stopped after {$this->alertChildMaxSeconds}s. The database or an alert channel did not answer.");
                }

                $this->alertChildPid = null;
                $this->alertChildIsPrime = false;

                continue;
            }

            // Find which worker this PID belongs to
            $deadWorkerId = array_search($result, $this->drainChildPids, true);
            if ($deadWorkerId === false) {
                continue;
            }

            unset($this->drainChildPids[$deadWorkerId]);

            // Release any rows claimed by the dead worker back to pending. A buffer
            // that refuses this (corrupt, read-only) must not throw out of the
            // SIGCHLD handler: that ends the event loop, health reports included.
            // The rows stay claimed and the next start releases them.
            if ($this->buffer !== null) {
                try {
                    $this->buffer->releaseClaimed($deadWorkerId);
                } catch (\Throwable $e) {
                    error_log("[NightOwl Agent] Could not release the rows drain worker #{$deadWorkerId} had claimed: {$e->getMessage()}");
                }
            }

            // Don't restart during shutdown — but keep reaping other dead children.
            // Nor during a buffer recovery, which stopped them and restarts them
            // itself once the file is fixed.
            if ($this->shuttingDown || $this->recoveringBuffer) {
                continue;
            }

            // Restart cooldown — prevents fork bomb when child crashes on startup
            $elapsed = microtime(true) - ($this->lastChildSpawns[$deadWorkerId] ?? 0);
            if ($elapsed < self::RESTART_COOLDOWN) {
                $delay = self::RESTART_COOLDOWN - $elapsed;
                error_log("[NightOwl Agent] Drain worker #{$deadWorkerId} died, restarting in {$delay}s...");
                $this->loop->addTimer($delay, function () use ($deadWorkerId) {
                    if (! $this->shuttingDown && ! $this->recoveringBuffer && ! isset($this->drainChildPids[$deadWorkerId])) {
                        $this->forkDrainWorker($deadWorkerId);
                    }
                });
                continue;
            }

            $exitCode = pcntl_wifexited($status) ? pcntl_wexitstatus($status) : -1;
            error_log("[NightOwl Agent] Drain worker #{$deadWorkerId} exited (code: {$exitCode}), restarting...");
            $this->forkDrainWorker($deadWorkerId);
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
            $this->openBuffer();
            throw new RuntimeException('Failed to fork drain worker process');
        }

        if ($pid === 0) {
            // === Child process ===
            // Close inherited sockets so neither the exec'd process nor the
            // in-process drain loop holds the parent's listeners open. Going
            // through close() — which calls removeReadStream on the loop — is
            // safe ONLY because the constructor pins StreamSelectLoop, whose
            // watcher lists are process-local PHP arrays. Under a kernel-backed
            // loop these same calls would issue epoll_ctl against the backend
            // the fork shares with the parent, deregistering the parent's own
            // listeners (issue #6).
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
        $this->openBuffer();

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
                $this->appendToBuffer($result['rawPayload']);
                $this->metrics->recordIngest();
                $conn->write('2:OK');
            } catch (\Throwable $e) {
                $this->recordBufferWriteFailure($e);
                $conn->write('5:ERROR');
            }
        }

        $conn->end();
    }

    /**
     * (Re)open the parent's buffer handle after a fork.
     *
     * A buffer that can no longer be opened (chowned or made read-only under the
     * running agent, corrupt, its directory gone) leaves the handle closed rather
     * than throwing: this runs in the SIGCHLD handler and the restart timer, and
     * an exception there ends the event loop, so the agent exited before any
     * health report could say why. Appends retry the open and count as refused
     * meanwhile, which is what raises BUFFER_UNWRITABLE.
     */
    private function openBuffer(): void
    {
        $this->lastBufferOpenAttempt = microtime(true);

        try {
            $this->buffer = new SqliteBuffer($this->sqlitePath);
            $this->bufferOpenError = null;
        } catch (\Throwable $e) {
            $this->buffer = null;
            if ($this->bufferOpenError === null) {
                error_log("[NightOwl Agent] Cannot open the SQLite buffer at {$this->sqlitePath}: {$e->getMessage()}");
            }
            $this->bufferOpenError = $e;
        }
    }

    /** Append one payload, retrying a failed open at most once a second. */
    private function appendToBuffer(string $rawPayload): void
    {
        if ($this->buffer === null && $this->bufferOpenError !== null
            && microtime(true) - $this->lastBufferOpenAttempt >= 1.0) {
            $this->openBuffer();
        }

        if ($this->buffer === null) {
            throw $this->bufferOpenError ?? new RuntimeException('The SQLite buffer is not open.');
        }

        $this->buffer->appendRaw($rawPayload);
    }

    /**
     * Pending rows for the back-pressure check, the diagnosis tick and /status.
     *
     * A buffer that cannot answer (a corrupt page under the COUNT) must not throw
     * out of a loop timer: an exception there ends the event loop, and with it the
     * health reports that carry BUFFER_UNWRITABLE. Falls back to the last count it
     * did answer — the refused appends and the drain report the failure itself.
     */
    private function backlogCount(): int
    {
        if ($this->buffer === null) {
            return 0;
        }

        try {
            return $this->lastBacklogCount = $this->buffer->backlogCount();
        } catch (\Throwable $e) {
            if (SqliteFailure::isCorruption(SqliteFailure::code($e))) {
                $this->parentBufferCorruptAt = microtime(true);
            }

            return $this->lastBacklogCount;
        }
    }

    /**
     * A payload the buffer refused: counted (BUFFER_UNWRITABLE, and as a reject)
     * and logged at most once a minute. It was one line per payload, and the usual
     * cause is a full disk that those lines were filling further.
     */
    private function recordBufferWriteFailure(\Throwable $e): void
    {
        $report = $this->metrics->recordBufferWriteFailure(SqliteFailure::code($e));
        if ($report > 0) {
            error_log("[NightOwl Agent] SQLite buffer error ({$report} payload(s) refused since the last report): {$e->getMessage()}");
        }
    }

    private function requestShutdown(): void
    {
        $this->shutdownRequested = true;
        $this->loop->futureTick(fn () => $this->shutdown());
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

        // A prime in flight is only refreshing a file, and one stuck on a slow
        // database would otherwise outlive the agent. An alert round is left
        // to finish: it is bounded, and killing it loses the alert.
        $alertChild = $this->alertChildPid;
        if ($alertChild !== null && $this->alertChildIsPrime
            && pcntl_waitpid($alertChild, $status, WNOHANG) === 0) {
            $this->stopAlertChild();
        }

        $this->stopDrainWorkers();

        $this->loop->stop();
    }

    /**
     * Stop every drain worker and wait for it: SIGTERM, then SIGKILL for any
     * still running after CHILD_SHUTDOWN_TIMEOUT. Blocks the event loop while it
     * waits.
     */
    private function stopDrainWorkers(): void
    {
        foreach ($this->drainChildPids as $workerId => $pid) {
            posix_kill($pid, SIGTERM);
        }

        $deadline = time() + self::CHILD_SHUTDOWN_TIMEOUT;
        while (! empty($this->drainChildPids) && time() < $deadline) {
            foreach ($this->drainChildPids as $workerId => $pid) {
                // Not 0 = gone: reaped here, or already by the SIGCHLD handler
                // (-1), which async signals can run in the middle of this loop.
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
    }

    /**
     * Report a recovery nightowl:agent ran on a buffer that was corrupt at
     * startup, before this server existed: BUFFER_REPAIRED, as for one run
     * here, and the cooldown starts from it.
     *
     * @param  array{outcome: string, problems: list<string>, pending_rows: ?int, moved_to: ?string}  $result
     */
    public function recordStartupBufferRecovery(array $result): void
    {
        $this->lastBufferRecoveryAt = microtime(true);
        $this->metrics->recordBufferRecovery(
            $result['outcome'],
            $result['pending_rows'],
            $result['moved_to'] !== null ? basename($result['moved_to']) : null,
        );
    }

    /**
     * Start a buffer recovery when the buffer has recently said it is corrupt.
     *
     * The signal comes from the drain (its claim/fetch/mark, via the metrics
     * file), from refused appends, or from the parent's own backlog count. Only
     * a signal newer than the last recovery counts, at most one recovery per
     * BUFFER_RECOVERY_COOLDOWN_SECONDS.
     */
    private function recoverBufferIfCorrupt(): void
    {
        $now = microtime(true);
        $signalAt = max($this->metrics->bufferCorruptionSignalAt(), $this->parentBufferCorruptAt);

        if ($signalAt <= $this->lastBufferRecoveryAt
            || $now - $signalAt > self::BUFFER_CORRUPTION_FRESH_SECONDS
            || $now - $this->lastBufferRecoveryAt < self::BUFFER_RECOVERY_COOLDOWN_SECONDS) {
            return;
        }

        $this->recoverBuffer();
    }

    /**
     * Repair a corrupt buffer, or set it aside and start a fresh one.
     *
     * Stops the drain workers and closes the parent's handle first, so nothing
     * else has the file open while BufferRecovery reads, rebuilds or renames
     * it. Blocks the event loop while the workers stop (up to
     * CHILD_SHUTDOWN_TIMEOUT) and for the length of the check (about 2s on a
     * 1 GB buffer on a fast disk, longer on a slow one). Payloads that arrive
     * meanwhile wait in the socket backlog. Ingest is usually failing already,
     * but not always: with only an index damaged, appends still succeed while
     * the drain's mark fails.
     */
    private function recoverBuffer(): void
    {
        $this->lastBufferRecoveryAt = microtime(true);
        $this->recoveringBuffer = true;
        $workerIds = array_keys($this->drainChildPids);

        error_log('[NightOwl Agent] The SQLite buffer reports corruption. Stopping the drain to check it.');

        try {
            $this->stopDrainWorkers();
            $this->buffer = null;

            $result = BufferRecovery::recover($this->sqlitePath);
            $movedTo = $result['moved_to'] !== null ? basename($result['moved_to']) : null;

            error_log('[NightOwl Agent] '.BufferRecovery::describe($result)
                .($result['outcome'] === BufferRecovery::UNCHECKED ? ' Checking again in '.(self::BUFFER_RECOVERY_COOLDOWN_SECONDS / 60).' minutes.' : ''));

            $this->metrics->recordBufferRecovery($result['outcome'], $result['pending_rows'], $movedTo);
        } catch (\Throwable $e) {
            error_log("[NightOwl Agent] Buffer recovery failed: {$e->getMessage()}");
        } finally {
            $this->openBuffer();

            // Before the claims go: a released row is up for any worker to drain.
            if ($this->buffer !== null) {
                $this->markRowsWorkersCommitted($this->buffer, isset($result) && $result['outcome'] !== BufferRecovery::MOVED_ASIDE);
            }

            // The stopped workers' claims, released as at startup: no worker is
            // running, so nothing else can hold one.
            try {
                $this->buffer?->releaseAllClaimed();
            } catch (\Throwable) {
                // A buffer still broken gets another recovery after the cooldown.
            }

            $this->recoveringBuffer = false;
            $this->parentBufferCorruptAt = 0.0;

            foreach ($workerIds as $workerId) {
                // A SIGTERM that came during the recovery: the agent is
                // stopping, and a worker started now would only be stopped again.
                if ($this->shutdownRequested || $this->shuttingDown) {
                    break;
                }
                if (isset($this->drainChildPids[$workerId])) {
                    continue;
                }

                try {
                    $this->forkDrainWorker($workerId);
                } catch (\Throwable $e) {
                    error_log("[NightOwl Agent] Could not restart drain worker #{$workerId} after the buffer recovery: {$e->getMessage()}");
                }
            }
        }
    }

    /**
     * Mark synced the rows the stopped workers had committed to PostgreSQL
     * but could not mark (DrainWorker::unmarkedIdsPath()). The failure that
     * starts a recovery is usually that very mark, so without this every
     * recovery would drain up to a batch per worker into PostgreSQL twice.
     *
     * Only on the file the ids came from: a fresh buffer numbers its rows from
     * 1 again, and marking those would drop new telemetry. Hence $sameFile
     * (false after a move aside) and the inode each list was written with.
     * Every worker's list, not only the stopped ones': one left by an earlier
     * run with more workers is just as good.
     */
    private function markRowsWorkersCommitted(SqliteBuffer $buffer, bool $sameFile): void
    {
        $inode = @fileinode($this->sqlitePath);

        foreach (glob(DrainWorker::unmarkedIdsPattern($this->sqlitePath)) ?: [] as $path) {
            $workerId = (int) substr(basename($path, '.json'), strrpos(basename($path, '.json'), '-') + 1);
            $saved = json_decode((string) @file_get_contents($path), true);
            @unlink($path);

            if (! $sameFile || ! is_array($saved)
                || ($saved['inode'] ?? null) !== $inode || ! is_array($saved['ids'] ?? null)) {
                continue;
            }

            $ids = array_values(array_filter($saved['ids'], 'is_int'));
            try {
                $buffer->markSynced($ids);
                error_log('[NightOwl Agent] Marked '.count($ids)." rows drain worker #{$workerId} had already written to PostgreSQL, so they are not drained twice.");
            } catch (\Throwable $e) {
                error_log("[NightOwl Agent] Could not mark the rows drain worker #{$workerId} had already written; they may reach PostgreSQL twice: {$e->getMessage()}");
            }
        }
    }

    /**
     * Checkpoint once the WAL passes WAL_CHECKPOINT_BYTES, waiting at most
     * WAL_CHECKPOINT_LOCK_WAIT_MS for its turn; when a worker's write runs
     * longer, a pass WAL_CHECKPOINT_RETRY_SECONDS later tries again. The -wal
     * file keeps its size after a checkpoint (SQLite rewrites it from the
     * start), so past 4 MB the size is always over. From then on a checkpoint
     * is due once WAL_CHECKPOINT_BYTES of WAL wait to be copied back (see
     * WAL_BYTES_PER_WRITE), or when anything at all was written and the last
     * checkpoint is WAL_CHECKPOINT_MAX_INTERVAL_SECONDS old.
     */
    private function checkpointIfWalLarge(): void
    {
        if ($this->buffer === null || microtime(true) < $this->walCheckpointRetryAt
            || $this->buffer->walSize() <= self::WAL_CHECKPOINT_BYTES) {
            return;
        }

        try {
            $waiting = $this->buffer->uncheckpointedWalBytes()
                ?? $this->buffer->writesSinceCheckpoint() * self::WAL_BYTES_PER_WRITE;
            // Changed since, too: a reader can keep a checkpoint from copying
            // everything back, and retrying with nothing new written gains nothing.
            $due = $this->buffer->changedSinceCheckpoint()
                && ($waiting >= self::WAL_CHECKPOINT_BYTES
                    || microtime(true) - $this->buffer->lastCheckpointAt() >= self::WAL_CHECKPOINT_MAX_INTERVAL_SECONDS);
            if (! $due) {
                return;
            }
            if (! $this->buffer->checkpoint(lockWaitMs: self::WAL_CHECKPOINT_LOCK_WAIT_MS)) {
                $this->walCheckpointRetryAt = microtime(true) + self::WAL_CHECKPOINT_RETRY_SECONDS;
            }
        } catch (\Throwable $e) {
            // Ten times a second while it lasts, so at most one line a minute.
            if (microtime(true) - $this->lastWalCheckpointErrorLogAt >= 60) {
                $this->lastWalCheckpointErrorLogAt = microtime(true);
                error_log("[NightOwl Agent] WAL checkpoint failed (will retry): {$e->getMessage()}");
            }
        }
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
                        $this->appendToBuffer($result['rawPayload']);
                        $this->metrics->recordIngest();
                    } catch (\Throwable $e) {
                        $this->recordBufferWriteFailure($e);
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
            $this->backlogCount(),
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
