<?php

namespace NightOwl;

use Illuminate\Contracts\Events\Dispatcher as DispatcherContract;
use Illuminate\Events\Dispatcher;
use Illuminate\Support\ServiceProvider;
use Illuminate\Foundation\Http\Events\RequestHandled;
use Laravel\Nightwatch\Contracts\Ingest as IngestContract;
use Laravel\Nightwatch\Core;
use Laravel\Nightwatch\Ingest;
use Laravel\Nightwatch\Records\Request as RequestRecord;
use Laravel\Nightwatch\RecordsBuffer;
use Laravel\Nightwatch\SocketStreamFactory;
use NightOwl\Agent\AsyncServer;
use NightOwl\Agent\ConnectionHandler;
use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\PayloadParser;
use NightOwl\Agent\RecordWriter;
use NightOwl\Agent\VersionDriftWatcher;
use NightOwl\Agent\Server;
use NightOwl\Commands\AgentCommand;
use NightOwl\Commands\BackfillLogContextCommand;
use NightOwl\Commands\BackfillRollupsCommand;
use NightOwl\Commands\ClearCommand;
use NightOwl\Commands\DrainWorkerCommand;
use NightOwl\Commands\DropV1HistogramsCommand;
use NightOwl\Commands\GcDictRoutesCommand;
use NightOwl\Commands\GcDictTracesCommand;
use NightOwl\Commands\InstallCommand;
use NightOwl\Commands\MigrateCommand;
use NightOwl\Commands\PartitionCommand;
use NightOwl\Commands\PruneCommand;
use NightOwl\Commands\RepairCacheRollupKeysCommand;
use NightOwl\Commands\TestAlertCommand;
use NightOwl\Support\CapturingIngest;
use NightOwl\Support\DatabaseSslMode;
use NightOwl\Support\InstalledVersionReader;
use NightOwl\Support\MultiIngest;
use NightOwl\Support\NightwatchIngestArguments;
use NightOwl\Support\PayloadCapture;

class NightOwlAgentServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../config/nightowl.php', 'nightowl');

        $this->app->singleton(PayloadParser::class, function ($app) {
            $debugDumpPath = null;
            if ((bool) config('nightowl.agent.debug_raw_payloads', false)) {
                $debugDumpPath = (string) config(
                    'nightowl.agent.debug_raw_payloads_path',
                    storage_path('nightowl/raw-payloads.jsonl'),
                );
                error_log('[NightOwl Agent] RAW PAYLOAD DEBUG ENABLED — dumping to '.$debugDumpPath.' (DO NOT leave on in prod)');
            }

            return new PayloadParser(
                (bool) config('nightowl.agent.gzip_enabled', true),
                $debugDumpPath,
            );
        });

        $this->app->singleton(RecordWriter::class, function ($app) {
            return RecordWriter::fromConfig();
        });

        $this->app->singleton(ConnectionHandler::class, function ($app) {
            return new ConnectionHandler(
                $app->make(PayloadParser::class),
                $app->make(RecordWriter::class),
                config('nightowl.agent.token'),
            );
        });

        $this->app->singleton(Server::class, function ($app) {
            return new Server(
                $app->make(ConnectionHandler::class),
            );
        });

        $this->app->singleton(DrainWorker::class, function ($app) {
            return new DrainWorker(
                sqlitePath: config('nightowl.agent.sqlite_path', storage_path('nightowl/agent-buffer.sqlite')),
                pgHost: config('nightowl.database.host', '127.0.0.1'),
                pgPort: (int) config('nightowl.database.port', 5432),
                pgDatabase: config('nightowl.database.database', 'nightowl'),
                pgUsername: config('nightowl.database.username', 'nightowl'),
                pgPassword: config('nightowl.database.password', 'nightowl'),
                batchSize: (int) config('nightowl.agent.drain_batch_size', 1000),
                intervalMs: (int) config('nightowl.agent.drain_interval_ms', 100),
                maxWaitMs: (int) config('nightowl.agent.drain_max_wait_ms', 5000),
                appName: config('app.name', 'NightOwl'),
                environment: config('nightowl.environment') ?: config('app.env', 'production'),
                sslmode: config('nightowl.database.sslmode', 'prefer'),
                quarantineEnabled: (bool) config('nightowl.agent.drain_quarantine_enabled', false),
            );
        });

        // Swap Nightwatch's ingest for NightOwl's at the START of boot: every
        // provider has registered by then (so Core is bound whichever of the two
        // packages registered first) and none has booted. Nightwatch's listeners
        // are live from its register(), so a log line, query or cache call in any
        // provider's boot() is recorded straight away. Swapping in booted(), as
        // this used to, left those records in the SDK's own ingest: alone,
        // nothing ever sent them, and in parallel mode only Nightwatch got them.
        // It also missed an exception thrown by a boot(), after which booted()
        // never fires. booted() stays as the fallback for a provider registered
        // after the application booted, where booting() never fires.
        //
        // In booted(), after every boot(), wireIngest() checks the settings
        // again, because a provider's boot() may have changed them after the
        // swap: nightowl.enabled turned off detaches NightOwl, and changed
        // ingest settings (port, token, ingest_uri, parallel mode, capture)
        // rebuild the ingest with the new values. Otherwise it is a no-op.
        $this->app->booting(fn () => $this->wireIngest());
        $this->app->booted(fn () => $this->wireIngest());

        $this->app->singleton(AsyncServer::class, function ($app) {
            return new AsyncServer(
                $app->make(PayloadParser::class),
                config('nightowl.agent.sqlite_path', storage_path('nightowl/agent-buffer.sqlite')),
                $app->make(DrainWorker::class),
                config('nightowl.agent.token'),
                (int) config('nightowl.agent.max_pending_rows', 100_000),
                (int) config('nightowl.agent.max_buffer_memory', 256 * 1024 * 1024),
                (bool) config('nightowl.agent.enable_udp', false),
                (int) config('nightowl.agent.udp_port', 2408),
                (bool) config('nightowl.agent.health_enabled', true),
                (int) config('nightowl.agent.health_port', 2409),
                (string) config('nightowl.agent.api_url', 'https://api.usenightowl.com'),
                (string) config('nightowl.agent.token', ''),
                (bool) config('nightowl.agent.health_report_enabled', true),
                (int) config('nightowl.agent.health_report_interval', 30),
                (array) config('nightowl.agent.health_report_intervals', []),
                (string) config('nightowl.database.database', 'nightowl'),
                drainWorkerCount: (int) config('nightowl.agent.drain_workers', 1),
                drainSpawner: $this->makeDrainSpawner($app),
                drainWedgeSeconds: (float) config('nightowl.drain_connection.wedge_warn_seconds', 180),
                driftWatcher: $this->makeDriftWatcher(),
                backfillMarkerPath: MigrateCommand::backfillMarkerPath(),
            );
        });
    }

    /**
     * Point Nightwatch's Core at NightOwl's ingest (alone, or next to
     * Nightwatch's own in parallel mode). See register() for when this runs.
     *
     * Fails open: this runs inside the host application's boot, and a
     * monitoring package must never be the reason an application does not
     * start. On any error the SDK's ingest is left exactly as it was.
     */
    protected function wireIngest(): void
    {
        // Idempotent: this hook can fire more than once (provider re-registered,
        // Octane reset, etc). Re-wrapping $core->ingest with MultiIngest each time
        // multiplies outbound writes — N wraps = each record sent to the NightOwl
        // agent N times. Once attached, a later call only applies settings that
        // changed since (see rewireIfChanged()).
        if ($this->app->bound('nightowl.ingest_wired')) {
            $this->rewireIfChanged();

            return;
        }

        // Master switch: when disabled (e.g. NIGHTOWL_ENABLED=false in the
        // testing environment) we never rebind Nightwatch's ingest, so no
        // telemetry is collected or transmitted to the agent socket.
        if (! config('nightowl.enabled', true)) {
            return;
        }

        if (! $this->app->bound(Core::class)) {
            return;
        }

        $this->app->instance('nightowl.ingest_wired', true);

        try {
            $this->app->instance('nightowl.ingest_wired', $this->attachIngest($this->app->make(Core::class)));
        } catch (\Throwable $e) {
            error_log('[NightOwl Agent] could not attach to Nightwatch, so this process sends no telemetry to NightOwl: '.$e->getMessage());
        }
    }

    /**
     * Apply NightOwl settings a provider's boot() changed after the swap.
     *
     * The swap runs before any boot(), so a boot() that sets nightowl.* config
     * was not read. nightowl.enabled turned off puts the SDK's own ingest back,
     * as if NightOwl had never attached. Changed ingest settings rebuild
     * NightOwl's ingest with the new values. Fails open, like the swap.
     */
    protected function rewireIfChanged(): void
    {
        try {
            $attached = $this->app->make('nightowl.ingest_wired');
            if (! is_array($attached)) {
                return;
            }

            // Something else has replaced the ingest since: it is not ours to undo.
            $core = $attached['core'];
            if ($core->ingest !== $attached['wired']) {
                return;
            }

            $enabled = (bool) config('nightowl.enabled', true);
            if ($enabled && self::ingestSettings() === $attached['settings']) {
                return;
            }

            $this->detachIngest($attached);

            if (! $enabled) {
                $this->app->forgetInstance('nightowl.ingest_wired');

                return;
            }

            $this->app->instance('nightowl.ingest_wired', true);
            $this->app->instance('nightowl.ingest_wired', $this->attachIngest($core));
        } catch (\Throwable $e) {
            error_log('[NightOwl Agent] could not apply NightOwl settings changed during boot: '.$e->getMessage());
        }
    }

    /**
     * Every setting attachIngest() and wirePayloadCapture() read, to tell
     * whether a provider's boot() changed one after the swap.
     *
     * @return list<mixed>
     */
    protected static function ingestSettings(): array
    {
        return [
            config('nightowl.agent.port'),
            config('nightowl.agent.token'),
            config('nightwatch.token'),
            config('nightowl.agent.ingest_uri'),
            config('nightowl.agent.ingest_timeout'),
            config('nightowl.parallel_with_nightwatch'),
            config('nightowl.capture'),
            config('nightwatch.redact_payload_fields'),
        ];
    }

    /**
     * Put the ingest attachIngest() replaced back on Core.
     *
     * Alone, the records NightOwl's ingest holds would have been the SDK's to
     * send had NightOwl never attached, so they go back to it. In parallel mode
     * the SDK's ingest already has its own copy, and NightOwl's are dropped.
     *
     * @param  array{core: object, previous: mixed, nightowl: IngestContract, wired: MultiIngest, parallel: bool, settings: list<mixed>}  $attached
     */
    protected function detachIngest(array $attached): void
    {
        $attached['core']->ingest = $attached['previous'];

        $records = self::bufferedRecords($attached['nightowl']);
        $attached['nightowl']->flush();

        if (! $attached['parallel'] && $attached['previous'] instanceof IngestContract) {
            // Through MultiIngest for its fail-open write, as in the hand-over.
            $sink = new MultiIngest($attached['previous']);
            foreach ($records as $record) {
                if (is_array($record)) {
                    $sink->write($record);
                }
            }
        }
    }

    /**
     * What an ingest holds and has not sent yet. Empty when that cannot be
     * read: RecordsBuffer is @internal, and all() does not exist on 1.26.
     *
     * @return list<mixed>
     */
    protected static function bufferedRecords(mixed $ingest): array
    {
        if ($ingest instanceof CapturingIngest) {
            $ingest = $ingest->inner;
        }

        if (! $ingest instanceof Ingest) {
            return [];
        }

        // ?? rather than property_exists(): it also answers null for a
        // property that is there but not public, instead of throwing.
        $buffer = $ingest->buffer ?? null;
        if (! $buffer instanceof RecordsBuffer || ! method_exists($buffer, 'all')) {
            return [];
        }

        return $buffer->all();
    }

    /**
     * Build NightOwl's ingest and swap it onto Core::$ingest.
     *
     * @return array{core: object, previous: mixed, nightowl: IngestContract, wired: MultiIngest, parallel: bool, settings: list<mixed>}
     */
    protected function attachIngest(object $core): array
    {
        $settings = self::ingestSettings();

        $nightowlPort = (int) config('nightowl.agent.port', 2407);
        $nightowlToken = (string) config('nightowl.agent.token', config('nightwatch.token', ''));
        $tokenHash = substr(hash('xxh128', $nightowlToken), 0, 7);

        // Where THIS app transmits telemetry. Defaults to the loopback
        // listener (co-located agent) so existing single-host installs are
        // unaffected. Set NIGHTOWL_INGEST_URI to the agent's private
        // host:port to ship to a remote agent — the supported path for
        // serverless hosts (Laravel Vapor) that can't run the long-lived
        // agent in-process. A bare host with no port falls back to the
        // configured agent port. Mirrors nightwatch's NIGHTWATCH_INGEST_URI.
        $nightowlTransmitTo = trim((string) config('nightowl.agent.ingest_uri', "127.0.0.1:{$nightowlPort}"));
        if ($nightowlTransmitTo === '') {
            $nightowlTransmitTo = "127.0.0.1:{$nightowlPort}";
        }
        // Append the agent port when only a host was given (e.g.
        // NIGHTOWL_INGEST_URI=10.0.0.5). IPv6 literals must be bracketed;
        // for those the port is the ":NNNN" that FOLLOWS the closing "]",
        // not the colons inside the address — so "[::1]" (no port) still
        // gets the agent port while "[::1]:2407" passes through untouched.
        if (str_starts_with($nightowlTransmitTo, '[')) {
            $closingBracket = strpos($nightowlTransmitTo, ']');
            $hasPort = $closingBracket !== false
                && ($nightowlTransmitTo[$closingBracket + 1] ?? '') === ':';
        } else {
            // Bare host / IPv4 — any colon delimits the port.
            $hasPort = str_contains($nightowlTransmitTo, ':');
        }
        if (! $hasPort) {
            $nightowlTransmitTo .= ":{$nightowlPort}";
        }
        $nightowlTimeout = (float) config('nightowl.agent.ingest_timeout', 0.5);

        $parallel = (bool) config('nightowl.parallel_with_nightwatch', false);

        // Nightwatch >= 1.29 asks Ingest for an event dispatcher: before
        // each transmit it fires IngestingEvents and drops the records if a
        // listener returns false (their per-app ingest limiter). Alone, our
        // ingest IS the application's ingest, so it gets the application's
        // dispatcher and the hook fires exactly as it would for Nightwatch.
        // In parallel mode Nightwatch's own Ingest already fires it once per
        // record set; ours gets an empty dispatcher, or every listener would
        // run twice and a limiter would count each event double. Older
        // Nightwatch releases do not accept the argument at all — see
        // NightwatchIngestArguments.
        $nightowlIngest = new Ingest(...NightwatchIngestArguments::complete(Ingest::class, [
            'transmitTo' => $nightowlTransmitTo,
            'connectionTimeout' => $nightowlTimeout,
            'timeout' => $nightowlTimeout,
            'streamFactory' => new SocketStreamFactory,
            'buffer' => new RecordsBuffer(length: 500),
            'tokenHash' => $tokenHash,
        ], fn () => $parallel
            ? new Dispatcher($this->app)
            : $this->app->make(DispatcherContract::class)));

        $nightowlIngest = $this->wirePayloadCapture($core, $nightowlIngest);

        // Both modes go through MultiIngest — single-agent mode wraps a lone
        // ingest purely for its fail-open write path. A monitoring package
        // must never take the host application down: Nightwatch's own hooks
        // guard every call site they own, but that is their guarantee to
        // maintain, not ours to inherit. Anything that reaches
        // Core::$ingest through a path they haven't wrapped (a customer
        // calling Nightwatch::report(), a future hook, an older SDK) would
        // otherwise surface an unreachable-agent socket error inside an
        // unrelated customer request. (Reported by @TheDaveKent, #4.)
        $wired = $parallel
            ? new MultiIngest($core->ingest, $nightowlIngest)
            : new MultiIngest($nightowlIngest);

        $previous = $core->ingest;

        $this->handOverBufferedRecords($previous, $nightowlIngest, $wired, $parallel);

        $core->ingest = $wired;

        return [
            'core' => $core,
            'previous' => $previous,
            'nightowl' => $nightowlIngest,
            'wired' => $wired,
            'parallel' => $parallel,
            'settings' => $settings,
        ];
    }

    /**
     * Give NightOwl the records the SDK buffered before the swap.
     *
     * These are records written before the swap: in a provider's register(),
     * since the swap runs at the start of boot, or anywhere in boot when this
     * provider was registered after the application booted and swapped in
     * booted(). Alone, they are moved: the SDK's ingest is dropped from Core
     * and would never send them. In parallel mode they are copied, and
     * Nightwatch still sends its own. When MultiIngest collapsed NightOwl's
     * ingest into the SDK's (both aimed at one agent with one token), that one
     * send already covers NightOwl and nothing is copied.
     *
     * RecordsBuffer is @internal to Nightwatch, and its all() does not exist on
     * 1.26, so every step is feature-checked and guarded: an SDK without them
     * misses those few records, as every version did before this existed, and
     * never raises an error in the host application.
     */
    protected function handOverBufferedRecords(mixed $previous, IngestContract $nightowlIngest, MultiIngest $wired, bool $parallel): void
    {
        try {
            if (! $previous instanceof Ingest || ! $wired->contains($nightowlIngest)) {
                return;
            }

            $records = self::bufferedRecords($previous);
            if ($records === []) {
                return;
            }

            // Through MultiIngest for its fail-open write: a full buffer
            // digests, and an unreachable agent must not surface here.
            $sink = new MultiIngest($nightowlIngest);
            foreach ($records as $record) {
                if (is_array($record)) {
                    $sink->write($record);
                }
            }

            if (! $parallel) {
                $previous->flush();
            }
        } catch (\Throwable $e) {
            error_log('[NightOwl Agent] records buffered before NightOwl attached to Nightwatch were not handed over: '.$e->getMessage());
        }
    }

    /**
     * Wrap the NightOwl-bound ingest with request-payload / response-body
     * capture when `nightowl.capture` asks for it (see PayloadCapture).
     *
     * Runs during the host application's boot, so it fails OPEN: if the
     * installed Nightwatch lacks a hook this relies on, capture is skipped with
     * a log line and the ingest is returned as it came. A monitoring setting
     * must never be the reason an application does not boot.
     */
    protected function wirePayloadCapture(object $core, IngestContract $ingest): IngestContract
    {
        try {
            $capture = PayloadCapture::fromConfig(
                (array) config('nightowl.capture', []),
                (array) config('nightwatch.redact_payload_fields', ['_token', 'password', 'password_confirmation']),
            );
            if ($capture === null) {
                return $ingest;
            }

            // Nightwatch runs these right before it writes the request record
            // (CapturesState::request), which is what pairs the two exactly.
            // A request Nightwatch sampled out is still built and written, then
            // flushed unsent — so skip it here, or every discarded request would
            // pay for a response-body encode.
            $core->redactRequests(static function ($record) use ($capture, $core): void {
                if ($record instanceof RequestRecord && (! method_exists($core, 'sampling') || $core->sampling())) {
                    $capture->remember($record);
                }
            });

            $this->app['events']->listen(RequestHandled::class, static function (RequestHandled $event) use ($capture): void {
                $capture->rememberResponse($event->response);
            });

            return new CapturingIngest($ingest, $capture);
        } catch (\Throwable $e) {
            error_log('[NightOwl Agent] payload capture disabled — could not hook into Nightwatch: '.$e->getMessage());

            return $ingest;
        }
    }

    /**
     * Build the update-drift watcher, or null when disabled.
     *
     * Returns null when installed.php can't be located (odd bundler setups):
     * with no baseline to compare against there is nothing to warn about.
     */
    protected function makeDriftWatcher(): ?VersionDriftWatcher
    {
        if (! config('nightowl.update_check.enabled', true)) {
            return null;
        }

        $path = InstalledVersionReader::defaultPath();
        if ($path === null) {
            return null; // silent: not locating installed.php is not an error worth a log line every boot
        }

        return new VersionDriftWatcher(
            new InstalledVersionReader($path),
            (float) config('nightowl.update_check.poll_seconds', 300),
        );
    }

    public function boot(): void
    {
        $this->registerConfig();
        $this->registerCommands();
        $this->registerDatabaseConnection();
        $this->registerMigrations();
    }

    protected function registerConfig(): void
    {
        $this->publishes([
            __DIR__.'/../config/nightowl.php' => config_path('nightowl.php'),
        ], 'nightowl-config');
    }

    /**
     * Build the drain-worker respawn strategy for the async server.
     *
     * Returns a closure that pcntl_exec()s a fresh `php artisan nightowl:drain-worker`
     * so the drain worker runs in a clean interpreter, isolated from the parent
     * agent's long-lived ReactPHP state (event-loop FDs, signal handlers, sockets
     * opened for health reporting) — sound hygiene for a long-running forking
     * process versus continuing to run PHP in a bare fork() child.
     *
     * Returns null when exec can't be used (missing pcntl_exec, or no artisan
     * entrypoint — e.g. some embedded/Octane contexts), so AsyncServer falls back
     * to the in-process fork. Inherited env vars carry NIGHTOWL_DB_* to the new
     * process; the sqlite path is passed explicitly because AgentCommand may set
     * it via a runtime config() mutation that exec would not inherit.
     */
    protected function makeDrainSpawner($app): ?\Closure
    {
        if (! function_exists('pcntl_exec')) {
            return null;
        }

        $artisan = $app->basePath('artisan');
        if (! is_file($artisan)) {
            return null;
        }

        return function (int $workerId, int $totalWorkers, string $sqlitePath) use ($artisan): void {
            pcntl_exec(PHP_BINARY, [
                $artisan,
                'nightowl:drain-worker',
                '--worker-id='.$workerId,
                '--total-workers='.$totalWorkers,
                '--sqlite-path='.$sqlitePath,
            ]);
            // Only returns on failure; AsyncServer logs and falls back in-process.
        };
    }

    protected function registerCommands(): void
    {
        if ($this->app->runningInConsole()) {
            $commands = [
                AgentCommand::class,
                DrainWorkerCommand::class,
                InstallCommand::class,
                MigrateCommand::class,
                PruneCommand::class,
                PartitionCommand::class,
                DropV1HistogramsCommand::class,
                BackfillRollupsCommand::class,
                BackfillLogContextCommand::class,
                RepairCacheRollupKeysCommand::class,
                ClearCommand::class,
                GcDictRoutesCommand::class,
                GcDictTracesCommand::class,
                TestAlertCommand::class,
            ];

            // The synthetic-traffic feeder commands (simulator-loop/backfill) live in
            // the SEPARATE nightowl/agent-simulator package — never shipped to a customer
            // install — and are registered by ITS service provider when present + enabled.

            $this->commands($commands);
        }
    }

    protected function registerDatabaseConnection(): void
    {
        $connection = [
            'driver' => 'pgsql',
            'host' => config('nightowl.database.host', '127.0.0.1'),
            'port' => config('nightowl.database.port', 5432),
            'database' => config('nightowl.database.database', 'nightowl'),
            'username' => config('nightowl.database.username', 'nightowl'),
            'password' => config('nightowl.database.password', 'nightowl'),
            'charset' => 'utf8',
            'prefix' => '',
            'schema' => 'public',
        ];

        // NIGHTOWL_DB_SSLMODE when it is set, so install, migrate and the
        // other commands connect the way the drain does. Left out when it is
        // not, so libpq applies PGSSLMODE or its own default, as it always did
        // for these commands. See DatabaseSslMode.
        $sslmode = DatabaseSslMode::explicit();
        if ($sslmode !== null) {
            $connection['sslmode'] = $sslmode;
        }

        $this->app['config']->set('database.connections.nightowl', $connection);
    }

    protected function registerMigrations(): void
    {
        // When disabled, don't register the migrations so the host app's
        // `php artisan migrate` (and RefreshDatabase in tests) doesn't try to
        // create the nightowl_* tables on a connection that may not exist.
        // `nightowl:install` loads them explicitly via --path regardless.
        if (! config('nightowl.enabled', true)) {
            return;
        }

        // The schema is managed by `nightowl:install` / `nightowl:migrate`,
        // which track history inside the nightowl database (idempotent across
        // environments). Registering the migrations with the host app's
        // `php artisan migrate` is opt-in legacy behavior — it tracks history in
        // the PRIMARY database and must not be combined with those commands, so
        // it's off by default.
        if (! config('nightowl.run_migrations', false)) {
            return;
        }

        $this->loadMigrationsFrom(__DIR__.'/../database/migrations');
    }
}
