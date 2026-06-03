<?php

namespace NightOwl;

use Illuminate\Support\ServiceProvider;
use Laravel\Nightwatch\Core;
use Laravel\Nightwatch\Ingest;
use Laravel\Nightwatch\RecordsBuffer;
use Laravel\Nightwatch\SocketStreamFactory;
use NightOwl\Agent\AsyncServer;
use NightOwl\Agent\ConnectionHandler;
use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\PayloadParser;
use NightOwl\Agent\RecordWriter;
use NightOwl\Agent\Server;
use NightOwl\Commands\AgentCommand;
use NightOwl\Commands\ClearCommand;
use NightOwl\Commands\DrainWorkerCommand;
use NightOwl\Commands\InstallCommand;
use NightOwl\Commands\PruneCommand;
use NightOwl\Support\MultiIngest;

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
            );
        });

        $this->app->booted(function () {
            if (! $this->app->bound(Core::class)) {
                return;
            }

            // Idempotent: this hook can fire more than once (provider re-registered,
            // Octane reset, etc). Re-wrapping $core->ingest with MultiIngest each time
            // multiplies outbound writes — N wraps = each record sent to the NightOwl
            // agent N times.
            if ($this->app->bound('nightowl.ingest_wired')) {
                return;
            }
            $this->app->instance('nightowl.ingest_wired', true);

            $core = $this->app->make(Core::class);

            $nightowlPort = (int) config('nightowl.agent.port', 2407);
            $nightowlToken = (string) config('nightowl.agent.token', config('nightwatch.token', ''));
            $tokenHash = substr(hash('xxh128', $nightowlToken), 0, 7);

            $nightowlIngest = new Ingest(
                transmitTo: "127.0.0.1:{$nightowlPort}",
                connectionTimeout: 0.5,
                timeout: 0.5,
                streamFactory: new SocketStreamFactory,
                buffer: new RecordsBuffer(length: 500),
                tokenHash: $tokenHash,
            );

            if (config('nightowl.parallel_with_nightwatch', false)) {
                $core->ingest = new MultiIngest($core->ingest, $nightowlIngest);
            } else {
                $core->ingest = $nightowlIngest;
            }
        });

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
                drainSpawner: $this->makeDrainSpawner($app),
            );
        });
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
            $this->commands([
                AgentCommand::class,
                DrainWorkerCommand::class,
                InstallCommand::class,
                PruneCommand::class,
                ClearCommand::class,
            ]);
        }
    }

    protected function registerDatabaseConnection(): void
    {
        $this->app['config']->set('database.connections.nightowl', [
            'driver' => 'pgsql',
            'host' => config('nightowl.database.host', '127.0.0.1'),
            'port' => config('nightowl.database.port', 5432),
            'database' => config('nightowl.database.database', 'nightowl'),
            'username' => config('nightowl.database.username', 'nightowl'),
            'password' => config('nightowl.database.password', 'nightowl'),
            'charset' => 'utf8',
            'prefix' => '',
            'schema' => 'public',
        ]);
    }

    protected function registerMigrations(): void
    {
        $this->loadMigrationsFrom(__DIR__.'/../database/migrations');
    }
}
