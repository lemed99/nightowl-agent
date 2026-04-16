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
use NightOwl\Agent\Redactor;
use NightOwl\Agent\Sampler;
use NightOwl\Agent\Server;
use NightOwl\Commands\AgentCommand;
use NightOwl\Commands\CheckThresholdsCommand;
use NightOwl\Commands\ClearCommand;
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

        $this->app->singleton(Sampler::class, function ($app) {
            $requestRate = config('nightowl.agent.request_sample_rate');
            $commandRate = config('nightowl.agent.command_sample_rate');
            $scheduledRate = config('nightowl.agent.scheduled_task_sample_rate');

            return new Sampler(
                (float) config('nightowl.agent.sample_rate', 1.0),
                $requestRate !== null ? (float) $requestRate : null,
                $commandRate !== null ? (float) $commandRate : null,
                $scheduledRate !== null ? (float) $scheduledRate : null,
            );
        });

        $this->app->singleton(Redactor::class, function ($app) {
            return new Redactor(
                config('nightowl.agent.redact_keys', ['password', 'token', 'authorization', 'cookie', 'secret']),
                (bool) config('nightowl.agent.redact_enabled', false),
            );
        });

        $this->app->singleton(RecordWriter::class, function ($app) {
            return RecordWriter::fromConfig();
        });

        $this->app->singleton(ConnectionHandler::class, function ($app) {
            return new ConnectionHandler(
                $app->make(PayloadParser::class),
                $app->make(RecordWriter::class),
                $app->make(Sampler::class),
                $app->make(Redactor::class),
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
                config('nightowl.agent.sqlite_path', storage_path('nightowl/agent-buffer.sqlite')),
                config('nightowl.database.host', '127.0.0.1'),
                (int) config('nightowl.database.port', 5432),
                config('nightowl.database.database', 'nightowl'),
                config('nightowl.database.username', 'nightowl'),
                config('nightowl.database.password', 'nightowl'),
                (int) config('nightowl.agent.drain_batch_size', 1000),
                (int) config('nightowl.agent.drain_interval_ms', 100),
                (int) config('nightowl.agent.drain_max_wait_ms', 5000),
            );
        });

        $this->app->booted(function () {
            if (! $this->app->bound(Core::class)) {
                return;
            }

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
                $app->make(Sampler::class),
                $app->make(Redactor::class),
                config('nightowl.agent.token'),
                (int) config('nightowl.agent.max_pending_rows', 100_000),
                (int) config('nightowl.agent.max_buffer_memory', 256 * 1024 * 1024),
                (bool) config('nightowl.agent.enable_udp', false),
                (int) config('nightowl.agent.udp_port', 2408),
                (bool) config('nightowl.agent.health_enabled', true),
                (int) config('nightowl.agent.health_port', 2409),
                'https://api.usenightowl.com',
                (string) config('nightowl.agent.token', ''),
                (bool) config('nightowl.agent.health_report_enabled', true),
                (int) config('nightowl.agent.health_report_interval', 30),
                (array) config('nightowl.agent.health_report_intervals', []),
                (string) config('nightowl.database.database', 'nightowl'),
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

    protected function registerCommands(): void
    {
        if ($this->app->runningInConsole()) {
            $this->commands([
                AgentCommand::class,
                InstallCommand::class,
                PruneCommand::class,
                ClearCommand::class,
                CheckThresholdsCommand::class,
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
