<?php

namespace NightOwl\Commands;

use Illuminate\Console\Command;
use NightOwl\Agent\DrainWorker;

/**
 * Internal command — spawned by nightowl:agent via pcntl_exec() for each drain worker.
 * Not intended to be run directly by users.
 */
class DrainWorkerCommand extends Command
{
    protected $signature = 'nightowl:drain-worker
        {--worker-id=0 : Worker ID for multi-worker mode}
        {--total-workers=1 : Total number of drain workers}
        {--sqlite-path= : SQLite buffer file path}';

    protected $description = 'Run a NightOwl drain worker (internal — spawned by nightowl:agent)';

    protected $hidden = true;

    public function handle(): int
    {
        ini_set('memory_limit', '-1');

        $sqlitePath = $this->option('sqlite-path')
            ?? config('nightowl.agent.sqlite_path', storage_path('nightowl/agent-buffer.sqlite'));

        $workerId = (int) $this->option('worker-id');
        $totalWorkers = (int) $this->option('total-workers');

        $worker = new DrainWorker(
            sqlitePath: $sqlitePath,
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

        $worker->setWorkerConfig($workerId, $totalWorkers);
        $worker->run(); // never returns

        return self::SUCCESS;
    }
}
