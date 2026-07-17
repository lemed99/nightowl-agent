<?php

namespace NightOwl\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Schema;
use NightOwl\Agent\AsyncServer;
use NightOwl\Agent\PortInUseException;
use NightOwl\Agent\Server;
use NightOwl\Agent\SqliteBuffer;

class AgentCommand extends Command
{
    protected $signature = 'nightowl:agent
        {--host= : The host to listen on}
        {--port= : The port to listen on}
        {--driver= : Server driver (async or sync)}
        {--sqlite-path= : SQLite buffer file path (required for multi-instance, overrides config)}';

    protected $description = 'Start the NightOwl monitoring agent';

    public function handle(): int
    {
        // The agent is a long-running daemon with its own memory back-pressure
        // system (totalBufferBytes inline check + RSS periodic check). PHP's
        // memory_limit would cause an ungraceful fatal crash that bypasses all
        // of that. Default memory_limit is 128M on some distros, which is below
        // our default max_buffer_memory (256MB).
        ini_set('memory_limit', '-1');

        $host = $this->option('host') ?? config('nightowl.agent.host', '127.0.0.1');
        $port = (int) ($this->option('port') ?? config('nightowl.agent.port', 2407));
        $driver = $this->option('driver') ?? config('nightowl.agent.driver', 'async');

        // --sqlite-path overrides config. This is critical for multi-instance
        // deployment: env vars are ignored when Laravel config is cached
        // (php artisan config:cache), but CLI options always work.
        if ($this->option('sqlite-path')) {
            config()->set('nightowl.agent.sqlite_path', $this->option('sqlite-path'));
        }

        if (! $this->guardBufferWritable()) {
            return self::FAILURE;
        }

        $this->warnOnSchemaDrift();

        if ($driver === 'async') {
            return $this->runAsync($host, $port);
        }

        return $this->runSync($host, $port);
    }

    /**
     * Refuse to start if the agent can't write its own SQLite buffer.
     *
     * markSynced() (recording drain progress) is a write to this file. If it's
     * readable but not writable — the agent running as a different user than owns
     * agent-buffer.sqlite is the classic cause, plus a full disk / account quota /
     * exhausted inodes / read-only mount — the drain commits rows to Postgres and
     * then can't mark them done, re-COPYing the same rows every loop: silent,
     * unbounded telemetry duplication. Failing loudly here turns that into an
     * obvious startup error. Constructing the buffer as the running user also
     * creates the file with the right ownership on a fresh install.
     */
    private function guardBufferWritable(): bool
    {
        $path = (string) config('nightowl.agent.sqlite_path');

        try {
            $buffer = new SqliteBuffer($path);
            $buffer->assertWritable();
            unset($buffer); // close before any fork (WAL fork-safety invariant)

            return true;
        } catch (\Throwable $e) {
            $dir = dirname($path);

            $this->error('NightOwl agent cannot write its buffer database and will not start.');
            $this->line('  Buffer: '.$path);
            $this->line('  Reason: '.$e->getMessage());
            $this->newLine();
            $this->line('Most likely one of:');
            $this->line("  • the agent is running as a different user than owns the buffer file");
            $this->line("    (check `ls -l {$dir}` — run the agent as the file's owner, or delete the buffer to recreate it)");
            $this->line('  • the disk or account quota is full, or inodes are exhausted (`df -h`, `df -i`, `quota`)');
            $this->line('  • the filesystem is mounted read-only');
            $this->newLine();
            $this->line('Starting anyway would let the drain re-send the same rows in a loop (duplicated telemetry),');
            $this->line("so the agent is stopping instead. Fix the above and restart; to reset the buffer, delete {$path}*");

            error_log('[NightOwl Agent] Refusing to start — buffer not writable at '.$path.': '.$e->getMessage());

            return false;
        }
    }

    /**
     * Warn loudly at startup if the NightOwl schema is behind the package's
     * migrations.
     *
     * In the default (DB-history) model the schema is applied by
     * `php artisan nightowl:migrate`, which is NOT wired into the host app's
     * `php artisan migrate`. If a package upgrade ships a new migration and the
     * operator forgets to run nightowl:migrate, the agent would otherwise fail
     * mid-drain on a missing column. Surfacing it here turns a silent break
     * into an obvious one.
     *
     * A migration counts as applied if it's recorded in EITHER the nightowl
     * database (the DB-history model) or the host app's primary history (legacy
     * ride-along / old install), so a legacy install that's fallen behind is
     * still caught. An empty applied set everywhere is deliberately not treated
     * as drift — that's an untracked-but-present schema, not a known gap.
     *
     * Skipped under legacy ride-along (`NIGHTOWL_RUN_MIGRATIONS=true`), where the
     * host's `php artisan migrate` keeps the schema current. Best-effort: any
     * error is swallowed so a transient DB issue never blocks the agent.
     */
    private function warnOnSchemaDrift(): void
    {
        if (config('nightowl.run_migrations', false)) {
            return;
        }

        try {
            if (! Schema::connection('nightowl')->hasTable('nightowl_requests')) {
                return; // not initialised — handled by the onboarding path, not drift
            }

            $repository = app('migrator')->getRepository();
            $repository->setSource('nightowl');
            $nightowlHistory = $repository->repositoryExists() ? $repository->getRan() : [];

            $all = collect(glob(realpath(__DIR__.'/../../database/migrations').'/*.php'))
                ->map(fn (string $file) => basename($file, '.php'))
                ->all();

            $applied = MigrateCommand::appliedSet($all, $nightowlHistory, MigrateCommand::primaryHistory());

            if (! MigrateCommand::isBehind($all, $applied)) {
                return;
            }

            $pending = MigrateCommand::pendingMigrations($all, $applied);

            $message = sprintf(
                'NightOwl schema is behind: %d migration(s) not applied to the nightowl database. '
                .'Run `php artisan nightowl:migrate`. The agent will keep running, but some writes '
                .'may fail until the schema is updated.',
                count($pending),
            );

            error_log('[NightOwl Agent] '.$message);
            $this->warn($message);
        } catch (\Throwable $e) {
            error_log('[NightOwl Agent] Schema drift check skipped: '.$e->getMessage());
        }

        $this->warnOnUnpartitionedTables();
    }

    /**
     * Startup twin of MigrateCommand::warnIfUnpartitioned — populated tables
     * never auto-partition, so surface the row-DELETE-forever state everywhere
     * an operator might look (deploy log via migrate, daemon log here, and the
     * Data Management panel in the dashboard).
     */
    private function warnOnUnpartitionedTables(): void
    {
        try {
            $tables = \NightOwl\Support\RawPartitions::unpartitionedPopulated(
                \Illuminate\Support\Facades\DB::connection('nightowl')->getPdo()
            );
        } catch (\Throwable) {
            return;
        }

        if ($tables !== []) {
            $message = sprintf(
                '%d raw table(s) unpartitioned — prune row-deletes them (disk reused, never returned). '
                .'Run `php artisan nightowl:partition` once to enable instant partition-drop pruning.',
                count($tables),
            );
            error_log('[NightOwl Agent] '.$message);
            $this->warn($message);
        }
    }

    private function runAsync(string $host, int $port): int
    {
        if (! function_exists('pcntl_fork') || ! function_exists('posix_kill')) {
            $this->error('The async driver requires the pcntl and posix PHP extensions.');
            $this->line('Run with --driver=sync to use the synchronous fallback, or install the missing extensions.');

            return self::FAILURE;
        }

        $server = app(AsyncServer::class);

        $this->info("NightOwl agent (async) listening on {$host}:{$port}");
        $this->line('SQLite buffer: ' . config('nightowl.agent.sqlite_path'));

        if (config('nightowl.agent.enable_udp', false)) {
            $this->line('UDP listener: ' . $host . ':' . config('nightowl.agent.udp_port', 2408));
        }

        if (config('nightowl.agent.health_enabled', true)) {
            $healthPort = config('nightowl.agent.health_port', 2409);
            $this->line("Health API: http://{$host}:{$healthPort}/status");
        }

        $this->line('Press Ctrl+C to stop.');

        try {
            $server->listen($host, $port);
        } catch (PortInUseException $e) {
            $this->newLine();
            $this->error($e->getMessage());

            return self::FAILURE;
        }

        $this->info('NightOwl agent stopped.');

        return self::SUCCESS;
    }

    private function runSync(string $host, int $port): int
    {
        $server = app(Server::class);

        $this->info("NightOwl agent (sync) listening on {$host}:{$port}");
        $this->line('Press Ctrl+C to stop.');

        if (function_exists('pcntl_async_signals')) {
            pcntl_async_signals(true);
            pcntl_signal(SIGINT, fn () => $server->stop());
            pcntl_signal(SIGTERM, fn () => $server->stop());
        }

        $server->listen($host, $port);

        $this->info('NightOwl agent stopped.');

        return self::SUCCESS;
    }
}
