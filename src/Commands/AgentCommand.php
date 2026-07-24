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

    /** Shared tail of every "schema is behind" operator message — one string so the remediation advice can't drift between branches. */
    private const SCHEMA_BEHIND_ADVICE = 'The agent will keep running, but some writes may fail until the schema is updated.';

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

        $this->syncOrWarnSchema();

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
     * Bring the NightOwl schema up at startup, or warn loudly when we can't.
     *
     * In the default (DB-history) model the schema is applied by
     * `php artisan nightowl:migrate`, which is NOT wired into the host app's
     * `php artisan migrate`. With auto-restart, a `composer update` restarts
     * the daemon into new code — this hook is what makes the schema come up
     * WITH that code instead of relying on the operator remembering a second
     * command. Auto-migrate (default on, `NIGHTOWL_AUTO_MIGRATE=false` for
     * DB roles without DDL rights) runs migrate `--no-backfill` synchronously —
     * in a child process under a hard deadline (runBootMigrate) so a lock wait
     * can never keep the ingest port unbound, and crucially BEFORE the drain
     * children fork, because RecordWriter caches rollup-table existence per
     * process — then hands the potentially long rollup backfill to a detached
     * background process (retried each boot via a completion marker).
     *
     * Failure policy is warn-and-continue, matching the pre-auto-migrate
     * behavior: a behind schema means some writes fail — an accepted running
     * state — unlike guardBufferWritable(), which blocks because continuing
     * would corrupt. Skipped under legacy ride-along
     * (`NIGHTOWL_RUN_MIGRATIONS=true`), where the host's `php artisan migrate`
     * owns the schema and a second history would conflict.
     */
    private function syncOrWarnSchema(): void
    {
        if (config('nightowl.run_migrations', false)) {
            return;
        }

        $pending = $this->pendingNightowlMigrations();

        if ($pending !== null && $pending !== []) {
            if (config('nightowl.auto_migrate', true)) {
                $this->autoMigrate(count($pending));
            } else {
                $message = sprintf(
                    'NightOwl schema is behind: %d migration(s) not applied to the nightowl database. '
                    .'Run `php artisan nightowl:migrate`. ',
                    count($pending),
                ).self::SCHEMA_BEHIND_ADVICE;

                error_log('[NightOwl Agent] '.$message);
                $this->warn($message);
            }
        } elseif ($pending !== null && config('nightowl.auto_migrate', true) && is_file($this->backfillMarkerPath())) {
            // A previous boot's detached backfill never completed (its marker
            // is still on disk): the rollup tables that boot created may still
            // be existing-but-empty — and the API read path prefers any rollup
            // table that EXISTS over raw, so those serve zeroed charts. With
            // zero pending migrations nothing else would ever retry, so the
            // marker is what keeps re-spawning the backfill until one finishes.
            $this->line('A previous rollup backfill did not complete — retrying in the background.');
            $this->spawnBackgroundBackfill();
        }

        $this->warnOnUnpartitionedTables();
    }

    /**
     * Package migrations not yet applied to the nightowl database.
     *
     * null = cannot tell (uninitialised schema — the onboarding path owns that
     * — or a transient DB error, swallowed so it never blocks the agent).
     *
     * A migration counts as applied if it's recorded in EITHER the nightowl
     * database (the DB-history model) or the host app's primary history (legacy
     * ride-along / old install), so a legacy install that's fallen behind is
     * still caught. An empty applied set everywhere is deliberately not treated
     * as drift — that's an untracked-but-present schema, not a known gap.
     */
    private function pendingNightowlMigrations(): ?array
    {
        try {
            if (! Schema::connection('nightowl')->hasTable('nightowl_requests')) {
                return null; // not initialised — handled by the onboarding path, not drift
            }

            $repository = app('migrator')->getRepository();
            $repository->setSource('nightowl');
            $nightowlHistory = $repository->repositoryExists() ? $repository->getRan() : [];

            $all = collect(glob(realpath(__DIR__.'/../../database/migrations').'/*.php'))
                ->map(fn (string $file) => basename($file, '.php'))
                ->all();

            $applied = MigrateCommand::appliedSet($all, $nightowlHistory, MigrateCommand::primaryHistory());

            if (! MigrateCommand::isBehind($all, $applied)) {
                return [];
            }

            return MigrateCommand::pendingMigrations($all, $applied);
        } catch (\Throwable $e) {
            error_log('[NightOwl Agent] Schema drift check skipped: '.$e->getMessage());

            return null;
        }
    }

    /**
     * Run the schema half of nightowl:migrate synchronously (bounded — see
     * runBootMigrate), then detach the backfill half. Warn-and-continue on any
     * failure (see syncOrWarnSchema).
     */
    private function autoMigrate(int $pendingCount): void
    {
        $this->info(sprintf(
            'NightOwl schema is behind by %d migration(s) — running nightowl:migrate before starting.',
            $pendingCount,
        ));

        try {
            $exit = $this->runBootMigrate();
        } catch (\Throwable $e) {
            $exit = self::FAILURE;
            error_log('[NightOwl Agent] Boot migrate threw: '.$e->getMessage());
        }

        if ($exit !== self::SUCCESS) {
            $message = 'Boot migrate failed — run `php artisan nightowl:migrate` manually. '
                .self::SCHEMA_BEHIND_ADVICE;
            error_log('[NightOwl Agent] '.$message);
            $this->warn($message);

            return;
        }

        $this->spawnBackgroundBackfill();
    }

    /**
     * `nightowl:migrate --no-backfill` in a child process under a hard deadline.
     *
     * An in-process `$this->call()` would break the guarantee the old warn-only
     * path gave (startup never blocks on tenant-DB state): migration DDL can
     * wait INDEFINITELY on a lock — e.g. an orphaned idle-in-transaction
     * session behind a pooler holding out against an ALTER TABLE — and that
     * wait would sit BEFORE the ingest port binds, with every Nightwatch
     * client dropping telemetry after its 0.5s timeout while the supervisor
     * sees a healthy "running" process. A child process gives us a kill
     * switch: on deadline we SIGTERM it (aborting the lock wait server-side)
     * and start ingesting against the old schema — warn-and-continue, the
     * pre-1.5.0 contract. The migrations stay pending, so the next boot
     * retries.
     */
    private function runBootMigrate(): int
    {
        $artisan = base_path('artisan');
        if (! is_file($artisan)) {
            // Non-standard bootstrap (harness) — nothing to exec; the
            // in-process call is unbounded, accepted for this rare case.
            return (int) $this->call('nightowl:migrate', ['--no-backfill' => true]);
        }

        $timeout = max(30, (int) config('nightowl.auto_migrate_timeout', 300));

        $cmd = sprintf(
            '%s %s nightowl:migrate --no-backfill 2>&1',
            escapeshellarg(PHP_BINARY),
            escapeshellarg($artisan),
        );

        $process = proc_open($cmd, [1 => ['pipe', 'w']], $pipes);
        if (! is_resource($process)) {
            error_log('[NightOwl Agent] Boot migrate could not be spawned — run `php artisan nightowl:migrate` manually.');

            return self::FAILURE;
        }

        // Non-blocking read: migrate output can exceed the pipe buffer, and a
        // full pipe would block the child — the exact hang this bounds.
        stream_set_blocking($pipes[1], false);
        $output = '';
        $deadline = microtime(true) + $timeout;

        while (true) {
            $output .= (string) stream_get_contents($pipes[1]);
            $status = proc_get_status($process);

            if (! $status['running']) {
                $output .= (string) stream_get_contents($pipes[1]);
                fclose($pipes[1]);
                proc_close($process);
                $this->output->write($output);

                return (int) $status['exitcode'];
            }

            if (microtime(true) >= $deadline) {
                proc_terminate($process);
                usleep(500_000);
                if (proc_get_status($process)['running']) {
                    proc_terminate($process, 9); // SIGKILL — literal: the constant needs pcntl, this path doesn't
                }
                fclose($pipes[1]);
                proc_close($process);
                $this->output->write($output);

                $message = "Boot migrate exceeded {$timeout}s (blocked on a lock?) and was aborted so the agent can start "
                    .'(NIGHTOWL_AUTO_MIGRATE_TIMEOUT tunes the deadline).';
                error_log('[NightOwl Agent] '.$message);
                $this->warn($message);

                return self::FAILURE;
            }

            usleep(100_000);
        }
    }

    /** Survives until a detached backfill COMPLETES; see spawnBackgroundBackfill. */
    private function backfillMarkerPath(): string
    {
        return storage_path('nightowl/backfill-pending');
    }

    /**
     * Detached one-shot `nightowl:migrate` (full — its schema half is now a
     * history-guarded no-op; what's left is rollup backfill reconciliation).
     *
     * Backfill can run for tens of minutes on a big tenant, and running it
     * pre-listen would leave the ingest port unbound — Nightwatch clients drop
     * telemetry after their 0.5s timeout. The exists-but-empty-rollup zeros
     * window is the smaller harm and self-heals the moment backfill lands;
     * backfill is advisory-locked and commutes with live drain by design.
     *
     * Shell-backgrounded subshell → the child reparents to init: no pipes held,
     * no interaction with AsyncServer's SIGCHLD reaper (which tolerates unknown
     * PIDs anyway). A deploy pipeline running nightowl:migrate concurrently is
     * harmless — both sides are idempotent and history-guarded; worst case one
     * errors "already exists" and this path has already warn-continued.
     */
    private function spawnBackgroundBackfill(): void
    {
        try {
            $artisan = base_path('artisan');
            if (! is_file($artisan)) {
                return; // non-standard bootstrap (harness) — backfill stays manual
            }

            // Completion marker, written BEFORE the spawn and removed by the
            // subshell only after nightowl:migrate exits 0. If the backfill
            // dies mid-run (OOM, statement_timeout, tenant PG restart), the
            // marker survives and syncOrWarnSchema() re-spawns on the next
            // boot — without it, a boot with zero pending migrations would
            // never retry and the rollup tables would stay existing-but-empty
            // forever.
            $marker = $this->backfillMarkerPath();
            @mkdir(dirname($marker), 0755, true);
            @file_put_contents($marker, (string) time());

            $log = storage_path('logs/nightowl-boot-migrate.log');

            // `>` truncates: this log covers the CURRENT reconciliation only.
            // Laravel's rotation never touches it, and appending across years
            // of unattended auto-updates would grow it without bound.
            $cmd = sprintf(
                '(%s %s nightowl:migrate > %s 2>&1 && rm -f %s &)',
                escapeshellarg(PHP_BINARY),
                escapeshellarg($artisan),
                escapeshellarg($log),
                escapeshellarg($marker),
            );

            $process = proc_open($cmd, [], $pipes);
            if (is_resource($process)) {
                proc_close($process); // subshell already detached; close immediately
                $this->line('  Rollup backfill reconciliation running in the background (log: '.$log.')');
            } else {
                $message = 'Could not spawn the background rollup backfill — run `php artisan nightowl:migrate` manually (the next agent boot also retries).';
                error_log('[NightOwl Agent] '.$message);
                $this->warn($message);
            }
        } catch (\Throwable $e) {
            error_log('[NightOwl Agent] Background backfill spawn failed (run `php artisan nightowl:migrate` manually; the next agent boot retries): '.$e->getMessage());
        }
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
