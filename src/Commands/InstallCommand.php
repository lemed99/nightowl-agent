<?php

namespace NightOwl\Commands;

use Illuminate\Console\Command;
use PDO;

class InstallCommand extends Command
{
    protected $signature = 'nightowl:install';

    protected $description = 'Install NightOwl: publish config, run migrations, verify fork safety';

    public function handle(): int
    {
        $this->info('Installing NightOwl...');

        // 1. Publish config
        $this->callSilent('vendor:publish', [
            '--tag' => 'nightowl-config',
        ]);
        $this->line('  Published config/nightowl.php');

        // 2. Run migrations. We point --path explicitly at the package's
        // migrations rather than relying on the service provider's
        // loadMigrationsFrom(): when NIGHTOWL_ENABLED=false the provider does
        // not register the path, but `nightowl:install` is an explicit opt-in,
        // so it must run them regardless of the master switch.
        //
        // We deliberately do NOT pass --database=nightowl: each migration
        // declares `protected $connection = 'nightowl'` and uses
        // Schema::connection(...), so the schema lands in the customer's nightowl
        // database either way. Passing --database would also record the migration
        // history in the nightowl DB, so the app's normal `php artisan migrate`
        // (which tracks against the primary connection) wouldn't see these as run
        // and would try to re-create the tables — "table already exists" on deploy.
        $this->call('migrate', [
            '--path' => realpath(__DIR__.'/../../database/migrations'),
            '--realpath' => true,
        ]);
        $this->line('  Ran migrations');

        // 3. Fork-safety probe — catches PHP builds / filesystems where the
        // SQLite WAL + pcntl_fork pattern the agent depends on doesn't work,
        // before someone hits it at 2am in production.
        if (! $this->verifyForkSafety()) {
            return self::FAILURE;
        }

        $this->newLine();
        $this->info('NightOwl installed successfully!');
        $this->newLine();
        $this->line('Next step:');
        $this->line('  - Start the agent: <comment>php artisan nightowl:agent</comment>');

        return self::SUCCESS;
    }

    /**
     * Exercise the agent's runtime hazard: parent + child both writing to the
     * same SQLite WAL file under contention, then verify integrity. A simple
     * fork-and-write smoke test would miss the failure mode that actually
     * bites — silent journal corruption that surfaces hours later.
     *
     * Returns false on failure; caller should abort install loudly so the
     * customer doesn't start the daemon and silently degrade.
     */
    private function verifyForkSafety(): bool
    {
        if (! function_exists('pcntl_fork') || ! function_exists('pcntl_waitpid')) {
            $this->line('  <comment>Skipped fork-safety probe (pcntl unavailable — only the --driver=sync agent will work)</comment>');

            return true;
        }

        $path = sys_get_temp_dir().'/nightowl-fork-probe-'.getmypid().'.sqlite';
        $this->cleanupProbeFiles($path);

        try {
            $pdo = $this->openProbeDb($path);
            $pdo->exec('CREATE TABLE probe (id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL)');
            $pdo = null;

            $pid = pcntl_fork();
            if ($pid === -1) {
                throw new \RuntimeException('pcntl_fork() returned -1');
            }

            if ($pid === 0) {
                exit($this->probeWriteLoop($path, 'child') ? 0 : 1);
            }

            // Parent writes concurrently with the child — busy_timeout
            // serialises them at the SQLite writer lock, exercising the
            // contention path the real drain workers hit.
            $parentOk = $this->probeWriteLoop($path, 'parent');

            pcntl_waitpid($pid, $status);
            $childOk = pcntl_wifexited($status) && pcntl_wexitstatus($status) === 0;

            if (! $parentOk) {
                throw new \RuntimeException('parent write loop failed under contention');
            }
            if (! $childOk) {
                throw new \RuntimeException('child write loop failed under contention');
            }

            $pdo = $this->openProbeDb($path);
            $integrity = $pdo->query('PRAGMA integrity_check')->fetchColumn();
            if ($integrity !== 'ok') {
                throw new \RuntimeException("PRAGMA integrity_check returned: {$integrity}");
            }

            $counts = $pdo->query("SELECT source, COUNT(*) AS n FROM probe GROUP BY source")
                ->fetchAll(PDO::FETCH_KEY_PAIR);
            $pdo = null;

            $parentRows = (int) ($counts['parent'] ?? 0);
            $childRows = (int) ($counts['child'] ?? 0);

            if ($parentRows === 0 || $childRows === 0) {
                throw new \RuntimeException("missing rows under contention (parent={$parentRows}, child={$childRows})");
            }

            $this->line(sprintf(
                '  Verified concurrent SQLite WAL writes (%d parent + %d child rows, integrity ok)',
                $parentRows,
                $childRows
            ));

            return true;
        } catch (\Throwable $e) {
            $this->newLine();
            $this->error('Fork-safety probe failed: '.$e->getMessage());
            $this->line('  The agent needs pcntl_fork + SQLite WAL to work correctly together.');
            $this->line('  Check that PHP was built with --enable-pcntl, that disable_functions does not strip pcntl_*,');
            $this->line('  and that the buffer path is on a local filesystem (not NFS / network-mounted).');
            $this->line('  You can still run the agent with <comment>--driver=sync</comment> (single-process fallback).');

            return false;
        } finally {
            $this->cleanupProbeFiles($path);
        }
    }

    /**
     * Tight insert loop for the probe — bounded by wall time, not row count,
     * so slow machines and shared CI runners don't artificially fail. Treats
     * "database is locked" as expected (busy_timeout will retry).
     */
    private function probeWriteLoop(string $path, string $source): bool
    {
        try {
            $pdo = $this->openProbeDb($path);
            $stmt = $pdo->prepare('INSERT INTO probe (source) VALUES (:s)');
            $deadline = microtime(true) + 1.0;
            do {
                $stmt->execute([':s' => $source]);
                usleep(500); // yield so the other process can win the writer lock
            } while (microtime(true) < $deadline);

            return true;
        } catch (\Throwable) {
            return false;
        }
    }

    private function openProbeDb(string $path): PDO
    {
        $pdo = new PDO("sqlite:{$path}");
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->exec('PRAGMA busy_timeout=5000');
        $pdo->exec('PRAGMA journal_mode=WAL');

        return $pdo;
    }

    private function cleanupProbeFiles(string $path): void
    {
        foreach ([$path, $path.'-wal', $path.'-shm'] as $file) {
            if (file_exists($file)) {
                @unlink($file);
            }
        }
    }
}
