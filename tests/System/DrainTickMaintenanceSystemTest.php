<?php

namespace NightOwl\Tests\System;

use NightOwl\Support\RawPartitions;
use NightOwl\Tests\Integration\MigrationRunner;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * Proves the drain's ~60s cleanup tick actually CALLS its two maintenance
 * sweeps — requires live PostgreSQL.
 *
 *   NIGHTOWL_TEST_DB_PORT=5433 vendor/bin/phpunit tests/System/DrainTickMaintenanceSystemTest.php
 *
 * Both sweeps were already covered where they are DEFINED — RecordWriterTest
 * drives healRawPartitionLeftovers() and maintainRawPartitions() directly, and
 * DrainWorkerIsolationTest reflects into maintainPartitionsIfDue(). None of that
 * touches the call sites in run(), so deleting either line from the cleanup
 * block left the whole suite green: the 61-minute-outage fix was one unguarded
 * line, and so was the hourly child sweep.
 *
 * They can't be driven in-process — run() is a `never`-returning fork loop that
 * ends in exit() — so this boots a REAL DrainWorker::run() in a subprocess
 * (checkpointIntervalSeconds: 0, so the cleanup block fires on its first
 * iteration instead of 60s later), plants the exact damage each sweep exists to
 * repair, and waits for the tick to repair it. The buffer is empty throughout:
 * nothing but the cleanup block can produce these effects.
 */
class DrainTickMaintenanceSystemTest extends TestCase
{
    private const TICK_TIMEOUT = 20.0;

    private static ?PDO $pdo = null;

    private static string $host;

    private static int $port;

    private static string $database;

    private static string $username;

    private static string $password;

    private string $workDir;

    /** @var resource|null */
    private $proc = null;

    /** @var array<int, resource> */
    private array $pipes = [];

    public static function setUpBeforeClass(): void
    {
        self::$host = getenv('NIGHTOWL_TEST_DB_HOST') ?: '127.0.0.1';
        self::$port = (int) (getenv('NIGHTOWL_TEST_DB_PORT') ?: 5432);
        self::$database = getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test';
        self::$username = getenv('NIGHTOWL_TEST_DB_USERNAME') ?: 'nightowl_test';
        self::$password = getenv('NIGHTOWL_TEST_DB_PASSWORD') ?: 'test123';

        try {
            $dsn = sprintf('pgsql:host=%s;port=%d;dbname=%s', self::$host, self::$port, self::$database);
            self::$pdo = new PDO($dsn, self::$username, self::$password);
            self::$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (\Exception) {
            self::$pdo = null;
        }

        if (self::$pdo) {
            MigrationRunner::migrate(self::$host, self::$port, self::$database, self::$username, self::$password);
        }
    }

    protected function setUp(): void
    {
        if (self::$pdo === null) {
            $this->markTestSkipped('PostgreSQL not available. Set NIGHTOWL_TEST_DB_* env vars.');
        }

        if (! function_exists('proc_open') || ! function_exists('posix_kill')) {
            $this->markTestSkipped('proc_open + posix required to run the worker in its own process.');
        }

        $this->workDir = sys_get_temp_dir().'/nightowl-drain-tick-'.getmypid().'-'.uniqid();
        mkdir($this->workDir, 0755, true);
    }

    protected function tearDown(): void
    {
        $this->stopWorker();

        if (self::$pdo !== null) {
            // Never leave a stranded boundary CHECK behind: it rejects every write
            // to that table with 23514 for every test that follows.
            self::$pdo->exec('ALTER TABLE nightowl_mail DROP CONSTRAINT IF EXISTS nightowl_mail_hist_ck');
        }

        if (isset($this->workDir) && is_dir($this->workDir)) {
            foreach (glob($this->workDir.'/*') ?: [] as $f) {
                @unlink($f);
            }
            @rmdir($this->workDir);
        }
    }

    public static function tearDownAfterClass(): void
    {
        self::$pdo = null;
    }

    /**
     * The per-tick leftover heal — DrainWorker::run()'s cleanup block.
     *
     * A nightowl:partition run SIGKILLed between VALIDATE and the swap strands
     * {t}_hist_ck on the live table, where it rejects every drained row for that
     * table with 23514 the moment its frozen boundary passes: a silent, total
     * write outage for that table. The heal used to ride the HOURLY child sweep,
     * which made the tick cadence the ceiling on that outage — up to 61 minutes.
     * Splitting it out onto the ~60s cleanup tick is the whole fix, and it is
     * exactly one call: without it the plant below survives forever.
     */
    public function test_the_cleanup_tick_heals_an_interrupted_conversions_leftovers(): void
    {
        // An ALREADY-EXPIRED boundary — the shape that is actively rejecting
        // writes, not the harmless not-yet-passed one.
        $expired = gmdate('Y-m-d 00:00:00', intdiv(time(), 86400) * 86400);
        self::$pdo->exec(
            "ALTER TABLE nightowl_mail ADD CONSTRAINT nightowl_mail_hist_ck
             CHECK (created_at IS NOT NULL AND created_at < '{$expired}') NOT VALID"
        );
        $this->assertSame(1, $this->leftoverCheckCount(), 'fixture: the leftover CHECK must be planted');

        $this->startWorker();
        $healed = $this->waitUntil(fn (): bool => $this->leftoverCheckCount() === 0);

        $this->assertTrue($healed, sprintf(
            "the drain's cleanup tick must call RecordWriter::healRawPartitionLeftovers() — nightowl_mail_hist_ck "
            ."was still on the table after %.0fs of a live drain worker, so an interrupted nightowl:partition run "
            ."keeps rejecting every row for that table with 23514 until someone notices by hand.\nWorker output:\n%s",
            self::TICK_TIMEOUT,
            $this->workerOutput(),
        ));
    }

    /**
     * The hourly child sweep — same cleanup block, same shape of gap. It was
     * reachable from a test only through a ReflectionMethod on
     * maintainPartitionsIfDue(), which proves the GATE and not the call.
     *
     * Missing future children are not a write outage: rows route to {t}_pdefault,
     * which nightowl:prune can only row-DELETE, never DROP PARTITION.
     * lastPartitionCheck starts at 0 precisely so this runs on the first tick.
     */
    public function test_the_cleanup_tick_recreates_a_missing_future_child(): void
    {
        $day = intdiv(time(), 86400) * 86400 + 3 * 86400;
        $child = RawPartitions::childName('nightowl_mail', $day);

        self::$pdo->exec("DROP TABLE IF EXISTS {$child}");
        $this->assertNull($this->regclass($child), 'fixture: the future child must be missing');

        $this->startWorker();
        $created = $this->waitUntil(fn (): bool => $this->regclass($child) !== null);

        $this->assertTrue($created, sprintf(
            "the drain's cleanup tick must call maintainPartitionsIfDue() — %s was still missing after %.0fs of a "
            ."live drain worker, so a day rollover routes every drained row to nightowl_mail_pdefault, which prune "
            ."can only row-DELETE.\nWorker output:\n%s",
            $child,
            self::TICK_TIMEOUT,
            $this->workerOutput(),
        ));
    }

    // ─── Harness ───────────────────────────────────────────────

    /**
     * Boot a real DrainWorker::run() in a fresh interpreter against the test
     * database, with an empty buffer so the cleanup block is the only thing that
     * can touch the tenant schema.
     */
    private function startWorker(): void
    {
        $harness = $this->workDir.'/drain-tick-harness.php';
        file_put_contents($harness, $this->harnessScript());

        $cmd = sprintf('exec %s %s 2>&1', escapeshellarg(PHP_BINARY), escapeshellarg($harness));
        $descriptors = [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']];
        $this->proc = proc_open($cmd, $descriptors, $this->pipes);

        $this->assertIsResource($this->proc, 'Failed to start the drain-worker harness.');
        stream_set_blocking($this->pipes[1], false);
        stream_set_blocking($this->pipes[2], false);
    }

    private function stopWorker(): void
    {
        if (! is_resource($this->proc)) {
            return;
        }

        $status = proc_get_status($this->proc);
        if ($status['running'] ?? false) {
            @posix_kill($status['pid'], SIGKILL);
        }
        foreach ($this->pipes as $pipe) {
            if (is_resource($pipe)) {
                @fclose($pipe);
            }
        }
        @proc_close($this->proc);
        $this->proc = null;
        $this->pipes = [];
    }

    /**
     * Poll a condition until it holds or the tick budget runs out. Returns
     * whether it held — the caller owns the failure message, because "the sweep
     * never ran" is the only interesting thing this file reports.
     */
    private function waitUntil(callable $condition): bool
    {
        $deadline = microtime(true) + self::TICK_TIMEOUT;

        while (microtime(true) < $deadline) {
            if ($condition()) {
                return true;
            }
            usleep(200_000);
        }

        return false;
    }

    /** Whatever the worker has written so far — its stderr is merged into stdout. */
    private function workerOutput(): string
    {
        if (! isset($this->pipes[1]) || ! is_resource($this->pipes[1])) {
            return '(no output captured)';
        }

        return trim((string) stream_get_contents($this->pipes[1])) ?: '(worker produced no output)';
    }

    private function leftoverCheckCount(): int
    {
        return (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_constraint
             WHERE conrelid = 'nightowl_mail'::regclass AND conname = 'nightowl_mail_hist_ck'"
        )->fetchColumn();
    }

    private function regclass(string $table): ?string
    {
        $found = self::$pdo->query("SELECT to_regclass('{$table}')")->fetchColumn();

        return $found === false || $found === null ? null : (string) $found;
    }

    /**
     * Standalone harness: no Laravel app, so bind a bare config repository —
     * run() reads config('nightowl.drain_connection.*') and builds an
     * AlertNotifier::fromConfig(), both of which resolve 'config' off the
     * container. Every key falls through to the same defaults a stock install
     * gets.
     */
    private function harnessScript(): string
    {
        $autoload = var_export(realpath(__DIR__.'/../../vendor/autoload.php'), true);
        $sqlite = var_export($this->workDir.'/buffer.sqlite', true);
        $host = var_export(self::$host, true);
        $database = var_export(self::$database, true);
        $username = var_export(self::$username, true);
        $password = var_export(self::$password, true);
        $port = self::$port;

        return <<<PHP
        <?php
        require {$autoload};

        use Illuminate\\Config\\Repository;
        use Illuminate\\Container\\Container;
        use NightOwl\\Agent\\DrainWorker;

        \$container = new Container();
        \$container->instance('config', new Repository([]));
        Container::setInstance(\$container);

        (new DrainWorker(
            sqlitePath: {$sqlite},
            pgHost: {$host},
            pgPort: {$port},
            pgDatabase: {$database},
            pgUsername: {$username},
            pgPassword: {$password},
            // 0 = the cleanup block runs on the FIRST loop iteration. The default
            // 60 would put the whole point of this test past any sane timeout.
            checkpointIntervalSeconds: 0,
        ))->run();
        PHP;
    }
}
