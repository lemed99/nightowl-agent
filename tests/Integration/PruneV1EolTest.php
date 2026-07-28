<?php

namespace NightOwl\Tests\Integration;

use Illuminate\Config\Repository;
use Illuminate\Database\DatabaseServiceProvider;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use NightOwl\Commands\PruneCommand;
use PDO;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * The prune-integrated v1 end-of-life step and its four gates. The actual
 * DROP is exercised against nightowl_mail only (smallest blast radius);
 * tearDown restores a column-compatible plain table from a pre-drop shape
 * backup so the rest of the suite keeps its writable v1 mail table.
 */
class PruneV1EolTest extends TestCase
{
    private static ?PDO $pdo = null;

    private static string $host;

    private static int $port;

    private static string $database;

    private static string $username;

    private static string $password;

    private Application $app;

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

    /**
     * Hand the shared test DB back with its REAL schema, not a column-compatible
     * stand-in.
     *
     * tearDown's `LIKE` restore is enough for this class (it only needs a
     * writable mail table between methods, and method order is not fixed —
     * executionOrder="defects" can run the retiring test first), but a `LIKE`
     * clone is a PLAIN table: it drops the partitioning the migrations built.
     * PartitioningTest asserts that no unpartitioned raw table holds rows, so it
     * failed or passed purely on class order — nightowl_mail arrived either as
     * the migrations' partitioned parent or as this class's flat copy of it.
     *
     * A replay is the only equivalent restore: nothing here knows the shape,
     * and MigrationRunner's warm-DB probe cannot detect the loss (see
     * MigrationRunner::rebuild).
     */
    public static function tearDownAfterClass(): void
    {
        if (self::$pdo !== null) {
            MigrationRunner::rebuild(self::$host, self::$port, self::$database, self::$username, self::$password);
        }

        self::$pdo = null;
    }

    protected function setUp(): void
    {
        if (self::$pdo === null) {
            $this->markTestSkipped('PostgreSQL not available. Set NIGHTOWL_TEST_DB_* env vars.');
        }

        $this->app = new Application(sys_get_temp_dir().'/nightowl-prune-eol-test');
        $this->app->singleton('config', fn () => new Repository([
            'database' => [
                'default' => 'nightowl',
                'connections' => ['nightowl' => [
                    'driver' => 'pgsql',
                    'host' => self::$host,
                    'port' => self::$port,
                    'database' => self::$database,
                    'username' => self::$username,
                    'password' => self::$password,
                    'charset' => 'utf8',
                ]],
            ],
        ]));
        (new DatabaseServiceProvider($this->app))->register();
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication($this->app);
    }

    protected function tearDown(): void
    {
        // Restore ANY v1 table the EOL retired (it drops every qualifying
        // empty table, not just the one under test) and reset the fence.
        if (self::$pdo !== null) {
            foreach (array_keys(\NightOwl\Support\StorageV2::TABLES) as $v1) {
                $missing = ! (bool) self::$pdo->query(
                    "SELECT to_regclass('public.{$v1}') IS NOT NULL AS e"
                )->fetchColumn();
                $backup = "__{$v1}_shape_backup";
                $backupExists = (bool) self::$pdo->query(
                    "SELECT to_regclass('public.{$backup}') IS NOT NULL AS e"
                )->fetchColumn();

                if ($missing && $backupExists) {
                    self::$pdo->exec("ALTER TABLE {$backup} RENAME TO {$v1}");
                    // The shape backup carried no id default; give the
                    // restored table a self-contained identity so later
                    // suites can insert into it.
                    self::$pdo->exec("ALTER TABLE {$v1} ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY");
                } elseif ($backupExists) {
                    self::$pdo->exec("DROP TABLE {$backup}");
                }
            }

            self::$pdo->exec(
                "INSERT INTO nightowl_settings (key, value, created_at, updated_at)
                 VALUES ('v2_fence', to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'), now(), now())
                 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
            );
        }

        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);
    }

    private string $lastPruneOutput = '';

    private function prune(array $args = []): void
    {
        $command = new PruneCommand;
        $command->setLaravel($this->app);
        $output = new BufferedOutput;
        $command->run(new ArrayInput($args + ['--days' => '14']), $output);
        $this->lastPruneOutput = $output->fetch();
    }

    private function setFence(string $timestamp): void
    {
        // Upsert: other suites' cleanup loops truncate nightowl_settings, so
        // the migration-time fence row may be gone by the time we run.
        self::$pdo->exec(
            "INSERT INTO nightowl_settings (key, value, created_at, updated_at)
             VALUES ('v2_fence', '{$timestamp}', now(), now())
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
        );
    }

    private function mailExists(): bool
    {
        return (bool) self::$pdo->query(
            "SELECT to_regclass('public.nightowl_mail') IS NOT NULL AS e"
        )->fetchColumn();
    }

    /** Column-shape backups for every v1 table the EOL might retire. */
    private function backupAllShapes(): void
    {
        foreach (array_keys(\NightOwl\Support\StorageV2::TABLES) as $v1) {
            $exists = (bool) self::$pdo->query(
                "SELECT to_regclass('public.{$v1}') IS NOT NULL AS e"
            )->fetchColumn();
            if (! $exists) {
                continue;
            }
            self::$pdo->exec("DROP TABLE IF EXISTS __{$v1}_shape_backup");
            // Columns/NOT NULLs only — INCLUDING DEFAULTS would copy the
            // nextval() default and make the backup depend on the source's
            // id sequence, blocking the very DROP under test with 2BP01.
            self::$pdo->exec("CREATE TABLE __{$v1}_shape_backup (LIKE {$v1})");
        }
    }

    public function test_young_fence_keeps_v1(): void
    {
        self::$pdo->exec('DELETE FROM nightowl_mail');
        $this->setFence(gmdate('Y-m-d H:i:s', time() - 86400)); // 1 day old < 14d retention

        $this->prune();

        $this->assertTrue($this->mailExists(), 'a fence younger than retention must never retire v1');
    }

    public function test_populated_v1_is_kept_even_with_old_fence(): void
    {
        // Gate 4 = the mixed-fleet guard: rows in v1 mean an old agent may
        // still be draining there.
        self::$pdo->exec("INSERT INTO nightowl_mail (trace_id, mailer, created_at, environment)
                          VALUES ('eol-guard', 'smtp', now(), 'production')");
        $this->setFence(gmdate('Y-m-d H:i:s', time() - 40 * 86400));

        $this->prune(['--hours' => '1']); // aggressive retention still can't touch the fresh row

        $this->assertTrue($this->mailExists(), 'a v1 table with rows must never be retired');
        self::$pdo->exec("DELETE FROM nightowl_mail WHERE trace_id = 'eol-guard'");
    }

    public function test_keep_v1_flag_blocks_retirement(): void
    {
        self::$pdo->exec('DELETE FROM nightowl_mail');
        $this->setFence(gmdate('Y-m-d H:i:s', time() - 40 * 86400));

        $this->prune(['--keep-v1' => true]);

        $this->assertTrue($this->mailExists(), '--keep-v1 is the operator escape hatch');
    }

    public function test_old_fence_and_empty_v1_retires_the_table(): void
    {
        $this->backupAllShapes();
        self::$pdo->exec('DELETE FROM nightowl_mail');
        $this->setFence(gmdate('Y-m-d H:i:s', time() - 40 * 86400));

        $this->prune();

        $this->assertFalse($this->mailExists(),
            "all four gates open → v1 retired. Prune output:\n{$this->lastPruneOutput}");
        $this->assertTrue((bool) self::$pdo->query(
            "SELECT to_regclass('public.nightowl_mail_v2') IS NOT NULL AS e"
        )->fetchColumn(), 'the v2 twin must survive');
    }
}
