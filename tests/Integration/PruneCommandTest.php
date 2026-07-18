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
 * The chunked raw-table trim behind nightowl:prune. The first prune after
 * nightowl:partition deletes the entire pre-conversion backlog in one go —
 * previously a single mega-DELETE that ran minutes with no output ("prune
 * gets stuck", reported from the field 2026-07-18). The trim must delete in
 * bounded statements, converge exactly on the cutoff, and leave newer rows
 * alone.
 *
 * Runs inside a dedicated Postgres schema so the suite's real nightowl_*
 * tables are untouched; tables the command sweeps that don't exist here are
 * skipped by its own guards.
 */
final class PruneCommandTest extends TestCase
{
    private const SCHEMA = 'nightowl_prune_test';

    private static ?PDO $pdo = null;

    private Application $app;

    private static function dbConfig(): array
    {
        return [
            'driver' => 'pgsql',
            'host' => getenv('NIGHTOWL_TEST_DB_HOST') ?: '127.0.0.1',
            'port' => (int) (getenv('NIGHTOWL_TEST_DB_PORT') ?: 5432),
            'database' => getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test',
            'username' => getenv('NIGHTOWL_TEST_DB_USERNAME') ?: 'nightowl_test',
            'password' => getenv('NIGHTOWL_TEST_DB_PASSWORD') ?: 'test123',
            'charset' => 'utf8',
            'search_path' => self::SCHEMA,
        ];
    }

    public static function setUpBeforeClass(): void
    {
        $c = self::dbConfig();

        try {
            self::$pdo = new PDO(
                sprintf('pgsql:host=%s;port=%d;dbname=%s', $c['host'], $c['port'], $c['database']),
                $c['username'],
                $c['password'],
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            );
        } catch (\Throwable) {
            self::$pdo = null;
        }
    }

    protected function setUp(): void
    {
        if (self::$pdo === null) {
            $this->markTestSkipped('PostgreSQL unavailable.');
        }

        self::$pdo->exec('DROP SCHEMA IF EXISTS '.self::SCHEMA.' CASCADE');
        self::$pdo->exec('CREATE SCHEMA '.self::SCHEMA);

        // The command sweeps every raw table unconditionally (a migrated
        // install always has them all), so the scratch schema needs them all.
        foreach ([
            'nightowl_requests', 'nightowl_queries', 'nightowl_exceptions', 'nightowl_commands',
            'nightowl_jobs', 'nightowl_cache_events', 'nightowl_mail', 'nightowl_notifications',
            'nightowl_outgoing_requests', 'nightowl_scheduled_tasks', 'nightowl_logs',
        ] as $table) {
            self::$pdo->exec('CREATE TABLE '.self::SCHEMA.".{$table} (
                id bigserial primary key, trace_id text, created_at timestamp
            )");
        }

        $this->app = new Application(sys_get_temp_dir().'/nightowl-prune-test');
        $this->app->singleton('config', fn () => new Repository([
            'database' => [
                'default' => 'nightowl',
                'connections' => ['nightowl' => self::dbConfig()],
            ],
            'nightowl' => [
                'database' => ['retention_days' => 14, 'rollup_retention_days' => 90],
            ],
        ]));
        (new DatabaseServiceProvider($this->app))->register();

        Facade::clearResolvedInstances();
        Facade::setFacadeApplication($this->app);
    }

    protected function tearDown(): void
    {
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);

        self::$pdo?->exec('DROP SCHEMA IF EXISTS '.self::SCHEMA.' CASCADE');
    }

    public function test_raw_trim_deletes_in_bounded_chunks_and_spares_recent_rows(): void
    {
        // 25 rows past retention + 5 inside it. --delete-chunk=10 forces the
        // loop through full, full, partial chunks (10 + 10 + 5).
        self::$pdo->exec('INSERT INTO '.self::SCHEMA.".nightowl_requests (trace_id, created_at)
            SELECT 'old-' || i, now() - interval '30 days' - (i || ' minutes')::interval
            FROM generate_series(1, 25) i");
        self::$pdo->exec('INSERT INTO '.self::SCHEMA.".nightowl_requests (trace_id, created_at)
            SELECT 'fresh-' || i, now() - (i || ' minutes')::interval
            FROM generate_series(1, 5) i");

        $command = new PruneCommand;
        $command->setLaravel($this->app);
        $output = new BufferedOutput;
        $exit = $command->run(new ArrayInput(['--delete-chunk' => '10']), $output);
        $text = $output->fetch();

        $this->assertSame(0, $exit, $text);
        $this->assertStringContainsString('nightowl_requests: 25 records deleted', $text);

        $remaining = self::$pdo->query(
            'SELECT trace_id FROM '.self::SCHEMA.'.nightowl_requests ORDER BY trace_id'
        )->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['fresh-1', 'fresh-2', 'fresh-3', 'fresh-4', 'fresh-5'], $remaining);
    }

    public function test_trim_emits_progress_heartbeats_on_long_deletes(): void
    {
        // 25 expired rows at chunk size 1 → heartbeat due every 10 chunks.
        self::$pdo->exec('INSERT INTO '.self::SCHEMA.".nightowl_requests (trace_id, created_at)
            SELECT 'old-' || i, now() - interval '30 days'
            FROM generate_series(1, 25) i");

        $command = new PruneCommand;
        $command->setLaravel($this->app);
        $output = new BufferedOutput;
        $command->run(new ArrayInput(['--delete-chunk' => '1']), $output);
        $text = $output->fetch();

        $this->assertStringContainsString('nightowl_requests: 10 records deleted so far...', $text);
        $this->assertStringContainsString('nightowl_requests: 20 records deleted so far...', $text);
    }
}
