<?php

namespace NightOwl\Tests\Integration;

use Illuminate\Config\Repository;
use Illuminate\Database\DatabaseServiceProvider;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use NightOwl\Commands\PruneCommand;
use NightOwl\Tests\Integration\Concerns\ReleasesAppConnections;
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
    use ReleasesAppConnections;

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
        $this->releaseAppConnections();

        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);

        self::$pdo?->exec('DROP SCHEMA IF EXISTS '.self::SCHEMA.' CASCADE');
    }

    /** nightowl_requests as a partitioned parent with two expired daily children (10 and 20 rows), created newest-first. */
    private function partitionRequests(): array
    {
        $s = self::SCHEMA;
        self::$pdo->exec("DROP TABLE {$s}.nightowl_requests");
        self::$pdo->exec("CREATE TABLE {$s}.nightowl_requests (id bigserial, trace_id text, created_at timestamp NOT NULL, PRIMARY KEY (id, created_at)) PARTITION BY RANGE (created_at)");
        self::$pdo->exec("CREATE TABLE {$s}.nightowl_requests_pdefault PARTITION OF {$s}.nightowl_requests DEFAULT");
        $children = [];
        foreach ([20 => 20, 30 => 10] as $daysAgo => $rows) {
            $day = intdiv(time(), 86400) * 86400 - $daysAgo * 86400;
            $child = 'nightowl_requests_p'.gmdate('Ymd', $day);
            self::$pdo->exec(sprintf("CREATE TABLE {$s}.{$child} PARTITION OF {$s}.nightowl_requests FOR VALUES FROM ('%s') TO ('%s')", gmdate('Y-m-d', $day), gmdate('Y-m-d', $day + 86400)));
            self::$pdo->exec(sprintf("INSERT INTO {$s}.nightowl_requests (trace_id, created_at) SELECT 'old-' || i, '%s'::timestamp + (i || ' minutes')::interval FROM generate_series(1, %d) i", gmdate('Y-m-d 00:00:00', $day), $rows));
            $children[] = $child;
        }
        // An expired row that lands in the DEFAULT child (before the oldest daily child) — row-DELETE territory.
        self::$pdo->exec("INSERT INTO {$s}.nightowl_requests (trace_id, created_at) VALUES ('old-default', now() - interval '60 days')");
        sort($children);

        return $children;
    }

    private function runPrune(): array
    {
        $command = new PruneCommand;
        $command->setLaravel($this->app);
        $output = new BufferedOutput;
        $exit = $command->run(new ArrayInput([]), $output);

        return [$exit, $output->fetch()];
    }

    private function rowsIn(string $table): int
    {
        return (int) self::$pdo->query('SELECT count(*) FROM '.self::SCHEMA.".{$table}")->fetchColumn();
    }

    /**
     * A holder on the parent (nightowl:gc-dict-routes' SHARE for its whole run)
     * leaves the WHOLE table for the next run: no sibling retries each parking
     * another ACCESS EXCLUSIVE wait in front of the parent's readers, and no
     * row-DELETE chewing through the rows the DROP would have unlinked for free
     * (a withdrawn candidate did both — it reported the children "left for the
     * next run" and deleted their rows in the same run). The other tables are
     * still pruned. The lock wait is a CEILING: this session's tighter 200 ms is
     * kept (the whole run stays well under a single 3 s wait) and survives the
     * command untouched.
     */
    public function test_a_held_parent_leaves_the_whole_table_for_the_next_run(): void
    {
        [$oldest, $newer] = $this->partitionRequests();
        self::$pdo->exec('INSERT INTO '.self::SCHEMA.".nightowl_queries (trace_id, created_at) VALUES ('q', now() - interval '30 days')");

        $conn = $this->app['db']->connection('nightowl');
        $conn->statement("SET lock_timeout = '200ms'");

        self::$pdo->exec('BEGIN');
        self::$pdo->exec('LOCK TABLE '.self::SCHEMA.'.nightowl_requests IN SHARE MODE');
        try {
            $started = microtime(true);
            [$exit, $text] = $this->runPrune();
            $elapsed = microtime(true) - $started;
        } finally {
            self::$pdo->exec('ROLLBACK');
        }

        $this->assertSame(0, $exit, $text);
        $this->assertStringContainsString("nightowl_requests: locked by another session (at partition {$oldest})", $text, 'oldest first');
        $this->assertStringNotContainsString($newer, $text, 'the first refusal settles the table: no sibling is retried');
        $this->assertStringNotContainsString('nightowl_requests: ', str_replace('nightowl_requests: locked', '', $text), 'no row-DELETE on the held table');
        $this->assertSame(31, $this->rowsIn('nightowl_requests'), 'every expired row survives: left for the next run means left');
        $this->assertSame(0, $this->rowsIn('nightowl_queries'), 'the other tables are still pruned');
        $this->assertLessThan(1.5, $elapsed, 'the session\'s 200 ms ceiling was honoured, not raised to 3 s');
        $this->assertSame('200ms', $conn->selectOne("SELECT current_setting('lock_timeout') AS v")->v, 'the session value is left exactly as found');

        // Holder gone: the next run drops both children and row-deletes the default child.
        [$exit, $text] = $this->runPrune();
        $this->assertSame(0, $exit, $text);
        $this->assertStringContainsString("dropped partition {$oldest} (10 records)", $text);
        $this->assertStringContainsString("dropped partition {$newer} (20 records)", $text);
        $this->assertStringContainsString('nightowl_requests: 1 records deleted', $text);
        $this->assertSame(0, $this->rowsIn('nightowl_requests'));
    }

    /**
     * A holder on ONE CHILD (VACUUM FULL, pg_repack, TRUNCATE) blocks the
     * pre-DROP row count as surely as the DROP. A withdrawn candidate counted
     * outside the guard, so that 55P03 aborted the entire prune.
     */
    public function test_a_held_child_skips_the_table_instead_of_aborting_the_prune(): void
    {
        [$oldest] = $this->partitionRequests();
        self::$pdo->exec('INSERT INTO '.self::SCHEMA.".nightowl_queries (trace_id, created_at) VALUES ('q', now() - interval '30 days')");
        $this->app['db']->connection('nightowl')->statement("SET lock_timeout = '200ms'");

        self::$pdo->exec('BEGIN');
        self::$pdo->exec('LOCK TABLE '.self::SCHEMA.".{$oldest} IN ACCESS EXCLUSIVE MODE");
        try {
            [$exit, $text] = $this->runPrune();
        } finally {
            self::$pdo->exec('ROLLBACK');
        }

        $this->assertSame(0, $exit, $text);
        $this->assertStringContainsString("nightowl_requests: locked by another session (at partition {$oldest})", $text);
        $this->assertSame(31, $this->rowsIn('nightowl_requests'));
        $this->assertSame(0, $this->rowsIn('nightowl_queries'), 'the prune carried on past the held table');
    }

    /** The historic partition's DROP is bounded the same way: a held parent answers "not dropped", not a queued wait. */
    public function test_a_held_parent_defers_the_historic_drop(): void
    {
        $this->partitionRequests();
        $s = self::SCHEMA;
        self::$pdo->exec("CREATE TABLE {$s}.nightowl_requests_phistoric PARTITION OF {$s}.nightowl_requests FOR VALUES FROM ('2000-01-01') TO ('2000-01-02')");
        $pdo = $this->app['db']->connection('nightowl')->getPdo();
        $pdo->exec("SET lock_timeout = '200ms'");

        self::$pdo->exec('BEGIN');
        self::$pdo->exec("LOCK TABLE {$s}.nightowl_requests IN SHARE MODE");
        try {
            $this->assertFalse(\NightOwl\Support\RawPartitions::dropEmptyHistoric($pdo, 'nightowl_requests'));
        } finally {
            self::$pdo->exec('ROLLBACK');
        }
        $this->assertFalse($pdo->inTransaction(), 'the refused DROP leaves no transaction open');
        $this->assertSame('200ms', $pdo->query("SELECT current_setting('lock_timeout')")->fetchColumn());

        $this->assertTrue(\NightOwl\Support\RawPartitions::dropEmptyHistoric($pdo, 'nightowl_requests'));
        $this->assertFalse($pdo->inTransaction());
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
