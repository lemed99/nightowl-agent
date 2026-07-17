<?php

namespace NightOwl\Tests\Integration;

use Illuminate\Config\Repository;
use Illuminate\Database\DatabaseServiceProvider;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use NightOwl\Commands\BackfillRollupsCommand;
use NightOwl\Support\QueryHistogram;
use PDO;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * A rollup table that exists but is empty is worse than an absent one — the
 * API's read path switches to a rollup the moment it EXISTS and serves zeros,
 * falling back to raw only when it is absent. So a failing backfill has two
 * obligations, both covered here: give every OTHER type its pass (one bad type
 * must not strand the rest), and fail LOUDLY enough that nightowl:migrate —
 * which backfills via $this->call() from a `: void` method and discards the
 * exit code — cannot report success over tables it just left empty.
 *
 * Runs inside a dedicated Postgres schema: search_path excludes public, so the
 * suite's real nightowl_* tables are neither read nor written, and the specs
 * whose tables this test does not create are skipped as absent.
 */
final class BackfillRollupsFailureTest extends TestCase
{
    private const SCHEMA = 'nightowl_backfill_failure_test';

    /** rollup table => raw source table, per RollupSpecs */
    private const TYPES = [
        'nightowl_mail_rollups' => 'nightowl_mail',
        'nightowl_notification_rollups' => 'nightowl_notifications',
    ];

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

        // Rollup tables that exist but whose shape the generated INSERT cannot
        // satisfy (no call_count) — the partially-migrated tenant, reduced to
        // its essence. Both types carry source rows, so neither can slip out
        // through the "no source rows" early return.
        foreach (self::TYPES as $rollup => $source) {
            self::$pdo->exec('CREATE TABLE '.self::SCHEMA.".{$rollup} (group_hash text, bucket_start timestamp)");
            self::$pdo->exec('CREATE TABLE '.self::SCHEMA.".{$source} (
                id bigserial, group_hash text, environment text, duration bigint,
                queued boolean, failed boolean, created_at timestamp
            )");
            self::$pdo->exec('INSERT INTO '.self::SCHEMA.".{$source}
                (group_hash, environment, duration, queued, failed, created_at)
                VALUES ('g1', 'production', 100, false, false, now() - interval '2 days')");
        }

        $this->app = new Application(sys_get_temp_dir().'/nightowl-backfill-failure-test');
        $this->app->singleton('config', fn () => new Repository([
            'database' => [
                'default' => 'nightowl',
                'connections' => ['nightowl' => self::dbConfig()],
            ],
        ]));
        (new DatabaseServiceProvider($this->app))->register();

        // An earlier test class in the suite leaves its own resolved `db` root
        // on the facade; without dropping it, DB::connection('nightowl') hands
        // back that connection and this test silently runs against public.
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication($this->app);
    }

    protected function tearDown(): void
    {
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);

        self::$pdo?->exec('DROP SCHEMA IF EXISTS '.self::SCHEMA.' CASCADE');
    }

    private function runBackfill(): void
    {
        $command = new BackfillRollupsCommand;
        $command->setLaravel($this->app);
        $command->run(new ArrayInput([]), new BufferedOutput);
    }

    /**
     * Build a mail rollup table whose optional columns are switchable, so a
     * tier pair can be desynced the way a half-finished DDL leaves it.
     */
    private function makeRollupTable(string $name, bool $hist, bool $sketch, bool $durationCount): void
    {
        $cols = [
            'group_hash text not null',
            'bucket_start timestamp not null',
            "environment text not null default ''",
            'call_count bigint not null default 0',
            'queued_count bigint not null default 0',
            'failed_count bigint not null default 0',
            'total_duration bigint not null default 0',
            'min_duration bigint',
            'max_duration bigint',
            'mailable text',
        ];

        if ($durationCount) {
            $cols[] = 'duration_count bigint not null default 0';
        }
        if ($sketch) {
            $cols[] = 'sketch bytea';
            $cols[] = 'sketch_version int';
        }
        if ($hist) {
            foreach (QueryHistogram::columns() as $c) {
                $cols[] = "{$c} bigint not null default 0";
            }
        }

        $cols[] = 'primary key (group_hash, bucket_start, environment)';

        self::$pdo->exec('CREATE TABLE '.self::SCHEMA.".{$name} (".implode(', ', $cols).')');
    }

    public function test_a_tier_pair_desynced_by_a_partial_drop_still_backfills(): void
    {
        // nightowl:drop-v1-histograms drops per-table outside a transaction
        // (base→hourly→daily), so a lock timeout on the large hourly table
        // leaves the minute table hist-less while hourly still has the bins.
        // The tier SELECT reads hist_00 from the minute table: probing only the
        // destination emitted SUM(hist_00) over a source without it — 42703,
        // aborting the pass and leaving the daily tier empty.
        self::$pdo->exec('DROP SCHEMA IF EXISTS '.self::SCHEMA.' CASCADE');
        self::$pdo->exec('CREATE SCHEMA '.self::SCHEMA);

        self::$pdo->exec('CREATE TABLE '.self::SCHEMA.'.nightowl_mail (
            id bigserial, group_hash text, mailable text, environment text,
            duration bigint, queued boolean, failed boolean, created_at timestamp
        )');
        self::$pdo->exec('INSERT INTO '.self::SCHEMA.".nightowl_mail
            (group_hash, mailable, environment, duration, queued, failed, created_at)
            VALUES ('g1', 'App\\Mail\\Welcome', 'production', 120, false, false, now() - interval '2 days')");

        $this->makeRollupTable('nightowl_mail_rollups', hist: false, sketch: false, durationCount: true);
        $this->makeRollupTable('nightowl_mail_hourly_rollups', hist: true, sketch: false, durationCount: true);
        $this->makeRollupTable('nightowl_mail_daily_rollups', hist: true, sketch: false, durationCount: true);

        $command = new BackfillRollupsCommand;
        $command->setLaravel($this->app);
        $output = new BufferedOutput;
        $exit = $command->run(new ArrayInput(['--type' => 'nightowl_mail_rollups']), $output);
        $text = $output->fetch();

        $this->assertSame(0, $exit, 'The desynced pair must not abort the pass: '.$text);

        // The daily tier is the one the wide-range views read; an empty table
        // there renders as zeros rather than falling back to raw.
        $daily = (int) self::$pdo->query('SELECT count(*) FROM '.self::SCHEMA.'.nightowl_mail_daily_rollups')->fetchColumn();
        $this->assertSame(1, $daily, 'The daily tier must be populated through the desync.');

        // Dropping the column from the copy resets it on the side that still
        // has it, so the operator has to be told rather than left guessing.
        $this->assertStringContainsString('disagree on hist_00', $text);
    }

    public function test_a_failing_type_raises_rather_than_returning_a_discardable_code(): void
    {
        // Returning self::FAILURE here is not enough: nightowl:migrate's
        // backfillEmptyRollups() is `: void` and throws the int away, so the
        // deploy would go green over rollup tables it just left empty.
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/Backfill incomplete/');

        $this->runBackfill();
    }

    public function test_every_failing_type_is_named_not_just_the_first(): void
    {
        try {
            $this->runBackfill();
            $this->fail('Expected the backfill to raise.');
        } catch (\RuntimeException $e) {
            // Aborting on the first type would leave the second un-backfilled
            // and unmentioned — the state the API renders as zeros.
            foreach (array_keys(self::TYPES) as $rollup) {
                $this->assertStringContainsString($rollup, $e->getMessage());
            }
        }
    }
}
