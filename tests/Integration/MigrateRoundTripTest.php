<?php

namespace NightOwl\Tests\Integration;

use Illuminate\Config\Repository;
use Illuminate\Database\DatabaseServiceProvider;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Facade;
use NightOwl\Commands\MigrateCommand;
use NightOwl\Support\RollupSpecs;
use NightOwl\Support\RollupTiers;
use NightOwl\Support\StorageV2;
use NightOwl\Support\TableCatalog;
use NightOwl\Tests\Integration\Concerns\ReleasesAppConnections;
use PDO;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Illuminate\Console\OutputStyle;

/**
 * `nightowl:migrate`'s deploy-path cost is measured in ROUND TRIPS, not rows.
 *
 * A customer (tinybit.farm, 2026-08-25) reported 72 seconds per deploy on a
 * dev server with almost no traffic AND on prod — the same number on both.
 * The steady-state run ("Nothing to migrate", every rollup already complete)
 * issued 249 statements: a `hasTable` per rollup table, two `to_regclass` plus
 * an aggregate per raw family probe, two `SUM`s per tier, two per v2 sequence
 * pair, one relkind per raw table. Each one is a catalog lookup the server
 * answers in microseconds and the network charges a full round trip for —
 * 0.6s against a local database, 80s through a 100ms-RTT proxy. Nothing about
 * the count depends on data volume, which is why dev matched prod.
 *
 * The completeness pass is the bulk of it and is now one catalog statement
 * plus one measurement statement. These tests pin the count from the
 * database's side — a PDO that counts what it is asked to run, installed on
 * the very connection the command uses, so Laravel-issued and raw-PDO-issued
 * statements are both seen — because a `DB::listen` only sees the former,
 * which is precisely how the raw-PDO probes went unnoticed.
 */
final class MigrateRoundTripTest extends TestCase
{
    use ReleasesAppConnections;

    /**
     * Steady-state budget for the completeness pass: the presence catalog, the
     * one-row measurement, the repair-marker read. Anything above this is a
     * per-table query creeping back in.
     */
    private const STEADY_STATE_STATEMENTS = 3;

    private static ?StatementCountingPdo $pdo = null;

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
            self::$pdo = new StatementCountingPdo($dsn, self::$username, self::$password);
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

        $this->app = new Application(sys_get_temp_dir().'/nightowl-migrate-round-trip-test');
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

        // The command's connection IS the counting handle, so nothing it runs
        // — through the query builder or through getPdo() — escapes the count.
        DB::connection('nightowl')->setPdo(self::$pdo);

        // The customer's shape: every rollup empty, every raw source empty.
        foreach (self::everyRollupAndRawTable() as $table) {
            self::$pdo->exec("TRUNCATE {$table}");
        }
        self::$pdo->exec("DELETE FROM nightowl_settings WHERE key = 'rollup_repair_from'");
    }

    protected function tearDown(): void
    {
        $this->releaseAppConnections();

        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);
    }

    /**
     * Empty over empty, for every type at once — the state that used to be
     * read as "15 empty rollup tables to populate" and cost fifteen backfill
     * sub-commands plus a bogus "restart the daemon" warning per deploy.
     */
    public function test_an_app_with_no_telemetry_has_nothing_to_backfill_and_pays_three_statements(): void
    {
        $output = $this->runCompletenessPass();

        $this->assertStringNotContainsString('Populating', $output, 'empty over empty is complete, not a hole');
        $this->assertStringNotContainsString('Rebuilding', $output);
        $this->assertStringNotContainsString('restart it', $output, 'no work was done, so no restart is owed');

        $this->assertLessThanOrEqual(
            self::STEADY_STATE_STATEMENTS,
            self::$pdo->statements,
            'the completeness pass must stay a fixed handful of statements, whatever the number of rollup tables: '
            .implode(' | ', self::$pdo->log),
        );
    }

    /**
     * The everyday case on a live tenant: raw and every rollup populated and
     * current. Also nothing to do, also the same fixed budget — the count
     * must not scale with how many bases have data.
     */
    public function test_a_healthy_tenant_pays_the_same_three_statements(): void
    {
        $minute = gmdate('Y-m-d H:i:00', time() - 1800);

        self::$pdo->exec("INSERT INTO nightowl_requests
            (method, url, status_code, exceptions, logs, queries, jobs_queued, mail, notifications, outgoing_requests, cache_events, created_at)
            VALUES ('GET', '/round-trip', 200, 0, 0, 0, 0, 0, 0, 0, 0, '{$minute}')");
        // Every spec sourced from nightowl_requests must cover that row, or
        // the pass is RIGHT to flag the one that doesn't (user rollups share
        // the source).
        foreach (['nightowl_request_rollups', 'nightowl_request_hourly_rollups', 'nightowl_request_daily_rollups'] as $table) {
            self::$pdo->exec("INSERT INTO {$table} (group_hash, bucket_start, environment, call_count)
                VALUES ('g', '{$minute}', 'production', 1)");
        }
        foreach (['nightowl_user_rollups', 'nightowl_user_hourly_rollups', 'nightowl_user_daily_rollups'] as $table) {
            self::$pdo->exec("INSERT INTO {$table} (user_id, bucket_start, environment, call_count, success_count, client_error_count, server_error_count)
                VALUES ('u', '{$minute}', 'production', 1, 1, 0, 0)");
        }
        self::$pdo->exec("INSERT INTO nightowl_request_concurrency_rollups (bucket_start, delta_sum, max_prefix)
            VALUES ('{$minute}', 0, 1)");

        $output = $this->runCompletenessPass();

        $this->assertStringNotContainsString('Populating', $output);
        $this->assertStringNotContainsString('Rebuilding', $output);
        $this->assertLessThanOrEqual(self::STEADY_STATE_STATEMENTS, self::$pdo->statements, implode(' | ', self::$pdo->log));
    }

    /**
     * The catalog is what replaced ~120 per-table probes; it must answer for
     * plain and partitioned tables alike and stay silent on absent ones.
     */
    public function test_the_table_catalog_answers_for_every_kind_in_one_statement(): void
    {
        self::$pdo->reset();

        $present = TableCatalog::relkinds(self::$pdo, [
            'nightowl_settings',                 // plain
            'nightowl_requests_v2',              // partitioned parent
            'nightowl_request_rollups',          // plain
            'nightowl_definitely_not_a_table',   // absent
        ]);

        $this->assertSame(1, self::$pdo->statements);
        $this->assertSame('r', $present['nightowl_settings']);
        $this->assertSame('p', $present['nightowl_requests_v2']);
        $this->assertSame('r', $present['nightowl_request_rollups']);
        $this->assertArrayNotHasKey('nightowl_definitely_not_a_table', $present);
    }

    // ---------------------------------------------------------------- helpers

    /**
     * Run just the completeness pass (private, reached by reflection — the
     * full command needs the migrator wired up, which this bare container
     * does not have). Counting starts after the container's own connection
     * setup so only the pass is measured.
     */
    private function runCompletenessPass(): string
    {
        $command = new MigrateCommand;
        $command->setLaravel($this->app);
        $buffer = new BufferedOutput;
        $command->setOutput(new OutputStyle(new ArrayInput([]), $buffer));

        // Warm the connection (search_path etc.) outside the count.
        DB::connection('nightowl')->getPdo();
        self::$pdo->reset();

        (new \ReflectionClass(MigrateCommand::class))
            ->getMethod('backfillEmptyRollups')
            ->invoke($command);

        return $buffer->fetch();
    }

    /** @return list<string> */
    private static function everyRollupAndRawTable(): array
    {
        $tables = ['nightowl_request_concurrency_rollups'];
        foreach (RollupSpecs::all() as $spec) {
            $tables[] = $spec->table;
            foreach (RollupTiers::tierTables($spec->table) as $tier) {
                $tables[] = $tier;
            }
        }
        foreach (StorageV2::TABLES as $v1 => $v2) {
            $tables[] = $v1;
            $tables[] = $v2;
        }

        $present = TableCatalog::relkinds(self::$pdo, $tables);

        return array_values(array_filter($tables, static fn (string $t): bool => isset($present[$t])));
    }
}

/**
 * A PDO that counts every statement it is asked to execute — prepared or
 * direct — so a test can budget round trips from the client's side.
 */
final class StatementCountingPdo extends PDO
{
    public int $statements = 0;

    /** @var list<string> */
    public array $log = [];

    public function reset(): void
    {
        $this->statements = 0;
        $this->log = [];
    }

    public function prepare(string $query, array $options = []): PDOStatement|false
    {
        $this->count($query);

        return parent::prepare($query, $options);
    }

    public function query(string $query, ?int $fetchMode = null, mixed ...$fetchModeArgs): PDOStatement|false
    {
        $this->count($query);

        return parent::query($query, $fetchMode, ...$fetchModeArgs);
    }

    public function exec(string $statement): int|false
    {
        $this->count($statement);

        return parent::exec($statement);
    }

    private function count(string $sql): void
    {
        $this->statements++;
        $this->log[] = substr(preg_replace('/\s+/', ' ', $sql), 0, 80);
    }
}
