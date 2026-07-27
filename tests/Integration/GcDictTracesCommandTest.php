<?php

namespace NightOwl\Tests\Integration;

use Illuminate\Config\Repository;
use Illuminate\Database\DatabaseServiceProvider;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use NightOwl\Commands\GcDictTracesCommand;
use PDO;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * nightowl:gc-dict-traces — the one dictionary GC. A trace is reclaimed only
 * when it is BOTH unreferenced by any nightowl_exceptions_v2 row (the anti-join)
 * AND older than the quarantine window (the created_at race guard). Everything
 * else — young traces, referenced traces, and any tenant missing the v2 twin or
 * the created_at clock — must be left untouched.
 *
 * Runs inside a dedicated Postgres schema so the suite's real nightowl_* tables
 * are untouched. Skips when PostgreSQL is unavailable.
 */
final class GcDictTracesCommandTest extends TestCase
{
    private const SCHEMA = 'nightowl_gc_dict_test';

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
        self::$pdo->exec('SET search_path TO '.self::SCHEMA);

        // Minimal shapes matching the columns the command touches: the trace
        // dict with its created_at clock, and the v2 exceptions twin holding the
        // only thing that references a trace (trace_ref).
        self::$pdo->exec('CREATE TABLE '.self::SCHEMA.'.nightowl_dict_trace (
            id bigserial primary key, hash bytea, trace_z bytea,
            created_at timestamptz NOT NULL DEFAULT now()
        )');
        self::$pdo->exec('CREATE TABLE '.self::SCHEMA.'.nightowl_exceptions_v2 (
            id bigserial primary key, trace_ref bigint, created_at timestamp
        )');

        $this->app = new Application(sys_get_temp_dir().'/nightowl-gc-dict-test');
        $this->app->singleton('config', fn () => new Repository([
            'database' => [
                'default' => 'nightowl',
                'connections' => ['nightowl' => self::dbConfig()],
            ],
            'nightowl' => [
                'dict_trace_gc' => ['quarantine_days' => 7],
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

    private function runGc(array $options = []): string
    {
        $command = new GcDictTracesCommand;
        $command->setLaravel($this->app);
        $output = new BufferedOutput;
        $exit = $command->run(new ArrayInput($options), $output);
        $text = $output->fetch();
        $this->assertSame(0, $exit, $text);

        return $text;
    }

    /** @return int id of the inserted trace */
    private function insertTrace(string $hash, string $ageInterval): int
    {
        $stmt = self::$pdo->query("INSERT INTO ".self::SCHEMA.".nightowl_dict_trace (hash, trace_z, created_at)
            VALUES (decode('".bin2hex($hash)."', 'hex'), '\\x00', now() - interval '{$ageInterval}')
            RETURNING id");

        return (int) $stmt->fetchColumn();
    }

    private function traceIds(): array
    {
        return array_map('intval', self::$pdo
            ->query('SELECT id FROM '.self::SCHEMA.'.nightowl_dict_trace ORDER BY id')
            ->fetchAll(PDO::FETCH_COLUMN));
    }

    public function test_reclaims_an_old_unreferenced_trace(): void
    {
        $id = $this->insertTrace('t-old', '30 days');

        $text = $this->runGc();

        $this->assertStringContainsString('Reclaimed 1 orphaned trace', $text);
        $this->assertSame([], $this->traceIds());
        unset($id);
    }

    public function test_spares_a_referenced_trace_however_old(): void
    {
        $id = $this->insertTrace('t-ref', '90 days');
        self::$pdo->exec('INSERT INTO '.self::SCHEMA.".nightowl_exceptions_v2 (trace_ref, created_at)
            VALUES ({$id}, now())");

        $this->runGc();

        $this->assertSame([$id], $this->traceIds());
    }

    public function test_spares_a_young_unreferenced_trace(): void
    {
        // Inside the 7-day quarantine window: a batch might still reference it.
        $id = $this->insertTrace('t-young', '1 day');

        $text = $this->runGc();

        $this->assertStringContainsString('No orphaned traces', $text);
        $this->assertSame([$id], $this->traceIds());
    }

    public function test_dry_run_reports_without_deleting(): void
    {
        $id = $this->insertTrace('t-dry', '30 days');

        $text = $this->runGc(['--dry-run' => true]);

        $this->assertStringContainsString('would be reclaimed (dry run)', $text);
        $this->assertSame([$id], $this->traceIds());
    }

    public function test_quarantine_days_option_overrides_config(): void
    {
        // 3 days old: inside the config default (7) but past an explicit 1-day.
        $this->insertTrace('t-opt', '3 days');

        $this->runGc(['--quarantine-days' => '1']);

        $this->assertSame([], $this->traceIds());
    }

    public function test_cutoff_is_utc_under_a_non_utc_session(): void
    {
        // created_at is timestamptz; the cutoff is a string literal. Under a
        // non-UTC session, a BARE cutoff string is parsed in that session's zone,
        // shifting the boundary by the offset — a young trace inside quarantine
        // would then be mis-classified as old and reclaimed. America/New_York is
        // always behind UTC, so the bug shifts the cutoff LATER and wrongly
        // deletes this row; the offset-tagged cutoff keeps it correct.
        //
        // The command reuses the DatabaseManager's cached 'nightowl' connection,
        // so setting the zone here pins it for the command's own reads/deletes.
        \Illuminate\Support\Facades\DB::connection('nightowl')
            ->statement("SET TIME ZONE 'America/New_York'");

        // 2h YOUNGER than a 10-day cutoff → must be spared under any session zone.
        $stmt = self::$pdo->query('INSERT INTO '.self::SCHEMA.".nightowl_dict_trace (hash, trace_z, created_at)
            VALUES (decode('".bin2hex('t-tz')."', 'hex'), '\\x00', now() - interval '10 days' + interval '2 hours')
            RETURNING id");
        $id = (int) $stmt->fetchColumn();

        $this->runGc(['--quarantine-days' => '10']);

        $this->assertSame([$id], $this->traceIds(), 'a young trace must survive under a non-UTC session');
    }

    public function test_no_op_when_v2_twin_is_absent(): void
    {
        $id = $this->insertTrace('t-nov2', '30 days');
        self::$pdo->exec('DROP TABLE '.self::SCHEMA.'.nightowl_exceptions_v2');

        $text = $this->runGc();

        $this->assertStringContainsString('not present', $text);
        $this->assertSame([$id], $this->traceIds());
    }

    public function test_skips_when_created_at_clock_is_absent(): void
    {
        // A pre-000068 tenant: the dict exists but has no created_at signal, so
        // an actively-referenced trace has no "young" proxy — GC must not run.
        $id = $this->insertTrace('t-noclock', '30 days');
        self::$pdo->exec('ALTER TABLE '.self::SCHEMA.'.nightowl_dict_trace DROP COLUMN created_at');

        $text = $this->runGc();

        $this->assertStringContainsString('created_at is absent', $text);
        $this->assertSame([$id], $this->traceIds());
    }
}
