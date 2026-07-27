<?php

namespace NightOwl\Tests\Integration;

use Illuminate\Config\Repository;
use Illuminate\Database\DatabaseServiceProvider;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use NightOwl\Agent\RecordWriter;
use NightOwl\Commands\BackfillRollupsCommand;
use NightOwl\Simulator\NightwatchSimulator;
use PDO;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * nightowl:backfill-rollups' bespoke concurrency branch, end to end against
 * the real migrated schema. Both writers of this table (the cleanup tick's
 * maintenance and this backfill) call the SAME ConcurrencyRollup::recompute
 * SQL, so equivalence between them is structural — what needs pinning is the
 * recompute's ARITHMETIC, against hand-computed expectations, plus the
 * command plumbing (chunk walk, advisory lock, HAVING bounds) that wraps it.
 *
 * Runs against the suite's real migrated test DB (public schema), unlike
 * BackfillRollupsFailureTest's synthetic schema.
 */
final class ConcurrencyBackfillTest extends TestCase
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

    protected function setUp(): void
    {
        if (self::$pdo === null) {
            $this->markTestSkipped('PostgreSQL not available. Set NIGHTOWL_TEST_DB_* env vars.');
        }

        $this->app = new Application(sys_get_temp_dir().'/nightowl-concurrency-backfill-test');
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

        self::$pdo->exec('TRUNCATE nightowl_request_concurrency_rollups');
        self::$pdo->exec("DELETE FROM nightowl_requests WHERE trace_id LIKE 'concbf-%'");
    }

    protected function tearDown(): void
    {
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);
    }

    public function test_backfill_computes_exact_folds_from_raw(): void
    {
        $writer = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password, storageV2Config: false);
        $sim = new NightwatchSimulator('test-token');

        // An hour back — outside the cleanup tick's 20-min maintenance window,
        // so ONLY the backfill command can populate these buckets. Expectations
        // are hand-computed, not snapshot-compared: the recompute SQL is shared
        // with maintenance (ConcurrencyRollup), so correctness must be pinned
        // against arithmetic, not against another caller of the same SQL.
        $minute = (intdiv(time(), 60) - 60) * 60;
        $writer->write([
            $sim->makeRequest(['trace_id' => 'concbf-a', 'timestamp' => (float) ($minute + 1), 'duration' => 10_000_000]),
            $sim->makeRequest(['trace_id' => 'concbf-b', 'timestamp' => (float) ($minute + 3), 'duration' => 10_000_000]),
            $sim->makeRequest(['trace_id' => 'concbf-c', 'timestamp' => (float) ($minute + 30), 'duration' => 90_000_000]),
            $sim->makeRequest(['trace_id' => 'concbf-d', 'timestamp' => (float) ($minute + 150), 'duration' => 2_000_000]),
        ]);

        $command = new BackfillRollupsCommand;
        $command->setLaravel($this->app);
        $command->run(new ArrayInput(['--type' => 'nightowl_request_concurrency_rollups']), new BufferedOutput);

        $rows = self::$pdo->query(
            'SELECT EXTRACT(EPOCH FROM bucket_start)::bigint AS e, delta_sum, max_prefix
             FROM nightowl_request_concurrency_rollups ORDER BY bucket_start'
        )->fetchAll(PDO::FETCH_ASSOC);
        $byEpoch = [];
        foreach ($rows as $r) {
            $byEpoch[(int) $r['e']] = [(int) $r['delta_sum'], (int) $r['max_prefix']];
        }

        // Minute M: +1@1, +1@3, -1@11, -1@13, +1@30 → running 1,2,1,0,1.
        $this->assertSame([1, 2], $byEpoch[$minute] ?? null, 'first minute: net +1, peak 2');
        // M+60: the spanner runs straight through — eventless, no row.
        $this->assertArrayNotHasKey($minute + 60, $byEpoch, 'eventless bucket gets no row');
        // M+120: -1@120 (spanner ends), +1@150, -1@152 → running -1,0,-1 → max 0.
        $this->assertSame([-1, 0], $byEpoch[$minute + 120] ?? null, 'third minute: net -1, nothing above entry');
    }
}
