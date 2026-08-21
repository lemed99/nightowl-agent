<?php

namespace NightOwl\Tests\Integration;

use Illuminate\Config\Repository;
use Illuminate\Database\DatabaseServiceProvider;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use NightOwl\Agent\RecordWriter;
use NightOwl\Commands\BackfillRollupsCommand;
use NightOwl\Simulator\NightwatchSimulator;
use NightOwl\Tests\Integration\Concerns\ReleasesAppConnections;
use PDO;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * Rollup backfill over a window that straddles the storage-v2 cutover: half
 * the raw rows live in v1, half in the v2 twin. The replace-per-bucket chunk
 * must aggregate BOTH families in one statement (StorageV2::unionFrom), and
 * the result must equal an all-v1 control run of the SAME logical records —
 * the proof that the compat projection keeps every RollupSpec expression and
 * every rollup key byte-identical across families.
 */
class BackfillRollupsV2Test extends TestCase
{
    use ReleasesAppConnections;

    private static ?PDO $pdo = null;

    private static string $host;

    private static int $port;

    private static string $database;

    private static string $username;

    private static string $password;

    private Application $app;

    private NightwatchSimulator $sim;

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

        $this->app = new Application(sys_get_temp_dir().'/nightowl-backfill-v2-test');
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

        foreach (['nightowl_jobs', 'nightowl_jobs_v2', 'nightowl_queries', 'nightowl_queries_v2'] as $t) {
            self::$pdo->exec("DELETE FROM {$t}");
        }
        foreach (['nightowl_job_rollups', 'nightowl_query_rollups'] as $t) {
            self::$pdo->exec("TRUNCATE {$t}");
            self::$pdo->exec('TRUNCATE '.str_replace('_rollups', '_hourly_rollups', $t));
            self::$pdo->exec('TRUNCATE '.str_replace('_rollups', '_daily_rollups', $t));
        }

        $this->sim = new NightwatchSimulator('test-token');
    }

    protected function tearDown(): void
    {
        $this->releaseAppConnections();

        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);
    }

    private function writer(bool $v2): RecordWriter
    {
        return new RecordWriter(
            self::$host, self::$port, self::$database, self::$username, self::$password,
            storageV2Config: $v2,
        );
    }

    private function backfill(string $type): void
    {
        $command = new BackfillRollupsCommand;
        $command->setLaravel($this->app);
        $command->run(new ArrayInput(['--type' => $type]), new BufferedOutput);
    }

    private function rollupRows(string $table): array
    {
        $rows = self::$pdo->query(
            "SELECT * FROM {$table} ORDER BY bucket_start, group_hash NULLS FIRST"
        )->fetchAll(PDO::FETCH_ASSOC);

        // bytea columns fetch as stream resources — compare their bytes.
        foreach ($rows as &$row) {
            foreach ($row as &$value) {
                if (is_resource($value)) {
                    $value = bin2hex(stream_get_contents($value));
                }
            }
        }

        return $rows;
    }

    public function test_split_window_backfill_equals_all_v1_control_for_jobs(): void
    {
        // Two hours back — far outside the drain's live-rollup interference
        // and the backfill's 10-min safety margin.
        $base = (intdiv(time(), 60) - 120) * 60;

        $records = [];
        for ($i = 0; $i < 8; $i++) {
            $records[] = $this->sim->makeJobAttempt([
                'timestamp' => (string) ($base + $i * 60),
                'duration' => 1_000_000 + $i * 100_000,
                'status' => $i % 2 === 0 ? 'processed' : 'failed',
            ]);
        }

        // Mixed run: first half drained pre-cutover (v1), second half post (v2).
        $this->writer(false)->write(array_slice($records, 0, 4));
        $this->writer(true)->write(array_slice($records, 4));
        self::$pdo->exec('TRUNCATE nightowl_job_rollups');

        $this->backfill('nightowl_job_rollups');
        $mixed = $this->rollupRows('nightowl_job_rollups');
        $this->assertNotEmpty($mixed);

        // Control: the SAME records, all in v1.
        self::$pdo->exec('DELETE FROM nightowl_jobs');
        self::$pdo->exec('DELETE FROM nightowl_jobs_v2');
        self::$pdo->exec('TRUNCATE nightowl_job_rollups');
        $this->writer(false)->write($records);
        self::$pdo->exec('TRUNCATE nightowl_job_rollups');

        $this->backfill('nightowl_job_rollups');
        $control = $this->rollupRows('nightowl_job_rollups');

        $this->assertSame($control, $mixed,
            'split-family backfill must equal the all-v1 control byte-for-byte');
    }

    public function test_split_window_backfill_resolves_sql_dictionary_representatives(): void
    {
        $base = (intdiv(time(), 60) - 120) * 60;
        $sql = 'select * from "orders" where "tenant_id" = ?';

        $records = [];
        for ($i = 0; $i < 6; $i++) {
            $records[] = $this->sim->makeQuery([
                'timestamp' => (string) ($base + $i * 30),
                'sql' => $sql,
                'duration' => 2_000 + $i,
            ]);
        }

        $this->writer(false)->write(array_slice($records, 0, 3));
        $this->writer(true)->write(array_slice($records, 3));
        self::$pdo->exec('TRUNCATE nightowl_query_rollups');

        $this->backfill('nightowl_query_rollups');

        $rows = self::$pdo->query(
            'SELECT sql_query, call_count FROM nightowl_query_rollups'
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertNotEmpty($rows);
        $total = 0;
        foreach ($rows as $row) {
            $this->assertSame($sql, $row['sql_query'],
                'the MIN(sql_query) representative must resolve through the dict join');
            $total += (int) $row['call_count'];
        }
        $this->assertSame(6, $total, 'both families must contribute to call_count');
    }
}
