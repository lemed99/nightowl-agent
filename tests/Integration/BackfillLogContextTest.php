<?php

namespace NightOwl\Tests\Integration;

use Illuminate\Config\Repository;
use Illuminate\Database\DatabaseServiceProvider;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use NightOwl\Agent\RecordWriter;
use NightOwl\Commands\BackfillLogContextCommand;
use NightOwl\Simulator\NightwatchSimulator;
use NightOwl\Support\StorageV2;
use NightOwl\Tests\Integration\Concerns\ReleasesAppConnections;
use PDO;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * nightowl:backfill-log-context — converting already-stored log context from
 * the deflated context_z to the searchable plaintext context.
 *
 * The property that matters is not "rows got converted", it is that the FENCE
 * never claims more than the data can answer. It walks backwards, one
 * completed chunk at a time, so a run that stops early leaves a smaller
 * searchable window rather than a false one — pinned here by converting only
 * part of a range and asserting the fence landed on the boundary.
 */
class BackfillLogContextTest extends TestCase
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

        $this->app = new Application(sys_get_temp_dir().'/nightowl-log-context-test');
        $this->app->singleton('config', fn () => new Repository([
            'nightowl' => ['log_context_searchable' => true],
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

        self::$pdo->exec('DELETE FROM nightowl_logs_v2');
        self::$pdo->exec('DELETE FROM nightowl_dict_string');
        self::$pdo->exec("DELETE FROM nightowl_settings WHERE key = '".StorageV2::LOG_CONTEXT_FENCE_KEY."'");

        $this->sim = new NightwatchSimulator('test-token');
    }

    protected function tearDown(): void
    {
        $this->releaseAppConnections();
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);
    }

    public function test_it_converts_compressed_context_to_searchable_plaintext(): void
    {
        $this->seedCompressed(['order_id' => 8412], minutesAgo: 30);
        $this->seedCompressed(['order_id' => 9999], minutesAgo: 20);
        $this->openFence('-10 minutes');

        $this->backfill();

        $rows = self::$pdo->query('SELECT context, context_z FROM nightowl_logs_v2 ORDER BY id')
            ->fetchAll(PDO::FETCH_ASSOC);

        foreach ($rows as $row) {
            $this->assertNull($row['context_z'], 'the compressed half must be cleared');
            $this->assertNotNull($row['context']);
        }

        $n = (int) self::$pdo->query(
            "SELECT count(*) FROM nightowl_logs_v2 WHERE context LIKE '%8412%'"
        )->fetchColumn();
        $this->assertSame(1, $n, 'converted context must be matchable in SQL');
    }

    /** The whole point of walking backwards: the fence ends up covering the converted history. */
    public function test_the_fence_walks_back_over_converted_history(): void
    {
        $this->seedCompressed(['order_id' => 1], minutesAgo: 120);
        $before = $this->openFence('-10 minutes');

        $this->backfill();

        $after = $this->fence();
        $this->assertNotNull($after);
        $this->assertLessThan($before, $after, 'the fence must move earlier, never later');
        $this->assertLessThanOrEqual(
            gmdate('Y-m-d H:i:s', strtotime('-120 minutes')),
            $after,
            'the fence must reach back over every converted row',
        );
    }

    /** --since bounds the work, and the fence stops exactly there. */
    public function test_since_bounds_the_conversion_and_the_fence(): void
    {
        $this->seedCompressed(['order_id' => 'old'], minutesAgo: 300);
        $this->seedCompressed(['order_id' => 'new'], minutesAgo: 30);
        $this->openFence('-10 minutes');

        $since = gmdate('Y-m-d H:i:s', strtotime('-60 minutes'));
        $this->backfill(['--since' => $since]);

        $old = self::$pdo->query(
            "SELECT context, context_z FROM nightowl_logs_v2 ORDER BY created_at LIMIT 1"
        )->fetch(PDO::FETCH_ASSOC);

        $this->assertNotNull($old['context_z'], 'a row before --since must be left alone');
        $this->assertNull($old['context']);

        // Never claims coverage it did not convert.
        $this->assertGreaterThanOrEqual($since, $this->fence());
    }

    public function test_it_refuses_when_the_feature_is_off_and_no_fence_exists(): void
    {
        $this->app['config']->set('nightowl.log_context_searchable', false);
        $this->seedCompressed(['order_id' => 1], minutesAgo: 30);

        $output = new BufferedOutput;
        $exit = $this->runCommand([], $output);

        $this->assertSame(1, $exit);
        $this->assertStringContainsString('NIGHTOWL_LOG_CONTEXT_SEARCHABLE', $output->fetch());
        $this->assertNotNull(
            self::$pdo->query('SELECT context_z FROM nightowl_logs_v2 LIMIT 1')->fetchColumn(),
            'a refused run must change nothing',
        );
    }

    public function test_dry_run_measures_without_converting(): void
    {
        $this->seedCompressed(['order_id' => 8412, 'note' => str_repeat('x', 400)], minutesAgo: 30);
        $this->openFence('-10 minutes');

        $output = new BufferedOutput;
        $this->runCommand(['--dry-run' => true], $output);
        $text = $output->fetch();

        $this->assertStringContainsString('after conversion', $text);
        $this->assertNotNull(
            self::$pdo->query('SELECT context_z FROM nightowl_logs_v2 LIMIT 1')->fetchColumn(),
            '--dry-run must not convert anything',
        );
    }

    /** A re-run has nothing left to do and must not disturb the fence. */
    public function test_a_second_run_is_a_no_op(): void
    {
        $this->seedCompressed(['order_id' => 1], minutesAgo: 60);
        $this->openFence('-10 minutes');

        $this->backfill();
        $fence = $this->fence();

        $this->backfill();

        $this->assertSame($fence, $this->fence());
    }

    /**
     * Recovery after the setting is turned off and back on.
     *
     * Turning it off DELETES the fence, so a tenant that had converted months
     * of history loses the searchable window in one drained batch. The way
     * back must be one cheap command, not a second full conversion: re-running
     * the backfill has to walk the fence back over spans that are ALREADY
     * plaintext, converting nothing. If it did not, the documented recovery
     * would be a lie.
     */
    public function test_it_walks_the_fence_back_over_already_converted_history(): void
    {
        $this->seedCompressed(['order_id' => 8412], minutesAgo: 240);
        $this->openFence('-10 minutes');
        $this->backfill();

        $converted = self::$pdo->query('SELECT count(*) FROM nightowl_logs_v2 WHERE context IS NOT NULL')->fetchColumn();
        $this->assertSame(1, (int) $converted);

        // The flag goes off: the drain closes the fence. Then back on, so the
        // drain opens a fresh one at now.
        StorageV2::closeLogContextFence(self::$pdo);
        $this->assertNull($this->fence());
        $reopened = $this->openFence('-1 minute');

        $this->backfill();

        $after = $this->fence();
        $this->assertLessThan($reopened, $after, 'the fence must reach back over the plaintext history again');
        $this->assertSame(
            0,
            (int) self::$pdo->query('SELECT count(*) FROM nightowl_logs_v2 WHERE context_z IS NOT NULL')->fetchColumn(),
            'nothing was left to convert — the second pass only moved the fence',
        );
    }

    private function seedCompressed(array $context, int $minutesAgo): void
    {
        $writer = new RecordWriter(
            self::$host, self::$port, self::$database, self::$username, self::$password,
            logContextSearchable: false,
        );

        $writer->write([$this->sim->makeLog([
            'context' => json_encode($context),
            'timestamp' => time() - ($minutesAgo * 60),
        ])]);

        // The drain stamps created_at from the event clock, but the fixture's
        // exact placement relative to the fence is what these tests turn on —
        // so pin it rather than trusting the guard's fallbacks.
        self::$pdo->exec(sprintf(
            "UPDATE nightowl_logs_v2 SET created_at = '%s' WHERE context_z IS NOT NULL AND created_at > '%s'",
            gmdate('Y-m-d H:i:s', time() - ($minutesAgo * 60)),
            gmdate('Y-m-d H:i:s', time() - 60),
        ));

        // The drain opens no fence in this mode, but it DOES close one — clear
        // any state the write left so each fixture starts from a known point.
        self::$pdo->exec("DELETE FROM nightowl_settings WHERE key = '".StorageV2::LOG_CONTEXT_FENCE_KEY."'");
    }

    private function openFence(string $offset): string
    {
        $at = gmdate('Y-m-d H:i:s', strtotime($offset));
        StorageV2::openLogContextFence(self::$pdo, $at);

        return $at;
    }

    private function fence(): ?string
    {
        return StorageV2::logContextFence(self::$pdo);
    }

    private function backfill(array $args = []): void
    {
        $this->runCommand($args, new BufferedOutput);
    }

    private function runCommand(array $args, BufferedOutput $output): int
    {
        $command = new BackfillLogContextCommand;
        $command->setLaravel($this->app);

        return $command->run(new ArrayInput($args), $output);
    }
}
