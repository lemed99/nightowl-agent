<?php

namespace NightOwl\Tests\Integration;

use Illuminate\Config\Repository;
use Illuminate\Database\DatabaseServiceProvider;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Facade;
use NightOwl\Commands\MigrateCommand;
use NightOwl\Tests\Integration\Concerns\ReleasesAppConnections;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * MigrateCommand's rollup-completeness predicate, with the tail arm added
 * after the 2026-07-31 Yomoney freeze.
 *
 * The failure that motivated it: prune's v1-EOL dropped `nightowl_requests`
 * under a running daemon, the bespoke concurrency recompute named a relation
 * that no longer existed, and 42P01 stopped that ONE table while every other
 * rollup stayed current. It sat frozen for thirteen hours across a customer's
 * deploys because every completeness check in the codebase looked at floors:
 * the head arm compares minimums (untouched by a freeze — only the ceiling
 * stops), and the tier `call_count` comparison is blind by construction, since
 * a frozen minute base freezes its hourly child too and the sums keep agreeing.
 *
 * So the tail arm is the only thing standing between a stopped rollup and a
 * permanent hole, which makes its FALSE-POSITIVE behaviour just as load-bearing
 * as its detection: it runs on every deploy of every tenant, and a predicate
 * that fires on an idle app or a drain working through a backlog would put a
 * full raw scan into the deploy path of customers with nothing wrong.
 * Both directions are pinned below.
 */
final class RollupTailFreezeTest extends TestCase
{
    use ReleasesAppConnections;

    private const ROLLUP = 'nightowl_request_concurrency_rollups';

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

        $this->app = new Application(sys_get_temp_dir().'/nightowl-rollup-tail-freeze-test');
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

        // The predicate unions BOTH storage families, so a stray row in either
        // one moves the ceiling this test is asserting about. Controlled state
        // means both are emptied, not just the one being written.
        self::$pdo->exec('TRUNCATE '.self::ROLLUP);
        self::$pdo->exec('TRUNCATE nightowl_requests');
        if (self::$pdo->query("SELECT to_regclass('nightowl_requests_v2') IS NOT NULL")->fetchColumn()) {
            self::$pdo->exec('TRUNCATE nightowl_requests_v2');
        }
    }

    protected function tearDown(): void
    {
        $this->releaseAppConnections();

        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);
    }

    // ---------------------------------------------------------------- detects

    /**
     * The Yomoney signature. Raw kept arriving for six hours after the rollup
     * stopped, and the floor is identical on both sides — exactly the shape
     * that every pre-existing check reports as healthy.
     */
    public function test_a_frozen_tail_is_caught_even_though_the_head_is_intact(): void
    {
        $start = time() - 6 * 3600;

        $this->rawAt($start);
        $this->rawAt(time() - 60);          // raw ran all the way to now
        $this->rollupAt($start);            // ...the rollup stopped at hour 0

        $this->assertTrue(
            $this->isIncomplete(),
            'a rollup six hours behind raw must queue a repair',
        );
    }

    /** The floor arm that predates this change — still live. */
    public function test_missing_head_history_still_triggers(): void
    {
        $this->rawAt(time() - 6 * 3600);
        $this->rollupAt(time() - 3600);     // rollup starts late
        $this->rollupAt(time() - 60);       // ...but its tail is current

        $this->assertTrue($this->isIncomplete(), 'raw predating the earliest bucket must queue a repair');
    }

    public function test_an_empty_rollup_triggers(): void
    {
        $this->rawAt(time() - 3600);

        $this->assertTrue($this->isIncomplete(), 'an empty rollup table must be populated');
    }

    // --------------------------------------------------------- refuses to fire

    /**
     * The everyday case: a healthy drain. `bucket_start` is event time and
     * `created_at` is the insert clock, so the two never read equal even when
     * nothing is wrong.
     */
    public function test_normal_drain_lag_is_not_mistaken_for_a_freeze(): void
    {
        $this->rawAt(time() - 7200);
        $this->rawAt(time());
        $this->rollupAt(time() - 7200);
        $this->rollupAt(time() - 90);

        $this->assertFalse($this->isIncomplete(), '90 seconds of drain lag is not a freeze');
    }

    /**
     * A buffer working through a backlog widens the same gap by the whole
     * length of the backlog while losing nothing — the rows are in SQLite and
     * arrive later with their original event times. Repairing here would put a
     * full raw scan in the deploy path of a tenant that is merely busy.
     */
    public function test_a_drain_backlog_inside_the_tolerance_is_not_repaired(): void
    {
        $this->rawAt(time() - 6 * 3600);
        $this->rawAt(time());
        $this->rollupAt(time() - 6 * 3600);
        $this->rollupAt(time() - 100 * 60);   // 100 min behind, tolerance is 120

        $this->assertFalse($this->isIncomplete(), 'a backlog under the tolerance must not trigger a raw scan');
    }

    /**
     * An app with no traffic stalls BOTH clocks together. Nothing is missing —
     * there is simply nothing to roll up — and this runs on every deploy, so a
     * predicate that measured the rollup against wall-clock now instead of
     * against raw would rescan history forever on every quiet tenant.
     */
    public function test_an_idle_app_is_not_repaired_every_deploy(): void
    {
        $threeDays = time() - 3 * 86400;

        $this->rawAt($threeDays);
        $this->rollupAt($threeDays);

        $this->assertFalse($this->isIncomplete(), 'an idle app has no gap, however old its last row');
    }

    public function test_no_raw_history_never_triggers(): void
    {
        $this->rollupAt(time() - 6 * 3600);

        $this->assertFalse($this->isIncomplete(), 'with raw pruned away there is nothing to rebuild from');
    }

    /**
     * Empty over empty. An app that has never produced this telemetry has an
     * empty rollup AND an empty raw source, and "empty" used to be judged
     * before raw was even looked at — so every deploy of such an app ran a
     * backfill sub-command that found "no source rows" and then told the
     * operator to restart the daemon (tinybit.farm, 2026-08-25: three rollup
     * types, every deploy, dev and prod).
     */
    public function test_an_empty_rollup_over_an_empty_source_is_not_repaired(): void
    {
        $this->assertFalse($this->isIncomplete(), 'nothing to roll up is not a hole');
    }

    // ---------------------------------------------------------------- fixtures

    private function rawAt(int $epoch): void
    {
        $stmt = self::$pdo->prepare(
            'INSERT INTO nightowl_requests
                (method, url, status_code, exceptions, logs, queries, jobs_queued,
                 mail, notifications, outgoing_requests, cache_events, created_at)
             VALUES (?, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0, ?)'
        );
        $stmt->execute(['GET', '/tail-freeze', 200, gmdate('Y-m-d H:i:s', $epoch)]);
    }

    private function rollupAt(int $epoch): void
    {
        $stmt = self::$pdo->prepare(
            'INSERT INTO '.self::ROLLUP.' (bucket_start, delta_sum, max_prefix) VALUES (?, 0, 1)
             ON CONFLICT (bucket_start) DO NOTHING'
        );
        $stmt->execute([gmdate('Y-m-d H:i:s', intdiv($epoch, 60) * 60)]);
    }

    /**
     * The predicate is private and DB-backed — there is no seam that does not
     * either reach through reflection or run a full migrate. Reflection keeps
     * the assertion on the logic that changed.
     */
    private function isIncomplete(): bool
    {
        $command = new MigrateCommand;
        $command->setLaravel($this->app);

        $method = (new \ReflectionClass(MigrateCommand::class))->getMethod('concurrencyIsIncomplete');

        return $method->invoke($command, DB::connection('nightowl'));
    }
}
