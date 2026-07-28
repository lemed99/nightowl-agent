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
        self::$pdo->exec("DELETE FROM nightowl_requests WHERE trace_id LIKE 'concbf%'");
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

        $byEpoch = $this->rollupsByEpoch();

        // Minute M: +1@1, +1@3, -1@11, -1@13, +1@30 → running 1,2,1,0,1.
        $this->assertSame([1, 2], $byEpoch[$minute] ?? null, 'first minute: net +1, peak 2');
        // M+60: the spanner runs straight through — eventless, no row.
        $this->assertArrayNotHasKey($minute + 60, $byEpoch, 'eventless bucket gets no row');
        // M+120: -1@120 (spanner ends), +1@150, -1@152 → running -1,0,-1 → max 0.
        $this->assertSame([-1, 0], $byEpoch[$minute + 120] ?? null, 'third minute: net -1, nothing above entry');
    }

    /**
     * app.timezone belongs to the customer's app, and BYO deployments are not
     * all UTC — while every column this command compares against is written in
     * UTC (RecordWriter::eventCreatedAt / eventBucket). Carbon's now()/parse()
     * read app.timezone, so a chunk used to scan raw rows in one wall clock and
     * DELETE/re-INSERT buckets in another: the offset-wide head of every chunk
     * was cleared and never repopulated, punching a multi-hour hole in the
     * peak-concurrency chart — including buckets the live drain had written
     * correctly.
     *
     * Laravel applies app.timezone by calling date_default_timezone_set() at
     * boot, and that is exactly what Carbon reads, so setting it here is a
     * non-UTC app for every clock this command touches.
     */
    public function test_backfill_window_stays_utc_under_a_non_utc_app_timezone(): void
    {
        $appTz = date_default_timezone_get();
        date_default_timezone_set('America/New_York');

        try {
            // 6h back, so that even app-time parsing of --since (which shifts it
            // up to 5h toward `now`) stays under the safety ceiling: otherwise
            // the pre-fix run bailed out with "nothing to backfill" before
            // reaching the DELETE this test is about.
            $minute = (intdiv(time(), 60) - 360) * 60;

            // Measured, not assumed — New York is -5h or -4h depending on DST,
            // and this is exactly how far gmdate() of an app-time epoch sits
            // from the UTC wall clock the data is written in.
            $offset = (new \DateTimeZone(date_default_timezone_get()))
                ->getOffset(new \DateTimeImmutable('@'.$minute));
            $this->assertLessThan(0, $offset, 'the fixture needs a non-UTC offset to test anything');

            // The suite shares this database, so clear whatever already sits in
            // the scan window — the hand-computed fold has to be the only fold
            // the recompute can find there. Both storage families, because the
            // recompute scans both when the v2 twin exists.
            foreach (['nightowl_requests', 'nightowl_requests_v2'] as $family) {
                if (! self::$pdo->query("SELECT to_regclass('{$family}') IS NOT NULL AS e")->fetchColumn()) {
                    continue;
                }
                $wipe = self::$pdo->prepare("DELETE FROM {$family} WHERE created_at >= ? AND created_at < ?");
                $wipe->execute([gmdate('Y-m-d H:i:s', $minute - 900), gmdate('Y-m-d H:i:s', $minute + 1200)]);
            }

            $writer = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password, storageV2Config: false);
            $sim = new NightwatchSimulator('test-token');
            $writer->write([
                $sim->makeRequest(['trace_id' => 'concbftz-a', 'timestamp' => (float) ($minute + 1), 'duration' => 5_000_000]),
                $sim->makeRequest(['trace_id' => 'concbftz-b', 'timestamp' => (float) ($minute + 2), 'duration' => 5_000_000]),
            ]);

            // Two sentinels the run must treat differently. $inside sits in the
            // requested window with no raw rows behind it — the DELETE has to
            // clear it and the recompute must not bring it back. $drifted sits
            // an offset away, where the app-time epochs used to point the
            // DELETE, and must survive untouched.
            $inside = $minute + 240;
            $drifted = $minute - $offset;
            $this->seedBucket($inside);
            $this->seedBucket($drifted);

            $command = new BackfillRollupsCommand;
            $command->setLaravel($this->app);
            $command->run(new ArrayInput([
                '--type' => 'nightowl_request_concurrency_rollups',
                '--since' => gmdate('Y-m-d H:i:s', $minute),
                '--until' => gmdate('Y-m-d H:i:s', $minute + 300),
            ]), new BufferedOutput);

            $byEpoch = $this->rollupsByEpoch();

            // +1@1, +1@2, -1@6, -1@7 — all four events inside the one minute.
            $this->assertSame([0, 2], $byEpoch[$minute] ?? null, 'the fold lands in the UTC bucket its rows belong to');
            $this->assertArrayNotHasKey($inside, $byEpoch, 'a stale bucket inside the window is cleared, not left behind');
            $this->assertSame([7, 7], $byEpoch[$drifted] ?? null, 'a bucket an offset away from the window is left alone');
        } finally {
            date_default_timezone_set($appTz);
        }
    }

    /**
     * The same clocks one bound further out: with no --until the ceiling is
     * `now` minus the safety margin, and the floor is whatever the operator
     * typed. Both were born in app time before the fix, so on a negative-offset
     * app a UTC --since resolved to an instant an offset AHEAD of the ceiling
     * and the run bailed out at "nothing to backfill" without writing a bucket
     * at all — the failure mode of nightowl:migrate's automatic backfill, which
     * passes neither bound. backfillSpec derives $until from the same
     * expression, so this pins the ceiling for the generic passes too.
     */
    public function test_default_ceiling_and_since_option_are_utc_under_a_non_utc_app_timezone(): void
    {
        $appTz = date_default_timezone_get();
        date_default_timezone_set('America/New_York');

        try {
            // 2h back — far past the 10-min safety margin, so the ceiling can
            // never be the honest reason this bucket is missing.
            $minute = (intdiv(time(), 60) - 120) * 60;

            // Same shared-database precaution as the test above: the fold has to
            // be the only one the recompute can find in this bucket.
            foreach (['nightowl_requests', 'nightowl_requests_v2'] as $family) {
                if (! self::$pdo->query("SELECT to_regclass('{$family}') IS NOT NULL AS e")->fetchColumn()) {
                    continue;
                }
                $wipe = self::$pdo->prepare("DELETE FROM {$family} WHERE created_at >= ? AND created_at < ?");
                $wipe->execute([gmdate('Y-m-d H:i:s', $minute - 900), gmdate('Y-m-d H:i:s', $minute + 1200)]);
            }

            $writer = new RecordWriter(self::$host, self::$port, self::$database, self::$username, self::$password, storageV2Config: false);
            $sim = new NightwatchSimulator('test-token');
            $writer->write([
                $sim->makeRequest(['trace_id' => 'concbfceil-a', 'timestamp' => (float) ($minute + 1), 'duration' => 5_000_000]),
                $sim->makeRequest(['trace_id' => 'concbfceil-b', 'timestamp' => (float) ($minute + 2), 'duration' => 5_000_000]),
            ]);

            $command = new BackfillRollupsCommand;
            $command->setLaravel($this->app);
            // --since only: the run must find its own ceiling. The floor stays
            // explicit so the walk is one chunk — without it the cursor starts at
            // the shared database's oldest raw row and every intervening day
            // costs a chunk.
            $command->run(new ArrayInput([
                '--type' => 'nightowl_request_concurrency_rollups',
                '--since' => gmdate('Y-m-d H:i:s', $minute),
            ]), new BufferedOutput);

            $byEpoch = $this->rollupsByEpoch();

            // +1@1, +1@2, -1@6, -1@7 — one minute, peak 2, net 0.
            $this->assertSame([0, 2], $byEpoch[$minute] ?? null, 'a two-hour-old bucket is inside the safety ceiling and must be rolled up');
        } finally {
            date_default_timezone_set($appTz);
        }
    }

    /** A hand-planted bucket, so a DELETE that lands on the wrong window shows up. */
    private function seedBucket(int $epoch, int $deltaSum = 7, int $maxPrefix = 7): void
    {
        $stmt = self::$pdo->prepare(
            'INSERT INTO nightowl_request_concurrency_rollups (bucket_start, delta_sum, max_prefix) VALUES (?, ?, ?)'
        );
        $stmt->execute([gmdate('Y-m-d H:i:s', $epoch), $deltaSum, $maxPrefix]);
    }

    /**
     * bucket_start is `timestamp` (no zone) holding UTC wall time, and
     * EXTRACT(EPOCH ...) over it converts nothing — so these epochs are
     * comparable to the gmdate() clock the fixtures are built on.
     *
     * @return array<int, array{0: int, 1: int}>  bucket epoch => [delta_sum, max_prefix]
     */
    private function rollupsByEpoch(): array
    {
        $rows = self::$pdo->query(
            'SELECT EXTRACT(EPOCH FROM bucket_start)::bigint AS e, delta_sum, max_prefix
             FROM nightowl_request_concurrency_rollups ORDER BY bucket_start'
        )->fetchAll(PDO::FETCH_ASSOC);

        $byEpoch = [];
        foreach ($rows as $r) {
            $byEpoch[(int) $r['e']] = [(int) $r['delta_sum'], (int) $r['max_prefix']];
        }

        return $byEpoch;
    }
}
