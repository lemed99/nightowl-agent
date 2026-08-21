<?php

namespace NightOwl\Tests\Integration;

use Illuminate\Config\Repository;
use Illuminate\Console\Application as ConsoleApplication;
use Illuminate\Console\OutputStyle;
use Illuminate\Database\DatabaseServiceProvider;
use Illuminate\Events\Dispatcher;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use NightOwl\Agent\RecordWriter;
use NightOwl\Commands\BackfillRollupsCommand;
use NightOwl\Commands\MigrateCommand;
use NightOwl\Simulator\NightwatchSimulator;
use NightOwl\Tests\Integration\Concerns\ReleasesAppConnections;
use PDO;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * The 2.0.0 wedge: a boot rollup backfill stops the live drain, and the agent
 * ends up rejecting telemetry.
 *
 * Mechanism. 2.0.0 defaults NIGHTOWL_AUTO_MIGRATE=true, so
 * AgentCommand::spawnBackgroundBackfill detaches `nightowl:migrate`, whose
 * backfillEmptyRollups() runs `nightowl:backfill-rollups --tiers-only` for every
 * incomplete tier. Each chunk of that pass takes a rollup table's EXCLUSIVE
 * advisory lock for its whole transaction (BackfillRollupsCommand::backfillTierChunk);
 * the drain takes the paired SHARED lock under `SET LOCAL lock_timeout`
 * (RecordWriter::lockRollupForWriteShared). Two independent things then went wrong:
 *
 *   1. A chunk was a CALENDAR span (7 days of minute rollups for the hourly tier),
 *      so its duration grew with the tenant. Measured locally: 1.99s at 100 query
 *      groups over 14 days, 5.75s at 250, 13.87s at 500, 24.18s at 800 — crossing
 *      the 10s default at ~360 groups. In the field it held 24s per chunk.
 *   2. The drain treated the resulting 55P03 as a transient and deferred the whole
 *      batch. Deferring is only free while the contention is shorter than the
 *      SQLite buffer's headroom. It wasn't: the buffer crossed
 *      NIGHTOWL_MAX_PENDING_ROWS and AsyncServer began answering `5:ERROR` to live
 *      payloads for two hours.
 *
 * Both halves are fixed and both are guarded here — chunk duration is now paced to
 * a measured target instead of a calendar span, and a rollup lock the drain cannot
 * take costs the ROLLUP for that batch, never the raw telemetry.
 */
final class RollupBackfillDrainContentionTest extends TestCase
{
    use ReleasesAppConnections;

    private const MINUTE_TABLE = 'nightowl_query_rollups';

    private const TIER_TABLE = 'nightowl_query_hourly_rollups';

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
            self::$pdo = new PDO(
                sprintf('pgsql:host=%s;port=%d;dbname=%s', self::$host, self::$port, self::$database),
                self::$username,
                self::$password,
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            );
        } catch (\Throwable) {
            self::$pdo = null;

            return;
        }

        MigrationRunner::migrate(self::$host, self::$port, self::$database, self::$username, self::$password);
    }

    protected function setUp(): void
    {
        if (self::$pdo === null) {
            $this->markTestSkipped('PostgreSQL not available. Set NIGHTOWL_TEST_DB_* env vars.');
        }

        $this->sim = new NightwatchSimulator('test-token');

        self::$pdo->exec('TRUNCATE '.self::MINUTE_TABLE.', '.self::TIER_TABLE.', nightowl_query_daily_rollups');
        self::$pdo->exec('DELETE FROM nightowl_queries');
        self::$pdo->exec("DELETE FROM nightowl_settings WHERE key = 'rollup_repair_from'");

        $this->app = new Application(sys_get_temp_dir().'/nightowl-contention-test');
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
    }

    protected function tearDown(): void
    {
        $this->releaseAppConnections();

        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);
    }

    private function writer(int $lockTimeoutMs = 500): RecordWriter
    {
        return new RecordWriter(
            self::$host, self::$port, self::$database, self::$username, self::$password,
            lockTimeoutMs: $lockTimeoutMs,
            storageV2Config: false,
        );
    }

    /** A batch of query records inside one hour bucket. */
    private function queryBatch(string $prefix, int $count = 4): array
    {
        $hourStart = intdiv(time(), 3600) * 3600;

        return array_map(fn (int $i): array => $this->sim->makeQuery([
            'trace_id' => "{$prefix}-{$i}",
            '_group' => 'contention'.$prefix,
            'sql' => 'SELECT * FROM widgets',
            'duration' => 500 + $i * 50,
            'connection' => 'pgsql',
            'timestamp' => $hourStart + $i,
        ]), range(0, $count - 1));
    }

    /**
     * Hold the lock a backfill chunk holds, exactly the way
     * BackfillRollupsCommand::backfillTierChunk takes it, on a second session.
     */
    private function holdBackfillChunkLock(string $table): PDO
    {
        $holder = new PDO(
            sprintf('pgsql:host=%s;port=%d;dbname=%s', self::$host, self::$port, self::$database),
            self::$username,
            self::$password,
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $holder->beginTransaction();
        $stmt = $holder->prepare('SELECT pg_advisory_xact_lock(hashtext(?))');
        $stmt->execute(['nightowl_rollup:'.$table]);

        return $holder;
    }

    private function rawCount(string $prefix): int
    {
        $stmt = self::$pdo->prepare('SELECT COUNT(*) FROM nightowl_queries WHERE group_hash = ?');
        $stmt->execute(['contention'.$prefix]);

        return (int) $stmt->fetchColumn();
    }

    private function rollupCalls(string $table, string $prefix): int
    {
        $stmt = self::$pdo->prepare("SELECT COALESCE(SUM(call_count), 0) FROM {$table} WHERE group_hash = ?");
        $stmt->execute(['contention'.$prefix]);

        return (int) $stmt->fetchColumn();
    }

    /** @return array<string, string> table => earliest bucket owed a repair */
    private function repairDebt(): array
    {
        $raw = self::$pdo->query("SELECT value FROM nightowl_settings WHERE key = 'rollup_repair_from'")->fetchColumn();

        return $raw === false || $raw === null ? [] : json_decode((string) $raw, true, 8);
    }

    /**
     * The heart of the fix. A held rollup lock used to abort the batch with 55P03
     * and land nothing; now the raw telemetry commits and only the locked rollup
     * is skipped.
     */
    public function test_a_locked_tier_costs_the_tier_row_and_nothing_else(): void
    {
        $holder = $this->holdBackfillChunkLock(self::TIER_TABLE);

        try {
            $this->writer()->write($this->queryBatch('tier'));
        } catch (\Throwable $e) {
            $this->fail('A locked rollup table must not fail the drain batch: '.$e->getMessage());
        } finally {
            $holder->rollBack();
        }

        $this->assertSame(4, $this->rawCount('tier'), 'The raw query rows must land — losing them is the one outcome this path may not choose.');
        $this->assertSame(4, $this->rollupCalls(self::MINUTE_TABLE, 'tier'), 'The minute rollup was not locked, so it must still be written.');
        $this->assertSame(0, $this->rollupCalls(self::TIER_TABLE, 'tier'), 'The locked tier is the only thing skipped.');

        // A tier shortfall against its minute source is exactly what
        // MigrateCommand::tierIsIncomplete() already detects, so this state needs
        // no marker of its own.
        $this->assertSame([], $this->repairDebt(), 'A skipped TIER is self-detecting and must not write a repair marker.');
    }

    /**
     * Locking the minute table skips the whole chain — and that hole IS invisible
     * to the completeness checks (MIN still predates raw, tier sums still agree
     * with their source), so it must leave a marker naming the batch's earliest
     * bucket.
     */
    public function test_a_locked_minute_rollup_keeps_the_raw_rows_and_records_the_repair_floor(): void
    {
        $holder = $this->holdBackfillChunkLock(self::MINUTE_TABLE);

        try {
            $this->writer()->write($this->queryBatch('minute'));
        } finally {
            $holder->rollBack();
        }

        $this->assertSame(4, $this->rawCount('minute'), 'Raw telemetry survives a locked rollup.');
        $this->assertSame(0, $this->rollupCalls(self::MINUTE_TABLE, 'minute'));
        $this->assertSame(
            0,
            $this->rollupCalls(self::TIER_TABLE, 'minute'),
            'Tiers are skipped WITH the base, so a wide-range chart never disagrees with the narrow one over the same span.',
        );

        $debt = $this->repairDebt();
        $this->assertArrayHasKey(self::MINUTE_TABLE, $debt, 'The hole must be recorded — nothing else can find it.');

        // Same clock and format RecordWriter::eventBucket emits.
        $expectedFloor = gmdate('Y-m-d H:i:s', intdiv(time(), 3600) * 3600);
        $this->assertSame($expectedFloor, $debt[self::MINUTE_TABLE], 'The floor is the earliest bucket in the skipped batch.');
    }

    /**
     * Yomoney's drain retried forever and never made progress. Now every retry
     * makes progress on the raw rows, and the recorded floor stays the EARLIEST
     * across batches so one repair covers all of them.
     */
    public function test_repeated_batches_all_land_their_raw_rows_and_keep_the_earliest_floor(): void
    {
        // Writing the oldest batch first would make "earliest" trivially the first
        // write, so the earliest bucket goes SECOND: only a real min() keeps it.
        $holder = $this->holdBackfillChunkLock(self::MINUTE_TABLE);
        $writer = $this->writer();
        $now = time();

        try {
            foreach ([-60, -600, -300] as $i => $offset) {
                $writer->write([$this->sim->makeQuery([
                    'trace_id' => "floor-{$i}",
                    '_group' => 'contentionfloor',
                    'sql' => 'SELECT * FROM widgets',
                    'duration' => 500,
                    'connection' => 'pgsql',
                    'timestamp' => $now + $offset,
                ])]);
            }
        } finally {
            $holder->rollBack();
        }

        $this->assertSame(3, $this->rawCount('floor'), 'Every batch drained its raw rows despite the lock.');
        $this->assertSame(0, $this->rollupCalls(self::MINUTE_TABLE, 'floor'));

        $this->assertSame(
            gmdate('Y-m-d H:i:s', intdiv($now - 600, 60) * 60),
            $this->repairDebt()[self::MINUTE_TABLE] ?? null,
            'Repeated debt writes converge on the earliest floor, not the last writer.',
        );
    }

    /**
     * Control: with no backfill in flight the identical batch fills every tier and
     * records no debt. This is what makes the skips above attributable to the lock.
     */
    public function test_an_uncontended_batch_fills_every_tier_and_owes_nothing(): void
    {
        $this->writer()->write($this->queryBatch('clear'));

        $this->assertSame(4, $this->rollupCalls(self::MINUTE_TABLE, 'clear'));
        $this->assertSame(4, $this->rollupCalls(self::TIER_TABLE, 'clear'));
        $this->assertSame(4, $this->rollupCalls('nightowl_query_daily_rollups', 'clear'));
        $this->assertSame([], $this->repairDebt());
    }

    /**
     * The other half of the fix: chunk size is paced by measured hold time, not by
     * a calendar span.
     *
     * Asserted structurally rather than by load, because the load version needs a
     * seed big enough to take a minute. The `--tiers-only` pass over a 3-day span
     * has a 7-day ceiling for the hourly tier and a 30-day ceiling for the daily
     * one, so the OLD calendar loop took the lock exactly ONCE per tier for the
     * whole span. A paced loop starts far narrower and grows into the ceiling, so
     * it takes it many times — and it can never take it fewer times, whatever the
     * constants are tuned to, as long as the initial window is under the span.
     */
    public function test_the_tier_pass_takes_the_lock_in_many_paced_chunks_not_once_per_calendar_ceiling(): void
    {
        $this->seedMinuteRollups(3, 2);

        $output = $this->backfillTiersOnly();

        $this->assertGreaterThan(
            1,
            $this->reportedChunks($output, self::TIER_TABLE),
            "The hourly pass must break its 3-day span into several chunks; the calendar loop did it in one. Output:\n".$output,
        );

        // And every pass reports what it held, so an operator can see the pacing
        // working (or failing) without instrumenting anything.
        $this->assertMatchesRegularExpression(
            '/longest lock hold \d+\.\d\ds/',
            $output,
            'Each pass reports its longest lock hold.',
        );

        // Correctness is not traded for pacing: the tiers still equal their source.
        $this->assertSame(
            (int) self::$pdo->query('SELECT SUM(call_count) FROM '.self::MINUTE_TABLE)->fetchColumn(),
            (int) self::$pdo->query('SELECT SUM(call_count) FROM '.self::TIER_TABLE)->fetchColumn(),
            'A paced hourly tier still covers every minute row exactly once.',
        );
    }

    /**
     * The pacing controller itself, at the boundaries that matter: a chunk that
     * overran must shrink, a fast one may grow but only 2x (so the worst hold
     * after a converged chunk stays an order of magnitude under a default
     * lock_timeout), and neither direction may escape [min, max].
     */
    public function test_the_pacer_shrinks_after_an_overrun_and_never_leaves_its_bounds(): void
    {
        $next = function (float $current, float $elapsed, float $min = 60.0, float $max = 604800.0): float {
            $m = new \ReflectionMethod(BackfillRollupsCommand::class, 'nextWindow');

            return $m->invoke(null, $current, $elapsed, $min, $max);
        };

        // A 24s hold — the field incident — must collapse the window, and by the
        // clamp's full factor rather than proportionally, so recovery is bounded.
        $this->assertSame(1000.0, $next(4000.0, 24.0), 'An overrun shrinks by the 0.25x clamp.');

        // A hold at target leaves the window alone.
        $this->assertSame(3600.0, $next(3600.0, 1.0), 'A converged chunk holds its window.');

        // A trivially fast chunk grows, capped at 2x — never straight to the max.
        $this->assertSame(7200.0, $next(3600.0, 0.001), 'A fast chunk at most doubles.');

        // Bounds win over the controller in both directions.
        $this->assertSame(60.0, $next(60.0, 30.0), 'Shrinking stops at the minimum window.');
        $this->assertSame(604800.0, $next(400000.0, 0.001), 'Growth stops at the ceiling.');
    }

    /**
     * By design, and worth pinning because it looks like the same asymmetry bug:
     * backfillSpec() keeps its ceiling SAFETY_MARGIN_SECONDS behind now while
     * backfillTiers() runs to now, so the tier pass DOES recompute the bucket the
     * drain is still writing. That is safe — the exclusive/shared lock protocol
     * makes the replace commute with the drain's additive upsert — and it is
     * necessary: excluding the in-progress hour would leave a permanent shortfall
     * that tierIsIncomplete() flags on every boot without ever healing.
     */
    public function test_the_tier_pass_deliberately_covers_the_bucket_the_drain_is_still_writing(): void
    {
        $this->writer()->write($this->queryBatch('current'));

        $currentHour = gmdate('Y-m-d H:00:00', intdiv(time(), 3600) * 3600);
        $stmt = self::$pdo->prepare('SELECT COUNT(*) FROM '.self::TIER_TABLE.' WHERE bucket_start = ?');
        $stmt->execute([$currentHour]);

        $this->assertSame(1, (int) $stmt->fetchColumn(), 'The drain writes the in-progress hour bucket.');

        // The tier pass recomputes it anyway, and the recompute is not lossy.
        $this->backfillTiersOnly();

        $stmt->execute([$currentHour]);
        $this->assertSame(1, (int) $stmt->fetchColumn());
        $this->assertSame(
            4,
            $this->rollupCalls(self::TIER_TABLE, 'current'),
            'Recomputing the live bucket reproduces the drain\'s own count, not a partial one.',
        );
    }

    /**
     * The debt marker is only worth writing if something consumes it. End to end:
     * a locked rollup leaves raw rows with no rollup and a marker, and the repair
     * pass re-derives the rollup from those raw rows and clears the marker.
     */
    public function test_the_repair_pass_fills_the_hole_the_drain_recorded_and_clears_the_marker(): void
    {
        // Outside BackfillRollupsCommand::SAFETY_MARGIN_SECONDS, or the backfill
        // would refuse the range (covered by the next test).
        $eventTs = time() - 3600;
        $holder = $this->holdBackfillChunkLock(self::MINUTE_TABLE);

        try {
            $this->writer()->write(array_map(fn (int $i): array => $this->sim->makeQuery([
                'trace_id' => "repair-{$i}",
                '_group' => 'contentionrepair',
                'sql' => 'SELECT * FROM widgets',
                'duration' => 700,
                'connection' => 'pgsql',
                'timestamp' => $eventTs + $i,
            ]), range(0, 3)));
        } finally {
            $holder->rollBack();
        }

        $this->assertSame(4, $this->rawCount('repair'));
        $this->assertSame(0, $this->rollupCalls(self::MINUTE_TABLE, 'repair'), 'Precondition: the hole exists.');

        $repaired = $this->runRepair();

        $this->assertSame([self::MINUTE_TABLE], $repaired);
        $this->assertSame(4, $this->rollupCalls(self::MINUTE_TABLE, 'repair'), 'The hole is filled from the raw rows the drain kept.');
        $this->assertSame(4, $this->rollupCalls(self::TIER_TABLE, 'repair'), 'And the chain above it is rebuilt too.');
        $this->assertSame([], $this->repairDebt(), 'A fully repaired marker is cleared.');
    }

    /**
     * The one case where clearing the marker would lose the hole for good:
     * backfill-rollups caps its ceiling at now minus the safety margin, so a floor
     * inside that margin backfills NOTHING while still exiting SUCCESS.
     */
    public function test_a_floor_inside_the_backfill_safety_margin_is_kept_not_cleared(): void
    {
        $floor = gmdate('Y-m-d H:i:s', intdiv(time() - 120, 60) * 60);
        self::$pdo->prepare(
            "INSERT INTO nightowl_settings (key, value, created_at, updated_at) VALUES ('rollup_repair_from', ?, now(), now())"
        )->execute([json_encode([self::MINUTE_TABLE => $floor])]);

        $this->assertSame([], $this->runRepair(), 'Nothing can be repaired inside the margin.');
        $this->assertSame(
            [self::MINUTE_TABLE => $floor],
            $this->repairDebt(),
            'The debt survives verbatim so the next run picks it up.',
        );
    }

    /**
     * A marker naming something that is not a rollup table is never acted on and
     * never discarded — acting on it would run an arbitrary --type, and discarding
     * it would erase the only record that data is missing.
     */
    public function test_an_unrecognised_marker_is_kept_and_not_acted_on(): void
    {
        self::$pdo->prepare(
            "INSERT INTO nightowl_settings (key, value, created_at, updated_at) VALUES ('rollup_repair_from', ?, now(), now())"
        )->execute([json_encode(['nightowl_users' => '2026-01-01 00:00:00'])]);

        $this->assertSame([], $this->runRepair());
        $this->assertSame(['nightowl_users' => '2026-01-01 00:00:00'], $this->repairDebt());
    }

    /**
     * The debt the drain records has to travel to the health payload, or a tenant
     * that never deploys carries a chart-distorting hole in silence — which is the
     * half of the 2.0.0 incident that made it last two hours.
     *
     * Read from the DB rather than remembered in memory ON PURPOSE: the marker is
     * cleared by nightowl:migrate in a DIFFERENT process, so only a re-read lets
     * the health signal go away. Both directions are asserted here.
     */
    public function test_the_recorded_debt_is_readable_for_the_health_payload_and_stops_being_so_once_repaired(): void
    {
        $writer = $this->writer();
        $holder = $this->holdBackfillChunkLock(self::MINUTE_TABLE);

        try {
            $writer->write($this->queryBatch('health'));
        } finally {
            $holder->rollBack();
        }

        $debt = $writer->readRollupRepairDebt();
        $this->assertSame(
            $this->repairDebt(),
            $debt,
            'What the health payload reports must be exactly what the drain wrote — same key, same floor.',
        );
        $this->assertArrayHasKey(self::MINUTE_TABLE, $debt);

        // What a deploy does. The reader must then answer "nothing owed" — an
        // in-memory counter would keep asserting the hole for the daemon's life.
        self::$pdo->exec("DELETE FROM nightowl_settings WHERE key = 'rollup_repair_from'");

        $this->assertSame([], $writer->readRollupRepairDebt(), 'A repaired tenant must stop reporting a debt.');
    }

    /**
     * A hand-edited or older-shape entry stays in the setting (MigrateCommand
     * preserves what it cannot name a backfill for) but must not reach the health
     * payload, whose entire downstream purpose is to print the command that fixes
     * the named table.
     */
    public function test_an_unrecognised_marker_never_reaches_the_health_payload(): void
    {
        self::$pdo->prepare(
            "INSERT INTO nightowl_settings (key, value, created_at, updated_at) VALUES ('rollup_repair_from', ?, now(), now())"
        )->execute([json_encode([
            'nightowl_users' => '2026-01-01 00:00:00',
            self::MINUTE_TABLE => '2026-01-02 00:00:00',
        ])]);

        $this->assertSame(
            [self::MINUTE_TABLE => '2026-01-02 00:00:00'],
            $this->writer()->readRollupRepairDebt(),
            'The recognised table is reported; the one no --type can address is dropped from the payload, not from the setting.',
        );
        $this->assertArrayHasKey('nightowl_users', $this->repairDebt(), 'And the setting itself is untouched.');
    }

    /**
     * A value that is not the map shape — hand-edited, or a shape from a future
     * version — must not throw out of the drain's cleanup tick. null means "could
     * not read", which makes the caller keep its last good value rather than
     * falsely reporting the debt cleared.
     */
    public function test_an_unreadable_marker_reads_as_unknown_not_as_repaired(): void
    {
        self::$pdo->prepare(
            "INSERT INTO nightowl_settings (key, value, created_at, updated_at) VALUES ('rollup_repair_from', ?, now(), now())"
        )->execute(['not json at all']);

        $this->assertNull($this->writer()->readRollupRepairDebt());
    }

    /**
     * MigrateCommand's repair pass, with no full-rebuild list to defer to. Driven
     * directly rather than through `nightowl:migrate` so the assertion is about
     * the repair and not about 69 migrations; it still goes through the real
     * `$this->call('nightowl:backfill-rollups')`, which is the part that matters.
     */
    private function runRepair(): array
    {
        $console = new ConsoleApplication($this->app, new Dispatcher($this->app), 'test');
        $console->add(new BackfillRollupsCommand);

        $command = new MigrateCommand;
        $command->setLaravel($this->app);
        $command->setApplication($console);
        $command->setInput(new ArrayInput([]));
        $command->setOutput(new OutputStyle(new ArrayInput([]), new BufferedOutput));

        $method = new \ReflectionMethod(MigrateCommand::class, 'repairMarkedRollups');

        return $method->invoke($command, $this->app['db']->connection('nightowl'), []);
    }

    /** Minute rollups spanning $days back, $groups distinct query groups, every 3rd minute. */
    private function seedMinuteRollups(int $days, int $groups): void
    {
        for ($d = 0; $d < $days; $d++) {
            self::$pdo->exec("
                INSERT INTO ".self::MINUTE_TABLE."
                    (group_hash, bucket_start, environment, connection, call_count, total_duration,
                     min_duration, max_duration, sql_query, hist_05)
                SELECT 'seed_' || g,
                       date_trunc('minute', now() at time zone 'utc' - ({$d} || ' days')::interval - (m || ' minutes')::interval),
                       'production', 'pgsql', 5, 2500, 300, 900, 'SELECT * FROM seeded', 5
                FROM generate_series(0, 1439, 3) AS m
                CROSS JOIN generate_series(1, {$groups}) AS g
                ON CONFLICT DO NOTHING
            ");
        }
    }

    /**
     * How many chunk transactions a pass reported for $table. Read off the
     * command's own output rather than pg_stat_database, whose xact_commit lags
     * a burst of sub-second transactions by up to a second.
     */
    private function reportedChunks(string $output, string $table): int
    {
        $this->assertMatchesRegularExpression(
            '/'.preg_quote($table, '/').': \d+ rollup rows in \d+ chunk\(s\)/',
            $output,
            "No pass line for {$table}.",
        );
        preg_match('/'.preg_quote($table, '/').': \d+ rollup rows in (\d+) chunk\(s\)/', $output, $m);

        return (int) $m[1];
    }

    private function backfillTiersOnly(): string
    {
        $command = new BackfillRollupsCommand;
        $command->setLaravel($this->app);
        $output = new BufferedOutput;
        $command->run(new ArrayInput(['--type' => self::MINUTE_TABLE, '--tiers-only' => true]), $output);

        return $output->fetch();
    }
}
