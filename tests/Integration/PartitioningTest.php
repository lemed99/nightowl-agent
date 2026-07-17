<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Support\RawPartitions;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * The partitioning machinery end-to-end: populated-table conversion (rename +
 * validated CHECK + ATTACH — the nightowl:partition path), routing of new rows
 * into daily/default children, future-child maintenance, and expired-child
 * discovery for the prune fast path. The REAL raw tables are partitioned by
 * migration 000058 in this suite's DB, so every other integration test also
 * exercises COPY/INSERT through partitioned parents implicitly.
 */
final class PartitioningTest extends TestCase
{
    private static ?PDO $pdo = null;

    public static function setUpBeforeClass(): void
    {
        $host = getenv('NIGHTOWL_TEST_DB_HOST') ?: '127.0.0.1';
        $port = (int) (getenv('NIGHTOWL_TEST_DB_PORT') ?: 5432);
        $database = getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test';
        $username = getenv('NIGHTOWL_TEST_DB_USERNAME') ?: 'nightowl_test';
        $password = getenv('NIGHTOWL_TEST_DB_PASSWORD') ?: 'test123';

        try {
            self::$pdo = new PDO(
                sprintf('pgsql:host=%s;port=%d;dbname=%s', $host, $port, $database),
                $username,
                $password,
            );
            self::$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (\Exception) {
            self::$pdo = null;
        }

        if (self::$pdo) {
            MigrationRunner::migrate($host, $port, $database, $username, $password);
        }
    }

    protected function setUp(): void
    {
        if (self::$pdo === null) {
            $this->markTestSkipped('PostgreSQL not available. Set NIGHTOWL_TEST_DB_* env vars.');
        }

        self::$pdo->exec('DROP TABLE IF EXISTS nightowl_ptest CASCADE');
        self::$pdo->exec('DROP TABLE IF EXISTS nightowl_ptest_phistoric CASCADE');
        self::$pdo->exec('DROP TABLE IF EXISTS nightowl_ptest_pnew CASCADE');
        self::$pdo->exec('DROP TABLE IF EXISTS nightowl_ptest2 CASCADE');
    }

    protected function tearDown(): void
    {
        if (self::$pdo !== null) {
            self::$pdo->exec('DROP TABLE IF EXISTS nightowl_ptest CASCADE');
            self::$pdo->exec('DROP TABLE IF EXISTS nightowl_ptest2 CASCADE');
        }
    }

    private function childExists(string $table, int $dayEpoch): bool
    {
        return (bool) self::$pdo->query(sprintf(
            "SELECT to_regclass('%s') IS NOT NULL",
            RawPartitions::childName($table, $dayEpoch),
        ))->fetchColumn();
    }

    /**
     * RecordWriter::maintainRawPartitions' control flow exactly: ONE transaction
     * spanning the sweep, committed on success and rolled back on a throw. Every
     * assertion about isolation has to be made through this — isolation that only
     * holds when the caller commits regardless is isolation the drain never gets.
     *
     * @param  list<string>  $tables
     * @return list<string> the sweep's reported failures
     */
    private function tick(array $tables): array
    {
        self::$pdo->beginTransaction();

        try {
            $failures = RawPartitions::ensureFutureChildren(self::$pdo, $tables);
            self::$pdo->commit();

            return $failures;
        } catch (\Throwable $e) {
            if (self::$pdo->inTransaction()) {
                self::$pdo->rollBack();
            }

            throw $e;
        }
    }

    /** Index definitions with the table's own name masked, so children compare. */
    private function indexShape(string $child): array
    {
        return self::$pdo->query(sprintf(
            "SELECT regexp_replace(indexdef, '%s', 'CHILD', 'g') FROM pg_indexes
             WHERE tablename = '%s' ORDER BY indexname",
            $child,
            $child,
        ))->fetchAll(PDO::FETCH_COLUMN);
    }

    public function test_real_raw_tables_are_partitioned_by_migration(): void
    {
        $this->assertTrue(RawPartitions::isPartitioned(self::$pdo, 'nightowl_requests'));
        $this->assertTrue(RawPartitions::isPartitioned(self::$pdo, 'nightowl_queries'));
        // Logs partitions too (000060 converts its varchar created_at first).
        $this->assertTrue(RawPartitions::isPartitioned(self::$pdo, 'nightowl_logs'));

        $type = self::$pdo->query(
            "SELECT data_type FROM information_schema.columns WHERE table_name = 'nightowl_logs' AND column_name = 'created_at'"
        )->fetchColumn();
        $this->assertSame('timestamp without time zone', $type);
    }

    public function test_populated_table_converts_in_place_with_rows_preserved(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            trace_id varchar(255),
            duration integer,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        self::$pdo->exec('CREATE INDEX nightowl_ptest_trace_id_index ON nightowl_ptest (trace_id)');
        self::$pdo->exec("INSERT INTO nightowl_ptest (trace_id, duration, created_at)
            SELECT 'pt-' || i, i, now() - (i || ' days')::interval FROM generate_series(0, 29) i");

        RawPartitions::convert(self::$pdo, 'nightowl_ptest');

        $this->assertTrue(RawPartitions::isPartitioned(self::$pdo, 'nightowl_ptest'));

        // Every pre-conversion row survived, inside the attached historic child.
        $this->assertSame(30, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest')->fetchColumn());
        $this->assertSame(30, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest_phistoric')->fetchColumn());

        // The secondary index was replayed onto the parent (cascading to children).
        $replayed = (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'nightowl_ptest' AND indexname = 'nightowl_ptest_trace_id_index_pt'"
        )->fetchColumn();
        $this->assertSame(1, $replayed, 'secondary indexes must be replayed onto the parent');

        // A tomorrow-dated row routes to its daily child, not historic/default.
        self::$pdo->exec("INSERT INTO nightowl_ptest (trace_id, duration, created_at)
            VALUES ('pt-next', 1, (now() at time zone 'utc')::date + interval '1 day 1 hour')");
        $child = RawPartitions::childName('nightowl_ptest', intdiv(time(), 86400) * 86400 + 86400);
        $this->assertSame(1, (int) self::$pdo->query("SELECT COUNT(*) FROM {$child}")->fetchColumn());

        // A far-backdated row (older than any child) lands in DEFAULT, never lost.
        self::$pdo->exec("INSERT INTO nightowl_ptest (trace_id, duration, created_at)
            VALUES ('pt-old', 1, now() + interval '30 days')"); // future beyond children -> default
        $this->assertSame(1, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest_pdefault')->fetchColumn());

        // id sequence survived the swap: next insert gets a fresh id.
        $maxId = (int) self::$pdo->query('SELECT MAX(id) FROM nightowl_ptest')->fetchColumn();
        $this->assertGreaterThanOrEqual(31, $maxId);
    }

    /**
     * The nightowl:partition logs path: a POPULATED varchar-created_at table
     * (nulls and all) is type-normalised then converted, rows preserved.
     */
    public function test_populated_varchar_created_at_table_converts(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            message text,
            created_at varchar(255)
        )');
        self::$pdo->exec("INSERT INTO nightowl_ptest (message, created_at)
            SELECT 'm-' || i, to_char(now() - (i || ' days')::interval, 'YYYY-MM-DD HH24:MI:SS')
            FROM generate_series(0, 9) i");
        // A NULL and an empty-string created_at — the invisible legacy rows.
        self::$pdo->exec("INSERT INTO nightowl_ptest (message, created_at) VALUES ('m-null', NULL), ('m-empty', '')");

        RawPartitions::convert(self::$pdo, 'nightowl_ptest');

        $this->assertTrue(RawPartitions::isPartitioned(self::$pdo, 'nightowl_ptest'));
        $this->assertSame(12, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest')->fetchColumn());

        // NULL/empty rows became epoch-dated (age out via prune), not lost.
        $epoch = (int) self::$pdo->query(
            "SELECT COUNT(*) FROM nightowl_ptest WHERE created_at = '1970-01-01 00:00:00'"
        )->fetchColumn();
        $this->assertSame(2, $epoch);
    }

    public function test_expired_children_are_discovered_and_droppable(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        RawPartitions::convert(self::$pdo, 'nightowl_ptest');

        // Manufacture an expired daily child (20 days ago) with rows.
        $old = intdiv(time(), 86400) * 86400 - 20 * 86400;
        RawPartitions::ensureDailyChild(self::$pdo, 'nightowl_ptest', $old);
        self::$pdo->exec(sprintf(
            "INSERT INTO nightowl_ptest (created_at) VALUES ('%s')",
            gmdate('Y-m-d 12:00:00', $old),
        ));

        $cutoff = gmdate('Y-m-d H:i:s', time() - 14 * 86400);
        $expired = RawPartitions::expiredChildren(self::$pdo, 'nightowl_ptest', $cutoff);

        $this->assertSame([RawPartitions::childName('nightowl_ptest', $old)], $expired);

        // Fresh children and the historic/default partitions are never listed.
        $this->assertNotContains('nightowl_ptest_pdefault', $expired);

        self::$pdo->exec('DROP TABLE '.$expired[0]);
        $this->assertSame(0, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest')->fetchColumn());
    }

    /**
     * The space-reclaim endgame: once prune's row-DELETE empties the historic
     * partition, dropEmptyHistoric unlinks it — and refuses while rows remain.
     */
    public function test_empty_historic_partition_is_dropped(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        self::$pdo->exec("INSERT INTO nightowl_ptest (created_at)
            SELECT now() - (i || ' days')::interval FROM generate_series(1, 5) i");

        RawPartitions::convert(self::$pdo, 'nightowl_ptest');

        // Rows still present → refuses.
        $this->assertFalse(RawPartitions::dropEmptyHistoric(self::$pdo, 'nightowl_ptest'));

        // Prune's row-DELETE empties it → drops, space unlinked.
        self::$pdo->exec('DELETE FROM nightowl_ptest_phistoric');
        $this->assertTrue(RawPartitions::dropEmptyHistoric(self::$pdo, 'nightowl_ptest'));
        $this->assertFalse((bool) self::$pdo->query(
            "SELECT to_regclass('nightowl_ptest_phistoric') IS NOT NULL"
        )->fetchColumn());

        // Idempotent: second call is a clean no-op.
        $this->assertFalse(RawPartitions::dropEmptyHistoric(self::$pdo, 'nightowl_ptest'));
    }

    public function test_unpartitioned_populated_reports_plain_tables_with_rows(): void
    {
        // The suite's real tables are all partitioned → empty report.
        $this->assertSame([], RawPartitions::unpartitionedPopulated(self::$pdo));

        // Positive path: a plain POPULATED table is reported; a plain EMPTY
        // one is not. The api's storage() panel probes rows the same exact
        // way, so both surfaces agree on any given table.
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');

        $this->assertSame([], RawPartitions::unpartitionedPopulated(self::$pdo, ['nightowl_ptest']),
            'an empty plain table must not be reported');

        self::$pdo->exec('INSERT INTO nightowl_ptest DEFAULT VALUES');
        $this->assertSame(['nightowl_ptest'], RawPartitions::unpartitionedPopulated(self::$pdo, ['nightowl_ptest']),
            'a populated plain table must be reported');

        RawPartitions::convert(self::$pdo, 'nightowl_ptest');
        $this->assertSame([], RawPartitions::unpartitionedPopulated(self::$pdo, ['nightowl_ptest']),
            'conversion clears the report');
    }

    /** convert() retries cleanly after a simulated mid-run kill. */
    public function test_convert_recovers_from_interrupted_previous_attempt(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        self::$pdo->exec('INSERT INTO nightowl_ptest DEFAULT VALUES');
        // Simulate the leftover of a killed earlier run.
        self::$pdo->exec('CREATE TABLE nightowl_ptest_pnew (LIKE nightowl_ptest) PARTITION BY RANGE (created_at)');

        RawPartitions::convert(self::$pdo, 'nightowl_ptest');

        $this->assertTrue(RawPartitions::isPartitioned(self::$pdo, 'nightowl_ptest'));
        $this->assertSame(1, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest')->fetchColumn());
    }

    /**
     * The maintenance tick on conversion day: the historic child still covers
     * today, so today's child would OVERLAP it (42P17 — IF NOT EXISTS only
     * suppresses a name clash). The sweep must skip the covered day, not die on
     * it before the other ten tables get any children at all.
     */
    public function test_sweep_skips_days_the_historic_partition_still_covers(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        // Populated → convert ATTACHes _phistoric covering MINVALUE..TOMORROW.
        self::$pdo->exec("INSERT INTO nightowl_ptest (created_at)
            SELECT now() - (i || ' days')::interval FROM generate_series(1, 3) i");
        RawPartitions::convert(self::$pdo, 'nightowl_ptest');

        $failures = RawPartitions::ensureFutureChildren(self::$pdo, ['nightowl_ptest']);

        $today = intdiv(time(), 86400) * 86400;
        // The child is absent whether the day was skipped or attempted and
        // rejected, so only a clean sweep tells those two apart.
        $this->assertSame([], $failures, "a day inside _phistoric's range must not be attempted at all");
        $this->assertFalse($this->childExists('nightowl_ptest', $today));

        for ($d = 1; $d <= RawPartitions::DAYS_AHEAD; $d++) {
            $this->assertTrue($this->childExists('nightowl_ptest', $today + $d * 86400),
                "day +{$d} is beyond the historic bound and must have a child");
        }

        // Today's writes keep landing in historic until the bound passes.
        self::$pdo->exec('INSERT INTO nightowl_ptest DEFAULT VALUES');
        $this->assertSame(4, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest_phistoric')->fetchColumn());
    }

    /**
     * The >7d drain gap: rows for a day with no child of its own COPY into
     * _pdefault, and the sweep's CREATE is then rejected (23514). Left alone
     * every raw row stays in _pdefault forever and prune's DROP fast path never
     * fires again — so the sweep adopts them into the child it creates.
     */
    public function test_sweep_adopts_rows_stranded_in_the_default_partition(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            trace_id varchar(255),
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        self::$pdo->exec('CREATE INDEX nightowl_ptest_trace_id_index ON nightowl_ptest (trace_id)');
        RawPartitions::convert(self::$pdo, 'nightowl_ptest');

        // Drop a child inside the window, then drain a row for that day: with no
        // child to route to it lands in DEFAULT (the >7d-stale-window state).
        $today = intdiv(time(), 86400) * 86400;
        $stranded = $today + 3 * 86400;
        self::$pdo->exec('DROP TABLE '.RawPartitions::childName('nightowl_ptest', $stranded));
        self::$pdo->exec(sprintf(
            "INSERT INTO nightowl_ptest (trace_id, created_at) VALUES ('pt-stranded', '%s')",
            gmdate('Y-m-d 12:00:00', $stranded),
        ));
        $this->assertSame(1, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest_pdefault')->fetchColumn());

        $this->assertSame([], RawPartitions::ensureFutureChildren(self::$pdo, ['nightowl_ptest']));

        $child = RawPartitions::childName('nightowl_ptest', $stranded);
        $this->assertSame(1, (int) self::$pdo->query("SELECT COUNT(*) FROM {$child}")->fetchColumn(),
            'the stranded row must be adopted into the child the sweep creates');
        $this->assertSame(0, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest_pdefault')->fetchColumn());
        // Adoption moves rows, it never loses or duplicates them.
        $this->assertSame(1, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest')->fetchColumn());
        $this->assertSame('pt-stranded', self::$pdo->query('SELECT trace_id FROM nightowl_ptest')->fetchColumn());

        // A child that got there by ATTACH must be indistinguishable from one
        // CREATE ... PARTITION OF made, or the day silently lost its indexes.
        $this->assertSame(
            $this->indexShape(RawPartitions::childName('nightowl_ptest', $today + 4 * 86400)),
            $this->indexShape($child),
            'the adopted child must carry the same indexes as a plainly-created one',
        );
        $this->assertSame(0, (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = '{$child}'::regclass AND contype = 'c'"
        )->fetchColumn(), 'the scan-skipping CHECK must not outlive the attach');

        // Routing keeps working through the child that was attached, not created.
        self::$pdo->exec(sprintf(
            "INSERT INTO nightowl_ptest (trace_id, created_at) VALUES ('pt-after', '%s')",
            gmdate('Y-m-d 23:00:00', $stranded),
        ));
        $this->assertSame(2, (int) self::$pdo->query("SELECT COUNT(*) FROM {$child}")->fetchColumn());
    }

    /**
     * Adoption in the shape the drain actually runs it — inside the tick's
     * transaction, where it must not try to open one of its own (PDO cannot
     * nest) and its work must survive the tick's commit.
     */
    public function test_sweep_adopts_stranded_rows_inside_the_callers_transaction(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            trace_id varchar(255),
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        RawPartitions::convert(self::$pdo, 'nightowl_ptest');

        $today = intdiv(time(), 86400) * 86400;
        $stranded = $today + 3 * 86400;
        self::$pdo->exec('DROP TABLE '.RawPartitions::childName('nightowl_ptest', $stranded));
        self::$pdo->exec(sprintf(
            "INSERT INTO nightowl_ptest (trace_id, created_at) VALUES ('pt-stranded', '%s')",
            gmdate('Y-m-d 12:00:00', $stranded),
        ));

        $this->assertSame([], $this->tick(['nightowl_ptest']));

        $child = RawPartitions::childName('nightowl_ptest', $stranded);
        $this->assertSame(1, (int) self::$pdo->query("SELECT COUNT(*) FROM {$child}")->fetchColumn(),
            'the adoption must survive the tick that made it');
        $this->assertSame(0, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest_pdefault')->fetchColumn());
        $this->assertSame(1, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest')->fetchColumn());
        $this->assertSame('pt-stranded', self::$pdo->query('SELECT trace_id FROM nightowl_ptest')->fetchColumn());
    }

    /**
     * A table (or day) the sweep genuinely cannot fix must cost only itself: the
     * remaining tables still get their children, and the failure surfaces rather
     * than wedging every later tick silently.
     */
    public function test_one_unfixable_table_neither_aborts_the_sweep_nor_passes_silently(): void
    {
        $today = intdiv(time(), 86400) * 86400;

        foreach (['nightowl_ptest', 'nightowl_ptest2'] as $table) {
            self::$pdo->exec("CREATE TABLE {$table} (
                id bigserial PRIMARY KEY,
                created_at timestamp DEFAULT CURRENT_TIMESTAMP
            )");
            RawPartitions::convert(self::$pdo, $table);
        }

        // An unmanaged child squatting on today's range: today's CREATE overlaps
        // (42P17) and no row-moving can resolve it.
        self::$pdo->exec('DROP TABLE '.RawPartitions::childName('nightowl_ptest', $today));
        self::$pdo->exec(sprintf(
            "CREATE TABLE nightowl_ptest_psquat PARTITION OF nightowl_ptest FOR VALUES FROM ('%s') TO ('%s')",
            gmdate('Y-m-d 00:00:00', $today),
            gmdate('Y-m-d 00:00:00', $today + 86400),
        ));
        // A later day on the SAME table, and a later table entirely — both healthy.
        self::$pdo->exec('DROP TABLE '.RawPartitions::childName('nightowl_ptest', $today + 5 * 86400));
        self::$pdo->exec('DROP TABLE '.RawPartitions::childName('nightowl_ptest2', $today));

        $logFile = tempnam(sys_get_temp_dir(), 'nightowl-partition-log');
        $previous = ini_set('error_log', $logFile);

        try {
            $failures = RawPartitions::ensureFutureChildren(self::$pdo, ['nightowl_ptest', 'nightowl_ptest2']);
        } finally {
            ini_set('error_log', $previous === false ? '' : $previous);
        }

        $logged = (string) file_get_contents($logFile);
        @unlink($logFile);

        $this->assertCount(1, $failures, 'only the squatted day may be reported');
        $this->assertStringContainsString(RawPartitions::childName('nightowl_ptest', $today), $failures[0]);

        // The drain has no other window onto this: the tick logs whatever the
        // sweep hands back, and the sweep must not hand back silence.
        $this->assertStringContainsString('daily partition(s) not created', $logged);
        $this->assertStringContainsString(RawPartitions::childName('nightowl_ptest', $today), $logged);

        $this->assertTrue($this->childExists('nightowl_ptest', $today + 5 * 86400),
            'a failed day must not abort the days after it');
        $this->assertTrue($this->childExists('nightowl_ptest2', $today),
            'a failed table must not abort the tables after it');
    }

    /**
     * The drain's tick rolls its whole transaction back on a throw, so a sweep
     * that raised for one unfixable table would take every other table's children
     * down with it — every tick, for as long as that table stayed broken, which is
     * forever. The squatter here is unfixable by design; ptest2 must be maintained
     * anyway, and still be maintained on the tick after that.
     */
    public function test_a_permanently_unfixable_table_never_wedges_the_other_tables(): void
    {
        $today = intdiv(time(), 86400) * 86400;

        foreach (['nightowl_ptest', 'nightowl_ptest2'] as $table) {
            self::$pdo->exec("CREATE TABLE {$table} (
                id bigserial PRIMARY KEY,
                trace_id varchar(255),
                created_at timestamp DEFAULT CURRENT_TIMESTAMP
            )");
            RawPartitions::convert(self::$pdo, $table);
        }

        self::$pdo->exec('DROP TABLE '.RawPartitions::childName('nightowl_ptest', $today));
        self::$pdo->exec(sprintf(
            "CREATE TABLE nightowl_ptest_psquat PARTITION OF nightowl_ptest FOR VALUES FROM ('%s') TO ('%s')",
            gmdate('Y-m-d 00:00:00', $today),
            gmdate('Y-m-d 00:00:00', $today + 86400),
        ));

        // The healthy table needs real work done for it: a missing child AND rows
        // that drained into DEFAULT while it was missing.
        $stranded = $today + 5 * 86400;
        self::$pdo->exec('DROP TABLE '.RawPartitions::childName('nightowl_ptest2', $stranded));
        self::$pdo->exec(sprintf(
            "INSERT INTO nightowl_ptest2 (trace_id, created_at) VALUES ('pt2-stranded', '%s')",
            gmdate('Y-m-d 12:00:00', $stranded),
        ));

        foreach ([1, 2] as $tickNumber) {
            $failures = $this->tick(['nightowl_ptest', 'nightowl_ptest2']);

            $this->assertCount(1, $failures, "tick {$tickNumber}: only the squatted day may fail");
            $this->assertTrue($this->childExists('nightowl_ptest2', $stranded),
                "tick {$tickNumber}: the healthy table's child must survive the tick");
            $this->assertSame(0, (int) self::$pdo->query(
                'SELECT COUNT(*) FROM nightowl_ptest2_pdefault'
            )->fetchColumn(), "tick {$tickNumber}: the healthy table's stranded rows must be adopted, not left in DEFAULT");
            $this->assertSame(1, (int) self::$pdo->query(
                'SELECT COUNT(*) FROM nightowl_ptest2'
            )->fetchColumn(), "tick {$tickNumber}: adoption must not lose or duplicate the row");
        }
    }

    /**
     * The real call path: the drain's tick holds a transaction-scoped advisory
     * lock, so the sweep runs inside a transaction — where the first error
     * aborts the block and every later statement dies 25P02 unless each attempt
     * is savepointed. Both the isolation and the reported cause depend on it.
     */
    public function test_sweep_isolates_failures_when_run_inside_a_transaction(): void
    {
        $today = intdiv(time(), 86400) * 86400;

        foreach (['nightowl_ptest', 'nightowl_ptest2'] as $table) {
            self::$pdo->exec("CREATE TABLE {$table} (
                id bigserial PRIMARY KEY,
                created_at timestamp DEFAULT CURRENT_TIMESTAMP
            )");
            RawPartitions::convert(self::$pdo, $table);
        }

        self::$pdo->exec('DROP TABLE '.RawPartitions::childName('nightowl_ptest', $today));
        self::$pdo->exec(sprintf(
            "CREATE TABLE nightowl_ptest_psquat PARTITION OF nightowl_ptest FOR VALUES FROM ('%s') TO ('%s')",
            gmdate('Y-m-d 00:00:00', $today),
            gmdate('Y-m-d 00:00:00', $today + 86400),
        ));
        self::$pdo->exec('DROP TABLE '.RawPartitions::childName('nightowl_ptest', $today + 5 * 86400));
        self::$pdo->exec('DROP TABLE '.RawPartitions::childName('nightowl_ptest2', $today));

        $failures = $this->tick(['nightowl_ptest', 'nightowl_ptest2']);

        // Without a savepoint per attempt this is 9 entries: the one real 42P17
        // and eight later statements dying 25P02 on the block it poisoned.
        $this->assertCount(1, $failures, 'exactly one day genuinely failed');
        $this->assertStringContainsString(RawPartitions::childName('nightowl_ptest', $today), $failures[0]);
        $this->assertStringContainsString('42P17', $failures[0]);
        $this->assertStringNotContainsString('25P02', implode(' ', $failures),
            'only the real cause may be reported — not a cascade of aborted-transaction noise');

        $this->assertTrue($this->childExists('nightowl_ptest', $today + 5 * 86400),
            'a failed day must not abort the days after it, even inside a transaction');
        $this->assertTrue($this->childExists('nightowl_ptest2', $today),
            'a failed table must not abort the tables after it, even inside a transaction');
    }

    public function test_ensure_future_children_is_idempotent(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        RawPartitions::convert(self::$pdo, 'nightowl_ptest');

        $count = fn (): int => (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent WHERE p.relname = 'nightowl_ptest'"
        )->fetchColumn();

        // An empty table converts by rebuild, so it has no historic child and
        // convert() already created today's: DAYS_AHEAD + today, plus DEFAULT.
        $before = $count();
        $this->assertSame(RawPartitions::DAYS_AHEAD + 2, $before);
        $this->assertTrue($this->childExists('nightowl_ptest', intdiv(time(), 86400) * 86400));

        RawPartitions::ensureDailyChild(self::$pdo, 'nightowl_ptest', time());
        RawPartitions::ensureDailyChild(self::$pdo, 'nightowl_ptest', time());

        $this->assertSame($before, $count(), 'neither call may error or duplicate');
    }
}
