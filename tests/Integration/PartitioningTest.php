<?php

namespace NightOwl\Tests\Integration;

use Illuminate\Container\Container;
use Illuminate\Database\Capsule\Manager as Capsule;
use Illuminate\Support\Facades\Facade;
use NightOwl\Commands\PartitionCommand;
use NightOwl\Support\ConversionInProgressException;
use NightOwl\Support\PoolerAffinityException;
use NightOwl\Support\RawPartitions;
use PDO;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

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

    private static string $dsn = '';

    private static string $username = '';

    private static string $password = '';

    /** @var array<string, mixed> */
    private static array $config = [];

    public static function setUpBeforeClass(): void
    {
        $host = getenv('NIGHTOWL_TEST_DB_HOST') ?: '127.0.0.1';
        $port = (int) (getenv('NIGHTOWL_TEST_DB_PORT') ?: 5432);
        $database = getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test';
        $username = getenv('NIGHTOWL_TEST_DB_USERNAME') ?: 'nightowl_test';
        $password = getenv('NIGHTOWL_TEST_DB_PASSWORD') ?: 'test123';

        self::$dsn = sprintf('pgsql:host=%s;port=%d;dbname=%s', $host, $port, $database);
        self::$username = $username;
        self::$password = $password;

        // The command reaches PostgreSQL through the `nightowl` Laravel
        // connection, never through a PDO a test can hand it — keep the parts.
        self::$config = [
            'driver' => 'pgsql',
            'host' => $host,
            'port' => $port,
            'database' => $database,
            'username' => $username,
            'password' => $password,
            'charset' => 'utf8',
            'prefix' => '',
            'schema' => 'public',
        ];

        try {
            self::$pdo = new PDO(self::$dsn, $username, $password);
            self::$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (\Exception) {
            self::$pdo = null;
        }

        if (self::$pdo) {
            MigrationRunner::migrate($host, $port, $database, $username, $password);
        }
    }

    /**
     * Re-open the class-wide handle if a previous test left it unusable.
     *
     * Several tests here deliberately destroy a backend to reproduce what the
     * production code has to survive: holder sessions armed with
     * idle_session_timeout / idle_in_transaction_session_timeout, and outright
     * pg_terminate_backend. When the victim happens to be THIS handle rather
     * than the holder's, every remaining test in the class died in setUp with
     * SQLSTATE[HY000] "no connection to the server" — 15-20 errors attributed to
     * tests that were fine, at a different first casualty on each run, which is
     * a suite that cannot substantiate anything it asserts. An aborted
     * transaction left behind by a failed test does the same thing more quietly
     * (25P02 on the next statement), and reconnecting clears that too.
     */
    private static function ensureLiveConnection(): void
    {
        if (self::$pdo === null) {
            return;
        }

        try {
            self::$pdo->query('SELECT 1');

            if (self::$pdo->inTransaction()) {
                self::$pdo->rollBack();
            }

            return;
        } catch (\Throwable) {
            // Dead or poisoned — replaced below.
        }

        try {
            self::$pdo = new PDO(self::$dsn, self::$username, self::$password);
            self::$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (\Throwable) {
            self::$pdo = null;
        }
    }

    /**
     * Drop every fixture relation, discovered from the catalog rather than
     * hand-listed.
     *
     * The hand-listed version named the parent, _phistoric and _pnew only. A
     * test killed mid-conversion leaves more than that — _pdefault and dated
     * _pYYYYMMDD children, which survive whenever they are orphaned from the
     * parent whose CASCADE would have taken them — and the next run's
     * CREATE TABLE nightowl_ptest then died SQLSTATE[23505] on
     * pg_type_typname_nsp_index. That is a red suite until somebody purges
     * nightowl_ptest% by hand, so the sweep does it instead. Parents first, so
     * CASCADE claims the children and the per-child DROP is the no-op.
     */
    private static function dropFixtures(): void
    {
        $names = self::$pdo->query(
            "SELECT c.relname
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'public'
               AND c.relkind IN ('r', 'p')
               AND c.relname LIKE 'nightowl\\_ptest%'
             ORDER BY (c.relkind = 'p') DESC, c.relname"
        )->fetchAll(PDO::FETCH_COLUMN);

        foreach ($names as $name) {
            self::$pdo->exec("DROP TABLE IF EXISTS {$name} CASCADE");
        }
    }

    protected function setUp(): void
    {
        self::ensureLiveConnection();

        if (self::$pdo === null) {
            $this->markTestSkipped('PostgreSQL not available. Set NIGHTOWL_TEST_DB_* env vars.');
        }

        self::dropFixtures();
    }

    protected function tearDown(): void
    {
        self::ensureLiveConnection();

        if (self::$pdo !== null) {
            self::dropFixtures();
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

    /**
     * RecordWriter::healRawPartitionLeftovers' control flow: the leftover sweep
     * with NO caller transaction, so healConversionLeftovers takes one short
     * transaction PER CANDIDATE TABLE — separate from the hourly child sweep
     * above, and separate from each other.
     *
     * They are deliberately not one call, and this is deliberately not one
     * transaction. Run inside the hourly transaction, the heal held each table's
     * conversion key from the moment it healed until the whole 11-table window
     * committed, and an operator re-running nightowl:partition in that window was
     * refused against a peer that does not exist. Run inside ANY caller
     * transaction, its per-candidate begin/commit is skipped and every healed
     * table's ACCESS EXCLUSIVE is held to that one commit.
     *
     * @param  list<string>  $tables
     * @return list<string> the tables healed
     */
    private function healTick(array $tables): array
    {
        return RawPartitions::healConversionLeftovers(self::$pdo, $tables);
    }

    /**
     * nightowl:partition against the real test database, exactly as artisan runs
     * it. The command resolves its connection through the DB facade, so the only
     * way to drive its control flow is to give it one: a Capsule bound as `db` on
     * a container that can answer runningUnitTests() (Illuminate\Console\Command
     * ::run asks the application that before it ever reaches handle()). The
     * package ships no Testbench, and what is asserted here — which tables the
     * command delegates for, and what it exits — exists nowhere else.
     *
     * $pdo replaces the connection's handle after it is built, for the fixtures
     * that need the command's OWN reads to fail.
     *
     * @return array{0: int, 1: string} exit code, buffered output
     */
    private function runPartitionCommand(string $table, ?PDO $pdo = null): array
    {
        $app = new class extends Container
        {
            public function runningUnitTests(): bool
            {
                return true;
            }
        };

        $capsule = new Capsule($app);
        $capsule->addConnection(self::$config, 'nightowl');
        $app->instance('db', $capsule->getDatabaseManager());
        Facade::setFacadeApplication($app);

        if ($pdo !== null) {
            $capsule->getConnection('nightowl')->setPdo($pdo);
        }

        try {
            $command = new PartitionCommand;
            $command->setLaravel($app);
            $output = new BufferedOutput;

            return [$command->run(new ArrayInput(['--table' => $table]), $output), $output->fetch()];
        } finally {
            // Facade state is static and every other test in this suite uses raw
            // PDO — an application left bound would outlive the test.
            Facade::clearResolvedInstances();
            Facade::setFacadeApplication(null);
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

        // A row past the frozen historic bound routes to its daily child, not
        // historic/default. The bound is the SECOND midnight ahead — the
        // conversion needs a full day of headroom before its own {t}_hist_ck can
        // start rejecting live drain rows — so the boundary day is the first day
        // with a child of its own. Derived, never hardcoded: two copies of the
        // rule is how the gap count drifted in the first place.
        $firstDaily = RawPartitions::historicBoundary(time());
        self::$pdo->exec(sprintf(
            "INSERT INTO nightowl_ptest (trace_id, duration, created_at) VALUES ('pt-next', 1, '%s')",
            gmdate('Y-m-d 01:00:00', $firstDaily),
        ));
        $child = RawPartitions::childName('nightowl_ptest', $firstDaily);
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
     * The field incident (tinybit.farm, 2026-07-18): a deploy-pipeline
     * nightowl:partition run still in flight while the operator ran it by
     * hand. Pre-fix, the loser's freshly-created _pnew was dropped out from
     * under it ("retry from scratch" hardening) and it died 42P01 mid
     * index-replay. Now the whole _pnew lifecycle sits under a per-table
     * advisory xact lock: the loser is refused with a clear message and
     * leaves NO trace; other tables and the eventual retry are unaffected.
     */
    public function test_concurrent_conversion_is_refused_not_sabotaged(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        self::$pdo->exec("INSERT INTO nightowl_ptest (created_at)
            SELECT now() - (i || ' days')::interval FROM generate_series(1, 5) i");

        // The "peer": a second connection holding the table's conversion lock.
        // Session-scoped rather than xact-scoped (same advisory keyspace, so it
        // still blocks convert's pg_try_advisory_xact_lock) because an open
        // peer TRANSACTION would make convert's CREATE INDEX CONCURRENTLY wait
        // on its virtualxid forever — a real peer's lock-holding transaction
        // lasts milliseconds, but a test would deadlock itself.
        $peer = new PDO(self::$dsn, self::$username, self::$password);
        $peer->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $lock = $peer->prepare('SELECT pg_try_advisory_lock(hashtext(?))');
        $lock->execute(['nightowl_partition:nightowl_ptest']);
        $this->assertTrue((bool) $lock->fetchColumn(), 'peer must hold the lock for the scenario to be real');

        try {
            RawPartitions::convert(self::$pdo, 'nightowl_ptest');
            $this->fail('convert must refuse while another run holds the conversion lock');
        } catch (ConversionInProgressException $e) {
            $this->assertStringContainsString('nightowl_ptest', $e->getMessage());
        }

        // Refusal leaves no trace: table still plain, rows intact, no _pnew.
        $this->assertFalse(RawPartitions::isPartitioned(self::$pdo, 'nightowl_ptest'));
        $this->assertSame(5, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest')->fetchColumn());
        $this->assertFalse((bool) self::$pdo->query(
            "SELECT to_regclass('nightowl_ptest_pnew') IS NOT NULL"
        )->fetchColumn(), 'a refused run must not strand a _pnew');

        // "No trace" has to mean the LIVE TABLE too, not just the absence of a
        // _pnew: the refused run must not have run any of the prep DDL. A
        // dropped PK would silently deprotect the table, and a stranded
        // validated hist_ck starts rejecting every drain row (23514) the moment
        // its frozen boundary passes — a silent write outage hours later.
        $this->assertSame(1, (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'nightowl_ptest'::regclass AND contype = 'p'"
        )->fetchColumn(), 'a refused run must not drop the live table\'s primary key');
        $this->assertSame(0, (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'nightowl_ptest'::regclass AND conname = 'nightowl_ptest_hist_ck'"
        )->fetchColumn(), 'a refused run must not strand a boundary CHECK on the live table');

        // Proof the table is still writable at any timestamp — the 23514 a
        // stranded CHECK would cause is invisible to a plain row count.
        self::$pdo->exec("INSERT INTO nightowl_ptest (created_at) VALUES (now() + interval '3 days')");
        self::$pdo->exec("DELETE FROM nightowl_ptest WHERE created_at > now() + interval '2 days'");

        // The lock is per-table: a different table converts while it is held.
        self::$pdo->exec('CREATE TABLE nightowl_ptest2 (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        RawPartitions::convert(self::$pdo, 'nightowl_ptest2');
        $this->assertTrue(RawPartitions::isPartitioned(self::$pdo, 'nightowl_ptest2'));

        // Peer finishes → the same conversion succeeds, rows preserved.
        $unlock = $peer->prepare('SELECT pg_advisory_unlock(hashtext(?))');
        $unlock->execute(['nightowl_partition:nightowl_ptest']);
        RawPartitions::convert(self::$pdo, 'nightowl_ptest');
        $this->assertTrue(RawPartitions::isPartitioned(self::$pdo, 'nightowl_ptest'));
        $this->assertSame(5, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest')->fetchColumn());
    }

    /**
     * The case convert() cannot clean up after itself: a run SIGKILLed between
     * VALIDATE and the swap (deploy step times out, container dies) leaves a
     * validated frozen-boundary CHECK on the LIVE table. Harmless until the
     * boundary passes, then every drain write fails 23514 — silent, total, and
     * hours removed from the command anyone would blame. The drain's
     * maintenance tick has to find and strip it.
     */
    public function test_maintenance_strips_a_killed_conversions_expired_check(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        self::$pdo->exec("INSERT INTO nightowl_ptest (created_at) VALUES (now() - interval '1 day')");

        // Exactly what prep leaves behind, with a boundary that has since passed.
        $expired = gmdate('Y-m-d 00:00:00', intdiv(time(), 86400) * 86400);
        self::$pdo->exec(
            "ALTER TABLE nightowl_ptest ADD CONSTRAINT nightowl_ptest_hist_ck
             CHECK (created_at IS NOT NULL AND created_at < '{$expired}')"
        );

        // The outage it causes, demonstrated before the fix runs.
        try {
            self::$pdo->exec('INSERT INTO nightowl_ptest DEFAULT VALUES');
            $this->fail('the stale CHECK must reject a now-dated row for this test to mean anything');
        } catch (\PDOException $e) {
            $this->assertSame('23514', $e->getCode());
        }

        $healed = RawPartitions::healConversionLeftovers(self::$pdo, ['nightowl_ptest']);

        $this->assertSame(['nightowl_ptest'], $healed);
        self::$pdo->exec('INSERT INTO nightowl_ptest DEFAULT VALUES');
        $this->assertSame(2, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest')->fetchColumn());

        // Idempotent: nothing left to heal on the next tick.
        $this->assertSame([], RawPartitions::healConversionLeftovers(self::$pdo, ['nightowl_ptest']));
    }

    /**
     * A LIVE conversion's CHECK must survive the sweep — its boundary is the
     * second UTC midnight ahead, so it accepts today's rows and is doing its
     * job. A sweep that stripped it would force ATTACH into a full-table scan
     * under ACCESS EXCLUSIVE, turning maintenance into the outage it prevents.
     */
    public function test_maintenance_leaves_a_live_conversions_check_alone(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        // The boundary a live conversion would actually have frozen — never a
        // hand-written "tomorrow". Under the old rule a run that started in the
        // seconds before UTC midnight planted a boundary that had ALREADY passed
        // by the time the sweep probed it, so the sweep correctly called it
        // expired and this test failed as a false positive once a day.
        $boundary = gmdate('Y-m-d 00:00:00', RawPartitions::historicBoundary(time()));
        self::$pdo->exec(
            "ALTER TABLE nightowl_ptest ADD CONSTRAINT nightowl_ptest_hist_ck
             CHECK (created_at IS NOT NULL AND created_at < '{$boundary}')"
        );

        $this->assertSame([], RawPartitions::healConversionLeftovers(self::$pdo, ['nightowl_ptest']));
        $this->assertSame(1, (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'nightowl_ptest'::regclass AND conname = 'nightowl_ptest_hist_ck'"
        )->fetchColumn(), "a live conversion's CHECK must not be swept away");
    }

    /**
     * Healing tenants damaged by agents that predate the conversion lock: prep
     * ran unprotected there, so a concurrent run could land the CHECK on an
     * already-converted PARENT, where it cascades to every child and rejects
     * writes past its boundary. The parent is never a legitimate home for it.
     */
    public function test_maintenance_strips_a_check_stranded_on_a_partitioned_parent(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        self::$pdo->exec("INSERT INTO nightowl_ptest (created_at)
            SELECT now() - (i || ' days')::interval FROM generate_series(1, 3) i");
        RawPartitions::convert(self::$pdo, 'nightowl_ptest');

        // The corruption a pre-lock concurrent run produced. The boundary must be
        // the one THIS conversion froze (historicBoundary — the second midnight
        // ahead), not a hand-written "tomorrow": adding a CHECK to a partitioned
        // parent cascades to every child, and _phistoric already carries one under
        // this exact name from the swap's rename. Postgres MERGES the two only
        // when the expressions are identical — a mismatched literal is rejected
        // 42710 as a name clash, and the fixture never reaches the heal it exists
        // to exercise.
        $boundaryEpoch = RawPartitions::historicBoundary(time());
        $boundary = gmdate('Y-m-d 00:00:00', $boundaryEpoch);
        self::$pdo->exec(
            "ALTER TABLE nightowl_ptest ADD CONSTRAINT nightowl_ptest_hist_ck
             CHECK (created_at IS NOT NULL AND created_at < '{$boundary}')"
        );

        $this->assertSame(['nightowl_ptest'], RawPartitions::healConversionLeftovers(self::$pdo, ['nightowl_ptest']));

        // Rows beyond the boundary are accepted again, and routing still works:
        // the historic child's partition BOUND enforces its range on its own, so
        // losing the redundant CHECK costs nothing.
        self::$pdo->exec("INSERT INTO nightowl_ptest (created_at) VALUES ('{$boundary}')");
        $child = RawPartitions::childName('nightowl_ptest', $boundaryEpoch);
        $this->assertSame(1, (int) self::$pdo->query("SELECT COUNT(*) FROM {$child}")->fetchColumn());
        $this->assertSame(3, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest_phistoric')->fetchColumn());
        $this->assertSame(4, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest')->fetchColumn());
    }

    /**
     * The sweep rides the drain's cleanup tick, so it has to work in the shape
     * the drain runs it: inside a transaction, savepoint-isolated, surviving the
     * commit — and never costing the hourly child sweep its children.
     *
     * The two run in SEPARATE transactions (healRawPartitionLeftovers per tick,
     * maintainRawPartitions hourly). Folding the heal into the child sweep, as it
     * once was, held each table's conversion key across the whole 11-table × 8-day
     * window, so an operator re-running nightowl:partition during it was refused
     * against a peer run that does not exist.
     */
    public function test_maintenance_tick_heals_leftovers_while_creating_children(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        RawPartitions::convert(self::$pdo, 'nightowl_ptest');

        // The boundary THIS conversion froze (historicBoundary), not a
        // hand-written "tomorrow": the CHECK goes on a partitioned parent, where
        // it cascades to every child, and Postgres merges it with the copy
        // _phistoric already carries under the same name only when the two
        // expressions are identical.
        $boundary = gmdate('Y-m-d 00:00:00', RawPartitions::historicBoundary(time()));
        self::$pdo->exec(
            "ALTER TABLE nightowl_ptest ADD CONSTRAINT nightowl_ptest_hist_ck
             CHECK (created_at IS NOT NULL AND created_at < '{$boundary}')"
        );
        $today = intdiv(time(), 86400) * 86400;
        self::$pdo->exec('DROP TABLE '.RawPartitions::childName('nightowl_ptest', $today + 4 * 86400));

        $this->assertSame(['nightowl_ptest'], $this->healTick(['nightowl_ptest']));
        $this->assertSame([], $this->tick(['nightowl_ptest']));

        $this->assertSame(0, (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'nightowl_ptest'::regclass AND conname = 'nightowl_ptest_hist_ck'"
        )->fetchColumn(), 'the heal must survive the tick that made it');
        $this->assertTrue($this->childExists('nightowl_ptest', $today + 4 * 86400),
            'healing must not cost the tick its children');

        // The key must be free the moment the heal's own transaction commits —
        // that separation is the whole point of the two-transaction shape.
        $probe = self::$pdo->prepare('SELECT pg_try_advisory_lock(hashtext(?))');
        $probe->execute(['nightowl_partition:nightowl_ptest']);
        $this->assertTrue((bool) $probe->fetchColumn(),
            'the heal must not still hold a conversion key after its transaction commits');
        $release = self::$pdo->prepare('SELECT pg_advisory_unlock(hashtext(?))');
        $release->execute(['nightowl_partition:nightowl_ptest']);
    }

    /**
     * The prep's validated {t}_hist_ck is a time bomb on a live table: its
     * boundary is frozen at the second UTC midnight ahead, so once that passes
     * it rejects every drain row (23514) — a silent write outage. Any conversion that
     * aborts after adding it must take it back off, or the operator is left
     * with a table that works today and stops accepting telemetry a day or two
     * later.
     */
    public function test_a_failed_conversion_removes_its_boundary_check(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        self::$pdo->exec("INSERT INTO nightowl_ptest (created_at)
            SELECT now() - (i || ' days')::interval FROM generate_series(1, 5) i");

        // A squatter on the historic name: the swap's RENAME TO fails (42P07)
        // after prep has already added and validated the CHECK.
        self::$pdo->exec('CREATE TABLE nightowl_ptest_phistoric (x integer)');

        try {
            RawPartitions::convert(self::$pdo, 'nightowl_ptest');
            $this->fail('the conversion must fail while the historic name is taken');
        } catch (\Throwable $e) {
            $this->assertNotInstanceOf(ConversionInProgressException::class, $e);
        }

        $this->assertSame(0, (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'nightowl_ptest'::regclass AND conname = 'nightowl_ptest_hist_ck'"
        )->fetchColumn(), 'a failed conversion must not leave its boundary CHECK on the live table');

        // The finding the CHECK assertion alone was blind to. Prep used to demote
        // the PK and force created_at NOT NULL in AUTOCOMMIT, so this abort left a
        // live production table with NO PRIMARY KEY, permanently, with nothing
        // anywhere that repairs it — pg_dump silently stops emitting the
        // constraint and relreplident still reports a healthy DEFAULT over zero
        // indexes. Both statements now run inside the swap transaction, where the
        // 42P07 on RENAME rolls them back.
        $this->assertSame(1, (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'nightowl_ptest'::regclass AND contype = 'p'"
        )->fetchColumn(), 'a failed conversion must not leave the live table without its primary key');

        $this->assertFalse((bool) self::$pdo->query(
            "SELECT attnotnull FROM pg_attribute
             WHERE attrelid = 'nightowl_ptest'::regclass AND attname = 'created_at'"
        )->fetchColumn(), 'a failed conversion must not leave created_at forced NOT NULL');

        // The table still accepts writes at ANY timestamp — including past the
        // boundary the abandoned CHECK would have frozen. (Removed again right
        // after: a row dated beyond that boundary — the second UTC midnight
        // ahead — genuinely cannot live in the historic partition, so it would
        // fail the retry's VALIDATE. That is the conversion working as designed,
        // not the leak under test.)
        self::$pdo->exec("INSERT INTO nightowl_ptest (created_at) VALUES (now() + interval '5 days')");
        $this->assertSame(6, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest')->fetchColumn());
        self::$pdo->exec("DELETE FROM nightowl_ptest WHERE created_at > now() + interval '1 day'");

        // Clearing the squatter lets a retry convert cleanly.
        self::$pdo->exec('DROP TABLE nightowl_ptest_phistoric');
        RawPartitions::convert(self::$pdo, 'nightowl_ptest');
        $this->assertTrue(RawPartitions::isPartitioned(self::$pdo, 'nightowl_ptest'));
        $this->assertSame(5, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest')->fetchColumn());
    }

    /**
     * F1. The sweep and a live conversion both mutate {t}'s schema, so the sweep
     * takes the SAME per-table key convert() holds. The boundary rule alone
     * cannot separate them, whatever headroom it carries: historicBoundary()
     * freezes the SECOND midnight ahead, so a live scaffold now has 24-48h — but
     * CIC + VALIDATE on a big enough table is unbounded (VALIDATE sits outside
     * withLockTimeout on purpose), and once the clock passes, the rule calls that
     * live scaffold expired. Dropping it forces the pending ATTACH into a
     * full-table scan under ACCESS EXCLUSIVE, or fails it 23514. A clock the
     * sweep races is not a guarantee; the lock is.
     */
    public function test_the_sweep_never_touches_a_table_a_conversion_holds(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');

        // A boundary that has ALREADY PASSED — the sweep's own rule says strip it.
        $expired = gmdate('Y-m-d 00:00:00', intdiv(time(), 86400) * 86400);
        self::$pdo->exec(
            "ALTER TABLE nightowl_ptest ADD CONSTRAINT nightowl_ptest_hist_ck
             CHECK (created_at IS NOT NULL AND created_at < '{$expired}')"
        );

        $peer = new PDO(self::$dsn, self::$username, self::$password);
        $peer->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $lock = $peer->prepare('SELECT pg_try_advisory_lock(hashtext(?))');
        $lock->execute(['nightowl_partition:nightowl_ptest']);
        $this->assertTrue((bool) $lock->fetchColumn(), 'the peer must hold the lock for this test to mean anything');

        try {
            $this->assertSame([], RawPartitions::healConversionLeftovers(self::$pdo, ['nightowl_ptest']),
                'a table whose conversion lock is held must be left for the next tick, whatever its boundary says');
            $this->assertSame(1, (int) self::$pdo->query(
                "SELECT COUNT(*) FROM pg_constraint
                 WHERE conrelid = 'nightowl_ptest'::regclass AND conname = 'nightowl_ptest_hist_ck'"
            )->fetchColumn(), "a live conversion's scaffold must survive the sweep");
        } finally {
            $unlock = $peer->prepare('SELECT pg_advisory_unlock(hashtext(?))');
            $unlock->execute(['nightowl_partition:nightowl_ptest']);
        }

        // Peer gone: the same passed boundary is now provably wreckage.
        $this->assertSame(['nightowl_ptest'], RawPartitions::healConversionLeftovers(self::$pdo, ['nightowl_ptest']));
    }

    /**
     * F5a. A held conversion key is the CONSTANT state of a healthy in-flight
     * nightowl:partition run — prep adds {t}_hist_ck, so the table is a candidate
     * on every 60s tick until the swap lands, which on a populated table is
     * minutes to hours of them. Routing that skip into the failure channel
     * printed an error a minute carrying the HARD-failure remediation ("drop the
     * constraint by hand"), and acting on it would have dropped the live
     * conversion's validated CHECK — the sweep advising the operator to cause the
     * outage the sweep exists to prevent.
     */
    public function test_a_live_peers_conversion_lock_is_not_reported_as_a_failure(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');

        // The boundary prep actually leaves on a run proceeding normally.
        $boundary = gmdate('Y-m-d H:i:s', RawPartitions::historicBoundary(time()));
        self::$pdo->exec(
            "ALTER TABLE nightowl_ptest ADD CONSTRAINT nightowl_ptest_hist_ck
             CHECK (created_at IS NOT NULL AND created_at < '{$boundary}')"
        );

        $peer = new PDO(self::$dsn, self::$username, self::$password);
        $peer->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $lock = $peer->prepare('SELECT pg_try_advisory_lock(hashtext(?))');
        $lock->execute(['nightowl_partition:nightowl_ptest']);
        $this->assertTrue((bool) $lock->fetchColumn(), 'fixture: the peer must hold the key');

        $logFile = tempnam(sys_get_temp_dir(), 'nightowl-heal-log');
        $previousLog = ini_get('error_log');
        ini_set('error_log', $logFile);
        $failures = [];

        try {
            $healed = RawPartitions::healConversionLeftovers(self::$pdo, ['nightowl_ptest'], $failures);
        } finally {
            ini_set('error_log', $previousLog === false ? '' : $previousLog);
            $unlock = $peer->prepare('SELECT pg_advisory_unlock(hashtext(?))');
            $unlock->execute(['nightowl_partition:nightowl_ptest']);
        }

        $logged = (string) file_get_contents($logFile);
        @unlink($logFile);

        $this->assertSame([], $healed);
        $this->assertSame([], $failures,
            'a healthy in-flight conversion is not a failure — this fires on EVERY tick of a run that can last hours');
        $this->assertStringNotContainsString('[NightOwl Support]', $logged);
        $this->assertStringNotContainsString('drop the constraint by hand', $logged,
            'the advice printed would have destroyed the live conversion it was printed about');
    }

    /**
     * F5b. The other half of the same rule, and the reason the skip cannot simply
     * be silenced: the state it HIDES is a conversion key stranded on a backend
     * nobody can reach (a transaction-mode pooler), where the sweep skips forever
     * while the table sits in a permanent total 23514 write outage. The
     * discriminator is not a timer — it is whether the leftover is rejecting
     * writes RIGHT NOW, i.e. whether the skip cost a heal that was due.
     */
    public function test_a_held_lock_over_a_leftover_that_is_already_rejecting_writes_is_still_reported(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');

        // A boundary that has ALREADY PASSED: every drain row is failing 23514.
        $expired = gmdate('Y-m-d 00:00:00', intdiv(time(), 86400) * 86400);
        self::$pdo->exec(
            "ALTER TABLE nightowl_ptest ADD CONSTRAINT nightowl_ptest_hist_ck
             CHECK (created_at IS NOT NULL AND created_at < '{$expired}')"
        );

        $peer = new PDO(self::$dsn, self::$username, self::$password);
        $peer->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $lock = $peer->prepare('SELECT pg_try_advisory_lock(hashtext(?))');
        $lock->execute(['nightowl_partition:nightowl_ptest']);
        $this->assertTrue((bool) $lock->fetchColumn(), 'fixture: the peer must hold the key');

        $logFile = tempnam(sys_get_temp_dir(), 'nightowl-heal-log');
        $previousLog = ini_get('error_log');
        ini_set('error_log', $logFile);
        $failures = [];

        try {
            $healed = RawPartitions::healConversionLeftovers(self::$pdo, ['nightowl_ptest'], $failures);
        } finally {
            ini_set('error_log', $previousLog === false ? '' : $previousLog);
            $unlock = $peer->prepare('SELECT pg_advisory_unlock(hashtext(?))');
            $unlock->execute(['nightowl_partition:nightowl_ptest']);
        }

        $logged = (string) file_get_contents($logFile);
        @unlink($logFile);

        $this->assertSame([], $healed, 'the lock is held, so nothing may be dropped');
        $this->assertCount(1, $failures, 'a leftover that is ALREADY rejecting writes must not be silent');
        $this->assertStringContainsString('23514', $failures[0]);
        $this->assertStringContainsString('rejecting drain writes', $logged);
        $this->assertStringContainsString('stranded', $logged);
        $this->assertStringNotContainsString('drop the constraint by hand', $logged,
            'a run may still be in flight — this channel must never print the hard-failure remediation');
        $this->assertSame(1, (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_constraint
             WHERE conrelid = 'nightowl_ptest'::regclass AND conname = 'nightowl_ptest_hist_ck'"
        )->fetchColumn());
    }

    /**
     * F14. isConversionWreckage evaluates the constraint expression against a
     * probe row exposing exactly ONE column, so a {t}_hist_ck naming any other —
     * hand-repaired, or an older agent's — raised 42703, which aborts the
     * statement and recurs identically on every tick. The table could then NEVER
     * be healed and sat in a permanent 23514 write outage past its boundary. The
     * catalog (pg_constraint.conkey) answers the question before the probe is
     * ever handed an expression it cannot evaluate, and an uninterpretable
     * constraint is left in place and reported rather than dropped blind.
     */
    public function test_a_hist_check_the_agent_cannot_interpret_is_reported_never_probed(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP,
            environment text
        )');
        // Boundary already passed, so the old code reached its probe.
        $expired = gmdate('Y-m-d 00:00:00', intdiv(time(), 86400) * 86400);
        self::$pdo->exec(
            "ALTER TABLE nightowl_ptest ADD CONSTRAINT nightowl_ptest_hist_ck
             CHECK (created_at IS NOT NULL AND created_at < '{$expired}' AND environment IS NOT NULL)"
        );

        // A second table with a leftover the agent CAN read, to prove one
        // unreadable constraint costs only its own table.
        self::$pdo->exec('CREATE TABLE nightowl_ptest2 (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        self::$pdo->exec(
            "ALTER TABLE nightowl_ptest2 ADD CONSTRAINT nightowl_ptest2_hist_ck
             CHECK (created_at IS NOT NULL AND created_at < '{$expired}')"
        );

        $logFile = tempnam(sys_get_temp_dir(), 'nightowl-heal-log');
        $previousLog = ini_get('error_log');
        ini_set('error_log', $logFile);
        $failures = [];

        try {
            // Inside a caller transaction deliberately: the drain no longer runs
            // this shape (healRawPartitionLeftovers opens none, so the heal takes
            // one per candidate), but it is the STRICTER case — one shared
            // transaction is where a 42703 poisons everything after it, so a fix
            // that holds here holds on the per-candidate path too.
            self::$pdo->beginTransaction();
            $healed = RawPartitions::healConversionLeftovers(
                self::$pdo, ['nightowl_ptest', 'nightowl_ptest2'], $failures
            );
            self::$pdo->commit();
        } finally {
            ini_set('error_log', $previousLog === false ? '' : $previousLog);
            if (self::$pdo->inTransaction()) {
                self::$pdo->rollBack();
            }
        }

        $logged = (string) file_get_contents($logFile);
        @unlink($logFile);

        $this->assertSame(['nightowl_ptest2'], $healed,
            'a constraint the agent cannot read must never be dropped, and must not cost its neighbour its heal');
        $this->assertSame(1, (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_constraint
             WHERE conrelid = 'nightowl_ptest'::regclass AND conname = 'nightowl_ptest_hist_ck'"
        )->fetchColumn(), 'the unreadable constraint must still be there');

        $this->assertCount(1, $failures);
        $this->assertStringNotContainsString('42703', $failures[0],
            'the probe must not be reached at all — reaching it is what wedged the table forever');
        $this->assertStringContainsString('created_at', $failures[0]);
        $this->assertStringContainsString('NEVER be healed automatically', $logged,
            'the operator must be told this one will not clear itself');

        // And it is not a per-tick loop that silently changes its mind: a second
        // tick behaves identically.
        $second = [];
        $secondLog = tempnam(sys_get_temp_dir(), 'nightowl-heal-log');
        ini_set('error_log', $secondLog);

        try {
            $this->assertSame([], RawPartitions::healConversionLeftovers(self::$pdo, ['nightowl_ptest'], $second));
        } finally {
            ini_set('error_log', $previousLog === false ? '' : $previousLog);
            @unlink($secondLog);
        }

        $this->assertCount(1, $second);
    }

    /**
     * F14, the variant the first catalog gate let straight through. conkey
     * records a WHOLE-ROW reference as attnum 0, which has no pg_attribute row,
     * so an inner JOIN dropped it and bool_and answered "created_at only" over
     * the entry that survived. The probe then raised 42P01 instead of 42703 —
     * same mechanism, same permanent wedge, same per-tick recurrence, only the
     * SQLSTATE moved — and it was routed to the hard-failure channel that
     * promises the next tick will retry, which it never could.
     *
     * Two things must hold together here: the catalog test is now equality
     * against created_at's attnum (so {2,0} is not our shape), and the probe
     * carries its own savepoint and catch, so any expression a future gate
     * mis-admits degrades to the same null verdict instead of wedging.
     */
    public function test_a_hist_check_referencing_the_whole_row_is_reported_not_wedged(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        $expired = gmdate('Y-m-d 00:00:00', intdiv(time(), 86400) * 86400);
        // The shape a hand repair produces: created_at IS one of the columns it
        // names, so every "do all referenced columns resolve to created_at" test
        // says yes while conkey actually reads {2,0}.
        self::$pdo->exec(
            "ALTER TABLE nightowl_ptest ADD CONSTRAINT nightowl_ptest_hist_ck
             CHECK (created_at IS NOT NULL AND created_at < '{$expired}' AND (nightowl_ptest.*) IS NOT NULL)"
        );

        // The neighbour proves one unreadable constraint still costs only itself.
        self::$pdo->exec('CREATE TABLE nightowl_ptest2 (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        self::$pdo->exec(
            "ALTER TABLE nightowl_ptest2 ADD CONSTRAINT nightowl_ptest2_hist_ck
             CHECK (created_at IS NOT NULL AND created_at < '{$expired}')"
        );

        $logFile = tempnam(sys_get_temp_dir(), 'nightowl-heal-log');
        $previousLog = ini_get('error_log');
        ini_set('error_log', $logFile);
        $failures = [];

        try {
            self::$pdo->beginTransaction();
            $healed = RawPartitions::healConversionLeftovers(
                self::$pdo, ['nightowl_ptest', 'nightowl_ptest2'], $failures
            );
            self::$pdo->commit();
        } finally {
            ini_set('error_log', $previousLog === false ? '' : $previousLog);
            if (self::$pdo->inTransaction()) {
                self::$pdo->rollBack();
            }
        }

        $logged = (string) file_get_contents($logFile);
        @unlink($logFile);

        $this->assertSame(['nightowl_ptest2'], $healed);
        $this->assertSame(1, (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_constraint
             WHERE conrelid = 'nightowl_ptest'::regclass AND conname = 'nightowl_ptest_hist_ck'"
        )->fetchColumn(), 'a constraint the agent cannot read must never be dropped');

        $this->assertCount(1, $failures);
        // The discriminating assertions: pre-fix the probe raised 42P01 and the
        // report went out on the hard-failure channel.
        $this->assertStringNotContainsString('42P01', $failures[0]);
        $this->assertStringNotContainsString('not stripped', $failures[0]);
        $this->assertStringContainsString('created_at', $failures[0]);
        $this->assertStringContainsString('NEVER be healed automatically', $logged);
        $this->assertStringNotContainsString('retried next tick', $logged,
            'this state never clears by itself, so it must not be reported as one that will');
    }

    /**
     * F8's surviving sibling. Every lock-bounded statement in the conversion goes
     * through withLockTimeout — which only ever TIGHTENS — except the swap
     * transaction, which emitted a bare `SET LOCAL lock_timeout =
     * SWAP_LOCK_TIMEOUT_MS`. An operator who had lowered
     * NIGHTOWL_DB_LOCK_TIMEOUT_MS because ACCESS EXCLUSIVE queueing on their raw
     * tables is an outage had that RAISED thirtyfold for the one statement it
     * matters most for: the LOCK TABLE ... ACCESS EXCLUSIVE, which parks a
     * pending exclusive in front of every later reader and writer of the live
     * table for as long as it waits.
     *
     * The table is empty on purpose, so nothing before the swap requests an
     * exclusive lock (normalizeCreatedAtType returns early on a timestamp column,
     * and an empty table skips CIC and the CHECK entirely) and the blocker's
     * ACCESS SHARE — a long dashboard read — lets every catalog step through. The
     * first and only contended statement is the swap's own LOCK TABLE.
     *
     * The blocker self-terminates so an over-loose swap makes this slow rather
     * than hanging: pre-fix it waited the blocker out at 15s and then SUCCEEDED,
     * which is why the timing assertion and the 55P03 are both needed.
     */
    public function test_a_tighter_configured_lock_timeout_is_never_raised_by_the_swap(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');

        $blocker = new PDO(self::$dsn, self::$username, self::$password);
        $blocker->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $blocker->exec('SET idle_in_transaction_session_timeout = 8000');
        $blocker->beginTransaction();
        $blocker->exec('LOCK TABLE nightowl_ptest IN ACCESS SHARE MODE');

        // Session-scoped, not SET LOCAL: convert() runs its prep in autocommit
        // and opens the swap transaction itself, so this is the only way an
        // operator's value actually reaches it.
        self::$pdo->exec('SET lock_timeout = 200');

        $started = microtime(true);
        $raised = null;

        try {
            RawPartitions::convert(self::$pdo, 'nightowl_ptest');
        } catch (\Throwable $e) {
            $raised = $e;
        } finally {
            self::$pdo->exec('RESET lock_timeout');
            try {
                $blocker->rollBack();
            } catch (\Throwable) {
                // Already terminated by its own idle timeout.
            }
        }

        $this->assertLessThan(3.0, microtime(true) - $started,
            "the operator's 200ms ceiling must stand — raising it to the hardcoded 15000 is the bug");
        $this->assertNotNull($raised, 'the swap must fail fast, not wait the blocker out');
        $this->assertStringContainsString('55P03', $raised->getMessage());

        // And the live table is untouched: the ceiling produces a clean retryable
        // failure, not a half-converted parent.
        $this->assertFalse((bool) self::$pdo->query(
            "SELECT relkind = 'p' FROM pg_class WHERE relname = 'nightowl_ptest'"
        )->fetchColumn());
    }

    /**
     * F1. The swap transaction's rollback was the last unguarded one on this
     * path. Its most violent deaths take the BACKEND with them — the
     * idle_in_transaction_session_timeout the agent itself sets, an admin's
     * pg_terminate_backend, a pooler dropping the connection — and
     * PDO::inTransaction() still answers true against a dead handle, so ROLLBACK
     * raises ON TOP of the cause and REPLACED it, skipping the rethrow. Two
     * losses: the operator saw a connection error instead of the 55P03 / 40P01 /
     * 57P01 that classifies the failure, and the exception TYPE went with it — so
     * a ConversionInProgressException stopped reaching PartitionCommand's
     * contention branch and a retryable BUSY (3) was reported as FAILURE (1).
     */
    public function test_a_failing_rollback_never_replaces_the_swaps_real_cause(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        self::$pdo->exec("INSERT INTO nightowl_ptest (created_at) VALUES (now() - interval '1 day')");
        // A squatter under the name the swap renames onto: the rename fails 42P07
        // inside the swap transaction, which is a deterministic way to reach the
        // rollback under test.
        self::$pdo->exec('CREATE TABLE nightowl_ptest_phistoric (x int)');

        // A handle whose FIRST rollback dies the way a terminated backend does:
        // inTransaction() still answers true, ROLLBACK raises instead of
        // returning. The second really rolls back, so the test can free the
        // swap's ACCESS EXCLUSIVE for tearDown.
        $flaky = new class(self::$dsn, self::$username, self::$password) extends PDO
        {
            public bool $rollbackFailed = false;

            public function rollBack(): bool
            {
                if (! $this->rollbackFailed) {
                    $this->rollbackFailed = true;

                    throw new \PDOException('SQLSTATE[08006]: server closed the connection unexpectedly');
                }

                return parent::rollBack();
            }
        };
        $flaky->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        // The failed unwind logs; keep it out of the suite's output.
        $logFile = tempnam(sys_get_temp_dir(), 'nightowl-swap-log');
        $previousLog = ini_get('error_log');
        ini_set('error_log', $logFile);

        try {
            RawPartitions::convert($flaky, 'nightowl_ptest');
            $this->fail('the squatting {t}_phistoric must have failed the swap');
        } catch (\Throwable $e) {
            $this->assertTrue($flaky->rollbackFailed, 'fixture: the rollback must actually have been attempted');
            $this->assertStringNotContainsString('server closed the connection unexpectedly', $e->getMessage(),
                'a failing rollback must never replace the conversion\'s real cause');
            $this->assertStringContainsString('nightowl_ptest_phistoric', $e->getMessage(),
                'the operator must still get the statement that actually failed');
        } finally {
            ini_set('error_log', $previousLog === false ? '' : $previousLog);
            @unlink($logFile);
            if ($flaky->inTransaction()) {
                $flaky->rollBack();
            }
            self::$pdo->exec('DROP TABLE IF EXISTS nightowl_ptest_phistoric CASCADE');
        }
    }

    /**
     * F2. The boundary probe must bind a PHP gmdate() UTC literal, not ask the
     * server for now()::timestamp, which renders in the session's TimeZone GUC —
     * on a BYO PostgreSQL, the customer's. CI runs Etc/UTC, which is the only
     * reason this ever looked correct.
     *
     * Deterministic at any wall-clock hour by construction: real UTC offsets span
     * -12 to +14, so a boundary sixty seconds in the past is unconditionally in
     * the FUTURE for a -12 session, and one an hour ahead is unconditionally in
     * the PAST for a +14 one. Both directions are damage: the first leaves a
     * table in its 23514 outage for twelve more hours, the second strips a live
     * conversion's CHECK fourteen hours early.
     */
    public function test_the_sweeps_boundary_rule_is_evaluated_in_utc(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');

        try {
            // WEST of UTC: the session clock reads twelve hours BEHIND, so a
            // boundary that passed a minute ago still looks comfortably ahead.
            self::$pdo->exec("SET TIME ZONE 'Etc/GMT+12'");
            $passed = gmdate('Y-m-d H:i:s', time() - 60);
            self::$pdo->exec(
                "ALTER TABLE nightowl_ptest ADD CONSTRAINT nightowl_ptest_hist_ck
                 CHECK (created_at IS NOT NULL AND created_at < '{$passed}')"
            );

            $this->assertSame(['nightowl_ptest'], RawPartitions::healConversionLeftovers(self::$pdo, ['nightowl_ptest']),
                'a passed boundary must be healed regardless of the session TimeZone');

            // EAST of UTC: the session clock reads fourteen hours AHEAD, so a
            // boundary still an hour away looks passed.
            self::$pdo->exec("SET TIME ZONE 'Pacific/Kiritimati'");
            $ahead = gmdate('Y-m-d H:i:s', time() + 3600);
            self::$pdo->exec(
                "ALTER TABLE nightowl_ptest ADD CONSTRAINT nightowl_ptest_hist_ck
                 CHECK (created_at IS NOT NULL AND created_at < '{$ahead}')"
            );

            $this->assertSame([], RawPartitions::healConversionLeftovers(self::$pdo, ['nightowl_ptest']),
                'a boundary still ahead must survive regardless of the session TimeZone');
            $this->assertSame(1, (int) self::$pdo->query(
                "SELECT COUNT(*) FROM pg_constraint
                 WHERE conrelid = 'nightowl_ptest'::regclass AND conname = 'nightowl_ptest_hist_ck'"
            )->fetchColumn());
        } finally {
            // Shared static connection — leaking the GUC would corrupt every
            // later test in this class.
            self::$pdo->exec("SET TIME ZONE 'UTC'");
        }
    }

    /**
     * F3 + F6 together. A heal that can never succeed used to be invisible — a
     * bare catch(\Throwable) with a comment for a body, and a return value the
     * only production caller discarded — while the table sat in a total 23514
     * write outage. And its ALTER TABLE needs ACCESS EXCLUSIVE with no timeout of
     * its own, so under NIGHTOWL_DRAIN_CONN_TIMEOUTS=false (a documented rollback
     * switch that removes the drain guards' lock_timeout) it queues unbounded,
     * parking every later reader and writer behind its pending request.
     *
     * The blocker self-terminates via idle_in_transaction_session_timeout so that
     * an UNBOUNDED heal makes this test slow rather than hanging forever.
     */
    public function test_a_heal_that_cannot_take_its_lock_is_bounded_and_reported(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        $expired = gmdate('Y-m-d 00:00:00', intdiv(time(), 86400) * 86400);
        self::$pdo->exec(
            "ALTER TABLE nightowl_ptest ADD CONSTRAINT nightowl_ptest_hist_ck
             CHECK (created_at IS NOT NULL AND created_at < '{$expired}')"
        );

        $blocker = new PDO(self::$dsn, self::$username, self::$password);
        $blocker->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $blocker->exec('SET idle_in_transaction_session_timeout = 8000');
        $blocker->beginTransaction();
        $blocker->exec('LOCK TABLE nightowl_ptest IN ACCESS EXCLUSIVE MODE');

        $failures = [];
        $started = microtime(true);

        try {
            // No caller transaction — the shape the drain runs. The heal opens
            // its own per candidate, so SET LOCAL still applies inside it.
            $healed = RawPartitions::healConversionLeftovers(self::$pdo, ['nightowl_ptest'], $failures);
        } finally {
            if (self::$pdo->inTransaction()) {
                self::$pdo->rollBack();
            }
            try {
                $blocker->rollBack();
            } catch (\Throwable) {
                // Already terminated by its own idle timeout.
            }
        }

        $this->assertLessThan(7.0, microtime(true) - $started,
            'the heal must bound its own ACCESS EXCLUSIVE wait, not inherit the caller\'s (absent) one');
        $this->assertSame([], $healed, 'a heal that could not take its lock must not report success');
        $this->assertCount(1, $failures, 'a heal that can never succeed must not be silent');
        $this->assertStringContainsString('nightowl_ptest', $failures[0]);
        $this->assertStringContainsString('55P03', $failures[0], 'the report must carry the real cause');
    }

    /**
     * F12. "Already partitioned" is not "already finished": a peer SIGKILLed
     * between its swap commit and its own child sweep leaves a parent whose only
     * children are _phistoric and _pdefault. Both of convert()'s early returns
     * used to exit before the child window, so nightowl:partition printed "daily
     * children ahead" and exited SUCCESS over a table with none — and every row
     * drained from then on landed in the DEFAULT, which PruneCommand can only
     * row-DELETE, never DROP PARTITION.
     *
     * This drives the ENTRY probe, which is now the one EVERY re-run reaches:
     * nightowl:partition no longer skips a table it finds already partitioned, it
     * delegates and reports the gaps. The under-lock probe takes the identical
     * call and is not deterministically reachable without racing two processes.
     */
    public function test_a_run_that_finds_the_table_already_partitioned_still_creates_its_children(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        RawPartitions::convert(self::$pdo, 'nightowl_ptest');

        // The state the killed peer left: converted, no daily children at all.
        $today = intdiv(time(), 86400) * 86400;
        for ($d = 0; $d <= RawPartitions::DAYS_AHEAD; $d++) {
            self::$pdo->exec('DROP TABLE IF EXISTS '.RawPartitions::childName('nightowl_ptest', $today + $d * 86400));
        }
        $this->assertFalse($this->childExists('nightowl_ptest', $today), 'fixture: the children must be gone');

        $this->assertSame([], RawPartitions::convert(self::$pdo, 'nightowl_ptest'));

        for ($d = 0; $d <= RawPartitions::DAYS_AHEAD; $d++) {
            $this->assertTrue($this->childExists('nightowl_ptest', $today + $d * 86400),
                "day +{$d} must have a child after a run that found the table already partitioned");
        }
    }

    /**
     * F6. The command's isPartitioned() pre-check used to `continue` before
     * delegating, so convert()'s already-partitioned recovery — the branch
     * written for a conversion SIGKILLed between its swap committing and its
     * child sweep — was reachable only by a run that LOST a race, never by the
     * run an operator starts to repair that state. It printed "already
     * partitioned." and exited SUCCESS over a parent owing its whole child
     * window, while every drained row landed in the DEFAULT that prune can only
     * row-DELETE.
     *
     * nightowl_queries is partitioned by migration 000058 in this suite's DB, so
     * it IS the already-partitioned case. Only the far end of the window is
     * dropped: nothing is ever written seven days ahead, so the fixture destroys
     * no other test's rows, and the command puts the child back.
     */
    public function test_the_command_repairs_the_child_window_of_a_table_it_finds_already_partitioned(): void
    {
        $far = intdiv(time(), 86400) * 86400 + RawPartitions::DAYS_AHEAD * 86400;
        self::$pdo->exec('DROP TABLE IF EXISTS '.RawPartitions::childName('nightowl_queries', $far));
        $this->assertFalse($this->childExists('nightowl_queries', $far), 'fixture: the child must be gone');

        [$code, $output] = $this->runPartitionCommand('nightowl_queries');

        $this->assertTrue($this->childExists('nightowl_queries', $far),
            'a run over an already-partitioned table must still create the children it owes');
        $this->assertSame(PartitionCommand::SUCCESS, $code);
        $this->assertStringContainsString('already partitioned', $output);
    }

    /**
     * F15 end to end. childGaps was collected and warned about but never reached
     * the exit ladder, so a run whose conversions landed while their child
     * windows failed — a lock_timeout against one long dashboard read is the
     * documented common cause — still printed "Done… no restart needed" and
     * exited SUCCESS. A pipeline gating on the status recorded the step complete.
     *
     * The fixture fails the child window's FIRST read, which is exactly where a
     * real connection blip lands: after the conversion (here a no-op, the table
     * is already partitioned) and outside ensureChildWindow's per-day try.
     *
     * Deliberately no assertion on the gap COUNT: dueChildDaysBestEffort decides
     * that, and what this test owns is the exit code and the warning.
     */
    public function test_a_run_that_owes_children_does_not_report_success(): void
    {
        $flaky = new class(self::$dsn, self::$username, self::$password) extends PDO
        {
            public function prepare(string $query, array $options = []): \PDOStatement|false
            {
                if (str_contains($query, 'pg_get_partition_constraintdef')) {
                    throw new \PDOException('SQLSTATE[08006]: server closed the connection unexpectedly');
                }

                return parent::prepare($query, $options);
            }
        };
        $flaky->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        [$code, $output] = $this->runPartitionCommand('nightowl_queries', $flaky);

        $this->assertSame(PartitionCommand::INCOMPLETE, $code,
            'converted-but-incomplete must not be indistinguishable from a complete run');
        $this->assertStringContainsString('could not be created', $output);
        $this->assertStringNotContainsString('no restart needed', $output,
            'the "Done" line claims a finished child window this run does not have');
    }

    /**
     * F10 + F14. Behind a transaction-mode pooler the session lock convert()
     * trusts lives on a backend it may no longer be talking to. That is detected,
     * and the detection must carry its OWN type: nightowl:partition stops the
     * whole run on it rather than pushing prep DDL through the same pooler for
     * the ten remaining tables and stranding a session lock on each. Before this
     * diff the plain RuntimeException escaped handle() and stopped the run by
     * accident; the new per-table catch(\Throwable) silently turned that into
     * "log it and continue".
     *
     * The PDO subclass rewrites every pg_backend_pid() after the first, so the
     * abort lands on the check that now guards normalizeCreatedAtType — the
     * longest and least reversible statement in the conversion, which used to run
     * with no affinity check between it and the lock at all.
     */
    public function test_a_pooler_move_is_typed_and_leaves_the_live_table_intact(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        self::$pdo->exec("INSERT INTO nightowl_ptest (created_at)
            SELECT now() - (i || ' days')::interval FROM generate_series(1, 5) i");

        $moving = new class(self::$dsn, self::$username, self::$password) extends PDO
        {
            private int $pidCalls = 0;

            // The baseline is read in the SAME statement that takes the conversion
            // key — a prepare, not a query — so it has to be counted here too, or
            // the FIRST assertSameBackend() would compare against a pid this fake
            // never moved and the abort would slide to a later phase than the
            // docblock above claims.
            public function prepare(string $query, array $options = []): \PDOStatement|false
            {
                return parent::prepare($this->move($query), $options);
            }

            public function query(string $query, ?int $fetchMode = null, mixed ...$fetchModeArgs): \PDOStatement|false
            {
                return parent::query($this->move($query), $fetchMode, ...$fetchModeArgs);
            }

            private function move(string $query): string
            {
                if (str_contains($query, 'pg_backend_pid') && ++$this->pidCalls > 1) {
                    return str_replace('pg_backend_pid()', '(pg_backend_pid() + 1)', $query);
                }

                return $query;
            }
        };
        $moving->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        try {
            RawPartitions::convert($moving, 'nightowl_ptest');
            $this->fail('convert must abort when the connection moves between server backends');
        } catch (PoolerAffinityException $e) {
            $this->assertStringContainsString('pooler', $e->getMessage());
        }

        $this->assertFalse(RawPartitions::isPartitioned(self::$pdo, 'nightowl_ptest'));
        $this->assertSame(5, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest')->fetchColumn());
        $this->assertSame(1, (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'nightowl_ptest'::regclass AND contype = 'p'"
        )->fetchColumn(), 'a pooler abort must not leave the live table without a primary key');
        $this->assertSame(0, (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_constraint
             WHERE conrelid = 'nightowl_ptest'::regclass AND conname = 'nightowl_ptest_hist_ck'"
        )->fetchColumn());
    }

    /**
     * F9. The heal moved out of the child sweep, but its out-param survived the
     * move: ensureFutureChildren reset $heal on entry and never wrote to it
     * again, so RecordWriter's logHealedPartitionChecks($heal['healed']) was
     * unreachable — while BOTH docblocks still specified a live heal contract, so
     * a maintainer following either one filled an array nothing reads and the
     * operator never heard that a total 23514 write outage had been cleared.
     *
     * Two halves, and the structural one is the point: the sweep must carry no
     * heal plumbing at all. The behavioural half pins where the responsibility
     * went — the hourly sweep does its real work and leaves an interrupted run's
     * leftovers exactly where it found them; the per-tick heal strips them.
     */
    public function test_the_child_sweep_neither_heals_nor_carries_heal_plumbing(): void
    {
        $params = array_map(
            static fn (\ReflectionParameter $p): string => $p->getName(),
            (new \ReflectionMethod(RawPartitions::class, 'ensureFutureChildren'))->getParameters(),
        );

        $this->assertSame(['conn', 'tables'], $params,
            'the child sweep must carry no heal out-param — the last one was reset on entry, never filled, '
            .'and made the caller\'s operator notice unreachable');

        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        RawPartitions::convert(self::$pdo, 'nightowl_ptest');

        // A killed run's leftover on the converted parent, plus a missing child so
        // the sweep has real work to do on the same table in the same pass. The
        // boundary must be the one THIS conversion froze, not a hand-written
        // "tomorrow" — see test_maintenance_strips_a_check_stranded_on_a_partitioned_parent
        // for why a mismatched literal is rejected 42710 instead of merging.
        $boundary = gmdate('Y-m-d 00:00:00', RawPartitions::historicBoundary(time()));
        self::$pdo->exec(
            "ALTER TABLE nightowl_ptest ADD CONSTRAINT nightowl_ptest_hist_ck
             CHECK (created_at IS NOT NULL AND created_at < '{$boundary}')"
        );
        $today = intdiv(time(), 86400) * 86400;
        self::$pdo->exec('DROP TABLE '.RawPartitions::childName('nightowl_ptest', $today + 4 * 86400));

        $this->assertSame([], $this->tick(['nightowl_ptest']));
        $this->assertTrue($this->childExists('nightowl_ptest', $today + 4 * 86400),
            'the hourly sweep must still create the children it exists for');
        $this->assertSame(1, (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_constraint
             WHERE conrelid = 'nightowl_ptest'::regclass AND conname = 'nightowl_ptest_hist_ck'"
        )->fetchColumn(), 'the hourly child sweep must not heal — that is the per-tick sweep\'s job');

        $this->assertSame(['nightowl_ptest'], RawPartitions::healConversionLeftovers(self::$pdo, ['nightowl_ptest']));
        $this->assertSame(0, (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_constraint
             WHERE conrelid = 'nightowl_ptest'::regclass AND conname = 'nightowl_ptest_hist_ck'"
        )->fetchColumn(), 'the per-tick heal is where the responsibility went');
    }

    /**
     * When the heal owns its transaction, a table counts as healed only once the
     * DROP is DURABLE. Appending to $healed inside the closure meant a commit
     * that then threw returned the table in BOTH lists, and RecordWriter
     * announced "cleaned up an interrupted nightowl:partition run's leftovers"
     * for a statement that rolled back — the same false-success line that moving
     * the announcement after the caller's commit exists to prevent.
     */
    public function test_a_heal_whose_commit_fails_is_not_reported_as_healed(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        $expired = gmdate('Y-m-d 00:00:00', intdiv(time(), 86400) * 86400);
        self::$pdo->exec(
            "ALTER TABLE nightowl_ptest ADD CONSTRAINT nightowl_ptest_hist_ck
             CHECK (created_at IS NOT NULL AND created_at < '{$expired}')"
        );

        // No caller transaction, so the heal opens its own — and its commit dies.
        $flaky = new class(self::$dsn, self::$username, self::$password) extends PDO
        {
            public function commit(): bool
            {
                throw new \PDOException('SQLSTATE[08006]: server closed the connection unexpectedly');
            }
        };
        $flaky->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        $failures = [];
        $healed = RawPartitions::healConversionLeftovers($flaky, ['nightowl_ptest'], $failures);

        $this->assertSame([], $healed, 'a DROP whose commit failed must never be reported as healed');
        $this->assertCount(1, $failures);
        $this->assertStringContainsString('nightowl_ptest', $failures[0]);
    }

    /**
     * healConversionLeftovers and ensureFutureChildren both document "never
     * throws", and the drain depends on it: inside maintainRawPartitions a throw
     * rolls the tick back and costs every healthy table its child window for the
     * hour. beginTransaction sat outside the try, so a dead handle escaped both.
     */
    public function test_the_heal_never_throws_even_if_it_cannot_open_its_transaction(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        $expired = gmdate('Y-m-d 00:00:00', intdiv(time(), 86400) * 86400);
        self::$pdo->exec(
            "ALTER TABLE nightowl_ptest ADD CONSTRAINT nightowl_ptest_hist_ck
             CHECK (created_at IS NOT NULL AND created_at < '{$expired}')"
        );

        $flaky = new class(self::$dsn, self::$username, self::$password) extends PDO
        {
            public function beginTransaction(): bool
            {
                throw new \PDOException('SQLSTATE[08006]: server closed the connection unexpectedly');
            }
        };
        $flaky->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        $failures = [];
        $healed = RawPartitions::healConversionLeftovers($flaky, ['nightowl_ptest'], $failures);

        $this->assertSame([], $healed);
        $this->assertCount(1, $failures, 'the failure must be reported, not thrown');
    }

    /**
     * The drain's heal sweep takes the SAME per-table key convert() wants, and it
     * takes it only when a table carries an interrupted run's leftovers — exactly
     * when the operator re-runs the command. Refusing on the first miss therefore
     * reported "another nightowl:partition run is converting {t}" about a run
     * that did not exist, as the COMMON case. A brief wait separates the two
     * holders: a tick-bounded sweep frees the key, a real peer conversion does
     * not.
     */
    public function test_a_briefly_held_lock_is_waited_out_rather_than_reported_as_a_peer_run(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        self::$pdo->exec("INSERT INTO nightowl_ptest (created_at) VALUES (now() - interval '1 day')");

        // A holder whose session the SERVER ends shortly — the shape of the
        // drain's sweep, whose xact lock dies with its tick. idle_session_timeout
        // does it without a subprocess: the holder goes idle the moment it has
        // the key, and Postgres closes it, releasing the lock.
        $holder = new PDO(self::$dsn, self::$username, self::$password);
        $holder->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $holder->exec('SET idle_session_timeout = 2000');
        $lock = $holder->prepare('SELECT pg_try_advisory_lock(hashtext(?))');
        $lock->execute(['nightowl_partition:nightowl_ptest']);
        $this->assertTrue((bool) $lock->fetchColumn(), 'fixture: the holder must actually hold the key');

        $started = microtime(true);
        $gaps = RawPartitions::convert(self::$pdo, 'nightowl_ptest');
        $elapsed = microtime(true) - $started;

        $this->assertSame([], $gaps);
        $this->assertTrue(RawPartitions::isPartitioned(self::$pdo, 'nightowl_ptest'),
            'a lock held only briefly must be waited out, not reported as a peer conversion');
        $this->assertGreaterThan(1.0, $elapsed, 'the fixture must actually have made convert() wait');
    }

    /**
     * A conversion that COMMITTED must never be reported as a failure, whatever
     * happens afterwards. ensureChildWindow only isolates the PER-DAY work — its
     * isPartitioned() and historicConstraint() reads sit outside that try — so a
     * connection blip in the moments after the swap committed threw straight out
     * of convert(), and PartitionCommand printed its "still readable and
     * writable… a failure rolls it back" summary over a fully converted table
     * with a valid PK. The operator acts on that; the next run says "already
     * partitioned" and contradicts it.
     */
    public function test_a_committed_conversion_reports_gaps_not_failure_if_the_child_window_dies(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        self::$pdo->exec("INSERT INTO nightowl_ptest (created_at) VALUES (now() - interval '1 day')");

        // Fails only historicConstraint()'s read — the first statement of the
        // child window, reached only AFTER the swap transaction has committed,
        // and deliberately outside ensureChildWindow's per-day try.
        $flaky = new class(self::$dsn, self::$username, self::$password) extends PDO
        {
            public function prepare(string $query, array $options = []): \PDOStatement|false
            {
                if (str_contains($query, 'pg_get_partition_constraintdef')) {
                    throw new \PDOException('SQLSTATE[08006]: server closed the connection unexpectedly');
                }

                return parent::prepare($query, $options);
            }
        };
        $flaky->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        $gaps = RawPartitions::convert($flaky, 'nightowl_ptest');

        $today = intdiv(time(), 86400) * 86400;
        $boundary = RawPartitions::historicBoundary(time());
        $report = implode(' ', $gaps);

        // The days _phistoric does NOT cover — the only ones ensureChildWindow
        // ever intends to create.
        $due = [];
        for ($d = 0; $d <= RawPartitions::DAYS_AHEAD; $d++) {
            if ($today + $d * 86400 >= $boundary) {
                $due[] = $d;
            }
        }
        $this->assertNotEmpty($due, 'fixture: some days must lie beyond the historic bound');
        $this->assertNotCount(RawPartitions::DAYS_AHEAD + 1, $due,
            'fixture: the historic child must cover at least one day, or this test proves nothing');

        // One entry per day that was DUE. Callers print count($gaps) as a number
        // of partitions, so it is read as "children missing": one string for the
        // whole window under-reported it, and a bare loop over DAYS_AHEAD + 1
        // over-reported it by counting days _phistoric owns — days no child is
        // ever created for, so an operator counting children afterwards found the
        // table permanently short and concluded one was lost.
        $this->assertCount(count($due), $gaps,
            'a post-commit child-window failure is a GAP per DUE day — never a day _phistoric already covers');
        $this->assertStringContainsString('after conversion', $gaps[0]);

        foreach ($due as $d) {
            $this->assertStringContainsString(RawPartitions::childName('nightowl_ptest', $today + $d * 86400), $report,
                "day +{$d} is beyond the historic bound and must be named as a gap");
        }
        for ($d = 0; $d <= RawPartitions::DAYS_AHEAD; $d++) {
            if (in_array($d, $due, true)) {
                continue;
            }

            $this->assertStringNotContainsString(RawPartitions::childName('nightowl_ptest', $today + $d * 86400), $report,
                "day +{$d} is inside _phistoric and was never due a child — naming it inflates the count");
        }

        // Every day really is childless, so the report has to be about which were
        // DUE, not which exist — otherwise the assertion above passes for the
        // wrong reason.
        for ($d = 0; $d <= RawPartitions::DAYS_AHEAD; $d++) {
            $this->assertFalse($this->childExists('nightowl_ptest', $today + $d * 86400),
                "day +{$d} really has no child");
        }
        $this->assertTrue(RawPartitions::isPartitioned(self::$pdo, 'nightowl_ptest'),
            'the conversion itself committed and must be reported as done');
        $this->assertSame(1, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest')->fetchColumn());
    }

    /**
     * One locked table must cost only itself. The candidacy probe asks nothing
     * but "is a {t}_hist_ck present?", yet it used to fetch the deparsed
     * expression too — and pg_get_expr opens the constraint's relation, taking
     * ACCESS SHARE on every table it reports on. Behind any in-flight ALTER the
     * whole batch then timed out, so a SECOND table sat in its 23514 write
     * outage for the duration of an unrelated table's conversion — the outage
     * ceiling the per-tick cadence exists to establish, lost to a column the
     * candidacy test discards.
     */
    public function test_one_locked_table_does_not_cost_the_others_their_heal(): void
    {
        foreach (['nightowl_ptest', 'nightowl_ptest2'] as $table) {
            self::$pdo->exec("CREATE TABLE {$table} (
                id bigserial PRIMARY KEY,
                created_at timestamp DEFAULT CURRENT_TIMESTAMP
            )");
            $expired = gmdate('Y-m-d 00:00:00', intdiv(time(), 86400) * 86400);
            self::$pdo->exec(
                "ALTER TABLE {$table} ADD CONSTRAINT {$table}_hist_ck
                 CHECK (created_at IS NOT NULL AND created_at < '{$expired}')"
            );
        }

        // Blocker on ONE of them only.
        $blocker = new PDO(self::$dsn, self::$username, self::$password);
        $blocker->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $blocker->exec('SET idle_in_transaction_session_timeout = 8000');
        $blocker->beginTransaction();
        $blocker->exec('LOCK TABLE nightowl_ptest IN ACCESS EXCLUSIVE MODE');

        $failures = [];

        try {
            self::$pdo->beginTransaction();
            $healed = RawPartitions::healConversionLeftovers(
                self::$pdo, ['nightowl_ptest', 'nightowl_ptest2'], $failures
            );
            self::$pdo->commit();
        } finally {
            if (self::$pdo->inTransaction()) {
                self::$pdo->rollBack();
            }
            try {
                $blocker->rollBack();
            } catch (\Throwable) {
                // Already terminated by its own idle timeout.
            }
        }

        $this->assertSame(['nightowl_ptest2'], $healed,
            'the unlocked table must be healed even while another table is locked');
        $this->assertCount(1, $failures, 'only the locked table may be reported as unhealed');
        $this->assertStringContainsString('nightowl_ptest', $failures[0]);
        $this->assertStringContainsString('55P03', $failures[0]);

        // The point of all of it: the second table's write outage is over.
        self::$pdo->exec('INSERT INTO nightowl_ptest2 (created_at) VALUES (now())');
        $this->assertSame(1, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest2')->fetchColumn());
    }

    /**
     * The empty-table path DROPs the original and rebuilds it partitioned, so no
     * {t}_phistoric is ever attached — nothing happens "in place". The command
     * printed the historic-partition line unconditionally, which was false on
     * nearly every real run (most tenants have at least one empty raw table).
     */
    public function test_an_empty_table_converts_without_a_historic_child(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');

        RawPartitions::convert(self::$pdo, 'nightowl_ptest');

        $this->assertTrue(RawPartitions::isPartitioned(self::$pdo, 'nightowl_ptest'));
        $this->assertFalse(RawPartitions::hasHistoricChild(self::$pdo, 'nightowl_ptest'),
            'the empty path attaches nothing — the command must not claim it did');

        // And the populated path still does have one, so the branch discriminates.
        self::$pdo->exec('CREATE TABLE nightowl_ptest2 (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        self::$pdo->exec("INSERT INTO nightowl_ptest2 (created_at) VALUES (now() - interval '1 day')");
        RawPartitions::convert(self::$pdo, 'nightowl_ptest2');

        $this->assertTrue(RawPartitions::hasHistoricChild(self::$pdo, 'nightowl_ptest2'));
    }

    /**
     * The other leftover of the same kill, and the one nothing unattended ever
     * collected: a CREATE INDEX CONCURRENTLY killed mid-build leaves an INVALID
     * {t}_id_created_at_pt. The planner never uses an invalid index, so it is
     * pure cost — on a 40M-row raw table, over a gigabyte — and only a RETRY of
     * nightowl:partition removed it. A VALID one is left alone on purpose: it is
     * completed prep, and the retry reuses it.
     */
    public function test_maintenance_drops_an_invalid_scaffolding_index(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            dup integer,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        self::$pdo->exec('INSERT INTO nightowl_ptest (dup) VALUES (1), (1)');

        // A failing unique CONCURRENTLY build is the only way to leave a
        // genuinely invalid index without superuser. The heal matches on the
        // index NAME and indisvalid, never on its columns.
        try {
            self::$pdo->exec('CREATE UNIQUE INDEX CONCURRENTLY nightowl_ptest_id_created_at_pt ON nightowl_ptest (dup)');
            $this->fail('the CONCURRENTLY build must fail for this fixture to leave an invalid index');
        } catch (\PDOException $e) {
            $this->assertSame('23505', $e->getCode());
        }

        $this->assertFalse((bool) self::$pdo->query(
            "SELECT indisvalid FROM pg_index x JOIN pg_class c ON c.oid = x.indexrelid
             WHERE c.relname = 'nightowl_ptest_id_created_at_pt'"
        )->fetchColumn(), 'the fixture must actually be an INVALID index');

        $this->assertSame(['nightowl_ptest'], RawPartitions::healConversionLeftovers(self::$pdo, ['nightowl_ptest']));
        $this->assertFalse((bool) self::$pdo->query(
            "SELECT to_regclass('nightowl_ptest_id_created_at_pt') IS NOT NULL"
        )->fetchColumn(), 'the invalid scaffolding index must be dropped');

        // A VALID scaffolding index is completed prep — leave it for the retry.
        self::$pdo->exec('DELETE FROM nightowl_ptest');
        self::$pdo->exec('CREATE UNIQUE INDEX nightowl_ptest_id_created_at_pt ON nightowl_ptest (id, created_at)');
        $this->assertSame([], RawPartitions::healConversionLeftovers(self::$pdo, ['nightowl_ptest']));
        $this->assertTrue((bool) self::$pdo->query(
            "SELECT to_regclass('nightowl_ptest_id_created_at_pt') IS NOT NULL"
        )->fetchColumn(), 'a valid scaffolding index is a retry accelerator, not wreckage');
    }

    /**
     * The conversion's own scaffolding index ({t}_id_created_at_pt, built
     * CONCURRENTLY during prep so the historic child already satisfies the new
     * parent's composite PK) must NOT be replayed onto the parent as a user
     * index. Replaying it gives the parent an (id, created_at) unique index the
     * historic child lacks, so ATTACH builds one inline — full-table, under the
     * ACCESS EXCLUSIVE this whole design keeps brief — and every daily child
     * then carries a duplicate btree forever.
     */
    public function test_conversion_scaffolding_index_is_not_replayed_as_a_duplicate(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            trace_id varchar(255),
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        self::$pdo->exec('CREATE INDEX nightowl_ptest_trace_id_index ON nightowl_ptest (trace_id)');
        self::$pdo->exec("INSERT INTO nightowl_ptest (trace_id, created_at)
            SELECT 'pt-' || i, now() - (i || ' days')::interval FROM generate_series(1, 5) i");

        RawPartitions::convert(self::$pdo, 'nightowl_ptest');

        $this->assertFalse((bool) self::$pdo->query(
            "SELECT to_regclass('nightowl_ptest_id_created_at_pt_pt') IS NOT NULL"
        )->fetchColumn(), 'the scaffolding index must not be replayed under a _pt_pt name');

        // The real invariant behind that name check: the parent carries exactly
        // ONE (id, created_at) unique index — its PK — so ATTACH had a matching
        // child index and never needed an inline build.
        $uniques = (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_index x
             JOIN pg_class c ON c.oid = x.indexrelid
             WHERE x.indrelid = 'nightowl_ptest'::regclass AND x.indisunique"
        )->fetchColumn();
        $this->assertSame(1, $uniques, 'the parent must carry exactly one unique index (its PK)');

        // The real user index still made the trip.
        $this->assertSame(1, (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'nightowl_ptest' AND indexname = 'nightowl_ptest_trace_id_index_pt'"
        )->fetchColumn(), 'genuine user indexes must still be replayed');

        // And a fresh daily child inherits one unique index, not two. The first
        // day with a child of its own is the historic bound itself (the second
        // midnight ahead); every earlier day belongs to _phistoric.
        $child = RawPartitions::childName('nightowl_ptest', RawPartitions::historicBoundary(time()));
        $this->assertSame(1, (int) self::$pdo->query(
            "SELECT COUNT(*) FROM pg_index x WHERE x.indrelid = '{$child}'::regclass AND x.indisunique"
        )->fetchColumn(), 'daily children must not carry a duplicate unique btree');

        $this->assertSame(5, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest')->fetchColumn());
    }

    /**
     * A run killed between VALIDATE CONSTRAINT and the swap leaves a validated
     * {t}_hist_ck with a FROZEN boundary (plus the pre-built unique index).
     * Pre-fix the retry died 42710 re-ADDing it — and had the stale constraint
     * survived, its passed boundary would reject every new drain row. The
     * retry must rebuild the constraint with a fresh boundary.
     */
    public function test_convert_retries_after_a_crash_that_left_the_check_constraint(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        self::$pdo->exec("INSERT INTO nightowl_ptest (created_at)
            SELECT now() - (i || ' days')::interval FROM generate_series(1, 5) i");

        // The leftovers of a run from YESTERDAY killed after its prep: its
        // boundary (today 00:00 UTC) has already passed.
        $staleBoundary = gmdate('Y-m-d 00:00:00', intdiv(time(), 86400) * 86400);
        self::$pdo->exec(
            'CREATE UNIQUE INDEX nightowl_ptest_id_created_at_pt ON nightowl_ptest (id, created_at)'
        );
        self::$pdo->exec(
            "ALTER TABLE nightowl_ptest ADD CONSTRAINT nightowl_ptest_hist_ck
             CHECK (created_at IS NOT NULL AND created_at < '{$staleBoundary}')"
        );

        RawPartitions::convert(self::$pdo, 'nightowl_ptest');

        $this->assertTrue(RawPartitions::isPartitioned(self::$pdo, 'nightowl_ptest'));
        $this->assertSame(5, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest')->fetchColumn());

        // A row stamped NOW routes into the historic child (its bound is the
        // second midnight ahead) — with the stale constraint still in place this
        // INSERT would die 23514, so its success proves the boundary was rebuilt.
        self::$pdo->exec('INSERT INTO nightowl_ptest DEFAULT VALUES');
        $this->assertSame(6, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest')->fetchColumn());
    }

    /**
     * The maintenance tick on conversion day: the historic child covers today AND
     * tomorrow — its bound is the second midnight ahead, the headroom
     * historicBoundary() gives the conversion's own CHECK — so those days' children
     * would OVERLAP it (42P17 — IF NOT EXISTS only suppresses a name clash). The
     * sweep must skip EVERY covered day, not die on one before the other ten
     * tables get any children at all.
     */
    public function test_sweep_skips_days_the_historic_partition_still_covers(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        // Populated → convert ATTACHes _phistoric covering MINVALUE..the second
        // UTC midnight ahead (RawPartitions::historicBoundary).
        self::$pdo->exec("INSERT INTO nightowl_ptest (created_at)
            SELECT now() - (i || ' days')::interval FROM generate_series(1, 3) i");
        RawPartitions::convert(self::$pdo, 'nightowl_ptest');

        $failures = RawPartitions::ensureFutureChildren(self::$pdo, ['nightowl_ptest']);

        $today = intdiv(time(), 86400) * 86400;
        $boundary = RawPartitions::historicBoundary(time());

        // The child is absent whether the day was skipped or attempted and
        // rejected, so only a clean sweep tells those two apart.
        $this->assertSame([], $failures, "a day inside _phistoric's range must not be attempted at all");

        for ($d = 0; $d <= RawPartitions::DAYS_AHEAD; $d++) {
            $day = $today + $d * 86400;

            if ($day < $boundary) {
                $this->assertFalse($this->childExists('nightowl_ptest', $day),
                    "day +{$d} is inside _phistoric's range and must NOT get a child");

                continue;
            }

            $this->assertTrue($this->childExists('nightowl_ptest', $day),
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

    /**
     * F8. withLockTimeout is a CEILING, and it used to be an assignment. An
     * operator who set NIGHTOWL_DB_LOCK_TIMEOUT_MS=200 did it because even a
     * fraction of a second of ACCESS EXCLUSIVE queueing is an outage on their
     * table; the heal then overwrote it with the hardcoded 3000 and ran its
     * ALTER TABLE ... DROP CONSTRAINT under a 15x LOOSER ceiling than configured,
     * once a minute — the exact hazard the constant exists to prevent, inflicted
     * by the code preventing it.
     *
     * The blocker self-terminates via idle_in_transaction_session_timeout so an
     * over-loose heal makes this test slow rather than hanging forever.
     */
    public function test_a_tighter_configured_lock_timeout_is_never_raised_by_the_heal(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        $expired = gmdate('Y-m-d 00:00:00', intdiv(time(), 86400) * 86400);
        self::$pdo->exec(
            "ALTER TABLE nightowl_ptest ADD CONSTRAINT nightowl_ptest_hist_ck
             CHECK (created_at IS NOT NULL AND created_at < '{$expired}')"
        );

        $blocker = new PDO(self::$dsn, self::$username, self::$password);
        $blocker->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $blocker->exec('SET idle_in_transaction_session_timeout = 8000');
        $blocker->beginTransaction();
        $blocker->exec('LOCK TABLE nightowl_ptest IN ACCESS EXCLUSIVE MODE');

        $failures = [];
        $started = microtime(true);

        try {
            // A tighter caller-set lock_timeout, driven through the heal because
            // it is the cheapest withLockTimeout call site to set up. The HEAL no
            // longer inherits applyTransactionGuards (its caller opens no
            // transaction), but the hourly child sweep still runs under exactly
            // this shape, and withLockTimeout's tighten-only rule is shared.
            self::$pdo->beginTransaction();
            self::$pdo->exec('SET LOCAL lock_timeout = 200');
            $healed = RawPartitions::healConversionLeftovers(self::$pdo, ['nightowl_ptest'], $failures);
            self::$pdo->commit();
        } finally {
            if (self::$pdo->inTransaction()) {
                self::$pdo->rollBack();
            }
            try {
                $blocker->rollBack();
            } catch (\Throwable) {
                // Already terminated by its own idle timeout.
            }
        }

        $this->assertLessThan(2.0, microtime(true) - $started,
            "the operator's 200ms ceiling must stand — raising it to the hardcoded 3000 is the bug");
        $this->assertSame([], $healed);
        $this->assertCount(1, $failures);
        $this->assertStringContainsString('55P03', $failures[0]);
    }

    /**
     * F4. ensureChildWindowBounded was wired into the operator command's path only
     * (via childWindowOrGaps); the DRAIN's hourly sweep still called
     * ensureChildWindow raw, so its CREATE TABLE ... PARTITION OF — ACCESS
     * EXCLUSIVE on the parent — carried no ceiling of its own. Under
     * NIGHTOWL_DRAIN_CONN_TIMEOUTS=false (a documented rollback switch that drops
     * BOTH of applyTransactionGuards' SET LOCALs) it queued unbounded, and Postgres
     * parks every later reader and writer of that table behind the pending
     * exclusive request for the blocker's whole duration.
     *
     * The caller's transaction here sets no lock_timeout at all, which IS the
     * kill-switch shape — and it doubles as the test that 0 is read as "wait
     * forever" rather than as the tightest value: a plain min() ceiling would
     * compute 0 here and leave the sweep exactly as unbounded as before.
     *
     * Exactly one day is missing, and CREATE TABLE IF NOT EXISTS whose child
     * already exists returns without ever requesting the parent lock (verified
     * against postgres:17), so precisely one statement contends.
     *
     * The blocker self-terminates via idle_in_transaction_session_timeout so an
     * unbounded sweep makes this test slow rather than hanging forever.
     */
    public function test_the_hourly_child_sweep_bounds_its_own_access_exclusive_wait(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        RawPartitions::convert(self::$pdo, 'nightowl_ptest');

        $today = intdiv(time(), 86400) * 86400;
        $missing = $today + 3 * 86400;
        $child = RawPartitions::childName('nightowl_ptest', $missing);
        self::$pdo->exec('DROP TABLE '.$child);

        $blocker = new PDO(self::$dsn, self::$username, self::$password);
        $blocker->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $blocker->exec('SET idle_in_transaction_session_timeout = 12000');
        $blocker->beginTransaction();
        $blocker->exec('LOCK TABLE nightowl_ptest IN ACCESS EXCLUSIVE MODE');

        $failures = [];
        $started = microtime(true);

        try {
            // A transaction with NO lock_timeout on it — the drain's shape under
            // NIGHTOWL_DRAIN_CONN_TIMEOUTS=false.
            self::$pdo->beginTransaction();
            $failures = RawPartitions::ensureFutureChildren(self::$pdo, ['nightowl_ptest']);
            self::$pdo->commit();
        } finally {
            if (self::$pdo->inTransaction()) {
                self::$pdo->rollBack();
            }
            try {
                $blocker->rollBack();
            } catch (\Throwable) {
                // Already terminated by its own idle timeout.
            }
        }

        $this->assertLessThan(10.0, microtime(true) - $started,
            "the hourly sweep must bound its own ACCESS EXCLUSIVE wait, not inherit the caller's (absent) one");
        $this->assertCount(1, $failures, 'only the contended day may be reported');
        $this->assertStringContainsString($child, $failures[0]);
        $this->assertStringContainsString('55P03', $failures[0], 'the report must carry the real cause');
    }

    /**
     * F13. The baseline pid used to be sampled in a round trip SEPARATE from
     * pg_try_advisory_lock, which left the one backend move that matters most
     * undetectable: behind a transaction-mode pooler the lock lands on backend A,
     * the very next statement lands on B, and B becomes the "expected" pid — so
     * every later assertSameBackend() compares against B and passes, the
     * conversion runs its non-transactional prep believing it is serialised while
     * the key sits unreachable on A, and the final pg_advisory_unlock answers
     * false over a key stranded there, refusing every later run until that backend
     * is recycled.
     *
     * The fake moves the connection the instant the lock statement is issued: that
     * statement itself runs on the original backend, everything after it does not.
     * Taking the lock and the pid in ONE statement is what closes it — a pooler
     * cannot split a single statement across backends, so the gap is genuinely
     * zero rather than one round trip wide.
     */
    public function test_a_backend_move_between_the_lock_and_its_baseline_is_detected(): void
    {
        self::$pdo->exec('CREATE TABLE nightowl_ptest (
            id bigserial PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )');
        self::$pdo->exec("INSERT INTO nightowl_ptest (created_at)
            SELECT now() - (i || ' days')::interval FROM generate_series(1, 5) i");

        $moving = new class(self::$dsn, self::$username, self::$password) extends PDO
        {
            private bool $lockIssued = false;

            public function prepare(string $query, array $options = []): \PDOStatement|false
            {
                // The statement that TAKES the key runs on the original backend,
                // pid and all. Everything issued after it has "moved".
                if (str_contains($query, 'pg_try_advisory_lock')) {
                    $stmt = parent::prepare($query, $options);
                    $this->lockIssued = true;

                    return $stmt;
                }

                return parent::prepare($this->moved($query), $options);
            }

            public function query(string $query, ?int $fetchMode = null, mixed ...$fetchModeArgs): \PDOStatement|false
            {
                return parent::query($this->moved($query), $fetchMode, ...$fetchModeArgs);
            }

            private function moved(string $query): string
            {
                return $this->lockIssued
                    ? str_replace('pg_backend_pid()', '(pg_backend_pid() + 1)', $query)
                    : $query;
            }
        };
        $moving->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        try {
            RawPartitions::convert($moving, 'nightowl_ptest');
            $this->fail('a move between taking the conversion key and baselining the backend must be detected');
        } catch (PoolerAffinityException $e) {
            $this->assertStringContainsString('pooler', $e->getMessage());
        }

        $this->assertFalse(RawPartitions::isPartitioned(self::$pdo, 'nightowl_ptest'),
            'the abort must land before anything destructive');
        $this->assertSame(5, (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_ptest')->fetchColumn());
    }
}
