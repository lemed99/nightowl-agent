<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Support\V1HistogramCleanup;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * The guarded hist_NN cleanup (nightowl:drop-v1-histograms): verify() must
 * block while any row lacks a v2 sketch or any table lacks the sketch column,
 * and drop() must remove exactly the hist columns once clean. Runs against
 * synthetic tables so the shared suite's real rollups keep their bins.
 */
final class V1HistogramCleanupTest extends TestCase
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

        foreach (['nightowl_fake_rollups', 'nightowl_fake_hourly_rollups', 'nightowl_fake_daily_rollups'] as $t) {
            self::$pdo->exec("DROP TABLE IF EXISTS {$t}");
            self::$pdo->exec("CREATE TABLE {$t} (
                group_hash varchar(64), bucket_start timestamp, environment varchar(64) DEFAULT '',
                call_count bigint DEFAULT 0, hist_00 bigint DEFAULT 0, hist_38 bigint DEFAULT 0,
                duration_count bigint DEFAULT 0, sketch bytea, sketch_version smallint
            )");
        }
    }

    protected function tearDown(): void
    {
        if (self::$pdo !== null) {
            foreach (['nightowl_fake_rollups', 'nightowl_fake_hourly_rollups', 'nightowl_fake_daily_rollups'] as $t) {
                self::$pdo->exec("DROP TABLE IF EXISTS {$t}");
            }
        }
    }

    public function test_verify_blocks_on_v1_only_rows_with_histogram_mass(): void
    {
        // 'a' is fully replaced: 3 samples in the sketch for 3 in the bins.
        self::$pdo->exec("INSERT INTO nightowl_fake_rollups (group_hash, bucket_start, sketch, hist_00, duration_count)
            VALUES ('a', now(), '\\x0103', 3, 3), ('b', now(), NULL, 5, 5)");

        $offenders = V1HistogramCleanup::verify(self::$pdo, ['nightowl_fake_rollups']);

        $this->assertSame(['nightowl_fake_rollups' => 1], $offenders);
    }

    public function test_verify_ignores_sketchless_rows_with_zero_bins(): void
    {
        // A dispatch-only bucket: no durations, all-zero bins, NULL sketch —
        // dropping its hist columns loses nothing, so it must not block.
        self::$pdo->exec("INSERT INTO nightowl_fake_rollups (group_hash, bucket_start, sketch, hist_00, hist_38)
            VALUES ('dispatch-only', now(), NULL, 0, 0)");

        $this->assertSame([], V1HistogramCleanup::verify(self::$pdo, ['nightowl_fake_rollups']));
    }

    /**
     * The tier backfill's own aggregate over minute rows that all pre-date raw
     * retention: nightowl_ddsketch_agg has INITCOND = '' over a NULL-skipping
     * SFUNC, so it yields an EMPTY sketch, not NULL. Such a row still carries
     * its bins, and dropping them would strand it with no percentile source.
     */
    public function test_verify_blocks_on_empty_sketch_from_aggregating_sketchless_rows(): void
    {
        $aggregated = self::$pdo->query(
            'SELECT nightowl_ddsketch_agg(s) FROM (VALUES (NULL::bytea), (NULL::bytea)) t(s)'
        )->fetchColumn();
        $aggregated = is_resource($aggregated) ? stream_get_contents($aggregated) : $aggregated;
        $this->assertNotNull($aggregated, 'premise: the aggregate yields empty, never NULL');

        $insert = self::$pdo->prepare("INSERT INTO nightowl_fake_rollups
            (group_hash, bucket_start, sketch, hist_00, duration_count) VALUES ('past-retention', now(), :s, 5, 5)");
        $insert->bindValue(':s', $aggregated, PDO::PARAM_LOB);
        $insert->execute();

        $this->assertSame(
            ['nightowl_fake_rollups' => 1],
            V1HistogramCleanup::verify(self::$pdo, ['nightowl_fake_rollups']),
        );
    }

    /**
     * A tier bucket straddling the raw-retention edge: some minute rows still
     * had raw to rebuild from, the rest did not. The aggregate is non-NULL and
     * non-empty, but covers only part of the bins' mass.
     */
    public function test_verify_blocks_on_partial_sketch(): void
    {
        // 3 samples in the sketch against 9 in the bins.
        self::$pdo->exec("INSERT INTO nightowl_fake_rollups (group_hash, bucket_start, sketch, hist_00, duration_count)
            VALUES ('boundary', now(), '\\x0603', 9, 9)");

        $this->assertSame(
            ['nightowl_fake_rollups' => 1],
            V1HistogramCleanup::verify(self::$pdo, ['nightowl_fake_rollups']),
        );
    }

    /**
     * Post-drop the avg denominator comes straight off duration_count, so a row
     * a pre-000061 drain never populated must keep its bins.
     */
    public function test_verify_blocks_when_duration_count_undercounts_histogram_mass(): void
    {
        self::$pdo->exec("INSERT INTO nightowl_fake_rollups (group_hash, bucket_start, sketch, hist_00, duration_count)
            VALUES ('no-denominator', now(), '\\x0103', 3, 0)");

        $this->assertSame(
            ['nightowl_fake_rollups' => 1],
            V1HistogramCleanup::verify(self::$pdo, ['nightowl_fake_rollups']),
        );
    }

    /** Coverage cannot be proven without the counter, so the drop is refused. */
    public function test_verify_refuses_when_sample_count_function_missing(): void
    {
        self::$pdo->exec("INSERT INTO nightowl_fake_rollups (group_hash, bucket_start, sketch, hist_00, duration_count)
            VALUES ('a', now(), '\\x0103', 3, 3)");

        // DDL is transactional here: the rollback restores the shared test DB.
        self::$pdo->beginTransaction();

        try {
            self::$pdo->exec('DROP FUNCTION nightowl_ddsketch_count(bytea)');

            $offenders = V1HistogramCleanup::verify(self::$pdo, ['nightowl_fake_rollups']);

            $this->assertSame(V1HistogramCleanup::MISSING_COUNT_FN, $offenders['nightowl_fake_rollups'] ?? null);
        } finally {
            self::$pdo->rollBack();
        }
    }

    /**
     * verify() runs nightowl_ddsketch_count once per row over whole rollup
     * tables. The function is pure byte arithmetic over the sketch, so it must
     * carry the PARALLEL SAFE label that lets the planner parallelise that scan.
     */
    public function test_sample_count_function_is_parallel_safe(): void
    {
        // Re-apply 000062 so the assertion reflects the migration on disk, not
        // a definition an earlier run may have left on the shared test DB;
        // CREATE OR REPLACE makes this a no-op when it already matches.
        (require __DIR__.'/../../database/migrations/2024_01_01_000062_add_ddsketch_count_function.php')->up();

        $proparallel = self::$pdo->query(
            "SELECT proparallel FROM pg_proc WHERE proname = 'nightowl_ddsketch_count'"
        )->fetchColumn();

        $this->assertSame('s', $proparallel, 'nightowl_ddsketch_count must be PARALLEL SAFE');
    }

    public function test_verify_blocks_when_sketch_column_missing(): void
    {
        self::$pdo->exec('ALTER TABLE nightowl_fake_hourly_rollups DROP COLUMN sketch, DROP COLUMN sketch_version');

        $offenders = V1HistogramCleanup::verify(self::$pdo, ['nightowl_fake_rollups']);

        $this->assertSame(V1HistogramCleanup::MISSING_SKETCH, $offenders['nightowl_fake_hourly_rollups'] ?? null);
    }

    public function test_verify_blocks_when_duration_count_column_missing(): void
    {
        // The hist bin sum is the avg denominator for these types — the bins
        // may not go until the duration_count replacement exists (000061).
        self::$pdo->exec('ALTER TABLE nightowl_fake_daily_rollups DROP COLUMN duration_count');

        $offenders = V1HistogramCleanup::verify(self::$pdo, ['nightowl_fake_rollups']);

        $this->assertSame(V1HistogramCleanup::MISSING_DURATION_COUNT, $offenders['nightowl_fake_daily_rollups'] ?? null);
    }

    public function test_drop_removes_hist_columns_when_clean(): void
    {
        // Fully replaced: the sketch's samples account for the bins' mass.
        self::$pdo->exec("INSERT INTO nightowl_fake_rollups (group_hash, bucket_start, sketch, hist_00, duration_count)
            VALUES ('a', now(), '\\x0103', 3, 3)");

        $this->assertSame([], V1HistogramCleanup::verify(self::$pdo, ['nightowl_fake_rollups']));

        $altered = V1HistogramCleanup::drop(self::$pdo, ['nightowl_fake_rollups']);
        $this->assertCount(3, $altered, 'all three tiers must be altered');

        $remaining = (int) self::$pdo->query(
            "SELECT COUNT(*) FROM information_schema.columns
             WHERE table_name LIKE 'nightowl_fake%' AND column_name LIKE 'hist\\_%'"
        )->fetchColumn();
        $this->assertSame(0, $remaining);

        // Idempotent: a second drop alters nothing.
        $this->assertSame([], V1HistogramCleanup::drop(self::$pdo, ['nightowl_fake_rollups']));
    }
}
