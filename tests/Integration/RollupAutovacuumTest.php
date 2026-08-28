<?php

namespace NightOwl\Tests\Integration;

use PDO;
use PHPUnit\Framework\TestCase;

/**
 * Pins migration 000071: autovacuum frequency on the rollup tables must not
 * scale INVERSELY with tier coarseness.
 *
 * Migration 000055 put `autovacuum_vacuum_scale_factor = 0.02` on every rollup
 * table. Because the drain upserts all three tiers on every batch, the coarse
 * tiers take the same update volume as the minute tier while holding 60x/1440x
 * fewer rows — so 2% of a 574-row daily table is ~61 dead tuples, and Postgres
 * re-vacuums it every naptime forever. A live tenant showed 3,227 lifetime
 * vacuums on nightowl_job_daily_rollups (574 rows) against 32 on
 * nightowl_query_rollups (1.3M rows).
 *
 * The guard is the effective trigger — `threshold + scale_factor * reltuples`
 * — not the raw setting, because that is the number Postgres actually uses.
 */
final class RollupAutovacuumTest extends TestCase
{
    /** Must match migration 000071. */
    private const VACUUM_THRESHOLD = 50_000;

    /** 14 rollup types x 3 tiers, plus the bespoke concurrency rollup. */
    private const EXPECTED_ROLLUP_TABLES = 43;

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
    }

    public function test_no_rollup_table_is_left_without_reloptions(): void
    {
        $missing = [];

        foreach ($this->rollupTables() as $table => $opts) {
            if (($opts['fillfactor'] ?? null) !== '70'
                || ($opts['autovacuum_vacuum_scale_factor'] ?? null) !== '0.02'
                || ($opts['autovacuum_analyze_scale_factor'] ?? null) !== '0.02') {
                $missing[] = $table;
            }
        }

        $this->assertSame([], $missing, implode("\n", [
            'These rollup tables are missing the standard reloptions.',
            'The hand-listed table sets in 000053/000054/000055 are how this',
            'happens: a rollup type added later (concurrency, 000063) or a',
            'table rebuilt by CacheRollupSwap never gets them. 000071',
            'discovers tables from the catalog precisely to avoid that.',
        ]));
    }

    public function test_tier_tables_carry_an_absolute_vacuum_floor(): void
    {
        $tiers = array_filter(
            $this->rollupTables(),
            fn (array $o, string $t): bool => $this->isTier($t),
            ARRAY_FILTER_USE_BOTH
        );

        $this->assertNotEmpty($tiers, 'No tier rollup tables found — 000054 did not run.');

        foreach ($tiers as $table => $opts) {
            $this->assertSame(
                (string) self::VACUUM_THRESHOLD,
                $opts['autovacuum_vacuum_threshold'] ?? null,
                "{$table} has no absolute autovacuum_vacuum_threshold — it will vacuum on every naptime once its row count is small."
            );
            $this->assertSame(
                (string) self::VACUUM_THRESHOLD,
                $opts['autovacuum_analyze_threshold'] ?? null,
                "{$table} has no absolute autovacuum_analyze_threshold."
            );
        }
    }

    /**
     * The minute tables are deliberately left proportional: they are large
     * enough for 2% to be a sane trigger, and retention DELETEs against them
     * produce dead tuples that genuinely need collecting.
     */
    public function test_minute_tables_stay_proportional(): void
    {
        foreach ($this->rollupTables() as $table => $opts) {
            if ($this->isTier($table)) {
                continue;
            }

            $this->assertArrayNotHasKey(
                'autovacuum_vacuum_threshold',
                $opts,
                "{$table} is a minute-tier table and should not carry an absolute vacuum floor."
            );
        }
    }

    /**
     * The behavioural invariant, stated the way Postgres computes it. A tier
     * table must never be vacuumable at a trivial dead-tuple count no matter
     * how few rows it holds — that is the regression 000071 exists to stop.
     */
    public function test_tier_vacuum_trigger_cannot_collapse_on_a_tiny_table(): void
    {
        $rows = self::$pdo->query(
            "SELECT c.relname, GREATEST(c.reltuples, 0) AS reltuples,
                    COALESCE((SELECT option_value FROM pg_options_to_table(c.reloptions)
                               WHERE option_name = 'autovacuum_vacuum_threshold'), '50') AS thresh,
                    COALESCE((SELECT option_value FROM pg_options_to_table(c.reloptions)
                               WHERE option_name = 'autovacuum_vacuum_scale_factor'), '0.2') AS scale
               FROM pg_class c
               JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = current_schema()
                AND c.relkind = 'r'
                AND c.relname LIKE 'nightowl\_%\_rollups'"
        )->fetchAll(PDO::FETCH_ASSOC);

        foreach ($rows as $r) {
            if (! $this->isTier($r['relname'])) {
                continue;
            }

            $trigger = (float) $r['thresh'] + ((float) $r['scale'] * (float) $r['reltuples']);

            $this->assertGreaterThanOrEqual(
                self::VACUUM_THRESHOLD,
                $trigger,
                "{$r['relname']} triggers autovacuum at {$trigger} dead tuples. On a coarse tier that "
                    ."collapses toward zero as the table shrinks, which is the inverse-scaling bug: the "
                    .'table is re-read from disk every autovacuum_naptime.'
            );
        }
    }

    public function test_discovery_covers_the_tables_the_hand_lists_missed(): void
    {
        $tables = $this->rollupTables();

        $this->assertArrayHasKey('nightowl_request_concurrency_rollups', $tables);
        $this->assertArrayHasKey('nightowl_cache_rollups', $tables);
        $this->assertArrayHasKey('nightowl_cache_hourly_rollups', $tables);
        $this->assertArrayHasKey('nightowl_cache_daily_rollups', $tables);

        $this->assertCount(
            self::EXPECTED_ROLLUP_TABLES,
            $tables,
            'Rollup table count changed. If you added a rollup type, confirm its tier tables '
                .'exist and that 000071 covers them — it discovers from the catalog, so this '
                .'count is the only thing that will tell you the shape changed.'
        );
    }

    /** @return array<string, array<string, string>> table => reloption => value */
    private function rollupTables(): array
    {
        $rows = self::$pdo->query(
            "SELECT c.relname, COALESCE(array_to_string(c.reloptions, ','), '') AS opts
               FROM pg_class c
               JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = current_schema()
                AND c.relkind = 'r'
                AND c.relname LIKE 'nightowl\_%\_rollups'
              ORDER BY c.relname"
        )->fetchAll(PDO::FETCH_ASSOC);

        $out = [];
        foreach ($rows as $r) {
            $opts = [];
            foreach (array_filter(explode(',', $r['opts'])) as $pair) {
                [$k, $v] = explode('=', $pair, 2);
                $opts[$k] = $v;
            }
            $out[$r['relname']] = $opts;
        }

        return $out;
    }

    private function isTier(string $table): bool
    {
        return str_ends_with($table, '_hourly_rollups') || str_ends_with($table, '_daily_rollups');
    }
}
