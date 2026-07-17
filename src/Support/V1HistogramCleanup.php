<?php

namespace NightOwl\Support;

use PDO;

/**
 * The v1→v2 histogram cleanup (specs/ddsketch_percentiles.md §Versioning):
 * once every rollup row within retention carries a v2 DDSketch, the 39
 * hist_NN columns per duration-bearing table are dead weight — dropping them
 * roughly halves those tables' row width.
 *
 * verify() is the guard the drop command refuses without: on ANY duration-
 * bearing table (all three tiers), any row whose sketch is not proven to hold
 * at least the samples its bins hold blocks the drop, because the reader's v1
 * fallback needs the bins for exactly those rows. The drop is irreversible —
 * the bins are gone and the raw rows behind them are long pruned — so the
 * guard blocks whenever coverage is unproven, not only when it is disproven.
 */
final class V1HistogramCleanup
{
    /** Duration-bearing rollup types (the tables 000057 gave sketches to). */
    public const BASE_TABLES = [
        'nightowl_query_rollups',
        'nightowl_request_rollups',
        'nightowl_job_rollups',
        'nightowl_outgoing_request_rollups',
        'nightowl_mail_rollups',
        'nightowl_notification_rollups',
        'nightowl_command_rollups',
        'nightowl_scheduled_task_rollups',
    ];

    /**
     * Types whose avg denominator is derived from the hist bin sum (see the
     * API's RollupReader::durationCountExpr). Dropping their bins without the
     * duration_count replacement column (migration 000061) would destroy the
     * denominator, so verify() blocks until it exists.
     */
    public const DENOMINATOR_BASES = [
        'nightowl_mail_rollups',
        'nightowl_notification_rollups',
        'nightowl_command_rollups',
        'nightowl_scheduled_task_rollups',
    ];

    /** verify() marker: table lacks the sketch column entirely. */
    public const MISSING_SKETCH = -1;

    /** verify() marker: table lacks the duration_count replacement column. */
    public const MISSING_DURATION_COUNT = -2;

    /** verify() marker: nightowl_ddsketch_count is absent (migration 000062). */
    public const MISSING_COUNT_FN = -3;

    /**
     * @param  list<string>|null  $bases  test hook; production uses BASE_TABLES
     * @return list<string> all tables (base + hourly + daily), existing ones only
     */
    public static function tables(PDO $conn, ?array $bases = null): array
    {
        $tables = [];
        foreach ($bases ?? self::BASE_TABLES as $base) {
            foreach (RollupTiers::tables($base) as $table => $granularity) {
                if (self::tableExists($conn, $table)) {
                    $tables[] = $table;
                }
            }
        }

        return $tables;
    }

    /**
     * Rows whose v1 bins are not yet fully replaced, per table. Empty result =
     * safe to drop. A table without the sketch column at all (000057 skipped —
     * CREATE FUNCTION denied) is reported with -1: the drop is categorically
     * unavailable there.
     *
     * The guard proves coverage rather than looking for a counterexample: a
     * row is only clear once its sketch is shown to hold at least the samples
     * its bins hold. Anything that cannot be proven — a missing counting
     * function, an unreadable sketch — blocks.
     *
     * @return array<string, int> table => unreplaced row count (or a marker)
     */
    public static function verify(PDO $conn, ?array $bases = null): array
    {
        $offenders = [];

        // Test hook: injected bases are all treated as denominator-gated.
        $denominatorTables = self::tables($conn, $bases ?? self::DENOMINATOR_BASES);

        $canCountSamples = self::functionExists($conn, 'nightowl_ddsketch_count(bytea)');

        foreach (self::tables($conn, $bases) as $table) {
            if (! self::columnExists($conn, $table, 'sketch')) {
                $offenders[$table] = self::MISSING_SKETCH;

                continue;
            }
            // The hist sum is these types' avg denominator; its replacement
            // column must exist before the bins may go (migration 000061).
            if (in_array($table, $denominatorTables, true) && ! self::columnExists($conn, $table, 'duration_count')) {
                $offenders[$table] = self::MISSING_DURATION_COUNT;

                continue;
            }
            if (! self::columnExists($conn, $table, 'hist_00')) {
                continue; // already dropped
            }
            // Coverage is unprovable without the counter (migration 000062).
            if (! $canCountSamples) {
                $offenders[$table] = self::MISSING_COUNT_FN;

                continue;
            }

            // Only rows with actual histogram mass block the drop. Backfilled
            // dispatch-only buckets (a queued job minute with no attempt) have
            // sketch NULL but all-zero bins — dropping their hist columns
            // loses nothing, and counting them would hold the guard hostage
            // for the full 90 days after every backfill. Summed over the hist
            // columns actually present (tolerant of partial layouts).
            $stmt = $conn->prepare(
                "SELECT column_name FROM information_schema.columns
                 WHERE table_schema = 'public' AND table_name = ? AND column_name LIKE 'hist\\_%'"
            );
            $stmt->execute([$table]);
            $histCols = $stmt->fetchAll(PDO::FETCH_COLUMN);
            $histSum = '('.implode(' + ', $histCols).')';

            // A non-NULL sketch is not evidence of coverage: nightowl_ddsketch_agg
            // carries INITCOND = '' over a NULL-skipping SFUNC, so a tier bucket
            // whose minute rows all pre-date raw retention aggregates to an EMPTY
            // sketch, and one straddling the retention edge to a PARTIAL sketch.
            // An unreadable sketch counts NULL, which loses to the -1 and blocks.
            $unproven = ["COALESCE(nightowl_ddsketch_count(sketch), -1) < {$histSum}"];

            // Post-drop these types read their avg denominator straight off
            // duration_count (the API's RollupReader::durationCountExpr drops the
            // GREATEST against the bins), so a row the column never caught up on
            // — written by a pre-000061 drain still caching the old layout —
            // would silently start averaging over zero.
            if (in_array($table, $denominatorTables, true)) {
                $unproven[] = "duration_count < {$histSum}";
            }

            $count = (int) $conn->query(
                "SELECT COUNT(*) FROM {$table}
                 WHERE {$histSum} > 0 AND (".implode(' OR ', $unproven).')'
            )->fetchColumn();
            if ($count > 0) {
                $offenders[$table] = $count;
            }
        }

        return $offenders;
    }

    /**
     * Drop the hist_NN columns everywhere. Call ONLY after verify() returns
     * empty. Returns the tables actually altered.
     *
     * @return list<string>
     */
    public static function drop(PDO $conn, ?array $bases = null): array
    {
        $altered = [];

        foreach (self::tables($conn, $bases) as $table) {
            // Drop the hist columns the table actually has — tolerant of
            // partial layouts, and a no-op second run.
            $stmt = $conn->prepare(
                "SELECT column_name FROM information_schema.columns
                 WHERE table_schema = 'public' AND table_name = ? AND column_name LIKE 'hist\\_%'"
            );
            $stmt->execute([$table]);
            $present = $stmt->fetchAll(PDO::FETCH_COLUMN);
            if ($present === []) {
                continue;
            }

            $drops = implode(', ', array_map(
                static fn (string $c): string => "DROP COLUMN {$c}",
                $present,
            ));
            $conn->exec("ALTER TABLE {$table} {$drops}");
            $altered[] = $table;
        }

        return $altered;
    }

    private static function tableExists(PDO $conn, string $table): bool
    {
        $stmt = $conn->prepare('SELECT to_regclass(?) IS NOT NULL');
        $stmt->execute([$table]);

        return (bool) $stmt->fetchColumn();
    }

    /** @param  string  $signature  name + argument types, e.g. 'f(bytea)' */
    private static function functionExists(PDO $conn, string $signature): bool
    {
        $stmt = $conn->prepare('SELECT to_regprocedure(?) IS NOT NULL');
        $stmt->execute([$signature]);

        return (bool) $stmt->fetchColumn();
    }

    private static function columnExists(PDO $conn, string $table, string $column): bool
    {
        $stmt = $conn->prepare(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ? AND column_name = ?"
        );
        $stmt->execute([$table, $column]);

        return (int) $stmt->fetchColumn() > 0;
    }
}
