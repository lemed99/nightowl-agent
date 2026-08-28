<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use NightOwl\Support\RollupTiers;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Decouple rollup autovacuum frequency from row count.
     *
     * Migration 000055 set `autovacuum_vacuum_scale_factor = 0.02` on every
     * rollup table, minute and tier alike. On the minute tables that is right:
     * 2% of a 1.3M-row table is ~26k dead tuples, so vacuum paces the drain.
     * On the COARSE tiers it inverts — the whole point of an hourly/daily
     * rollup is to hold 60x/1440x fewer rows, but they absorb the SAME update
     * volume, because the drain upserts all three tiers on every batch. 2% of
     * a 574-row table is ~61 dead tuples, reached in seconds, so autovacuum
     * re-reads the table every `autovacuum_naptime` forever.
     *
     * Measured on a live tenant (2026-08-26), lifetime autovacuum counts:
     *
     *   nightowl_job_daily_rollups        574 rows,  67 MB — 3,227 vacuums
     *   nightowl_query_daily_rollups   18,932 rows, 273 MB — 2,905 vacuums
     *   nightowl_request_daily_rollups  3,729 rows,  52 MB — 2,670 vacuums
     *   nightowl_query_rollups      1,321,274 rows, 1.7 GB —    32 vacuums
     *
     * Each pass re-reads the table's heap, and because the churn keeps these
     * tables physically far larger than their live rows, that is ~13 GB/hour
     * of disk reads from a single 273 MB table — competing for the buffer pool
     * and IOPS the drain needs at peak.
     *
     * The fix is an ABSOLUTE floor alongside the existing proportional factor.
     * Postgres triggers on `dead > threshold + scale_factor * reltuples`, so
     * keeping 0.02 and adding a 50k threshold gives sane behaviour at both
     * ends: tiny tier tables stop vacuuming on every naptime, while a large
     * tier table on a big tenant still scales proportionally.
     *
     * Tables are DISCOVERED from the catalog rather than hand-listed. The
     * hand-lists in 000053/000054/000055 are why four tables sit in production
     * with no reloptions at all: nightowl_request_concurrency_rollups was
     * added in 000063 (after the lists were written), and the three cache
     * rollup tables lose theirs on every CacheRollupSwap rebuild.
     *
     * Metadata-only ALTERs — instant, and the brief lock is safe against the
     * drain's short transactions.
     */
    private const VACUUM_THRESHOLD = 50_000;

    private const SCALE_FACTOR = '0.02';

    private const FILLFACTOR = 70;

    public function up(): void
    {
        $statements = [];

        foreach ($this->rollupTables() as $table) {
            $opts = [
                'fillfactor = '.self::FILLFACTOR,
                'autovacuum_vacuum_scale_factor = '.self::SCALE_FACTOR,
                'autovacuum_analyze_scale_factor = '.self::SCALE_FACTOR,
            ];

            // The absolute floor only goes on the coarse tiers — the minute
            // tables are large enough that the proportional factor already
            // yields a sane threshold, and retention DELETEs against them
            // produce dead tuples that genuinely need collecting.
            if ($this->isTierTable($table)) {
                $opts[] = 'autovacuum_vacuum_threshold = '.self::VACUUM_THRESHOLD;
                $opts[] = 'autovacuum_analyze_threshold = '.self::VACUUM_THRESHOLD;
            }

            $statements[] = "ALTER TABLE {$table} SET (".implode(', ', $opts).')';
        }

        $this->runBatch($statements);
    }

    /**
     * Drops only the thresholds this migration introduces. The scale factors
     * and fillfactor are deliberately NOT reset: for most tables they restate
     * what 000053/000055 already intended, and for the four that were missing
     * them entirely, resetting would re-open the gap this migration closed.
     */
    public function down(): void
    {
        $statements = [];

        foreach ($this->rollupTables() as $table) {
            if ($this->isTierTable($table)) {
                $statements[] = "ALTER TABLE {$table} RESET (autovacuum_vacuum_threshold, autovacuum_analyze_threshold)";
            }
        }

        $this->runBatch($statements);
    }

    /**
     * One round trip for ~43 metadata-only ALTERs.
     *
     * Round trips, not rows, are what make schema work slow against a distant
     * BYO database — a customer with a ~100ms app→DB hop measured 72s on a
     * command that issued ~250 microsecond-cheap statements (see
     * `nightowl:migrate` / MigrateRoundTripTest). Statement-per-table here
     * would repeat that at a smaller scale, so they ship as one simple-protocol
     * batch. Laravel wraps the migration in a transaction on PostgreSQL, so
     * this stays all-or-nothing.
     *
     * @param  list<string>  $statements
     */
    private function runBatch(array $statements): void
    {
        if ($statements === []) {
            return;
        }

        DB::connection($this->connection)->unprepared(implode(";\n", $statements).';');
    }

    /**
     * Every rollup table that actually exists, from the catalog. Covers types
     * added after the hand-lists were written and tables rebuilt by a swap.
     *
     * @return list<string>
     */
    private function rollupTables(): array
    {
        $rows = DB::connection($this->connection)->select(
            "SELECT c.relname
               FROM pg_class c
               JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = current_schema()
                AND c.relkind = 'r'
                AND c.relname LIKE 'nightowl\_%\_rollups'
              ORDER BY c.relname"
        );

        return array_map(static fn ($r) => $r->relname, $rows);
    }

    /** `*_hourly_rollups` / `*_daily_rollups` — the coarse tiers. */
    private function isTierTable(string $table): bool
    {
        foreach (array_keys(RollupTiers::TIERS) as $tier) {
            if (str_ends_with($table, "_{$tier}_rollups")) {
                return true;
            }
        }

        return false;
    }
};
