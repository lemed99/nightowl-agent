<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * duration_count — the number of duration-bearing rows folded into each
     * rollup bucket — for the four types whose avg denominator cannot come
     * from call_count (queued mail/notifications carry no duration, so
     * dividing by call_count dilutes the average). Today the API derives that
     * denominator by summing the 39 v1 hist_NN bins; those bins are dropped
     * by nightowl:drop-v1-histograms, so the count must live in its own
     * column first. Backfilled here from the hist sums — exact, since the
     * drain increments exactly one bin per duration — while the bins still
     * exist (the drop command refuses to run until this column is present).
     *
     * The full-table UPDATE rewrites every row once; these four types are
     * low-cardinality (groups × minutes), and fillfactor 70 (000053) absorbs
     * the churn. Tables created after this migration get the column with its
     * DEFAULT and are written directly by the drain (which probes for it, so
     * an un-migrated tenant keeps its rollups minus the new counter).
     */
    private const BASE_TABLES = [
        'nightowl_mail_rollups',
        'nightowl_notification_rollups',
        'nightowl_command_rollups',
        'nightowl_scheduled_task_rollups',
    ];

    private const TIER_SUFFIXES = ['hourly', 'daily'];

    public function up(): void
    {
        $conn = DB::connection($this->connection);

        foreach ($this->allTables() as $table) {
            $conn->statement(
                "ALTER TABLE {$table} ADD COLUMN IF NOT EXISTS duration_count bigint NOT NULL DEFAULT 0"
            );

            // Seed from the v1 bins when they are still present. hist mass ==
            // duration-bearing rows by construction, so this matches what the
            // drain would have written. Rows that accumulate between this
            // UPDATE and the post-migrate agent restart under-count slightly;
            // nightowl:backfill-rollups recomputes them from raw.
            if (Schema::connection($this->connection)->hasColumn($table, 'hist_00')) {
                $histSum = implode(' + ', array_map(
                    static fn (int $i): string => sprintf('hist_%02d', $i),
                    range(0, 38),
                ));
                $conn->statement("UPDATE {$table} SET duration_count = {$histSum} WHERE duration_count = 0");
            }
        }
    }

    public function down(): void
    {
        foreach ($this->allTables() as $table) {
            DB::connection($this->connection)->statement(
                "ALTER TABLE {$table} DROP COLUMN IF EXISTS duration_count"
            );
        }
    }

    /** @return list<string> every existing table of the four types across all tiers */
    private function allTables(): array
    {
        $schema = Schema::connection($this->connection);
        $tables = [];

        foreach (self::BASE_TABLES as $base) {
            $candidates = [$base];
            foreach (self::TIER_SUFFIXES as $tier) {
                $candidates[] = str_replace('_rollups', "_{$tier}_rollups", $base);
            }
            foreach ($candidates as $table) {
                if ($schema->hasTable($table)) {
                    $tables[] = $table;
                }
            }
        }

        return $tables;
    }
};
