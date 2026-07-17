<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * nightowl_ddsketch_count(bytea) — the number of samples packed into a
     * sketch. Decodes the same varint layout as nightowl_ddsketch_merge
     * (000057) but keeps only the counts.
     *
     * The v1 hist_NN drop guard (V1HistogramCleanup::verify) needs it to prove
     * that a row's sketch actually covers the histogram mass it is about to
     * replace. A non-NULL sketch is not evidence of that: nightowl_ddsketch_agg
     * has INITCOND = '' over a non-strict SFUNC, so a tier bucket whose minute
     * rows all pre-date raw retention aggregates to an EMPTY sketch, and a
     * bucket straddling the retention edge aggregates to a PARTIAL one. Both
     * are non-NULL and both would silently lose their percentiles on drop.
     *
     * Returns NULL on a truncated payload — the guard reads that as "cannot
     * prove coverage" and blocks, rather than the merge function's RAISE,
     * which would abort the whole verify pass over one corrupt row.
     */
    public function up(): void
    {
        try {
            DB::connection($this->connection)->statement(<<<'SQL'
CREATE OR REPLACE FUNCTION nightowl_ddsketch_count(payload bytea)
RETURNS bigint LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $fn$
DECLARE
    pos int; len int;
    byte int; shift int; val bigint;
    total bigint := 0;
BEGIN
    IF payload IS NULL OR length(payload) = 0 THEN RETURN 0; END IF;

    pos := 0; len := length(payload);
    WHILE pos < len LOOP
        -- varint: index delta (positional only — the guard sums counts)
        LOOP
            IF pos >= len THEN RETURN NULL; END IF;
            byte := get_byte(payload, pos); pos := pos + 1;
            EXIT WHEN (byte & 128) = 0;
        END LOOP;
        -- varint: count
        val := 0; shift := 0;
        LOOP
            IF pos >= len THEN RETURN NULL; END IF;
            byte := get_byte(payload, pos); pos := pos + 1;
            val := val | ((byte & 127)::bigint << shift);
            EXIT WHEN (byte & 128) = 0;
            shift := shift + 7;
        END LOOP;
        total := total + val;
    END LOOP;

    RETURN total;
END;
$fn$
SQL);
        } catch (\Throwable $e) {
            // Same capability probe as 000057: a managed PG that denies CREATE
            // FUNCTION has no sketch columns either, so the reader stays on v1
            // and the guard refuses the drop for want of this function.
            error_log('[NightOwl] CREATE FUNCTION denied — nightowl:drop-v1-histograms stays unavailable: '.$e->getMessage());
        }
    }

    public function down(): void
    {
        DB::connection($this->connection)->statement('DROP FUNCTION IF EXISTS nightowl_ddsketch_count(bytea)');
    }
};
