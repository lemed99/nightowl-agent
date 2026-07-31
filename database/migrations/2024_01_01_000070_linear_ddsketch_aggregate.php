<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Make DDSketch aggregation linear in its input.
     *
     * 000057 declared nightowl_ddsketch_agg with STYPE = bytea over
     * nightowl_ddsketch_merge, so every input row decoded the WHOLE accumulated
     * sketch into a jsonb map and re-encoded it. Cost per row therefore grew
     * with the state rather than staying flat, which is what put the 14d
     * percentile charts past a 20s statement_timeout (the 57014 reports).
     *
     * The index domain is closed — UNDERFLOW = -1 .. OVERFLOW = 1261 in
     * src/Support/DDSketchHistogram.php — so the state can be a dense
     * bigint[] instead: 1263 count slots at position idx + 2, plus slots
     * 1264/1265 carrying the min/max slot touched. accum() decodes ONE payload
     * per call and adds into the array; finalize() packs once per group, over
     * the occupied span only. Nothing re-encodes per input row.
     *
     * Measured against THIS migration (not a prototype), 200 routes x 336
     * hours = 67,200 rollup rows, referencing every output column so the
     * planner cannot elide the aggregate, serial plan, jit off. Two profiles,
     * because the whole cost model turns on how many indices a row occupies:
     * (A) 40-byte sketches, ~20 occupied indices — a route whose durations sit
     * in a narrow band; (B) 763-byte sketches, ~380 indices — a route spanning
     * milliseconds to seconds. Both are real rollup shapes.
     *
     *                                   (A) 40-byte        (B) 763-byte
     *                                  old  ->  new       old  ->  new
     *   50 groups x 336 rows       103,031 ->    477   >120,000 ->  1,977
     *   ungrouped 67,200 rows     >150,000 ->  1,779   >150,000 ->  7,381
     *   67,200 groups x 1 row        3,618 ->  4,491   >120,000 -> 23,675
     *
     * The first row is the read that was failing (histogramP95Map over 14d);
     * the second is the overview's ungrouped percentile. On (B) the old fold
     * did not finish 4 groups in 18.4s, so the >120,000 entries are floors,
     * not measurements.
     *
     * The last row is the one shape that gets SLOWER, and only on (A): a group
     * holding a single rollup row never builds a state worth amortising and
     * pays the array setup instead. It is kept rather than special-cased
     * because it is also the shape that was never in trouble — and on (B) even
     * that shape is a straight win, since a 380-index payload costs the old
     * fold more to decode than the array costs to allocate.
     *
     * nightowl_ddsketch_merge keeps its (bytea, bytea) signature — the drain's
     * ON CONFLICT SET calls it per rollup upsert — and is re-expressed over the
     * same primitives, which made 9,715 real merge pairs byte-identical and
     * 4.85x faster (1,413ms -> 291ms).
     *
     * Every output is byte-identical to the old fold: 1,022 groups across both
     * profiles and both group shapes, 0 mismatches, plus the boundary cases
     * (zero rows, all-NULL, empty payloads, the underflow and overflow indices,
     * a full-span sketch, multi-byte counts, repeated indices, a zero count).
     *
     * DROP + CREATE is forced by the STYPE change and runs inside the
     * migration's transaction, so a failure leaves the old aggregate in place
     * rather than no aggregate at all. Replacing merge() before the aggregate
     * is rebuilt is safe: the old declaration's INITCOND = '' feeds the new
     * body an empty first argument, which accum() skips.
     *
     * Anything that re-runs 000057 AFTER this silently undoes it. Postgres
     * accepts a CREATE OR REPLACE AGGREGATE that changes STYPE without error or
     * notice, so 000057's declaration puts the quadratic bytea fold back while
     * every function this migration created stays in place — there is no
     * artifact left that reads as "reverted". The migrator makes that
     * unreachable in production (000057 has a history row and never runs
     * twice), but the test harness replays migrations by hand: see the
     * nightowl_ddsketch clause in RecordWriterTest::restoreQueryRollupsSchema
     * and MigrationRunner's aggtranstype probe, both of which exist for this.
     * A later migration that restores 000057-era objects must re-assert this
     * declaration after it.
     */
    public function up(): void
    {
        $conn = DB::connection($this->connection);

        // 000057's capability probe: a managed PG that denied CREATE FUNCTION
        // has no merge function and no sketch columns, so there is no
        // aggregate to make linear — the reader is on the v1 bins there.
        $present = $conn->selectOne("SELECT to_regprocedure('nightowl_ddsketch_merge(bytea, bytea)') IS NOT NULL AS present");
        if (! $present || ! $present->present) {
            error_log('[NightOwl] nightowl_ddsketch_merge absent — DDSketch stayed disabled on this database, nothing to upgrade');

            return;
        }

        $conn->statement(<<<'SQL'
CREATE OR REPLACE FUNCTION nightowl_ddsketch_accum(state bigint[], payload bytea)
RETURNS bigint[] LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $fn$
DECLARE
    pos int; len int; prev bigint;
    byte int; shift int; val bigint;
    cnt bigint; slot int;
BEGIN
    -- Before the allocation: a group of NULL/empty sketches never builds one.
    IF payload IS NULL OR length(payload) = 0 THEN
        RETURN state;
    END IF;

    IF state IS NULL THEN
        -- 1..1263 = counts (position = index + 2); 1264/1265 = min/max touched.
        state := array_fill(0::bigint, ARRAY[1265]);
    END IF;

    pos := 0; len := length(payload); prev := -1;
    WHILE pos < len LOOP
        -- varint: index delta
        val := 0; shift := 0;
        LOOP
            IF pos >= len THEN RAISE EXCEPTION 'ddsketch payload truncated'; END IF;
            byte := get_byte(payload, pos); pos := pos + 1;
            val := val | ((byte & 127)::bigint << shift);
            EXIT WHEN (byte & 128) = 0;
            shift := shift + 7;
        END LOOP;
        prev := prev + val;
        -- varint: count
        val := 0; shift := 0;
        LOOP
            IF pos >= len THEN RAISE EXCEPTION 'ddsketch payload truncated'; END IF;
            byte := get_byte(payload, pos); pos := pos + 1;
            val := val | ((byte & 127)::bigint << shift);
            EXIT WHEN (byte & 128) = 0;
            shift := shift + 7;
        END LOOP;
        cnt := val;

        slot := (prev + 2)::int;
        IF slot < 1 OR slot > 1263 THEN
            RAISE EXCEPTION 'ddsketch index % out of range', prev;
        END IF;
        state[slot] := state[slot] + cnt;
        IF state[1264] = 0 OR slot < state[1264] THEN state[1264] := slot; END IF;
        IF slot > state[1265] THEN state[1265] := slot; END IF;
    END LOOP;

    RETURN state;
END;
$fn$
SQL);

        // COMBINEFUNC: lets the planner split the aggregate across parallel
        // workers. Only reachable via a parallel plan; the serial path never
        // calls it.
        $conn->statement(<<<'SQL'
CREATE OR REPLACE FUNCTION nightowl_ddsketch_combine(a bigint[], b bigint[])
RETURNS bigint[] LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $fn$
DECLARE
    slot int;
BEGIN
    IF a IS NULL THEN RETURN b; END IF;
    IF b IS NULL OR b[1264] = 0 THEN RETURN a; END IF;
    IF a[1264] = 0 THEN RETURN b; END IF;

    FOR slot IN b[1264]..b[1265] LOOP
        CONTINUE WHEN b[slot] = 0;
        a[slot] := a[slot] + b[slot];
    END LOOP;
    IF b[1264] < a[1264] THEN a[1264] := b[1264]; END IF;
    IF b[1265] > a[1265] THEN a[1265] := b[1265]; END IF;

    RETURN a;
END;
$fn$
SQL);

        // Packs the same layout DDSketchHistogram writes: unsigned-LEB128
        // varints, (index_delta, count) pairs ascending, first delta measured
        // from the underflow index (-1). Zero-count slots are skipped, which is
        // what the old jsonb fold did by never recording them.
        $conn->statement(<<<'SQL'
CREATE OR REPLACE FUNCTION nightowl_ddsketch_finalize(state bigint[])
RETURNS bytea LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $fn$
DECLARE
    out bytea := ''::bytea;
    slot int; prev bigint := -1; idx bigint; val bigint; c bigint;
BEGIN
    IF state IS NULL OR state[1264] = 0 THEN RETURN ''::bytea; END IF;

    FOR slot IN state[1264]..state[1265] LOOP
        c := state[slot];
        CONTINUE WHEN c = 0;
        idx := slot - 2;

        val := idx - prev;
        WHILE val >= 128 LOOP
            out := out || decode(lpad(to_hex((val % 128) + 128), 2, '0'), 'hex');
            val := val / 128;
        END LOOP;
        out := out || decode(lpad(to_hex(val), 2, '0'), 'hex');

        val := c;
        WHILE val >= 128 LOOP
            out := out || decode(lpad(to_hex((val % 128) + 128), 2, '0'), 'hex');
            val := val / 128;
        END LOOP;
        out := out || decode(lpad(to_hex(val), 2, '0'), 'hex');

        prev := idx;
    END LOOP;

    RETURN out;
END;
$fn$
SQL);

        $conn->statement(<<<'SQL'
CREATE OR REPLACE FUNCTION nightowl_ddsketch_merge(a bytea, b bytea)
RETURNS bytea LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $fn$
    SELECT nightowl_ddsketch_finalize(
        nightowl_ddsketch_accum(nightowl_ddsketch_accum(NULL::bigint[], a), b)
    )
$fn$
SQL);

        // STYPE changes, so the aggregate has to be dropped and recreated —
        // CREATE OR REPLACE AGGREGATE cannot change it. Both statements are in
        // the migration's transaction: a failure rolls back to the 000057
        // aggregate rather than leaving none.
        $conn->statement('DROP AGGREGATE IF EXISTS nightowl_ddsketch_agg(bytea)');
        $conn->statement(<<<'SQL'
CREATE AGGREGATE nightowl_ddsketch_agg(bytea) (
    SFUNC = nightowl_ddsketch_accum,
    STYPE = bigint[],
    COMBINEFUNC = nightowl_ddsketch_combine,
    FINALFUNC = nightowl_ddsketch_finalize,
    PARALLEL = SAFE
)
SQL);
    }

    /**
     * Restores 000057's bytea-state fold verbatim, so a rollback leaves the
     * database in the schema state the earlier release expects.
     */
    public function down(): void
    {
        $conn = DB::connection($this->connection);

        $present = $conn->selectOne("SELECT to_regprocedure('nightowl_ddsketch_accum(bigint[], bytea)') IS NOT NULL AS present");
        if (! $present || ! $present->present) {
            return;
        }

        $conn->statement(<<<'SQL'
CREATE OR REPLACE FUNCTION nightowl_ddsketch_merge(a bytea, b bytea)
RETURNS bytea LANGUAGE plpgsql IMMUTABLE AS $fn$
DECLARE
    m jsonb := '{}'::jsonb;
    payload bytea;
    pos int; len int; prev bigint;
    byte int; shift int; val bigint;
    delta bigint; cnt bigint;
    out bytea := ''::bytea;
    rec record;
BEGIN
    FOREACH payload IN ARRAY ARRAY[a, b] LOOP
        CONTINUE WHEN payload IS NULL OR length(payload) = 0;
        pos := 0; len := length(payload); prev := -1;
        WHILE pos < len LOOP
            val := 0; shift := 0;
            LOOP
                IF pos >= len THEN RAISE EXCEPTION 'ddsketch payload truncated'; END IF;
                byte := get_byte(payload, pos); pos := pos + 1;
                val := val | ((byte & 127)::bigint << shift);
                EXIT WHEN (byte & 128) = 0;
                shift := shift + 7;
            END LOOP;
            delta := val;
            val := 0; shift := 0;
            LOOP
                IF pos >= len THEN RAISE EXCEPTION 'ddsketch payload truncated'; END IF;
                byte := get_byte(payload, pos); pos := pos + 1;
                val := val | ((byte & 127)::bigint << shift);
                EXIT WHEN (byte & 128) = 0;
                shift := shift + 7;
            END LOOP;
            cnt := val;
            prev := prev + delta;
            m := jsonb_set(m, ARRAY[prev::text],
                to_jsonb(COALESCE((m ->> prev::text)::bigint, 0) + cnt));
        END LOOP;
    END LOOP;

    prev := -1;
    FOR rec IN SELECT key::bigint AS idx, value::bigint AS c
               FROM jsonb_each_text(m) ORDER BY key::bigint LOOP
        val := rec.idx - prev;
        WHILE val >= 128 LOOP
            out := out || decode(lpad(to_hex((val % 128) + 128), 2, '0'), 'hex');
            val := val / 128;
        END LOOP;
        out := out || decode(lpad(to_hex(val), 2, '0'), 'hex');
        val := rec.c;
        WHILE val >= 128 LOOP
            out := out || decode(lpad(to_hex((val % 128) + 128), 2, '0'), 'hex');
            val := val / 128;
        END LOOP;
        out := out || decode(lpad(to_hex(val), 2, '0'), 'hex');
        prev := rec.idx;
    END LOOP;

    RETURN out;
END;
$fn$
SQL);

        $conn->statement('DROP AGGREGATE IF EXISTS nightowl_ddsketch_agg(bytea)');
        $conn->statement(
            "CREATE AGGREGATE nightowl_ddsketch_agg(bytea) (SFUNC = nightowl_ddsketch_merge, STYPE = bytea, INITCOND = '')"
        );

        $conn->statement('DROP FUNCTION IF EXISTS nightowl_ddsketch_finalize(bigint[])');
        $conn->statement('DROP FUNCTION IF EXISTS nightowl_ddsketch_combine(bigint[], bigint[])');
        $conn->statement('DROP FUNCTION IF EXISTS nightowl_ddsketch_accum(bigint[], bytea)');
    }
};
