<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * DDSketch percentile storage (specs/ddsketch_percentiles.md): a sparse
     * varint-packed sketch per rollup row alongside the v1 hist_NN columns
     * (dual-write transition; v1 stays readable for the whole rollup
     * retention). `sketch_version` freezes the mapping that wrote each row —
     * a mapping change is a NEW version, never a re-edge in place, because
     * 14-90-day-old rollups cannot be rebuilt from pruned raw.
     *
     * nightowl_ddsketch_merge(bytea, bytea) mirrors the PHP pack layout
     * (DDSketchHistogram, checksum-guarded both repos): unsigned-LEB128
     * varints, (index_delta, count) pairs ascending, first delta measured from
     * the underflow index (-1). It runs inside the drain's ON CONFLICT SET, so
     * concurrent workers serialise on the row lock with no PHP read-modify-
     * write. nightowl_ddsketch_agg powers the tier backfill's GROUP BY
     * (minute→hour→day re-aggregation — post-dates the spec).
     *
     * Capability probe: some locked-down managed PG deny CREATE FUNCTION. In
     * that case we skip the sketch columns entirely — the reader then keeps
     * using the v1 √2 path, which is never worse than today.
     */
    private const BASE_TABLES = [
        'nightowl_query_rollups',
        'nightowl_request_rollups',
        'nightowl_job_rollups',
        'nightowl_outgoing_request_rollups',
        'nightowl_mail_rollups',
        'nightowl_notification_rollups',
        'nightowl_command_rollups',
        'nightowl_scheduled_task_rollups',
    ];

    private const TIER_SUFFIXES = ['hourly', 'daily'];

    public function up(): void
    {
        if (! $this->createMergeFunction()) {
            return; // reader stays on the v1 √2 path; log emitted below
        }

        $schema = Schema::connection($this->connection);

        foreach ($this->allTables() as $table) {
            if (! $schema->hasColumn($table, 'sketch')) {
                $schema->table($table, function ($t): void {
                    $t->binary('sketch')->nullable();
                    $t->smallInteger('sketch_version')->nullable();
                });
            }
        }
    }

    public function down(): void
    {
        $schema = Schema::connection($this->connection);

        foreach ($this->allTables() as $table) {
            if ($schema->hasColumn($table, 'sketch')) {
                $schema->table($table, function ($t): void {
                    $t->dropColumn(['sketch', 'sketch_version']);
                });
            }
        }

        DB::connection($this->connection)->statement('DROP AGGREGATE IF EXISTS nightowl_ddsketch_agg(bytea)');
        DB::connection($this->connection)->statement('DROP FUNCTION IF EXISTS nightowl_ddsketch_merge(bytea, bytea)');
    }

    private function createMergeFunction(): bool
    {
        try {
            DB::connection($this->connection)->statement(<<<'SQL'
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
            -- varint: index delta
            val := 0; shift := 0;
            LOOP
                IF pos >= len THEN RAISE EXCEPTION 'ddsketch payload truncated'; END IF;
                byte := get_byte(payload, pos); pos := pos + 1;
                val := val | ((byte & 127)::bigint << shift);
                EXIT WHEN (byte & 128) = 0;
                shift := shift + 7;
            END LOOP;
            delta := val;
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

            // Single-pair encoder: pack one (index, count) into the varint
            // layout, so SQL-side GROUP BYs can build sketches without PHP —
            // nightowl_ddsketch_agg(nightowl_ddsketch_single(idx, cnt)) is the
            // backfill's whole sketch pipeline. First delta is idx − (−1).
            DB::connection($this->connection)->statement(<<<'SQL'
CREATE OR REPLACE FUNCTION nightowl_ddsketch_single(idx bigint, cnt bigint)
RETURNS bytea LANGUAGE plpgsql IMMUTABLE AS $fn$
DECLARE
    out bytea := ''::bytea;
    val bigint;
BEGIN
    IF cnt IS NULL OR cnt <= 0 THEN RETURN ''::bytea; END IF;
    val := idx + 1; -- delta from the underflow index (-1)
    WHILE val >= 128 LOOP
        out := out || decode(lpad(to_hex((val % 128) + 128), 2, '0'), 'hex');
        val := val / 128;
    END LOOP;
    out := out || decode(lpad(to_hex(val), 2, '0'), 'hex');
    val := cnt;
    WHILE val >= 128 LOOP
        out := out || decode(lpad(to_hex((val % 128) + 128), 2, '0'), 'hex');
        val := val / 128;
    END LOOP;
    out := out || decode(lpad(to_hex(val), 2, '0'), 'hex');
    RETURN out;
END;
$fn$
SQL);

            DB::connection($this->connection)->statement(
                "CREATE OR REPLACE AGGREGATE nightowl_ddsketch_agg(bytea) (SFUNC = nightowl_ddsketch_merge, STYPE = bytea, INITCOND = '')"
            );

            return true;
        } catch (\Throwable $e) {
            error_log('[NightOwl] CREATE FUNCTION denied — DDSketch percentiles disabled, √2 histogram remains: '.$e->getMessage());

            return false;
        }
    }

    /** @return list<string> */
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
