<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Last-referenced clock for nightowl_dict_trace, so the trace dictionary
     * can finally be garbage-collected (nightowl:gc-dict-traces) — the one
     * follow-up 000066 flagged. The other three dicts stay un-GC'd: their
     * values are unbounded-lifetime keys (a route/sql/label id, once minted,
     * may be referenced by any future row), whereas a deflated stack trace is
     * only ever pointed at by exception rows, which age out at retention — so
     * once every referencing exception is pruned the trace is dead weight.
     *
     * The GC's race-safety rests on this column: the drain's dictionary warm
     * pass now bumps created_at = now() every time a trace is referenced
     * (DictionaryCache trace insert → ON CONFLICT DO UPDATE, and the collector
     * always re-warms traces instead of trusting the LRU), so an actively-used
     * trace is always "young" and the GC's `created_at < now() - quarantine`
     * predicate — the cheap timestamp proxy for "recently referenced" — can
     * never target a trace an in-flight batch is about to point at. A backfill
     * scan of exception rows keeps them referenced regardless of this clock.
     *
     * Existing rows are stamped now() by the DEFAULT — a one-time grace window
     * (they all start young, then either get touched on their next reference or
     * age past the quarantine and, if unreferenced, get reclaimed). The table
     * is small (distinct traces, not per-row), so the volatile-default rewrite
     * is trivial and well inside the boot-migrate deadline.
     */
    public function up(): void
    {
        if (! Schema::connection($this->connection)->hasTable('nightowl_dict_trace')) {
            return;
        }
        if (Schema::connection($this->connection)->hasColumn('nightowl_dict_trace', 'created_at')) {
            return;
        }

        DB::connection($this->connection)->statement(
            'ALTER TABLE nightowl_dict_trace ADD COLUMN created_at timestamptz NOT NULL DEFAULT now()'
        );

        // The GC's candidate scan filters on created_at; the DELETE walks it too.
        DB::connection($this->connection)->statement(
            'CREATE INDEX IF NOT EXISTS nightowl_dict_trace_created_at_idx ON nightowl_dict_trace (created_at)'
        );
    }

    public function down(): void
    {
        DB::connection($this->connection)->statement('DROP INDEX IF EXISTS nightowl_dict_trace_created_at_idx');
        DB::connection($this->connection)->statement('ALTER TABLE nightowl_dict_trace DROP COLUMN IF EXISTS created_at');
    }
};
