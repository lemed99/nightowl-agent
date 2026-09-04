<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * The uncompressed twin of nightowl_logs_v2.context_z, so log search can
     * reach inside a log's context.
     *
     * v2 stores context as a deflate stream in `context_z bytea`, and Postgres
     * has no inflate (no built-in, and pgcrypto does not add one), so NO sql
     * predicate can see the plaintext — which is why the dashboard narrows a
     * log search to the message alone once a tenant carries v2 rows. With
     * `nightowl.log_context_searchable` on, the drain writes the plaintext
     * here instead and an ordinary LIKE works again.
     *
     * The column is added unconditionally but written only under that flag, so
     * this migration is inert on an install that never turns it on: one null
     * column on a table whose rows keep exactly the shape they have today.
     * Both columns stay readable forever — the API renders whichever of the
     * two a row populated — so no row ever has to be rewritten to stay
     * VISIBLE. Rewriting (nightowl:backfill-log-context) is only about making
     * OLD rows searchable.
     *
     * No index: an unanchored `LIKE '%needle%'` cannot use a btree, and the
     * trigram index that could is a `CREATE EXTENSION` we must never require
     * of a customer's own database. Search stays a scan of the window, exactly
     * as the message half of the same predicate already is.
     *
     * nightowl_logs_v2 is daily-partitioned; ADD COLUMN on the parent
     * propagates to every child and to partitions created later, and takes
     * only a brief ACCESS EXCLUSIVE lock (no table rewrite — the added column
     * is nullable with no default).
     */
    public function up(): void
    {
        $schema = Schema::connection($this->connection);

        if (! $schema->hasTable('nightowl_logs_v2')) {
            return; // pre-v2 tenant — nothing to extend
        }
        if ($schema->hasColumn('nightowl_logs_v2', 'context')) {
            return;
        }

        DB::connection($this->connection)->statement(
            'ALTER TABLE nightowl_logs_v2 ADD COLUMN context text'
        );
    }

    public function down(): void
    {
        DB::connection($this->connection)->statement(
            'ALTER TABLE nightowl_logs_v2 DROP COLUMN IF EXISTS context'
        );
    }
};
