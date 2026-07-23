<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * key_pattern on the raw cache events: the CacheKeyTemplate-templated form
     * of `key` (`user:8213:profile` → `user:{int}:profile`), computed once in
     * PHP at drain time and stored so the cache rollup's SQL group form can
     * read it back (COALESCE(key_pattern, key, '')) instead of re-deriving it
     * — that read-what-PHP-wrote shape is what makes the rollup's PHP and SQL
     * group forms provably equivalent (a shared regex can't be: PCRE and
     * POSIX ARE diverge on the shipped defaults).
     *
     * Nullable, no default, no index: a catalog-only ADD COLUMN on both the
     * partitioned parent (cascades to children) and a pre-partition plain
     * table. NULL means "written before templating" and COALESCEs to the
     * literal key — historic rollup rows keep the grouping their drain wrote,
     * new rows group by pattern, and the two coexist in the same rollup table
     * without any rewrite of history (boot-migrate's 300s deadline forbids
     * one anyway).
     *
     * Ordering contract: nightowl:backfill-rollups over cache MUST run after
     * this migration (the spec's SQL form references the column). Every
     * shipped path already does — migrate auto-backfills, and the manual
     * runbook is migrate-then-backfill.
     */
    public function up(): void
    {
        if (! Schema::connection($this->connection)->hasTable('nightowl_cache_events')) {
            return;
        }
        if (Schema::connection($this->connection)->hasColumn('nightowl_cache_events', 'key_pattern')) {
            return;
        }

        DB::connection($this->connection)->statement(
            'ALTER TABLE nightowl_cache_events ADD COLUMN key_pattern varchar(255)'
        );
    }

    public function down(): void
    {
        if (Schema::connection($this->connection)->hasColumn('nightowl_cache_events', 'key_pattern')) {
            DB::connection($this->connection)->statement(
                'ALTER TABLE nightowl_cache_events DROP COLUMN key_pattern'
            );
        }
    }
};
