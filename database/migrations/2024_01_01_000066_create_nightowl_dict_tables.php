<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Storage-format-v2 dictionaries. Raw v2 tables (000067) store small int
     * ids where v1 repeated the same strings on every row; these four tables
     * hold each distinct value exactly once.
     *
     *  - dict_string: low-cardinality labels, keyed (kind, value). `kind` is a
     *    closed set of StorageV2::KIND_* constants (environment, server,
     *    deploy, execution_source, execution_stage, connection, queue, status,
     *    event_type, method, level, channel, store, host, connection_type,
     *    job_class, cache_pattern).
     *  - dict_sql:    one row per distinct (sql, file, line) tuple, keyed by a
     *    16-byte xxh128 content hash. Replaces queries.sql_query/file/line.
     *  - dict_route:  one row per distinct route tuple, content-hash keyed and
     *    APPEND-ONLY — a renamed controller action gets a NEW row, history
     *    keeps pointing at the old one (no-loss rule: never overwrite).
     *  - dict_trace:  one row per distinct exception stack trace, deflated.
     *
     * All four are append-only (writers use ON CONFLICT DO NOTHING and only
     * ever read ids back). They are deliberately EXCLUDED from nightowl:prune
     * and nightowl:clear: a running daemon's LRU caches value→id, and removing
     * rows under it would leave later telemetry referencing dead ids. They are
     * tiny relative to raw telemetry (hundreds of rows per tenant, not
     * millions; dict_trace GC is a tracked follow-up).
     */
    public function up(): void
    {
        $ddl = [
            'nightowl_dict_string' => 'CREATE TABLE nightowl_dict_string (
                id     serial PRIMARY KEY,
                kind   varchar(32)  NOT NULL,
                value  varchar(512) NOT NULL,
                CONSTRAINT nightowl_dict_string_kind_value_unique UNIQUE (kind, value)
            )',
            'nightowl_dict_sql' => 'CREATE TABLE nightowl_dict_sql (
                id    bigserial PRIMARY KEY,
                hash  bytea NOT NULL,
                sql   text  NOT NULL,
                file  varchar(512),
                line  integer,
                CONSTRAINT nightowl_dict_sql_hash_unique UNIQUE (hash)
            )',
            'nightowl_dict_route' => 'CREATE TABLE nightowl_dict_route (
                id       bigserial PRIMARY KEY,
                hash     bytea NOT NULL,
                method   varchar(16),
                domain   varchar(255),
                path     varchar(512),
                name     varchar(255),
                action   varchar(512),
                methods  varchar(255),
                CONSTRAINT nightowl_dict_route_hash_unique UNIQUE (hash)
            )',
            'nightowl_dict_trace' => 'CREATE TABLE nightowl_dict_trace (
                id       bigserial PRIMARY KEY,
                hash     bytea NOT NULL,
                trace_z  bytea NOT NULL,
                CONSTRAINT nightowl_dict_trace_hash_unique UNIQUE (hash)
            )',
        ];

        foreach ($ddl as $table => $create) {
            if (Schema::connection($this->connection)->hasTable($table)) {
                continue;
            }

            DB::connection($this->connection)->statement($create);
        }
    }

    public function down(): void
    {
        foreach (['nightowl_dict_trace', 'nightowl_dict_route', 'nightowl_dict_sql', 'nightowl_dict_string'] as $table) {
            DB::connection($this->connection)->statement("DROP TABLE IF EXISTS {$table}");
        }
    }
};
