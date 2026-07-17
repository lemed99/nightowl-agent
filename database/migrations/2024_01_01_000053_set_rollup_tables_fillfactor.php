<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * fillfactor = 70 on every rollup table. Rollup rows are upserted
     * continuously at drain time — the current minute's row for a hot group is
     * UPDATEd on every drain batch, dozens to hundreds of times over its
     * minute. At the default fillfactor (100) pages are packed full at insert,
     * so those updates can't fit the new row version on the same page: they go
     * non-HOT, writing new entries into the PK and bucket_start indexes on
     * every update and leaving dead tuples only autovacuum can reclaim. The
     * bloat concentrates on the most recent buckets — exactly the range the
     * narrow-window chart queries scan (statement_timeout 504 incident,
     * 2026-07-16). 30% per-page headroom keeps those updates HOT: no index
     * writes, dead versions pruned on-page without vacuum.
     *
     * Metadata-only ALTER (instant; brief exclusive lock, drain transactions
     * are short). Applies to newly written pages only — existing pages benefit
     * as rows cycle out via nightowl:prune, or after a manual VACUUM FULL.
     */
    private const ROLLUP_TABLES = [
        'nightowl_query_rollups',
        'nightowl_request_rollups',
        'nightowl_job_rollups',
        'nightowl_outgoing_request_rollups',
        'nightowl_cache_rollups',
        'nightowl_user_rollups',
        'nightowl_user_job_rollups',
        'nightowl_user_exception_rollups',
        'nightowl_exception_rollups',
        'nightowl_exception_server_rollups',
        'nightowl_mail_rollups',
        'nightowl_notification_rollups',
        'nightowl_command_rollups',
        'nightowl_scheduled_task_rollups',
    ];

    public function up(): void
    {
        $schema = Schema::connection($this->connection);

        foreach (self::ROLLUP_TABLES as $table) {
            if (! $schema->hasTable($table)) {
                continue;
            }

            DB::connection($this->connection)->statement(
                "ALTER TABLE {$table} SET (fillfactor = 70)"
            );
        }
    }

    public function down(): void
    {
        $schema = Schema::connection($this->connection);

        foreach (self::ROLLUP_TABLES as $table) {
            if (! $schema->hasTable($table)) {
                continue;
            }

            DB::connection($this->connection)->statement(
                "ALTER TABLE {$table} RESET (fillfactor)"
            );
        }
    }
};
