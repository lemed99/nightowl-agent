<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Aggressive autovacuum on every rollup table (minute + hourly + daily
     * tiers). The drain UPDATEs the current bucket's row on every batch —
     * dozens to hundreds of times over its lifetime — so dead-tuple churn is
     * the tables' steady state. Postgres' default trigger (20% of the table
     * dead) lets that churn accumulate into index bloat on exactly the recent
     * buckets every chart reads; 2% keeps vacuum pacing the writes instead.
     * Complements fillfactor 70 (migration 000053): fillfactor keeps updates
     * HOT and self-pruning on-page, autovacuum reclaims what HOT can't.
     *
     * Metadata-only ALTERs (instant, brief lock — drain transactions are
     * short). Same hand-listed base set as 000053/000054; tier names derived
     * the same way 000054 derives them.
     */
    private const BASE_TABLES = [
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

    private const TIER_SUFFIXES = ['hourly', 'daily'];

    public function up(): void
    {
        foreach ($this->allTables() as $table) {
            DB::connection($this->connection)->statement(
                "ALTER TABLE {$table} SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.02)"
            );
        }
    }

    public function down(): void
    {
        foreach ($this->allTables() as $table) {
            DB::connection($this->connection)->statement(
                "ALTER TABLE {$table} RESET (autovacuum_vacuum_scale_factor, autovacuum_analyze_scale_factor)"
            );
        }
    }

    /** @return list<string> every existing rollup table across all tiers */
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
