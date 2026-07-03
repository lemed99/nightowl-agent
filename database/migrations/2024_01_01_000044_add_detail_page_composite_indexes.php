<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Composite indexes for the per-entity DETAIL pages and filter dropdowns, which
     * cannot be served from the route/user rollups (they read individual rows or
     * scan one entity's rows over the window). Without these, a detail page for a
     * single hot route/user/fingerprint sequentially scans that entity's whole
     * history — nightowl_jobs had NO group_hash index at all, so job-class detail
     * was a full table scan.
     *
     * Access patterns:
     *   (group_hash, created_at)  → route/query/job/host/command/task detail row lists + charts
     *   (user_id, created_at)     → user-detail pages (top-routes, slowest, top-jobs, per-user charts)
     *   (fingerprint, created_at) → issues list/detail/occurrences
     *   (created_at, user_id)     → DISTINCT-user filter dropdowns for types with no per-user rollup
     *
     * CONCURRENTLY (hence $withinTransaction = false): these run on customers' live
     * production DBs, several on the highest-volume tables (requests/queries/jobs/
     * logs). A plain blocking CREATE INDEX would hold a write lock for the whole
     * build and stall the agent's drain. CONCURRENTLY trades a slower build for no
     * write lock. IF NOT EXISTS keeps it idempotent across re-runs / partial applies.
     */
    public $withinTransaction = false;

    /** @var list<array{0: string, 1: string, 2: string}> [index name, table, column list] */
    private const INDEXES = [
        ['nightowl_requests_group_hash_created_at_idx', 'nightowl_requests', 'group_hash, created_at'],
        ['nightowl_requests_user_id_created_at_idx', 'nightowl_requests', 'user_id, created_at'],
        ['nightowl_queries_group_hash_created_at_idx', 'nightowl_queries', 'group_hash, created_at'],
        ['nightowl_jobs_group_hash_created_at_idx', 'nightowl_jobs', 'group_hash, created_at'],
        ['nightowl_jobs_user_id_created_at_idx', 'nightowl_jobs', 'user_id, created_at'],
        ['nightowl_exceptions_fingerprint_created_at_idx', 'nightowl_exceptions', 'fingerprint, created_at'],
        ['nightowl_exceptions_user_id_created_at_idx', 'nightowl_exceptions', 'user_id, created_at'],
        ['nightowl_outgoing_requests_group_hash_created_at_idx', 'nightowl_outgoing_requests', 'group_hash, created_at'],
        ['nightowl_outgoing_requests_created_at_user_id_idx', 'nightowl_outgoing_requests', 'created_at, user_id'],
        ['nightowl_cache_events_created_at_user_id_idx', 'nightowl_cache_events', 'created_at, user_id'],
        ['nightowl_logs_created_at_user_id_idx', 'nightowl_logs', 'created_at, user_id'],
        ['nightowl_commands_group_hash_created_at_idx', 'nightowl_commands', 'group_hash, created_at'],
        ['nightowl_scheduled_tasks_group_hash_created_at_idx', 'nightowl_scheduled_tasks', 'group_hash, created_at'],
    ];

    public function up(): void
    {
        $schema = Schema::connection($this->connection);

        foreach (self::INDEXES as [$name, $table, $columns]) {
            if (! $schema->hasTable($table)) {
                continue;
            }

            DB::connection($this->connection)->statement(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS {$name} ON {$table} ({$columns})"
            );
        }
    }

    public function down(): void
    {
        foreach (self::INDEXES as [$name]) {
            DB::connection($this->connection)->statement("DROP INDEX CONCURRENTLY IF EXISTS {$name}");
        }
    }
};
