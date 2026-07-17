<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Drop raw-table indexes with no reader. Every COPY row pays index
     * maintenance on 5-7 btrees per table — at high volume that tax, not the
     * heap write, dominates drain cost on the customer's PostgreSQL. A full
     * reader audit (2026-07-17) across nightowl-api and the agent classed
     * these 22 as dead:
     *
     * - The string `timestamp` indexes (all 10 tables that have one): no query
     *   anywhere filters on the column; reads use created_at.
     * - idx_requests_method_url: nothing filters/joins requests on method/url;
     *   list search is a leading-% LIKE (unindexable).
     * - trace_id on cache_events/mail/notifications/outgoing_requests: the
     *   trace waterfall reaches those four tables via execution_id only.
     * - logs.channel, jobs.job_class: no reader / leading-% LIKE only.
     * - Single-column group_hash (requests, queries) and fingerprint
     *   (exceptions): strict prefixes of the 000044 composites — every reader
     *   is served by the composite's prefix.
     * - Single-column duration (requests, queries): the only orderer is the
     *   created_at-bounded short-range p95 branch; wide-range percentiles read
     *   rollup histograms. The planner never picks these.
     *
     * Deliberately KEPT: status_code/status/level/class/event_type singles
     * (hard-required by DataManagementController's loose index scans), every
     * created_at (prune + DESC pagination), all 000044 composites, remaining
     * trace_id/execution_id/attempt_id/job_id, and the weak trio
     * (exceptions.environment, outgoing.status_code, scheduled_tasks.status)
     * pending pg_stat evidence.
     *
     * CONCURRENTLY (hence $withinTransaction = false): drops take a brief
     * ShareUpdateExclusive instead of blocking the live drain.
     */
    public $withinTransaction = false;

    /** @var array<string, array{0: string, 1: string}> index name => [table, column list for down()] */
    private const DEAD_INDEXES = [
        'nightowl_requests_timestamp_index' => ['nightowl_requests', 'timestamp'],
        'nightowl_queries_timestamp_index' => ['nightowl_queries', 'timestamp'],
        'nightowl_exceptions_timestamp_index' => ['nightowl_exceptions', 'timestamp'],
        'nightowl_commands_timestamp_index' => ['nightowl_commands', 'timestamp'],
        'nightowl_jobs_timestamp_index' => ['nightowl_jobs', 'timestamp'],
        'nightowl_cache_events_timestamp_index' => ['nightowl_cache_events', 'timestamp'],
        'nightowl_mail_timestamp_index' => ['nightowl_mail', 'timestamp'],
        'nightowl_notifications_timestamp_index' => ['nightowl_notifications', 'timestamp'],
        'nightowl_outgoing_requests_timestamp_index' => ['nightowl_outgoing_requests', 'timestamp'],
        'nightowl_scheduled_tasks_timestamp_index' => ['nightowl_scheduled_tasks', 'timestamp'],
        'idx_requests_method_url' => ['nightowl_requests', 'method, url'],
        'nightowl_cache_events_trace_id_index' => ['nightowl_cache_events', 'trace_id'],
        'nightowl_mail_trace_id_index' => ['nightowl_mail', 'trace_id'],
        'nightowl_notifications_trace_id_index' => ['nightowl_notifications', 'trace_id'],
        'nightowl_outgoing_requests_trace_id_index' => ['nightowl_outgoing_requests', 'trace_id'],
        'nightowl_logs_channel_index' => ['nightowl_logs', 'channel'],
        'nightowl_jobs_job_class_index' => ['nightowl_jobs', 'job_class'],
        'nightowl_requests_group_hash_index' => ['nightowl_requests', 'group_hash'],
        'nightowl_queries_group_hash_index' => ['nightowl_queries', 'group_hash'],
        'nightowl_exceptions_fingerprint_index' => ['nightowl_exceptions', 'fingerprint'],
        'nightowl_requests_duration_index' => ['nightowl_requests', 'duration'],
        'nightowl_queries_duration_index' => ['nightowl_queries', 'duration'],
    ];

    public function up(): void
    {
        foreach (array_keys(self::DEAD_INDEXES) as $name) {
            DB::connection($this->connection)->statement("DROP INDEX CONCURRENTLY IF EXISTS {$name}");
        }
    }

    public function down(): void
    {
        $schema = Schema::connection($this->connection);

        foreach (self::DEAD_INDEXES as $name => [$table, $columns]) {
            if (! $schema->hasTable($table)) {
                continue;
            }

            DB::connection($this->connection)->statement(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS {$name} ON {$table} ({$columns})"
            );
        }
    }
};
