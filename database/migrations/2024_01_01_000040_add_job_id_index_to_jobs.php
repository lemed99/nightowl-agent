<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * The job-attempt detail page (JobController::attempt) and every detail
     * endpoint that resolves an execution's sibling/child jobs via
     * ExecutionDetailFetcher::fetch (commands, scheduled tasks, requests) walk the
     * job family by filtering nightowl_jobs on `job_id`:
     *   - the dispatch pair lookup `WHERE job_id = ? AND status IS NULL`
     *   - the ancestor walk + descendant BFS `WHERE job_id IN (...)`
     * Only `trace_id`, `execution_id`, `attempt_id`, `status`, `job_class`,
     * `timestamp` and `created_at` were indexed on nightowl_jobs — `job_id` never
     * was — so each of those ~10-20 lookups ran as a sequential scan. Every scan
     * sits under the 20s statement_timeout individually, but their sum overran
     * PHP's 30s max_execution_time: an uncatchable FatalError mid-query
     * (Connection.php:425) that captured no query/stack/context.
     *
     * Matches the blocking-index approach already used for these tenant tables in
     * 000039_add_attempt_id_index_to_jobs (CREATE INDEX takes a brief write lock;
     * run during a deploy window like the rest of nightowl:migrate).
     */
    public function up(): void
    {
        if (! Schema::connection($this->connection)->hasTable('nightowl_jobs')) {
            return;
        }

        // IF NOT EXISTS so the migration is safe under baseline adoption / re-run.
        Schema::connection($this->connection)->getConnection()->statement(
            'CREATE INDEX IF NOT EXISTS nightowl_jobs_job_id_index ON nightowl_jobs (job_id)'
        );
    }

    public function down(): void
    {
        if (! Schema::connection($this->connection)->hasTable('nightowl_jobs')) {
            return;
        }

        Schema::connection($this->connection)->getConnection()->statement(
            'DROP INDEX IF EXISTS nightowl_jobs_job_id_index'
        );
    }
};
