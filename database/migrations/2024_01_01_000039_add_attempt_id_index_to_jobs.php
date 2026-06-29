<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Job children (exceptions/queries/logs emitted inside a job) are keyed by the
     * attempt's `attempt_id`, not the job's `trace_id`. The dashboard's parent-label
     * and group-hash enrichment (ParentLabelResolver, HasTimeRangeFilter::
     * enrichWithGroupHash, ExceptionController) therefore query
     * `nightowl_jobs WHERE attempt_id IN (...)`. Only `trace_id` and `execution_id`
     * were indexed on nightowl_jobs, so every cache-miss on a detail-list page that
     * contains a job-source row ran that lookup as a sequential scan.
     *
     * Matches the blocking-index approach already used for these tenant tables in
     * 000029_add_created_at_indexes (CREATE INDEX takes a brief write lock; run
     * during a deploy window like the rest of nightowl:migrate).
     */
    public function up(): void
    {
        if (! Schema::connection($this->connection)->hasTable('nightowl_jobs')) {
            return;
        }

        // IF NOT EXISTS so the migration is safe under baseline adoption / re-run.
        Schema::connection($this->connection)->getConnection()->statement(
            'CREATE INDEX IF NOT EXISTS nightowl_jobs_attempt_id_index ON nightowl_jobs (attempt_id)'
        );
    }

    public function down(): void
    {
        if (! Schema::connection($this->connection)->hasTable('nightowl_jobs')) {
            return;
        }

        Schema::connection($this->connection)->getConnection()->statement(
            'DROP INDEX IF EXISTS nightowl_jobs_attempt_id_index'
        );
    }
};
