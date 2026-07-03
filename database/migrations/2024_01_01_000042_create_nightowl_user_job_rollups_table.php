<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Per-minute per-user summary of nightowl_jobs — enriches the users list's
     * "queued jobs" column (which counts job ATTEMPTS, i.e. attempt_id IS NOT NULL,
     * per user). Keyed by user_id like nightowl_user_rollups; the route-keyed
     * nightowl_job_rollups has no user dimension. attempts_count mirrors the
     * attempts_count counter in nightowl_job_rollups. Count-only, no
     * duration/histogram — the list shows a count, not per-user job latency.
     */
    public function up(): void
    {
        if (Schema::connection($this->connection)->hasTable('nightowl_user_job_rollups')) {
            return;
        }

        Schema::connection($this->connection)->create('nightowl_user_job_rollups', function (Blueprint $table): void {
            $table->string('user_id')->default('');
            $table->timestamp('bucket_start');
            $table->string('environment')->default('');

            $table->bigInteger('call_count')->default(0);
            $table->bigInteger('attempts_count')->default(0);

            $table->primary(['user_id', 'bucket_start', 'environment'], 'nightowl_user_job_rollups_pk');
            $table->index('bucket_start', 'nightowl_user_job_rollups_bucket_idx');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_user_job_rollups');
    }
};
