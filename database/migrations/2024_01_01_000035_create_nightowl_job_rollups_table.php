<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Per-minute pre-aggregated summaries of nightowl_jobs. attempts_count is
     * the "total" the list shows (a queued job has no attempt_id); duration
     * totals + histogram cover attempts only (queued rows have null duration, so
     * they never enter the histogram). job_class/queue are first-seen reps.
     *
     * BIN_COUNT must equal QueryHistogram::binCount() in both repos (symlink-
     * shared file, guarded by QueryHistogramTest).
     */
    private const BIN_COUNT = 39;

    public function up(): void
    {
        if (Schema::connection($this->connection)->hasTable('nightowl_job_rollups')) {
            return;
        }

        Schema::connection($this->connection)->create('nightowl_job_rollups', function (Blueprint $table): void {
            $table->string('group_hash')->default('');
            $table->timestamp('bucket_start');
            $table->string('environment')->default('');

            $table->bigInteger('call_count')->default(0);
            $table->bigInteger('attempts_count')->default(0);
            $table->bigInteger('queued_count')->default(0);
            $table->bigInteger('processed_count')->default(0);
            $table->bigInteger('released_count')->default(0);
            $table->bigInteger('failed_count')->default(0);

            $table->bigInteger('total_duration')->default(0);
            $table->bigInteger('min_duration')->nullable();
            $table->bigInteger('max_duration')->nullable();

            for ($i = 0; $i < self::BIN_COUNT; $i++) {
                $table->bigInteger(sprintf('hist_%02d', $i))->default(0);
            }

            $table->text('job_class')->nullable();
            $table->text('queue')->nullable();

            $table->primary(['group_hash', 'bucket_start', 'environment'], 'nightowl_job_rollups_pk');
            $table->index('bucket_start', 'nightowl_job_rollups_bucket_idx');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_job_rollups');
    }
};
