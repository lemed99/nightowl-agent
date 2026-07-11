<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Per-minute pre-aggregated summaries of nightowl_scheduled_tasks, keyed by
     * group_hash (schedule identifier). Additive call_count + failed/processed/
     * skipped counters (split on status, matching ScheduledTaskController's raw
     * bands — 'processed' folds in the legacy 'success' alias) + duration totals +
     * √2 histogram bins, with the command + cron expression kept as first-seen
     * representatives.
     *
     * Singular stem in the rollup name (nightowl_scheduled_task_rollups) vs the
     * plural source (nightowl_scheduled_tasks) — matches the convention set by the
     * other rollups.
     *
     * BIN_COUNT must equal QueryHistogram::binCount() in both repos; symlink-shared,
     * so the count is inlined and guarded by QueryHistogramTest.
     */
    private const BIN_COUNT = 39;

    public function up(): void
    {
        if (Schema::connection($this->connection)->hasTable('nightowl_scheduled_task_rollups')) {
            return;
        }

        Schema::connection($this->connection)->create('nightowl_scheduled_task_rollups', function (Blueprint $table): void {
            $table->string('group_hash')->default('');
            $table->timestamp('bucket_start');
            $table->string('environment')->default('');

            $table->bigInteger('call_count')->default(0);
            $table->bigInteger('failed_count')->default(0);
            $table->bigInteger('processed_count')->default(0);
            $table->bigInteger('skipped_count')->default(0);

            $table->bigInteger('total_duration')->default(0);
            $table->bigInteger('min_duration')->nullable();
            $table->bigInteger('max_duration')->nullable();

            for ($i = 0; $i < self::BIN_COUNT; $i++) {
                $table->bigInteger(sprintf('hist_%02d', $i))->default(0);
            }

            $table->text('command')->nullable();
            $table->text('expression')->nullable();
            // A schedule's cadence is fixed per group_hash, so the representative's
            // first-seen/MIN semantics equal the raw list's MAX(repeat_seconds).
            $table->bigInteger('repeat_seconds')->nullable();

            $table->primary(['group_hash', 'bucket_start', 'environment'], 'nightowl_scheduled_task_rollups_pk');
            $table->index('bucket_start', 'nightowl_scheduled_task_rollups_bucket_idx');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_scheduled_task_rollups');
    }
};
