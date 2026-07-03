<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Per-minute pre-aggregated summaries of nightowl_notifications, keyed by
     * (group_hash, channel) — the same two-column grouping nightowl_query_rollups
     * uses for (group_hash, connection). Keying on channel lets the list rebuild
     * its DISTINCT channel set (STRING_AGG over the rollup rows) additively.
     * call_count + queued/failed counters + duration totals + √2 histogram (the
     * list sorts by p95), with the notification class as a first-seen representative.
     *
     * BIN_COUNT must equal QueryHistogram::binCount() in both repos; symlink-shared,
     * so the count is inlined and guarded by QueryHistogramTest.
     */
    private const BIN_COUNT = 39;

    public function up(): void
    {
        if (Schema::connection($this->connection)->hasTable('nightowl_notification_rollups')) {
            return;
        }

        Schema::connection($this->connection)->create('nightowl_notification_rollups', function (Blueprint $table): void {
            $table->string('group_hash')->default('');
            $table->string('channel')->default('');
            $table->timestamp('bucket_start');
            $table->string('environment')->default('');

            $table->bigInteger('call_count')->default(0);
            $table->bigInteger('queued_count')->default(0);
            $table->bigInteger('failed_count')->default(0);

            $table->bigInteger('total_duration')->default(0);
            $table->bigInteger('min_duration')->nullable();
            $table->bigInteger('max_duration')->nullable();

            for ($i = 0; $i < self::BIN_COUNT; $i++) {
                $table->bigInteger(sprintf('hist_%02d', $i))->default(0);
            }

            $table->text('notification')->nullable();

            $table->primary(['group_hash', 'channel', 'bucket_start', 'environment'], 'nightowl_notification_rollups_pk');
            $table->index('bucket_start', 'nightowl_notification_rollups_bucket_idx');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_notification_rollups');
    }
};
