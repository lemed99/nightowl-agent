<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Per-minute pre-aggregated summaries of nightowl_mail, keyed by group_hash
     * (mailable class). Additive call_count + queued/failed counters + duration
     * totals + √2 histogram bins (the mail list sorts by p95 and shows avg), with
     * the mailable class kept as a first-seen representative.
     *
     * BIN_COUNT must equal QueryHistogram::binCount() in both repos; symlink-shared,
     * so the count is inlined and guarded by QueryHistogramTest.
     */
    private const BIN_COUNT = 39;

    public function up(): void
    {
        if (Schema::connection($this->connection)->hasTable('nightowl_mail_rollups')) {
            return;
        }

        Schema::connection($this->connection)->create('nightowl_mail_rollups', function (Blueprint $table): void {
            $table->string('group_hash')->default('');
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

            $table->text('mailable')->nullable();

            $table->primary(['group_hash', 'bucket_start', 'environment'], 'nightowl_mail_rollups_pk');
            $table->index('bucket_start', 'nightowl_mail_rollups_bucket_idx');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_mail_rollups');
    }
};
