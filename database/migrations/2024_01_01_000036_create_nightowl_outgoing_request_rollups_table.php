<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Per-minute pre-aggregated summaries of nightowl_outgoing_requests. Same
     * shape as request rollups (status-band counters + duration histogram), with
     * the extracted host (scheme://host) as the first-seen representative.
     *
     * BIN_COUNT must equal QueryHistogram::binCount() in both repos (symlink-
     * shared file, guarded by QueryHistogramTest).
     */
    private const BIN_COUNT = 39;

    public function up(): void
    {
        if (Schema::connection($this->connection)->hasTable('nightowl_outgoing_request_rollups')) {
            return;
        }

        Schema::connection($this->connection)->create('nightowl_outgoing_request_rollups', function (Blueprint $table): void {
            $table->string('group_hash')->default('');
            $table->timestamp('bucket_start');
            $table->string('environment')->default('');

            $table->bigInteger('call_count')->default(0);
            $table->bigInteger('success_count')->default(0);
            $table->bigInteger('client_error_count')->default(0);
            $table->bigInteger('server_error_count')->default(0);

            $table->bigInteger('total_duration')->default(0);
            $table->bigInteger('min_duration')->nullable();
            $table->bigInteger('max_duration')->nullable();

            for ($i = 0; $i < self::BIN_COUNT; $i++) {
                $table->bigInteger(sprintf('hist_%02d', $i))->default(0);
            }

            $table->text('host')->nullable();

            $table->primary(['group_hash', 'bucket_start', 'environment'], 'nightowl_outgoing_request_rollups_pk');
            $table->index('bucket_start', 'nightowl_outgoing_request_rollups_bucket_idx');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_outgoing_request_rollups');
    }
};
