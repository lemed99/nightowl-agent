<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Per-minute pre-aggregated summaries of nightowl_requests — the highest-
     * volume table after queries (one row per HTTP request). Mirrors
     * nightowl_query_rollups: additive call_count + status-band counters +
     * duration totals + √2 histogram bins, with route_methods/route_path kept as
     * first-seen representatives. PK collapses on the '' environment sentinel.
     *
     * BIN_COUNT must equal QueryHistogram::binCount() in both repos. This file
     * is symlink-shared, so it can't reference the class — count is inlined and
     * guarded by QueryHistogramTest.
     */
    private const BIN_COUNT = 39;

    public function up(): void
    {
        if (Schema::connection($this->connection)->hasTable('nightowl_request_rollups')) {
            return;
        }

        Schema::connection($this->connection)->create('nightowl_request_rollups', function (Blueprint $table): void {
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

            $table->text('route_methods')->nullable();
            $table->text('route_path')->nullable();

            $table->primary(['group_hash', 'bucket_start', 'environment'], 'nightowl_request_rollups_pk');
            $table->index('bucket_start', 'nightowl_request_rollups_bucket_idx');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_request_rollups');
    }
};
