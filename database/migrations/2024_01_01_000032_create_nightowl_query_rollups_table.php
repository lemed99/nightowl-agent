<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Pre-aggregated per-minute summaries of nightowl_queries — the
     * highest-volume telemetry table. The list/overview/calls endpoints
     * aggregate the whole time window at read time, which on a busy tenant
     * scans tens of millions of raw rows and can blow past PHP's execution
     * limit (the production incident this table cures). The agent maintains
     * one row per (group_hash, minute bucket, environment, connection) at
     * drain time, so reads aggregate a few thousand summary rows instead.
     *
     * environment / connection are NOT NULL DEFAULT '' (sentinel-on-write):
     * Postgres treats NULLs as distinct in a unique index and NULLS NOT
     * DISTINCT is PG 15+, which we can't assume on BYO customer databases.
     * Coalescing to '' before the upsert makes the composite PK collapse
     * correctly on every supported version.
     */
    public function up(): void
    {
        if (Schema::connection($this->connection)->hasTable('nightowl_query_rollups')) {
            return;
        }

        Schema::connection($this->connection)->create('nightowl_query_rollups', function (Blueprint $table) {
            $table->string('group_hash')->default('');
            $table->timestamp('bucket_start');           // created_at truncated to 60s
            $table->string('environment')->default('');  // '' sentinel, not nullable
            $table->string('connection')->default('');   // '' sentinel, not nullable

            $table->bigInteger('call_count')->default(0);
            $table->bigInteger('total_duration')->default(0); // µs; avg = total / count
            $table->bigInteger('min_duration')->nullable();
            $table->bigInteger('max_duration')->nullable();
            $table->text('sql_query')->nullable();            // representative query for the hash

            $table->primary(
                ['group_hash', 'bucket_start', 'environment', 'connection'],
                'nightowl_query_rollups_pk'
            );
            $table->index('bucket_start', 'nightowl_query_rollups_bucket_idx');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_query_rollups');
    }
};
