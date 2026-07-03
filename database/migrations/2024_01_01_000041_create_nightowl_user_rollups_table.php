<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Per-minute pre-aggregated summaries of nightowl_requests keyed by USER
     * instead of route group_hash — powers the users list, which groups requests
     * by user_id and cannot be served from nightowl_request_rollups (those
     * collapse the user dimension away). Additive call_count (= requests) plus the
     * same status-band counters as nightowl_request_rollups. No duration/histogram:
     * the users list shows counts + last_seen, no per-user percentile.
     *
     * last_seen at read time is MAX(bucket_start) — minute-granular, which is fine
     * for a "last seen" column. PK collapses on the '' environment/user sentinels.
     */
    public function up(): void
    {
        if (Schema::connection($this->connection)->hasTable('nightowl_user_rollups')) {
            return;
        }

        Schema::connection($this->connection)->create('nightowl_user_rollups', function (Blueprint $table): void {
            $table->string('user_id')->default('');
            $table->timestamp('bucket_start');
            $table->string('environment')->default('');

            $table->bigInteger('call_count')->default(0);
            $table->bigInteger('success_count')->default(0);
            $table->bigInteger('client_error_count')->default(0);
            $table->bigInteger('server_error_count')->default(0);

            $table->primary(['user_id', 'bucket_start', 'environment'], 'nightowl_user_rollups_pk');
            $table->index('bucket_start', 'nightowl_user_rollups_bucket_idx');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_user_rollups');
    }
};
