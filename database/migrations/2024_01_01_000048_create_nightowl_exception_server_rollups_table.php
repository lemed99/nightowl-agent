<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Distinct-server-per-fingerprint rollup: one row per
     * (fingerprint, server, 60s bucket, environment). Lets the exception detail
     * "servers affected" stat read COUNT(DISTINCT server) off a compact per-minute
     * summary instead of an unbounded COUNT(DISTINCT server) scan of every raw
     * nightowl_exceptions row for a fingerprint — the scan that trips the tenant
     * statement timeout on high-volume fingerprints. Mirrors the distinct-connection
     * (queries) and distinct-channel (notifications) rollups. Count-only: server
     * presence is the signal; call_count rides along. PK collapses on the ''
     * fingerprint/server/environment sentinels.
     */
    public function up(): void
    {
        if (Schema::connection($this->connection)->hasTable('nightowl_exception_server_rollups')) {
            return;
        }

        Schema::connection($this->connection)->create('nightowl_exception_server_rollups', function (Blueprint $table): void {
            $table->string('fingerprint')->default('');
            $table->string('server')->default('');
            $table->timestamp('bucket_start');
            $table->string('environment')->default('');

            $table->bigInteger('call_count')->default(0);

            $table->primary(['fingerprint', 'server', 'bucket_start', 'environment'], 'nightowl_exception_server_rollups_pk');
            $table->index('bucket_start', 'nightowl_exception_server_rollups_bucket_idx');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_exception_server_rollups');
    }
};
