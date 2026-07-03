<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Per-minute per-user count of nightowl_exceptions — enriches the users list's
     * "exceptions" column (exceptions per user). Exceptions have no rollup at all
     * today; this is the first. call_count (the implicit counter) is the exception
     * count — no status bands, no duration/histogram. Keyed by user_id, mirroring
     * nightowl_user_rollups. PK collapses on the '' environment/user sentinels.
     */
    public function up(): void
    {
        if (Schema::connection($this->connection)->hasTable('nightowl_user_exception_rollups')) {
            return;
        }

        Schema::connection($this->connection)->create('nightowl_user_exception_rollups', function (Blueprint $table): void {
            $table->string('user_id')->default('');
            $table->timestamp('bucket_start');
            $table->string('environment')->default('');

            $table->bigInteger('call_count')->default(0);

            $table->primary(['user_id', 'bucket_start', 'environment'], 'nightowl_user_exception_rollups_pk');
            $table->index('bucket_start', 'nightowl_user_exception_rollups_bucket_idx');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_user_exception_rollups');
    }
};
