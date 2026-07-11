<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Per-minute pre-aggregated summaries of nightowl_commands, keyed by group_hash
     * (command class). Additive call_count + successful/unsuccessful counters (split
     * on exit_code, matching CommandController's raw bands) + duration totals + √2
     * histogram bins (the commands page shows avg + p95), with the command name kept
     * as a first-seen representative.
     *
     * BIN_COUNT must equal QueryHistogram::binCount() in both repos; symlink-shared,
     * so the count is inlined and guarded by QueryHistogramTest.
     */
    private const BIN_COUNT = 39;

    public function up(): void
    {
        if (Schema::connection($this->connection)->hasTable('nightowl_command_rollups')) {
            return;
        }

        Schema::connection($this->connection)->create('nightowl_command_rollups', function (Blueprint $table): void {
            $table->string('group_hash')->default('');
            $table->timestamp('bucket_start');
            $table->string('environment')->default('');

            $table->bigInteger('call_count')->default(0);
            $table->bigInteger('successful_count')->default(0);
            $table->bigInteger('unsuccessful_count')->default(0);

            $table->bigInteger('total_duration')->default(0);
            $table->bigInteger('min_duration')->nullable();
            $table->bigInteger('max_duration')->nullable();

            for ($i = 0; $i < self::BIN_COUNT; $i++) {
                $table->bigInteger(sprintf('hist_%02d', $i))->default(0);
            }

            $table->text('command')->nullable();

            $table->primary(['group_hash', 'bucket_start', 'environment'], 'nightowl_command_rollups_pk');
            $table->index('bucket_start', 'nightowl_command_rollups_bucket_idx');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_command_rollups');
    }
};
