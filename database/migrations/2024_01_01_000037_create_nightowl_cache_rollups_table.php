<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Per-minute pre-aggregated summaries of nightowl_cache_events, grouped by
     * (key, store) — the only rollup keyed on something other than group_hash.
     * No histogram: the cache UI shows no duration percentile, so duration is
     * just total/min/max (powering the list's avg column). key/store are
     * varchar(255) to match the source columns.
     *
     * Note: `key` is high-cardinality, so this table can approach the raw row
     * count for apps with unbounded cache keys. It still shrinks the
     * overview/charts (which don't group by key) and bounded-key list views.
     */
    public function up(): void
    {
        if (Schema::connection($this->connection)->hasTable('nightowl_cache_rollups')) {
            return;
        }

        Schema::connection($this->connection)->create('nightowl_cache_rollups', function (Blueprint $table): void {
            $table->string('key')->default('');
            $table->string('store')->default('');
            $table->timestamp('bucket_start');
            $table->string('environment')->default('');

            $table->bigInteger('call_count')->default(0);
            $table->bigInteger('hits')->default(0);
            $table->bigInteger('misses')->default(0);
            $table->bigInteger('writes')->default(0);
            $table->bigInteger('deletes')->default(0);
            $table->bigInteger('fails')->default(0);
            $table->bigInteger('delete_failures')->default(0);
            $table->bigInteger('write_failures')->default(0);

            $table->bigInteger('total_duration')->default(0);
            $table->bigInteger('min_duration')->nullable();
            $table->bigInteger('max_duration')->nullable();

            $table->primary(['key', 'store', 'bucket_start', 'environment'], 'nightowl_cache_rollups_pk');
            $table->index('bucket_start', 'nightowl_cache_rollups_bucket_idx');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_cache_rollups');
    }
};
