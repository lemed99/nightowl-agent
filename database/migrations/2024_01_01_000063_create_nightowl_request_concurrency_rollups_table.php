<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Per-minute request-concurrency summaries: the (delta_sum, max_prefix)
     * decomposition of the in-flight running count, one row per minute bucket.
     *
     *  - delta_sum:  net change in in-flight requests over the bucket
     *                (starts minus ends). Exact, additive across batches.
     *  - max_prefix: the highest the running count rose WITHIN the bucket,
     *                relative to the count at bucket entry. Never negative
     *                (an empty prefix is 0).
     *
     * A window's peak is then a linear walk: carry += delta_sum per bucket,
     * peak = max(carry_before_bucket + max_prefix). This replaces the API's
     * 2x-scan + full-window SUM() OVER (ORDER BY ts) — the query that forced
     * peak-concurrency reads to clamp at 6h and still tripped the 20s tenant
     * statement_timeout (SQLSTATE 57014, 2026-07-20).
     *
     * Two deliberate shape decisions:
     *
     *  - NO environment column. The batch combine (delta = d1+d2, max_prefix =
     *    max(p1, d1+p2)) is only valid for SEQUENTIAL appends to one stream.
     *    Per-environment rows would be parallel streams, and no function of
     *    their (delta, max_prefix) pairs reconstructs the combined peak. The
     *    unfiltered all-environments view is both the default and the
     *    expensive one, so this table stores exactly that stream; an
     *    environment-filtered read falls back to the raw path.
     *
     *  - NOT in 000054's tier tables. The combine is an ordered fold, not the
     *    straight re-aggregation RollupTiers::collapse performs, and a 30d
     *    window is only ~43k minute rows — a trivial walk needing no tiers.
     *
     * Not a RollupSpec: the ON CONFLICT combine isn't expressible in
     * rollupSql()'s additive/LEAST/GREATEST vocabulary. Written by the bespoke
     * RecordWriter::writeConcurrencyRollup, backfilled by the bespoke branch in
     * nightowl:backfill-rollups, pruned with minute-rollup retention.
     */
    public function up(): void
    {
        if (Schema::connection($this->connection)->hasTable('nightowl_request_concurrency_rollups')) {
            return;
        }

        Schema::connection($this->connection)->create('nightowl_request_concurrency_rollups', function (Blueprint $table): void {
            $table->timestamp('bucket_start');
            $table->bigInteger('delta_sum')->default(0);
            $table->bigInteger('max_prefix')->default(0);

            $table->primary(['bucket_start'], 'nightowl_request_concurrency_rollups_pk');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_request_concurrency_rollups');
    }
};
