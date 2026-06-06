<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Histogram bins for windowed percentiles (B2). Percentiles aren't additive
     * across rollup buckets, so per-bucket p95s can't be merged; a fixed
     * log-scale histogram can. The agent increments one bin per query at drain
     * time; merging buckets is element-wise column addition; the API estimates
     * p50/p95/p99 by walking the cumulative bin counts.
     *
     * Named bigint columns (not bigint[] — Postgres has no native element-wise
     * array add — and not jsonb — that would force a PHP read-modify-write and
     * reintroduce the concurrency problem the SQL-side upsert avoids).
     *
     * BIN_COUNT must equal QueryHistogram::binCount() in BOTH repos
     * (nightowl-agent NightOwl\Support, nightowl-api App\Support). This file is
     * symlink-shared between the repos, so it cannot reference either class —
     * the count is inlined and guarded by QueryHistogramTest on each side.
     */
    private const BIN_COUNT = 39;

    public function up(): void
    {
        if (! Schema::connection($this->connection)->hasTable('nightowl_query_rollups')) {
            return;
        }

        $columns = $this->columns();

        Schema::connection($this->connection)->table('nightowl_query_rollups', function (Blueprint $table) use ($columns): void {
            foreach ($columns as $column) {
                if (! Schema::connection($this->connection)->hasColumn('nightowl_query_rollups', $column)) {
                    $table->bigInteger($column)->default(0);
                }
            }
        });
    }

    public function down(): void
    {
        if (! Schema::connection($this->connection)->hasTable('nightowl_query_rollups')) {
            return;
        }

        Schema::connection($this->connection)->table('nightowl_query_rollups', function (Blueprint $table): void {
            $table->dropColumn($this->columns());
        });
    }

    /**
     * @return list<string>
     */
    private function columns(): array
    {
        $columns = [];
        for ($i = 0; $i < self::BIN_COUNT; $i++) {
            $columns[] = sprintf('hist_%02d', $i);
        }

        return $columns;
    }
};
