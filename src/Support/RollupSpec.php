<?php

namespace NightOwl\Support;

/**
 * Declarative description of one telemetry type's rollup, driving both the
 * live-drain upsert (PHP predicates) and the backfill INSERT…SELECT (SQL
 * conditions). One spec per high-volume table; the generic
 * RecordWriter::writeRollup() and BackfillRollupsCommand consume it so adding a
 * type is a spec entry plus a migration, not a copy of the query-rollup code.
 *
 * Each column definition carries two forms because the two write paths see the
 * data differently:
 *   - 'php'  — closure over the in-flight record array (live drain).
 *   - 'sql'  — expression over the raw source row (backfill aggregation).
 *
 * The PK is groupColumns + bucket_start + environment. Counters and (when
 * hasDuration) duration totals/min/max + histogram bins are additive across
 * buckets; representatives are first-seen (COALESCE).
 */
final class RollupSpec
{
    /**
     * @param  string  $table  rollup table (e.g. nightowl_request_rollups)
     * @param  string  $source  raw source table (e.g. nightowl_requests)
     * @param  array<string, array{php: callable, sql: string}>  $groupColumns  PK cols besides bucket_start/environment → value extractor + raw expr (coalesced to '')
     * @param  array<string, array{php: callable, sql: string}>  $counters  additive counter cols → predicate + SQL boolean condition (call_count is implicit)
     * @param  array<string, array{php: callable, sql: string}>  $representatives  first-seen cols → value extractor + raw column for MIN()
     * @param  bool  $hasDuration  track total/min/max duration (powers avg)
     * @param  bool  $hasHistogram  track the √2 duration histogram (powers p50/p95/p99). Kept
     *                              off for high-cardinality groupings (cache by key) where 39
     *                              extra columns would bloat the table and no percentile is shown.
     * @param  string  $durationField  record field holding the duration (µs)
     */
    public function __construct(
        public string $table,
        public string $source,
        public array $groupColumns,
        public array $counters,
        public array $representatives,
        public bool $hasDuration,
        public bool $hasHistogram,
        public string $durationField = 'duration',
        /**
         * Optional {php: closure, sql: string} gating which rows contribute to the
         * duration total/min/max + histogram. Jobs use it to count ATTEMPT rows only
         * — a queued-job (dispatch) row carries enqueue overhead, not execution time,
         * so folding it in drags min ~280x low and skews p95. null = all rows.
         *
         * @var array{php: callable, sql: string}|null
         */
        public ?array $durationPredicate = null,
    ) {}

    /** @return list<string> counter column names */
    public function counterColumns(): array
    {
        return array_keys($this->counters);
    }

    /** @return list<string> representative column names */
    public function representativeColumns(): array
    {
        return array_keys($this->representatives);
    }

    /** @return list<string> group (PK) column names, excluding bucket_start/environment */
    public function groupColumnNames(): array
    {
        return array_keys($this->groupColumns);
    }

    /**
     * Build the backfill INSERT…SELECT pieces from the raw source table: the
     * destination column list, the matching SELECT expressions, and the number
     * of leading positional GROUP BY columns (group cols + bucket + environment).
     *
     * @param  array<string, string>  $histCase  hist column => SUM(CASE …) from QueryHistogram::caseSql
     * @return array{columns: list<string>, selects: list<string>, groupByCount: int}
     */
    public function backfillSql(array $histCase): array
    {
        $groupCols = $this->groupColumnNames();
        $columns = [...$groupCols, 'bucket_start', 'environment', 'call_count', ...$this->counterColumns()];

        $selects = [];
        foreach ($this->groupColumns as $def) {
            $selects[] = $def['sql'];
        }
        $selects[] = "date_trunc('minute', created_at)";
        $selects[] = "COALESCE(environment, '')";
        $selects[] = 'COUNT(*)';
        foreach ($this->counters as $def) {
            $selects[] = "SUM(CASE WHEN {$def['sql']} THEN 1 ELSE 0 END)";
        }

        // Restrict duration + histogram to the predicate's rows (e.g. job attempts),
        // matching the live drain — a Postgres FILTER on each aggregate.
        $filter = $this->durationPredicate ? ' FILTER (WHERE '.$this->durationPredicate['sql'].')' : '';

        if ($this->hasDuration) {
            $columns = [...$columns, 'total_duration', 'min_duration', 'max_duration'];
            $selects[] = 'COALESCE(SUM('.$this->durationField.')'.$filter.', 0)';
            $selects[] = 'MIN('.$this->durationField.')'.$filter;
            $selects[] = 'MAX('.$this->durationField.')'.$filter;
        }

        if ($this->hasHistogram) {
            $columns = [...$columns, ...array_keys($histCase)];
            foreach ($histCase as $expr) {
                // COALESCE to 0: a duration-predicate FILTER matching zero rows
                // (e.g. a bucket with only a queued dispatch and no attempt) makes
                // SUM(...) FILTER (...) return NULL, which violates the hist_NN
                // NOT NULL constraint. The live drain writes 0 for such buckets, so
                // the backfill must too. total_duration above is already coalesced.
                $selects[] = 'COALESCE('.$expr.$filter.', 0)';
            }
        }

        foreach ($this->representatives as $col => $def) {
            $columns[] = $col;
            $selects[] = $def['sql'];
        }

        return ['columns' => $columns, 'selects' => $selects, 'groupByCount' => count($groupCols) + 2];
    }
}
