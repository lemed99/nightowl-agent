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
        /**
         * Track duration_count — the number of rows folded into total_duration
         * (i.e. duration-bearing rows, after the durationPredicate). Types whose
         * avg denominator can't come from call_count or an existing counter
         * (mail/notifications/commands/scheduled tasks) need it so the API keeps
         * an exact denominator after the v1 hist_NN bins — whose sum used to
         * derive it — are dropped. The column is optional on the table: both
         * write paths probe for it and omit it when absent (RecordWriter::
         * durationCountEnabled, BackfillRollupsCommand) so an un-migrated
         * tenant keeps its rollups; nightowl:drop-v1-histograms refuses until
         * it exists.
         */
        public bool $hasDurationCount = false,
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
     * The optional columns are the caller's live probe of the destination
     * table, not the spec flags alone: hist_NN goes away with
     * nightowl:drop-v1-histograms and duration_count arrives with migration
     * 000061, so a spec that declares them can still meet a table that lacks
     * them. Both are nullable or NOT NULL DEFAULT 0 — omitting one is safe.
     *
     * @param  array<string, string>  $histCase  hist column => SUM(CASE …) from QueryHistogram::caseSql; empty when the table has no bins
     * @param  bool  $withDurationCount  the table carries duration_count
     * @return array{columns: list<string>, selects: list<string>, groupByCount: int}
     */
    public function backfillSql(array $histCase, bool $withDurationCount = false): array
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

        if ($this->hasDurationCount && $withDurationCount) {
            // COUNT(col) counts non-null values, and the FILTER applies the same
            // duration predicate as the total above — so this matches the live
            // drain's fold condition exactly.
            $columns[] = 'duration_count';
            $selects[] = 'COUNT('.$this->durationField.')'.$filter;
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

    /**
     * Build the tier backfill INSERT…SELECT pieces — a rollup-from-rollup
     * re-aggregation (minute→hour, hour→day): counters and histogram bins SUM,
     * min/max fold, representatives keep MIN (deterministic; matches the raw
     * backfill's first-representative style). No duration predicate FILTER —
     * the finer tier already applied it.
     *
     * Every optional column here is both SELECTed from the finer source table
     * and INSERTed into the coarser destination, so the caller may only pass it
     * once it has probed BOTH — the two tiers can disagree (a partial
     * nightowl:drop-v1-histograms, a migration that reached one table and not
     * the other). See BackfillRollupsCommand::tierColumns.
     *
     * @param  string  $unit  Postgres date_trunc unit: 'hour' or 'day'
     * @param  list<string>  $histColumns  hist_NN column names
     * @return array{columns: list<string>, selects: list<string>, groupByCount: int}
     */
    public function tierBackfillSql(string $unit, array $histColumns, bool $withSketch = false, bool $withDurationCount = false): array
    {
        $groupCols = $this->groupColumnNames();
        $columns = [...$groupCols, 'bucket_start', 'environment', 'call_count', ...$this->counterColumns()];

        $selects = [...$groupCols];
        $selects[] = "date_trunc('{$unit}', bucket_start)";
        $selects[] = 'environment';
        $selects[] = 'SUM(call_count)';
        foreach ($this->counterColumns() as $c) {
            $selects[] = "SUM({$c})";
        }

        if ($this->hasDuration) {
            $columns = [...$columns, 'total_duration', 'min_duration', 'max_duration'];
            $selects[] = 'SUM(total_duration)';
            $selects[] = 'MIN(min_duration)';
            $selects[] = 'MAX(max_duration)';
        }

        if ($this->hasDurationCount && $withDurationCount) {
            $columns[] = 'duration_count';
            $selects[] = 'SUM(duration_count)';
        }

        if ($this->hasHistogram) {
            $columns = [...$columns, ...$histColumns];
            foreach ($histColumns as $c) {
                $selects[] = "SUM({$c})";
            }
        }

        if ($withSketch) {
            $columns = [...$columns, 'sketch', 'sketch_version'];
            $selects[] = 'nightowl_ddsketch_agg(sketch)';
            $selects[] = 'MAX(sketch_version)';
        }

        foreach ($this->representativeColumns() as $col) {
            $columns[] = $col;
            $selects[] = "MIN({$col})";
        }

        return ['columns' => $columns, 'selects' => $selects, 'groupByCount' => count($groupCols) + 2];
    }

    /**
     * SQL that recomputes the DDSketch column for minute buckets in a window,
     * entirely database-side — a per-row PHP pass over raw would not scale on
     * exactly the tenants that need backfill. The CASE mirrors
     * DDSketchHistogram::index() (constants checksum-guarded there); each
     * (index, count) pair packs via nightowl_ddsketch_single and folds via
     * nightowl_ddsketch_agg.
     *
     * @return array{sql: string}  parameterised on [since, until]
     */
    public function sketchBackfillSql(string $rollupTable): array
    {
        $g = DDSketchHistogram::GAMMA;
        $idxExpr = sprintf(
            'CASE WHEN %1$s < %2$d THEN %3$d WHEN %1$s > %4$d THEN %5$d '
            .'ELSE LEAST(GREATEST(ceil(ln(%1$s::double precision) / ln(%6$.17g))::bigint, 0), %7$d) END',
            $this->durationField,
            DDSketchHistogram::MIN_VALUE,
            DDSketchHistogram::UNDERFLOW_INDEX,
            DDSketchHistogram::MAX_VALUE,
            DDSketchHistogram::OVERFLOW_INDEX,
            $g,
            DDSketchHistogram::OVERFLOW_INDEX - 1,
        );

        $groupSelects = [];
        $joinConds = [];
        $n = 1;
        foreach ($this->groupColumns as $col => $def) {
            $groupSelects[] = "{$def['sql']} AS g{$n}";
            $joinConds[] = "r.{$col} = s.g{$n}";
            $n++;
        }
        $groupSelectSql = implode(', ', $groupSelects);
        $joinSql = implode(' AND ', $joinConds);
        $groupByCount = count($this->groupColumns) + 2; // + bucket + environment

        $predicate = $this->durationPredicate ? " AND ({$this->durationPredicate['sql']})" : '';
        $version = DDSketchHistogram::VERSION;

        $gAliases = implode(', ', array_map(static fn (int $i): string => "g{$i}", range(1, count($this->groupColumns))));

        $sql = "UPDATE {$rollupTable} r
            SET sketch = s.sk, sketch_version = {$version}
            FROM (
                SELECT {$gAliases}, bucket, env,
                       nightowl_ddsketch_agg(nightowl_ddsketch_single(idx, c)) AS sk
                FROM (
                    SELECT {$groupSelectSql},
                           date_trunc('minute', created_at) AS bucket,
                           COALESCE(environment, '') AS env,
                           {$idxExpr} AS idx,
                           COUNT(*) AS c
                    FROM {$this->source}
                    WHERE created_at >= ? AND created_at < ?
                      AND {$this->durationField} IS NOT NULL{$predicate}
                    GROUP BY ".implode(', ', range(1, $groupByCount + 1))."
                ) g
                GROUP BY {$gAliases}, bucket, env
            ) s
            WHERE {$joinSql} AND r.bucket_start = s.bucket AND r.environment = s.env";

        return ['sql' => $sql];
    }
}
