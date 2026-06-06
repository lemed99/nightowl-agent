<?php

namespace NightOwl\Support;

/**
 * Fixed log-scale (√2-spaced) duration histogram shared by the agent (which
 * increments bins at drain time) and the API (which estimates percentiles from
 * the summed bins). Percentiles are NOT additive across rollup buckets, so a
 * histogram is the only way to recover windowed p50/p95/p99 from pre-aggregated
 * summaries without a tdigest/TimescaleDB extension we can't assume on a BYO
 * customer Postgres.
 *
 * EDGES MUST stay byte-identical to nightowl-api's App\Support\QueryHistogram —
 * the agent's bin assignment and the API's estimator have to agree exactly.
 * QueryHistogramTest freezes the checksum in both repos to catch drift.
 *
 * Bins (39 total) over the EDGES (in microseconds):
 *   bin 0           — underflow: duration < EDGES[0]
 *   bin i (1..37)   — [EDGES[i-1], EDGES[i])
 *   bin 38          — overflow:  duration >= EDGES[37]
 *
 * √2 spacing (~2 bins/octave) bounds bin width to ~41%, so interpolating within
 * the crossing bin lands a percentile within a few percent for realistic
 * duration distributions. These estimates are approximate by construction.
 */
final class QueryHistogram
{
    /**
     * Frozen √2-spaced boundaries from 128µs to ~47s, computed as
     * round(128 * sqrt(2)**k). Do not edit without updating both repos and the
     * checksum in QueryHistogramTest — the agent and API must agree exactly.
     */
    public const EDGES = [
        128, 181, 256, 362, 512, 724, 1024, 1448,
        2048, 2896, 4096, 5793, 8192, 11585, 16384, 23170,
        32768, 46341, 65536, 92682, 131072, 185364, 262144, 370728,
        524288, 741455, 1048576, 1482910, 2097152, 2965821, 4194304, 5931642,
        8388608, 11863283, 16777216, 23726566, 33554432, 47453133,
    ];

    /** Number of histogram bins = edges + 1 (underflow and overflow are bins). */
    public static function binCount(): int
    {
        return count(self::EDGES) + 1;
    }

    /**
     * Column names hist_00 … hist_NN, in bin order.
     *
     * @return list<string>
     */
    public static function columns(): array
    {
        $columns = [];
        for ($i = 0, $n = self::binCount(); $i < $n; $i++) {
            $columns[] = sprintf('hist_%02d', $i);
        }

        return $columns;
    }

    /**
     * Bin index (0 … binCount-1) for a duration in microseconds, defined as the
     * number of edges <= duration (EDGES is ascending). A null/negative duration
     * should not be passed — callers exclude nulls, matching raw percentile_disc
     * which ignores NULL durations.
     */
    public static function binIndex(int $duration): int
    {
        $i = 0;
        foreach (self::EDGES as $edge) {
            if ($duration < $edge) {
                break;
            }
            $i++;
        }

        return $i;
    }

    /**
     * Estimate a percentile (p in 0..1) in microseconds from per-bin counts,
     * by walking the cumulative counts to the rank crossing and interpolating
     * linearly within the crossing bin. The overflow bin has no upper bound, so
     * a percentile landing there returns its lower edge.
     *
     * @param  array<int, int|float|string|null>  $binCounts  Indexed 0..binCount-1
     */
    public static function estimatePercentile(array $binCounts, float $p): float
    {
        $total = 0;
        $n = self::binCount();
        for ($i = 0; $i < $n; $i++) {
            $total += (int) ($binCounts[$i] ?? 0);
        }
        if ($total <= 0) {
            return 0.0;
        }

        $rank = (int) ceil($total * $p);
        if ($rank < 1) {
            $rank = 1;
        }

        $cumulative = 0;
        for ($i = 0; $i < $n; $i++) {
            $count = (int) ($binCounts[$i] ?? 0);
            if ($count <= 0) {
                continue;
            }

            if ($cumulative + $count >= $rank) {
                $lo = $i === 0 ? 0 : self::EDGES[$i - 1];

                // Overflow bin — unbounded above, so report its lower edge.
                if ($i >= $n - 1) {
                    return (float) $lo;
                }

                $hi = self::EDGES[$i];
                $within = ($rank - $cumulative) / $count; // (0, 1]

                return $lo + ($hi - $lo) * $within;
            }

            $cumulative += $count;
        }

        return (float) self::EDGES[count(self::EDGES) - 1];
    }

    /**
     * SQL `SUM(CASE …)` expression per bin for aggregating a raw duration column
     * into histogram counts (used by the agent backfill and the API test
     * rebuild). NULL durations fall through every CASE (NULL comparisons are
     * NULL), so they're excluded — matching raw percentile_disc.
     *
     * @return array<string, string> column name => SQL expression
     */
    public static function caseSql(string $durationColumn): array
    {
        $edges = self::EDGES;
        $n = self::binCount();
        $out = [];

        for ($i = 0; $i < $n; $i++) {
            if ($i === 0) {
                $cond = "{$durationColumn} < {$edges[0]}";
            } elseif ($i === $n - 1) {
                $cond = "{$durationColumn} >= {$edges[$n - 2]}";
            } else {
                $cond = "{$durationColumn} >= {$edges[$i - 1]} AND {$durationColumn} < {$edges[$i]}";
            }

            $out[sprintf('hist_%02d', $i)] = "SUM(CASE WHEN {$cond} THEN 1 ELSE 0 END)";
        }

        return $out;
    }
}
