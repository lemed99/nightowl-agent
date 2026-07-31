<?php

namespace NightOwl\Support;

/**
 * The ONE definition of the request-concurrency recompute — shared by the
 * drain's cleanup-tick maintenance (RecordWriter::maintainConcurrencyRollup)
 * and nightowl:backfill-rollups, so the two can never drift.
 *
 * Why recompute-from-raw is the ONLY writer: drain batches arrive
 * COMPLETION-ordered — a request's +1 event (at its start timestamp) reaches
 * the writer ~duration late, so any incremental batch-append fold loses the
 * overlap between requests completing in different batches. Measured on a
 * realistic mix: avg 70% under-reported max_prefix; 50 concurrent 1s requests
 * folded incrementally reported a peak of 3. The append combine is exact only
 * for event-time-ordered input, which completion order is not. A window
 * function over raw rows sorts the full event stream and is exact regardless
 * of arrival order — so every write path uses it, and the table never holds a
 * batch-order-dependent number.
 *
 * Event guards mirror the API's raw sweep (numeric-timestamp regex, non-null
 * duration) plus a garbage ceiling: a record whose computed END lands beyond
 * the plausible-future bound is dropped WHOLE on both legs — admitting its +1
 * while the far-future -1 falls outside every recompute window would bake a
 * permanent phantom in-flight request into every later walk.
 *
 * Ties order (ts, delta DESC): starts before ends at equal timestamps — the
 * conservative order the API's raw sweep documents.
 */
final class ConcurrencyRollup
{
    public const TABLE = 'nightowl_request_concurrency_rollups';

    /**
     * How far outside the recompute window the created_at scan widens; same
     * event-time-vs-row-time margin as the API's raw sweep. A request longer
     * than the margin escapes the scan — the documented limit of every
     * event-time read in this codebase.
     */
    public const SCAN_MARGIN_SECONDS = 900;

    /** Plausible-future bound for a computed end (mirrors EVENT_TS_MAX_FUTURE_SECONDS). */
    public const END_CEIL_FUTURE_SECONDS = 86400;

    private const TS_GUARD = "timestamp ~ '^[0-9]+(\\.[0-9]+)?\$'";

    /**
     * Exact per-minute (delta_sum, max_prefix) recompute over the raw request
     * families, INSERTing buckets in [bucketLow, bucketHigh) epochs. Callers
     * DELETE the same bucket range first, inside the same transaction, under
     * the 'nightowl_rollup:' advisory lock for TABLE.
     *
     * BOTH families are optional, and the v1 one is the reason this is not a
     * constant: prune's v1-EOL step DROPS `nightowl_requests` outright once the
     * fence is past retention, under a running daemon. A leg naming a dropped
     * table raises 42P01 for the WHOLE statement — the recompute writes
     * nothing, the maintenance tick catches it into error_log, and the table
     * freezes at the minute the drop landed while every other rollup stays
     * current. That is invisible from the outside: the API's coverage gate sees
     * a stale max(bucket_start), silently falls back to its raw sweep, and a
     * wide range 57014s. So callers probe per call and never cache — the same
     * rule RecordWriter::syncIssuesToExceptions follows for its v1 leg, and for
     * the same reason. (Yomoney, 2026-07-31: v1 dropped at 00:00 UTC, every
     * tick since aborted, 14d peak-concurrency charts 504'd.)
     *
     * Returns null when NEITHER family is present — there is no statement to
     * run, and callers must skip rather than issue one.
     *
     * @return array{sql: string, bindings: list<int|string>}|null
     */
    public static function recompute(
        string $scanFrom,
        string $scanTo,
        int $endCeilEpoch,
        int $bucketLowEpoch,
        int $bucketHighEpoch,
        bool $includeV2 = false,
        bool $includeV1 = true,
    ): ?array {
        $guard = self::TS_GUARD;
        $startLegs = [];
        $endLegs = [];

        if ($includeV1) {
            $startLegs[] = "SELECT timestamp::float8 AS ts, 1 AS delta
                FROM nightowl_requests
                WHERE created_at >= ? AND created_at < ?
                  AND duration IS NOT NULL AND duration >= 0 AND {$guard}
                  AND timestamp::float8 + duration / 1000000.0 <= ?";
            $endLegs[] = "SELECT timestamp::float8 + duration / 1000000.0, -1
                FROM nightowl_requests
                WHERE created_at >= ? AND created_at < ?
                  AND duration IS NOT NULL AND duration >= 0 AND {$guard}
                  AND timestamp::float8 + duration / 1000000.0 <= ?";
        }

        if ($includeV2) {
            // v2 legs: ts_us is a typed bigint (event µs) — no regex guard
            // needed; duration/end-ceiling guards identical.
            $startLegs[] = 'SELECT ts_us / 1000000.0 AS ts, 1 AS delta
                FROM nightowl_requests_v2
                WHERE created_at >= ? AND created_at < ?
                  AND duration IS NOT NULL AND duration >= 0
                  AND ts_us / 1000000.0 + duration / 1000000.0 <= ?';
            $endLegs[] = 'SELECT ts_us / 1000000.0 + duration / 1000000.0, -1
                FROM nightowl_requests_v2
                WHERE created_at >= ? AND created_at < ?
                  AND duration IS NOT NULL AND duration >= 0
                  AND ts_us / 1000000.0 + duration / 1000000.0 <= ?';
        }

        if ($startLegs === []) {
            return null;
        }

        $leg = implode(' UNION ALL
                ', $startLegs);
        $endLeg = implode(' UNION ALL
                ', $endLegs);

        $sql = 'INSERT INTO '.self::TABLE." (bucket_start, delta_sum, max_prefix)
             SELECT to_timestamp(bucket) AT TIME ZONE 'UTC',
                    SUM(delta),
                    GREATEST(MAX(running), 0)
             FROM (
                 SELECT (floor(ts / 60) * 60)::bigint AS bucket,
                        delta,
                        SUM(delta) OVER (
                            PARTITION BY (floor(ts / 60) * 60)::bigint
                            ORDER BY ts, delta DESC
                            ROWS UNBOUNDED PRECEDING
                        ) AS running
                 FROM ({$leg} UNION ALL {$endLeg}) events
             ) folded
             GROUP BY bucket
             HAVING bucket >= ? AND bucket < ?";

        // One window per arm, in arm order — driven by the arms actually
        // emitted, never by a family flag read a second time.
        $perLeg = [];
        foreach ($startLegs as $_) {
            array_push($perLeg, $scanFrom, $scanTo, $endCeilEpoch);
        }

        return [
            'sql' => $sql,
            'bindings' => [
                ...$perLeg,   // start leg, one window per family arm
                ...$perLeg,   // end leg, same arms in the same order
                $bucketLowEpoch, $bucketHighEpoch,
            ],
        ];
    }
}
