<?php

namespace NightOwl\Support;

/**
 * The minute→hour→day rollup tier progression. Wide-range reads aggregate
 * every rollup row in the window, so cost scales with groups × minutes; the
 * hourly and daily siblings cut that 60× / 1440×. Every rollup column is
 * mergeable (counters/histogram bins sum, min/max fold, representatives keep
 * first-seen), so a coarser tier is a lossless collapse of the finer one.
 *
 * Tier tables are structurally identical to their base (created via LIKE
 * INCLUDING ALL — see migration 000054); only bucket_start truncation differs.
 * The tier word sits BEFORE the `_rollups` suffix (nightowl_request_hourly_
 * rollups) so name-based guards matching `nightowl_\w+_rollups` keep covering
 * them.
 */
final class RollupTiers
{
    /** Tier name => bucket granularity in seconds. Base (minute) tier excluded. */
    public const TIERS = [
        'hourly' => 3600,
        'daily' => 86400,
    ];

    /** Tier name => Postgres date_trunc unit, for SQL-side re-aggregation. */
    public const TRUNC_UNIT = [
        'hourly' => 'hour',
        'daily' => 'day',
    ];

    public const BASE_GRANULARITY = 60;

    /** nightowl_request_rollups + 'hourly' → nightowl_request_hourly_rollups */
    public static function table(string $baseTable, string $tier): string
    {
        return str_replace('_rollups', "_{$tier}_rollups", $baseTable);
    }

    /**
     * All tables of a rollup type, finest first: base minute table plus its
     * tier siblings, as table => granularity seconds.
     *
     * @return array<string, int>
     */
    public static function tables(string $baseTable): array
    {
        $all = [$baseTable => self::BASE_GRANULARITY];
        foreach (self::TIERS as $tier => $granularity) {
            $all[self::table($baseTable, $tier)] = $granularity;
        }

        return $all;
    }

    /**
     * Tier-sibling table names only (no base), for callers that enumerate the
     * base tables elsewhere (prune/clear/migrate sweeps).
     *
     * @return list<string>
     */
    public static function tierTables(string $baseTable): array
    {
        return array_map(
            static fn (string $tier): string => self::table($baseTable, $tier),
            array_keys(self::TIERS),
        );
    }

    /** UTC bucket string re-truncated to a coarser granularity. */
    public static function truncateBucket(string $bucketStart, int $granularitySeconds): string
    {
        $ts = strtotime($bucketStart.' UTC');

        return gmdate('Y-m-d H:i:s', intdiv($ts, $granularitySeconds) * $granularitySeconds);
    }

    /**
     * Collapse already-minute-grouped drain accumulators to a coarser tier —
     * pure array math on the handful of groups a batch produced, so the extra
     * tiers cost no additional per-record work.
     *
     * $identityKeys name the fields that identify a group besides bucket_start:
     * ['group'] for the generic writeRollup shape (assoc of group columns), or
     * ['group_hash', 'connection'] for the bespoke query shape. Merge rules by
     * field: call_count/total_duration and each counters/hist entry sum;
     * min/max_duration fold null-aware; reps entries and sql_query keep the
     * first non-null (matching the upsert's COALESCE semantics).
     *
     * The identity values arrive already varchar-clamped by the caller (writeRollup /
     * writeQueryRollups clamp before keying), so collapsed keys inherit the same
     * additive merge — two base groups that clamp equal cannot re-appear here as
     * distinct tier tuples that would collide on the conflict key.
     *
     * @param  array<string, array<string, mixed>>  $groups
     * @param  list<string>  $identityKeys
     * @return array<string, array<string, mixed>>
     */
    public static function collapse(array $groups, int $granularitySeconds, array $identityKeys): array
    {
        $out = [];

        foreach ($groups as $g) {
            $bucket = self::truncateBucket((string) $g['bucket_start'], $granularitySeconds);

            $keyParts = [$bucket];
            foreach ($identityKeys as $ik) {
                $value = $g[$ik];
                if (is_array($value)) {
                    foreach ($value as $v) {
                        $keyParts[] = (string) $v;
                    }
                } else {
                    $keyParts[] = (string) $value;
                }
            }
            $key = implode("\0", $keyParts);

            if (! isset($out[$key])) {
                $copy = $g;
                $copy['bucket_start'] = $bucket;
                $out[$key] = $copy;

                continue;
            }

            $merged = &$out[$key];

            $merged['call_count'] += $g['call_count'];

            if (array_key_exists('total_duration', $g)) {
                $merged['total_duration'] += $g['total_duration'];
            }
            if (array_key_exists('duration_count', $g)) {
                $merged['duration_count'] += $g['duration_count'];
            }
            if (array_key_exists('min_duration', $g)) {
                $merged['min_duration'] = self::foldMin($merged['min_duration'], $g['min_duration']);
            }
            if (array_key_exists('max_duration', $g)) {
                $merged['max_duration'] = self::foldMax($merged['max_duration'], $g['max_duration']);
            }

            if (isset($g['counters']) && is_array($g['counters'])) {
                foreach ($g['counters'] as $c => $v) {
                    $merged['counters'][$c] += $v;
                }
            }
            if (isset($g['hist']) && is_array($g['hist'])) {
                foreach ($g['hist'] as $i => $v) {
                    $merged['hist'][$i] += $v;
                }
            }
            if (isset($g['sketch']) && is_array($g['sketch']) && $g['sketch'] !== []) {
                $merged['sketch'] = DDSketchHistogram::mergeCounts($merged['sketch'] ?? [], $g['sketch']);
            }
            if (isset($g['reps']) && is_array($g['reps'])) {
                foreach ($g['reps'] as $rc => $v) {
                    if ($merged['reps'][$rc] === null && $v !== null) {
                        $merged['reps'][$rc] = $v;
                    }
                }
            }
            if (array_key_exists('sql_query', $g)
                && $merged['sql_query'] === null
                && $g['sql_query'] !== null) {
                $merged['sql_query'] = $g['sql_query'];
            }

            unset($merged);
        }

        return $out;
    }

    private static function foldMin(?int $a, ?int $b): ?int
    {
        if ($a === null) {
            return $b;
        }

        return $b === null ? $a : min($a, $b);
    }

    private static function foldMax(?int $a, ?int $b): ?int
    {
        if ($a === null) {
            return $b;
        }

        return $b === null ? $a : max($a, $b);
    }
}
