<?php

namespace NightOwl\Support;

/**
 * Registry of rollup specs, one per high-volume telemetry type. The agent drain
 * (RecordWriter::writeRollup) and the backfill command iterate these so the
 * rollup machinery is written once and configured per type.
 *
 * Counter/group/representative SQL expressions MUST match the raw aggregation in
 * nightowl-api's controllers exactly (e.g. the status_code bands in
 * RequestController) — the rollup has to reproduce what the read path used to
 * compute over raw rows.
 */
final class RollupSpecs
{
    /** @return list<RollupSpec> */
    public static function all(): array
    {
        return [
            self::queries(),
            self::requests(),
            self::jobs(),
            self::outgoingRequests(),
            self::cacheEvents(),
        ];
    }

    /**
     * Queries rollup spec. The live drain writes this table via the bespoke
     * RecordWriter::writeQueryRollups (kept for its proven path); this spec
     * exists so backfill and prune cover queries through the same generic
     * machinery as every other type.
     */
    public static function queries(): RollupSpec
    {
        return new RollupSpec(
            table: 'nightowl_query_rollups',
            source: 'nightowl_queries',
            groupColumns: [
                'group_hash' => ['php' => static fn (array $r): string => (string) ($r['_group'] ?? ''), 'sql' => "COALESCE(group_hash, '')"],
                'connection' => ['php' => static fn (array $r): string => (string) ($r['connection'] ?? ''), 'sql' => "COALESCE(connection, '')"],
            ],
            counters: [],
            representatives: [
                'sql_query' => ['php' => static fn (array $r) => $r['sql'] ?? null, 'sql' => 'MIN(sql_query)'],
            ],
            hasDuration: true,
            hasHistogram: true,
        );
    }

    public static function requests(): RollupSpec
    {
        return new RollupSpec(
            table: 'nightowl_request_rollups',
            source: 'nightowl_requests',
            groupColumns: [
                'group_hash' => ['php' => static fn (array $r): string => (string) ($r['_group'] ?? ''), 'sql' => "COALESCE(group_hash, '')"],
            ],
            counters: [
                'success_count' => ['php' => static fn (array $r): bool => (int) ($r['status_code'] ?? 200) < 400, 'sql' => 'status_code < 400'],
                'client_error_count' => ['php' => static fn (array $r): bool => (int) ($r['status_code'] ?? 200) >= 400 && (int) ($r['status_code'] ?? 200) < 500, 'sql' => 'status_code >= 400 AND status_code < 500'],
                'server_error_count' => ['php' => static fn (array $r): bool => (int) ($r['status_code'] ?? 200) >= 500, 'sql' => 'status_code >= 500'],
            ],
            representatives: [
                'route_methods' => ['php' => static fn (array $r): string => is_array($r['route_methods'] ?? null) ? (string) json_encode($r['route_methods']) : (string) ($r['route_methods'] ?? json_encode([])), 'sql' => 'MIN(route_methods)'],
                'route_path' => ['php' => static fn (array $r) => $r['route_path'] ?? null, 'sql' => 'MIN(route_path)'],
            ],
            hasDuration: true,
            hasHistogram: true,
        );
    }

    public static function jobs(): RollupSpec
    {
        return new RollupSpec(
            table: 'nightowl_job_rollups',
            source: 'nightowl_jobs',
            groupColumns: [
                'group_hash' => ['php' => static fn (array $r): string => (string) ($r['_group'] ?? ''), 'sql' => "COALESCE(group_hash, '')"],
            ],
            counters: [
                // total = attempts (a queued job has no attempt_id); duration/p95
                // are over attempts only, so attempts_count is the avg denominator.
                'attempts_count' => ['php' => static fn (array $r): bool => ! empty($r['attempt_id']), 'sql' => 'attempt_id IS NOT NULL'],
                'queued_count' => ['php' => static fn (array $r): bool => empty($r['attempt_id']), 'sql' => 'attempt_id IS NULL'],
                'processed_count' => ['php' => static fn (array $r): bool => ($r['status'] ?? '') === 'processed', 'sql' => "status = 'processed'"],
                'released_count' => ['php' => static fn (array $r): bool => ($r['status'] ?? '') === 'released', 'sql' => "status = 'released'"],
                'failed_count' => ['php' => static fn (array $r): bool => ($r['status'] ?? '') === 'failed', 'sql' => "status = 'failed'"],
            ],
            representatives: [
                'job_class' => ['php' => static fn (array $r) => $r['name'] ?? $r['job_class'] ?? 'Unknown', 'sql' => 'MIN(job_class)'],
                'queue' => ['php' => static fn (array $r) => $r['queue'] ?? null, 'sql' => 'MIN(queue)'],
            ],
            hasDuration: true,
            hasHistogram: true,
        );
    }

    public static function outgoingRequests(): RollupSpec
    {
        // The grouped list displays host = extractHost(url) =
        // SPLIT_PART(url,'/',1) || '//' || SPLIT_PART(url,'/',3) (scheme + host).
        // The PHP representative replicates SPLIT_PART exactly so a backfill (SQL)
        // and live drain (PHP) agree.
        $extractHost = static function (array $r): string {
            $parts = explode('/', (string) ($r['url'] ?? ''));

            return ($parts[0] ?? '').'//'.($parts[2] ?? '');
        };

        return new RollupSpec(
            table: 'nightowl_outgoing_request_rollups',
            source: 'nightowl_outgoing_requests',
            groupColumns: [
                'group_hash' => ['php' => static fn (array $r): string => (string) ($r['_group'] ?? ''), 'sql' => "COALESCE(group_hash, '')"],
            ],
            counters: [
                'success_count' => ['php' => static fn (array $r): bool => (int) ($r['status_code'] ?? 0) < 400 && ($r['status_code'] ?? null) !== null, 'sql' => 'status_code < 400'],
                'client_error_count' => ['php' => static fn (array $r): bool => (int) ($r['status_code'] ?? 0) >= 400 && (int) ($r['status_code'] ?? 0) < 500, 'sql' => 'status_code >= 400 AND status_code < 500'],
                'server_error_count' => ['php' => static fn (array $r): bool => (int) ($r['status_code'] ?? 0) >= 500, 'sql' => 'status_code >= 500'],
            ],
            representatives: [
                'host' => ['php' => $extractHost, 'sql' => "MIN(SPLIT_PART(url, '/', 1) || '//' || SPLIT_PART(url, '/', 3))"],
            ],
            hasDuration: true,
            hasHistogram: true,
        );
    }

    public static function cacheEvents(): RollupSpec
    {
        // Cache groups by (key, store) — no group_hash, no percentile (the cache
        // UI shows no p95), so no histogram. Duration totals power the list's avg
        // column only. event_type lives in the record's `type` field.
        $type = static fn (array $r): string => (string) ($r['type'] ?? '');

        return new RollupSpec(
            table: 'nightowl_cache_rollups',
            source: 'nightowl_cache_events',
            groupColumns: [
                'key' => ['php' => static fn (array $r): string => (string) ($r['key'] ?? ''), 'sql' => "COALESCE(key, '')"],
                'store' => ['php' => static fn (array $r): string => (string) ($r['store'] ?? ''), 'sql' => "COALESCE(store, '')"],
            ],
            counters: [
                'hits' => ['php' => static fn (array $r): bool => $type($r) === 'hit', 'sql' => "event_type = 'hit'"],
                'misses' => ['php' => static fn (array $r): bool => $type($r) === 'miss', 'sql' => "event_type = 'miss'"],
                'writes' => ['php' => static fn (array $r): bool => in_array($type($r), ['set', 'put'], true), 'sql' => "event_type IN ('set', 'put')"],
                'deletes' => ['php' => static fn (array $r): bool => in_array($type($r), ['forget', 'delete'], true), 'sql' => "event_type IN ('forget', 'delete')"],
                'fails' => ['php' => static fn (array $r): bool => $type($r) === 'fail', 'sql' => "event_type = 'fail'"],
                'delete_failures' => ['php' => static fn (array $r): bool => in_array($type($r), ['delete_fail', 'forget_fail'], true), 'sql' => "event_type IN ('delete_fail', 'forget_fail')"],
                'write_failures' => ['php' => static fn (array $r): bool => in_array($type($r), ['write_fail', 'set_fail', 'put_fail', 'fail'], true), 'sql' => "event_type IN ('write_fail', 'set_fail', 'put_fail', 'fail')"],
            ],
            representatives: [],
            hasDuration: true,
            hasHistogram: false,
        );
    }
}
