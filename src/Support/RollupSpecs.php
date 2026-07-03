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
            self::requestUsers(),
            self::jobUsers(),
            self::exceptionUsers(),
            self::exceptionGroups(),
            self::mail(),
            self::notifications(),
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
                // NULL-check (not empty()) so PHP matches the SQL `IS [NOT] NULL` —
                // empty() would also drop attempt_id '' / '0', diverging the live and
                // backfill write paths for those (malformed) values.
                'attempts_count' => ['php' => static fn (array $r): bool => ($r['attempt_id'] ?? null) !== null, 'sql' => 'attempt_id IS NOT NULL'],
                'queued_count' => ['php' => static fn (array $r): bool => ($r['attempt_id'] ?? null) === null, 'sql' => 'attempt_id IS NULL'],
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
            // Duration metrics over ATTEMPT rows only — a queued-job (dispatch) row's
            // duration is enqueue overhead, not execution time, so folding it in drags
            // min ~280x low and skews p95.
            durationPredicate: ['php' => static fn (array $r): bool => ($r['attempt_id'] ?? null) !== null, 'sql' => 'attempt_id IS NOT NULL'],
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

    /**
     * nightowl_requests keyed by USER (not route group_hash) — powers the users
     * list, which groups requests per user. call_count = requests; the status-band
     * counters are byte-identical to requests() so the per-user counts reproduce
     * the same status split. No duration/histogram (the list shows no per-user
     * p95), so this stays a compact count rollup.
     */
    public static function requestUsers(): RollupSpec
    {
        return new RollupSpec(
            table: 'nightowl_user_rollups',
            source: 'nightowl_requests',
            groupColumns: [
                'user_id' => ['php' => static fn (array $r): string => (string) ($r['user'] ?? ''), 'sql' => "COALESCE(user_id, '')"],
            ],
            counters: [
                'success_count' => ['php' => static fn (array $r): bool => (int) ($r['status_code'] ?? 200) < 400, 'sql' => 'status_code < 400'],
                'client_error_count' => ['php' => static fn (array $r): bool => (int) ($r['status_code'] ?? 200) >= 400 && (int) ($r['status_code'] ?? 200) < 500, 'sql' => 'status_code >= 400 AND status_code < 500'],
                'server_error_count' => ['php' => static fn (array $r): bool => (int) ($r['status_code'] ?? 200) >= 500, 'sql' => 'status_code >= 500'],
            ],
            representatives: [],
            hasDuration: false,
            hasHistogram: false,
        );
    }

    /**
     * nightowl_jobs keyed by USER — enriches the users list's "queued jobs"
     * column, which counts job ATTEMPTS per user (attempt_id IS NOT NULL). The
     * attempts_count predicate is byte-identical to jobs() so live drain and
     * backfill agree. Count-only; the list shows a count, not per-user job latency.
     */
    public static function jobUsers(): RollupSpec
    {
        return new RollupSpec(
            table: 'nightowl_user_job_rollups',
            source: 'nightowl_jobs',
            groupColumns: [
                'user_id' => ['php' => static fn (array $r): string => (string) ($r['user'] ?? ''), 'sql' => "COALESCE(user_id, '')"],
            ],
            counters: [
                // NULL-check (not empty()) so PHP matches SQL `IS NOT NULL` — mirrors jobs().
                'attempts_count' => ['php' => static fn (array $r): bool => ($r['attempt_id'] ?? null) !== null, 'sql' => 'attempt_id IS NOT NULL'],
            ],
            representatives: [],
            hasDuration: false,
            hasHistogram: false,
        );
    }

    /**
     * nightowl_exceptions keyed by USER — enriches the users list's "exceptions"
     * column (exceptions per user). Exceptions have no other rollup; the implicit
     * call_count IS the exception count, so no explicit counters are needed.
     */
    public static function exceptionUsers(): RollupSpec
    {
        return new RollupSpec(
            table: 'nightowl_user_exception_rollups',
            source: 'nightowl_exceptions',
            groupColumns: [
                'user_id' => ['php' => static fn (array $r): string => (string) ($r['user'] ?? ''), 'sql' => "COALESCE(user_id, '')"],
            ],
            counters: [],
            representatives: [],
            hasDuration: false,
            hasHistogram: false,
        );
    }

    /**
     * nightowl_exceptions keyed by FINGERPRINT (the exception group the Exceptions
     * section + dashboard group by) — powers the exceptions list/overview/chart.
     * call_count = occurrences; handled/unhandled bands match ExceptionController's
     * raw SQL verbatim. The list's class/message/file/line + affected_users
     * (non-additive COUNT DISTINCT) stay a bounded raw enrichment, so no
     * representatives / duration here.
     */
    public static function exceptionGroups(): RollupSpec
    {
        // Must reproduce writeExceptions()'s fingerprint column exactly: the SDK
        // `_group` when present, else a local hash of class|code|file|line.
        $fingerprint = static fn (array $r): string => ! empty($r['_group'])
            ? (string) $r['_group']
            : md5(($r['class'] ?? '').'|'.($r['code'] ?? '').'|'.($r['file'] ?? '').'|'.($r['line'] ?? ''));

        return new RollupSpec(
            table: 'nightowl_exception_rollups',
            source: 'nightowl_exceptions',
            groupColumns: [
                'fingerprint' => ['php' => $fingerprint, 'sql' => "COALESCE(fingerprint, '')"],
            ],
            counters: [
                'handled_count' => ['php' => static fn (array $r): bool => filter_var($r['handled'] ?? false, FILTER_VALIDATE_BOOLEAN), 'sql' => 'handled = true'],
                'unhandled_count' => ['php' => static fn (array $r): bool => ! filter_var($r['handled'] ?? false, FILTER_VALIDATE_BOOLEAN), 'sql' => 'handled != true OR handled IS NULL'],
            ],
            representatives: [],
            hasDuration: false,
            hasHistogram: false,
        );
    }

    /**
     * nightowl_mail keyed by group_hash (mailable class). Additive call_count +
     * queued/failed counters + duration histogram (the mail list sorts by p95 and
     * shows avg). mailable kept as first-seen representative for the list label.
     */
    public static function mail(): RollupSpec
    {
        return new RollupSpec(
            table: 'nightowl_mail_rollups',
            source: 'nightowl_mail',
            groupColumns: [
                'group_hash' => ['php' => static fn (array $r): string => (string) ($r['_group'] ?? ''), 'sql' => "COALESCE(group_hash, '')"],
            ],
            counters: [
                'queued_count' => ['php' => static fn (array $r): bool => filter_var($r['queued'] ?? false, FILTER_VALIDATE_BOOLEAN), 'sql' => 'queued = true'],
                'failed_count' => ['php' => static fn (array $r): bool => filter_var($r['failed'] ?? false, FILTER_VALIDATE_BOOLEAN), 'sql' => 'failed = true'],
            ],
            representatives: [
                // writeMail maps $r['class'] ?? $r['mailable'] into the mailable column.
                'mailable' => ['php' => static fn (array $r) => $r['class'] ?? $r['mailable'] ?? null, 'sql' => 'MIN(mailable)'],
            ],
            hasDuration: true,
            hasHistogram: true,
        );
    }

    /**
     * nightowl_notifications keyed by (group_hash, channel) — the two-column
     * grouping mirrors query_rollups' (group_hash, connection), so the list can
     * rebuild its DISTINCT channel set additively from the rollup rows. call_count
     * + queued/failed + duration histogram; notification class as representative.
     */
    public static function notifications(): RollupSpec
    {
        return new RollupSpec(
            table: 'nightowl_notification_rollups',
            source: 'nightowl_notifications',
            groupColumns: [
                'group_hash' => ['php' => static fn (array $r): string => (string) ($r['_group'] ?? ''), 'sql' => "COALESCE(group_hash, '')"],
                'channel' => ['php' => static fn (array $r): string => (string) ($r['channel'] ?? ''), 'sql' => "COALESCE(channel, '')"],
            ],
            counters: [
                'queued_count' => ['php' => static fn (array $r): bool => filter_var($r['queued'] ?? false, FILTER_VALIDATE_BOOLEAN), 'sql' => 'queued = true'],
                'failed_count' => ['php' => static fn (array $r): bool => filter_var($r['failed'] ?? false, FILTER_VALIDATE_BOOLEAN), 'sql' => 'failed = true'],
            ],
            representatives: [
                // writeNotifications maps $r['class'] ?? $r['notification'] into the notification column.
                'notification' => ['php' => static fn (array $r) => $r['class'] ?? $r['notification'] ?? null, 'sql' => 'MIN(notification)'],
            ],
            hasDuration: true,
            hasHistogram: true,
        );
    }
}
