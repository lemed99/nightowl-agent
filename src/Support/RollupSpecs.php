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
            self::exceptionServers(),
            self::mail(),
            self::notifications(),
            self::commands(),
            self::scheduledTasks(),
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
        return new RollupSpec(
            table: 'nightowl_exception_rollups',
            source: 'nightowl_exceptions',
            groupColumns: [
                'fingerprint' => ['php' => self::exceptionFingerprint(), 'sql' => "COALESCE(fingerprint, '')"],
            ],
            counters: [
                'handled_count' => ['php' => static fn (array $r): bool => filter_var($r['handled'] ?? false, FILTER_VALIDATE_BOOLEAN), 'sql' => 'handled = true'],
                'unhandled_count' => ['php' => static fn (array $r): bool => ! filter_var($r['handled'] ?? false, FILTER_VALIDATE_BOOLEAN), 'sql' => 'handled != true OR handled IS NULL'],
                // Occurrences with a user attached — powers the detail page's
                // authenticated-vs-guest split (guest = call_count - authenticated_count).
                // The '0' literal must count as authenticated, so test for a non-empty
                // STRING (not empty(), which treats '0' as empty) to match the SQL side.
                'authenticated_count' => ['php' => static fn (array $r): bool => isset($r['user']) && (string) $r['user'] !== '', 'sql' => "user_id IS NOT NULL AND user_id != ''"],
            ],
            representatives: [],
            hasDuration: false,
            hasHistogram: false,
        );
    }

    /**
     * Distinct-server-per-fingerprint rollup keyed (fingerprint, server): powers the
     * exception detail "servers affected" stat as COUNT(DISTINCT server) over the
     * compact rollup instead of an unbounded distinct scan of raw nightowl_exceptions.
     * Count-only — server presence is the signal (call_count rides along). Mirrors the
     * queries/connection + notifications/channel two-dimension rollups.
     */
    public static function exceptionServers(): RollupSpec
    {
        return new RollupSpec(
            table: 'nightowl_exception_server_rollups',
            source: 'nightowl_exceptions',
            groupColumns: [
                'fingerprint' => ['php' => self::exceptionFingerprint(), 'sql' => "COALESCE(fingerprint, '')"],
                'server' => ['php' => static fn (array $r): string => (string) ($r['server'] ?? ''), 'sql' => "COALESCE(server, '')"],
            ],
            counters: [],
            representatives: [],
            hasDuration: false,
            hasHistogram: false,
        );
    }

    /**
     * The exception fingerprint extractor, shared by exceptionGroups() and
     * exceptionServers() so their PK keys can never drift apart. MUST reproduce
     * writeExceptions()'s fingerprint column exactly: the SDK `_group` when present,
     * else a local hash of class|code|file|line.
     */
    private static function exceptionFingerprint(): callable
    {
        return static fn (array $r): string => ! empty($r['_group'])
            ? (string) $r['_group']
            : md5(($r['class'] ?? '').'|'.($r['code'] ?? '').'|'.($r['file'] ?? '').'|'.($r['line'] ?? ''));
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
            hasDurationCount: true,
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
            hasDurationCount: true,
        );
    }

    /**
     * nightowl_commands keyed by group_hash (command class) — powers the commands
     * list/overview/charts. Success is exit_code = 0; failure is exit_code != 0,
     * matching CommandController's raw bands verbatim. A NULL exit_code (never in
     * practice) satisfies neither, exactly like the SQL `= 0` / `!= 0`. Every row
     * carries one execution duration (no queued/attempt split), so no
     * durationPredicate — writeRollup's own null-duration guard is enough.
     */
    public static function commands(): RollupSpec
    {
        return new RollupSpec(
            table: 'nightowl_command_rollups',
            source: 'nightowl_commands',
            groupColumns: [
                'group_hash' => ['php' => static fn (array $r): string => (string) ($r['_group'] ?? ''), 'sql' => "COALESCE(group_hash, '')"],
            ],
            counters: [
                // Match `exit_code = 0` / `exit_code != 0`: NULL/absent is neither.
                'successful_count' => ['php' => static fn (array $r): bool => ($r['exit_code'] ?? null) !== null && (int) $r['exit_code'] === 0, 'sql' => 'exit_code = 0'],
                'unsuccessful_count' => ['php' => static fn (array $r): bool => ($r['exit_code'] ?? null) !== null && (int) $r['exit_code'] !== 0, 'sql' => 'exit_code != 0'],
            ],
            representatives: [
                // writeCommands writes `command` = $r['command'] ?? 'unknown'.
                'command' => ['php' => static fn (array $r) => $r['command'] ?? 'unknown', 'sql' => 'MIN(command)'],
            ],
            hasDuration: true,
            hasHistogram: true,
            hasDurationCount: true,
        );
    }

    /**
     * nightowl_scheduled_tasks keyed by group_hash (schedule identifier) — powers
     * the scheduled-tasks list/overview/charts. Status bands match
     * ScheduledTaskController verbatim: failed = 'failed', processed = 'processed'
     * OR the legacy 'success' alias, skipped = 'skipped'. command + cron expression
     * + the fixed cadence (repeat_seconds) are first-seen representatives.
     */
    public static function scheduledTasks(): RollupSpec
    {
        return new RollupSpec(
            table: 'nightowl_scheduled_task_rollups',
            source: 'nightowl_scheduled_tasks',
            groupColumns: [
                'group_hash' => ['php' => static fn (array $r): string => (string) ($r['_group'] ?? ''), 'sql' => "COALESCE(group_hash, '')"],
            ],
            counters: [
                'failed_count' => ['php' => static fn (array $r): bool => ($r['status'] ?? '') === 'failed', 'sql' => "status = 'failed'"],
                'processed_count' => ['php' => static fn (array $r): bool => in_array($r['status'] ?? '', ['processed', 'success'], true), 'sql' => "status = 'processed' OR status = 'success'"],
                'skipped_count' => ['php' => static fn (array $r): bool => ($r['status'] ?? '') === 'skipped', 'sql' => "status = 'skipped'"],
            ],
            representatives: [
                // writeScheduledTasks writes command = $r['name'] ?? $r['command'] ?? 'unknown',
                // expression = $r['cron'] ?? $r['expression'], repeat_seconds = $r['repeat_seconds'] ?? 0.
                'command' => ['php' => static fn (array $r) => $r['name'] ?? $r['command'] ?? 'unknown', 'sql' => 'MIN(command)'],
                'expression' => ['php' => static fn (array $r) => $r['cron'] ?? $r['expression'] ?? null, 'sql' => 'MIN(expression)'],
                'repeat_seconds' => ['php' => static fn (array $r) => $r['repeat_seconds'] ?? 0, 'sql' => 'MIN(repeat_seconds)'],
            ],
            hasDuration: true,
            hasHistogram: true,
            hasDurationCount: true,
        );
    }
}
