<?php

namespace NightOwl\Support;

use PDO;

/**
 * Storage format v2: fence/probe helpers plus the pure value encoders the v2
 * row builders share.
 *
 * Wire form through the drain: everything travels as text — uuids as their
 * 36-char string (PG casts on ingest), bytea as '\x' + hex (survives the COPY
 * TSV escaper's backslash doubling AND binds identically through the
 * insertBatch fallback; round-trip pinned by RecordWriterV2Test).
 *
 * Encoder philosophy (mirrors the drain's poison-data precedents — varchar
 * clamp, eventEpoch range guard): a malformed value NULLs that one field and
 * logs, it never throws. A typed column rejecting garbage with 22P02 would
 * head-of-line-block the whole drain; losing one unparseable uuid is the
 * established smaller loss.
 */
final class StorageV2
{
    /** v1 table → v2 twin. Key order mirrors RawPartitions::TABLES. */
    public const TABLES = [
        'nightowl_requests' => 'nightowl_requests_v2',
        'nightowl_queries' => 'nightowl_queries_v2',
        'nightowl_exceptions' => 'nightowl_exceptions_v2',
        'nightowl_commands' => 'nightowl_commands_v2',
        'nightowl_jobs' => 'nightowl_jobs_v2',
        'nightowl_cache_events' => 'nightowl_cache_events_v2',
        'nightowl_mail' => 'nightowl_mail_v2',
        'nightowl_notifications' => 'nightowl_notifications_v2',
        'nightowl_outgoing_requests' => 'nightowl_outgoing_requests_v2',
        'nightowl_scheduled_tasks' => 'nightowl_scheduled_tasks_v2',
        'nightowl_logs' => 'nightowl_logs_v2',
    ];

    /** Closed set of nightowl_dict_string kinds. */
    public const KINDS = [
        'environment', 'server', 'deploy', 'execution_source', 'execution_stage',
        'connection', 'queue', 'status', 'event_type', 'method', 'level',
        'channel', 'store', 'host', 'connection_type', 'job_class', 'cache_pattern',
    ];

    public const FENCE_KEY = 'v2_fence';

    // Mirrors RecordWriter::EVENT_TS_MAX_* — the two clocks must agree so a
    // row's created_at (partition key) and ts_us (event time) never diverge
    // on which guard branch they took.
    private const EVENT_TS_MAX_PAST_SECONDS = 31622400;

    private const EVENT_TS_MAX_FUTURE_SECONDS = 86400;

    /** Placeholder strings that carry no information; deflateOrNull maps them to NULL. */
    private const BLOB_PLACEHOLDERS = ['', '{}', 'null', '[]'];

    /** @var array<string, true> once-per-process log throttle, keyed by field */
    private static array $logged = [];

    public static function v2Name(string $v1Table): string
    {
        return self::TABLES[$v1Table] ?? $v1Table.'_v2';
    }

    /**
     * All 11 v2 parents present? The 000067 migration creates them in one
     * transaction, so this is all-or-nothing on a healthy tenant; a partial
     * count means a half-applied schema and reads as "not enabled".
     */
    public static function enabled(PDO $pdo): bool
    {
        $terms = [];
        foreach (self::TABLES as $v2) {
            $terms[] = "(to_regclass('{$v2}') IS NOT NULL)::int";
        }
        $sum = $pdo->query('SELECT '.implode(' + ', $terms).' AS n')->fetchColumn();

        return (int) $sum === count(self::TABLES);
    }

    /** The cutover fence timestamp ('Y-m-d H:i:s' UTC), or null pre-cutover. */
    public static function fence(PDO $pdo): ?string
    {
        $stmt = $pdo->prepare('SELECT value FROM nightowl_settings WHERE key = ?');
        $stmt->execute([self::FENCE_KEY]);
        $value = $stmt->fetchColumn();

        return $value === false ? null : (string) $value;
    }

    /**
     * Event time in microseconds. Same range guard as RecordWriter::eventEpoch
     * (applied to the seconds value) but keeps the wire float's sub-second
     * precision instead of truncating. Implausible/absent → drain clock.
     */
    public static function tsMicros(array $r, int $nowTs): int
    {
        $ts = $r['timestamp'] ?? null;
        if (is_numeric($ts)) {
            $seconds = (int) $ts;
            if ($seconds >= $nowTs - self::EVENT_TS_MAX_PAST_SECONDS
                && $seconds <= $nowTs + self::EVENT_TS_MAX_FUTURE_SECONDS) {
                return (int) round(((float) $ts) * 1_000_000);
            }
        }

        return $nowTs * 1_000_000;
    }

    /** Lowercased canonical uuid, or NULL (+ one log per field per process) for garbage. */
    public static function uuidOrNull(mixed $value, string $field = 'uuid'): ?string
    {
        if ($value === null || $value === '') {
            return null;
        }
        if (is_string($value)
            && preg_match('/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/', $value) === 1) {
            return strtolower($value);
        }

        self::logOnce($field, 'not a uuid');

        return null;
    }

    /**
     * trace_id for a child record: NULL when it equals execution_id (the SDK
     * stamps both from the same uuid on request-sourced children). Readers
     * reconstruct with COALESCE(trace_id, execution_id) — exact, lossless.
     */
    public static function traceIdFor(array $r): ?string
    {
        $trace = self::uuidOrNull($r['trace_id'] ?? null, 'trace_id');
        if ($trace === null) {
            return null;
        }

        $execution = self::uuidOrNull($r['execution_id'] ?? null, 'execution_id');

        return $trace === $execution ? null : $trace;
    }

    /** 32-hex-char digest → bytea wire form ('\x' + lowercase hex), or NULL. */
    public static function hex16OrNull(mixed $value, string $field = 'hash'): ?string
    {
        if ($value === null || $value === '') {
            return null;
        }
        if (is_string($value) && preg_match('/^[0-9a-fA-F]{32}$/', $value) === 1) {
            return '\x'.strtolower($value);
        }

        self::logOnce($field, 'not a 32-char hex digest');

        return null;
    }

    /**
     * Deflated blob in bytea wire form, or NULL when the value is a
     * no-information placeholder. Arrays are json_encoded first (matching the
     * v1 writers' behavior for context/headers/payload).
     */
    public static function deflateOrNull(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }
        if (! is_string($value)) {
            $value = json_encode($value);
            if ($value === false) {
                return null;
            }
        }
        if (in_array($value, self::BLOB_PLACEHOLDERS, true)) {
            return null;
        }

        $deflated = gzdeflate($value, 6);
        if ($deflated === false) {
            return null;
        }

        return '\x'.bin2hex($deflated);
    }

    /**
     * Raw-history floor across both storage families (MigrateCommand's
     * backfill reconciliation): min(created_at) over v1 and its v2 twin,
     * whichever exists.
     */
    public static function rawMinCreatedAt(PDO $pdo, string $v1Table): ?string
    {
        $legs = [];
        foreach ([$v1Table, self::v2Name($v1Table)] as $table) {
            $exists = $pdo->query("SELECT to_regclass('{$table}') IS NOT NULL AS e")->fetchColumn();
            if ($exists) {
                $legs[] = "SELECT MIN(created_at) AS m FROM {$table}";
            }
        }
        if ($legs === []) {
            return null;
        }

        $min = $pdo->query('SELECT MIN(m)::text FROM ('.implode(' UNION ALL ', $legs).') mins')->fetchColumn();

        return $min === false || $min === null ? null : (string) $min;
    }

    private static function logOnce(string $field, string $reason): void
    {
        if (isset(self::$logged[$field])) {
            return;
        }
        self::$logged[$field] = true;
        error_log("[NightOwl Agent] StorageV2: {$field} {$reason}; stored NULL (logged once per process)");
    }

    // -------------------------------------------------- v1-compat projection

    /**
     * Per-table shaping of v2 columns back into their v1 names/kinds. This is
     * the agent-side twin of nightowl-api's App\Support\V2\V2Schema map —
     * structurally parallel by contract (the API shapes reads, this shapes
     * the rollup backfill's scan). Kinds:
     *   plain | ts | uuid | uuid_coalesce | hash | label:<kind> | sql | route
     * Blob columns are omitted on purpose: no rollup spec, sketch pass, or
     * issue leg reads headers/payload/context/trace text.
     */
    private const COMPAT = [
        // STRICTLY the columns each table's backfill consumers reference —
        // its RollupSpecs' group exprs / counters / representatives /
        // durationField / user cols, plus sketchBackfillSql's needs
        // (created_at is always projected). Over-projection is not harmless:
        // the v1 UNION arm must select the same list, so every extra column
        // becomes a schema assumption the arm imposes on the v1 table.
        'nightowl_requests' => [
            'group_hash' => 'hash', 'environment' => 'label:environment',
            'user_id' => 'plain', 'duration' => 'plain', 'status_code' => 'plain',
            'route_methods' => 'route', 'route_path' => 'route',
        ],
        'nightowl_queries' => [
            'group_hash' => 'hash', 'environment' => 'label:environment',
            'connection' => 'label:connection', 'duration' => 'plain',
            'sql_query' => 'sql',
        ],
        'nightowl_exceptions' => [
            'fingerprint' => 'hash', 'environment' => 'label:environment',
            'user_id' => 'plain', 'server' => 'label:server',
            'class' => 'plain', 'handled' => 'plain',
        ],
        'nightowl_commands' => [
            'group_hash' => 'hash', 'environment' => 'label:environment',
            'command' => 'plain', 'exit_code' => 'plain', 'duration' => 'plain',
        ],
        'nightowl_jobs' => [
            'group_hash' => 'hash', 'environment' => 'label:environment',
            'user_id' => 'plain', 'attempt_id' => 'uuid',
            'job_class' => 'label:job_class', 'queue' => 'label:queue',
            'status' => 'label:status', 'duration' => 'plain',
        ],
        'nightowl_cache_events' => [
            'environment' => 'label:environment',
            'event_type' => 'label:event_type', 'key' => 'plain',
            'key_pattern' => 'label:cache_pattern', 'store' => 'label:store',
            'duration' => 'plain',
        ],
        'nightowl_mail' => [
            'group_hash' => 'hash', 'environment' => 'label:environment',
            'mailable' => 'plain', 'duration' => 'plain',
            'failed' => 'plain', 'queued' => 'plain',
        ],
        'nightowl_notifications' => [
            'group_hash' => 'hash', 'environment' => 'label:environment',
            'notification' => 'plain', 'channel' => 'label:channel',
            'duration' => 'plain', 'failed' => 'plain', 'queued' => 'plain',
        ],
        'nightowl_outgoing_requests' => [
            'group_hash' => 'hash', 'environment' => 'label:environment',
            'url' => 'plain', 'status_code' => 'plain', 'duration' => 'plain',
        ],
        'nightowl_scheduled_tasks' => [
            'group_hash' => 'hash', 'environment' => 'label:environment',
            'command' => 'plain', 'expression' => 'plain', 'repeat_seconds' => 'plain',
            'status' => 'label:status', 'duration' => 'plain', 'exit_code' => 'plain',
        ],
        // nightowl_logs: no rollup spec reads it — no compat projection.
    ];

    /** v2 physical column overrides where the {col}_id derivation doesn't hold. */
    private const COMPAT_SRC = [
        'nightowl_cache_events' => ['key_pattern' => 'pattern_id'],
    ];

    /**
     * A SELECT presenting `{table}_v2` under the v1 column names every rollup
     * spec / sketch pass / issue leg references — `encode(group_hash,'hex')`,
     * dict-joined labels, `s.sql AS sql_query` — plus `created_at`. UNION'd
     * with the v1 scan by unionFrom(), which is what keeps every RollupSpec
     * SQL expression byte-identical across storage families (hex == the PHP
     * arm's keys, so rollup rows agree across all three write paths).
     */
    public static function compatSelect(string $v1Table, string $whereSql = ''): string
    {
        $map = self::COMPAT[$v1Table] ?? null;
        if ($map === null) {
            throw new \InvalidArgumentException("StorageV2::compatSelect: no projection for {$v1Table}");
        }

        $selects = ['v.created_at'];
        $joins = [];

        foreach ($map as $column => $kind) {
            $quoted = '"'.$column.'"';

            if (str_starts_with($kind, 'label:')) {
                $dictKind = substr($kind, 6);
                $alias = 'd_'.str_replace(':', '_', $column);
                $src = self::COMPAT_SRC[$v1Table][$column] ?? $column.'_id';
                $joins[$alias] = "LEFT JOIN nightowl_dict_string {$alias} ON {$alias}.id = v.{$src}";
                $selects[] = "{$alias}.value AS {$quoted}";

                continue;
            }

            $selects[] = match ($kind) {
                'ts' => "rtrim(rtrim(to_char(v.ts_us / 1000000.0, 'FM999999999999999990.999999'), '0'), '.') AS {$quoted}",
                'uuid' => "v.{$column}::text AS {$quoted}",
                'uuid_coalesce' => "COALESCE(v.trace_id, v.execution_id)::text AS {$quoted}",
                'hash' => "encode(v.{$column}, 'hex') AS {$quoted}",
                'sql' => match ($column) {
                    'sql_query' => "s.sql AS {$quoted}",
                    default => "s.{$column} AS {$quoted}",
                },
                'route' => 'dr."'.substr($column, 6)."\" AS {$quoted}",
                default => "v.{$column} AS {$quoted}",
            };

            if ($kind === 'sql') {
                $joins['s'] = 'LEFT JOIN nightowl_dict_sql s ON s.id = v.sql_id';
            }
            if ($kind === 'route') {
                $joins['dr'] = 'LEFT JOIN nightowl_dict_route dr ON dr.id = v.route_id';
            }
        }

        return 'SELECT '.implode(', ', $selects)
            .' FROM '.self::v2Name($v1Table).' v '
            .implode(' ', $joins)
            .($whereSql !== '' ? " WHERE {$whereSql}" : '');
    }

    /**
     * The FROM source for a rollup backfill chunk: the plain v1 scan, UNION
     * ALL'd with the v2 compat projection when the v2 twin exists. Both arms
     * carry the same WHERE (created_at range etc. — column names align by
     * construction). Caller supplies `?` placeholders; bindings must be
     * duplicated per arm in the same order.
     *
     * @return array{0: string, 1: int} [fromSql, armCount]
     */
    public static function unionFrom(PDO $pdo, string $v1Table, string $whereSql): array
    {
        $v1Exists = (bool) $pdo->query(
            "SELECT to_regclass('{$v1Table}') IS NOT NULL AS e"
        )->fetchColumn();
        $v2Exists = (bool) $pdo->query(
            "SELECT to_regclass('".self::v2Name($v1Table)."') IS NOT NULL AS e"
        )->fetchColumn();

        // Both arms must select the SAME ordered column list (UNION ALL
        // aligns by position): created_at + the compat map's v1 columns.
        $map = self::COMPAT[$v1Table] ?? [];
        $v1Cols = ['created_at', ...array_map(fn (string $c): string => '"'.$c.'"', array_keys($map))];
        $v1Arm = 'SELECT '.implode(', ', $v1Cols)." FROM {$v1Table}"
            .($whereSql !== '' ? " WHERE {$whereSql}" : '');

        $arms = [];
        if ($v1Exists) {
            $arms[] = $v1Arm;
        }
        if ($v2Exists && $map !== []) {
            $arms[] = self::compatSelect($v1Table, $whereSql);
        }

        if ($arms === []) {
            // Neither family exists — let the caller's query fail naturally.
            $arms[] = $v1Arm;
        }

        return ['('.implode(' UNION ALL ', $arms).') u', count($arms)];
    }
}
