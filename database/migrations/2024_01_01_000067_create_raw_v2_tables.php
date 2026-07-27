<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Storage format v2: a parallel family of raw telemetry parents, born in
     * their final shape. Compared to v1 (per row, at the measured production
     * tenant's shape): the varchar `timestamp` becomes ts_us bigint (event
     * time, µs); trace/execution/job/attempt ids become native uuid (16B vs
     * 37B text) with trace_id stored NULL when it equals execution_id (the
     * SDK duplicates them for request-sourced children — readers reconstruct
     * with COALESCE, losslessly); group_hash/fingerprint become 16-byte bytea;
     * repeated label strings become int ids into nightowl_dict_string
     * (000066); queries' sql/file/line collapse to sql_id → nightowl_dict_sql;
     * the request route tuple collapses to route_id → nightowl_dict_route
     * (append-only — renames create new entries, history never rewrites);
     * exception stack traces collapse to trace_ref → nightowl_dict_trace;
     * JSON blobs are agent-side-deflated bytea, NULL when they were only a
     * ''/'{}'/'null'/'[]' placeholder; `v` (constant wire version) is not
     * carried. Nothing else changes: counters, timings, sizes, url, key,
     * user_id, message/class columns stay inline. No information is dropped —
     * every v1 value is byte-recoverable from a v2 row plus the dictionaries.
     *
     * The partition key stays `created_at timestamp` so ALL of RawPartitions
     * (child sweep, heal, prune drop) runs unchanged against this family.
     *
     * Index set mirrors the post-diet v1 kept set exactly (as replayed by
     * 000058), translated: label columns → their _id twins, trace_id becomes
     * PARTIAL (WHERE trace_id IS NOT NULL — mostly NULL by design). Naming is
     * `{t}_v2_{cols}_idx` — the API's StaleTenantSchemaHint knows this
     * spelling.
     *
     * Sequence fencing: each v2 id sequence starts ABOVE the v1 twin's
     * MAX(id) (instant via the PK's leading column) so ids never collide
     * inside the API's v1+v2 union pages (cursor pagination keys on id).
     *
     * The LAST statement writes the cutover fence `v2_fence` into
     * nightowl_settings with ON CONFLICT DO NOTHING: re-runs and baseline
     * adoption never move an existing fence. Rows before the fence live in
     * v1, after it in v2; the API unions both while the window straddles.
     *
     * Everything here is catalog-only (empty tables) — safely inside the
     * boot-migrate deadline.
     */
    private const DAYS_AHEAD = 7;

    private const COLUMNS = [
        'nightowl_requests_v2' => '
            id bigserial NOT NULL,
            created_at timestamp(0) NOT NULL,
            ts_us bigint NOT NULL,
            trace_id uuid,
            group_hash bytea,
            environment_id integer,
            server_id integer,
            deploy_id integer,
            user_id varchar(255),
            method_id integer,
            route_id bigint,
            url text NOT NULL,
            ip varchar(255),
            duration bigint,
            status_code integer NOT NULL,
            request_size bigint,
            response_size bigint,
            bootstrap bigint,
            before_middleware bigint,
            action bigint,
            render bigint,
            after_middleware bigint,
            sending bigint,
            terminating bigint,
            exceptions integer NOT NULL DEFAULT 0,
            logs integer NOT NULL DEFAULT 0,
            queries integer NOT NULL DEFAULT 0,
            jobs_queued integer NOT NULL DEFAULT 0,
            mail integer NOT NULL DEFAULT 0,
            notifications integer NOT NULL DEFAULT 0,
            outgoing_requests integer NOT NULL DEFAULT 0,
            cache_events integer NOT NULL DEFAULT 0,
            peak_memory_usage bigint,
            exception_preview text,
            context_z bytea,
            headers_z bytea,
            payload_z bytea',
        'nightowl_queries_v2' => '
            id bigserial NOT NULL,
            created_at timestamp(0) NOT NULL,
            ts_us bigint NOT NULL,
            trace_id uuid,
            execution_id uuid,
            group_hash bytea,
            environment_id integer,
            server_id integer,
            deploy_id integer,
            execution_source_id integer,
            execution_stage_id integer,
            execution_preview varchar(255),
            user_id varchar(255),
            sql_id bigint,
            duration bigint,
            connection_id integer,
            connection_type_id integer',
        'nightowl_exceptions_v2' => '
            id bigserial NOT NULL,
            created_at timestamp(0) NOT NULL,
            ts_us bigint NOT NULL,
            trace_id uuid,
            execution_id uuid,
            group_hash bytea,
            environment_id integer,
            server_id integer,
            deploy_id integer,
            execution_source_id integer,
            execution_stage_id integer,
            execution_preview varchar(255),
            user_id varchar(255),
            class varchar(255) NOT NULL,
            message text,
            code varchar(255),
            file varchar(255),
            line integer,
            trace_ref bigint,
            php_version varchar(255),
            laravel_version varchar(255),
            handled boolean NOT NULL DEFAULT false,
            fingerprint bytea NOT NULL',
        'nightowl_commands_v2' => '
            id bigserial NOT NULL,
            created_at timestamp(0) NOT NULL,
            ts_us bigint NOT NULL,
            trace_id uuid,
            group_hash bytea,
            environment_id integer,
            server_id integer,
            deploy_id integer,
            user_id varchar(255),
            class varchar(255),
            name varchar(255),
            command text NOT NULL,
            exit_code integer,
            duration bigint,
            bootstrap bigint,
            action bigint,
            terminating bigint,
            exceptions integer NOT NULL DEFAULT 0,
            logs integer NOT NULL DEFAULT 0,
            queries integer NOT NULL DEFAULT 0,
            jobs_queued integer NOT NULL DEFAULT 0,
            mail integer NOT NULL DEFAULT 0,
            notifications integer NOT NULL DEFAULT 0,
            outgoing_requests integer NOT NULL DEFAULT 0,
            cache_events integer NOT NULL DEFAULT 0,
            peak_memory_usage bigint,
            exception_preview text,
            context_z bytea',
        'nightowl_jobs_v2' => '
            id bigserial NOT NULL,
            created_at timestamp(0) NOT NULL,
            ts_us bigint NOT NULL,
            trace_id uuid,
            execution_id uuid,
            group_hash bytea,
            environment_id integer,
            server_id integer,
            deploy_id integer,
            execution_source_id integer,
            execution_stage_id integer,
            execution_preview varchar(255),
            user_id varchar(255),
            job_id uuid,
            attempt_id uuid,
            attempt integer,
            job_class_id integer,
            queue_id integer,
            connection_id integer,
            status_id integer,
            duration bigint,
            attempts integer NOT NULL DEFAULT 1,
            exceptions integer NOT NULL DEFAULT 0,
            logs integer NOT NULL DEFAULT 0,
            queries integer NOT NULL DEFAULT 0,
            jobs_queued integer NOT NULL DEFAULT 0,
            mail integer NOT NULL DEFAULT 0,
            notifications integer NOT NULL DEFAULT 0,
            outgoing_requests integer NOT NULL DEFAULT 0,
            cache_events integer NOT NULL DEFAULT 0,
            peak_memory_usage bigint,
            exception_preview text,
            context_z bytea',
        'nightowl_cache_events_v2' => '
            id bigserial NOT NULL,
            created_at timestamp(0) NOT NULL,
            ts_us bigint NOT NULL,
            trace_id uuid,
            execution_id uuid,
            group_hash bytea,
            environment_id integer,
            server_id integer,
            deploy_id integer,
            execution_source_id integer,
            execution_stage_id integer,
            execution_preview varchar(255),
            user_id varchar(255),
            event_type_id integer,
            key varchar(255) NOT NULL,
            pattern_id integer,
            store_id integer,
            ttl integer,
            duration bigint',
        'nightowl_mail_v2' => '
            id bigserial NOT NULL,
            created_at timestamp(0) NOT NULL,
            ts_us bigint NOT NULL,
            trace_id uuid,
            execution_id uuid,
            group_hash bytea,
            environment_id integer,
            server_id integer,
            deploy_id integer,
            execution_source_id integer,
            execution_stage_id integer,
            execution_preview varchar(255),
            user_id varchar(255),
            mailer varchar(255),
            recipients text,
            cc integer NOT NULL DEFAULT 0,
            bcc integer NOT NULL DEFAULT 0,
            attachments integer NOT NULL DEFAULT 0,
            subject varchar(255),
            mailable varchar(255),
            duration bigint,
            failed boolean NOT NULL DEFAULT false,
            queued boolean NOT NULL DEFAULT false',
        'nightowl_notifications_v2' => '
            id bigserial NOT NULL,
            created_at timestamp(0) NOT NULL,
            ts_us bigint NOT NULL,
            trace_id uuid,
            execution_id uuid,
            group_hash bytea,
            environment_id integer,
            server_id integer,
            deploy_id integer,
            execution_source_id integer,
            execution_stage_id integer,
            execution_preview varchar(255),
            user_id varchar(255),
            notification varchar(255),
            channel_id integer,
            notifiable_type varchar(255),
            notifiable_id varchar(255),
            duration bigint,
            failed boolean NOT NULL DEFAULT false,
            queued boolean NOT NULL DEFAULT false',
        'nightowl_outgoing_requests_v2' => '
            id bigserial NOT NULL,
            created_at timestamp(0) NOT NULL,
            ts_us bigint NOT NULL,
            trace_id uuid,
            execution_id uuid,
            group_hash bytea,
            environment_id integer,
            server_id integer,
            deploy_id integer,
            execution_source_id integer,
            execution_stage_id integer,
            execution_preview varchar(255),
            user_id varchar(255),
            host_id integer,
            method_id integer,
            url text NOT NULL,
            status_code integer,
            duration bigint,
            request_size bigint,
            response_size bigint,
            headers_z bytea',
        'nightowl_scheduled_tasks_v2' => '
            id bigserial NOT NULL,
            created_at timestamp(0) NOT NULL,
            ts_us bigint NOT NULL,
            trace_id uuid,
            group_hash bytea,
            environment_id integer,
            server_id integer,
            deploy_id integer,
            user_id varchar(255),
            command text NOT NULL,
            expression varchar(255),
            timezone varchar(255),
            repeat_seconds integer NOT NULL DEFAULT 0,
            without_overlapping boolean NOT NULL DEFAULT false,
            on_one_server boolean NOT NULL DEFAULT false,
            run_in_background boolean NOT NULL DEFAULT false,
            even_in_maintenance_mode boolean NOT NULL DEFAULT false,
            status_id integer,
            duration bigint,
            exit_code integer,
            exceptions integer NOT NULL DEFAULT 0,
            logs integer NOT NULL DEFAULT 0,
            queries integer NOT NULL DEFAULT 0,
            jobs_queued integer NOT NULL DEFAULT 0,
            mail integer NOT NULL DEFAULT 0,
            notifications integer NOT NULL DEFAULT 0,
            outgoing_requests integer NOT NULL DEFAULT 0,
            cache_events integer NOT NULL DEFAULT 0,
            peak_memory_usage bigint,
            exception_preview text,
            context_z bytea',
        'nightowl_logs_v2' => '
            id bigserial NOT NULL,
            created_at timestamp(0) NOT NULL,
            ts_us bigint NOT NULL,
            trace_id uuid,
            execution_id uuid,
            environment_id integer,
            server_id integer,
            deploy_id integer,
            execution_source_id integer,
            execution_stage_id integer,
            execution_preview varchar(255),
            user_id varchar(255),
            level_id integer,
            message text,
            context_z bytea,
            extra_z bytea,
            channel_id integer',
    ];

    /**
     * Post-diet v1 kept set (as 000058 replays it), translated to v2 columns.
     * 'partial-trace' expands to the WHERE clause below.
     */
    private const INDEXES = [
        'nightowl_requests_v2' => [
            'created_at' => '(created_at)',
            'trace_id' => 'partial-trace',
            'group_hash_created_at' => '(group_hash, created_at)',
            'environment_id' => '(environment_id)',
            'status_code' => '(status_code)',
            'user_id_created_at' => '(user_id, created_at)',
        ],
        'nightowl_queries_v2' => [
            'created_at' => '(created_at)',
            'trace_id' => 'partial-trace',
            'execution_id' => '(execution_id)',
            'group_hash_created_at' => '(group_hash, created_at)',
            'environment_id' => '(environment_id)',
        ],
        'nightowl_exceptions_v2' => [
            'created_at' => '(created_at)',
            'trace_id' => 'partial-trace',
            'execution_id' => '(execution_id)',
            'class' => '(class)',
            'fingerprint_created_at' => '(fingerprint, created_at)',
            'environment_id' => '(environment_id)',
            'user_id_created_at' => '(user_id, created_at)',
        ],
        'nightowl_commands_v2' => [
            'created_at' => '(created_at)',
            'trace_id' => 'partial-trace',
            'group_hash_created_at' => '(group_hash, created_at)',
            'environment_id' => '(environment_id)',
        ],
        'nightowl_jobs_v2' => [
            'created_at' => '(created_at)',
            'trace_id' => 'partial-trace',
            'execution_id' => '(execution_id)',
            'job_id' => '(job_id)',
            'attempt_id' => '(attempt_id)',
            'group_hash_created_at' => '(group_hash, created_at)',
            'status_id' => '(status_id)',
            'environment_id' => '(environment_id)',
            'user_id_created_at' => '(user_id, created_at)',
        ],
        'nightowl_cache_events_v2' => [
            'created_at' => '(created_at)',
            'trace_id' => 'partial-trace',
            'execution_id' => '(execution_id)',
            'created_at_user_id' => '(created_at, user_id)',
            'event_type_id' => '(event_type_id)',
            'environment_id' => '(environment_id)',
        ],
        'nightowl_mail_v2' => [
            'created_at' => '(created_at)',
            'trace_id' => 'partial-trace',
            'execution_id' => '(execution_id)',
            'environment_id' => '(environment_id)',
        ],
        'nightowl_notifications_v2' => [
            'created_at' => '(created_at)',
            'trace_id' => 'partial-trace',
            'execution_id' => '(execution_id)',
            'environment_id' => '(environment_id)',
        ],
        'nightowl_outgoing_requests_v2' => [
            'created_at' => '(created_at)',
            'trace_id' => 'partial-trace',
            'execution_id' => '(execution_id)',
            'group_hash_created_at' => '(group_hash, created_at)',
            'created_at_user_id' => '(created_at, user_id)',
            'environment_id' => '(environment_id)',
        ],
        'nightowl_scheduled_tasks_v2' => [
            'created_at' => '(created_at)',
            'trace_id' => 'partial-trace',
            'group_hash_created_at' => '(group_hash, created_at)',
            'environment_id' => '(environment_id)',
        ],
        'nightowl_logs_v2' => [
            'created_at' => '(created_at)',
            'trace_id' => 'partial-trace',
            'execution_id' => '(execution_id)',
            'created_at_user_id' => '(created_at, user_id)',
            'level_id' => '(level_id)',
            'environment_id' => '(environment_id)',
        ],
    ];

    public function up(): void
    {
        $schema = Schema::connection($this->connection);
        $conn = DB::connection($this->connection);

        // The dictionaries (000066) and settings (000016) must exist; under
        // baseline adoption they always do by the time this file runs
        // (filename ordering).
        foreach (self::COLUMNS as $table => $columns) {
            if ($schema->hasTable($table)) {
                continue;
            }

            $conn->statement("CREATE TABLE {$table} ({$columns}) PARTITION BY RANGE (created_at)");
            $conn->statement("ALTER TABLE {$table} ADD PRIMARY KEY (id, created_at)");

            foreach (self::INDEXES[$table] as $suffix => $def) {
                $name = "{$table}_{$suffix}_idx";
                if ($def === 'partial-trace') {
                    $conn->statement("CREATE INDEX {$name} ON {$table} (trace_id) WHERE trace_id IS NOT NULL");
                } else {
                    $conn->statement("CREATE INDEX {$name} ON {$table} {$def}");
                }
            }

            // Start the v2 id sequence above the v1 twin's MAX(id): the API
            // pages v1+v2 unions with id as the cursor tiebreak, so ids must
            // never collide across the families. MAX(id) is instant via the
            // v1 PK's leading column; a missing/empty v1 twin leaves the
            // sequence at 1.
            $v1 = substr($table, 0, -3);
            if ($schema->hasTable($v1)) {
                $conn->statement(
                    "SELECT setval(pg_get_serial_sequence('{$table}', 'id'),
                                   COALESCE((SELECT MAX(id) FROM {$v1}), 0) + 1,
                                   false)"
                );
            }

            $conn->statement("CREATE TABLE {$table}_pdefault PARTITION OF {$table} DEFAULT");

            $today = intdiv(time(), 86400) * 86400;
            for ($d = 0; $d <= self::DAYS_AHEAD; $d++) {
                $day = $today + $d * 86400;
                $child = $table.'_p'.gmdate('Ymd', $day);
                $from = gmdate('Y-m-d 00:00:00', $day);
                $to = gmdate('Y-m-d 00:00:00', $day + 86400);
                $conn->statement("CREATE TABLE IF NOT EXISTS {$child} PARTITION OF {$table} FOR VALUES FROM ('{$from}') TO ('{$to}')");
            }
        }

        // Cutover fence — written once, never moved by re-runs or baseline
        // adoption. Rows created before this instant live in v1, at/after it
        // in v2; the API's dual-read unions both while the window straddles.
        if ($schema->hasTable('nightowl_settings')) {
            $now = gmdate('Y-m-d H:i:s');
            $conn->statement(
                "INSERT INTO nightowl_settings (key, value, created_at, updated_at)
                 VALUES ('v2_fence', ?, now(), now())
                 ON CONFLICT (key) DO NOTHING",
                [$now],
            );
        }
    }

    public function down(): void
    {
        $conn = DB::connection($this->connection);

        foreach (array_keys(self::COLUMNS) as $table) {
            $conn->statement("DROP TABLE IF EXISTS {$table}");
        }

        $conn->statement("DELETE FROM nightowl_settings WHERE key = 'v2_fence'");
    }
};
