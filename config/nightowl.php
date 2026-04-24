<?php

return [

    /*
    |--------------------------------------------------------------------------
    | Database
    |--------------------------------------------------------------------------
    |
    | NightOwl uses PostgreSQL to store monitoring data. Configure your
    | database connection here, or use the included docker-compose.yml
    | for a quick local setup.
    |
    */
    'database' => [
        'host' => env('NIGHTOWL_DB_HOST', '127.0.0.1'),
        'port' => env('NIGHTOWL_DB_PORT', 5432),
        'database' => env('NIGHTOWL_DB_DATABASE', 'nightowl'),
        'username' => env('NIGHTOWL_DB_USERNAME', 'nightowl'),
        'password' => env('NIGHTOWL_DB_PASSWORD', 'nightowl'),
        'retention_days' => env('NIGHTOWL_RETENTION_DAYS', 14),
    ],

    /*
    |--------------------------------------------------------------------------
    | Agent
    |--------------------------------------------------------------------------
    |
    | The TCP agent listens for payloads from laravel/nightwatch and writes
    | them to the database.
    |
    */
    'agent' => [
        'host' => env('NIGHTOWL_AGENT_HOST', '127.0.0.1'),
        'port' => env('NIGHTOWL_AGENT_PORT', 2407),
        'token' => env('NIGHTOWL_TOKEN', env('NIGHTWATCH_TOKEN')),

        // Platform app ID for this connected app — shown in the NightOwl
        // dashboard under Settings. When set, the agent embeds it in webhook
        // payloads (alongside view_url) so receivers can round-trip back to
        // the dashboard. Without it, alert webhooks still fire but ship
        // app_id=null and omit the direct-link view_url.
        'app_id' => env('NIGHTOWL_APP_ID'),
        'driver' => env('NIGHTOWL_AGENT_DRIVER', 'async'),
        'sqlite_path' => env('NIGHTOWL_AGENT_SQLITE_PATH', storage_path('nightowl/agent-buffer.sqlite')),
        'drain_interval_ms' => env('NIGHTOWL_DRAIN_INTERVAL_MS', 100),
        'drain_batch_size' => env('NIGHTOWL_DRAIN_BATCH_SIZE', 5000),
        'drain_workers' => env('NIGHTOWL_DRAIN_WORKERS', 1),
        'max_pending_rows' => env('NIGHTOWL_MAX_PENDING_ROWS', 100_000),
        'max_buffer_memory' => env('NIGHTOWL_MAX_BUFFER_MEMORY', 256 * 1024 * 1024),

        // UDP protocol (fire-and-forget, no ACK)
        'enable_udp' => env('NIGHTOWL_ENABLE_UDP', false),
        'udp_port' => env('NIGHTOWL_UDP_PORT', 2408),

        // Intelligent sampling (1.0 = keep all, 0.5 = ~50% drop, exceptions always kept)
        // Per-type rates override the global rate for their entry point type.
        // If not set, the global sample_rate applies to all types.
        'sample_rate' => env('NIGHTOWL_SAMPLE_RATE', 1.0),
        'request_sample_rate' => env('NIGHTOWL_REQUEST_SAMPLE_RATE'),
        'command_sample_rate' => env('NIGHTOWL_COMMAND_SAMPLE_RATE'),
        'scheduled_task_sample_rate' => env('NIGHTOWL_SCHEDULED_TASK_SAMPLE_RATE'),

        // Time-based flush (max ms between drains during low traffic)
        'drain_max_wait_ms' => env('NIGHTOWL_DRAIN_MAX_WAIT_MS', 5000),

        // PII redaction — scrubs sensitive keys from payloads before they
        // land in SQLite. Also redacts query-string parameters matching
        // these names inside any `url`/`uri`/`endpoint`/`href` field.
        // Enabled by default: if you have a legitimate reason to capture
        // credentials in telemetry (unlikely), set NIGHTOWL_REDACT_ENABLED=false.
        'redact_enabled' => env('NIGHTOWL_REDACT_ENABLED', true),
        'redact_keys' => array_filter(
            explode(',', env('NIGHTOWL_REDACT_KEYS', 'password,token,authorization,cookie,secret,api_key'))
        ),

        // Gzip decompression for compressed payloads
        'gzip_enabled' => env('NIGHTOWL_GZIP_ENABLED', true),

        // Debug: dump every decoded payload as JSONL for upstream record-type
        // inspection. DO NOT enable in production — writes on every ingest.
        // Used to answer "does laravel/nightwatch emit per-event records for
        // lazy_load, hydrated_model, file_read, file_write?" — grep the dump
        // after hitting a known scenario. Defaults to off.
        'debug_raw_payloads' => (bool) env('NIGHTOWL_DEBUG_RAW_PAYLOADS', false),
        'debug_raw_payloads_path' => env(
            'NIGHTOWL_DEBUG_RAW_PAYLOADS_PATH',
            storage_path('nightowl/raw-payloads.jsonl'),
        ),

        // Health & status API
        'health_enabled' => env('NIGHTOWL_HEALTH_ENABLED', true),
        'health_port' => env('NIGHTOWL_HEALTH_PORT', 2409),

        // Remote health reporting to dashboard
        'health_report_enabled' => env('NIGHTOWL_HEALTH_REPORT_ENABLED', true),
        'health_report_interval' => env('NIGHTOWL_HEALTH_REPORT_INTERVAL', 30),

        // Adaptive reporting intervals (override individual levels).
        // If not set, falls back to health_report_interval for all levels.
        'health_report_intervals' => array_filter([
            'healthy' => env('NIGHTOWL_HEALTH_INTERVAL_HEALTHY'),
            'degraded' => env('NIGHTOWL_HEALTH_INTERVAL_DEGRADED'),
            'critical' => env('NIGHTOWL_HEALTH_INTERVAL_CRITICAL'),
        ]),
    ],

    /*
    |--------------------------------------------------------------------------
    | Nightwatch Coexistence
    |--------------------------------------------------------------------------
    |
    | When `parallel_with_nightwatch` is true, the service provider keeps
    | Nightwatch's hosted-agent ingest AND fans out every record to the
    | NightOwl agent. Use this during migration to compare dashboards.
    | When false (default), the Nightwatch collector is redirected entirely
    | at the NightOwl agent — Laravel Cloud receives nothing.
    |
    */
    'parallel_with_nightwatch' => (bool) env('NIGHTOWL_PARALLEL_WITH_NIGHTWATCH', false),

    /*
    |--------------------------------------------------------------------------
    | Environment Override
    |--------------------------------------------------------------------------
    |
    | Stamped on every telemetry row as the `environment` column, and used
    | in the issue dedup key (group_hash, type, environment). Leave null to
    | fall back to APP_ENV — set this only for rare cases like standalone
    | harnesses running outside the host Laravel app, or when you want a
    | custom label like "prod-us-east". Pulled in via env() at config-load
    | so `php artisan config:cache` preserves it.
    |
    */
    'environment' => env('NIGHTOWL_ENVIRONMENT'),

    /*
    |--------------------------------------------------------------------------
    | Performance Thresholds
    |--------------------------------------------------------------------------
    |
    | The agent reads threshold settings from the nightowl_settings table
    | and caches them locally. When a record's duration exceeds a matching
    | threshold, a performance issue is created in nightowl_issues.
    |
    | The cache TTL controls the maximum lifetime of the threshold cache.
    | In addition, the agent polls the updated_at column every 30 seconds
    | to detect dashboard-side changes, so new thresholds take effect
    | within ~30s without restarting the agent.
    |
    */
    'threshold_cache_ttl' => env('NIGHTOWL_THRESHOLD_CACHE_TTL', 86400),

];
