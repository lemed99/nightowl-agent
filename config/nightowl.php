<?php

return [

    /*
    |--------------------------------------------------------------------------
    | Master Switch
    |--------------------------------------------------------------------------
    |
    | Set NIGHTOWL_ENABLED=false to make the package fully inert: the agent's
    | Nightwatch ingest hook is not wired (no telemetry is collected or
    | transmitted) and the migrations are not registered. Use this to turn
    | NightOwl off in environments where you don't want it — most commonly the
    | `testing` environment, so your unit/feature tests don't pay the ingest
    | overhead or require the `nightowl` database to exist.
    |
    | Read via env() at config-load so `php artisan config:cache` is safe.
    |
    */
    'enabled' => (bool) env('NIGHTOWL_ENABLED', true),

    /*
    |--------------------------------------------------------------------------
    | Migration Registration (legacy ride-along)
    |--------------------------------------------------------------------------
    |
    | NightOwl's tables live in your (BYO) `nightowl` PostgreSQL connection, and
    | the schema is managed by `php artisan nightowl:install` /
    | `php artisan nightowl:migrate`. Those commands track migration history
    | INSIDE the nightowl database, so they're idempotent across every
    | environment that shares that database — run them on each deploy and the
    | first creates the tables, the rest are no-ops.
    |
    | Set NIGHTOWL_RUN_MIGRATIONS=true to ALSO register the migrations with your
    | app, so they run as part of `php artisan migrate`. This is the legacy
    | behavior: it records history against your app's PRIMARY database, which
    | (a) breaks when several environments share one nightowl database — each
    | has its own empty history and re-creates the tables — and (b) must not be
    | combined with `nightowl:install`, since the two track history in different
    | places and would both try to create the tables. Only enable it for a
    | single-database setup where you'd rather not run `nightowl:migrate`.
    |
    */
    'run_migrations' => (bool) env('NIGHTOWL_RUN_MIGRATIONS', false),

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
        'sslmode' => env('NIGHTOWL_DB_SSLMODE', 'prefer'),
        'retention_days' => env('NIGHTOWL_RETENTION_DAYS', 14),
        // Query rollups (nightowl_query_rollups) are tiny pre-aggregated
        // summaries, so they're kept far longer than raw telemetry — this is
        // what powers long-range trend charts without retaining raw rows.
        'rollup_retention_days' => env('NIGHTOWL_ROLLUP_RETENTION_DAYS', 90),
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
        // NightOwl SaaS API base URL — destination for health reports. Override
        // for self-hosted dashboards or staging environments.
        'api_url' => env('NIGHTOWL_API_URL', 'https://api.usenightowl.com'),
        // NightOwl dashboard / frontend base URL — used in alert links, email
        // logos, and CLI output. Override for self-hosted dashboards.
        'dashboard_url' => env('NIGHTOWL_DASHBOARD_URL', 'https://usenightowl.com'),
        // NIGHTWATCH_TOKEN is a deprecated fallback for installs that pre-date
        // the rename — new installs should use NIGHTOWL_TOKEN.
        'token' => env('NIGHTOWL_TOKEN', env('NIGHTWATCH_TOKEN')),

        // Platform app ID for this connected app — shown in the NightOwl
        // dashboard under Settings. When set, alert emails and webhooks
        // include a direct-link `view_url` pointing at the issue. Without
        // it, links fall back to the generic dashboard root.
        'app_id' => env('NIGHTOWL_APP_ID'),
        'driver' => env('NIGHTOWL_AGENT_DRIVER', 'async'),
        'sqlite_path' => env('NIGHTOWL_AGENT_SQLITE_PATH', storage_path('nightowl/agent-buffer.sqlite')),
        'drain_interval_ms' => env('NIGHTOWL_DRAIN_INTERVAL_MS', 100),
        'drain_batch_size' => env('NIGHTOWL_DRAIN_BATCH_SIZE', 5000),
        'drain_workers' => env('NIGHTOWL_DRAIN_WORKERS', 1),
        // Poison-row isolation (Phase 2): when true, a batch failing with a
        // row-level data error (SQLSTATE class 22/23) is bisected to quarantine
        // the offending payload so the rest drain, instead of head-of-line
        // blocking. Quarantined rows are dropped after the retention window and
        // surfaced as the DRAIN_QUARANTINE health diagnosis. Off by default.
        'drain_quarantine_enabled' => env('NIGHTOWL_DRAIN_QUARANTINE', false),
        'max_pending_rows' => env('NIGHTOWL_MAX_PENDING_ROWS', 100_000),
        'max_buffer_memory' => env('NIGHTOWL_MAX_BUFFER_MEMORY', 256 * 1024 * 1024),

        // UDP protocol (fire-and-forget, no ACK)
        'enable_udp' => env('NIGHTOWL_ENABLE_UDP', false),
        'udp_port' => env('NIGHTOWL_UDP_PORT', 2408),

        // Time-based flush (max ms between drains during low traffic)
        'drain_max_wait_ms' => env('NIGHTOWL_DRAIN_MAX_WAIT_MS', 5000),

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

        // Remote health reporting to api
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

    /*
    |--------------------------------------------------------------------------
    | Auto-Reopen Cooldown (hours)
    |--------------------------------------------------------------------------
    |
    | When a fingerprint with status='resolved' recurs in a drain batch, the
    | agent flips it back to 'open' and fires an `issue.reopened` alert. Use
    | this cooldown to suppress that flip when a recurrence arrives within N
    | hours of the resolve action (anti-flapping). 0 = always reopen on first
    | recurrence (Sentry-style).
    |
    | Issues with status='ignored' are never auto-reopened, regardless of this
    | setting — "ignored" means the user explicitly asked to silence them.
    |
    */
    'reopen_cooldown_hours' => (int) env('NIGHTOWL_REOPEN_COOLDOWN_HOURS', 0),

];
