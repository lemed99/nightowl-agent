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
    | Auto-Migrate at Daemon Startup
    |--------------------------------------------------------------------------
    |
    | When the agent daemon starts and the nightowl schema is behind the
    | package's migrations, run `nightowl:migrate` automatically (schema part
    | synchronously before ingest starts; rollup backfill reconciliation in a
    | detached background process), so `composer update nightowl/agent` plus
    | your usual restart is the whole upgrade — no separate migrate step.
    |
    | Set false when the agent's DB role lacks DDL rights — the agent then
    | warns (pre-existing behavior) and you run nightowl:migrate yourself.
    | Ignored under NIGHTOWL_RUN_MIGRATIONS=true (the host app's migrate owns
    | the schema there). Failures never block startup: warn-and-continue, and
    | the schema run is bounded by auto_migrate_timeout — it executes in a
    | child process that is killed at the deadline (a lock wait on the tenant
    | DB must never keep the ingest port unbound), leaving the migrations
    | pending for the next boot to retry.
    |
    */
    'auto_migrate' => (bool) env('NIGHTOWL_AUTO_MIGRATE', true),
    // TOP-LEVEL on purpose (shallow-merge rule — see 'drain_connection').
    'auto_migrate_timeout' => (int) env('NIGHTOWL_AUTO_MIGRATE_TIMEOUT', 300),

    /*
    |--------------------------------------------------------------------------
    | Auto-Backfill of Rollups at Daemon Startup
    |--------------------------------------------------------------------------
    |
    | The second half of the boot upgrade: after the schema run lands, spawn a
    | detached `nightowl:migrate` to populate whatever rollup tables it left
    | existing-but-empty and to repair incomplete tiers. On by default, because
    | the alternative is worse than slow charts — the API read path prefers ANY
    | rollup table that exists over raw, so a table that exists and is empty
    | serves ZEROS, and nothing but this pass ever fills it.
    |
    | Set false when you would rather own that pass yourself — a very large
    | tenant, a maintenance window, or a tenant PG you don't want carrying the
    | aggregation while the daemon is also draining. The daemon then warns once
    | per boot with the tables that are still empty, and you run
    | `php artisan nightowl:migrate` (or `nightowl:backfill-rollups`) by hand.
    | Charts over those tables read zero until you do.
    |
    | Backfill chunks are paced to hold each rollup table's advisory lock for
    | ~1s regardless of tenant size (BackfillRollupsCommand::TARGET_CHUNK_SECONDS),
    | so leaving this on does not wedge the live drain. It did before that
    | pacing existed — see the constant's docblock.
    |
    */
    // TOP-LEVEL on purpose (shallow-merge rule — see 'drain_connection').
    'auto_backfill' => (bool) env('NIGHTOWL_AUTO_BACKFILL', true),

    /*
    |--------------------------------------------------------------------------
    | Cache Key Templating
    |--------------------------------------------------------------------------
    |
    | Group the cache rollup by key SHAPE instead of key instance: uuid/ulid/
    | hex/int/email/datetime segments collapse to placeholders at drain time
    | (`user:8213:profile` → `user:{int}:profile`), bounding the rollup's
    | cardinality for machine-generated key families. A bare 40-character
    | alphanumeric token carrying an upper, a lower and a digit collapses to
    | `{session}` — Laravel's own definition of a session id, which a redis /
    | memcached / apc / array SESSION_DRIVER turns into one cache key per
    | visitor per request. The rule is hardcoded
    | (NightOwl\Support\CacheKeyTemplate) — this is a kill switch for a tenant
    | the rule hurts, not a pattern DSL.
    |
    | Off, NEW keys group literally — but rollup rows written while it was ON
    | keep their pattern grouping (history is never rewritten), so a key's
    | history spans two groups across the flip, converging as the patterned
    | rows age out of rollup retention. Raw cache events always keep the
    | literal key either way. TOP-LEVEL key (shallow-merge rule).
    |
    */
    'cache_key_template' => (bool) env('NIGHTOWL_CACHE_KEY_TEMPLATE', true),

    /*
    |--------------------------------------------------------------------------
    | Storage Format v2
    |--------------------------------------------------------------------------
    |
    | The operational kill switch for the v2 raw-telemetry format (dictionary
    | ids, native uuid/bytea, deflated blobs — migrations 000066/000067).
    | With the v2 tables present the drain writes v2; flipping this to false
    | reverts it to the v1 tables WITHOUT any schema change, and the API's
    | dual-read keeps every row visible either way. Top-level key on purpose:
    | mergeConfigFrom() is a shallow array_merge, so a key nested under
    | 'database' would be swallowed by any published config.
    |
    */

    'storage_v2' => (bool) env('NIGHTOWL_STORAGE_V2', true),

    /*
    |--------------------------------------------------------------------------
    | Trace-dictionary GC
    |--------------------------------------------------------------------------
    |
    | nightowl_dict_trace is the one dictionary that can be reclaimed: a
    | deflated stack trace is only ever pointed at by exception rows, which age
    | out at retention, so once every referencing exception is pruned the trace
    | is dead weight (the other three dicts hold unbounded-lifetime keys and are
    | never pruned). nightowl:gc-dict-traces deletes traces that are BOTH
    | unreferenced by any nightowl_exceptions_v2 row AND older than the
    | quarantine window below.
    |
    | The quarantine is not a correctness knob — the drain bumps a trace's
    | created_at on every reference, so an in-flight batch can never race the
    | GC (see 000068). It only controls how long an already-unreferenced trace
    | lingers before reclamation; a week keeps recently-hot traces around for a
    | recurrence without re-deflating them. TOP-LEVEL key (shallow-merge rule).
    |
    */
    'dict_trace_gc' => [
        'quarantine_days' => (int) env('NIGHTOWL_DICT_TRACE_GC_QUARANTINE_DAYS', 7),
    ],

    /*
    |--------------------------------------------------------------------------
    | Table Statistics Reporting
    |--------------------------------------------------------------------------
    |
    | Hourly per-table catalog statistics (row counts, byte sizes, scan and
    | write counters, rollup bucket bounds) reported to the NightOwl platform
    | so operational problems — a wedged drain, a rollup coverage hole, a
    | disk filling up — are diagnosed from our side without ever asking you
    | to run queries. Reads catalog/statistics views ONLY; structurally
    | incapable of reading telemetry contents. Disclosed in the NightOwl
    | privacy policy; set false to opt out entirely. TOP-LEVEL keys
    | (shallow-merge rule).
    |
    */
    'table_stats' => (bool) env('NIGHTOWL_TABLE_STATS', true),
    'table_stats_interval' => (int) env('NIGHTOWL_TABLE_STATS_INTERVAL', 3600),

    /*
    |--------------------------------------------------------------------------
    | Update Check (warn only)
    |--------------------------------------------------------------------------
    |
    | Deliberately a TOP-LEVEL key (see 'drain_connection' below for why new
    | groups never nest under 'database').
    |
    | A running daemon can never pick up a `composer update` on its own: the
    | loaded code and Composer's version metadata are frozen in-process. Worse,
    | drain workers respawn as fresh interpreters, so after an update a
    | respawned worker runs NEW code under an OLD parent — a combination
    | nothing tests. The daemon polls vendor/composer/installed.php and logs a
    | warning once it sees a newer version on disk (confirmed on two
    | consecutive polls, so a half-written vendor tree never triggers it).
    |
    | It only WARNS. Restarting the agent is your call, made by whatever
    | already restarts your processes — the agent deliberately does not exit on
    | its own, because whether a supervisor would bring it back depends on
    | configuration the agent cannot verify, and being wrong about that leaves
    | the agent down instead of merely stale.
    |
    | Async driver only (the sync fallback has no event loop to poll on).
    |
    */
    'update_check' => [
        'enabled' => (bool) env('NIGHTOWL_UPDATE_CHECK', true),
        'poll_seconds' => (int) env('NIGHTOWL_UPDATE_CHECK_POLL_SECONDS', 300),
    ],

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
        // Rollups are tiny pre-aggregated summaries, so they're kept far longer
        // than raw telemetry — this is what powers long-range trend charts
        // without retaining raw rows. Applies to the minute-granular tables.
        // (Hour/day tier retentions are TOP-LEVEL 'rollup_tier_retention' — a
        // published config's 'database' array replaces this one wholesale under
        // mergeConfigFrom's shallow merge, which would swallow new sub-keys.)
        'rollup_retention_days' => env('NIGHTOWL_ROLLUP_RETENTION_DAYS', 90),
    ],

    /*
    |--------------------------------------------------------------------------
    | Drain Connection — network timeouts
    |--------------------------------------------------------------------------
    |
    | Deliberately a TOP-LEVEL key, not part of 'database'. mergeConfigFrom() is a
    | shallow array_merge, so an app that ran `vendor:publish` has a 'database'
    | array that wholly REPLACES the package's — any new sub-key there would be
    | silently invisible to it, and so would its env var. A new top-level key is
    | preserved by array_merge for exactly the same reason.
    |
    | Without these, a TCP stall on the drain write path is bounded only by the
    | kernel's tcp_retries2 (~15 min at the default), during which the drain is
    | wedged, the buffer fills to max_pending_rows and ingest starts REFUSING
    | payloads with '5:ERROR'.
    |
    */
    'drain_connection' => [
        // Master switch. false restores pre-1.2.14 NETWORK behaviour exactly: no
        // socket options, no ATTR_TIMEOUT (PDO's 30s default returns), DSN
        // connect_timeout=5. It does NOT revert the SET LOCAL scoping or the
        // HY000 classification — those are bug fixes, not tunables.
        'timeouts_enabled' => (bool) env('NIGHTOWL_DRAIN_CONN_TIMEOUTS', true),

        // Connect bound. The pre-1.2.14 30s was an accident of PDO::ATTR_TIMEOUT's
        // default; the DSN's connect_timeout never governed connect, because
        // PDO_PGSQL appends its own derived from ATTR_TIMEOUT and libpq is
        // last-key-wins. Never 0 — that hangs unbounded.
        'connect_timeout' => (int) env('NIGHTOWL_DB_CONNECT_TIMEOUT', 10),

        // Bounds a SEND-BLOCKED socket (drain mid-COPY). Counts only UNACKED time,
        // so it cannot fire on a healthy-but-slow link. Measured against a true
        // iptables partition: 5000 -> 5.22s, 20000 -> 20.32s, 40000 -> 40.64s;
        // without it the same stall was still wedged at 111s.
        // Requires libpq >= 12 — feature-detected, never concatenated on faith.
        // 0 disables just this param.
        'tcp_user_timeout_ms' => (int) env('NIGHTOWL_DB_TCP_USER_TIMEOUT_MS', 20000),

        // Bounds an IDLE-READ socket (awaiting a result), where tcp_user_timeout is
        // inert because nothing is unacked. idle + interval*count = 25s. libpq's own
        // default idle is 7200s, i.e. no protection.
        'keepalives_idle' => (int) env('NIGHTOWL_DB_KEEPALIVES_IDLE', 10),
        'keepalives_interval' => (int) env('NIGHTOWL_DB_KEEPALIVES_INTERVAL', 5),
        'keepalives_count' => (int) env('NIGHTOWL_DB_KEEPALIVES_COUNT', 3),

        // Caps a blocked ON CONFLICT upsert (issues / rollups / users), which
        // otherwise waits INDEFINITELY. A socket deadline cannot bound a lock wait —
        // the connection is healthy and keepalives are answered throughout. Raises
        // 55P03, which DrainWorker::isTransientFailure() already defers. 0 disables.
        'lock_timeout_ms' => (int) env('NIGHTOWL_DB_LOCK_TIMEOUT_MS', 10000),

        // Diagnosis-only wedge threshold (no kill in this release). 0 disables.
        'wedge_warn_seconds' => (int) env('NIGHTOWL_DRAIN_WEDGE_WARN_SECONDS', 180),

        // Orphan reaper: SET LOCAL idle_in_transaction_session_timeout on every
        // drain transaction, so a batch this process abandons mid-transaction (its
        // server-side session surviving behind a pooler, holding uncommitted
        // unique-index entries the retry then collides with — 55P03) is reaped by
        // Postgres itself. Scoped to the agent's own transactions; other apps on
        // the customer's database are untouched. Cannot fire on a healthy drain:
        // counts only idle-between-statements time (~ms), never statement runtime.
        // 0 disables.
        'idle_txn_timeout_ms' => (int) env('NIGHTOWL_DB_IDLE_TXN_TIMEOUT_MS', 30000),
    ],

    /*
    |--------------------------------------------------------------------------
    | Rollup tier retention
    |--------------------------------------------------------------------------
    |
    | TOP-LEVEL keys on purpose: mergeConfigFrom is a shallow array_merge, so a
    | previously-published config's 'database' array would swallow any new
    | sub-key added there — along with its env var.
    |
    | Hour/day rollup tiers are 60×/1440× sparser than the minute rollups, so
    | they keep history far past the minute tier's retention cutoff.
    */
    'rollup_tier_retention' => [
        'hourly_days' => (int) env('NIGHTOWL_HOURLY_ROLLUP_RETENTION_DAYS', 366),
        'daily_days' => (int) env('NIGHTOWL_DAILY_ROLLUP_RETENTION_DAYS', 1100),
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
        // Where the AGENT BINDS/LISTENS. Leave at loopback for a co-located
        // agent; set to 0.0.0.0 (or an LB VIP) to accept payloads from other
        // hosts. This is the listener address — NOT where your app transmits.
        'host' => env('NIGHTOWL_AGENT_HOST', '127.0.0.1'),
        'port' => env('NIGHTOWL_AGENT_PORT', 2407),

        // Where the INSTRUMENTED APP TRANSMITS telemetry — the agent's address
        // as seen from your app. Defaults to the loopback listener, matching
        // the co-located single-host deployment. Set it to the agent's private
        // host:port when the app and agent run on different machines — the
        // supported path for serverless hosts that can't run the long-lived
        // agent in-process. On Laravel Vapor, AWS Lambda is stateless and
        // short-lived, so you run the agent on a long-running box (EC2/Forge)
        // in the same VPC and point the Vapor app at it:
        //   NIGHTOWL_INGEST_URI=10.0.0.5:2407
        // Mirrors laravel/nightwatch's NIGHTWATCH_INGEST_URI. A bare host with
        // no port falls back to NIGHTOWL_AGENT_PORT. host:port, no scheme;
        // bracket IPv6 literals as [::1]:2407.
        //
        // Left unset (null) by default on purpose: the service provider then
        // derives the loopback default from the RESOLVED agent.port config, so
        // an install that sets the port via the published config file (not the
        // NIGHTOWL_AGENT_PORT env var) still transmits to the port the agent
        // actually binds. Hardcoding env('NIGHTOWL_AGENT_PORT') here would send
        // to 2407 while the listener bound the config port — silent record loss.
        'ingest_uri' => env('NIGHTOWL_INGEST_URI'),

        // Connect + write timeout (seconds) for transmitting to the agent. The
        // 0.5s default is tuned for a loopback agent; raise it when the agent
        // is a network hop away (e.g. Vapor → EC2) so a slower connect doesn't
        // silently drop records.
        'ingest_timeout' => env('NIGHTOWL_INGEST_TIMEOUT', 0.5),
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

    /*
    |--------------------------------------------------------------------------
    | Agent SMTP
    |--------------------------------------------------------------------------
    |
    | The agent sends its own email alerts (issue.new, issue.reopened, agent
    | health) straight from the drain worker over a raw socket — it cannot use
    | your app's mailer, because it runs in a forked child with no container.
    | The SMTP credentials themselves are NOT here: they live per-app in the
    | nightowl_alert_channels table, set from the dashboard. These are the
    | knobs for how the agent speaks the protocol.
    |
    | A TOP-LEVEL key, not nested under an existing array: mergeConfigFrom is a
    | shallow array_merge, so a published config predating this file would
    | replace a parent array wholesale and swallow anything added inside it.
    |
    | Verify the whole path with `php artisan nightowl:test-alert` — the
    | dashboard's own "send test" button goes through NightOwl's API, not
    | through this client, so it cannot tell you whether the agent can send.
    |
    */
    'smtp' => [
        // Hostname used in the SMTP HELO/EHLO greeting. Leave null and the
        // agent uses the machine's FQDN, falling back to an address literal
        // (e.g. "[10.0.1.7]") when the host has no dotted name — common in
        // containers. Set this only if your relay demands a specific name;
        // strict relays (Exchange/Office 365, Postfix with
        // reject_non_fqdn_helo_hostname) refuse anything that isn't one.
        'helo' => env('NIGHTOWL_SMTP_HELO'),

        // Seconds to wait for the TCP connection, and for each reply after it.
        // Raise for a slow or distant relay. Both are additionally capped by
        // the caller's dispatch budget, so a hung relay can't stall the drain.
        'connect_timeout' => (float) env('NIGHTOWL_SMTP_CONNECT_TIMEOUT', 10),
        'timeout' => (float) env('NIGHTOWL_SMTP_TIMEOUT', 10),
    ],

];
