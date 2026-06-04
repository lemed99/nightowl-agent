<p align="center">
  <img src=".github/assets/logo.svg" alt="NightOwl" width="160">
</p>

<h1 align="center">NightOwl Agent</h1>

<p align="center">
  <strong>Open-source Laravel monitoring agent. Captures telemetry from <a href="https://github.com/laravel/nightwatch"><code>laravel/nightwatch</code></a> and drains it into a PostgreSQL database you control.</strong>
</p>

<p align="center">
  <a href="https://packagist.org/packages/nightowl/agent"><img src="https://img.shields.io/packagist/v/nightowl/agent.svg?style=flat-square" alt="Packagist Version"></a>
  <a href="https://packagist.org/packages/nightowl/agent"><img src="https://img.shields.io/packagist/php-v/nightowl/agent.svg?style=flat-square" alt="PHP 8.2+"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/lemed99/nightowl-agent.svg?style=flat-square" alt="MIT License"></a>
  <a href="https://github.com/lemed99/nightowl-agent/actions/workflows/tests.yml"><img src="https://img.shields.io/github/actions/workflow/status/lemed99/nightowl-agent/tests.yml?branch=main&label=tests&style=flat-square" alt="Tests"></a>
</p>

---

## What is this?

NightOwl Agent is an MIT-licensed Laravel package that:

1. **Sits in front of [`laravel/nightwatch`](https://github.com/laravel/nightwatch)** — Laravel's official observability SDK. Nightwatch already does the hard part: instrumenting all 12 record types — requests, queries, jobs, exceptions, commands, cache events, mail, notifications, outgoing HTTP, scheduled tasks, logs, users. The agent receives those payloads over a local TCP socket.
2. **Buffers them in a local SQLite WAL** — non-blocking ReactPHP ingest, ~13,400 payloads/s on a single instance.
3. **Drains them into a PostgreSQL database you provision** via the COPY protocol. Telemetry never leaves your network.

All tables are prefixed `nightowl_` and the schema is documented. You're free to query the data with `psql`, point Metabase at it, or build your own UI on top — Livewire, Next.js, whatever.

## Run it standalone

This package is fully usable on its own. Point it at a PostgreSQL database you control and you have a self-hosted Laravel APM:

```bash
composer require nightowl/agent
php artisan nightowl:install        # publishes config + runs migrations against your PG
php artisan nightowl:agent          # starts the TCP/UDP/health daemon (ports 2407/2408/2409)
```

Minimal `.env` (PostgreSQL credentials — that's it):

```env
NIGHTOWL_DB_HOST=127.0.0.1
NIGHTOWL_DB_PORT=5432
NIGHTOWL_DB_DATABASE=nightowl
NIGHTOWL_DB_USERNAME=nightowl
NIGHTOWL_DB_PASSWORD=nightowl
NIGHTOWL_DB_SSLMODE=prefer
```

You don't need to wire up Nightwatch's transport — the service provider automatically redirects its ingest to the local agent on `127.0.0.1:2407`. For a local-only setup you also don't need any token; the agent only enforces one if you set `NIGHTOWL_TOKEN` (useful when the agent listens on something other than loopback).

Tables fill up. Run any SQL you want against them.

## Disabling NightOwl

Set `NIGHTOWL_ENABLED=false` to make the package fully inert — the Nightwatch ingest hook is not wired (no telemetry is collected or transmitted) and the migrations are not registered. The most common use is turning it off in your test suite so tests don't pay the ingest overhead or require the `nightowl` database to exist:

```xml
<!-- phpunit.xml -->
<php>
    <env name="NIGHTOWL_ENABLED" value="false"/>
</php>
```

`nightowl:install` runs its migrations regardless of this flag (it's an explicit opt-in), so you can still install while the switch is off.

## Sharing one database across environments

NightOwl stamps an `environment` column (your `APP_ENV`) on every row, so several app environments (local, staging, production) can point at one NightOwl database and be filtered apart in the dashboard. The data is partitioned by environment; the `nightowl_*` tables are shared.

`nightowl:install` and `nightowl:migrate` track their migration history **inside the NightOwl database**, so they're idempotent across environments. Run the schema sync as part of each deploy:

```bash
php artisan nightowl:migrate
```

The first environment to deploy creates the tables, the rest are no-ops, and upgrades' new migrations apply on whichever environment deploys first. No "owner" environment and no flags. A database that already has the tables but no NightOwl migration history (e.g. created by an older version or by your app's `php artisan migrate`) is adopted as a baseline, so you never hit `relation "nightowl_requests" already exists`.

By default these migrations are **not** bundled into your app's `php artisan migrate`. If you'd rather run them that way (single-database setups only — it tracks history in your primary database and must not be combined with `nightowl:install`), set `NIGHTOWL_RUN_MIGRATIONS=true`.

## What you get out of the box

These features run in the agent process. Postgres is the only thing it talks to.

- **Exception fingerprinting** — `nightowl_exceptions` upserts into `nightowl_issues` keyed on `(group_hash, type, environment)`, so repeats roll up into one grouped issue.
- **New-issue alerts** — when an issue is seen for the first time the drain worker fans it out to whatever you've configured in `nightowl_alert_channels`: Email (BYO SMTP), Webhook (HMAC-signed), Slack, Discord.
- **Threshold-based performance issues** — set a threshold per record type (slow request, slow query, slow job, and so on), and durations above it get turned into issues.
- **Agent + host health diagnosis** — ring buffers and EWMA feed a rule engine that produces a health score and surfaces stalls (drain lag, buffer depth, CPU, memory, load average).
- **Raw rows for every Nightwatch record type** — all 12 sit in your Postgres. `psql`, Metabase, or your own UI on top.

## Architecture

```
 Your Laravel app                             Your infrastructure
 ┌──────────────────┐    TCP    ┌──────────────────────────────┐
 │ laravel/         │──2407────▶│ NightOwl Agent (ReactPHP)    │
 │ nightwatch       │           │  ├─ SQLite WAL buffer        │
 └──────────────────┘           │  └─ pcntl drain workers      │
                                │         │                    │
                                │         │ COPY protocol      │
                                │         ▼                    │
                                │   PostgreSQL (yours)         │
                                └────────────┬─────────────────┘
                                             │
                                             ▼
                              ┌─────────────────────────┐
                              │ Your own UI / scripts   │
                              │ (psql, Metabase, vibe-  │
                              │  coded Next.js, etc.)   │
                              └─────────────────────────┘
```

> **13,400 payloads/s** on a single instance — ReactPHP non-blocking TCP ingest, SQLite WAL buffering, PostgreSQL `COPY` drain with `synchronous_commit = off`.

## What the agent collects

Whatever Nightwatch emits, the agent persists. Each row carries duration (microseconds), `environment`, `deploy`, and the request/job correlation IDs Nightwatch attaches.

- **Requests** — method, route, path, status, duration, memory, user ID
- **Jobs** — queue, attempts, status (queued/processed/released/failed), exception link
- **Queries** — SQL, bindings, connection, duration, request correlation
- **Exceptions** — class, message, stack trace, fingerprint hash (upserted into `nightowl_issues`)
- **Logs** — level, message, context, request correlation
- **Users** — `users_count` upsert (request + exception counters per authenticated user)
- **Cache events, mail, notifications, outgoing HTTP, scheduled tasks, commands** — same shape as Nightwatch
- **Host metrics** — CPU, memory, load average (Linux `/proc`)
- **Agent self-health** — ingest/drain rates, buffer depth, back-pressure, diagnosis rules

P95s, N+1 detection, slow-query rankings, request timelines, etc. are queries you write against these tables.

## Requirements

- PHP **8.2+** with extensions: `pdo_pgsql`, `pdo_sqlite` (always), `pcntl` + `posix` (for the async driver), `zlib` (for gzipped payloads)
- PostgreSQL **14+** (16 or 17 recommended)
- Laravel **11, 12, 13**

## Data ownership & privacy

The agent writes telemetry **directly to your PostgreSQL database**. Zero request, query, or exception data leaves your infrastructure.

The only thing the agent _can_ send outbound, and only if you opt in to remote health reporting, is **agent/host health metadata** (ingest rates, buffer depth, drain lag, CPU/memory), so a remote backend can warn you when the agent is unhealthy.

The schema is documented and stable, so your data stays usable even if you stop running the agent.

## Optional: the hosted dashboard

If you'd rather not build and maintain a UI, [usenightowl.com](https://usenightowl.com) is a managed service that connects to your Postgres with credentials you supply (and can rotate or revoke at any time). It adds an issue lifecycle UI (resolve / ignore / reopen, assignees, comments, activity timeline), alerts for those state transitions, teams, and an MCP server for AI tools. The agent itself stays MIT and works the same with or without it.

Full guide: [docs.usenightowl.com](https://docs.usenightowl.com)

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, test suite structure, and conventions. Bug reports and feature requests go through [GitHub Issues](https://github.com/lemed99/nightowl-agent/issues).

## License

[MIT](LICENSE).

## Related

- **Docs** — [docs.usenightowl.com](https://docs.usenightowl.com)
