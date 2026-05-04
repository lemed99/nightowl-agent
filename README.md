<p align="center">
  <img src=".github/assets/logo.svg" alt="NightOwl" width="160">
</p>

<h1 align="center">NightOwl Agent</h1>

<p align="center">
  <strong>Open-source Laravel monitoring agent. Captures telemetry from <a href="https://github.com/laravel/nightwatch"><code>laravel/nightwatch</code></a> and drains it into <em>your own</em> PostgreSQL — you keep 100% of the data.</strong>
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
3. **Drains them into _your own_ PostgreSQL** via the COPY protocol. The agent writes to a database you provision, on infrastructure you control. **No telemetry ever leaves your network.**

You own the database. You own the schema (all tables prefixed `nightowl_`). You own the data forever. Query it with `psql`, point Metabase at it, build a Livewire UI in an afternoon, vibe-code a Next.js dashboard — whatever you want.

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
```

You don't need to wire up Nightwatch's transport — the service provider automatically redirects its ingest to the local agent on `127.0.0.1:2407`. For a local-only setup you also don't need any token; the agent only enforces one if you set `NIGHTOWL_TOKEN` (useful when the agent listens on something other than loopback).

Tables fill up. Run any SQL you want against them.

## Optional: the hosted dashboard

If you don't want to build and maintain a UI, [usenightowl.com](https://usenightowl.com) is a managed service that connects to your Postgres with credentials you supply (and can rotate or revoke at any time). It adds issue management, multi-channel alerts (Email/Webhook/Slack/Discord), teams, and an MCP server for AI tools. Same data, rendered for you. The agent stays open-source and works exactly the same with or without it.

Full guide: [docs.usenightowl.com](https://docs.usenightowl.com)

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
                  ┌──────────────────────────┴──────────────────────────┐
                  │                                                     │
                  ▼                                                     ▼
    ┌─────────────────────────┐                       ┌──────────────────────────────┐
    │ Your own UI / scripts   │                       │ NightOwl hosted dashboard    │
    │ (psql, Metabase, vibe-  │            OR         │ (optional — issue mgmt,      │
    │  coded Next.js, etc.)   │                       │  alerts, teams, MCP)         │
    └─────────────────────────┘                       └──────────────────────────────┘
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

P95s, N+1 detection, slow-query rankings, request timelines, etc. are queries you write (or the dashboard does for you) — the agent just stores the underlying rows.

## What the agent does on its own

A few things run inside the agent process without needing the hosted dashboard:

- **Issue dedup** — `nightowl_exceptions` upserts into `nightowl_issues` keyed on `(group_hash, type, environment)`
- **New-issue alerts** — first occurrence of an issue can fan out to channels configured in `nightowl_alert_channels` (Email/SMTP, Webhook+HMAC, Slack, Discord) directly from the drain worker
- **Threshold issues** — durations over configured thresholds (per record type) become performance issues
- **Health diagnosis** — ring buffers + EWMA + a rule engine produce a health score and surface stalls

> Issue lifecycle (resolve / ignore / reopen / assignment / comments / activity timeline) and alerts for those transitions are driven by the hosted backend. In standalone mode you only get new-issue alerts; the columns exist in `nightowl_issues` for you to drive yourself if you wire up your own UI.

## Requirements

- PHP **8.2+** with extensions: `pdo_pgsql`, `pdo_sqlite` (always), `pcntl` + `posix` (for the async driver), `zlib` (for gzipped payloads)
- PostgreSQL **14+** (16 or 17 recommended)
- Laravel **11 or 12**

## Data ownership & privacy

The agent writes telemetry **directly to your PostgreSQL database**, never to ours. Zero request, query, or exception data leaves your infrastructure.

The only thing the agent _can_ send outbound — and only if you opt in by registering an app on the hosted dashboard — is **agent/host health metadata** (ingest rates, buffer depth, drain lag, CPU/memory) so the dashboard can warn you when your agent is unhealthy. Run the agent without registering and nothing is reported anywhere.

The schema is documented and stable. If you ever stop using the hosted dashboard, the data is still yours — read it, export it, migrate it, archive it.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, test suite structure, and conventions. Bug reports and feature requests go through [GitHub Issues](https://github.com/lemed99/nightowl-agent/issues).

## License

[MIT](LICENSE) — use it, fork it, ship it.

## Related

- **Docs** — [docs.usenightowl.com](https://docs.usenightowl.com)
- **Hosted dashboard (optional)** — [usenightowl.com](https://usenightowl.com)
