<p align="center">
  <img src=".github/assets/logo.svg" alt="NightOwl" width="160">
</p>

<h1 align="center">NightOwl Agent</h1>

<p align="center">
  <strong>Self-hosted Laravel monitoring agent. Drop-in Nightwatch alternative, open-source.</strong>
</p>

<p align="center">
  <a href="https://packagist.org/packages/nightowl/agent"><img src="https://img.shields.io/packagist/v/nightowl/agent.svg?style=flat-square" alt="Packagist Version"></a>
  <a href="https://packagist.org/packages/nightowl/agent"><img src="https://img.shields.io/packagist/php-v/nightowl/agent.svg?style=flat-square" alt="PHP 8.2+"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/lemed99/nightowl-agent.svg?style=flat-square" alt="MIT License"></a>
  <a href="https://github.com/lemed99/nightowl-agent/actions/workflows/tests.yml"><img src="https://img.shields.io/github/actions/workflow/status/lemed99/nightowl-agent/tests.yml?branch=main&label=tests&style=flat-square" alt="Tests"></a>
</p>

---

<p align="center">
  <img src=".github/assets/demo.gif" alt="NightOwl dashboard demo" width="800">
</p>

## What is this?

NightOwl is an open-source Laravel monitoring agent paired with a closed-source hosted dashboard. The agent runs inside your app, buffers telemetry locally, and drains directly into **your own** PostgreSQL database — your request data never touches our servers. The dashboard at [usenightowl.com](https://usenightowl.com) connects to your database with credentials you control to render the monitoring UI.

## Install

```bash
composer require nightowl/agent
php artisan nightowl:install
```

Sign up and create an app to get your agent token: **[usenightowl.com/signup](https://usenightowl.com/signup)**

Full installation guide: [docs.usenightowl.com](https://docs.usenightowl.com)

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
                                └──────────────────────────────┘
                                            ▲
                                            │ reads via your creds
                                 ┌──────────┴──────────┐
                                 │ NightOwl Dashboard  │
                                 │ (hosted, closed)    │
                                 └─────────────────────┘
```

> **13,400 payloads/s** on a single instance — ReactPHP non-blocking TCP ingest, SQLite WAL buffering, PostgreSQL `COPY` drain with `synchronous_commit = off`.

## Features

- [x] **Requests** — durations, status codes, routes, P95, slow endpoints
- [x] **Jobs** — queue latency, attempts, failures, per-queue breakdowns
- [x] **Queries** — N+1 detection, slow queries, per-request SQL timelines
- [x] **Exceptions** — fingerprinted groups, stack traces, assignees, resolve/ignore
- [x] **Logs** — level filtering, context metadata, per-request log streams
- [x] **Users** — request and exception counts per authenticated user
- [x] **Alerts** — Email (BYO SMTP), Webhook (HMAC), Slack, Discord
- [x] **Host metrics** — CPU, memory, load average
- [x] **Agent health** — ingest/drain rates, back-pressure, 19 diagnosis rules

## Requirements

- PHP **8.2+** (with `pdo_pgsql`, `pdo_sqlite`; `pcntl` + `posix` for the async driver)
- PostgreSQL **14+**
- Laravel **11 or 12**
- Redis — _optional_, only if your app already uses it for queues/cache

## Self-hosting

The agent writes telemetry **directly to your PostgreSQL database**, never to ours. The only data the agent sends to NightOwl is agent/host health (ingest rates, buffer depth, CPU/memory) — zero request, query, or exception data leaves your infrastructure. The dashboard is hosted but connects to your DB using credentials you provided, which you can rotate or revoke at any time.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, test suite structure, and conventions. Bug reports and feature requests go through [GitHub Issues](https://github.com/lemed99/nightowl-agent/issues).

## License

[MIT](LICENSE) — use it, fork it, ship it.

## Related

- 🦉 **Dashboard** — [usenightowl.com/signup](https://usenightowl.com/signup)
- 📖 **Docs** — [docs.usenightowl.com](https://docs.usenightowl.com)
- 🌐 **Website** — [usenightowl.com](https://usenightowl.com)
