# NightOwl Agent

## What This Is
Laravel package installed in customer apps. Receives telemetry from `laravel/nightwatch` via TCP, buffers in SQLite (WAL), drains to customer's PostgreSQL via COPY. Monitors its own health and the host system, reports to the NightOwl dashboard, and dispatches threshold + health alerts.

## Required Skills

### PHP 8.2+
- Constructor property promotion, match expressions, final classes, nullsafe (`?->`), named arguments, anonymous migration classes

### ReactPHP (Critical — Core Runtime)
- `react/event-loop` — Single-threaded loop (`Loop::get()`, timers, signals)
- `react/socket` — Non-blocking TCP (`TcpServer` with `SO_REUSEPORT`)
- `react/datagram` — UDP socket server (fire-and-forget)
- `react/http` — HTTP server (health API) + async client (`Browser`)
- Promise-based callbacks; recursive timer scheduling for adaptive intervals

### Process Management (Fork Safety)
- `pcntl_fork()` spawns N drain workers (`NIGHTOWL_DRAIN_WORKERS`)
- `pcntl_waitpid(WNOHANG)` non-blocking reaping; `SIGCHLD` restart
- **Invariant**: Close SQLite PDO BEFORE fork, re-create AFTER (parent handle inheritance corrupts WAL on child exit)
- **WAL pragma order**: `busy_timeout` MUST precede `journal_mode=WAL`
- Restart cooldown (2s) prevents fork bombs; crashed worker's claimed rows released via `releaseClaimed()`

### SQLite (Buffer Layer)
- PDO + WAL mode, NORMAL sync, 64MB cache, 256MB mmap
- `appendRaw($json)` zero-copy insert (skip `json_encode` when no redaction)
- Checkpoints: PASSIVE (non-blocking) → TRUNCATE (blocking, >200MB)
- Multi-worker claiming: `claimBatch()` atomically sets `synced=100+workerId`

### PostgreSQL (Drain Target)
- `synchronous_commit = off` (2-5x throughput; SQLite WAL provides crash safety)
- **COPY protocol** via `pgsqlCopyFromArray()` for 10 high-volume tables (5-10x faster than INSERT)
- **INSERT kept** for 2 upsert tables: `nightowl_exceptions` (fingerprint→issue upsert) and `nightowl_users` (ON CONFLICT)
- Batch size: 5,000 rows per COPY (`NIGHTOWL_DRAIN_BATCH_SIZE`)
- Auto-reconnect on PgBouncer/Supavisor errors ("server closed", "broken pipe")
- 12 record types: request, query, exception, command, job, cache_event, mail, notification, outgoing_request, scheduled_task, log, user

### Ring Buffers, EWMA, Diagnosis
- 60-slot ring buffers (1/s) for ingest/reject/drain rates + event-loop lag
- EWMA (α=0.3) smooths PostgreSQL write latency
- 19 diagnosis rules across pipeline / reject rate / system categories
- Anti-flapping: 2-tick debounce (20s) before reporting; 3+ ticks to resolve
- Resolved diagnosis GC: 5-minute retention
- Health score: `100 - (25×critical + 10×warning + 2×info)`, clamped to 0

### System Metrics (Linux)
- `/proc/stat` delta-sample CPU usage, `/proc/meminfo` parse, `sys_getloadavg()`
- Collected every 10s during diagnosis tick, sub-ms overhead

### Wire Protocol
- `[length]:[version]:[tokenHash]:[payload]`
- Gzip detected via magic byte (`0x1f 0x8b`)
- Token: `xxh128` truncated to 7 chars
- Response: `2:OK` / `5:ERROR`

### Back-Pressure (Two-Layer)
- Inline per-chunk memory guard (zero latency) + periodic 5s monitor (pending rows + RSS)
- Reject with `5:ERROR`; UDP silently dropped

## Coexistence with Laravel Nightwatch

**New since Apr 2026.** Agent can run in parallel with `laravel/nightwatch` so customers can trial NightOwl without ripping out existing monitoring.

- `NIGHTOWL_PARALLEL_WITH_NIGHTWATCH=true` enables dual ingestion
- `NightOwlAgentServiceProvider` boot hook detects `Core::ingest` binding and wraps it with `Support\MultiIngest` (fan-out to both Nightwatch hosted ingest and NightOwl TCP agent)
- `laravel/nightwatch ^1.26` is now a hard require (was `suggest`), enabling one-step install

## Key Files

```
src/Agent/
  AsyncServer.php        — Event loop, TCP/UDP, multi-fork, health, threshold polling (30s)
  DrainWorker.php        — Child process: batch drain (COPY), WAL checkpoint, IPC metrics, worker ID
  MetricsCollector.php   — Ring buffers, 19 diagnosis rules, lifecycle tracking, system metrics
  HealthReporter.php     — Adaptive HTTP reporting to dashboard, retry backoff, report_id
  HealthServer.php       — HTTP GET /status endpoint
  SqliteBuffer.php       — WAL buffer: append/fetch/claim/mark/release/cleanup/checkpoint
  RecordWriter.php       — PG writer: COPY (10 tables) + INSERT (2 upsert), sync_commit=off
  PayloadParser.php      — Wire protocol, gzip, token extraction
  Sampler.php            — RNG-based drop; exception/5xx bypass
  Redactor.php           — Recursive PII key redaction, O(1) lookup
  AlertNotifier.php      — Issue alerts: rich Slack blocks, Discord embeds, branded HTML email
  HealthAlertNotifier.php — Agent health alerts (DRAIN_STOPPED, PG_LATENCY_CRITICAL, etc.)
  EmailTemplate.php      — Branded email rendering (fallback logo if FRONTEND_URL unset)
  Server.php             — Sync fallback (stream_select)
  ConnectionHandler.php  — Sync payload handler
src/Support/
  MultiIngest.php        — Nightwatch coexistence adapter (fan-out wrapper)
src/Commands/
  AgentCommand.php        — nightowl:agent [--driver=async|sync]
  InstallCommand.php      — nightowl:install
  PruneCommand.php        — nightowl:prune (retention cleanup)
  ClearCommand.php        — nightowl:clear (truncate all tables)
```

## Artisan Commands

| Command | Purpose |
|---------|---------|
| `nightowl:agent [--driver=async\|sync]` | Start agent (TCP + UDP + Health API) |
| `nightowl:install` | Publish config, run migrations |
| `nightowl:prune` | Delete telemetry older than retention (14d default) |
| `nightowl:clear` | Truncate all NightOwl tables |

## Database

25 migrations, 16 tables (12 telemetry + 3 issues + alert_channels/settings):

- **Telemetry**: requests, queries, exceptions, commands, jobs, cache_events, mail, notifications, outgoing_requests, scheduled_tasks, logs, users
- **Issues**: issues (fingerprint upsert, subtype: exception/performance/health, threshold_metrics, deploy), issue_activity (with `actor_type`/`actor_meta` for MCP), issue_comments (with actor columns)
- **Alerts**: alert_channels, settings

**DB connection name**: `nightowl` (registered by service provider).

## Conventions
- All agent classes `final` — no inheritance
- No Eloquent in agent runtime — raw PDO only (performance critical)
- Durations in microseconds (DB) → milliseconds (API responses)
- Error logging: `error_log("[NightOwl Agent] ...")` with component tags
- Thresholds polled every 30s (live config changes without restart)
- Threshold checks extend beyond requests → queries, cache, mail, notifications, outgoing_requests
- Raw HTTP dispatchers (AlertNotifier, HealthAlertNotifier `httpPost`) reject non-http(s) schemes before `file_get_contents` — PHP's URL wrappers otherwise allow `file://`/`phar://` etc.
- SMTP header builders must pass user-controllable fields (from/to/subject) through `sanitizeHeader()` to strip CR/LF (email-header injection).
- `json_decode` in drain/runtime paths uses `(..., true, N, JSON_THROW_ON_ERROR)` — never no-args decode. Depth N: 512 for payload re-parse, 32 for channel config, 16 for metrics/thresholds.
- Redactor enabled by default; scrubs keys + URL query-string params under url/uri/endpoint/href fields.

## Performance
- **Ingest**: 13,400 payloads/s single instance (ReactPHP + SQLite WAL)
- **Drain**: ~5,600 rows/s per worker (COPY + sync_commit=off + batch 5,000)
- **Throughput**: 30 MB/s at 4 concurrent connections
- **Scaling**: `NIGHTOWL_DRAIN_WORKERS=N` parallel drain; `SO_REUSEPORT` multi-instance (Linux)
- **Back-pressure**: Activates at 100K pending rows; rejects with `5:ERROR`
- **Headroom**: Single instance handles apps doing 2,000-5,000 req/s

## Testing

| Suite | Count | Dependencies | Focus |
|-------|-------|--------------|-------|
| **Unit** | ~118 | None | PayloadParser, Sampler, Redactor, MetricsCollector, ConnectionHandler, AlertNotifier, DrainWorker |
| **Integration** | ~80 | SQLite always, PG skips if unavailable | SqliteBuffer (multi-worker claiming, WAL), RecordWriter (COPY/upsert/users_count), SimulatorPayload, EndToEnd |
| **System** | ~35 | PG + pcntl + posix | Real AsyncServer + fork + drain over TCP; sampling, redaction, thresholds, back-pressure, multi-worker, error storms, scaling |

**Total**: 233 test methods.

### Running Tests
- `vendor/bin/phpunit --testsuite Unit`
- `vendor/bin/phpunit --testsuite Unit --testsuite Integration` (PG tests skip if unavailable)
- `NIGHTOWL_TEST_DB_PORT=5433 vendor/bin/phpunit` (full suite)
- `NIGHTOWL_TEST_DB_PORT=5433 vendor/bin/phpunit --testsuite System`

### PostgreSQL for Tests
```bash
docker run -d --name nightowl-test-pg -p 5433:5432 \
  -e POSTGRES_DB=nightowl_test -e POSTGRES_USER=nightowl_test \
  -e POSTGRES_PASSWORD=test123 postgres:17-alpine
```

### Simulator & Benchmarks
- `php tests/Simulator/run.php --token=<token> --scenario=mixed --count=200`
- `php tests/Simulator/benchmark.php --token=<token> --workers=4 --duration=10`
- `php tests/Simulator/agent-harness-async.php --token=<token>` (standalone, no Laravel)

## Configuration
```
NIGHTOWL_ENVIRONMENT=                    # Override APP_ENV for the environment column (rare: standalone harness or custom labels)
NIGHTOWL_PARALLEL_WITH_NIGHTWATCH=false  # Run alongside Nightwatch (fan-out via MultiIngest)
NIGHTOWL_DRAIN_BATCH_SIZE=5000           # Rows per COPY batch
NIGHTOWL_DRAIN_WORKERS=1                 # Parallel drain workers
NIGHTOWL_DRAIN_INTERVAL_MS=100           # Drain loop idle interval
NIGHTOWL_SAMPLE_RATE=1.0                 # 1.0 = keep all (exceptions always kept)
NIGHTOWL_MAX_PENDING_ROWS=100000         # Back-pressure threshold
NIGHTOWL_MAX_BUFFER_MEMORY=268435456     # 256MB RSS limit
```

### `environment` vs `deploy` columns

Every telemetry row carries both:
- **`environment`** — where the app is running (`production`, `staging`, `local`). Read from `APP_ENV` (or `NIGHTOWL_ENVIRONMENT` override) by the agent at boot, stamped on every row. Drives the env filter in the dashboard and the issue dedup key `(group_hash, type, environment)` — staging noise can't mute production alerts.
- **`deploy`** — release/commit identifier, populated by the Nightwatch SDK from `NIGHTWATCH_DEPLOY` / `LARAVEL_CLOUD_DEPLOY_UUID` / `FORGE_DEPLOY_COMMIT` / `VAPOR_COMMIT_HASH`. Used for release tracking (seeing the same fingerprint reappear after a deploy).

## composer.json
- **Package**: `nightowl/agent`, PHP `^8.2`
- **Hard requires**: `laravel/framework ^11|^12`, `laravel/nightwatch ^1.26`, `react/{socket,datagram,event-loop,http}`
- **PHP extensions**: `pdo_pgsql`, `pdo_sqlite`

## Development
- `php artisan nightowl:agent` — Start agent (needs pcntl + posix for async)
- `php artisan nightowl:prune` — Delete old telemetry
- `php artisan nightowl:clear` — Truncate monitoring tables
- `vendor/bin/pint --dirty --format agent` — Format PHP after edits
