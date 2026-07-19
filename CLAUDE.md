# NightOwl Agent

## What This Is
Laravel package installed in customer apps. Receives telemetry from `laravel/nightwatch` via TCP, buffers in SQLite (WAL), drains to customer's PostgreSQL via COPY. Monitors its own health and the host system, reports to the NightOwl dashboard, and dispatches threshold + health alerts.

## Required Skills

### PHP 8.2+
- Constructor property promotion, match expressions, final classes, nullsafe (`?->`), named arguments, anonymous migration classes

### ReactPHP (Critical — Core Runtime)
- `react/event-loop` — Single-threaded loop (`Loop::get()`, timers, signals)
- `react/socket` — Non-blocking TCP (`TcpServer` with `SO_REUSEPORT`); also drives the health HTTP server and outgoing health reports via raw socket HTTP (no `react/http` dependency — avoids `psr/http-message ^1.0` conflict)
- `react/datagram` — UDP socket server (fire-and-forget)
- Promise-based callbacks; recursive timer scheduling for adaptive intervals

### Process Management (Fork Safety)
- `pcntl_fork()` spawns N drain workers (`NIGHTOWL_DRAIN_WORKERS`)
- `pcntl_waitpid(WNOHANG)` non-blocking reaping; `SIGCHLD` restart
- **Invariant**: Close SQLite PDO BEFORE fork, re-create AFTER (parent handle inheritance corrupts WAL on child exit)
- **WAL pragma order**: `busy_timeout` MUST precede `journal_mode=WAL`
- Restart cooldown (2s) prevents fork bombs; crashed worker's claimed rows released via `releaseClaimed()`

### SQLite (Buffer Layer)
- PDO + WAL mode, NORMAL sync, 64MB cache, 256MB mmap
- `appendRaw($json)` zero-copy insert (raw wire JSON straight into SQLite, no re-encode)
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
  AlertNotifier.php      — Issue alerts: rich Slack blocks, Discord embeds, branded HTML email
  HealthAlertNotifier.php — Agent health alerts (DRAIN_STOPPED, PG_LATENCY_CRITICAL, etc.)
  EmailTemplate.php      — Branded email rendering (fallback logo if FRONTEND_URL unset)
  Server.php             — Sync fallback (stream_select)
  ConnectionHandler.php  — Sync payload handler
src/Support/
  MultiIngest.php        — Nightwatch coexistence adapter (fan-out wrapper)
  QueryHistogram.php     — Frozen √2-spaced duration bin edges + bin assignment for rollups; MUST stay byte-identical to nightowl-api's App\Support\QueryHistogram (checksum-guarded both sides)
  RollupSpec.php / RollupSpecs.php — Declarative per-type rollup config (group cols, counters w/ PHP predicate + SQL condition, representatives, duration/histogram flags) driving RecordWriter::writeRollup + BackfillRollupsCommand. One spec each for queries/requests/jobs/outgoing/cache.
src/Commands/
  AgentCommand.php        — nightowl:agent [--driver=async|sync]; warns at startup if the nightowl-DB migration history is behind (MigrateCommand::isBehind), skipped under run_migrations ride-along
  MigrateCommand.php      — nightowl:migrate: migrate --database=nightowl (history in nightowl DB) + baseline adoption; pure helpers migrationsToBaseline/pendingMigrations/isBehind
  InstallCommand.php      — nightowl:install
  PruneCommand.php        — nightowl:prune (retention cleanup; raw + separate longer rollup retention)
  BackfillRollupsCommand.php — nightowl:backfill-rollups (replace-per-bucket backfill of nightowl_query_rollups)
  ClearCommand.php        — nightowl:clear (truncate all tables)
```

## Artisan Commands

| Command | Purpose |
|---------|---------|
| `nightowl:agent [--driver=async\|sync]` | Start agent (TCP + UDP + Health API) |
| `nightowl:install` | Publish config, create/update schema (via `nightowl:migrate`), fork-safety probe |
| `nightowl:migrate` | Idempotent schema sync — `migrate --database=nightowl` (history in the nightowl DB) + baseline adoption of an already-present schema. Run on each deploy. **Auto-backfills** any rollup table it leaves existing-but-empty (the API read path serves zeros off an empty rollup, so migrate populates it from raw immediately), and reconciles rollup completeness: a minute table missing raw history (min-vs-min) gets the full chain, a tier whose call_count sum falls short of its chain source (gaps, incl. mid-history holes) gets `nightowl:backfill-rollups --tiers-only` (minute→hour→day re-aggregation, no raw scan). Complete rollups are a cheap no-op per deploy (two MINs + one SUM per table). Skip with `--no-backfill`. |
| `nightowl:prune` | Delete telemetry older than retention (14d default); query rollups pruned separately (90d default) |
| `nightowl:backfill-rollups` | Backfill every `nightowl_*_rollups` table from raw telemetry (chunked, throttled, idempotent; skips the trailing 10min so it never races live drain; `--type=` restricts to one table) |
| `nightowl:clear` | Truncate all NightOwl tables |

## Database

45 migrations, 59 logical tables (12 telemetry + 3 issues + alert_channels/settings + 42 rollups) plus dynamic partition children:

- **Raw-table partitioning (000058 + `nightowl:partition`)**: the 10 timestamp-keyed raw tables are natively partitioned by `created_at`, daily children (`{t}_pYYYYMMDD`), a DEFAULT child for backdated drains, and — on converted live tenants — a `{t}_phistoric` child (the original table ATTACHed under a validated CHECK, zero row copy; its `(id)` PK is demoted to the `(id, created_at)` unique the parent PK requires). Fresh installs partition at migrate (empty-table rebuild); populated tenants run `nightowl:partition`. The drain's 60s maintenance tick pre-creates 7 days of children hourly (`RecordWriter::maintainRawPartitions`, advisory-locked); prune DROPs fully-expired children (instant) and row-DELETEs only boundary/historic/default. All 11 raw tables partition, including `nightowl_logs` (000060 rewrites its legacy nullable-varchar `created_at` to timestamp — empty tables at migrate, populated via `nightowl:partition` with a full-table rewrite warning; NULL/empty dates become epoch and age out). PKs are `(id, created_at)`; raw `id` is synthetic (only the api's deletion chunker reads it). Post-DDSketch cleanup: `nightowl:drop-v1-histograms` (guarded — refuses until every rollup row is v2; the writer and backfill are already hist-conditional, but the API's histogram selects must ship hist-conditional reads in the SAME release the drop runs — tracked in project memory).
- **DDSketch percentiles (000057, specs/ddsketch_percentiles.md)**: duration-bearing rollups (8 types × 3 tiers) carry a sparse varint-packed `sketch bytea` + `sketch_version` alongside the v1 `hist_NN` bins (dual-write transition). α=1% guaranteed relative error vs √2's ~2.8% worst case. `src/Support/DDSketchHistogram.php` MUST stay byte-identical to the api twin (checksum-guarded both sides). SQL side: `nightowl_ddsketch_merge` (drain ON CONFLICT), `nightowl_ddsketch_single` + `nightowl_ddsketch_agg` (backfill/tier GROUP BYs). If a managed PG denies CREATE FUNCTION the migration skips the columns and everything stays v1.
- **Drain write-path economics**: rollup upserts are multi-row (500 rows/statement, `ROLLUP_UPSERT_CHUNK`) — a 200-group batch is ~3 statements, not ~600; 22 audited-dead raw indexes dropped (000056 — every drop has a documented no-reader verdict; the string `timestamp` indexes, redundant single-column prefixes of the 000044 composites, and the duration singles); rollup tables run fillfactor 70 (000053) + autovacuum scale factor 0.02 (000055). Drain benchmark: 16.7k rows/s with sketch dual-write (23.7k without), vs the 5.6k/s target.

- **Rollup tiers (minute→hour→day)**: every rollup table has `_hourly_rollups` / `_daily_rollups` siblings (migration 000054, `LIKE base INCLUDING ALL` + fillfactor 70), written in the same drain pass by re-collapsing the batch's minute groups in PHP (`src/Support/RollupTiers.php::collapse`). Wide-range dashboard reads pick the coarsest tier the chart interval permits (api's `RollupTierResolver`). Backfill chains raw→minute→hourly→daily; tier passes run to NOW (no safety margin — the exclusive/shared advisory-lock protocol makes the replace commute with live drain). Per-tier retentions: `NIGHTOWL_HOURLY_ROLLUP_RETENTION_DAYS` (366) / `NIGHTOWL_DAILY_ROLLUP_RETENTION_DAYS` (1100) — TOP-LEVEL config keys (`rollup_tier_retention`), never under `database` (shallow-merge swallows new sub-keys there).

- **Telemetry**: requests, queries, exceptions, commands, jobs, cache_events, mail, notifications, outgoing_requests, scheduled_tasks, logs, users
- **Rollups** (14): query, request, job, outgoing_request, cache, mail, notification, command, scheduled_task (group_hash-keyed, duration + histogram); user, user_job, user_exception (per-user counts, no duration); exception, exception_server (fingerprint-keyed counts). Pre-aggregated per-minute summaries maintained at drain time. Driven by a declarative `RollupSpec` per type (`src/Support/RollupSpecs.php`) consumed by the generic `RecordWriter::writeRollup`, `nightowl:backfill-rollups`, `PruneCommand`, and `ClearCommand` — all iterate `RollupSpecs::all()`, so adding a spec (+ migration + one `writeRollup` call in the type's `writeX`) auto-propagates. Duration-bearing types carry √2-spaced `hist_NN` histogram bins for approximate windowed percentiles (`src/Support/QueryHistogram.php`); cache groups by `(key, store)` with no histogram. Queries keeps a bespoke drain path (`writeQueryRollups`) but shares the generic backfill/prune. See `specs/query_rollups.md`.
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
- Worker saturation alert (`RecordWriter::checkWorkerSaturation`, called from the drain cleanup tick): fires a `performance` issue (subtype `worker_saturation`) when avg request concurrency — `SUM(total_duration)` per completed minute from `nightowl_request_rollups` — holds ≥ `percent`% of the env's `http_workers` count for `sustained_minutes` consecutive minutes. Config = tenant settings keys `http_workers` + `worker_saturation` (same cached 30s updated_at poll as thresholds, via the generic `getCachedSetting`). Own-environment only (shared tenant DBs would double-alert otherwise); advisory-locked to one evaluation per tenant per minute. `threshold_ms`/`triggered_duration_ms` stay NULL by design — alert formatters suffix them with "ms"; the numbers ride `exception_message` / the group `message`.
- Raw HTTP dispatchers (AlertNotifier, HealthAlertNotifier `httpPost`) reject non-http(s) schemes before `file_get_contents` — PHP's URL wrappers otherwise allow `file://`/`phar://` etc.
- SMTP header builders must pass user-controllable fields (from/to/subject) through `sanitizeHeader()` to strip CR/LF (email-header injection).
- `json_decode` in drain/runtime paths uses `(..., true, N, JSON_THROW_ON_ERROR)` — never no-args decode. Depth N: 512 for payload re-parse, 32 for channel config, 16 for metrics/thresholds.
- PII redaction is delegated upstream to `laravel/nightwatch` via `NIGHTWATCH_REDACT_HEADERS` / `NIGHTWATCH_REDACT_PAYLOAD_FIELDS`. The agent itself does not redact — payloads are buffered and drained as received.

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
| **Unit** | ~110 | None | PayloadParser, MetricsCollector, ConnectionHandler, AlertNotifier, DrainWorker |
| **Integration** | ~80 | SQLite always, PG skips if unavailable | SqliteBuffer (multi-worker claiming, WAL), RecordWriter (COPY/upsert/users_count), SimulatorPayload, EndToEnd |
| **System** | ~30 | PG + pcntl + posix | Real AsyncServer + fork + drain over TCP; thresholds, back-pressure, multi-worker, error storms, scaling |

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
NIGHTOWL_ENABLED=true                    # Master switch — false makes the package inert (no ingest wiring, no migrations). Flip off in the testing env.
NIGHTOWL_RUN_MIGRATIONS=false            # Legacy ride-along. true also runs migrations via host `php artisan migrate` (history in PRIMARY db). Off by default — schema is managed by nightowl:migrate/install (history in the nightowl DB, idempotent across envs). Don't combine true with nightowl:install.
NIGHTOWL_ENVIRONMENT=                    # Override APP_ENV for the environment column (rare: standalone harness or custom labels)
NIGHTOWL_PARALLEL_WITH_NIGHTWATCH=false  # Run alongside Nightwatch (fan-out via MultiIngest)
NIGHTOWL_DRAIN_BATCH_SIZE=5000           # Rows per COPY batch
NIGHTOWL_DRAIN_WORKERS=1                 # Parallel drain workers
NIGHTOWL_DRAIN_INTERVAL_MS=100           # Drain loop idle interval
NIGHTOWL_MAX_PENDING_ROWS=100000         # Back-pressure threshold
NIGHTOWL_MAX_BUFFER_MEMORY=268435456     # 256MB RSS limit
NIGHTOWL_REOPEN_COOLDOWN_HOURS=0         # Hours to wait before flipping resolved → open on recurrence (0 = always reopen, Sentry-style)

# Drain connection — network deadline (config key is TOP-LEVEL `drain_connection`,
# NOT nested under `database`: mergeConfigFrom is a shallow array_merge, so a
# published config's `database` array replaces the package's and would swallow any
# new sub-key there along with its env var).
NIGHTOWL_DRAIN_CONN_TIMEOUTS=true        # Master switch. false restores pre-1.2.14 network behaviour
NIGHTOWL_DB_TCP_USER_TIMEOUT_MS=20000    # Bounds a SEND-BLOCKED socket (unacked data). libpq 12+, Linux-only, feature-detected
NIGHTOWL_DB_KEEPALIVES_IDLE=10           # Bounds an IDLE-READ socket, where tcp_user_timeout cannot fire (nothing unacked)
NIGHTOWL_DB_KEEPALIVES_INTERVAL=5        # idle + interval*count = 25s
NIGHTOWL_DB_KEEPALIVES_COUNT=3
NIGHTOWL_DB_CONNECT_TIMEOUT=10           # Sets PDO::ATTR_TIMEOUT — the SOLE control of the connect bound. Never 0 (hangs)
NIGHTOWL_DB_LOCK_TIMEOUT_MS=10000        # Caps a blocked ON CONFLICT upsert (raises 55P03 → deferred + retried)
NIGHTOWL_DRAIN_WEDGE_WARN_SECONDS=180    # DRAIN_WEDGED diagnosis threshold (no kill). 0 disables
NIGHTOWL_DB_IDLE_TXN_TIMEOUT_MS=30000    # SET LOCAL orphan reaper on every drain txn — an abandoned batch's server-side session (behind a pooler) is killed by PG instead of blocking retries with 55P03. Agent-scoped; can't fire on a healthy drain. 0 disables
```

### Why the drain needs a socket deadline

`pcntl_alarm` **cannot** bound a blocked libpq call: PHP dispatches async signals only
at VM opcode boundaries, and libpq retries `EINTR` internally, so the handler runs only
once libpq returns on its own. The old `COPY_TIMEOUT`/`CONNECT_TIMEOUT` backstops were
therefore post-hoc log lines, not deadlines — measured against a true `iptables`
partition, an alarm armed at 75s had not fired 233s later. Without a socket deadline a
stall is bounded only by `net.ipv4.tcp_retries2` (~15 min), and a wedged drain fills the
buffer until the agent **refuses** payloads with `5:ERROR`.

`tcp_user_timeout` and the keepalives cover **disjoint** regimes and are both required:
with unacked data in flight the socket is not idle so keepalives never fire, and on an
idle read there is nothing unacked so `tcp_user_timeout` never fires. Both bound an
*unreachable* peer, not an unresponsive one — a reachable-but-wedged backend or pooler
is bounded by neither (that is `lock_timeout`'s job, and the operator's).

### Auto-reopen on recurrence

When a fingerprint with `status='resolved'` recurs in a drain batch, the agent flips it back to `open`, fires an `issue.reopened` alert (Slack/Discord/Webhook/Email), and appends a `nightowl_issue_activity` row with `actor_type='agent'`. `status='ignored'` is never auto-reopened — "ignored" means the user explicitly silenced the fingerprint.

`NIGHTOWL_REOPEN_COOLDOWN_HOURS` suppresses the flip when the most recent `status_changed → resolved` activity is younger than the cooldown — useful for flapping issues. The cooldown is read via `config/nightowl.php` so `php artisan config:cache` is safe.

### `environment` vs `deploy` columns

Every telemetry row carries both:
- **`environment`** — where the app is running (`production`, `staging`, `local`). Read from `APP_ENV` (or `NIGHTOWL_ENVIRONMENT` override) by the agent at boot, stamped on every row. Drives the env filter in the dashboard and the issue dedup key `(group_hash, type, environment)` — staging noise can't mute production alerts.
- **`deploy`** — release/commit identifier, populated by the Nightwatch SDK from `NIGHTWATCH_DEPLOY` / `LARAVEL_CLOUD_DEPLOY_UUID` / `FORGE_DEPLOY_COMMIT` / `VAPOR_COMMIT_HASH`. Used for release tracking (seeing the same fingerprint reappear after a deploy).

## composer.json
- **Package**: `nightowl/agent`, PHP `^8.2`
- **Hard requires**: `laravel/framework ^11|^12`, `laravel/nightwatch ^1.26`, `react/{socket,datagram,event-loop}` — `react/http` intentionally excluded (its `psr/http-message ^1.0` pin conflicts with modern Laravel packages)
- **PHP extensions**: `pdo_pgsql`, `pdo_sqlite`

## Development
- `php artisan nightowl:agent` — Start agent (needs pcntl + posix for async)
- `php artisan nightowl:prune` — Delete old telemetry
- `php artisan nightowl:clear` — Truncate monitoring tables
- `vendor/bin/pint --dirty --format agent` — Format PHP after edits
