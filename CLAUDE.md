# NightOwl Agent

## What This Is
Laravel package installed in customer apps. Receives telemetry from `laravel/nightwatch` via TCP, buffers in SQLite, drains to customer's PostgreSQL. Also monitors its own health and the host system, reports to the NightOwl dashboard.

## Required Skills

### PHP 8.2+
- Constructor property promotion on every class
- Match expressions for routing and status logic
- Final classes throughout (no inheritance)
- Nullsafe operator (`?->`) for optional access
- Named arguments in stream functions
- Anonymous classes in migrations

### ReactPHP (Critical — Core Runtime)
- `react/event-loop` — Single-threaded event loop (`Loop::get()`, `addPeriodicTimer`, `addTimer`, `addSignal`, `run`, `stop`)
- `react/socket` — Non-blocking TCP server (`TcpServer` with `SO_REUSEPORT`)
- `react/datagram` — UDP socket server (fire-and-forget)
- `react/http` — HTTP server (health API), async HTTP client (`Browser` for health reporting)
- Promise-based callbacks (`.then(success, failure)`)
- Event-driven patterns (`$conn->on('data', fn)`, `$conn->on('close', fn)`)
- Recursive timer scheduling (NOT `addPeriodicTimer` — adaptive intervals)

### Process Management (Critical — Fork Safety)
- `pcntl_fork()` — Spawns N drain worker child processes (configurable via `NIGHTOWL_DRAIN_WORKERS`)
- `pcntl_waitpid()` with `WNOHANG` — Non-blocking child reaping
- `posix_kill()` — Signal delivery (`SIGTERM`, `SIGKILL`)
- Signal handlers — `SIGCHLD` (child died, identify by PID, restart specific worker), `SIGINT`/`SIGTERM` (graceful shutdown all)
- **Fork safety invariant**: Close SQLite PDO BEFORE fork, re-create AFTER. Child inheriting open SQLite handle = WAL corruption on exit.
- **WAL pragma ordering**: `busy_timeout` MUST be set before `journal_mode=WAL` to prevent fork race conditions
- Restart cooldown (2s) to prevent fork bombs
- Crashed worker's claimed SQLite rows released back to pending via `releaseClaimed()`

### SQLite (Buffer Layer)
- PDO with WAL mode, NORMAL sync, 64MB cache, 256MB mmap
- `busy_timeout=5000` set BEFORE `journal_mode=WAL` (prevents fork race)
- `appendRaw($json)` — Zero-copy insert (skip json_encode when no redaction)
- WAL checkpoint strategies: PASSIVE (non-blocking) → TRUNCATE (blocking, >200MB)
- Cross-process access: parent writes, children claim+drain, separate PDO connections
- Multi-worker row claiming: `claimBatch()` atomically claims rows (synced = 100+workerId), `releaseClaimed()` recovers on crash

### PostgreSQL (Drain Target)
- PDO with `synchronous_commit = off` (2-5x write throughput, SQLite buffer provides crash safety)
- **COPY protocol** for 10 high-volume tables via `pgsqlCopyFromArray()` (5-10x faster than INSERT)
- **INSERT kept** for 2 upsert tables: `nightowl_exceptions` (fingerprint upsert → issues) and `nightowl_users` (ON CONFLICT)
- Batch size: 5,000 rows per COPY (configurable via `NIGHTOWL_DRAIN_BATCH_SIZE`)
- Auto-reconnect on connection error (pattern matching: "server closed", "broken pipe", etc.)
- 12 record types: request, query, exception, command, job, cache_event, mail, notification, outgoing_request, scheduled_task, log, user
- Exception fingerprinting via `nightowl_issues` upsert
- **PgBouncer recommended**: Transaction-level pooling for multi-worker drain (Docker Compose included)

### Ring Buffers & EWMA
- 60-slot ring buffers (1 slot/second) for ingest, reject, drain rates and event loop lag
- Index wrapping: `($idx + 1) % 60`
- Average: `array_sum($ring) / 60`
- EWMA (α=0.3) for PostgreSQL write latency smoothing

### System Metrics Collection (Linux)
- `/proc/stat` parsing — CPU usage via two-sample delta method
- `/proc/meminfo` parsing — `MemTotal`, `MemAvailable` extraction
- `sys_getloadavg()` — Load average (cross-platform)
- All collected every 10s during diagnosis tick, sub-millisecond overhead

### Diagnosis & Health Scoring
- 19 diagnosis rules across 3 categories (pipeline, reject rate, system)
- Anti-flapping debounce: 2 consecutive ticks (20s) before reporting
- Resolution tracking: 3+ ticks for genuine resolve, <3 = transient noise
- Resolved diagnosis GC: 5-minute retention, collect-then-delete pattern
- Health score: 100 - (25×critical + 10×warning + 2×info), clamped to 0
- Status thresholds: healthy ≥80, degraded ≥40, critical <40

### Wire Protocol
- Length-prefixed format: `[length]:[version]:[tokenHash]:[payload]`
- Gzip decompression via magic byte detection (`0x1f 0x8b`)
- Token validation: `xxh128` hash truncated to 7 chars
- Response: `2:OK` (success) or `5:ERROR` (failure)

### Back-Pressure (Two-Layer)
- Layer 1: Inline per-chunk memory guard (zero latency)
- Layer 2: Periodic monitor every 5s (pending rows + RSS check)
- Reject with `5:ERROR`, UDP silently dropped

## Key Files
```
src/Agent/
  AsyncServer.php       — Event loop, TCP/UDP, multi-fork, health, back-pressure timers
  DrainWorker.php       — Child process: batch drain (COPY), WAL checkpoint, IPC metrics, worker ID
  MetricsCollector.php  — Ring buffers, 19 diagnosis rules, lifecycle tracking, system metrics
  HealthReporter.php    — Adaptive HTTP reporting, retry with backoff, report_id
  SqliteBuffer.php      — WAL buffer: append/fetch/claim/mark/release/cleanup/checkpoint
  RecordWriter.php      — PostgreSQL writer: COPY (10 tables) + INSERT (2 upsert tables), sync_commit=off
  PayloadParser.php     — Wire protocol, gzip, token extraction
  Sampler.php           — RNG-based drop, exception/5xx bypass
  Redactor.php          — Recursive PII key redaction, O(1) lookup
  HealthServer.php      — HTTP GET /status endpoint
  Server.php            — Sync fallback (stream_select loop)
  ConnectionHandler.php — Sync payload handler
src/Commands/
  AgentCommand.php      — nightowl:agent (routes to async or sync driver)

tests/
  Unit/                 — PayloadParser, Sampler, Redactor, MetricsCollector, ConnectionHandler, AlertNotifier, DrainWorker
  Integration/          — SqliteBuffer (multi-worker claiming, WAL checkpoint), SimulatorPayload, RecordWriter (users_count), EndToEnd
  System/               — AgentSystemTest (full pipeline over TCP), AgentFeaturesSystemTest (sampling, redaction, thresholds, back-pressure, multi-worker)
  Simulator/
    NightwatchSimulator.php  — Full traffic simulator (12 record types, 5 scenarios)
    run.php                  — CLI runner (--scenario=realistic --count=500)
    benchmark.php            — Concurrent throughput benchmark (--workers=4 --duration=10)
    benchmark-ingest.php     — Ingest-only benchmark (no PG drain)
    benchmark-multi.php      — Multi-instance benchmark
    agent-harness-async.php  — Standalone async agent for testing (no Laravel needed)
    agent-harness.php        — Standalone sync agent for testing
    schema.sql               — PostgreSQL table DDL for test setup

docker-compose.yml          — PostgreSQL + PgBouncer stack
docker/postgres/postgresql.conf — Write-optimized PG config
.github/workflows/tests.yml — CI: unit tests (PHP 8.2-8.4) + integration (PostgreSQL)
```

## Conventions
- All agent classes are `final` — no inheritance
- No Eloquent in agent runtime — raw PDO only (performance critical)
- Service provider wires everything: `NightOwlAgentServiceProvider`
- Config: `config/nightowl.php` with `env()` defaults for all settings
- Durations stored in microseconds, converted to ms in API responses
- DB connection name: `nightowl` (registered by service provider)
- Error logging: `error_log("[NightOwl Agent] ...")` with component tags

## Performance
- **Ingest**: 13,400 payloads/s single instance (ReactPHP + SQLite WAL)
- **Drain**: ~5,600 rows/s per worker (COPY + sync_commit=off + batch 5,000)
- **Throughput**: 30 MB/s at 4 concurrent connections
- **Scaling**: `NIGHTOWL_DRAIN_WORKERS=N` for parallel drain, `SO_REUSEPORT` for multi-instance on Linux
- **Back-pressure**: Activates at 100K pending SQLite rows, rejects with `5:ERROR`
- **Headroom**: Single instance handles apps doing 2,000-5,000 req/s

## Development
- `php artisan nightowl:agent` — Start agent (requires pcntl + posix for async)
- `php artisan nightowl:prune` — Delete old telemetry data
- `php artisan nightowl:clear` — Truncate all monitoring tables

## Testing

### Test Suites
| Suite | Tests | Dependencies | What it covers |
|-------|-------|-------------|----------------|
| **Unit** | ~120 | None | PayloadParser, Sampler, Redactor, MetricsCollector, ConnectionHandler, AlertNotifier, DrainWorker |
| **Integration** | ~70 | SQLite (always), PostgreSQL (skips if unavailable) | SqliteBuffer (multi-worker claiming, WAL checkpoint), SimulatorPayload, RecordWriter (COPY, upserts, users_count), EndToEnd (handler→PG) |
| **System** | ~30 | PostgreSQL + pcntl + posix | Real AsyncServer + fork + drain pipeline over TCP. Sampling, redaction, thresholds, back-pressure, multi-worker drain, error storms, concurrent connections |

### Running Tests
- `vendor/bin/phpunit --testsuite Unit` — Unit tests only (no dependencies)
- `vendor/bin/phpunit --testsuite Unit --testsuite Integration` — Unit + integration (PG tests skip if unavailable)
- `NIGHTOWL_TEST_DB_PORT=5433 vendor/bin/phpunit` — Full suite with PostgreSQL (all 3 suites)
- `NIGHTOWL_TEST_DB_PORT=5433 vendor/bin/phpunit --testsuite System` — System tests only (boots real agent subprocess)
- `NIGHTOWL_TEST_DB_PORT=5433 vendor/bin/phpunit --testsuite System --filter Features` — Feature tests only (sampling, redaction, thresholds)

### PostgreSQL for Tests
```bash
docker run -d --name nightowl-test-pg -p 5433:5432 \
  -e POSTGRES_DB=nightowl_test -e POSTGRES_USER=nightowl_test \
  -e POSTGRES_PASSWORD=test123 postgres:17-alpine
```

### Simulator & Benchmarks
- `php tests/Simulator/run.php --token=<token> --scenario=mixed --count=200` — Send simulated traffic
- `php tests/Simulator/benchmark.php --token=<token> --workers=4` — Throughput benchmark
- `php tests/Simulator/agent-harness-async.php --token=<token>` — Standalone agent (no Laravel)

## Configuration
```
NIGHTOWL_DRAIN_BATCH_SIZE=5000     # Rows per COPY batch (default: 5000)
NIGHTOWL_DRAIN_WORKERS=1           # Parallel drain workers (default: 1, increase for high throughput)
NIGHTOWL_DRAIN_INTERVAL_MS=100     # Drain loop interval when idle
NIGHTOWL_SAMPLE_RATE=1.0           # 1.0 = keep all, 0.5 = ~50% drop (exceptions always kept)
NIGHTOWL_MAX_PENDING_ROWS=100000   # Back-pressure threshold
NIGHTOWL_MAX_BUFFER_MEMORY=268435456  # 256MB RSS limit
```
