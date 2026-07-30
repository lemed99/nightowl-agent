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
- 21 diagnosis rules across pipeline / reject rate / system / rollup categories
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
- **Both** modes go through `MultiIngest` — single-agent mode wraps a lone ingest for its fail-open path, so an unreachable agent socket can never throw inside a host request. Exception: `ping()` deliberately rethrows (`nightwatch:status` reads reachability off it); absorbed failures go to `error_log` **and** `Nightwatch::handleUnrecoverableExceptionsUsing()`
- `laravel/nightwatch ^1.26` is now a hard require (was `suggest`), enabling one-step install

## Key Files

```
src/Agent/
  AsyncServer.php        — Event loop, TCP/UDP, multi-fork, health, threshold polling (30s), update-drift warn poll
  VersionDriftWatcher.php — WARN-ONLY update check: logs once per version when installed.php shows a newer agent than the running process (2-tick debounce vs a half-written vendor tree). Never exits — restarting is the operator's call, because supervisor behaviour on a self-initiated exit is unverifiable from inside the agent
  DrainWorker.php        — Child process: batch drain (COPY), WAL checkpoint, IPC metrics, worker ID
  MetricsCollector.php   — Ring buffers, 21 diagnosis rules, lifecycle tracking, system metrics
  HealthReporter.php     — Adaptive HTTP reporting to dashboard, retry backoff, report_id
  HealthServer.php       — HTTP GET /status endpoint
  SqliteBuffer.php       — WAL buffer: append/fetch/claim/mark/release/cleanup/checkpoint
  RecordWriter.php       — PG writer: COPY (10 tables) + INSERT (2 upsert), sync_commit=off
  PayloadParser.php      — Wire protocol, gzip, token extraction
  AlertNotifier.php      — Issue alerts: rich Slack blocks, Discord embeds, branded HTML email
  HealthAlertNotifier.php — Agent health alerts (DRAIN_STOPPED, PG_LATENCY_CRITICAL, etc.)
  SmtpClient.php         — The agent's SMTP, shared by both notifiers (they had a private copy each, drifted on three points). RFC-conformant HELO (FQDN or address literal — a bare `nightowl` is refused by Exchange/O365/Postfix), EHLO re-issued after STARTTLS, AUTH chosen from what's advertised, Date + Message-ID + dot-stuffing. Throws on incomplete config instead of returning
  WebhookClient.php      — The agent's webhook POST, shared by both notifiers. READS the response: non-2xx raises with the status + the receiver's first 200 bytes (it used to `@file_get_contents` and discard, so a revoked webhook looked exactly like a delivered alert). URLs redacted to `host/…` in errors — a Slack/Discord path IS the credential
  EmailTemplate.php      — Branded email rendering (fallback logo if FRONTEND_URL unset)
  Server.php             — Sync fallback (stream_select)
  ConnectionHandler.php  — Sync payload handler
src/Support/
  MultiIngest.php        — Nightwatch coexistence adapter (fan-out wrapper). Also the fail-open boundary in single-agent mode: write/writeNow/digest/flush absorb transport failures, ping() rethrows
  InstalledVersionReader.php — Fresh read of vendor/composer/installed.php via file_get_contents+regex (NEVER include/require: opcache validate_timestamps=0 serves a stale compile forever; never InstalledVersions: static-cached). Returns raw "pretty_version#reference" tuple; null on any failure
  QueryHistogram.php     — Frozen √2-spaced duration bin edges + bin assignment for rollups; MUST stay byte-identical to nightowl-api's App\Support\QueryHistogram (checksum-guarded both sides)
  RollupSpec.php / RollupSpecs.php — Declarative per-type rollup config (group cols, counters w/ PHP predicate + SQL condition, representatives, duration/histogram flags) driving RecordWriter::writeRollup + BackfillRollupsCommand. One spec each for queries/requests/jobs/outgoing/cache.
src/Commands/
  AgentCommand.php        — nightowl:agent [--driver=async|sync]; auto-migrates at startup when the nightowl-DB history is behind (schema sync via nightowl:migrate --no-backfill PRE-listen in a deadline-killed child process — NIGHTOWL_AUTO_MIGRATE_TIMEOUT — so drain children see new rollup tables and a tenant-DB lock can never hold the ingest port unbound; backfill reconciliation detached to background with a completion marker retried each boot, gated by NIGHTOWL_AUTO_BACKFILL and paced by measured lock-hold time so it can't starve the drain; warn-and-continue on failure; warns only under NIGHTOWL_AUTO_MIGRATE=false; skipped under run_migrations ride-along)
  MigrateCommand.php      — nightowl:migrate: migrate --database=nightowl (history in nightowl DB) + baseline adoption; pure helpers migrationsToBaseline/pendingMigrations/isBehind
  InstallCommand.php      — nightowl:install
  PruneCommand.php        — nightowl:prune (retention cleanup; raw + separate longer rollup retention)
  BackfillRollupsCommand.php — nightowl:backfill-rollups (replace-per-bucket backfill of nightowl_query_rollups)
  ClearCommand.php        — nightowl:clear (truncate all tables)
  TestAlertCommand.php    — nightowl:test-alert (dispatch a test issue.new through every enabled channel, as the agent)
```

## Artisan Commands

| Command | Purpose |
|---------|---------|
| `nightowl:agent [--driver=async\|sync]` | Start agent (TCP + UDP + Health API). Auto-migrates a behind schema at startup (see AgentCommand above) |
| `nightowl:install` | Publish config, create/update schema (via `nightowl:migrate`), fork-safety probe |
| `nightowl:migrate` | Idempotent schema sync — `migrate --database=nightowl` (history in the nightowl DB) + baseline adoption of an already-present schema. Run on each deploy. **Auto-backfills** any rollup table it leaves existing-but-empty (the API read path serves zeros off an empty rollup, so migrate populates it from raw immediately), and reconciles rollup completeness: a minute table missing raw history (min-vs-min) gets the full chain, a tier whose call_count sum falls short of its chain source (gaps, incl. mid-history holes) gets `nightowl:backfill-rollups --tiers-only` (minute→hour→day re-aggregation, no raw scan). Also repairs whatever the drain marked in `nightowl_settings.rollup_repair_from` (a batch that couldn't take the shared rollup lock) and clears the marker. Complete rollups are a cheap no-op per deploy (two MINs + one SUM per table). Skip with `--no-backfill`. |
| `nightowl:prune` | Delete telemetry older than retention (14d default); query rollups pruned separately (90d default) |
| `nightowl:backfill-rollups` | Backfill every `nightowl_*_rollups` table from raw telemetry (chunked, throttled, idempotent; skips the trailing 10min so it never races live drain; `--type=` restricts to one table) |
| `nightowl:clear` | Truncate all NightOwl tables |
| `nightowl:test-alert [--channel=]` | Send a test alert through the **agent's own** dispatchers. The dashboard's "Send test" and every triage alert go out from nightowl-api via Symfony Mailer; `issue.new`/`issue.reopened` go out from here via raw SMTP/HTTP. Two transports, one config row — this is the only thing that exercises the second. Reports PASS/FAIL per channel with the reason; ignores `notify_events` (so a transport failure can't hide behind a filter) but warns when a passing channel has `issue.new`/`issue.reopened` muted |
| `nightowl:gc-dict-traces` | Reclaim orphaned rows from the storage-v2 trace dictionary (unreferenced by `nightowl_exceptions_v2` AND older than quarantine). Rides `nightowl:prune`; `--dry-run`/`--quarantine-days`/`--chunk`. No-op without the v2 twin or the 000068 `created_at` clock |

## Database

69 migrations, 74 logical tables (12 telemetry + 11 `_v2` twins + 4 dictionaries + 3 issues + alert_channels/settings + 42 rollups) plus dynamic partition children:

- **Raw-table partitioning (000058 + `nightowl:partition`)**: the 10 timestamp-keyed raw tables are natively partitioned by `created_at`, daily children (`{t}_pYYYYMMDD`), a DEFAULT child for backdated drains, and — on converted live tenants — a `{t}_phistoric` child (the original table ATTACHed under a validated CHECK, zero row copy; its `(id)` PK is demoted to the `(id, created_at)` unique the parent PK requires). Fresh installs partition at migrate (empty-table rebuild); populated tenants run `nightowl:partition`. `RawPartitions::convert` holds a per-table **session** advisory lock (`pg_try_advisory_lock('nightowl_partition:{t}')`) across the WHOLE conversion — prep included — with the `{t}_pnew` build+swap additionally in one transaction, the build **before** `LOCK TABLE ... ACCESS EXCLUSIVE` (all catalog-only, but 5+N round trips that do not belong inside the exclusive window; the `LIKE`'s ACCESS SHARE makes the LOCK an upgrade, which queues FIFO; it CAN deadlock against the drain, and the ordering that prevents it is load-bearing — `ALTER SEQUENCE {t}_id_seq OWNED BY` must stay AFTER `LOCK TABLE`, because it takes SHARE ROW EXCLUSIVE on the sequence while every drain COPY takes ROW EXCLUSIVE on it via `nextval`, closing a cycle against the table lock; measured 21/40 conversions deadlocked before the LOCK vs 0/40 after, and `lock_timeout` does not help because `deadlock_timeout` fires first), and the whole transaction under its own `SET LOCAL lock_timeout` — the `nightowl` connection is registered with no `options` key, so without it a swap queued behind one long reader parks a pending exclusive in front of every reader and writer of the live table. Session-scoped is forced: prep runs in autocommit (CIC can't be transactional) and its `ADD CONSTRAINT`/`VALIDATE`/PK-demote target whatever `{t}` names when they run, so a run prepping after a peer's swap commits silently plants a boundary CHECK on the new partitioned parent (cascades to children → 23514 on every drain row past the boundary) and drops the parent's composite PK. The pooler hazard that makes the drain use xact locks is handled by detection, not assumption: the baseline pid is read in the SAME statement that takes the key (`SELECT pg_try_advisory_lock(hashtext(?)), pg_backend_pid()`) — sampled a statement later it baselines whichever backend the pooler moved us to, after which every check passes while the key sits stranded on the one that actually holds it — and `assertSameBackend()` re-reads `pg_backend_pid()` per phase and throws `PoolerAffinityException` behind a transaction-mode pooler; the unlock's own return value is read too, because `pg_advisory_unlock` answering false is the only evidence the key is stranded on an unreachable backend. Prep in autocommit is reduced to the two RECOVERABLE statements — the CONCURRENTLY-built `{t}_id_created_at_pt` and `{t}_hist_ck` — so only the CHECK needs a hand-written unwind (`$checkAdded`), and a failed unwind is now logged rather than swallowed; left behind it is a time bomb that fails all writes once its frozen boundary passes. The swap transaction's own `rollBack()` is guarded too: its most violent deaths take the backend with them, `PDO::inTransaction()` still answers true against a dead handle, and an unguarded ROLLBACK raised "server closed the connection unexpectedly" *over* the real cause — destroying both the 55P03/40P01/57P01 that classifies the failure and the exception TYPE the command dispatches on, so a retryable `ConversionInProgressException` was reported as a hard FAILURE (1) instead of BUSY (3). `ALTER COLUMN created_at SET NOT NULL` and the PK demote run **inside the swap transaction**, where rollback is the unwind: SET NOT NULL skips its scan given the validated CHECK (PG12+, 2.5ms vs 156ms on 2M rows) and a rolled-back PK drop restores the identical relfilenode with no reindex. In prep, any abort after them left the live table permanently with NO PRIMARY KEY — pg_dump silently stops emitting it and relreplident still reads DEFAULT. `indexDefs()` excludes `{t}_id_created_at_pt`: it's the conversion's own CONCURRENTLY-built scaffolding, and replaying it gives the parent a unique index the historic child lacks → ATTACH builds it inline under ACCESS EXCLUSIVE. Loser gets `ConversionInProgressException`; the command isolates per-table failures and exits `BUSY` (**3** — 2 is Symfony's inherited `Command::INVALID`) for contention, `INCOMPLETE` (**4**) when every conversion landed but a child window did not, and `FAILURE` (1) for real errors (ladder in the pure `PartitionCommand::exitCode`, and the "Done" line is derived from it so only a clean run claims one); it never short-circuits on an already-partitioned table — it delegates to `convert()` anyway, because that is the only path that repairs a parent whose swap committed before its child sweep ran; a `PoolerAffinityException` from `assertSameBackend()` is fatal to the whole run, not per-table, because the same pooler fronts every remaining table. A conversion SIGKILLed between VALIDATE and the swap can't clean up after itself, so `RawPartitions::healConversionLeftovers()` runs on **every** drain cleanup tick (~60s, `RecordWriter::healRawPartitionLeftovers`, which opens NO transaction of its own so the heal takes one short transaction PER AFFECTED TABLE — one caller-owned transaction skips those per-candidate commits and holds ACCESS EXCLUSIVE on every healed parent until a single commit) and strips both leftovers of a killed run — `{t}_hist_ck` where it's provably wreckage (provable means `pg_constraint.conkey` says the expression references `created_at` and nothing else — one naming another column is never dropped, only reported, because the verdict probe evaluates the expression against a one-column probe row, so such a constraint raised 42703 there, aborted the statement, recurred identically every tick and wedged that table's heal permanently), and an **INVALID** `{t}_id_created_at_pt` from a killed CIC: on a plain table once its boundary has passed (probe a **bound PHP `gmdate()` UTC literal** against the constraint expr — `now()::timestamp` renders in the session TimeZone GUC, the customer's on a BYO Postgres, and flips its answer against a gmdate-written boundary), and on a partitioned parent unconditionally (convert clones the parent without `INCLUDING CONSTRAINTS`, so it's always the pre-lock corruption; dropping it also clears the merged historic-child copy, which is safe — the partition bound enforces the range). The boundary is frozen at the **second** UTC midnight ahead (`RawPartitions::historicBoundary`, sampled after CIC and immediately before the CHECK), never the next one, so it always clears 24-48h: at the next midnight a 23:56 run had four minutes before its own `NOT VALID` CHECK began rejecting live drain rows (23514) and its `VALIDATE` failed on the rows that arrived mid-scan. The cost is that `{t}_phistoric` covers one extra UTC day — those days get no daily child (`ensureChildWindow` skips every day the historic child covers), so prune row-DELETEs them instead of DROPping a partition. The boundary is still NOT what protects a live conversion from the heal sweep — the per-table conversion key is. A clock the sweep races is not a guarantee, whatever its headroom. The sweep takes the **same per-table conversion key `convert()` holds** (`pg_try_advisory_xact_lock`, and it opens its own transaction when the caller has none) and re-reads relkind + the expression under it; the key is only taken once a batched catalog probe has found a candidate, so a healthy tenant takes no locks and issues no DDL. Its DDL — and the hourly child sweep's `CREATE ... PARTITION OF`, which needs the same ACCESS EXCLUSIVE — carries a 3s `lock_timeout` CEILING (`MAINTENANCE_LOCK_TIMEOUT_MS` via `withLockTimeout`; the maintenance txn sets none when `NIGHTOWL_DRAIN_CONN_TIMEOUTS=false`). Ceiling, not assignment: `withLockTimeout` only ever TIGHTENS, so an operator who lowered `NIGHTOWL_DB_LOCK_TIMEOUT_MS` keeps their value, and `0` (Postgres for "wait forever") counts as the loosest setting, not the tightest. It takes NO shared `nightowl_partition_maintenance` key — only each table's conversion key, so it cannot starve the hourly sweep, and it inherits no drain guard (its per-tick caller opens no transaction, and `NIGHTOWL_DRAIN_CONN_TIMEOUTS=false` would remove the guard anyway). Heal failures are returned and logged instead of swallowed, and the success line is emitted by the caller after each table's own commit. "Not swallowed" is not one channel: three states share the `$failures` out-param but get their OWN log line, because the right remediation differs and the wrong one is destructive — hard failures (something threw; retried next tick), stalled locks, and unreadable CHECKs. A held conversion key on its own is now SILENT: it is the constant state of a healthy in-flight `nightowl:partition` run (prep adds `{t}_hist_ck`, so the table is a candidate on every tick until the swap lands), and reporting it printed an error a minute whose hard-failure advice — drop the constraint by hand — would have destroyed the very conversion in progress. It is reported only when the skip actually COST a heal that was due, i.e. when the leftover is already past its boundary and the table is rejecting drain rows right now (`leftoverIsProvableWreckage()`, run after the lock attempt and never before it — `pg_get_expr` takes ACCESS SHARE and would otherwise queue behind a live swap on the healthy path). (tinybit.farm 42P01 incident, 2026-07-18.) The drain's cleanup tick runs the leftover sweep every 60s and pre-creates 7 days of children **hourly** (`RecordWriter::maintainRawPartitions`, advisory-locked) — which now RETURNS whether it ran, because `DrainWorker` used to advance its 3600s gate even on a lock skip, forfeiting the hour and, across workers, potentially every hour until `{t}_pdefault` swallowed the drain — each child takes ACCESS EXCLUSIVE on its parent, so that half is not a per-minute operation; prune DROPs fully-expired children (instant) and row-DELETEs only boundary/historic/default. All 11 raw tables partition, including `nightowl_logs` (000060 rewrites its legacy nullable-varchar `created_at` to timestamp — empty tables at migrate, populated via `nightowl:partition` with a full-table rewrite warning; NULL/empty dates become epoch and age out). PKs are `(id, created_at)`; raw `id` is synthetic (only the api's deletion chunker reads it). Post-DDSketch cleanup: `nightowl:drop-v1-histograms` (guarded — refuses until every rollup row is v2; the writer and backfill are already hist-conditional, but the API's histogram selects must ship hist-conditional reads in the SAME release the drop runs — tracked in project memory).
- **DDSketch percentiles (000057, specs/ddsketch_percentiles.md)**: duration-bearing rollups (8 types × 3 tiers) carry a sparse varint-packed `sketch bytea` + `sketch_version` alongside the v1 `hist_NN` bins (dual-write transition). α=1% guaranteed relative error vs √2's ~2.8% worst case. `src/Support/DDSketchHistogram.php` MUST stay byte-identical to the api twin (checksum-guarded both sides). SQL side: `nightowl_ddsketch_merge` (drain ON CONFLICT), `nightowl_ddsketch_single` + `nightowl_ddsketch_agg` (backfill/tier GROUP BYs). If a managed PG denies CREATE FUNCTION the migration skips the columns and everything stays v1.
- **Drain write-path economics**: rollup upserts are multi-row (500 rows/statement, `ROLLUP_UPSERT_CHUNK`) — a 200-group batch is ~3 statements, not ~600; 22 audited-dead raw indexes dropped (000056 — every drop has a documented no-reader verdict; the string `timestamp` indexes, redundant single-column prefixes of the 000044 composites, and the duration singles); rollup tables run fillfactor 70 (000053) + autovacuum scale factor 0.02 (000055). Drain benchmark: 16.7k rows/s with sketch dual-write (23.7k without), vs the 5.6k/s target.

- **Rollup tiers (minute→hour→day)**: every rollup table has `_hourly_rollups` / `_daily_rollups` siblings (migration 000054, `LIKE base INCLUDING ALL` + fillfactor 70), written in the same drain pass by re-collapsing the batch's minute groups in PHP (`src/Support/RollupTiers.php::collapse`). Wide-range dashboard reads pick the coarsest tier the chart interval permits (api's `RollupTierResolver`). Backfill chains raw→minute→hourly→daily; tier passes run to NOW, and the absence of a safety margin there is deliberate, not an oversight — the exclusive/shared advisory-lock protocol makes the replace commute with live drain, so a margin would buy nothing and instead leave a permanent coverage hole that the completeness check (`tierIsIncomplete()`) re-flags every boot and no pass ever heals. What bounds a pass is **hold time, not span**: each chunk is re-sized from the lock hold its predecessor measured, targeting ~1s (`TARGET_CHUNK_SECONDS`), because a calendar-span chunk says nothing about how many rows it aggregates — a dense tenant held the exclusive lock for tens of seconds per chunk, 55P03'd every drain batch on that table, and filled the buffer until the agent rejected payloads (the 2.0.0 field wedge). The floor is one bucket (an hourly chunk can never go under an hour, raw never under 60s), so a tenant where a single bucket's aggregation exceeds `NIGHTOWL_DB_LOCK_TIMEOUT_MS` is past what the pacer can absorb. A drain that still can't get the shared lock now commits its raw rows and records an IOU in `nightowl_settings.rollup_repair_from` (earliest bucket per table) instead of failing the batch; `nightowl:migrate` repairs the range and clears it, and `ROLLUP_REPAIR_PENDING` reports it meanwhile. Per-tier retentions: `NIGHTOWL_HOURLY_ROLLUP_RETENTION_DAYS` (366) / `NIGHTOWL_DAILY_ROLLUP_RETENTION_DAYS` (1100) — TOP-LEVEL config keys (`rollup_tier_retention`), never under `database` (shallow-merge swallows new sub-keys there).

- **Storage format v2 (migrations 000066/000067)**: every raw table has a `{table}_v2` partitioned twin in the definitive format — `ts_us bigint` event time (varchar `timestamp` gone), native uuid ids with `trace_id` NULLed when equal to `execution_id` (readers COALESCE), bytea `group_hash`/`fingerprint`, int dictionary ids into `nightowl_dict_string` for all low-cardinality labels, `sql_id`→`nightowl_dict_sql` (one row per distinct sql/file/line), `route_id`→`nightowl_dict_route` (append-only content-hash — renames never rewrite history), `trace_ref`→`nightowl_dict_trace` (deflated), agent-side-deflated `*_z bytea` blobs (NULL for placeholder literals), no `v`. NOTHING loses information — every v1 value is byte-recoverable. The drain branches per-type in `writeXV2` (`RecordWriter`), feeding the same copyBatch/insertBatch funnel; dictionary ids resolve in an autocommit warm pass BEFORE the batch txn (`DictionaryCache` — rollback can't poison the LRU; concurrent workers converge via ON CONFLICT DO NOTHING + re-select). `v2Enabled()` probes all 11 parents (process-cached; `nightowl.storage_v2` config = kill switch, top-level key). Cutover instant = `v2_fence` in nightowl_settings, written once by 000067 (`ON CONFLICT DO NOTHING` — re-runs never move it). v2 id sequences are setval'd above v1 MAX(id) (API union pages key on id). Rollups/issues/users keep hex-varchar keys — bridged with `decode(...,'hex')`; drain rollup rows are byte-identical across modes (PHP aggregation over in-memory records — pinned by RecordWriterV2Test). Backfill scans both families via `StorageV2::compatSelect`/`unionFrom` (v1-shaped projection UNION — every RollupSpec SQL expr stays byte-identical; split-window == all-v1 control pinned incl. sketch bytes). Maintenance/prune/clear iterate `RawPartitions::tablesIncludingV2` (existence-probes BOTH families — v1 names disappear post-EOL). Prune's v1-EOL: fence older than retention + v2 twin exists + v1 EMPTY (the mixed-fleet guard) + no `--keep-v1` → DROP v1 parent. Dictionaries are NEVER pruned/cleared (append-only; a running daemon's LRU holds ids) — with ONE deliberate exception: `nightowl_dict_trace` is the sole GC'd dict (`nightowl:gc-dict-traces`, migration 000068 adds its `created_at` clock, wired into `nightowl:prune`). A trace is reclaimed only when BOTH an anti-join against `nightowl_exceptions_v2.trace_ref` finds it unreferenced AND its `created_at` is older than the quarantine window (`NIGHTOWL_DICT_TRACE_GC_QUARANTINE_DAYS`, default 7). Race-safe without a lock: the warm pass touches `created_at = now()` on EVERY referencing batch (`DictionaryCache` trace insert → `ON CONFLICT DO UPDATE`; the collector always re-warms traces, never trusting the LRU), so an in-flight trace is always young, and the single-statement DELETE re-checks `created_at` (a concurrent touch spares the row; a lost race re-creates it append-only with a fresh id, so no exception ever dangles). The cutoff literal is offset-tagged (`+00:00`) because `created_at` is `timestamptz` — a bare literal would parse in the session TimeZone on a non-UTC BYO Postgres. The other three dicts (string/sql/route) stay un-GC'd: their ids are unbounded-lifetime keys. All probes schema-relative (`to_regclass` without `public.` — search_path-scoped tenants exist).
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
| **Unit** | 422 | None | PayloadParser, MetricsCollector, ConnectionHandler, AlertNotifier, DrainWorker |
| **Integration** | 335 | SQLite always, PG skips if unavailable | SqliteBuffer (multi-worker claiming, WAL), RecordWriter (COPY/upsert/users_count), SimulatorPayload, EndToEnd |
| **System** | 48 | PG + pcntl + posix | Real AsyncServer + fork + drain over TCP; thresholds, back-pressure, multi-worker, error storms, scaling |

**Total**: 805 test methods.

### Running Tests
- `vendor/bin/phpunit --testsuite Unit`
- `vendor/bin/phpunit --testsuite Unit --testsuite Integration` (PG tests skip if unavailable)
- `NIGHTOWL_TEST_DB_PORT=5433 vendor/bin/phpunit` (full suite)
- `NIGHTOWL_TEST_DB_PORT=5433 vendor/bin/phpunit --testsuite System`

### Storage family in tests

The System suite boots the real agent as a subprocess, so `NIGHTOWL_STORAGE_V2`
decides which family the daemon WRITES. `tests/System/Concerns/ReadsRawFamily`
reads the same variable so the assertions follow it — table name, uuid/bytea
identity predicates, and the dictionary joins that decode `method`/`status`/
`queue` back to their v1 spelling. Both readers apply the same rule as
`agent-harness-async.php`: **unset means v2**.

This has to be kept in step, because getting it wrong is silent. Reading
`nightowl_requests` while the daemon writes `nightowl_requests_v2` does not
error — it returns zero rows, which every System test reports as a drain
timeout. `phpunit.xml` pins the production default (v2); CI runs both legs
(`.github/workflows/tests.yml`, `matrix.storage`), and names `pcntl`/`posix`
explicitly so a missing extension can't turn the leg into an all-skip green.

One consequence for new System tests: record ids must be real uuids
(`self::uuid()` / `self::uuids(N)`). `StorageV2::uuidOrNull()` drops anything
else to NULL, so a `'sys-req-'.uniqid()` tag exercises an input the SDK never
sends. Group membership is an explicit id list via `self::traceIn()`, not a
`LIKE 'tag-%'` prefix scan. `nightowl_users`/`nightowl_issues` have no v2 twin
and keep their hex/varchar keys.

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
NIGHTOWL_STORAGE_V2=true                 # Storage-format-v2 kill switch: false reverts the drain to the v1 tables without a schema change (dual-read keeps every row visible). Top-level key (shallow-merge rule)
NIGHTOWL_DICT_TRACE_GC_QUARANTINE_DAYS=7 # nightowl:gc-dict-traces only reclaims traces unreferenced AND older than this. The warm pass touches created_at on every reference, so an active trace never ages in. Top-level key `dict_trace_gc` (shallow-merge rule)

# Agent SMTP (top-level key `smtp` — shallow-merge rule). Credentials are NOT here:
# they live per-app in nightowl_alert_channels. These are protocol knobs.
NIGHTOWL_SMTP_HELO=                      # HELO/EHLO name. Null = machine FQDN, else an address literal ("[10.0.1.7]") — containers rarely have a dotted name, and a bare hostname is refused by Exchange/O365/Postfix reject_non_fqdn_helo_hostname
NIGHTOWL_SMTP_CONNECT_TIMEOUT=10         # Seconds for the TCP connect
NIGHTOWL_SMTP_TIMEOUT=10                 # Seconds per reply. Both are additionally clamped to the caller's dispatch budget, so a hung relay can't stall the drain

# Boot-migrate + update check (all TOP-LEVEL config keys — shallow-merge rule)
NIGHTOWL_AUTO_MIGRATE=true               # Run nightowl:migrate at daemon startup when schema is behind (schema pre-listen in a deadline-killed child, backfill detached w/ completion-marker retry; warn-and-continue on failure; ignored under RUN_MIGRATIONS=true)
NIGHTOWL_AUTO_MIGRATE_TIMEOUT=300        # Hard deadline (s) on the boot schema run — killed on expiry so a tenant-DB lock never keeps the ingest port unbound; migrations stay pending, next boot retries
NIGHTOWL_AUTO_BACKFILL=true              # Whether the detached boot run does the rollup RECONCILIATION at all (schema sync is unaffected). Opt-OUT, not opt-in: an existing-but-empty rollup makes wide-range charts read zeros instead of falling back to raw, so skipping is worse than a paced backfill unless the operator runs nightowl:backfill-rollups themselves. false logs what it skipped and the command that does it
NIGHTOWL_UPDATE_CHECK=true               # Poll installed.php and LOG a warning when a newer agent is on disk than the running process. Warn only — never exits (supervisor behaviour on a self-initiated exit is unverifiable from inside the agent). Async driver only
NIGHTOWL_UPDATE_CHECK_POLL_SECONDS=300   # Poll interval (drift needs 2 consecutive reads — debounce vs a half-written vendor tree)

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
- No Pint here. This is a library: it ships no dev formatter (`vendor/bin/` has
  phpunit and nothing else), so match the surrounding file's style by hand.
  Running the API's Pint against this tree would impose an app's ruleset on a
  package that never opted into it.
