# Changelog

All notable changes to `nightowl/agent` are documented here. The released
version is taken from the git tag. Entries for `1.0.x` and earlier are
reconstructed from the annotated release tags; pre-`1.0` (`0.1.x`) history lives
in the git tags.

## [1.2.6] - unreleased

### Fixed

- **Job attempt detail pages no longer time out.** Opening a job attempt walks
  the job family by filtering `nightowl_jobs` on `job_id` — the dispatch-pair
  lookup plus the ancestor and descendant BFS — but `job_id` was never indexed,
  so each of those ~15–25 lookups ran as a sequential scan of the whole table.
  On a job-heavy app the scans summed past PHP's 30s request limit and the page
  died with an uncatchable "Maximum execution time exceeded". A new
  `nightowl:migrate` index on `nightowl_jobs.job_id` collapses each lookup to an
  index scan. The same index also speeds command, scheduled-task and request
  detail pages, which resolve their child jobs through the same path.

## [1.2.5] - 2026-06-29

### Added

- **Telemetry is now dated by when the event happened, not when it drained.**
  `created_at` and every per-minute rollup bucket are stamped from each row's own
  event timestamp (range-guarded to a plausible window). After a PostgreSQL
  outage the catch-up drain now lands rows in the minutes they actually occurred
  instead of bunching them all at "now", so time-range charts stay honest across
  a recovery.
- **New `DRAIN_UNREACHABLE` diagnosis.** When the drain genuinely can't connect
  to PostgreSQL (host/port/credentials/network/firewall), the health report says
  so directly — distinct from `DRAIN_WRITE_FAILING` (PG reachable but rejecting
  writes) and from a stuck/crashed worker. Telemetry keeps buffering and drains
  automatically once the connection recovers.
- **Friendly "port already in use" startup error.** If the agent can't bind its
  ingest port it now prints a clear message with the fix — including the common
  case where Nightwatch's own agent already holds the shared default port 2407
  and how to run the two in parallel — instead of a raw stack trace.
- **`nightowl:prune --hours`** for sub-day retention (overrides `--days`), and
  all prune cutoffs are now computed in UTC to match the UTC-stamped `created_at`.
- **Index on `nightowl_jobs.attempt_id`** (added by `nightowl:migrate`), so the
  dashboard's parent-label / group-hash lookups for job-sourced rows stop running
  as sequential scans on detail-list pages.

### Changed

- **Job duration metrics are computed over attempt rows only.** A queued-job
  (dispatch) row carries enqueue overhead, not execution time; folding it into
  the job rollup dragged the reported minimum ~280× low and skewed p95. The live
  drain and the backfill now both restrict duration/histogram to attempts.
- **`DRAIN_QUARANTINE` now reflects the cumulative count of dropped rows**, not
  the prunable live buffer gauge (which decayed to zero after the retention
  window, silently clearing a critical that had lost real telemetry). The
  poison-row circuit breaker is now tracked per table, so a genuine per-table
  schema mismatch trips it while unrelated tables keep draining.
- **Connection-vs-write error classification is SQLSTATE-authoritative** and
  never inspects raw libpq message text (which can echo a customer row value),
  removing a class of misclassification where a poison row read as a connection
  failure (or vice versa).
- **The synthetic-traffic simulator moved to a separate dev-only package**
  (`nightowl/agent-simulator`, `require-dev`) — it is never shipped to a customer
  install.

### Fixed

- **No more duplicate telemetry / double-counted rollups after a post-commit
  SQLite fault.** The local `markSynced()` bookkeeping now runs outside the
  PostgreSQL write transaction, so a SQLite error after the rows already
  committed no longer triggers a bisection storm that re-`COPY`s committed rows.
- **Transient PostgreSQL failures (serialization / deadlock / lock) now defer the
  whole batch** for the next loop instead of recursively bisecting to a single
  row — no wasted load while the condition clears.
- **Rollups can no longer be under-counted when `nightowl:backfill-rollups` runs
  against a live drain.** The drain's additive rollup UPSERT and the backfill's
  recompute now coordinate through matching advisory locks, so neither clobbers
  the other with a stale value.

## [1.2.4] - 2026-06-26

### Added

- **Actionable drain-failure diagnosis (`DRAIN_WRITE_FAILING`).** When PostgreSQL
  is reachable but rejecting the agent's writes — the schema isn't migrated
  (`42P01`), the role can't INSERT (`42501`), credentials/database are wrong
  (`28P01`/`3D000`), or it's out of connection slots (`53300`) — the health
  report now names the exact cause and the fix ("Run `php artisan
  nightowl:migrate`", "Grant INSERT…") instead of the misleading "Postgres may
  be unreachable". Only the SQLSTATE + table name leave the customer's box; the
  raw libpq message (which can echo row values) stays in the local log.
- **Opt-in poison-row isolation (`NIGHTOWL_DRAIN_QUARANTINE`, default off).** A
  batch that one bad row would reject (e.g. an over-long value or a type
  mismatch) is bisected to set that single payload aside in a SQLite dead-letter
  so the rest of the stream keeps draining, instead of the whole drain
  head-of-line blocking. A systematic-mismatch circuit breaker stops it from
  silently dropping a whole stream (it surfaces `DRAIN_WRITE_FAILING` instead),
  transient errors (deadlock/lock) are retried rather than dropped, and set-aside
  payloads are reported via a new `DRAIN_QUARANTINE` diagnosis. Dead-lettered
  rows are pruned after a bounded retention (1 day).

### Changed

- **`DRAIN_STOPPED` no longer blames connectivity when the drain is connected but
  rejecting writes** — it defers to the more specific `DRAIN_WRITE_FAILING`, and
  the batch-failure warning is no longer silenced on a brand-new app whose first
  batch fails.
- **Drain-metrics encoding hardened** against a non-UTF-8 byte in a database
  error message (could previously crash the forked drain worker).

## [1.2.3] - 2026-06-22

### Added

- **App-vitals in the health report (fleet overview).** The drain worker now
  tallies per-app request, 5xx, and exception counts off the records it already
  parses (zero extra decode on the hot path — counting happens in the forked
  drain child, never the ingest loop) and ships them as a cumulative
  `app_vitals` block on the existing `POST /agent/health` body:
  `{ "requests_total", "requests_5xx", "exceptions_total" }`. Counts are
  cumulative since agent start (like `rows_drained`); the platform computes
  window deltas. Multi-worker counts are summed across drain workers. The block
  also carries `open_issues` — a current gauge (not cumulative) of the tenant's
  open issues, refreshed at most once a minute by a cheap indexed `COUNT` off
  the ingest path, taken as a MAX across workers (they share one tenant DB).
  The block is additive/back-compat — older agents simply omit it. Powers the
  Agency fleet-overview / apps-page health. No request content leaves the
  customer's PostgreSQL — only counts.

- **`nightowl_reports` tenant table (Agency white-label reports).** New
  migration creating `nightowl_reports` (`period_start`, `period_end`, `payload`
  JSON snapshot, `created_at`, indexed on `period_start`) to store frozen
  aggregate report snapshots. Schema only — the agent does not write this table;
  report generation lives in the API. Created on the next `nightowl:migrate`.

### Changed

- **More accurate query percentile estimates (shared histogram).**
  `QueryHistogram::estimatePercentile()` now interpolates geometrically
  (log-linear) within the √2-spaced bins instead of linearly, and clamps the
  crossing bin to the rollup's observed min/max — so a high percentile on
  bounded or spiky data no longer overshoots into the empty top of a wide bin
  (e.g. p95 returning 211 ms when the largest observed query was 190 ms), and
  the previously-unbounded overflow bin gets a real upper edge from the observed
  max. The frozen bin edges are unchanged (agent and API stay byte-identical);
  percentiles are computed API-side at read time, so this mirrors the API's fix.

### Fixed

- **`created_at` is now stamped in UTC for every telemetry table, regardless of
  the tenant PostgreSQL server's timezone.** The 1.2.2 UTC fix only covered the
  writers that already authored `created_at` from the agent's clock
  (requests/queries/jobs/cache/outgoing/logs). The **exceptions, commands, mail,
  notifications, scheduled_tasks** writers and the **users** upsert never set
  `created_at` at all — they fell back to the column's `useCurrent()` default
  (`CURRENT_TIMESTAMP`), which resolves in the **database session timezone**. On
  a non-UTC tenant DB (e.g. `Asia/Dhaka`, UTC+6) those rows were stored as local
  wall-clock; the dashboard appended `Z` and rendered them hours in the future
  ("LAST SEEN" showing e.g. `-17923s ago`), and short time-range filters dropped
  fresh data. All these writers now stamp `created_at` explicitly via `gmdate()`
  (UTC), matching the rest and the API's read path; the users upsert stamps it on
  insert only (left untouched on conflict). A regression test pins `created_at`
  to UTC across all twelve write paths under a non-UTC session timezone.
  **Rows written by earlier versions on a non-UTC server are skewed by the
  server's UTC offset** — let them age out via `nightowl:prune`, or
  `nightowl:clear` on a throwaway dataset. There is no automatic correction
  migration.

## [1.2.2] - 2026-06-07

### Fixed

- **`created_at` is now always stamped in UTC, regardless of the agent host's
  timezone.** 1.2.0 moved `created_at`/rollup `bucket_start` authorship from the
  database default to the agent's clock, but formatted them with `date()` —
  which uses the host's local timezone. On a non-UTC host (e.g. `America/Bogota`,
  UTC−5) every telemetry row landed hours behind the API's UTC `now()`, so the
  dashboard's short time-range filters (1H/6H) showed **no data** even though
  rows were drained correctly. All `created_at`, `bucket_start`,
  `first_seen`/`last_seen`, log `created_at`, and `updated_at` stamps now use
  `gmdate()` (UTC), matching the API's read path and the pre-1.2.0 database
  default. **Rows written by 1.2.0/1.2.1 on a non-UTC host are skewed by the
  host's UTC offset** — see *Upgrading* below.

### Upgrading to 1.2.2

- **Restart the agent** after upgrading so the new code authors timestamps.
- **Existing skewed rows** (anything drained by 1.2.0/1.2.1 on a non-UTC host)
  keep their wrong `created_at`. Options: let raw telemetry age out via
  `nightowl:prune` (default 14d), or, on a throwaway/fresh dataset, run
  `nightowl:clear` and let live drain repopulate. There is no automatic
  correction migration.

## [1.2.1] - 2026-06-07

### Fixed

- **Health reports are no longer dropped on long instance IDs or extreme
  metrics.** The agent identifies each instance as `hostname:pid`; on a
  Kubernetes pod or a cloud FQDN host that string could exceed the API's column
  limit and `422` the whole health report. It's now built through a single
  helper (`Support\AgentInstanceId`) that caps it to 191 chars — truncating the
  hostname while always preserving the `:pid` suffix. Two drain gauges,
  `pg_latency_ms` and `buffer_utilization_pct`, are also clamped to sane
  ceilings before emit, so a stalled PostgreSQL or a misconfigured
  `NIGHTOWL_MAX_PENDING_ROWS` can't overflow the API's decimal columns and lose
  the report. This is the agent half of a belt-and-suspenders fix paired with
  the API-side column widening.

## [1.2.0] - 2026-06-06

### Added

- **Pre-aggregated rollups for fast dashboard reads.** The agent now maintains
  per-minute summary tables — `nightowl_query_rollups`, `nightowl_request_rollups`,
  `nightowl_job_rollups`, `nightowl_outgoing_request_rollups`, and
  `nightowl_cache_rollups` — at drain time, in the **same transaction** as the raw
  write (so a rollup can never diverge from raw). The dashboard reads these for
  wide time ranges instead of scanning the high-volume raw tables, which fixes
  read-time query timeouts on busy apps. Duration-bearing types also keep a
  fixed log-scale histogram for approximate p50/p95/p99 over wide ranges. New
  migrations create the tables — run `php artisan nightowl:migrate`. The drain
  skips a rollup whose table is missing rather than failing, so upgrading the
  package before running migrations is safe (restart the agent after migrating).
- **`php artisan nightowl:backfill-rollups`.** Populates every rollup table from
  existing raw telemetry so historical ranges work immediately after upgrade.
  Chunked, throttled, and idempotent; `--type=` restricts to one table,
  `--since=` / `--until=` / `--chunk-days=` bound and pace the run. It never
  touches the most recent ~10 minutes, so it can't race live drain.
- **`NIGHTOWL_ROLLUP_RETENTION_DAYS`** (default `90`). Rollups are tiny, so
  `nightowl:prune` now retains them far longer than raw telemetry — keep
  long-range trend charts while pruning raw aggressively. `--rollup-days=`
  overrides per run.

### Changed

- **`created_at` is now stamped by the agent on requests/queries/jobs/
  outgoing-requests/cache rows** (previously left to the database column
  default). One clock per drain batch is written to both `created_at` and the
  rollup bucket so the summaries align with the read path. The agent's clock —
  not the database's — now authors these timestamps, so keep the agent host
  NTP-synced (the offset was already bounded by drain lag before this change).

### Fixed

- **Health reports now surface API rejections instead of dropping them.** A
  health report that reached the API but was rejected (e.g. `401` bad token,
  `422` payload the API won't accept) was previously discarded silently. The
  reporter now retries transient `5xx` failures with backoff and logs a
  non-retryable `4xx` on its first occurrence, so a misconfigured token or
  contract mismatch is visible immediately. Response parsing was also hardened
  against partial reads.

### Upgrading to 1.2.0

- **Populate rollups for historical data.** Live drain only fills the rollup
  tables for telemetry collected *after* this upgrade. To make wide time ranges
  fast immediately, run `php artisan nightowl:backfill-rollups` once after
  `nightowl:migrate` (it's idempotent and throttled, safe to run alongside a
  live agent). Without it, recent data still works and the tables fill in over
  time as drain runs.

## [1.1.0] - 2026-06-04

### Added

- **`php artisan nightowl:migrate`.** Creates or updates the NightOwl tables.
  Migration history is tracked **inside the NightOwl database**, so the command
  is idempotent across every environment that shares that database — run it on
  each deploy and the first creates the tables while the rest are no-ops. A
  database that already has the tables is reconciled and adopted as a baseline —
  whether its NightOwl migration history is missing, partial, or split between
  the nightowl and primary databases (a legacy effect of history tracking having
  moved between connections across 1.0.x) — rather than failing to recreate them.
- **Startup schema-drift warning.** `php artisan nightowl:agent` now warns at
  startup if the NightOwl schema is behind the package's migrations — checking
  both the NightOwl database's own history and the host app's primary history —
  and keeps running rather than failing silently mid-drain.

### Changed

- **`NIGHTOWL_RUN_MIGRATIONS` now defaults to `false`** (was `true` in 1.0.12).
  NightOwl's migrations no longer ride along with your app's `php artisan
  migrate`; the schema is managed by `nightowl:install` / `nightowl:migrate`.
  See the upgrade notes below.
- `nightowl:install` now provisions the schema via `nightowl:migrate`.

### Fixed

- Shared-database deploys no longer require the manual `NIGHTOWL_RUN_MIGRATIONS`
  opt-out introduced in 1.0.12. Because history is tracked in the NightOwl
  database, `nightowl:migrate` is idempotent across environments — the
  `SQLSTATE[42P07] relation "nightowl_requests" already exists` failure is fixed
  without per-environment configuration.

### Upgrading to 1.1.0

**`php artisan migrate` no longer creates or updates NightOwl's tables.** Add
`php artisan nightowl:migrate` to your deploy — it is idempotent and safe to run
every time.

- **Already-provisioned deployments keep working.** Your tables already exist, so
  the change is a no-op in place; nothing breaks on upgrade.
- The change matters when **provisioning a new environment or database**, and when
  **applying migrations from a future NightOwl upgrade**. Both now go through
  `nightowl:migrate` (or `nightowl:install`) instead of plain `php artisan migrate`.
- The **first** `nightowl:migrate` reconciles an existing database automatically —
  no duplicate-table error — regardless of where its migration history currently
  lives. NightOwl's history moved between connections across 1.0.x (nightowl
  database in 1.0.0–1.0.10, primary database in 1.0.11–1.0.12), so your history may
  be in either, both, or partially split; `nightowl:migrate` reads both and records
  what's missing. If no prior history exists anywhere, it adopts the present schema
  and prints a warning — in that case run your previous version's `php artisan
  migrate` first so the schema is current before switching.
- To keep the old behavior (migrations run as part of `php artisan migrate`), set
  `NIGHTOWL_RUN_MIGRATIONS=true`. Only for a single-database setup, and do **not**
  combine it with `nightowl:install` / `nightowl:migrate` — the two track history
  in different places and will collide.

## [1.0.12] - 2026-06-04

### Added

- **`NIGHTOWL_ENABLED` master switch.** Set `NIGHTOWL_ENABLED=false` to make the
  package fully inert: no telemetry collected or transmitted, migrations not
  registered. Common in the `testing` environment so tests don't pay the ingest
  overhead or require the `nightowl` database. In `phpunit.xml`:
  ```xml
  <php>
      <env name="NIGHTOWL_ENABLED" value="false"/>
  </php>
  ```
- **`NIGHTOWL_RUN_MIGRATIONS` opt-out** (default `true`). Set to `false` on
  environments that share a NightOwl database with another, so only one runs the
  table creation — a first mitigation for the duplicate-table failure on shared
  databases. (Superseded by the history-in-the-NightOwl-database approach in
  1.1.0.)

## [1.0.11] - 2026-06-04

### Fixed

- int4 overflow on `duration` / size columns — widened to `bigint`.
- Install migration tracking.

## [1.0.10] - 2026-06-03

### Fixed

- Drain worker pegging 100% CPU under Octane/Swoole (busy-loop in the
  `pgsqlCopyFromArray` hook).

## [1.0.9] - 2026-06-03

### Fixed

- Drain workers now `exec` a fresh interpreter to avoid inherited TLS state.

## [1.0.8] - 2026-06-03

### Fixed

- Hang in `copyBatch`'s `pgsqlCopyFromArray`, guarded with a SIGALRM backstop.

## [1.0.7] - 2026-06-03

### Fixed

- SIGALRM backstop for a hung PostgreSQL SSL handshake on connect.

## [1.0.6] - 2026-06-03

### Added

- Configurable PostgreSQL SSL mode via `NIGHTOWL_DB_SSLMODE` (default `prefer`).

## [1.0.5] - 2026-06-02

### Fixed

- Null the poisoned PG handle after a COPY failure.

### Added

- Migration `000030` makes `trace_id` nullable.

## [1.0.4] - 2026-06-01

### Fixed

- Cap PDO `connect_timeout` at 5s; guard `pgsqlCopyFromArray` false return.

## [1.0.3] - 2026-05-28

### Changed

- Dropped `react/http` in favour of a raw `react/socket` Connector and
  SocketServer, removing the `psr/http-message ^1.0` pin that conflicts with
  modern Laravel packages.

## [1.0.2] - 2026-05-18

### Added

- Laravel 13 support — constraint widened to `^11.0 | ^12.0 | ^13.0`.

## [1.0.1] - 2026-05-13

### Added

- Fork-safety probe in `nightowl:install` (forks parent + child writing
  concurrently to a temp SQLite WAL, then runs `PRAGMA integrity_check`), so PHP
  builds without `pcntl` or buffer paths on NFS fail loudly at install time.
- Drain-worker checkpoint metrics (`truncate_attempts` / `successes` /
  `failures`, `wal_size_bytes`) and configurable checkpoint interval / truncate
  threshold (defaults 60s / 100MB).
- PostgreSQL-outage chaos system test covering back-pressure, drain catch-up, and
  WAL TRUNCATE under a real PG outage.

## [1.0.0] - 2026-05-08

- Initial stable release.
