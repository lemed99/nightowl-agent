# Changelog

All notable changes to `nightowl/agent` are documented here. The released
version is taken from the git tag. Entries for `1.0.x` and earlier are
reconstructed from the annotated release tags; pre-`1.0` (`0.1.x`) history lives
in the git tags.

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
