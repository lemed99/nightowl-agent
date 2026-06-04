# Changelog

All notable changes to `nightowl/agent` are documented here. The released
version is taken from the git tag. Entries for `1.0.x` and earlier are
reconstructed from the annotated release tags; pre-`1.0` (`0.1.x`) history lives
in the git tags.

## [1.1.0] - unreleased

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
