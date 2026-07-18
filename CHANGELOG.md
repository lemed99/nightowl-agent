# Changelog

All notable changes to `nightowl/agent` are documented here. The released
version is taken from the git tag. Entries for `1.0.x` and earlier are
reconstructed from the annotated release tags; pre-`1.0` (`0.1.x`) history lives
in the git tags.

## [1.3.2] - 2026-07-18

### Changed

- **`nightowl:prune` trims raw tables in bounded chunks with progress output.**
  The first prune after `nightowl:partition` deletes the entire pre-conversion
  backlog out of the historic partition — tens of GB on exactly the tenants
  that needed partitioning most — and that used to be a single `DELETE` that
  ran for many minutes with no output (reported from the field as "prune gets
  stuck"), held one long transaction, and handed autovacuum a giant dead-tuple
  wave. Raw trims now delete `--delete-chunk` rows per statement (default
  100k), print a heartbeat every ~10 chunks, and an interrupted prune resumes
  where it stopped instead of rolling the whole trim back.

## [1.3.1] - 2026-07-17

### Changed

- **`nightowl:migrate` now reconciles rollup completeness — no manual backfill
  on any upgrade path.** Its auto-backfill previously covered only empty *base*
  rollup tables, so upgrading to 1.3.0 created the hour/day tier tables empty and
  left them that way until someone ran `nightowl:backfill-rollups` by hand (the
  read side falls back to the minute base meanwhile, so charts stay correct but
  the tier speedup doesn't apply to existing history). Migrate now also detects
  incomplete state: a minute table missing history the raw table still holds
  (earliest bucket younger than the earliest raw row) gets the full
  raw→minute→tier chain, and a tier whose `call_count` sum falls short of its
  chain source's — the drain writes both in one transaction, so a shortfall is a
  gap wherever it sits, including the mid-history hole left when a daemon keeps
  writing minute-only between migrate and its restart — gets the new
  `nightowl:backfill-rollups --tiers-only` pass (minute→hour→day re-aggregation,
  no raw scan; replace-per-window, so it heals middle holes). Detection costs two
  index-backed MINs plus one SUM per rollup table per deploy. Retention asymmetry
  never false-triggers (tiers keep more history than their source).
  `--no-backfill` skips all of it, and `nightowl:backfill-rollups` remains
  available for exotic states.

## [1.3.0] - 2026-07-17

### Added

- **Minute→hour→day rollup tiers.** Every rollup table now has `_hourly_rollups`
  and `_daily_rollups` siblings (migration 000054, `LIKE base INCLUDING ALL`),
  written in the same drain pass by re-collapsing each batch's minute groups in
  PHP. Wide-range dashboard reads pick the coarsest tier the chart interval
  permits, so a 30-day chart scans days instead of every minute row — 60× / 1440×
  fewer rows. Every rollup column is mergeable (counters and histogram bins sum,
  min/max fold, representatives keep first-seen), so a coarser tier is a lossless
  collapse of the finer one. The tiers keep history far past the minute tier's
  retention: `NIGHTOWL_HOURLY_ROLLUP_RETENTION_DAYS` (366) /
  `NIGHTOWL_DAILY_ROLLUP_RETENTION_DAYS` (1100), TOP-LEVEL `rollup_tier_retention`
  config keys (never under `database`, where a published config's shallow merge
  would swallow the new sub-keys). `nightowl:backfill-rollups` chains
  raw→minute→hourly→daily.

- **DDSketch v2 percentile sketches on duration rollups.** Duration-bearing
  rollups (8 types × 3 tiers) now carry a sparse varint-packed `sketch` bytea plus
  `sketch_version`, written alongside the v1 `hist_NN` bins (migration 000057,
  dual-write transition — v1 stays readable for the whole rollup retention). The
  DDSketch mapping (α = 1%) guarantees 1% relative percentile error versus the √2
  histogram's ~2.8% worst case. Merging runs SQL-side inside the drain's
  `ON CONFLICT` via `nightowl_ddsketch_merge`, so concurrent workers serialise on
  the row lock with no PHP read-modify-write; `nightowl_ddsketch_agg` powers the
  tier backfill's re-aggregation. `src/Support/DDSketchHistogram.php` stays
  byte-identical to nightowl-api's twin (checksum-guarded on both sides). A managed
  PostgreSQL that denies `CREATE FUNCTION` skips the sketch columns entirely and
  keeps the v1 path — never worse than before. `nightowl:drop-v1-histograms`
  (guarded — refuses until every rollup row is v2) removes the old bins once the
  API ships hist-conditional reads.

- **Raw-index diet and rollup-table storage tuning.** Every COPY row pays index
  maintenance on 5–7 btrees per table; at high volume that tax, not the heap write,
  dominates drain cost on the customer's PostgreSQL. A full reader audit
  (2026-07-17) dropped 22 dead raw indexes (migration 000056 — the string
  `timestamp` indexes no query reads, single-column prefixes already served by the
  000044 composites, and unread `trace_id` / duration singles) plus 2 more once
  their readers proved always `created_at`-co-bounded (000059); every drop carries
  a documented no-reader verdict and the deliberately-kept indexes are listed
  alongside. Rollup tables now run `fillfactor 70` (000053) and
  `autovacuum_vacuum_scale_factor 0.02` (000055): the drain UPDATEs each hot
  bucket's row dozens–hundreds of times over its minute, and the default packing
  forced those updates non-HOT, bloating exactly the recent buckets the
  narrow-window charts scan (the statement_timeout 504 incident, 2026-07-16). The
  per-page headroom keeps those updates HOT; the aggressive autovacuum reclaims
  what HOT can't.

- **Drain transactions now reap their own orphans.** Every batch carries
  `SET LOCAL idle_in_transaction_session_timeout` (default 30s, env
  `NIGHTOWL_DB_IDLE_TXN_TIMEOUT_MS`, `0` disables). When an abandoned batch's
  server-side session survives behind a pooler (Supavisor/PgBouncer) holding
  uncommitted unique-index entries, the retry previously collided with its own
  ghost and died on `55P03` (`while inserting index tuple ... "nightowl_issues"`)
  until an operator intervened; now Postgres terminates the orphan itself and
  the drain self-heals. Scoped to the agent's transactions only — other
  applications on the customer's database are untouched, and a healthy drain
  cannot trip it (only idle-between-statements time counts, measured ~27ms live).
- **Disk-full and read-only databases are named plainly in health diagnoses.**
  `DRAIN_WRITE_FAILING` now maps SQLSTATE `53100` to "Your database is out of
  disk space" and `25006` to "Your database is in read-only mode" (managed-PG
  disk-full enforcement), instead of the generic "PostgreSQL is rejecting
  writes". `25006` is also classified whole-target so a read-only database
  defers-and-retries instead of quarantining (dropping) good rows. The
  `DRAIN_WEDGED` recommendation now points at server-side disk/read-only checks
  when the wedge survives agent restarts.

- **Raw telemetry tables are natively partitioned by day.** Fresh installs
  partition at `nightowl:migrate`; existing tenants convert with
  `php artisan nightowl:partition`. For 10 of the 11 tables the conversion
  attaches the existing data as-is — no rows are copied, and the exclusive
  locks last only for the rename/attach instants.

  **Upgrade note — `nightowl_logs` is the exception.** Its legacy `created_at`
  column is a nullable string, so converting it requires a full-table rewrite
  under an `ACCESS EXCLUSIVE` lock: on a large logs table this locks the table
  for the duration of the rewrite (minutes, proportional to row count). Ingest
  is unaffected — the agent keeps buffering and drains once the lock clears —
  but dashboard log reads will error until it finishes. Run `nightowl:partition`
  in a quiet window if your logs table is large, or prune logs first to shrink
  the rewrite. `NULL`/empty log dates become `1970-01-01` and age out with the
  next prune.

- **`duration_count` counter on the mail/notification/command/scheduled-task
  rollups** (migration 000061, all tiers): the number of duration-bearing rows,
  which is those types' average denominator (queued sends carry no duration and
  must not dilute the average). The API previously derived it by summing the 39
  v1 `hist_NN` bins; the dedicated column replaces that derivation so the bins
  can eventually be dropped. Backfilled from the bins at `nightowl:migrate`,
  written by the drain from then on — the writer probes for the column, so an
  un-migrated tenant keeps its rollups minus the new counter.
  `nightowl:drop-v1-histograms` refuses to run until the column exists.

### Fixed

- **One over-long field no longer stops the entire drain.** Reported from
  production: the drain wedged with `SQLSTATE[22001]: value too long for type
  character varying(255)`, repeating forever while the buffer climbed toward
  back-pressure. `RecordWriter` passed every field straight through to Postgres,
  but `$table->string()` is `varchar(255)` by default — `nightowl_requests` alone
  has twelve such columns (only `url` is `text`) fed by unbounded upstream values
  like `route_action` and `user_id`. With `drain_quarantine_enabled` off (the
  default) the rejected batch is retried intact every loop, so a single row
  head-of-line blocked all telemetry, silently and permanently.

  Values are now clamped to each column's real width, introspected per table from
  `information_schema` rather than hardcoded from the migrations — a tenant who
  widened a column themselves (`varchar(n)` → `text`) is not clamped back to 255,
  and `text` columns are never touched. Applied at every write path, not only the
  reported one: the COPY tables, the `nightowl_exceptions`/`nightowl_users`
  upserts, both issue upserts, and both rollup upserts (clamping a raw column but
  not its rollup would only move the poison). Clamping counts characters, not
  bytes, since `varchar(n)` does: a byte-prefix cut would sever a multibyte
  sequence and hand Postgres invalid UTF-8 (`22021`), trading one poison row for
  another. Truncation logs once per table+column, naming the column Postgres will
  not.

  This generalises the guard already on `eventEpoch()`, which range-clamps poison
  timestamps for the same reason (a `22008` would block the drain identically).

- **`agent_version` in health reports is now the real version.** It was a
  hardcoded `'1.0.0'` from the initial commit through 1.2.14 — never bumped — so
  every report ever sent misidentified the agent and support could not tell what a
  customer was running. It now resolves from Composer (`InstalledVersions`), which
  cannot drift, and pins branch installs to their commit (`dev-main@ce1fb23`). The
  value is truncated to 16 characters because the platform validates `max:16` into
  a `varchar(16)` and an over-long value would 422 the whole report.

  The unit test covering this asserted the constant against itself and stayed
  green for twelve releases of the version being wrong; it is replaced by tests
  asserting the contract.

### Changed

- **The `DRAIN_WRITE_FAILING` advice for rejected rows no longer sends operators
  to a log that cannot help them.** It said "check the agent log for the offending
  row", but Postgres names neither the column nor the row for a rejected `COPY`
  and the agent only logs the libpq message — so the advice sent a paying customer
  to an empty log during a live outage, and never conveyed that the drain was
  stuck rather than merely slow. It now says the drain will not recover on its own,
  and points at `NIGHTOWL_DRAIN_QUARANTINE` (or, when that is already on, at the
  systematic schema mismatch the breaker is reporting).

## [1.2.14] - 2026-07-16

### Fixed

- **A network stall on the drain write path no longer wedges the drain.** The agent
  had no working timeout on its Postgres writes. The `pcntl_alarm` backstop around
  `pgsqlCopyFromArray` was never a deadline: PHP dispatches async signals only at VM
  opcode boundaries and libpq retries `EINTR` internally, so a blocked libpq call
  never yields one and the handler only ever ran the instant libpq returned on its
  own. Measured against a true `iptables` partition: an alarm armed at 75s had **not
  fired 233s later**, with the process blocked in libpq (`state=Ss`, 0.1% CPU) and the
  kernel in retransmit backoff — bounded only by `net.ipv4.tcp_retries2`, ~15 minutes
  at the default. With the drain wedged, the SQLite buffer fills to
  `max_pending_rows` and the agent starts **refusing** payloads with `5:ERROR` rather
  than queuing them, so telemetry is lost rather than delayed.

  The deadline now comes from the socket, which bounds *every* statement on the
  connection rather than only the `COPY` call sites. Measured through the config path
  against the same partition: `tcp_user_timeout=5000` → 10.6s, `20000` (the default) →
  31.1s, `40000` → 50.8s, versus a control still wedged past 233s. Both knobs ship
  because they cover disjoint regimes — with unacked data in flight keepalives cannot
  fire (the socket is not idle), and on an idle read `tcp_user_timeout` cannot fire
  (nothing is unacked).

- **`tcp_user_timeout` is feature-detected, never concatenated on faith.** libpq
  rejects an unknown conninfo keyword *fatally*, and it needs libpq 12+ — verified
  against a real libpq 11.22, where the parameter fails the connect outright. The
  probe compares two errors against a socket path that cannot exist, so it never
  touches the network and is locale-proof. The agent warns at startup on pre-12 libpq,
  and on non-Linux, where the parameter is accepted but inert.

- **`PDO::ATTR_TIMEOUT` is now set explicitly (10s).** It is the sole control of the
  connect bound: `PDO_PGSQL` derives its own `connect_timeout` from it and libpq is
  last-key-wins, so the DSN's `connect_timeout=5` was dead code and the effective 30s
  bound was an accident of PDO's default. `0` hangs unbounded.

- **`lock_timeout` (10s) on the drain transaction.** A blocked `ON CONFLICT` upsert
  (issues, rollups, users) previously waited indefinitely; a socket deadline cannot
  bound a lock wait, because the connection is healthy throughout. Raises `55P03`,
  which the drain worker already treats as transient and defers.

- **`synchronous_commit` no longer leaks onto pooled connections.** It ran as a plain
  session `SET` at connect, so through a transaction-mode pooler it persisted onto the
  shared server connection and silently weakened durability for whatever other
  application borrowed it next. It is now `SET LOCAL` inside the drain transaction,
  which still governs that transaction's own commit — throughput is unchanged.

- **A stalled write is classified as a connection failure, not a write failure.**
  `HY000` is PDO's generic code for a libpq transport error with no server-side error,
  and it is exactly what the new deadline produces. It now falls through to the
  connection-error scan, so a network stall reports as "Postgres unreachable" instead
  of "your writes are being rejected". Other SQLSTATEs still short-circuit before the
  message scan, so the classifier never depends on customer row content.

- **`beginTransaction()` moved inside the drain transaction's `try`.** It is a network
  round trip like any other, and measurement shows it is exactly where a stalled batch
  blocks — BEGIN is first on the wire. Outside the `try`, that throw bypassed the catch
  entirely and the health report lost the SQLSTATE and failing table.

- **`rollBack()` can no longer mask the real error.** On a handle the deadline just
  killed, `inTransaction()` reports true and `rollBack()` then throws, abandoning the
  rest of the catch — so `lastWriteError` was never stamped and the rollback's
  exception was classified instead of the real one. It is now stamped first and the
  rollback is guarded.

### Added

- **`DRAIN_WEDGED` diagnosis.** Names the worker and the exact call it is blocked in
  (e.g. `pg:copy:nightowl_requests`), via a heartbeat stamped at each step boundary
  inside a batch — the drain-metrics file only advances *between* batches, so there a
  slow batch and a wedge are the same observation. Diagnosis only; it does not kill.
  Dormant by construction when the client deadline is active, so its firing is itself
  evidence that `tcp_user_timeout` is unavailable or inert.

- **`drain_connection` config block** (`NIGHTOWL_DRAIN_CONN_TIMEOUTS`,
  `NIGHTOWL_DB_TCP_USER_TIMEOUT_MS`, `NIGHTOWL_DB_KEEPALIVES_*`,
  `NIGHTOWL_DB_CONNECT_TIMEOUT`, `NIGHTOWL_DB_LOCK_TIMEOUT_MS`,
  `NIGHTOWL_DRAIN_WEDGE_WARN_SECONDS`). Deliberately a **top-level** key rather than
  part of `database`: `mergeConfigFrom` is a shallow `array_merge`, so a published
  config's `database` array wholly replaces the package's and any new sub-key there
  would be invisible to most installs, taking its env var with it.
  `NIGHTOWL_DRAIN_CONN_TIMEOUTS=false` restores the pre-1.2.14 network behaviour.

## [1.2.13] - 2026-07-15

### Added

- **Covering index for the sidebar issues-badge counts.** A new migration adds a
  composite index on `nightowl_issues (environment, status, type, assigned_to)`,
  backing the `issues/counts` aggregate the dashboard polls for the sidebar badge.
  Leading with `environment` (the sole filter) narrows the scan, and carrying
  `status`, `type` and `assigned_to` lets the aggregate run index-only off a narrow
  b-tree instead of scanning the wide heap (`exception_message` TEXT &c.) — the scan
  that was timing out on large tenant tables (a 504, or a worker-killing 30s abort on
  poolers that drop the statement timeout). The index is built `CONCURRENTLY` so it
  never blocks the live drain, and a re-run self-heals any `INVALID` leftover from a
  cancelled build. Run `nightowl:migrate` after upgrading to create it.

### Added

- **Command and scheduled-task telemetry now roll up into per-minute summaries.**
  `nightowl_command_rollups` and `nightowl_scheduled_task_rollups` join the existing
  rollup tables, so the dashboard's Commands and Scheduled Tasks pages — success/failure
  counts, duration charts, and percentiles — serve from compact aggregates instead of
  scanning raw rows, and that history survives when the raw `nightowl_commands` /
  `nightowl_scheduled_tasks` rows are pruned or cleared. Run `nightowl:migrate` after
  upgrading to create the two tables, then `nightowl:backfill-rollups` to populate them
  from existing telemetry.

### Fixed

- **Out-of-range log timestamps can no longer produce un-prunable log rows.**
  `nightowl_logs.created_at` is a text column, so Postgres won't reject a malformed or
  millisecond-scaled timestamp the way it does on the timestamp-typed tables. A log event
  arriving with such a timestamp is now clamped to a valid `created_at` (falling back to
  the drain clock), so `nightowl:prune`'s `created_at < cutoff` comparison can always
  match it and the row stays prunable.

## [1.2.11] - 2026-07-08

### Fixed

- **The drain no longer re-duplicates telemetry on large batches under older SQLite.**
  After a batch drained to Postgres, the agent marked it done with a single
  `UPDATE ... WHERE id IN (?, ? … )` carrying one bound variable per row. At the
  default batch size (5,000) that exceeds SQLite's host-parameter cap
  (`SQLITE_MAX_VARIABLE_NUMBER` = 999 on builds before 3.32), so the statement threw
  "too many SQL variables" on every drain, the batch was never marked synced, and the
  same rows were re-sent to Postgres each loop — duplicating request/query telemetry
  without bound (observed as request counts inflated tens-to-hundreds of times over
  reality). The mark now chunks its id list (500 per statement, in one transaction) so
  it stays under the cap regardless of batch size or SQLite version. Drain batch size
  and throughput are unchanged; `nightowl:clear` and poison-row quarantine use the same
  safe path.
- **`nightowl:clear` now truncates every telemetry *and* rollup table.** It previously
  cleared only 10 raw tables, silently leaving `nightowl_logs` and all rollup tables
  populated — so a "clear" left wide-range dashboard views reading from stale rollups.
  The table set is now derived from the rollup registry, so a newly added rollup type
  can never be missed again.

### Added

- **The agent refuses to start if it can't write its SQLite buffer.** On boot it probes
  the buffer file with a real (rolled-back) write; if that fails — a full disk, an
  exhausted quota or inode table, a read-only mount, or the agent running as a different
  user than owns the buffer file — it exits with an actionable error instead of starting
  and silently re-sending the same telemetry in a loop. Combined with the 1.2.10
  buffer-unwritable guard, a buffer it can't write can no longer cause silent
  duplication: the agent either won't start or pauses the drain, and says why.

## [1.2.10] - 2026-07-06

### Changed

- **`nightowl:migrate` now auto-populates rollup tables it creates.** The dashboard's
  read path switches a section to its rollup table the moment that table *exists*,
  falling back to raw telemetry only when the table is *absent*. So a bare
  `nightowl:migrate` — which created the rollup tables but left them empty until you
  remembered to run `nightowl:backfill-rollups` — made wide-range views (jobs,
  authenticated users, and the rest) read **0** even though the raw data was intact.
  Migrate now backfills any rollup table it leaves existing-but-empty, straight from
  existing raw telemetry, so the counts are right immediately. It's scoped to empty
  tables, so a routine re-deploy over already-populated rollups is a no-op; pass
  `--no-backfill` to skip and backfill manually. After migrating, restart a
  long-running agent so it begins writing rollups for new telemetry (it caches which
  rollup tables exist at boot).

### Fixed

- **The drain no longer duplicates committed telemetry when the local buffer goes
  unwritable.** If a batch's `write()` committed to Postgres but the follow-up
  `markSynced()` failed — the common cause being a full local disk, the very
  condition that made the SQLite buffer back up — those rows stayed `synced=0` and
  the next drain tick re-fetched and re-COPY'd data already durably in Postgres,
  duplicating it without bound for as long as the disk stayed full. The drain now
  holds the committed-but-unmarked ids and retries only the *mark* (never the write)
  until the buffer accepts it again, and surfaces the stall in the drain metrics
  (`buffer_mark_stalls`, `buffer_mark_stalled_since`, `committed_unmarked`) so a
  paused-but-not-duplicating drain is visible rather than silent. The at-most-one-
  batch-duplicate-on-hard-kill crash-safety tradeoff is unchanged.

## [1.2.9] - 2026-07-04

### Added

- **The exception detail page's "servers affected" and authenticated/guest counts
  now come from rollups.** Two new pre-aggregated summaries back the exception
  detail page so it no longer scans raw `nightowl_exceptions` for a high-volume
  fingerprint — an unbounded `COUNT(DISTINCT server)` / `SUM(CASE WHEN user_id …)`
  that could trip the tenant statement timeout (`SQLSTATE 57014`):
  - a new **`nightowl_exception_server_rollups`** table (one row per
    fingerprint × server × minute × environment) backs the distinct-server count, and
  - a new **`authenticated_count`** column on `nightowl_exception_rollups` backs the
    authenticated-vs-guest split (guest = `call_count − authenticated_count`).

  Run `php artisan nightowl:migrate`, then `php artisan nightowl:backfill-rollups`,
  after upgrading. Until the migration runs, the agent's column guard skips writing
  `nightowl_exception_rollups` (so migrate promptly); until the backfill runs, the
  new summaries only cover telemetry drained after the upgrade.

## [1.2.8] - 2026-07-03

### Fixed

- **`nightowl:backfill-rollups` no longer aborts on a queued-only minute.** For a
  minute bucket that contained only a queued job dispatch (or any duration-bearing
  type with no duration-carrying rows in that bucket), the backfill's
  `SUM(...) FILTER (...)` over zero matching rows returned `NULL`, which violated
  the `hist_NN NOT NULL` constraint and killed the whole backfill with
  `SQLSTATE[23502]`. The histogram selects are now `COALESCE(..., 0)` — matching
  the live drain, which already writes `0` for such buckets. Affects the job,
  mail and notification rollups; re-run `nightowl:backfill-rollups` after
  upgrading.

## [1.2.7] - 2026-07-03

### Added

- **Remote agent support (`NIGHTOWL_INGEST_URI`) — run NightOwl on Laravel
  Vapor.** The instrumented app previously always transmitted telemetry to a
  co-located agent on `127.0.0.1`, so serverless hosts (Vapor/Lambda) that can't
  run the long-lived agent in-process had no way to reach it. You can now point
  the app at a remote agent by setting `NIGHTOWL_INGEST_URI=host:port` (mirrors
  `laravel/nightwatch`'s `NIGHTWATCH_INGEST_URI`): run the agent on a
  long-running box in the same private network and have the app ship to it. A
  bare host with no port falls back to `NIGHTOWL_AGENT_PORT`. New
  `NIGHTOWL_INGEST_TIMEOUT` (default `0.5`s) tunes the connect/write timeout for
  the network hop. Both default to the loopback listener, so existing
  single-host installs are unchanged. See the new [Laravel Vapor
  guide](https://docs.usenightowl.com/agent/vapor).

- **Wider dashboard time ranges now serve from pre-aggregated rollups for more
  sections.** The agent already maintained per-minute rollups for queries and
  requests; it now also maintains them per-user (requests / jobs / exceptions),
  per-exception-fingerprint, per-mailable, and per-notification. The dashboard's
  Users, Mail, Notifications and Exceptions lists, overview stats and charts
  serve wide time ranges (1h and up) from these compact summaries instead of
  scanning raw telemetry, so they stay fast on high-volume apps. `nightowl:migrate`
  creates the new tables and `nightowl:backfill-rollups` fills them from existing
  telemetry; each is pruned on its own (longer) retention. A companion migration
  adds composite indexes that speed the request / job / mail / notification /
  exception detail pages.

### Changed

- **A rollup table that exists but is missing a column no longer stalls the
  drain.** Before writing a rollup the drain now verifies the target table
  carries every column it will write (guarding against a partial or not-yet-run
  migration, or an agent running ahead of a schema change). If a column is
  absent it disables just that rollup and keeps draining raw telemetry, instead
  of failing the shared drain transaction and head-of-line-blocking the whole
  pipeline.

## [1.2.6] - 2026-07-02

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
