# Changelog

All notable changes to `nightowl/agent` are documented here. The released
version is taken from the git tag. Entries for `1.0.x` and earlier are
reconstructed from the annotated release tags; pre-`1.0` (`0.1.x`) history lives
in the git tags.

## [2.2.0] - 2026-08-14

### Added

- **`nightowl:repair-cache-rollup-keys` — repairs cache rollups written before
  key templating existed.** Templating (2.0.0) bounds cache-rollup cardinality
  at drain time, but only for rows written after the upgrade; the rows already
  on disk keep their literal, near-unique keys, and the hourly/daily tiers keep
  them for `rollup_tier_retention.hourly_days` / `daily_days` — 366 and 1100 days
  by default. That legacy shape is worse than merely large: because a literal
  cache key is near-unique, the tier backfill that built hourly and daily out of
  the minute table collapsed almost nothing, measured in the field at a 45.6M-row
  minute table producing a 45.3M-row hourly and a 45.4M-row daily table. One
  bloated table became three. Once those tables and their four-column primary
  keys no longer fit in cache — a Postgres restart is enough to lose the warm
  state that was holding it together — every rollup write goes to disk, the drain
  falls below the ingest rate, the SQLite buffer fills past
  `NIGHTOWL_MAX_PENDING_ROWS`, and the agent starts rejecting live telemetry.
  The new command re-aggregates those rows: every key whose `CacheKeyTemplate`
  pattern differs from the key itself is summed into the pattern's row for the
  same store, bucket and environment. Counters and durations are regrouped, never
  discarded, so no chart point is lost and no history is shortened.

  By default it does that by **rebuilding the table and swapping it in**, the way
  `pg_repack` does. The collapsed copy is built beside the live table from an
  exported snapshot while an `AFTER INSERT OR UPDATE OR DELETE` trigger records
  everything the drain writes meanwhile; the two are folded together and the
  tables are renamed under the rollup advisory lock, held for **milliseconds**.
  The snapshot is what makes that safe — a trigger alone would count a row
  written between the trigger going live and the scan reaching it twice — and
  deletes are captured as negative deltas rather than tombstones, because a
  templated group is the sum of many raw keys and retention pruning only ever
  removes some of them. The rebuild needs roughly the largest table's size in
  free disk and ownership of the table (the swap ends in a `DROP TABLE`, so it
  says so up front rather than after a rebuild that can take minutes), and in
  exchange it leaves **no dead space** (so no `pg_repack`
  afterwards), collapses the current bucket too, and finishes one operator step
  instead of several. A measured run: three tables holding 135,040 legacy rows
  collapsed to 30,765 in 1.3s total with a 6–8ms lock hold, 73.6 MB reclaimed to
  7.6 MB, byte-identical to the same telemetry drained by a templating agent.

  `--in-place` keeps the original merge-inside-the-live-table strategy for the
  tenant whose disk cannot hold a second copy. It is far slower, leaves dead
  space until a `pg_repack`, and cannot touch the buckets the drain is still
  writing. It batches by key rather than by time window — the key is the primary
  key's leading column, and the insert merges additively, so splitting one
  pattern across two transactions gives the same totals as doing it in one. That
  makes the batch size free to be paced by measured hold time toward one second,
  which matters because each batch holds the same advisory lock the drain pairs
  with; the 2.0.0 field incident was a 24s-per-chunk hold that had the drain
  rejecting every payload for two hours. Because the drain safety margin rounds
  down to the tier's own bucket, an in-place run against a live agent always
  leaves the current day's daily row literal-keyed, so it names the skipped
  ceiling per table and says to come back for the tail rather than letting the
  collapse counts read as complete. `--since` / `--until` bound that walk, and
  are refused outright under the rebuild, which has no window to bound.

  Either way keys that already equal their own pattern are never rewritten and
  the pass is idempotent. `--dry-run` reports the collapse without writing.

### Fixed

- **Slack alerts no longer arrive as "[no preview available]".** The issue
  payload carried its content entirely inside `attachments[].blocks`, and Slack
  builds desktop and push previews from the top-level `text` field — mobile
  notifications use nothing else. With that field absent there was nothing to
  extract, so every alert pushed a notification with no indication of which app
  or which exception it was about; you had to open Slack to find out. The
  payload now carries a one-line plain-text summary (severity label, app,
  exception name, message) in both `text` and the attachment's `fallback`,
  collapsed to a single line and capped at 250 characters. The in-channel
  message is unchanged — Slack suppresses `text` when blocks are present.

## [2.1.1] - 2026-08-06

### Fixed

- **Two hosts draining into one database no longer deadlock each other out of a
  batch of rollups.** The rollup upserts chunked their `$groups` array straight
  into a multi-row `INSERT … ON CONFLICT`. The array is keyed by the conflict
  key but iterated in the order records happened to land in that batch, which is
  not the same order on any two agents — so two drains whose batches shared a
  key took the row locks in opposite orders, Postgres detected the cycle, and
  killed one of them. The victim lost its entire batch of aggregates, silently:
  the raw rows had already gone in, so nothing looked missing until a rollup-fed
  chart disagreed with the raw one. Both upsert paths now `ksort` before
  chunking, giving every writer one global lock order so overlapping batches
  queue instead of forming a cycle. The sort is `SORT_STRING` rather than PHP's
  default comparison, so the ordering can't shift on a key that happens to look
  numeric. The tuples and their contents are unchanged. Reported and diagnosed
  in [#5](https://github.com/lemed99/nightowl-agent/pull/5), against
  `nightowl_user_rollups` on a three-host deployment running v1.4.1 — one
  drainer per host, `drain_workers` at its default of 1, so this needs no
  in-process concurrency to hit. That patch covered the shared upsert only;
  `nightowl_query_rollups` reaches Postgres through a bespoke second upsert that
  does **not** route through it, and being the widest-fanout rollup it is the
  likeliest place of all for two drains to share a key — it is the table in the
  reported deadlock trace, and it is fixed here too.

### Fixed

- **A rollup table no longer freezes silently when prune drops its v1 source.**
  The per-minute request-concurrency recompute named `nightowl_requests`
  unconditionally. Prune's v1-EOL step DROPs that table under a running daemon
  once the storage-v2 fence is past retention, after which the recompute raised
  42P01 — which aborts the whole statement, so the rollup wrote nothing on that
  tick and every tick after. The only outward trace was one `[NightOwl Drain]`
  line per minute in a log nobody tails: the table simply stopped advancing
  while every other rollup stayed current, the API's coverage gate saw the stale
  bound, abandoned the rollup, swept raw telemetry instead, and 14-day charts
  hit the tenant's `statement_timeout`. Both raw families are now probed **per
  tick, never cached** (a stale `true` is the failure mode; a stale `false`
  costs one slightly narrower scan), the query is built from the arms actually
  emitted rather than from a flag read twice, and neither-family-present skips
  the statement instead of issuing one. The same probe was added to
  `nightowl:backfill-rollups`, which is what `nightowl:migrate` runs to repair
  this — it would otherwise have 42P01'd on exactly the tenants that needed it.
  (Yomoney, 2026-07-31: v1 dropped at 00:00 UTC, 11 hours of frozen buckets,
  four 504s.)
- **A deploy now repairs a rollup that stopped, not just one that started
  late.** `nightowl:migrate` has always rebuilt an incomplete rollup without
  being asked, but every check it made looked at floors: a base was incomplete
  if raw history predated its earliest bucket, and an hourly/daily tier if its
  `call_count` sum fell short of its source's. A freeze is invisible to both.
  The floor never moves — only the ceiling stops — and when a minute base
  freezes its hourly child freezes with it, so the two sums keep agreeing;
  both sides stuck at the same instant reads exactly like both sides being
  correct. The concurrency rollup was the most exposed of all: it sits outside
  `RollupSpecs::all()` and has no tiers, so the sum comparison never ran for it
  at all. Completeness now also compares **ceilings** — the newest bucket
  against the newest raw row — and a base trailing raw by more than two hours
  takes the same full rebuild the floor arm already triggered. The tolerance is
  deliberately loose: the two sides are measured on different clocks (bucket
  starts are event time, `created_at` is the drain-insert clock), so a buffer
  working through a backlog widens the gap legitimately, while the failure this
  catches grows without bound and trails by a day within a day. Without this a
  frozen table stayed frozen across every subsequent deploy, and stayed
  repairable only for as long as raw retention kept the rows to rebuild from.
- **Health-alert dispatch no longer blocks the event loop.** Every step of a
  dispatch round is blocking I/O — a Postgres connect to read the channels, then
  raw SMTP and raw HTTP sends — and it ran inline on the 10s diagnosis timer, on
  the assumption that a new diagnosis is rare. It is not: the dispatch fires on
  transitions, and a struggling agent flaps. One unreachable SMTP or webhook
  host then held the entire loop for the connect timeout, measured at **30.01
  seconds** against an unroutable address, during which nothing is ingested,
  drained, reported, or accepted on the TCP port. The round now runs in a
  short-lived forked child that closes the inherited listeners and `SIGKILL`s
  itself when done (no `exit()`, so no destructor can close the parent's SQLite
  handle and corrupt WAL shared memory), with one round in flight at a time.
  Same symptom, opposite direction, as the v2.0.1 backfill starvation: an agent
  at 4% CPU reporting sustained ~480ms loop lag where v1.4.0 reported 2ms.
- **DDSketch percentile aggregation is linear in its input** (migration
  000070). `nightowl_ddsketch_agg` was declared with `STYPE = bytea`, so
  Postgres copied the accumulated sketch into the transition function on every
  input row, where it was re-decoded into a jsonb map and re-packed — per-row
  cost growing with the state rather than staying flat. Measured at ~6.5ms per
  row once saturated, which exhausts a 20s `statement_timeout` at ~3000 rows, so
  a 14-day per-route read (16,800 rollup rows) never returned. The state is now
  a dense `bigint[]` over the closed index domain, decoding one payload per call
  and packing once per group. On a 200-route × 336-hour corpus, a 14d 50-group
  percentile read goes from 103s to 0.48s on narrow sketches and from
  not-finishing-in-120s to 2.0s on wide ones; `nightowl_ddsketch_merge` keeps its
  signature for the drain's `ON CONFLICT` upsert and is 4.85× faster. Output is
  byte-identical to the old fold across 1,022 real groups and the boundary cases.
  One shape is slower — a group holding a single narrow row pays the array setup
  it never amortises — and is left alone, because it was never the shape in
  trouble.

### Added

- **`ROLLUP_STALE` diagnosis.** A rollup table that stops being written while
  its peers keep current now reaches the health payload and the alert channels,
  instead of only `error_log`. Staleness is graded **peer-relative within a
  tier** — a table's newest bucket against the newest bucket any table of the
  same tier reached — which self-calibrates: it needs no assumption about which
  telemetry types an app emits, and a daily rollup is never judged against a
  minute one. An empty table is not stale (that is a backfill that has not run,
  which `ROLLUP_BACKFILL_PENDING` owns), and a wholesale freeze reports nothing
  here because everything froze together and `DRAIN_STOPPED`/`DRAIN_WEDGED`
  already say something more useful. The verdict rides the existing hourly
  `TableStatsCollector` pass, which already reads each rollup's bucket bounds
  under an advisory lock, so it costs no additional probes.

### Changed

- **`nightowl:drop-v1-histograms` is runnable again, behind the linear
  aggregate.** v2.0.2 made it refuse outright, because dropping the bins leaves
  the sketch as the only percentile source and the quadratic fold could not
  serve a wide range. With migration 000070 that premise is gone. The command
  now blocks on a new `MISSING_LINEAR_AGG` verdict from `V1HistogramCleanup`
  rather than on principle: a database still carrying 000057's bytea-state
  aggregate is told to run `nightowl:migrate` first. The irreversibility warning
  stays — the bins can only be rebuilt from raw telemetry, which is gone past
  retention.

## [2.0.2] - 2026-07-31

### Changed

- **`nightowl:drop-v1-histograms` now tells operators not to run it.** Its cost
  warning claimed the post-drop regression was confined to 1h/24h minute-tier
  charts and that "wide ranges (7d/30d) are unaffected" — the opposite of what
  the aggregate actually does. `nightowl_ddsketch_agg` declares `STYPE = bytea`,
  so Postgres copies the accumulated sketch into the transition function on
  every input row, where the merge re-decodes it into a jsonb map (one
  `jsonb_set` per accumulated index) and re-packs it (one bytea concat per
  pair). Per-row cost therefore scales with the accumulated state, not with the
  row: measured on a 14d hourly profile (~380 distinct indices, 1141-byte
  saturated state), **~6.5ms per input row** — 500 rows 3.2s, 1000 rows 6.5s,
  4000 rows 26.3s, while the same 1000 rows confined to 20 indices take 91ms. A
  20s tenant `statement_timeout` is exhausted around 3000 rows, and a 14d
  per-route read (50 groups × 336 hourly buckets = 16,800 rows) does not return
  at all. Wide ranges are precisely the case that breaks. Dropping the bins
  removes the only cheaper path the reader can degrade to, and they cannot be
  restored once the raw telemetry behind them ages out — so the command now
  leads with a refusal-shaped warning and lists a linear merge as a
  prerequisite. No schema or drain behaviour changed.

## [2.0.1] - 2026-07-30

### Fixed

- **The boot rollup backfill no longer starves the drain into rejecting
  payloads.** A backfill chunk was sized by a calendar span (15 minutes of
  history, adapted on wall-clock feedback), which says nothing about how much
  work a chunk actually is: on a dense tenant one chunk aggregated enough rows to
  hold the rollup table's exclusive advisory lock for tens of seconds. Every
  drain batch touching that table then hit `NIGHTOWL_DB_LOCK_TIMEOUT_MS` and
  raised 55P03, which the drain correctly classifies as transient and defers — so
  nothing errored, nothing was lost, and the SQLite buffer grew until it crossed
  `NIGHTOWL_MAX_PENDING_ROWS`, after which the agent answered `5:ERROR` to the
  SDK and telemetry stopped being accepted at all. Chunks are now bounded by
  **measured lock-hold time** rather than a span, re-paced against live drain
  contention toward a ~1s hold. Measured on an 800-group workload: failed drain
  batches 4 of 8 → **0 of 56**, longest window with no drain progress 23.6s →
  **0.0s**, longest lock hold 24.18s → **1.17s**, rows drained 40 of 80 → **560
  of 560**, at the cost of 11% more wall clock on the backfill pass itself. The
  floor is one bucket (an hourly tier chunk can never go below an hour, raw never
  below 60s), so a tenant where aggregating a single bucket exceeds the lock
  timeout is still beyond the pacer's help.
- **A drain that cannot take the shared rollup lock keeps the raw telemetry.**
  The 55P03 from a contended rollup table used to abort the whole batch, raw rows
  included, and the batch was retried later as a unit. The shared-lock
  acquisition is now savepointed: on 55P03 the raw rows commit anyway and the
  drain records what it owes in `nightowl_settings.rollup_repair_from` (per
  table, keeping the earliest bucket). `nightowl:migrate` repairs the marked
  range and clears the marker, so a deploy fixes it with no operator action. Only
  55P03 is absorbed — any other error still fails the batch.

### Added

- **`NIGHTOWL_AUTO_BACKFILL`** (default `true`) — set `false` to skip the boot
  rollup backfill entirely on a host where you would rather run
  `nightowl:backfill-rollups` yourself at a chosen time. Opt-out rather than
  opt-in on purpose: a rollup table that exists but is EMPTY makes the
  dashboard's wide-range charts read zeros instead of falling back to raw, so
  defaulting this off would trade a bounded slowdown for silently wrong charts.
- **Two rollup diagnoses in the health payload.** Both states were previously
  visible only in the local agent log. `ROLLUP_REPAIR_PENDING` (warning) names
  the tables owing a repair, the earliest bucket owed, and the `--since` that
  fixes them; it clears itself once a `nightowl:migrate` fills the hole, because
  the drain re-reads the marker rather than remembering it.
  `ROLLUP_BACKFILL_PENDING` reports a boot reconciliation that has not finished —
  `info` while it is plausibly still running, escalating to `warning` after six
  hours, when the likelier explanation is that nobody is going to run it.

### Changed

- **`info`-level diagnoses no longer fire health alerts.** They still appear in
  the health payload and on `/status`, but an expected, self-clearing state does
  not belong in a Slack message titled "degraded" — and a suppressed alert has to
  suppress its recovery too, or an all-clear arrives for something that never
  raised. A no-op against 2.0.0 (which had no `info` diagnosis); it exists so
  that a routine upgrade cannot page anyone.

## [2.0.0] - 2026-07-29

Upgrading from 1.x: see [UPGRADE.md](UPGRADE.md). No action is required for the
normal case (`composer update` + `nightowl:migrate`, or just a daemon restart);
the notes below matter if you query the telemetry tables yourself, manage schema
by hand, or need to keep a rollback path open.

### Breaking

- **The drain writes the `_v2` raw tables. Anything reading the v1 tables
  directly stops seeing new rows.** This is the whole point of the release and
  it is invisible from the dashboard, which reads both families — but the
  database is yours, and a report, BI extract or `psql` query pointed at
  `nightowl_requests` returns only rows written before the upgrade, with no
  error and no empty-result signal. Every v1 value is byte-recoverable from a v2
  row plus the dictionaries; UPGRADE.md carries the join for each renamed or
  dictionary-encoded column. `NIGHTOWL_STORAGE_V2=false` reverts the drain to v1
  with no schema change, which is the escape hatch if you need reading time.

- **`nightowl:prune` retires empty v1 tables.** Once the storage-v2 fence is
  older than your raw retention window (`NIGHTOWL_RETENTION_DAYS`, 14 days by
  default), the v2 twin exists, and the v1 parent is EMPTY, prune issues
  `DROP TABLE` on the v1 parent. No rows are destroyed — emptiness is a
  precondition, not a consequence, and it doubles as the mixed-fleet guard: an
  older agent still draining into v1 keeps rows younger than retention, so the
  drop cannot fire underneath it. What breaks is anything holding a hard
  reference to the table NAME — a view, a foreign key, a materialised report, a
  monitoring query. `--keep-v1` disables the retirement permanently.

- **Downgrading to 1.x is only safe until the v1 tables are dropped.** A 1.x
  agent writes v1 exclusively and knows nothing about the v2 twins, so a
  rollback before EOL works — it resumes writing v1, and the dashboard keeps
  reading both. After a prune has retired the v1 parents, a 1.x agent's drain
  fails with `42P01` on every batch (rows stay buffered in SQLite; nothing is
  lost, but nothing lands until you roll forward again). If you want the
  rollback path held open past retention, pass `--keep-v1`.

- **The daemon applies pending migrations at startup by default.**
  `nightowl:agent` now runs the schema sync before it binds the ingest port when
  the nightowl migration history is behind (`NIGHTOWL_AUTO_MIGRATE`, default
  `true`). This is DDL executed by a long-running process rather than by your
  deploy step, which some environments deliberately do not permit — the
  database role you give the agent is the boundary. Set
  `NIGHTOWL_AUTO_MIGRATE=false` to keep schema changes in the deploy step; the
  daemon then warns instead of acting. Ignored entirely under the legacy
  `NIGHTOWL_RUN_MIGRATIONS=true` ride-along.

### Added

- **Storage format v2 — the definitive raw-telemetry format** (migrations
  000066/000067; ships together with the 000063–000065 concurrency-rollup /
  cache-key-pattern work below). Every raw table gains a `{table}_v2`
  partitioned twin born in its final shape:
  - the varchar wire `timestamp` becomes `ts_us bigint` (event time, µs,
    eventEpoch-guarded — closes the poison-varchar hole for good);
  - `trace_id`/`execution_id`/`job_id`/`attempt_id` become native `uuid`
    (16 B vs 37 B text), with `trace_id` stored NULL when it equals
    `execution_id` (the SDK duplicates them on request-sourced children;
    readers reconstruct via COALESCE — exact and lossless);
  - `group_hash`/`fingerprint` become 16-byte `bytea`;
  - repeated low-cardinality labels (environment, server, deploy, source,
    stage, connection, queue, status, event_type, method, level, channel,
    store, host, job_class, cache pattern) become small int ids into the new
    append-only `nightowl_dict_string`;
  - each distinct `(sql, file, line)` is stored ONCE in `nightowl_dict_sql`
    (measured production: 28M query rows/day over 225 distinct statements),
    route tuples in `nightowl_dict_route` (content-hash keyed — a renamed
    controller action creates a NEW entry, history never rewrites), exception
    stack traces deflated in `nightowl_dict_trace`;
  - request/command/job/log JSON blobs are agent-side-deflated `bytea`,
    stored NULL when they were only a `''`/`'{}'`/`'null'`/`'[]'` placeholder
    (server-side TOAST compression cannot fire on sub-2 KB rows — measured);
  - `v` (constant wire version) is not carried. Nothing else is dropped:
    every v1 value is byte-recoverable from a v2 row plus the dictionaries.

  The drain writes v2 whenever the parents exist (probe cached per process,
  fail-open toward v1; `NIGHTOWL_STORAGE_V2=false` is the operational kill
  switch — reverts to v1 with no schema change). Dictionary ids resolve in an
  autocommit warm pass BEFORE the batch transaction (a rollback can never
  poison the cache); concurrent workers converge via `ON CONFLICT DO NOTHING`
  + re-select. The cutover instant is recorded once as the `v2_fence` settings
  row; the API reads v1, v2, or their union per window. Rollups, issues,
  users and settings tables are unchanged (hex-text keys, bridged with
  `decode(...,'hex')`), and drain-time rollup rows are byte-identical in both
  modes (they aggregate the in-memory records, not the tables — pinned by
  test).

  `nightowl:backfill-rollups` scans both families through a per-table
  v1-compat projection UNION, so a window straddling the cutover backfills
  byte-identically to an all-v1 control (pinned, DDSketch bytes included).
  Partition maintenance, heal, prune and clear all operate on both families;
  the dictionaries are never pruned or cleared (append-only value stores).

- **v1 end-of-life, prune-integrated.** Once the `v2_fence` is older than the
  prune retention window AND a v1 table is empty AND its v2 twin exists,
  `nightowl:prune` drops the v1 parent (instant on empty; `--keep-v1` opts
  out). Emptiness doubles as the mixed-fleet guard: an old agent still
  draining v1 keeps rows younger than retention, so the drop can never fire
  under it. Post-EOL, every table list (maintenance, prune, migrate baseline,
  issue upserts, alert enrichment) probes existence rather than assuming v1.

- **`dict_trace` garbage collection** (`nightowl:gc-dict-traces`, migration
  000068). `nightowl_dict_trace` is the one dictionary that can be reclaimed:
  a deflated stack trace is only ever pointed at by `nightowl_exceptions_v2`
  rows, which age out at retention, so once every referencing exception is
  pruned the trace is dead weight (the other three dicts hold
  unbounded-lifetime keys and stay append-only forever). A trace is deleted
  only when it is BOTH unreferenced (anti-join on `trace_ref`) AND older than
  the quarantine window (`NIGHTOWL_DICT_TRACE_GC_QUARANTINE_DAYS`, default 7).
  The GC needs no lock: migration 000068 adds a `created_at` clock that the
  drain's warm pass now bumps (`ON CONFLICT DO UPDATE SET created_at = now()`)
  on every batch that references a trace — the collector always re-warms
  traces rather than trusting its LRU — so an in-flight trace is always young,
  and the single-statement DELETE re-checks `created_at` (a concurrent
  reference spares the row; a lost race re-creates it append-only with a fresh
  id, so no exception is ever left with a dangling `trace_ref`). Rides the
  existing `nightowl:prune` schedule; `--dry-run`, `--quarantine-days`,
  `--chunk`; a no-op on any tenant without the v2 twin or the 000068 column.

- **Boot-migrate: the daemon applies pending migrations at startup.** In the
  default (DB-history) model the schema is applied by `php artisan
  nightowl:migrate`, which is not wired into the host app's `php artisan
  migrate` — so the step was easy to forget after a `composer update`, and a
  behind schema means failed writes. Now, when the agent starts and its
  nightowl-DB history is behind, it runs the migrations itself before ingest
  begins (both drivers), so schema changes arrive with the code that needs
  them.
  - The schema half runs **synchronously, before the ingest port binds** — drain
    children cache rollup-table existence per process, so they must fork after
    the tables exist. It executes in a **child process under a hard deadline**
    (`NIGHTOWL_AUTO_MIGRATE_TIMEOUT`, 300s) which the agent kills on expiry:
    migration DDL can wait indefinitely on a lock, and that wait would sit in
    front of the ingest port with clients dropping telemetry after their 0.5s
    timeout. On timeout the agent starts on the old schema and the migrations
    stay pending for the next boot.
  - The **rollup backfill** half is detached to a background process (log:
    `storage/logs/nightowl-boot-migrate.log`, truncated per run) so a backfill
    measured in tens of minutes never delays ingest. It leaves a marker
    (`storage/nightowl/backfill-pending`) removed only on success; if it dies
    partway (OOM, statement timeout, database restart) a later boot re-spawns
    it. Without that, a rollup table created empty would serve zeroed charts
    indefinitely — the API read path prefers any rollup table that exists over
    raw.
  - Failure never blocks startup: warn-and-continue, matching the pre-2.0.0
    behaviour. Opt out with `NIGHTOWL_AUTO_MIGRATE=false` (e.g. a DB role
    without DDL rights); skipped under the legacy `NIGHTOWL_RUN_MIGRATIONS=true`
    ride-along, where the host app's `php artisan migrate` owns the schema.
- **Update-available warning.** A running daemon cannot pick up a `composer
  update` on its own — the loaded code and Composer's version metadata are
  frozen in-process, and drain workers respawn as fresh interpreters, so after
  an update a respawned worker runs new code under an old parent. The agent
  (async driver) now polls `vendor/composer/installed.php` every 5 minutes and
  logs a warning once it sees a newer installed version than the one it is
  running, confirmed on two consecutive polls so a half-written vendor tree
  never triggers it. It warns once per version, not once per poll.

  It **only warns** — the agent never exits on its own. Whether a supervisor
  would restart it depends on configuration the agent cannot verify and gets no
  feedback about; being wrong about that leaves you with no agent at all rather
  than one running slightly stale code. Restarting stays your call. Opt out with
  `NIGHTOWL_UPDATE_CHECK=false`.

- **`nightowl:test-alert` — send a real alert through the agent's own
  dispatchers.** The dashboard's "Send test" button, and every triage alert
  (`issue.resolved`, `issue.ignored`), are dispatched by nightowl-api through
  Symfony Mailer. `issue.new` and `issue.reopened` are dispatched by the agent
  on the customer's own machine, through its raw SMTP and HTTP. Two independent
  transports reading one config row — so a customer could test green in the
  dashboard, receive triage mail all day, and never once be told about a new
  exception, with a single `error_log` line as the only evidence. Nothing in the
  product exercised the second transport. This command does, and reports
  PASS/FAIL per channel with the failure reason attached. `--channel=` restricts
  it to one. It deliberately ignores `notify_events` so a transport failure
  cannot hide behind a filter, and warns when a channel that passed has
  `issue.new`/`issue.reopened` switched off — a pass that would still be silent
  in production is a misleading pass.

- **Request concurrency is now a rollup, so the peak-concurrency chart works
  over any range** (migration 000063, `nightowl_request_concurrency_rollups`).
  Concurrency used to be derived from raw requests at read time, which is why
  the dashboard clamped that chart to 6 hours — the scan got too expensive past
  it. A per-minute `(delta_sum, max_prefix)` fold is now maintained at drain
  time and the clamp is gone.

  The fold is written **only** by an exact window-function recompute
  (`ConcurrencyRollup::recompute`), shared verbatim by the drain's 60s
  maintenance tick (trailing 20 minutes) and by `nightowl:backfill-rollups`, so
  a live bucket and a backfilled one cannot disagree. An earlier per-batch
  incremental version was removed after review measured it **~70% low**: drain
  batches arrive in completion order, and appending deltas is only exact in
  event-time order. `nightowl:migrate` backfills the table on upgrade.

- **The cache rollup groups by key SHAPE instead of key instance**
  (migration 000065, `NIGHTOWL_CACHE_KEY_TEMPLATE`, default on). `user:8213:profile`
  and `user:9147:profile` were two rollup groups, so an app keying cache by id
  produced a rollup row per user and a cache page that was one long list of
  near-identical keys. uuid/ulid/hex/int/email/datetime segments now collapse to
  placeholders (`user:{int}:profile`) — the pattern is computed once in PHP at
  ingest and stored on the raw row, and the rollup SQL only reads it back, so
  the two forms are equivalent by construction rather than by two rules kept in
  sync.

  Raw cache events always keep the literal key, so nothing is lost. Turning the
  switch off makes NEW keys group literally but never rewrites history: a key's
  history spans both groupings across the flip until the patterned rows age out
  of rollup retention.

- **`(group_hash, created_at)` indexes on the mail and notification tables**
  (migration 000064) — the detail pages for a mail/notification group scanned
  without them. Built per-partition-child CONCURRENTLY then ATTACHed, with the
  already-attached check keyed on the CHILD via `pg_inherits ⋈ pg_index`: a
  name-based check wedges permanently (`55000` on every retry) once the hourly
  partition sweep creates a child that auto-inherits the parent shell under the
  default name.

- **Hourly table statistics reported to the platform**
  (`NIGHTOWL_TABLE_STATS`, default on; `NIGHTOWL_TABLE_STATS_INTERVAL`, 3600s).
  This exists so an operational problem in your database — a wedged drain, a
  rollup coverage hole, a disk filling up, a missing index — is diagnosed from
  our side instead of by asking you to run SQL against your own database.

  What it sends is **catalog and statistics views only**: per-table sizes, tuple
  counts, scan/write/vacuum/HOT/TOAST and wraparound counters; per-index usage,
  size and INVALID shells; cluster I/O (cache hits, temp spills, deadlocks,
  checkpoints, WAL, `pg_stat_io`, archiver, bgwriter); activity (connection
  states, lock waits, transaction ages, running vacuums); replication slots and
  retained WAL; ~25 server settings, version and extension inventory; plus
  agent-side observations SQL cannot make — signed clock skew, best-of-3 ping
  RTT, connect timing, transaction-pooler detection, standby detection,
  SSL-in-use. `pg_stat_statements` and query text are excluded **by principle
  and enforced by test** — a tenant database can be shared with your own
  application, and those are content, not counts. No query in the collector can
  read row contents. Row counts are still information (`nightowl_users`' count
  is your user count), which is why this ships alongside the privacy-policy
  update disclosing exactly this list; `NIGHTOWL_TABLE_STATS=false` opts out
  entirely.

  Coverage is catalog-driven (`relname LIKE 'nightowl%'` at runtime, partition
  children folded into logical parents), so a table added by any future release
  is covered the moment it exists — this codebase has been bitten by
  hand-listed table sets repeatedly, and a test creates an unregistered table
  and asserts it shows up. It runs on its own short-lived connection with its
  own `statement_timeout`, never the drain's, takes a per-tenant advisory lock
  so one worker reports per interval, never throws, and posts to its own
  endpoint so the health channel is never at risk.

### Changed

- **The two SMTP implementations became one (`SmtpClient`), and it now speaks
  SMTP correctly.** `AlertNotifier` (drain child) and `HealthAlertNotifier`
  (parent process) each carried a private copy, and the copies had drifted on
  three separate points — only one of which was reachable by a customer testing
  their setup. Merged, with four conformance fixes that each cost real mail:
  - **`EHLO` no longer announces a bare hostname.** RFC 5321 §4.1.1.1 requires
    an FQDN or an address literal; `nightowl` is neither. Exchange, Office 365
    and any Postfix with `reject_non_fqdn_helo_hostname` refuse the session
    outright. Now: configured name, else `gethostname()` when it is an FQDN,
    else an address literal from the local socket. `NIGHTOWL_SMTP_HELO`
    overrides.
  - **`EHLO` is re-issued after `STARTTLS`** (RFC 3207 §4.2). The server
    discards pre-upgrade capabilities, and relays commonly withhold `AUTH`
    until the channel is encrypted — so authentication was being attempted
    against a capability list that no longer applied.
  - **`AUTH` follows what the server advertises.** `PLAIN` when offered, else
    `LOGIN`; the old code always tried `LOGIN`, which fails on relays that
    offer only `PLAIN`. Credentials never reach an exception message.
  - **Messages carry `Date` and `Message-ID`** (RFC 5322 §3.6). Without them a
    relay accepts the message with a 250 and then drops or spam-files it, which
    is indistinguishable from delivery at the agent. Lines beginning with a
    period are now dot-stuffed (RFC 5321 §4.5.2) — previously the relay
    truncated the message at the first such line.

  Timeouts are configurable (`NIGHTOWL_SMTP_CONNECT_TIMEOUT`,
  `NIGHTOWL_SMTP_TIMEOUT`) and clamped to the dispatch budget, which rose from
  5s to 30s: 5s was less than one real TLS handshake plus `AUTH` plus `DATA`,
  so a healthy-but-unhurried relay consumed the budget for every channel
  behind it.

- **Webhook dispatch reads the response, and both notifiers share one
  implementation (`WebhookClient`).** Slack, Discord and plain webhooks were
  posted with `@file_get_contents` and the result discarded — a revoked Slack
  webhook, a mistyped URL 404ing, or a receiver returning 500 was
  indistinguishable from a delivered alert. A non-2xx now raises, carrying the
  status and the receiver's own first 200 bytes. URLs are redacted to
  `host/…` in every error: a Slack or Discord webhook path IS the credential,
  and these strings reach logs and console output. A channel configured with an
  empty URL raises too, instead of returning as though it had sent. The
  per-request timeout moved 3s → 10s. Both notifiers already isolate a
  channel's dispatch in try/catch, so drain and health-tick behaviour is
  unchanged — what changes is that failures now have a reason.

  Two details worth naming, because the obvious implementation gets both wrong.
  The transport reason is captured through a scoped `set_error_handler` rather
  than `@` plus `error_get_last()`: `error_get_last()` is only written by PHP's
  *internal* handler, and any userland handler returning non-false bypasses it
  — Laravel installs one, so inside a real host app the reason degraded to a
  placeholder ("connection failed" whether the relay refused, timed out or
  resolved nowhere) in exactly the environment this runs in. And PHP's own
  warning text embeds the URL verbatim, so surfacing it raw leaked the whole
  webhook secret back out through the failure path — past the redaction the
  success path applies. Both were found by running `nightowl:test-alert`
  against a dead port in a real Laravel app, not by reading the code.

### Fixed

- **A PostgreSQL outage could wedge the drain permanently — it did not recover
  when PostgreSQL came back.** When a backend dies underneath an open
  transaction, `PDO::rollBack()` throws AND leaves `inTransaction()` answering
  true; a guarded rollback stops the throw but does not release the flag. Four
  cleanup-tick maintenance paths caught, logged and returned at exactly that
  point, so the stranded handle was kept and reused. Every later
  `beginTransaction()` on it raised "There is already an active transaction",
  and the next batch tripped `DictionaryCache::warm`'s outside-the-batch
  assertion — a `RuntimeException`, not a connection error, so `write()`'s
  reconnect-and-retry path never fired and nothing ever replaced the handle.
  Measured against a real agent and a 90s outage: 60 seconds after PostgreSQL
  was healthy again the drain was still throwing that assertion ~15 times a
  second, burning ~30% of a core and draining nothing. Rows stayed safe in the
  SQLite buffer, so nothing was lost — but nothing arrived either, and the only
  cure was restarting the process. What a customer saw was a dashboard frozen at
  the moment of a blip they had already fixed. Each swallowing catch now
  disposes of the handle it is walking away from, and every entry point that
  owns no transaction yet drops a stranded one first as a backstop — needed
  because two of those paths fail WITHOUT throwing (the leftover sweep reports
  per-table failures through an out-param, so its catch never runs; a probe-based
  early return bypasses its catch), and because an empty buffer means no drain
  batch is coming to notice. Covered by a System test that kills backends
  *inside a transaction* while PostgreSQL stays up: the pre-existing chaos test
  stopped the whole container, which cannot strand a transaction because every
  handle dies before one is open, and it passed against the wedged build.

- **One failed probe could disable a tenant's rollups for the life of the
  process.** `rollupEnabled()` and `rollupColumnsPresent()` cached their answer
  permanently, which is right — a table's existence does not change under a
  running agent — but they cached it even when the probe had not ANSWERED. A
  probe that could not run because the connection was dead returned "missing",
  and that verdict stuck: rollups for the table were skipped for every later
  batch, under a log line telling the operator to run `nightowl:migrate` and
  restart, when the migration had never been the problem. Wide-range dashboard
  views read the rollup tier when the table exists, so the damage outlived the
  blip as a growing hole in exactly the views that cannot fall back to raw. Only
  a probe that returned an answer is cached now; one that threw skips the rollup
  for that batch (the raw write still lands) and is re-probed on the next.

- **The drain retried an unreachable PostgreSQL at full idle cadence,
  indefinitely.** With the buffer holding rows and the database unreachable, the
  loop reconnected every `NIGHTOWL_DRAIN_INTERVAL_MS` (100ms default) and logged
  the failure each time. Measured over one 90s outage: 1,369 near-identical
  `SQLSTATE[08006]` lines and ~27 CPU-seconds spent achieving nothing, since the
  rows were already durable in SQLite. On a small host the retry storm competed
  with the application it was meant to be observing, and the log noise buried
  the one line that explained it. Consecutive connection failures now back off
  exponentially from one idle interval to a 10s ceiling — capped so recovery
  stays prompt, since an uncapped ladder would be hours deep by the time the
  database returned — and the log collapses to roughly one line per doubling.
  The first batch to complete a round trip clears the backoff immediately.
  A reachable-but-refusing PostgreSQL (a missing table, a permission error) is
  deliberately excluded: that is not a reachability problem, and an operator
  fixing a migration should not also be waiting out a 10s sleep.

- **The SQLite buffer never returned disk space after draining.** SQLite reuses
  the pages a deleted row occupied but does not hand them back, so the buffer
  file was high-water-mark sized: a load test measured it at 3.8 GB holding
  126,728 pending rows, and it stayed 3.8 GB after those rows drained.
  Checkpointing does not help — it only resets the `-wal`. On a host where the
  buffer shares a volume with the database, one traffic spike is enough to fill
  the disk permanently, which is how that test ended: Postgres out of space, and
  a drain that could not recover. New buffers are created with
  `auto_vacuum=INCREMENTAL` and the drain trims the freelist on its cleanup tick
  in bounded slices, so the write lock is never held long enough to stall
  ingest, and only above a threshold — below it the freelist is cheap reuse for
  the next spike. Already-deployed buffers cannot be converted incrementally, so
  they get one full `VACUUM`, taken only when the buffer is idle. The pragma
  ordering is load-bearing and pinned by a test: `auto_vacuum` lives in the
  database header and can only be set before that header exists, so running it
  after `journal_mode=WAL` is silently a no-op — no error, and `PRAGMA
  auto_vacuum` keeps answering 0.

- **Wide list views could skip or repeat a row while a tenant held both storage
  families.** The API pages raw lists on `(created_at, id)`, and a COPY batch
  stamps one `created_at` across every row it writes — so `id` is what orders
  rows inside a batch, and it has to be unique across the union. Migration
  000067 setvals each `{table}_v2` sequence above its v1 twin's `MAX(id)`, which
  is true at the instant the v2 tables are created and stops being true
  afterwards: any v1 insert landing later walks the v1 sequence back into the v2
  range (a mixed fleet where one host still runs a pre-2.0.0 agent, or a
  `NIGHTOWL_STORAGE_V2=false` excursion). New migration 000069 re-applies the
  fence, and `nightowl:migrate` re-applies it on EVERY run — every deploy step,
  `nightowl:install`, and the daemon's boot auto-migrate — which is what heals
  the drift for as long as the fleet stays mixed. Sequences that still hold
  headroom are left alone. Ids already handed out cannot be renumbered: where
  the ranges have already overlapped, migrate names the affected tables and says
  so, and the overlap clears as those rows age out at retention.

- **A drain batch carrying more distinct SQL statements, routes or stack traces
  than the dictionary cache holds silently lost those links.** The dictionary
  maps trim to a cap, and the trim ran inside the warm pass's chunk loop — so a
  batch wide enough to overflow a cap evicted its OWN earliest-warmed ids before
  the write read them back. The collector deliberately omits every value the
  cache already holds, so an id evicted mid-batch is never re-warmed: the write
  bound NULL into a nullable `sql_id` / `route_id` / `trace_ref`, the dictionary
  row stayed behind with nothing pointing at it, and nothing errored. What a
  customer saw was a query with a blank SQL card, a request with no route, or an
  exception with no stack trace. Eviction now happens at exactly one point —
  where a batch's outcome is published — so nothing can be dropped between the
  collect and the write. Peak map size becomes the cap plus one batch's distinct
  values, released when that batch ends. An unresolved hash-keyed link is now
  logged as well (once per process per column) instead of stored in silence.

- **A drain wedged on the dictionary tables grew those maps without bound.** The
  warm pass runs before the batch transaction, outside the try/catch that
  publishes a batch's outcome — so a warm that died part-way (statement timeout,
  lost connection) left its succeeded chunks staged in the maps and reached
  neither the promote nor the discard path. That leaked one partial batch's
  worth of entries per attempt, in precisely the situation where the attempts do
  not stop. The warm now discards its staged ids before rethrowing.

- **`nightowl:backfill-rollups` deleted rollup buckets it then never
  repopulated, on any app whose `app.timezone` is not UTC.** Every column the
  command compares against is UTC wall time — raw `created_at` and rollup
  `bucket_start` are written with `gmdate()` and come back out of Postgres as
  naive `timestamp` strings — while `now()` and `Carbon::parse()` render in
  `app.timezone`. So the window bounds were in the wrong wall clock, and the
  concurrency pass mixed both clocks inside one chunk: the scan window came from
  Carbon strings, the DELETE and `HAVING` bucket window from `gmdate()` of the
  same epochs. The two disagreed by the UTC offset, and the offset-wide head of
  every chunk was cleared with no scanned rows left to rebuild it from. Every
  instant the command builds is now born UTC, and one helper derives all four
  bounds of a concurrency chunk from the same two epochs, so containment (bucket
  window inside scan window) holds by construction. `--since`/`--until` are read
  as UTC unless the string carries its own offset.

- **Boot-migrate went quiet on a tenant that had passed v1 end-of-life.** The
  daemon decides whether the schema is behind by first confirming the schema
  exists at all, and it probed `nightowl_requests` — a table `nightowl:prune`
  DROPs once the v1 EOL gates open. A post-EOL tenant therefore read as
  never-initialised, which is the one verdict meaning "not drift, don't warn":
  no boot migrate ran and nothing said why, so a new rollup migration would
  never land. Either family now counts, the same rule `nightowl:migrate` already
  uses to adopt an existing schema.

- **A resolved issue with no resolve-activity row could never reopen, and never
  alerted again.** The reopen cooldown reads the most recent
  `status_changed → resolved` row out of `nightowl_issue_activity`; when there
  was none — a resolve done outside the dashboard, or an issue row predating
  that table — it fell back to `nightowl_issues.updated_at`. But the drain's
  issue upsert rewrites `updated_at` on EVERY recurrence
  (`updated_at = EXCLUDED.updated_at` fires unconditionally, including on the
  batches where the status CASE leaves the row resolved), so the fallback
  advanced by exactly the interval it was being compared against: each new
  occurrence pushed the cooldown window forward past itself. Under any non-zero
  `NIGHTOWL_REOPEN_COOLDOWN_HOURS` the issue then stayed `resolved` forever and
  fired no `issue.reopened` alert, silently and permanently — the more often it
  recurred, the more firmly it was suppressed. There is now no fallback: an
  absent resolve instant reopens, the same verdict an unreadable one already
  produced. (Cooldown `0`, the shipped default, was never affected.)

- **Clock skew between the dashboard host and the agent host no longer swallows
  a reopen.** The resolve instant is stamped by whichever machine ran the
  dashboard write; `now` is the agent's own clock. A few minutes of NTP drift
  puts the resolve in the agent's future, and the unclamped negative elapsed
  failed even `>= 0` — so the documented "0 = always reopen, Sentry-style"
  went quiet for the length of the skew. Elapsed is clamped at zero: a resolve
  that has not happened yet by our clock has had zero cooldown time, not
  negative.

- **An unreachable agent socket can no longer surface inside a host request.**
  Single-agent mode (the default) assigned Nightwatch's `Ingest` straight onto
  `Core::$ingest`, so a failed acknowledgement from the agent — a dead socket, a
  wrong `NIGHTOWL_INGEST_URI` — threw out of `Ingest::write()` on whatever
  telemetry call happened to fill the buffer. Both modes now go through
  `Support\MultiIngest`, which absorbs transport failures on every method it
  fans out except `ping` — `write`, `writeNow`, `digest`, `flush` and the two
  sampling setters. Nightwatch guards the call sites it owns, so
  this was already contained in practice; it is contained by construction now,
  including on paths they haven't wrapped (a customer calling
  `Nightwatch::report()`, a future hook, an older SDK). Single-agent mode still
  transmits exactly once — the wrapper holds one ingest.
  (Reported and fixed by [@TheDaveKent](https://github.com/TheDaveKent), #4.)

- **`nightwatch:status` no longer reports a healthy agent over a dead socket.**
  `MultiIngest::ping()` swallowed transport failures like every other method,
  but `ping()` is a diagnostic, not telemetry: `nightwatch:status` decides
  reachability purely by whether it throws, so the one command a customer runs
  to check connectivity printed "the agent is running and accepting
  connections" and exited `0` while nothing was listening. It now probes every
  target and rethrows the first failure. Affected parallel mode since the
  adapter was introduced.

- **A swallowed transport failure still reaches
  `Nightwatch::handleUnrecoverableExceptionsUsing()`.** That callback is the
  documented place to observe telemetry failures, and the exceptions reached it
  on their own before the ingest was wrapped. `MultiIngest` now forwards
  everything it absorbs there in addition to the `error_log` line, so wrapping
  cannot silently unregister a hook the customer set.

**Upgrade steps:** `composer update nightowl/agent`, then restart the agent.
The `nightowl:migrate` step now happens on boot; running it in your deploy
pipeline is still worthwhile, since a pipeline run does the rollup backfill
where its latency is visible.

## [1.4.1] - 2026-07-20

### Fixed

- **`nightowl:partition` is safe to interrupt, safe to run twice, and refuses
  to run behind a transaction-mode pooler.** The conversion now holds one
  per-table *session* advisory lock across the whole run — prep included — so a
  second concurrent run (a deploy step racing an operator) is refused cleanly
  instead of dropping the winner's half-built table out from under it (42P01).
  A refused table keeps its rows and its primary key, and the command exits `3`
  (BUSY) rather than reporting a hard failure. Behind PgBouncer/Supavisor in
  transaction mode the session lock silently moves between backends; the
  command now detects that (backend pid read in the same statement that takes
  the key, re-checked per phase) and aborts the whole run with an explanatory
  message instead of converting unprotected.
- **A killed conversion no longer leaves a table that stops accepting writes.**
  A `{table}_hist_ck` stranded by a SIGKILLed run rejects every drained row
  (23514) once its boundary passes. The drain now sweeps for that leftover —
  and for an INVALID `{table}_id_created_at_pt` from a killed `CREATE INDEX
  CONCURRENTLY` — on **every** cleanup tick (~60s) instead of hourly, cutting
  the worst-case write outage from 61 minutes to about one. The sweep takes no
  locks and issues no DDL on a healthy tenant, holds ACCESS EXCLUSIVE one table
  at a time, and only ever touches a table no conversion holds.
- **Deadlock between the conversion swap and the drain.** `ALTER SEQUENCE ...
  OWNED BY` now runs after `LOCK TABLE`, closing a lock cycle against the
  drain's `nextval`: 21 of 40 conversions deadlocked before this change, 0 of 40
  after. The swap also carries its own `lock_timeout`, so it can no longer park
  a pending exclusive in front of every reader of a live table.
- **Missed hourly partition sweeps are retried instead of forfeited.** The
  drain used to spend its hour whether or not the child sweep actually ran, so a
  worker that lost the advisory lock created no children and did not look again
  for an hour — repeatable across workers until every row landed in the DEFAULT
  partition, which prune can only delete row by row. The gate now advances only
  on a sweep that committed.
- **Failures are classified correctly.** A rolled-back swap no longer masks the
  real error with "server closed the connection unexpectedly", so a retryable
  contention error is reported as BUSY (`3`) rather than a hard failure. New
  exit code `4` (INCOMPLETE) means every conversion landed but some daily
  children did not — nothing is lost; a running agent creates them within the
  hour.

## [1.4.0] - 2026-07-19

### Added

- **Worker saturation alert.** When the tenant configures a per-environment
  HTTP worker count (`http_workers` setting) and enables the trigger
  (`worker_saturation` setting: `enabled` / `percent` 10–200 /
  `sustained_minutes` 1–60), the drain's cleanup tick evaluates average request
  concurrency against the worker count once per minute, reading occupancy back
  from `nightowl_request_rollups` (SUM(total_duration) per completed minute).
  When it holds at/above `percent`% for `sustained_minutes` consecutive
  completed minutes, the agent opens a `performance` issue (subtype
  `worker_saturation`, fingerprint `worker_saturation` per environment) through
  the existing threshold-issue protocol — dedup, resolve/reopen with
  `NIGHTOWL_REOPEN_COOLDOWN_HOURS`, and `issue.new` / `issue.reopened` channel
  dispatch all included. Each agent evaluates only its own environment
  (advisory-locked, one evaluation per tenant per minute across drain workers).
  A minute with no traffic breaks the streak; unconfigured or out-of-bounds
  settings disable the check entirely. Both settings are read with the same
  cached 30s `updated_at` poll as thresholds, so dashboard edits apply without
  an agent restart. No new env vars, no schema changes.

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
