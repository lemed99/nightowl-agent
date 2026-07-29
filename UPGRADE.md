# Upgrading NightOwl Agent

## 1.x → 2.0

**The short version.** For most installations there is nothing to do beyond the
usual:

```bash
composer update nightowl/agent
php artisan nightowl:migrate
# restart the daemon (supervisor/systemd/container) so it picks up the new code
```

The dashboard keeps working through the upgrade — it reads both storage formats
and stitches a window that straddles the cutover. No telemetry is lost, no
backfill is required, and there is no downtime window to schedule.

Read the rest of this document if any of these apply to you:

- you query the `nightowl_*` tables directly (reports, BI, `psql`, Metabase)
- your database role for the agent is restricted, or DDL is confined to your
  deploy step
- you want to keep a rollback path to 1.x open

---

### What actually changed

2.0 introduces **storage format v2**. Each of the eleven raw telemetry tables
gains a `_v2` twin, born in the format the schema should have had from the
start, and the drain writes the twin from the moment it exists:

| v1 | v2 |
| --- | --- |
| `nightowl_requests` | `nightowl_requests_v2` |
| `nightowl_queries` | `nightowl_queries_v2` |
| `nightowl_exceptions` | `nightowl_exceptions_v2` |
| `nightowl_commands` | `nightowl_commands_v2` |
| `nightowl_jobs` | `nightowl_jobs_v2` |
| `nightowl_cache_events` | `nightowl_cache_events_v2` |
| `nightowl_mail` | `nightowl_mail_v2` |
| `nightowl_notifications` | `nightowl_notifications_v2` |
| `nightowl_outgoing_requests` | `nightowl_outgoing_requests_v2` |
| `nightowl_scheduled_tasks` | `nightowl_scheduled_tasks_v2` |
| `nightowl_logs` | `nightowl_logs_v2` |

Rollups, issues, users, alert channels and settings are **unchanged**.

Nothing is thrown away. Every value a v1 row carried is byte-recoverable from
its v2 row plus the dictionary tables — the format trades width for joins, it
does not drop information. The one column not carried is `v`, the wire-protocol
version, which was the same constant on every row.

The moment of cutover is recorded once, as the `v2_fence` row in
`nightowl_settings`.

---

### If you query the telemetry tables yourself

**This is the part that breaks silently.** After the upgrade,
`SELECT ... FROM nightowl_requests` still succeeds and still returns rows — just
never any new ones. There is no error to catch and no empty result to notice.
Point your queries at the `_v2` tables and add the joins below.

#### Timestamps

The varchar `timestamp` column is gone. Event time is `ts_us` — a `bigint`,
microseconds since the epoch:

```sql
SELECT to_timestamp(ts_us / 1000000.0) AS event_time FROM nightowl_requests_v2;
```

`created_at` still exists and still means what it always did: the instant the
drain wrote the row, shared across a COPY batch. If you were using it as event
time, `ts_us` is what you actually wanted.

#### Labels (environment, server, deploy, method, queue, status, …)

Repeated low-cardinality strings became small integer ids into
`nightowl_dict_string`. Join on the id — it is the primary key, so you do not
need to filter on `kind`:

```sql
SELECT r.url,
       r.status_code,
       env.value    AS environment,
       srv.value    AS server,
       m.value      AS method
  FROM nightowl_requests_v2 r
  LEFT JOIN nightowl_dict_string env ON env.id = r.environment_id
  LEFT JOIN nightowl_dict_string srv ON srv.id = r.server_id
  LEFT JOIN nightowl_dict_string m   ON m.id   = r.method_id;
```

The columns backed by `nightowl_dict_string` are:

`environment_id`, `server_id`, `deploy_id`, `method_id`, `status_id`,
`queue_id`, `connection_id`, `connection_type_id`, `store_id`, `channel_id`,
`host_id`, `level_id`, `event_type_id`, `job_class_id`, `pattern_id`,
`execution_source_id`, `execution_stage_id`.

**Do not assume every `_id` column is a dictionary reference.** `user_id` and
`notifiable_id` are inline `varchar` and always were; `trace_id`,
`execution_id`, `job_id` and `attempt_id` are `uuid`; `route_id` and `sql_id`
point at their own dictionaries, not this one. Joining any of those against
`nightowl_dict_string` matches unrelated rows by numeric coincidence rather than
failing, so it is worth getting right.

#### Routes, SQL and stack traces

Three columns point at their own dictionaries rather than `dict_string`:

```sql
-- routes: nightowl_requests_v2.route_id
LEFT JOIN nightowl_dict_route dr ON dr.id = r.route_id
-- dr.method, dr.domain, dr.path, dr.name, dr.action, dr.methods

-- SQL: nightowl_queries_v2.sql_id
LEFT JOIN nightowl_dict_sql s ON s.id = q.sql_id
-- s.sql, s.file, s.line

-- stack traces: nightowl_exceptions_v2.trace_ref
LEFT JOIN nightowl_dict_trace t ON t.id = e.trace_ref
-- t.trace_z — deflated, see below
```

`nightowl_dict_route` is content-hash keyed and append-only: renaming a
controller action creates a NEW row rather than rewriting history, so a route
name in an old row stays the name that was live when the request was served.

#### Ids and hashes

- `trace_id`, `execution_id`, `job_id`, `attempt_id` are native `uuid`. Cast
  with `::text` for the v1 spelling.
- `trace_id` is stored **NULL when it equals `execution_id`** (the SDK
  duplicates them on request-sourced children). Reconstruct with
  `COALESCE(trace_id, execution_id)` — exact, not approximate.
- `group_hash` and `fingerprint` are 16-byte `bytea`. For the v1 hex string:
  `encode(group_hash, 'hex')`.

#### Compressed blobs

Columns ending in `_z` (`context_z`, `headers_z`, `payload_z`, `trace_z`) are
**raw DEFLATE** (RFC 1951, no zlib wrapper) — `gzdeflate()` in PHP terms.
PostgreSQL has no built-in inflate, so decompress on the client:

```php
$json = gzinflate($row['payload_z']);          // PHP
```

```python
json = zlib.decompress(row["payload_z"], -15)  # Python: negative wbits = raw
```

A `_z` column is `NULL` when the original value was only a placeholder — `''`,
`'{}'`, `'null'` or `'[]'`. Treat NULL as "empty", not as "missing".

#### Rollups are unaffected

If your reporting reads `nightowl_*_rollups`, nothing changes. Rollup rows are
aggregated from the in-memory records, not from the tables, and are
byte-identical in both storage modes.

---

### v1 table retirement

`nightowl:prune` will eventually `DROP` the v1 parent tables. It fires only when
**all four** hold:

1. `--keep-v1` was not passed;
2. the `v2_fence` is older than your raw retention window
   (`NIGHTOWL_RETENTION_DAYS`, 14 days by default);
3. the v2 twin exists;
4. **the v1 parent is empty.**

Gate 4 is the important one: emptiness is a precondition, not a consequence.
Prune never empties a v1 table in order to drop it, so **no rows are destroyed
by this**. It also doubles as the mixed-fleet guard — a host still running a 1.x
agent keeps writing v1 rows younger than retention, which blocks the drop for
everyone.

What does break is a hard reference to the table **name**: a view, a foreign
key, a materialised report, a monitoring query, a Grafana panel. Audit those
before your retention window elapses, or opt out permanently:

```bash
php artisan nightowl:prune --keep-v1
```

To check where you stand:

```sql
SELECT value AS v2_fence FROM nightowl_settings WHERE key = 'v2_fence';
SELECT to_regclass('nightowl_requests') AS v1_still_present;
```

---

### Rolling back to 1.x

Supported, with one deadline.

A 1.x agent writes v1 exclusively and does not know the v2 twins exist. Rolling
back **before** v1 retirement works cleanly: it resumes writing v1, and the
dashboard keeps reading both families, so history stays whole. The extra
migration-history rows from 2.0 are inert — Laravel's migrator only runs what is
pending.

Rolling back **after** prune has retired the v1 parents does not work: the drain
fails with `42P01` (undefined table) on every batch. Nothing is lost — the rows
stay in the SQLite buffer and drain once the schema can accept them — but
nothing lands in the meantime, and there is no command that brings a retired v1
table back. `nightowl:migrate` will not do it: those migrations are already
recorded as run, so the migrator considers them settled and skips them. Rolling
forward to 2.x again is the fix.

If you want the rollback path open past your retention window, run prune with
`--keep-v1` for as long as you need it.

There is also a softer lever that needs no rollback at all:

```dotenv
NIGHTOWL_STORAGE_V2=false
```

This reverts the drain to writing v1 with **no schema change** — the v2 tables
stay where they are and stay readable. Use it if you need time to migrate your
own queries.

---

### Schema changes now run at daemon startup

`nightowl:agent` applies pending migrations before it binds the ingest port,
whenever the nightowl migration history is behind. This is on by default
(`NIGHTOWL_AUTO_MIGRATE=true`).

It exists because a rollup or storage migration that has not run makes the
daemon write into tables that do not exist yet, and the failure is quiet. The
schema run is bounded by `NIGHTOWL_AUTO_MIGRATE_TIMEOUT` (300s) in a
deadline-killed child process, so a locked tenant database can never hold the
ingest port unbound; the rollup backfill is detached to the background with a
completion marker retried on the next boot.

If DDL belongs to your deploy step and not to a long-running process — a real
policy in plenty of shops, and the database role you grant the agent is where
you enforce it — turn it off:

```dotenv
NIGHTOWL_AUTO_MIGRATE=false
```

The daemon then warns that the schema is behind and carries on, and
`nightowl:migrate` in your deploy step stays the only thing that writes DDL.

---

### Verifying the upgrade

```bash
php artisan nightowl:migrate     # idempotent; safe to re-run
```

Then confirm the drain is writing v2:

```sql
SELECT count(*) FROM nightowl_requests_v2
 WHERE created_at > (now() AT TIME ZONE 'utc') - interval '5 minutes';
```

(`created_at` is a naive `timestamp` holding UTC, so the `AT TIME ZONE 'utc'`
matters — a bare `now()` renders in the session time zone and will report zero
on any server that is not on UTC.)

A non-zero count means the cutover took. If it is zero while traffic is flowing,
check the daemon log for a storage-v2 probe failure, and confirm
`NIGHTOWL_STORAGE_V2` is not set to `false`.

---

### Also worth knowing

Alert email now sends an RFC-conformant `EHLO` — a configured name, else the
machine FQDN, else an address literal — instead of a bare hostname. This fixes
delivery through Exchange, Office 365 and Postfix with
`reject_non_fqdn_helo_hostname`. If your relay allowlists the old bare value,
pin it explicitly:

```dotenv
NIGHTOWL_SMTP_HELO=agent.example.com
```

The SMTP dispatch budget also rose from 5s to 30s, because 5s was less than one
real TLS handshake plus `AUTH` plus `DATA`.
