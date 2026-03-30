# NightOwl Agent — Deep Architectural Review

## 1. Executive Summary (Top 5 Critical Issues)

**C1. SIGCHLD handler early-return drops sibling worker deaths** (`AsyncServer.php:219,232`). Two `return` statements inside the `while (pcntl_waitpid)` loop cause the handler to exit without reaping other dead workers. SIGCHLD is coalesced on Linux — if worker 0 and worker 1 die within the same signal delivery window, only worker 0 is reaped. Worker 1 becomes a zombie with its claimed rows permanently stuck.

**C2. Blocking I/O in drain worker during alert dispatch** (`AlertNotifier.php:350,369`). `file_get_contents()` (HTTP) and `stream_socket_client()` (SMTP) are synchronous. If 3 alert channels are configured and 5 new issues trigger in one batch, the drain worker blocks for up to 75 seconds (5s timeout × 3 channels × 5 issues). Backlog grows unbounded during this window.

**C3. No gzip decompression size limit** (`PayloadParser.php:76-81`). A 10MB compressed payload (allowed by `MAX_PAYLOAD_BYTES`) can decompress to 100MB+. With `memory_limit = -1`, a crafted payload can exhaust process memory. This is a practical DoS vector since the agent accepts connections from customer apps.

**C4. Multi-worker drain metrics only read worker 0** (`AsyncServer.php:189`, `MetricsCollector.php:171`). In `NIGHTOWL_DRAIN_WORKERS=4` mode, health diagnostics (drain rate, PG latency, batches_failed) reflect only 25% of actual drain activity. DRAIN_STOPPED diagnosis fires falsely when worker 0 is idle but workers 1-3 are draining. Operators get misleading health status.

**C5. `users_count` inflates indefinitely in issue upserts** (`RecordWriter.php:371`). The `ON CONFLICT` clause adds `EXCLUDED.users_count` to the running total. The same user appearing in 100 batches adds 100 to the count. For high-traffic exceptions, `users_count` will be orders of magnitude higher than actual unique users.

---

## 2. Strengths (What is Well Designed)

**SQLite as a crash-safe buffer is the right call.** WAL mode gives you write concurrency (parent writes, children drain), crash recovery (incomplete transactions roll back on reopen), and zero-dependency deployment. The PRAGMA ordering (busy_timeout before journal_mode=WAL) shows someone read the SQLite corruption docs carefully.

**Fork safety is exemplary.** Closing the PDO handle before fork (line 286) and recreating after is the exact mitigation for sqlite.org/howtocorrupt.html §2.7. The retry loop for `journal_mode=WAL` (10 attempts with backoff) handles the real-world race where the child is simultaneously opening the file. This is one of the best PHP fork+SQLite implementations I've seen.

**The COPY protocol choice is correct and well-implemented.** Using `pgsqlCopyFromArray` for high-volume tables and reserving INSERT for upsert tables (exceptions, users) is the right split. The TSV escaping covers all edge cases (backslash, tab, newline, CR). The `synchronous_commit = off` is safe here because SQLite is the durability layer.

**Two-layer backpressure is thoughtful.** Inline per-chunk memory guards (zero latency, line 130-141) catch burst scenarios, while the 5-second periodic monitor catches slow-drain pressure. Using pending row count instead of WAL file size (because WAL never shrinks during operation) shows understanding of SQLite internals.

**Zero-copy append via `appendRaw()` is a meaningful optimization.** Skipping `json_encode` on the hot path (~30-50us per payload at 13K payloads/s) saves ~0.5s/s of CPU time. The branch on `$wasRedacted` to choose between `appendRaw` and `append` is clean.

**The diagnosis system with anti-flapping is production-grade.** Debounce at 2 ticks (20s) prevents transient blips from alerting. Resolution requires 3+ ticks to distinguish genuine recovery from oscillation. Resolved diagnoses have a 5-minute retention with GC. This matches patterns from mature monitoring systems (Prometheus alertmanager, PagerDuty flap detection).

**Adaptive health reporting intervals** (30s healthy, 10s degraded, 5s critical) with recursive timer scheduling (not `addPeriodicTimer`) is the correct approach. It adjusts on every report rather than waiting for the period to expire.

---

## 3. Critical Risks (Must Fix)

### 3.1 SIGCHLD Handler Drops Sibling Deaths

**File:** `AsyncServer.php:201-238`

```php
while (($result = pcntl_waitpid(-1, $status, WNOHANG)) > 0) {
    // ...
    if ($this->shuttingDown) {
        return;  // exits entire handler, skips remaining dead workers
    }
    // ...
    if ($elapsed < self::RESTART_COOLDOWN) {
        // ...
        return;  // exits entire handler, skips remaining dead workers
    }
}
```

**How it fails:** Workers 0 and 1 crash within 50ms. Linux delivers one SIGCHLD. The handler reaps worker 0, schedules cooldown restart, then `return`s. Worker 1 is never reaped. No future SIGCHLD will arrive (both workers are already dead). Worker 1's claimed rows (synced=101) remain stuck forever — the backlog grows silently while the diagnosis system reports everything normal.

**Fix:** Replace both `return` statements with `continue`.

### 3.2 Blocking Alert Dispatch in Drain Worker

**File:** `AlertNotifier.php:350` (httpPost), `AlertNotifier.php:369` (smtpSend)

The drain worker calls `flushNotifications()` after every batch commit. Each notification does synchronous I/O:
- `file_get_contents()` with 5s timeout for Slack/Discord/Webhook
- `stream_socket_client()` + SMTP protocol with 5s timeout for Email

**How it fails:** Error storm triggers 10 new exceptions -> 10 new issues -> 10 notifications x 3 channels = 30 blocking HTTP/SMTP calls. At 5s timeout each, the drain worker is blocked for up to 2.5 minutes. Meanwhile, 100K+ payloads accumulate in SQLite, backpressure activates, and the monitoring tool stops monitoring.

**Fix options (in order of preference):**
1. Move notification dispatch to the parent process via IPC (write notification queue to a separate SQLite table or temp file, parent dispatches async via React HTTP client)
2. Use `stream_select` with timeout multiplexing in a non-blocking dispatch loop
3. At minimum: fire-and-forget with `stream_set_blocking($socket, false)` and cap total notification time per batch to 2s

### 3.3 Gzip Bomb Vector

**File:** `PayloadParser.php:76-81`

```php
if ($this->gzipEnabled && strlen($payload) >= 2 && $payload[0] === "\x1f" && $payload[1] === "\x8b") {
    $decompressed = @gzdecode($payload);  // no size limit
```

**How it fails:** A client sends a 10MB gzip payload (passes MAX_PAYLOAD_BYTES). The decompressed output is 1GB+ (gzip ratio 100:1 is achievable with repetitive data). `gzdecode()` allocates the full decompressed buffer in memory. With `memory_limit = -1`, PHP doesn't stop it. The process OOMs or the kernel kills it.

**Fix:** Use `inflate_init()` + `inflate_add()` in a loop with a running byte counter, aborting when decompressed size exceeds a limit (e.g., 50MB or `MAX_PAYLOAD_BYTES * 20`):

```php
$ctx = inflate_init(ZLIB_ENCODING_GZIP);
$decompressed = '';
$maxDecompressed = self::MAX_PAYLOAD_BYTES * 20;
foreach (str_split($payload, 8192) as $chunk) {
    $decompressed .= inflate_add($ctx, $chunk);
    if (strlen($decompressed) > $maxDecompressed) {
        return null; // reject oversized
    }
}
```

---

## 4. Performance Bottlenecks (with Concrete Fixes)

### 4.1 Exception INSERT Loop Under Error Storm

**File:** `RecordWriter.php:308-348`

Each exception is inserted individually via prepared statement execute in a loop. During an error storm (500 exceptions per batch), this is 500 sequential INSERT + 500 issue upsert executions. At ~0.5ms per INSERT, that's 500ms for exceptions alone (the COPY tables process 5000 rows in ~100ms).

**Fix:** Batch exceptions into a single multi-row INSERT:

```php
// Build VALUES clause for all exceptions at once
$values = [];
$params = [];
foreach ($records as $i => $r) {
    $values[] = "(:trace_id_{$i}, :timestamp_{$i}, ...)";
    $params["trace_id_{$i}"] = $r['trace_id'] ?? null;
    // ...
}
$sql = "INSERT INTO nightowl_exceptions (...) VALUES " . implode(', ', $values);
$pdo->prepare($sql)->execute($params);
```

Or use COPY for exceptions too and handle the issue upsert separately via a temp table + `INSERT INTO nightowl_issues SELECT ... FROM temp ON CONFLICT ...`.

### 4.2 json_decode per Row in Drain Worker

**File:** `DrainWorker.php:182`

Every SQLite row gets `json_decode`'d in the drain worker to extract records. For 5000 rows x ~3KB average, that's ~15MB of JSON parsing per batch. PHP's json_decode is ~200MB/s, so this costs ~75ms per batch.

**Not critical,** but for 10x scale: consider storing records pre-grouped by type in SQLite (using a `record_type` column), allowing the drain worker to COPY without decode+regroup. Trade-off: complicates the ingest hot path.

### 4.3 `pendingCount()` Under Extreme Backlog

**File:** `SqliteBuffer.php:193`

```php
return (int) $this->pdo->query('SELECT COUNT(*) FROM buffer WHERE synced = 0')->fetchColumn();
```

With the `(synced, id)` index, SQLite must walk the index counting entries where `synced = 0`. At 100K+ rows, this is O(n) in the number of pending rows. Called every 5s from the parent and on every health API request.

**Fix for extreme scale:** Maintain an in-memory counter in the parent process (increment on `appendRaw`, decrement when drain metrics report rows drained). Use the SQL COUNT only for periodic reconciliation (every 60s).

### 4.4 Drain Ring Buffer Distribution is Lossy

**File:** `MetricsCollector.php:194-199`

```php
$perSecond = (int) round($delta / 5);
for ($i = 0; $i < 5; $i++) {
    $idx = ($this->drainRingIdx - 1 - $i + self::RING_SIZE) % self::RING_SIZE;
    $this->drainRing[$idx] += $perSecond;
}
```

The `(int) round($delta / 5)` truncation loses remainders. If 13 rows were drained in 5s, `perSecond = 3`, so only 15 is distributed (2 rows lost). Over time, the drain rate metric underreports by ~10% at low throughput.

**Fix:** Use modular distribution: distribute `$delta % 5` extra rows across the first N slots.

---

## 5. Concurrency & Data Safety Issues

### 5.1 `claimBatch` Atomicity (Safe)

`SqliteBuffer::claimBatch()` uses a single SQL statement:
```sql
UPDATE buffer SET synced = {claimValue} WHERE id IN (
    SELECT id FROM buffer WHERE synced = 0 ORDER BY id ASC LIMIT {limit}
)
```

This IS atomic in SQLite because a single statement is always atomic, and WAL mode serializes all writes. Two workers executing this concurrently will be serialized by SQLite's write lock. The second worker's subquery will see rows already claimed by the first. **Safe.**

### 5.2 Write-Then-Mark Duplicate Window

Between PG `COMMIT` and SQLite `markSynced`, the process can be killed. On restart, those rows are re-drained. For COPY tables (no dedup key), this creates duplicate rows. The window is small (~1ms for `markSynced`), and monitoring data tolerates duplicates, but:

**Risk quantification:** At 100 batches/day with a 1-in-10,000 chance of kill during the 1ms window, expect ~1 duplicate batch per 100 days. Each batch = 5000 rows. Acceptable for this use case.

### 5.3 Reflection-Based Worker ID Assignment

**File:** `AsyncServer.php:323-326`

```php
$ref = new \ReflectionProperty($worker, 'workerId');
$ref->setValue($worker, $workerId);
```

If `workerId` property is renamed or the class structure changes, this fails silently at runtime with no compile-time or test-time warning. Use a setter or a factory method instead.

### 5.4 Shutdown Loop Blocks Event Loop

**File:** `AsyncServer.php:436-446`

The `shutdown()` method uses a synchronous `while` loop with `usleep(100_000)` to wait for children. Since shutdown runs inside a signal handler callback on the event loop, this blocks all event processing for up to 10 seconds. Any pending drain metrics reads, timer callbacks, or health API requests are frozen.

This is likely intentional (server is already closed), but it means the health API returns stale data during shutdown. A cleaner approach would use `addPeriodicTimer(0.1, ...)` to poll child status asynchronously while the loop runs.

---

## 6. Scalability Assessment

### Current Limits

| Dimension | Single Instance Limit | Bottleneck |
|---|---|---|
| Ingest throughput | ~13,400 payloads/s | Event loop CPU (single-threaded) |
| Drain throughput | ~7,600 rows/s per worker | PG COPY + network latency |
| Concurrent connections | ~5,000 (fd limit) | File descriptor limit + per-conn memory |
| SQLite buffer depth | ~100K rows (configurable) | WAL file size + disk I/O |
| Memory | ~300MB baseline (SQLite cache + mmap) | RSS staircase (Zend MM) |

### 10x Scale (50K req/s customer app)

Achievable with current architecture:
- 4 agent instances via `SO_REUSEPORT` (each handles ~13K payloads/s)
- 2 drain workers per instance (8 total, ~60K rows/s aggregate drain)
- PgBouncer for connection pooling (8 PG connections instead of 8 direct)
- Separate SSD for SQLite buffer files

**Limiting factor:** PG write throughput. 50K payloads/s x ~5 records/payload = 250K records/s. With COPY and `synchronous_commit=off`, a tuned PostgreSQL can handle this on modern hardware, but you're at ~40% of single-node PG capacity.

### 100x Scale (500K req/s)

**Architecture breaks.** Problems:
1. 40 agent instances on one host is impractical (40 SQLite files, 40x300MB = 12GB baseline memory)
2. Single PostgreSQL can't absorb 2.5M records/s via COPY
3. SQLite WAL checkpointing becomes the bottleneck (40 processes competing for disk I/O)

**What you'd need:**
- Replace SQLite with a shared buffer (Redis streams, or in-memory ring buffers with periodic batch write)
- PostgreSQL partitioning (daily/hourly partitions) + multiple read replicas
- Or: externalize to Kafka -> dedicated drain workers -> PostgreSQL, decoupling ingest from drain entirely
- Consider TimescaleDB or ClickHouse for the storage layer at this scale

---

## 7. Suggested Architectural Improvements

### 7.1 Async Notification Dispatch (High Impact, Medium Effort)

Move alert notification from the drain worker to the parent process. The drain worker writes new issue hashes to a small SQLite table or temp file. The parent's event loop reads this and dispatches via `React\Http\Browser` (already used in `HealthReporter`). This eliminates all blocking I/O from the drain path.

### 7.2 Multi-Worker Metrics Aggregation (Medium Impact, Low Effort)

Read metrics files from all workers, not just worker 0:

```php
for ($i = 0; $i < $this->drainWorkerCount; $i++) {
    $metricsPath = $this->sqlitePath . ".drain-metrics-{$i}.json";
    // ... read and aggregate
}
```

Sum `rows_drained` and `batches_failed`. Average `pg_latency_ms`. Use min `updated_at` for staleness detection.

### 7.3 Decompression Size Limit (High Impact, Low Effort)

Add a `MAX_DECOMPRESSED_BYTES` constant (e.g., 50MB) and use incremental decompression via `inflate_init()` + `inflate_add()`.

### 7.4 Connection Rate Limiting (Medium Impact, Low Effort)

Track connections per source IP in the event loop. Reject new connections from IPs exceeding a threshold (e.g., 100 connections/second). This prevents a single misbehaving client from exhausting file descriptors.

---

## 8. "If I Had to Redesign This"

I wouldn't fundamentally change the architecture. The two-tier buffer (SQLite -> PostgreSQL) is the right design for a self-hosted monitoring agent where you can't assume Kafka/Redis availability. The key changes I'd make:

1. **Replace file-based IPC with Unix domain sockets.** The temp-file-with-rename approach works but adds filesystem overhead and introduces the 5-second staleness window. A UDS pair per worker would give sub-millisecond bidirectional communication: drain metrics upstream, configuration changes downstream.

2. **Make the drain worker event-loop-based.** Instead of a `while(true) + usleep()` loop, run a ReactPHP loop in the child. This gives you non-blocking PG writes (via `react/pgsql`), non-blocking notifications (via `React\Http\Browser`), and timer-based checkpoint scheduling. The complexity increase is modest (DrainWorker already knows about the pattern from AsyncServer), and it eliminates all blocking I/O from the process.

3. **Add a dedicated notification worker.** Instead of alert dispatch inline with drain, fork a third process type that reads a notification queue (SQLite table or UDS) and dispatches asynchronously. This completely decouples throughput from alerting latency.

4. **Structured logging to stdout.** Replace `error_log()` scattered throughout with a structured JSON logger that writes to stdout (consistent with 12-factor apps). Include component tags, worker IDs, and structured context. This makes log aggregation, filtering, and alerting trivial.

---

## 9. Quick Wins (High Impact, Low Effort)

| Fix | Files | Impact | Effort |
|---|---|---|---|
| Replace `return` with `continue` in SIGCHLD handler | `AsyncServer.php:219,232` | Prevents zombie workers and stuck rows in multi-worker mode | 2 lines |
| Aggregate all worker metrics (not just worker 0) | `MetricsCollector.php:169-206`, `AsyncServer.php:189` | Accurate health diagnostics in multi-worker mode | ~20 lines |
| Add `MAX_DECOMPRESSED_BYTES` check | `PayloadParser.php:76-81` | Closes DoS vector | ~15 lines |
| Replace reflection with setter for worker ID | `AsyncServer.php:323-326`, `DrainWorker.php` | Prevents silent runtime failures on refactor | ~10 lines |
| Cap notification dispatch time per batch | `AlertNotifier.php:flushNotifications` | Prevents drain stall during notification bursts | ~10 lines |
| Fix drain ring buffer distribution remainder | `MetricsCollector.php:195` | Accurate drain rate reporting at low throughput | 3 lines |

---

## 10. Observability Gaps

- No per-record-type throughput metrics (how many requests vs queries vs exceptions)
- No COPY-specific timing (which table is slowest?)
- No queue depth trend (is backlog growing or shrinking over time?)
- The ring buffer approach gives 1-minute averages but no percentiles
- No long-term metrics export (Prometheus, StatsD, etc.)

---

## 11. Testing Gaps

- No tests for the SIGCHLD handler bug (multi-worker death scenarios)
- No concurrent multi-worker claiming tests
- No gzip bomb / decompression size tests
- No connection flooding tests
- No memory leak tests (long-running under sustained load)
- No test for blocking I/O impact in AlertNotifier
- No fork-safety invariant tests (SQLite PDO closure before fork)
- No WAL checkpoint strategy tests (PASSIVE -> TRUNCATE transition at 200MB)

---

## Conclusion

This is a well-engineered system for its scale target. The core design decisions (SQLite WAL buffer, COPY protocol, fork safety, two-layer backpressure) are sound and show deep understanding of the underlying systems. The critical issues above are real but bounded — none will cause data loss, and all are fixable without architectural changes. The system is ready for production at the 2K-5K req/s tier; the improvements above would extend that to 50K+ before needing to rethink the architecture.
