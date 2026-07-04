<?php

namespace NightOwl\Agent;

use NightOwl\Support\QueryHistogram;
use NightOwl\Support\RollupSpec;
use NightOwl\Support\RollupSpecs;
use PDO;

final class RecordWriter
{
    private ?PDO $pdo = null;

    /** @var array<string, list<array{target?: string, duration_ms: int}>> Thresholds grouped by type */
    private array $thresholdCache = [];

    private float $thresholdCacheExpiry = 0;

    /** Lightweight polling: detect settings changes without full reload */
    private float $thresholdVersionCheckAt = 0;

    private ?string $thresholdUpdatedAt = null;

    private AlertNotifier $notifier;

    /** Cached per rollup table: whether it exists on the target DB. */
    private array $rollupTableChecked = [];

    /** Cached per rollup table: whether it carries every column the spec's upsert writes. */
    private array $rollupColumnsChecked = [];

    /** Built once: the queries rollup upsert SQL (includes the generated hist_NN columns). */
    private ?string $rollupUpsertSql = null;

    /** Built once per rollup table: the generic spec-driven upsert SQL. */
    private array $rollupSqlCache = [];

    /**
     * Per-batch app-vitals counts from the last doWrite() call, read by the
     * drain worker to accumulate fleet-overview vitals. Counted directly off
     * the already-grouped/parsed records — no extra json_decode. See
     * AGENCY_PORTFOLIO_IMPL_PLAN §4.1.
     */
    public int $lastRequestCount = 0;

    public int $last5xxCount = 0;

    public int $lastExceptionCount = 0;

    /**
     * Details of the most recent write failure, set by copyBatch() (COPY path)
     * or doWrite()'s catch (INSERT/upsert path) and read by the drain worker.
     * Shape: ['sqlstate' => ?string, 'table' => ?string, 'connection' => bool].
     * Cleared to null at the start of every doWrite() — so a null value after a
     * write() call means the batch succeeded. Only SQLSTATE + table travel to the
     * health report; the raw libpq message (which can echo customer row values)
     * stays in the local error_log only.
     */
    public ?array $lastWriteError = null;

    /** Table currently being written — fallback table name for INSERT/upsert failures. */
    private ?string $currentWriteTarget = null;

    /**
     * Physical tables landed by the most recent SUCCESSFUL doWrite() (set only
     * after commit). The drain worker uses this to clear a table's systematic-poison
     * breaker streak — a table that just drained is not systematically broken. Built
     * from the actual write-target stamps so the names match lastWriteError['table']
     * exactly (no type→table map to drift). See DrainWorker::onDrainSuccess.
     *
     * @var list<string>
     */
    public array $lastWrittenTables = [];

    /** Tables stamped during the in-flight doWrite(), promoted to lastWrittenTables on commit. */
    private array $pendingWrittenTables = [];

    /** When true, copyBatch() routes the COPY tables through INSERT instead. */
    private bool $forceInsert = false;

    public function __construct(
        private string $host,
        private int $port,
        private string $database,
        private string $username,
        private string $password,
        private int $thresholdCacheTtl = 86400,
        ?AlertNotifier $notifier = null,
        private string $appName = 'NightOwl',
        private string $environment = 'production',
        private string $sslmode = 'prefer',
    ) {
        $this->notifier = $notifier ?? new AlertNotifier;
    }

    private const CONNECT_TIMEOUT = 5;

    private const COPY_TIMEOUT = 60;

    private function connect(): void
    {
        // connect_timeout in the DSN should cap the TCP+SSL handshake, but some
        // libpq builds don't properly interrupt a hung SSL negotiation (the TCP
        // connect() succeeds so the OS-level timer fires, but the SSL handshake
        // stalls in userspace with no further timeout). SIGALRM + exit() is the
        // guaranteed backstop: if the PDO constructor hasn't returned after 3×
        // the DSN timeout, the child exits and the parent's SIGCHLD handler
        // restarts it after the 2-second cooldown.
        pcntl_signal(SIGALRM, static function () {
            error_log('[NightOwl Drain] PostgreSQL connection timed out (SIGALRM backstop) — exiting for parent restart');
            exit(1);
        });
        pcntl_alarm(self::CONNECT_TIMEOUT * 3);

        try {
            $this->pdo = new PDO(
                "pgsql:host={$this->host};port={$this->port};dbname={$this->database};connect_timeout=".self::CONNECT_TIMEOUT.";sslmode={$this->sslmode}",
                $this->username,
                $this->password,
            );
            $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } finally {
            pcntl_alarm(0);
            pcntl_signal(SIGALRM, SIG_DFL);
        }

        // Disable synchronous commit — don't wait for WAL flush on each transaction.
        // Trades ~10ms of data durability for 2-5x write throughput. Acceptable for
        // monitoring data: a crash loses at most the last few milliseconds of events,
        // which are still safe in the SQLite buffer and will be re-drained on restart.
        $this->pdo->exec('SET synchronous_commit = off');
    }

    private function pdo(): PDO
    {
        if ($this->pdo === null) {
            $this->connect();
        }

        return $this->pdo;
    }

    /**
     * Create a RecordWriter from Laravel config.
     */
    public static function fromConfig(): self
    {
        return new self(
            config('nightowl.database.host', '127.0.0.1'),
            (int) config('nightowl.database.port', 5432),
            config('nightowl.database.database', 'nightowl'),
            config('nightowl.database.username', 'nightowl'),
            config('nightowl.database.password', 'nightowl'),
            (int) config('nightowl.threshold_cache_ttl', 86400),
            AlertNotifier::fromConfig(),
            config('app.name', 'NightOwl'),
            // NIGHTOWL_ENVIRONMENT overrides APP_ENV for rare cases where the
            // agent runs outside the Laravel app (standalone harness) or
            // customers want an explicit label like "prod-us-east". Read via
            // the config key so `php artisan config:cache` doesn't nuke it.
            config('nightowl.environment') ?: config('app.env', 'production'),
            config('nightowl.database.sslmode', 'prefer'),
        );
    }

    /**
     * Write an array of records to the database.
     * Each record has a 't' field indicating its type.
     *
     * Automatically reconnects and retries once on connection failure.
     */
    public function write(array $records): void
    {
        try {
            $this->doWrite($records);
        } catch (\Throwable $e) {
            // Reconnect+retry only on a genuine connection failure. Prefer the
            // structured classification already computed from the SQLSTATE
            // (copyBatch / doWrite's catch both set lastWriteError['connection']
            // off the captured code) over re-scanning $e — on the COPY path $e is a
            // RuntimeException whose message echoes the offending customer row value,
            // so isConnectionError($e) with no SQLSTATE falls through to the raw
            // message scan and a row value containing "connection refused" (etc.)
            // would force a needless reconnect + full-batch retry. lastWriteError is
            // null only when pdo() threw at connect time (no row context — outside
            // doWrite's try), where the message scan is the intended, safe fallback.
            $isConnectionError = $this->lastWriteError !== null
                ? ! empty($this->lastWriteError['connection'])
                : $this->isConnectionError($e);
            if ($isConnectionError) {
                $this->reconnect();
                try {
                    $this->doWrite($records);
                } catch (\Throwable $retry) {
                    // The reconnect+retry still failed. If it's STILL a connection error,
                    // Postgres is unreachable — but a connect-time pdo() throw leaves
                    // lastWriteError null (pdo() runs outside doWrite's try), which the
                    // drain worker can't tell apart from a local SQLite buffer error (also
                    // null). Stamp it as a connection failure so the worker can refresh its
                    // connection-failure clock on every failed batch of a sustained outage.
                    if ($this->lastWriteError === null && $this->isConnectionError($retry)) {
                        $this->lastWriteError = ['sqlstate' => null, 'table' => null, 'connection' => true];
                    }

                    throw $retry;
                }
            } else {
                throw $e;
            }
        }
    }

    /**
     * Like write(), but routes the 10 COPY tables through multi-row INSERT instead
     * of the COPY protocol (exceptions/users/rollups are already INSERT). Used by
     * the drain worker's poison-row isolation: re-running the FULL batch as INSERT
     * both clears a hypothetical COPY-hostile target and lets a single offending
     * row surface its own data-error SQLSTATE. The latch resets even on failure.
     */
    public function writeForceInsert(array $records): void
    {
        $this->forceInsert = true;
        try {
            $this->write($records);
        } finally {
            $this->forceInsert = false;
        }
    }

    /**
     * Current count of open issues in the tenant DB — the fleet overview's
     * per-app "issues" gauge. A snapshot (not cumulative): the platform stores
     * it directly rather than diffing. Cheap (indexed `status` column), run off
     * the ingest path at most once per minute by the drain worker.
     *
     * Returns null — never throws — when the issues table isn't present yet
     * (older tenant schema) or the query fails, so a missing table or a brief
     * PG blip never disrupts the drain. The caller keeps the last good value.
     */
    public function countOpenIssues(): ?int
    {
        try {
            $count = $this->pdo()->query("SELECT COUNT(*) FROM nightowl_issues WHERE status = 'open'")->fetchColumn();

            return $count === false ? null : (int) $count;
        } catch (\Throwable) {
            return null;
        }
    }

    /**
     * Drop the current connection and force a fresh one on next use.
     * Ensures any stale transaction state is cleaned up before the
     * socket is discarded — critical for PgBouncer/Supavisor which
     * recycle server connections and may inherit dirty transaction state.
     */
    private function reconnect(): void
    {
        if ($this->pdo !== null) {
            try {
                if ($this->pdo->inTransaction()) {
                    $this->pdo->rollBack();
                }
            } catch (\Throwable) {
                // Connection is likely already dead — ignore
            }
        }

        $this->pdo = null;
    }

    private function doWrite(array $records): void
    {
        // Clear last-error state; a null value after write() returns means success.
        $this->lastWriteError = null;
        $this->currentWriteTarget = null;
        $this->pendingWrittenTables = [];

        $grouped = [];
        foreach ($records as $record) {
            $type = $record['t'] ?? null;
            if ($type === null) {
                continue;
            }
            $grouped[$type][] = $record;
        }

        // App-vitals tally for the fleet overview — counted off the grouped,
        // already-parsed records (zero extra decode). 5xx is read from the
        // status_code present on every request record. See impl plan §4.1.
        $this->lastRequestCount = count($grouped['request'] ?? []);
        $this->lastExceptionCount = count($grouped['exception'] ?? []);
        $this->last5xxCount = 0;
        foreach ($grouped['request'] ?? [] as $r) {
            if ((int) ($r['status_code'] ?? 200) >= 500) {
                $this->last5xxCount++;
            }
        }

        $pdo = $this->pdo();

        $pdo->beginTransaction();

        try {
            foreach ($grouped as $type => $typeRecords) {
                match ($type) {
                    'request' => $this->writeRequests($typeRecords),
                    'query' => $this->writeQueries($typeRecords),
                    'exception' => $this->writeExceptions($typeRecords),
                    'command' => $this->writeCommands($typeRecords),
                    'queued-job' => $this->writeJobs($typeRecords),
                    'cache-event' => $this->writeCacheEvents($typeRecords),
                    'mail' => $this->writeMail($typeRecords),
                    'notification' => $this->writeNotifications($typeRecords),
                    'outgoing-request' => $this->writeOutgoingRequests($typeRecords),
                    'scheduled-task' => $this->writeScheduledTasks($typeRecords),
                    'job-attempt' => $this->writeJobs($typeRecords),
                    'log' => $this->writeLogs($typeRecords),
                    'user' => $this->writeUsers($typeRecords),
                    default => null,
                };
            }

            $pdo->commit();
            // All writes in this batch committed — publish the landed tables so the
            // drain worker can clear those tables' poison-breaker streaks.
            $this->lastWrittenTables = array_keys($this->pendingWrittenTables);
        } catch (\Throwable $e) {
            $this->notifier->clearPending(); // Discard — data was rolled back
            if ($pdo->inTransaction()) {
                $pdo->rollBack();
            }
            // COPY failures already recorded their SQLSTATE+table in copyBatch.
            // INSERT/upsert failures (PDOException) are captured here, where the
            // failing table is known via currentWriteTarget.
            if ($this->lastWriteError === null) {
                $sqlstate = $this->sqlStateOf($e);
                $this->lastWriteError = [
                    'sqlstate' => $sqlstate,
                    'table' => $this->currentWriteTarget,
                    'connection' => $this->isConnectionError($e, $sqlstate),
                ];
            }
            throw $e;
        }

        // Dispatch notifications AFTER commit — no blocking I/O inside the transaction
        $this->notifier->flushNotifications($pdo);
    }

    /**
     * Stamp the table currently being written: the fallback name for an INSERT/upsert
     * failure (currentWriteTarget) AND a member of the set promoted to lastWrittenTables
     * on commit (used to clear the per-table poison breaker on success).
     */
    private function markWriteTarget(string $table): void
    {
        $this->currentWriteTarget = $table;
        $this->pendingWrittenTables[$table] = true;
    }

    /**
     * Take a SHARED transaction-scoped advisory lock on a rollup table before the
     * additive UPSERT, coordinating with nightowl:backfill-rollups (which takes the
     * EXCLUSIVE lock around its DELETE-then-recompute). Without it, a backfill whose
     * recompute snapshot straddles this drain's commit overwrites the drain's just-
     * committed rows with a stale (lower) count — a silent rollup undercount. With it
     * the two serialize and COMMUTE: whichever commits first, the other sees/adds the
     * full set (the UPSERT is additive; the backfill recompute reads committed raw).
     * Shared, so concurrent drain workers never block each other — only an active
     * backfill on the SAME table briefly blocks them. Released at commit/rollback.
     *
     * hashtext()'s int4 result implicitly widens to the bigint advisory-lock key
     * (works on every supported Postgres). The key string must match the backfill's.
     */
    private function lockRollupForWriteShared(string $table): void
    {
        $stmt = $this->pdo()->prepare('SELECT pg_advisory_xact_lock_shared(hashtext(?))');
        $stmt->execute(['nightowl_rollup:'.$table]);
    }

    private function isConnectionError(\Throwable $e, ?string $sqlstate = null): bool
    {
        // SQLSTATE is AUTHORITATIVE when present. Class 08 = "connection exception":
        // libpq labels nearly every connect-phase failure 08006 — including wrong password
        // (28P01), wrong dbname (3D000), pg_hba rejection — which only surface as their
        // specific code once CONNECTED, so the most common first-run failure lands here.
        // The caller passes the code explicitly on the COPY path, whose RuntimeException
        // doesn't carry it (errorInfo lives on the discarded PDO handle).
        $sqlstate ??= $this->sqlStateOf($e);
        if (is_string($sqlstate) && str_starts_with($sqlstate, '08')) {
            return true;
        }
        // Any OTHER definite SQLSTATE is a write/data/config error, NOT a connection
        // failure — return false WITHOUT scanning the raw message. A write error's
        // DETAIL/CONTEXT echoes the offending customer ROW VALUE, which for a monitoring
        // agent storing other apps' telemetry routinely IS connection-error text
        // ("could not connect to server", "Operation timed out"). Scanning it would
        // misclassify a poison row as a connection failure → defer-forever instead of
        // quarantine → head-of-line-block the whole drain. The classifier must NEVER
        // depend on customer row content.
        if (is_string($sqlstate) && $sqlstate !== '') {
            return false;
        }

        // No SQLSTATE exposed: an OS-level connect failure (DNS / routing / timeout) or a
        // libpq drop PDO didn't tag. There is no statement / row-value context at connect
        // phase, so the message is safe to scan.
        $message = strtolower($e->getMessage());
        $prev = $e->getPrevious();
        $prevMessage = $prev ? strtolower($prev->getMessage()) : '';

        if (str_contains($message, 'sqlstate[08') || str_contains($prevMessage, 'sqlstate[08')) {
            return true;
        }

        $patterns = [
            'server closed', 'connection reset', 'broken pipe', 'gone away', 'no connection',
            'connection refused', 'connection timed out', 'eof detected', 'ssl syscall',
            'already an active transaction', 'ssl error',
            'connection to server', 'could not connect to server', 'could not translate host name',
            'name or service not known', 'no route to host', 'host is unreachable',
            'operation timed out', 'timeout expired', 'could not receive data',
        ];
        foreach ($patterns as $pattern) {
            if (str_contains($message, $pattern) || str_contains($prevMessage, $pattern)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Extract the 5-char PostgreSQL SQLSTATE from a write failure, or null.
     * Only PDOExceptions carry one (errorInfo[0] / getCode()); COPY failures
     * capture it directly in copyBatch. Used to pick the right operator advice
     * (42P01 → migrate, 42501 → grant, 22xxx → bad row) without shipping the
     * raw libpq message (which can contain customer row values).
     */
    private function sqlStateOf(\Throwable $e): ?string
    {
        if ($e instanceof \PDOException) {
            $state = $e->errorInfo[0] ?? null;
            if (is_string($state) && $state !== '' && $state !== '00000') {
                return $state;
            }
            $code = (string) $e->getCode();
            if ($code !== '' && $code !== '0' && $code !== '00000') {
                return $code;
            }
        }

        return null;
    }

    /**
     * COPY a batch of rows into a table using PostgreSQL's COPY protocol.
     * 5-10x faster than batched INSERTs because it bypasses the SQL parser.
     *
     * @param  string  $table  Target table name
     * @param  string[]  $columns  Column names in order
     * @param  array[]  $rows  Each row is an array of values matching $columns order
     */
    private function copyBatch(string $table, array $columns, array $rows): void
    {
        if (empty($rows)) {
            return;
        }

        $this->markWriteTarget($table);

        // Swoole/OpenSwoole's PDO-pgsql coroutine hook reimplements
        // pgsqlCopyFromArray() and busy-loops on COPY (100% CPU, never returns)
        // when the host app (typically Laravel Octane) has enabled runtime hooks.
        // Disabling the hooks reverts the connect override but NOT the COPY one —
        // once enabled it stays broken — so when either extension is present we
        // avoid COPY entirely and drain via multi-row INSERT instead. Plain
        // (non-Swoole) installs keep the faster COPY path.
        if ($this->forceInsert || extension_loaded('swoole') || extension_loaded('openswoole')) {
            $this->insertBatch($table, $columns, $rows);

            return;
        }

        $colList = implode(', ', $columns);
        $tsvRows = [];

        foreach ($rows as $row) {
            $escaped = [];
            foreach ($row as $value) {
                if ($value === null) {
                    $escaped[] = '\\N';
                } else {
                    // strtr single-pass substitution — marginally faster than
                    // str_replace with parallel arrays for the same result.
                    $escaped[] = strtr((string) $value, [
                        '\\' => '\\\\',
                        "\t" => '\\t',
                        "\n" => '\\n',
                        "\r" => '\\r',
                    ]);
                }
            }
            $tsvRows[] = implode("\t", $escaped);
        }

        // Note: do NOT pass the 4th NULL-marker arg. libpq mis-parses the
        // escaped string there, so any `\N` in the TSV is treated as the
        // literal string "N" for non-text columns. The text COPY default
        // null marker is already `\N`.
        //
        // pgsqlCopyFromArray can hang on a genuine libpq stall (mid-stream network
        // stall against a remote/managed Postgres) — it never returns false, it
        // just blocks. Wrap with the same SIGALRM backstop used in connect(): if
        // the call doesn't return within COPY_TIMEOUT seconds the drain child exits
        // so the parent SIGCHLD handler restarts it. pcntl_async_signals(true) is
        // already set by DrainWorker before run().
        //
        // Note: this backstop CANNOT catch a Swoole pgsqlCopyFromArray spin — that
        // busy-loops in swoole.so's C code without yielding to the PHP VM, so the
        // pending signal never dispatches. That case is handled earlier by routing
        // to insertBatch() when Swoole is loaded (see the top of this method).
        //
        // When pgsqlCopyFromArray does return, it returns false rather than
        // throwing (even under ERRMODE_EXCEPTION) on some errors. Convert to
        // an exception so the drain loop rolls back and retries the batch.
        if (function_exists('pcntl_signal') && function_exists('pcntl_alarm')) {
            pcntl_signal(SIGALRM, static function () use ($table) {
                error_log('[NightOwl Drain] pgsqlCopyFromArray hung on '.$table.' (SIGALRM backstop) — exiting for parent restart');
                exit(1);
            });
            pcntl_alarm(self::COPY_TIMEOUT);
        }

        try {
            $ok = $this->pdo()->pgsqlCopyFromArray($table.' ('.$colList.')', $tsvRows);
        } finally {
            if (function_exists('pcntl_alarm') && function_exists('pcntl_signal')) {
                pcntl_alarm(0);
                pcntl_signal(SIGALRM, SIG_DFL);
            }
        }

        if ($ok !== true) {
            // Capture the error before discarding the connection — errorInfo()
            // is only meaningful while the failing handle is still around.
            $error = $this->pdo()->errorInfo();
            $sqlstate = (isset($error[0]) && is_string($error[0]) && $error[0] !== '' && $error[0] !== '00000')
                ? $error[0]
                : null;

            // A failed COPY can leave libpq stuck in COPY_IN state, where every
            // later command on the same connection fails with "another command
            // is already in progress". Drop the handle so the next batch opens a
            // clean one instead of inheriting a poisoned connection. doWrite()
            // still holds its own reference for the rollback in its catch block.
            $this->pdo = null;

            $exception = new \RuntimeException(sprintf(
                'COPY into %s failed: %s',
                $table,
                $error[2] ?? 'unknown libpq error (pgsqlCopyFromArray returned '.var_export($ok, true).')'
            ));

            // Record SQLSTATE + table for the health report. The raw libpq message
            // ($error[2], which can echo customer row values) stays out of the
            // report — only stderr via the thrown exception's message.
            $this->lastWriteError = [
                'sqlstate' => $sqlstate,
                'table' => $table,
                // Pass the captured SQLSTATE — $exception is a RuntimeException whose
                // message echoes customer row values; classifying off it would misread a
                // poison row as a connection failure.
                'connection' => $this->isConnectionError($exception, $sqlstate),
            ];

            throw $exception;
        }
    }

    /**
     * COPY-equivalent write using multi-row INSERT. Used only when Swoole is
     * loaded (its pgsqlCopyFromArray hook is broken — see copyBatch). Slower than
     * COPY but correct; the values and column order are identical to copyBatch's,
     * so all callers stay unchanged.
     *
     * @param  string[]  $columns
     * @param  array[]  $rows  Each row is an array of values matching $columns order
     */
    private function insertBatch(string $table, array $columns, array $rows): void
    {
        $colCount = count($columns);
        if ($colCount === 0) {
            return;
        }

        $colList = implode(', ', $columns);
        $rowPlaceholder = '('.implode(', ', array_fill(0, $colCount, '?')).')';

        // Postgres caps bound parameters at 65535 per statement — chunk so a
        // large batch (up to drain_batch_size rows × columns) never exceeds it.
        $maxRowsPerStatement = max(1, intdiv(65535, $colCount));

        foreach (array_chunk($rows, $maxRowsPerStatement) as $chunk) {
            $values = implode(', ', array_fill(0, count($chunk), $rowPlaceholder));
            $params = [];
            foreach ($chunk as $row) {
                foreach ($row as $value) {
                    $params[] = $value;
                }
            }

            $stmt = $this->pdo()->prepare("INSERT INTO {$table} ({$colList}) VALUES {$values}");
            $stmt->execute($params);
        }
    }

    private function writeRequests(array $records): void
    {
        // created_at and the rollup bucket come from each event's OWN timestamp
        // (eventCreatedAt/eventBucket), so the read path filters/buckets on event
        // time; $nowTs is only the fallback clock for rows with no numeric timestamp.
        $nowTs = time();

        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'environment', 'server', 'group_hash',
            'user_id', 'method', 'url', 'route_name', 'route_methods',
            'route_domain', 'route_path', 'route_action', 'ip',
            'duration', 'status_code', 'request_size', 'response_size',
            'bootstrap', 'before_middleware', 'action', 'render',
            'after_middleware', 'sending', 'terminating',
            'exceptions', 'logs', 'queries',
            'jobs_queued', 'mail', 'notifications', 'outgoing_requests',
            'cache_events', 'peak_memory_usage',
            'exception_preview', 'context', 'headers', 'payload', 'created_at',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null,
                $r['timestamp'] ?? null,
                $r['deploy'] ?? null,
                $this->environment,
                $r['server'] ?? null,
                $r['_group'] ?? null,
                $r['user'] ?? null,
                $r['method'] ?? 'GET',
                $r['url'] ?? '/',
                $r['route_name'] ?? null,
                json_encode($r['route_methods'] ?? []),
                $r['route_domain'] ?? null,
                $r['route_path'] ?? null,
                $r['route_action'] ?? null,
                $r['ip'] ?? null,
                $r['duration'] ?? null,
                $r['status_code'] ?? 200,
                $r['request_size'] ?? null,
                $r['response_size'] ?? null,
                $r['bootstrap'] ?? null,
                $r['before_middleware'] ?? null,
                $r['action'] ?? null,
                $r['render'] ?? null,
                $r['after_middleware'] ?? null,
                $r['sending'] ?? null,
                $r['terminating'] ?? null,
                $r['exceptions'] ?? 0,
                $r['logs'] ?? 0,
                $r['queries'] ?? 0,
                $r['jobs_queued'] ?? 0,
                $r['mail'] ?? 0,
                $r['notifications'] ?? 0,
                $r['outgoing_requests'] ?? 0,
                $r['cache_events'] ?? 0,
                $r['peak_memory_usage'] ?? 0,
                $r['exception_preview'] ?? null,
                is_string($r['context'] ?? null) ? $r['context'] : json_encode($r['context'] ?? null),
                is_string($r['headers'] ?? null) ? $r['headers'] : json_encode($r['headers'] ?? null),
                is_string($r['payload'] ?? null) ? $r['payload'] : json_encode($r['payload'] ?? null),
                $this->eventCreatedAt($r, $nowTs),
            ];
        }

        $this->copyBatch('nightowl_requests', $columns, $rows);

        if ($this->rollupEnabled('nightowl_request_rollups')) {
            $this->writeRollup($records, RollupSpecs::requests(), $nowTs);
        }

        // Per-user rollup keyed on user_id (not route group_hash) — powers the
        // users list, which nightowl_request_rollups can't serve.
        if ($this->rollupEnabled('nightowl_user_rollups')) {
            $this->writeRollup($records, RollupSpecs::requestUsers(), $nowTs);
        }

        $this->checkRouteThresholds($records);
    }

    private function writeQueries(array $records): void
    {
        // created_at and the rollup bucket come from each event's OWN timestamp
        // (eventCreatedAt/eventBucket) — the same value the read path filters on —
        // stamped in UTC. $nowTs is only the fallback for rows with no numeric
        // timestamp (created_at was previously left to the column's useCurrent()).
        $nowTs = time();

        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'environment', 'server', 'group_hash',
            'execution_source', 'execution_id', 'execution_stage', 'execution_preview', 'user_id',
            'sql_query', 'file', 'line', 'duration', 'connection', 'connection_type', 'created_at',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null,
                $r['timestamp'] ?? null,
                $r['deploy'] ?? null,
                $this->environment,
                $r['server'] ?? null,
                $r['_group'] ?? null,
                $r['execution_source'] ?? null,
                $r['execution_id'] ?? null,
                $r['execution_stage'] ?? null,
                $r['execution_preview'] ?? null,
                $r['user'] ?? null,
                $r['sql'] ?? '',
                $r['file'] ?? null,
                $r['line'] ?? null,
                $r['duration'] ?? null,
                $r['connection'] ?? null,
                $r['connection_type'] ?? null,
                $this->eventCreatedAt($r, $nowTs),
            ];
        }

        $this->copyBatch('nightowl_queries', $columns, $rows);

        if ($this->rollupEnabled('nightowl_query_rollups')) {
            $this->writeQueryRollups($records, $nowTs);
        }

        $this->checkThresholds('query', $records, 'connection');
    }

    /**
     * A rollup table's existence is fixed for the agent's lifetime, so probe
     * once per table and cache. When the table is missing (a customer running
     * new agent code before `nightowl:migrate` created it), skip the rollup
     * write rather than aborting the whole drain transaction — the upsert shares
     * the COPY's transaction, so its failure would roll the raw write back too.
     * Restart picks up the table once migrations have run.
     */
    private function rollupEnabled(string $table): bool
    {
        if (! array_key_exists($table, $this->rollupTableChecked)) {
            try {
                $this->rollupTableChecked[$table] = (bool) $this->pdo()->query(
                    "SELECT to_regclass('public.".$table."') IS NOT NULL"
                )->fetchColumn();
            } catch (\Throwable) {
                $this->rollupTableChecked[$table] = false;
            }

            if ($this->rollupTableChecked[$table] === false) {
                error_log('[NightOwl Agent] '.$table.' missing — rollups for it disabled until nightowl:migrate runs (restart the agent afterward)');
            }
        }

        return $this->rollupTableChecked[$table];
    }

    /**
     * A rollup table can EXIST while lacking a column the spec's upsert writes —
     * a partial/failed migration, or an agent running ahead of a migration that
     * adds a column to an already-created rollup table. That would raise
     * SQLSTATE 42703 (undefined_column) from the prepared upsert INSIDE the drain
     * transaction, rolling the raw write back too and — because a prepared-
     * statement failure can't be bisected by poison-row quarantine — head-of-line-
     * blocking the whole drain forever. So verify the full written column set once
     * per table (same order rollupSql() builds it) BEFORE issuing any upsert;
     * disable the rollup and keep draining raw if a column is missing. Probed via
     * information_schema (a plain read, safe inside the transaction) and cached.
     *
     * @param  list<string>  $histColumns
     */
    private function rollupColumnsPresent(RollupSpec $spec, array $histColumns): bool
    {
        $table = $spec->table;
        if (array_key_exists($table, $this->rollupColumnsChecked)) {
            return $this->rollupColumnsChecked[$table];
        }

        // Mirror rollupSql()'s insert column set so the probe matches what we write.
        $expected = [...$spec->groupColumnNames(), 'bucket_start', 'environment', 'call_count', ...$spec->counterColumns()];
        if ($spec->hasDuration) {
            $expected = [...$expected, 'total_duration', 'min_duration', 'max_duration'];
        }
        if ($spec->hasHistogram) {
            $expected = [...$expected, ...$histColumns];
        }
        $expected = [...$expected, ...$spec->representativeColumns()];

        try {
            $stmt = $this->pdo()->prepare(
                'SELECT column_name FROM information_schema.columns WHERE table_schema = \'public\' AND table_name = ?'
            );
            $stmt->execute([$table]);
            $present = $stmt->fetchAll(\PDO::FETCH_COLUMN);
        } catch (\Throwable) {
            // Probe itself failed — the table-existence gate already passed, so
            // stay permissive rather than silently dropping every rollup.
            return $this->rollupColumnsChecked[$table] = true;
        }

        $missing = array_diff($expected, $present);
        if (! empty($missing)) {
            error_log('[NightOwl Agent] '.$table.' missing column(s) '.implode(', ', $missing).' — rollups for it disabled until nightowl:migrate runs (restart the agent afterward)');

            return $this->rollupColumnsChecked[$table] = false;
        }

        return $this->rollupColumnsChecked[$table] = true;
    }

    /**
     * created_at for a telemetry row: the EVENT's own time (from its `timestamp`),
     * so a backdated or delayed drain dates the row when the event happened — not
     * when it drained. The read path filters/buckets on created_at, so this keeps
     * time-range charts honest. Falls back to the batch clock for rows that carry no
     * numeric timestamp.
     */
    // Plausible event-time window. A malformed payload (e.g. a millisecond-scaled or
    // garbage timestamp) would otherwise stamp a row tens of thousands of years out —
    // invisible to every filter/bucket AND rejected by Postgres (datetime overflow,
    // 22008), which with quarantine off head-of-line-blocks the whole drain. Anything
    // outside the window falls back to the drain clock.
    private const EVENT_TS_MAX_PAST_SECONDS = 31622400; // ~366d, beyond any backfill/retention window

    private const EVENT_TS_MAX_FUTURE_SECONDS = 86400;  // 1d of clock skew

    /** The event's own epoch, range-guarded; falls back to the drain clock when absent
     *  or implausible so a poison timestamp can never freeze the drain. */
    private function eventEpoch(array $r, int $nowTs): int
    {
        $ts = $r['timestamp'] ?? null;
        if (is_numeric($ts)) {
            $ts = (int) $ts;
            if ($ts >= $nowTs - self::EVENT_TS_MAX_PAST_SECONDS && $ts <= $nowTs + self::EVENT_TS_MAX_FUTURE_SECONDS) {
                return $ts;
            }
        }

        return $nowTs;
    }

    private function eventCreatedAt(array $r, int $nowTs): string
    {
        return gmdate('Y-m-d H:i:s', $this->eventEpoch($r, $nowTs));
    }

    /** The event's minute bucket (same clock as eventCreatedAt) for rollups. */
    private function eventBucket(array $r, int $nowTs): string
    {
        return gmdate('Y-m-d H:i:s', intdiv($this->eventEpoch($r, $nowTs), 60) * 60);
    }

    /**
     * Generic, spec-driven rollup writer — the engine behind every non-query
     * rollup (requests/jobs/outgoing/cache). Groups the batch in PHP by the
     * spec's group columns PLUS the event minute-bucket, accumulates call_count +
     * additive counters + (when the spec carries duration) totals/min/max and
     * histogram bins + first-seen representatives, then one additive UPSERT per
     * (group, bucket). Same transaction as the COPY (atomic with raw), same
     * concurrency-safety as the query rollup.
     */
    private function writeRollup(array $records, RollupSpec $spec, int $nowTs): void
    {
        $histColumns = $spec->hasHistogram ? QueryHistogram::columns() : [];

        // Guard against a table that exists but is missing a written column: a
        // failing upsert would poison the shared drain transaction. Probe before
        // taking the write lock or issuing any upsert.
        if (! $this->rollupColumnsPresent($spec, $histColumns)) {
            return;
        }

        $this->markWriteTarget($spec->table);
        $this->lockRollupForWriteShared($spec->table);
        $counterCols = $spec->counterColumns();
        $repCols = $spec->representativeColumns();

        $groups = [];
        foreach ($records as $r) {
            // Bucket on the event's own time (matching created_at), so a backdated or
            // delayed drain lands in the correct minute instead of "now".
            $bucket = $this->eventBucket($r, $nowTs);
            $groupVals = [];
            $keyParts = [$bucket];
            foreach ($spec->groupColumns as $col => $def) {
                $value = (string) ($def['php'])($r);
                $groupVals[$col] = $value;
                $keyParts[] = $value;
            }
            $key = implode("\0", $keyParts);

            if (! isset($groups[$key])) {
                $groups[$key] = [
                    'group' => $groupVals,
                    'bucket_start' => $bucket,
                    'call_count' => 0,
                    'counters' => array_fill_keys($counterCols, 0),
                    'total_duration' => 0,
                    'min_duration' => null,
                    'max_duration' => null,
                    'hist' => $spec->hasHistogram ? array_fill(0, count($histColumns), 0) : [],
                    'reps' => array_fill_keys($repCols, null),
                ];
            }

            $groups[$key]['call_count']++;
            foreach ($spec->counters as $cc => $def) {
                if (($def['php'])($r)) {
                    $groups[$key]['counters'][$cc]++;
                }
            }
            $durationOk = $spec->durationPredicate === null || ($spec->durationPredicate['php'])($r);
            if ($spec->hasDuration && $durationOk) {
                $duration = $r[$spec->durationField] ?? null;
                if ($duration !== null) {
                    $duration = (int) $duration;
                    $groups[$key]['total_duration'] += $duration;
                    $groups[$key]['min_duration'] = $groups[$key]['min_duration'] === null
                        ? $duration : min($groups[$key]['min_duration'], $duration);
                    $groups[$key]['max_duration'] = $groups[$key]['max_duration'] === null
                        ? $duration : max($groups[$key]['max_duration'], $duration);
                    if ($spec->hasHistogram) {
                        $groups[$key]['hist'][QueryHistogram::binIndex($duration)]++;
                    }
                }
            }
            foreach ($spec->representatives as $rc => $def) {
                if ($groups[$key]['reps'][$rc] === null) {
                    $value = ($def['php'])($r);
                    if ($value !== null && $value !== '') {
                        $groups[$key]['reps'][$rc] = $value;
                    }
                }
            }
        }

        if (empty($groups)) {
            return;
        }

        $upsert = $this->pdo()->prepare($this->rollupSql($spec, $histColumns));

        foreach ($groups as $g) {
            $params = $g['group'];
            $params['bucket_start'] = $g['bucket_start'];
            $params['environment'] = $this->environment;
            $params['call_count'] = $g['call_count'];
            foreach ($g['counters'] as $cc => $value) {
                $params[$cc] = $value;
            }
            if ($spec->hasDuration) {
                $params['total_duration'] = $g['total_duration'];
                $params['min_duration'] = $g['min_duration'];
                $params['max_duration'] = $g['max_duration'];
                foreach ($histColumns as $i => $hc) {
                    $params[$hc] = $g['hist'][$i];
                }
            }
            foreach ($g['reps'] as $rc => $value) {
                $params[$rc] = $value;
            }

            $upsert->execute($params);
        }
    }

    /**
     * Build (and cache) the spec-driven upsert SQL. Counters, duration totals,
     * and histogram bins accumulate additively; min/max use LEAST/GREATEST;
     * representatives keep the first-seen value via COALESCE.
     *
     * @param  list<string>  $histColumns
     */
    private function rollupSql(RollupSpec $spec, array $histColumns): string
    {
        if (isset($this->rollupSqlCache[$spec->table])) {
            return $this->rollupSqlCache[$spec->table];
        }

        $groupCols = $spec->groupColumnNames();
        $insertCols = [...$groupCols, 'bucket_start', 'environment', 'call_count', ...$spec->counterColumns()];
        if ($spec->hasDuration) {
            $insertCols = [...$insertCols, 'total_duration', 'min_duration', 'max_duration'];
        }
        if ($spec->hasHistogram) {
            $insertCols = [...$insertCols, ...$histColumns];
        }
        $insertCols = [...$insertCols, ...$spec->representativeColumns()];

        $placeholders = array_map(static fn (string $c): string => ':'.$c, $insertCols);
        $pk = [...$groupCols, 'bucket_start', 'environment'];

        $t = $spec->table;
        $set = ["call_count = {$t}.call_count + EXCLUDED.call_count"];
        foreach ($spec->counterColumns() as $c) {
            $set[] = "{$c} = {$t}.{$c} + EXCLUDED.{$c}";
        }
        if ($spec->hasDuration) {
            $set[] = "total_duration = {$t}.total_duration + EXCLUDED.total_duration";
            $set[] = "min_duration = LEAST({$t}.min_duration, EXCLUDED.min_duration)";
            $set[] = "max_duration = GREATEST({$t}.max_duration, EXCLUDED.max_duration)";
        }
        if ($spec->hasHistogram) {
            foreach ($histColumns as $c) {
                $set[] = "{$c} = {$t}.{$c} + EXCLUDED.{$c}";
            }
        }
        foreach ($spec->representativeColumns() as $c) {
            $set[] = "{$c} = COALESCE({$t}.{$c}, EXCLUDED.{$c})";
        }

        return $this->rollupSqlCache[$t] = sprintf(
            'INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s',
            $t,
            implode(', ', $insertCols),
            implode(', ', $placeholders),
            implode(', ', $pk),
            implode(', ', $set),
        );
    }

    /**
     * Maintain the pre-aggregated nightowl_query_rollups summary alongside the
     * raw COPY, in the SAME transaction (doWrite wraps every type-writer in one
     * transaction), so the rollup can never diverge from raw — both commit or
     * both roll back. Mirrors syncIssuesToExceptions(): group in PHP, then one
     * additive UPSERT per group with SQL-side accumulation, which is
     * concurrency-safe across NIGHTOWL_DRAIN_WORKERS (two workers hitting the
     * same (hash, bucket) serialize on the row lock; both increments land).
     *
     * Unlike copyBatch, this is a plain prepared statement, so it is unaffected
     * by the Swoole pgsqlCopyFromArray spin — no special-casing needed.
     */
    private function writeQueryRollups(array $records, int $nowTs): void
    {
        $this->markWriteTarget('nightowl_query_rollups');
        $this->lockRollupForWriteShared('nightowl_query_rollups');
        // Bucket per-record on the event's own time (same clock as created_at), so a
        // backdated/delayed drain spreads across the right minutes instead of "now".
        $histColumns = QueryHistogram::columns();

        $groups = [];
        foreach ($records as $r) {
            $bucket = $this->eventBucket($r, $nowTs);
            $groupHash = (string) ($r['_group'] ?? '');
            $connection = (string) ($r['connection'] ?? ''); // '' sentinel (see migration)
            $key = $bucket."\0".$groupHash."\0".$connection;

            $duration = $r['duration'] ?? null;
            $duration = $duration === null ? null : (int) $duration;

            if (! isset($groups[$key])) {
                $groups[$key] = [
                    'group_hash' => $groupHash,
                    'connection' => $connection,
                    'bucket_start' => $bucket,
                    'call_count' => 0,
                    'total_duration' => 0,
                    'min_duration' => null,
                    'max_duration' => null,
                    'sql_query' => null,
                    'hist' => array_fill(0, count($histColumns), 0),
                ];
            }

            $groups[$key]['call_count']++;
            if ($duration !== null) {
                $groups[$key]['total_duration'] += $duration;
                $groups[$key]['min_duration'] = $groups[$key]['min_duration'] === null
                    ? $duration : min($groups[$key]['min_duration'], $duration);
                $groups[$key]['max_duration'] = $groups[$key]['max_duration'] === null
                    ? $duration : max($groups[$key]['max_duration'], $duration);
                $groups[$key]['hist'][QueryHistogram::binIndex($duration)]++;
            }
            if ($groups[$key]['sql_query'] === null && isset($r['sql']) && $r['sql'] !== '') {
                $groups[$key]['sql_query'] = $r['sql'];
            }
        }

        if (empty($groups)) {
            return;
        }

        $upsert = $this->pdo()->prepare($this->rollupUpsertSql($histColumns));

        foreach ($groups as $g) {
            $params = [
                'group_hash' => $g['group_hash'],
                'bucket_start' => $g['bucket_start'],
                'environment' => $this->environment,
                'connection' => $g['connection'],
                'call_count' => $g['call_count'],
                'total_duration' => $g['total_duration'],
                'min_duration' => $g['min_duration'],
                'max_duration' => $g['max_duration'],
                'sql_query' => $g['sql_query'],
            ];
            foreach ($histColumns as $i => $column) {
                $params[$column] = $g['hist'][$i];
            }

            $upsert->execute($params);
        }
    }

    /**
     * Build (and cache) the rollup upsert SQL. Additive columns accumulate via
     * `existing + EXCLUDED`; min/max use LEAST/GREATEST (which ignore NULL
     * operands in Postgres, so an all-null-duration batch leaves them
     * untouched); each histogram bin accumulates additively too.
     *
     * @param  list<string>  $histColumns
     */
    private function rollupUpsertSql(array $histColumns): string
    {
        if ($this->rollupUpsertSql !== null) {
            return $this->rollupUpsertSql;
        }

        $insertColumns = ['group_hash', 'bucket_start', 'environment', 'connection',
            'call_count', 'total_duration', 'min_duration', 'max_duration', 'sql_query', ...$histColumns];
        $placeholders = array_map(static fn (string $c): string => ':'.$c, $insertColumns);

        $setClauses = [
            'call_count     = nightowl_query_rollups.call_count     + EXCLUDED.call_count',
            'total_duration = nightowl_query_rollups.total_duration + EXCLUDED.total_duration',
            'min_duration   = LEAST(nightowl_query_rollups.min_duration,  EXCLUDED.min_duration)',
            'max_duration   = GREATEST(nightowl_query_rollups.max_duration, EXCLUDED.max_duration)',
            'sql_query      = COALESCE(nightowl_query_rollups.sql_query, EXCLUDED.sql_query)',
        ];
        foreach ($histColumns as $column) {
            $setClauses[] = "{$column} = nightowl_query_rollups.{$column} + EXCLUDED.{$column}";
        }

        return $this->rollupUpsertSql = sprintf(
            'INSERT INTO nightowl_query_rollups (%s) VALUES (%s) '.
            'ON CONFLICT (group_hash, bucket_start, environment, connection) DO UPDATE SET %s',
            implode(', ', $insertColumns),
            implode(', ', $placeholders),
            implode(', ', $setClauses),
        );
    }

    private function writeExceptions(array $records): void
    {
        $this->markWriteTarget('nightowl_exceptions');
        $stmt = $this->pdo()->prepare('INSERT INTO nightowl_exceptions (
            v, trace_id, timestamp, deploy, environment, server, group_hash,
            execution_source, execution_id, execution_stage, execution_preview, user_id,
            class, message, code, file, line, trace,
            php_version, laravel_version, handled, fingerprint, created_at
        ) VALUES (
            :v, :trace_id, :timestamp, :deploy, :environment, :server, :group_hash,
            :execution_source, :execution_id, :execution_stage, :execution_preview, :user_id,
            :class, :message, :code, :file, :line, :trace,
            :php_version, :laravel_version, :handled, :fingerprint, :created_at
        )');

        // Stamp created_at as an explicit UTC string rather than relying on the
        // column's useCurrent() default — CURRENT_TIMESTAMP resolves in the
        // PostgreSQL session timezone, so on a non-UTC tenant DB it stored local
        // wall-clock and the dashboard read it back as UTC (future-dated rows).
        // Matches writeRequests()/writeQueries().
        $nowTs = time();

        $issueGroups = [];

        foreach ($records as $r) {
            // Prefer the Nightwatch SDK's `_group` (xxh128 of class,code,file,line).
            // It includes `code` — which for QueryException is the SQLSTATE — so
            // "Duplicate table" (42P07) and "Undefined table" (42P01) thrown from
            // the same PDO throw site don't collapse into one issue. Fall back to
            // a local hash only if `_group` is missing (older SDKs or raw UDP).
            $fingerprint = ! empty($r['_group'])
                ? (string) $r['_group']
                : md5(($r['class'] ?? '').'|'.($r['code'] ?? '').'|'.($r['file'] ?? '').'|'.($r['line'] ?? ''));
            $deploy = $r['deploy'] ?? null;
            $groupKey = $fingerprint.'|'.$this->environment;

            $stmt->execute([
                'v' => $r['v'] ?? null,
                'trace_id' => $r['trace_id'] ?? null,
                'timestamp' => $r['timestamp'] ?? null,
                'deploy' => $deploy,
                'environment' => $this->environment,
                'server' => $r['server'] ?? null,
                'group_hash' => $r['_group'] ?? null,
                'execution_source' => $r['execution_source'] ?? null,
                'execution_id' => $r['execution_id'] ?? null,
                'execution_stage' => $r['execution_stage'] ?? null,
                'execution_preview' => $r['execution_preview'] ?? null,
                'user_id' => $r['user'] ?? null,
                'class' => $r['class'] ?? 'Unknown',
                'message' => $r['message'] ?? null,
                'code' => $r['code'] ?? null,
                'file' => $r['file'] ?? null,
                'line' => $r['line'] ?? null,
                'trace' => $r['trace'] ?? null,
                'php_version' => $r['php_version'] ?? null,
                'laravel_version' => $r['laravel_version'] ?? null,
                'handled' => filter_var($r['handled'] ?? false, FILTER_VALIDATE_BOOLEAN) ? 't' : 'f',
                'fingerprint' => $fingerprint,
                'created_at' => $this->eventCreatedAt($r, $nowTs),
            ]);

            if (! isset($issueGroups[$groupKey])) {
                $issueGroups[$groupKey] = [
                    'fingerprint' => $fingerprint,
                    'deploy' => $deploy,
                    'environment' => $this->environment,
                    'class' => $r['class'] ?? 'Unknown',
                    'message' => $r['message'] ?? null,
                    'count' => 0,
                    'users' => [],
                    'timestamps' => [],
                ];
            }
            $issueGroups[$groupKey]['count']++;
            if (! empty($r['user'])) {
                $issueGroups[$groupKey]['users'][$r['user']] = true;
            }
            if (! empty($r['timestamp'])) {
                $issueGroups[$groupKey]['timestamps'][] = $r['timestamp'];
            }
        }

        $snapshot = $this->notifier->snapshotExistingIssues($this->pdo(), $issueGroups, 'exception');
        $this->syncIssuesToExceptions($issueGroups, $snapshot['reopen'] ?? []);
        $this->notifier->queueIssueNotifications($this->appName, $issueGroups, 'exception', $snapshot);

        // Exception rollups — written LAST (after the issues upsert) so they don't
        // leave currentWriteTarget pointing at a rollup table: an issues-upsert
        // failure must still be attributed to nightowl_exceptions. Both share the
        // drain transaction, committing/rolling back atomically with the INSERT.
        // Per-user (users list "exceptions" column):
        if ($this->rollupEnabled('nightowl_user_exception_rollups')) {
            $this->writeRollup($records, RollupSpecs::exceptionUsers(), $nowTs);
        }
        // Per-fingerprint (Exceptions section list/overview/chart + dashboard):
        if ($this->rollupEnabled('nightowl_exception_rollups')) {
            $this->writeRollup($records, RollupSpecs::exceptionGroups(), $nowTs);
        }
        // Distinct-server-per-fingerprint (exception detail "servers affected"):
        if ($this->rollupEnabled('nightowl_exception_server_rollups')) {
            $this->writeRollup($records, RollupSpecs::exceptionServers(), $nowTs);
        }
    }

    /**
     * @param  array<string, int>  $reopenIds  Composite key → issue id for fingerprints
     *                                         transitioning resolved → open in this batch.
     */
    private function syncIssuesToExceptions(array $issueGroups, array $reopenIds = []): void
    {
        // users_count uses a subquery on nightowl_exceptions to compute the
        // actual distinct user count, instead of blindly accumulating per batch
        // (which inflates the count when the same user appears across batches).
        $upsertStmt = $this->pdo()->prepare('
            INSERT INTO nightowl_issues (
                type, deploy, environment, status, exception_class, exception_message, group_hash,
                first_seen_at, last_seen_at, occurrences_count, users_count,
                created_at, updated_at
            ) VALUES (
                :type, :deploy, :environment, :status, :exception_class, :exception_message, :group_hash,
                :first_seen_at, :last_seen_at, :occurrences_count, :users_count,
                :created_at, :updated_at
            )
            ON CONFLICT (group_hash, type, environment) DO UPDATE SET
                exception_message = EXCLUDED.exception_message,
                last_seen_at = GREATEST(nightowl_issues.last_seen_at, EXCLUDED.last_seen_at),
                occurrences_count = nightowl_issues.occurrences_count + EXCLUDED.occurrences_count,
                users_count = (
                    SELECT COUNT(DISTINCT user_id) FROM nightowl_exceptions
                    WHERE fingerprint = EXCLUDED.group_hash
                      AND environment IS NOT DISTINCT FROM EXCLUDED.environment
                      AND user_id IS NOT NULL
                ),
                status = CASE
                    WHEN :should_reopen::boolean AND nightowl_issues.status = \'resolved\'
                        THEN \'open\'
                    ELSE nightowl_issues.status
                END,
                updated_at = EXCLUDED.updated_at
        ');

        $now = gmdate('Y-m-d H:i:s');

        foreach ($issueGroups as $key => $group) {
            $timestamps = $group['timestamps'];
            sort($timestamps);
            $firstSeen = ! empty($timestamps) ? gmdate('Y-m-d H:i:s', (int) $timestamps[0]) : $now;
            $lastSeen = ! empty($timestamps) ? gmdate('Y-m-d H:i:s', (int) end($timestamps)) : $now;
            $userCount = count($group['users']);

            $upsertStmt->execute([
                'type' => 'exception',
                'deploy' => $group['deploy'] ?? null,
                'environment' => $group['environment'] ?? $this->environment,
                'status' => 'open',
                'exception_class' => $group['class'],
                'exception_message' => $group['message'],
                'group_hash' => $group['fingerprint'],
                'first_seen_at' => $firstSeen,
                'last_seen_at' => $lastSeen,
                'occurrences_count' => $group['count'],
                'users_count' => $userCount,
                'created_at' => $now,
                'updated_at' => $now,
                'should_reopen' => isset($reopenIds[$key]) ? 'true' : 'false',
            ]);
        }

        $this->logReopenActivity($reopenIds, $now);
    }

    private function writeCommands(array $records): void
    {
        // Stamp created_at in UTC instead of leaning on the column's useCurrent()
        // default, which resolves in the tenant DB's session timezone. See writeExceptions().
        $nowTs = time();

        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'environment', 'server', 'group_hash',
            'user_id', 'class', 'name', 'command', 'exit_code', 'duration',
            'bootstrap', 'action', 'terminating',
            'exceptions', 'logs', 'queries',
            'jobs_queued', 'mail', 'notifications', 'outgoing_requests',
            'cache_events', 'peak_memory_usage', 'exception_preview', 'context', 'created_at',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null, $r['timestamp'] ?? null, $r['deploy'] ?? null, $this->environment,
                $r['server'] ?? null, $r['_group'] ?? null, $r['user'] ?? null,
                $r['class'] ?? null, $r['name'] ?? null, $r['command'] ?? 'unknown', $r['exit_code'] ?? null, $r['duration'] ?? null,
                $r['bootstrap'] ?? null, $r['action'] ?? null, $r['terminating'] ?? null,
                $r['exceptions'] ?? 0, $r['logs'] ?? 0, $r['queries'] ?? 0,
                $r['jobs_queued'] ?? 0, $r['mail'] ?? 0, $r['notifications'] ?? 0, $r['outgoing_requests'] ?? 0,
                $r['cache_events'] ?? 0, $r['peak_memory_usage'] ?? 0, $r['exception_preview'] ?? null,
                is_string($r['context'] ?? null) ? $r['context'] : json_encode($r['context'] ?? null),
                $this->eventCreatedAt($r, $nowTs),
            ];
        }

        $this->copyBatch('nightowl_commands', $columns, $rows);

        $this->checkThresholds('command', $records, 'command');
    }

    private function writeJobs(array $records): void
    {
        $nowTs = time();

        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'environment', 'server', 'group_hash',
            'execution_source', 'execution_id', 'execution_stage', 'execution_preview', 'user_id',
            'job_id', 'attempt_id', 'attempt',
            'job_class', 'queue', 'connection', 'status', 'duration', 'attempts',
            'exceptions', 'logs', 'queries',
            'jobs_queued', 'mail', 'notifications', 'outgoing_requests',
            'cache_events', 'peak_memory_usage', 'exception_preview', 'context', 'created_at',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null, $r['timestamp'] ?? null, $r['deploy'] ?? null, $this->environment,
                $r['server'] ?? null, $r['_group'] ?? null, $r['execution_source'] ?? null,
                $r['execution_id'] ?? null, $r['execution_stage'] ?? null, $r['execution_preview'] ?? null, $r['user'] ?? null,
                $r['job_id'] ?? null, $r['attempt_id'] ?? null, $r['attempt'] ?? null,
                $r['name'] ?? $r['job_class'] ?? 'Unknown', $r['queue'] ?? null,
                $r['connection'] ?? null, $r['status'] ?? null, $r['duration'] ?? null, $r['attempts'] ?? 1,
                $r['exceptions'] ?? 0, $r['logs'] ?? 0, $r['queries'] ?? 0,
                $r['jobs_queued'] ?? 0, $r['mail'] ?? 0, $r['notifications'] ?? 0, $r['outgoing_requests'] ?? 0,
                $r['cache_events'] ?? 0, $r['peak_memory_usage'] ?? 0, $r['exception_preview'] ?? null,
                is_string($r['context'] ?? null) ? $r['context'] : json_encode($r['context'] ?? null),
                $this->eventCreatedAt($r, $nowTs),
            ];
        }

        $this->copyBatch('nightowl_jobs', $columns, $rows);

        if ($this->rollupEnabled('nightowl_job_rollups')) {
            $this->writeRollup($records, RollupSpecs::jobs(), $nowTs);
        }

        // Per-user job-attempt rollup — enriches the users list's "queued jobs" count.
        if ($this->rollupEnabled('nightowl_user_job_rollups')) {
            $this->writeRollup($records, RollupSpecs::jobUsers(), $nowTs);
        }

        $this->checkThresholds('job', $records, ['name', 'job_class']);
    }

    private function writeCacheEvents(array $records): void
    {
        $nowTs = time();

        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'environment', 'server', 'group_hash',
            'execution_source', 'execution_id', 'execution_stage', 'execution_preview', 'user_id',
            'event_type', 'key', 'store', 'ttl', 'duration', 'created_at',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null, $r['timestamp'] ?? null, $r['deploy'] ?? null, $this->environment, $r['server'] ?? null, $r['_group'] ?? null,
                $r['execution_source'] ?? null, $r['execution_id'] ?? null, $r['execution_stage'] ?? null, $r['execution_preview'] ?? null, $r['user'] ?? null,
                $r['type'] ?? 'unknown', $r['key'] ?? '', $r['store'] ?? null, $r['ttl'] ?? null, $r['duration'] ?? null,
                $this->eventCreatedAt($r, $nowTs),
            ];
        }

        $this->copyBatch('nightowl_cache_events', $columns, $rows);

        if ($this->rollupEnabled('nightowl_cache_rollups')) {
            $this->writeRollup($records, RollupSpecs::cacheEvents(), $nowTs);
        }

        $this->checkThresholds('cache', $records, 'store');
    }

    private function writeMail(array $records): void
    {
        // Stamp created_at in UTC instead of leaning on the column's useCurrent()
        // default, which resolves in the tenant DB's session timezone. See writeExceptions().
        $nowTs = time();

        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'environment', 'server', 'group_hash',
            'execution_source', 'execution_id', 'execution_stage', 'execution_preview', 'user_id',
            'mailer', 'recipients', 'cc', 'bcc', 'attachments', 'subject', 'mailable', 'duration', 'failed', 'queued', 'created_at',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null, $r['timestamp'] ?? null, $r['deploy'] ?? null, $this->environment, $r['server'] ?? null, $r['_group'] ?? null,
                $r['execution_source'] ?? null, $r['execution_id'] ?? null, $r['execution_stage'] ?? null, $r['execution_preview'] ?? null, $r['user'] ?? null,
                $r['mailer'] ?? null, is_array($r['to'] ?? null) ? json_encode($r['to']) : ($r['to'] ?? null),
                $r['cc'] ?? 0, $r['bcc'] ?? 0, $r['attachments'] ?? 0,
                $r['subject'] ?? null, $r['class'] ?? $r['mailable'] ?? null, $r['duration'] ?? null,
                filter_var($r['failed'] ?? false, FILTER_VALIDATE_BOOLEAN) ? 't' : 'f', filter_var($r['queued'] ?? false, FILTER_VALIDATE_BOOLEAN) ? 't' : 'f',
                $this->eventCreatedAt($r, $nowTs),
            ];
        }

        $this->copyBatch('nightowl_mail', $columns, $rows);

        if ($this->rollupEnabled('nightowl_mail_rollups')) {
            $this->writeRollup($records, RollupSpecs::mail(), $nowTs);
        }

        $this->checkThresholds('mail', $records, ['class', 'mailable']);
    }

    private function writeNotifications(array $records): void
    {
        // Stamp created_at in UTC instead of leaning on the column's useCurrent()
        // default, which resolves in the tenant DB's session timezone. See writeExceptions().
        $nowTs = time();

        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'environment', 'server', 'group_hash',
            'execution_source', 'execution_id', 'execution_stage', 'execution_preview', 'user_id',
            'notification', 'channel', 'notifiable_type', 'notifiable_id', 'duration', 'failed', 'queued', 'created_at',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null, $r['timestamp'] ?? null, $r['deploy'] ?? null, $this->environment, $r['server'] ?? null, $r['_group'] ?? null,
                $r['execution_source'] ?? null, $r['execution_id'] ?? null, $r['execution_stage'] ?? null, $r['execution_preview'] ?? null, $r['user'] ?? null,
                $r['class'] ?? $r['notification'] ?? null, $r['channel'] ?? null, $r['notifiable_type'] ?? null, $r['notifiable_id'] ?? null,
                $r['duration'] ?? null, filter_var($r['failed'] ?? false, FILTER_VALIDATE_BOOLEAN) ? 't' : 'f', filter_var($r['queued'] ?? false, FILTER_VALIDATE_BOOLEAN) ? 't' : 'f',
                $this->eventCreatedAt($r, $nowTs),
            ];
        }

        $this->copyBatch('nightowl_notifications', $columns, $rows);

        if ($this->rollupEnabled('nightowl_notification_rollups')) {
            $this->writeRollup($records, RollupSpecs::notifications(), $nowTs);
        }

        $this->checkThresholds('notification', $records, ['class', 'notification']);
    }

    private function writeOutgoingRequests(array $records): void
    {
        $nowTs = time();

        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'environment', 'server', 'group_hash',
            'execution_source', 'execution_id', 'execution_stage', 'execution_preview', 'user_id',
            'host', 'method', 'url', 'status_code', 'duration',
            'request_size', 'response_size', 'request_headers', 'created_at',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null, $r['timestamp'] ?? null, $r['deploy'] ?? null, $this->environment, $r['server'] ?? null, $r['_group'] ?? null,
                $r['execution_source'] ?? null, $r['execution_id'] ?? null, $r['execution_stage'] ?? null, $r['execution_preview'] ?? null, $r['user'] ?? null,
                $r['host'] ?? null, $r['method'] ?? 'GET', $r['url'] ?? '', $r['status_code'] ?? null, $r['duration'] ?? null,
                $r['request_size'] ?? null, $r['response_size'] ?? null, $r['request_headers'] ?? null,
                $this->eventCreatedAt($r, $nowTs),
            ];
        }

        $this->copyBatch('nightowl_outgoing_requests', $columns, $rows);

        if ($this->rollupEnabled('nightowl_outgoing_request_rollups')) {
            $this->writeRollup($records, RollupSpecs::outgoingRequests(), $nowTs);
        }

        $this->checkThresholds('outgoing_request', $records, 'host');
    }

    private function writeLogs(array $records): void
    {
        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'environment', 'server',
            'execution_source', 'execution_id', 'execution_stage', 'execution_preview', 'user_id',
            'level', 'message', 'context', 'extra', 'channel', 'created_at',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null, $r['timestamp'] ?? null, $r['deploy'] ?? null, $this->environment, $r['server'] ?? null,
                $r['execution_source'] ?? null, $r['execution_id'] ?? null, $r['execution_stage'] ?? null, $r['execution_preview'] ?? null, $r['user'] ?? null,
                $r['level'] ?? 'info', $r['message'] ?? null,
                is_string($r['context'] ?? null) ? $r['context'] : json_encode($r['context'] ?? null),
                is_string($r['extra'] ?? null) ? $r['extra'] : json_encode($r['extra'] ?? null),
                $r['channel'] ?? null,
                isset($r['timestamp']) ? gmdate('Y-m-d H:i:s', (int) $r['timestamp']) : gmdate('Y-m-d H:i:s'),
            ];
        }

        $this->copyBatch('nightowl_logs', $columns, $rows);
    }

    private function writeUsers(array $records): void
    {
        $this->markWriteTarget('nightowl_users');
        // created_at is set on first insert only (DO UPDATE leaves it untouched),
        // stamped in UTC rather than via the column's session-tz useCurrent() default.
        $stmt = $this->pdo()->prepare('
            INSERT INTO nightowl_users (v, user_id, name, email, timestamp, created_at, updated_at)
            VALUES (:v, :user_id, :name, :email, :timestamp, :created_at, :updated_at)
            ON CONFLICT (user_id) DO UPDATE SET
                v = EXCLUDED.v,
                name = EXCLUDED.name,
                email = EXCLUDED.email,
                timestamp = EXCLUDED.timestamp,
                updated_at = EXCLUDED.updated_at
        ');

        $now = gmdate('Y-m-d H:i:s');

        foreach ($records as $r) {
            $userId = $r['id'] ?? null;
            if ($userId === null || $userId === '') {
                continue;
            }

            $stmt->execute([
                'v' => $r['v'] ?? null,
                'user_id' => (string) $userId,
                'name' => $r['name'] ?? null,
                'email' => $r['username'] ?? null,
                'timestamp' => $r['timestamp'] ?? null,
                'created_at' => $now,
                'updated_at' => $now,
            ]);
        }
    }

    private function writeScheduledTasks(array $records): void
    {
        // Stamp created_at in UTC instead of leaning on the column's useCurrent()
        // default, which resolves in the tenant DB's session timezone. See writeExceptions().
        $nowTs = time();

        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'environment', 'server', 'group_hash',
            'user_id', 'command', 'expression',
            'timezone', 'repeat_seconds', 'without_overlapping', 'on_one_server', 'run_in_background', 'even_in_maintenance_mode',
            'status', 'duration', 'exit_code',
            'exceptions', 'logs', 'queries',
            'jobs_queued', 'mail', 'notifications', 'outgoing_requests',
            'cache_events', 'peak_memory_usage', 'exception_preview', 'context', 'created_at',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null, $r['timestamp'] ?? null, $r['deploy'] ?? null, $this->environment,
                $r['server'] ?? null, $r['_group'] ?? null, $r['user'] ?? null,
                $r['name'] ?? $r['command'] ?? 'unknown', $r['cron'] ?? $r['expression'] ?? null,
                $r['timezone'] ?? null, $r['repeat_seconds'] ?? 0,
                ($r['without_overlapping'] ?? false) ? 't' : 'f', ($r['on_one_server'] ?? false) ? 't' : 'f',
                ($r['run_in_background'] ?? false) ? 't' : 'f', ($r['even_in_maintenance_mode'] ?? false) ? 't' : 'f',
                $r['status'] ?? null, $r['duration'] ?? null, $r['exit_code'] ?? null,
                $r['exceptions'] ?? 0, $r['logs'] ?? 0, $r['queries'] ?? 0,
                $r['jobs_queued'] ?? 0, $r['mail'] ?? 0, $r['notifications'] ?? 0, $r['outgoing_requests'] ?? 0,
                $r['cache_events'] ?? 0, $r['peak_memory_usage'] ?? 0, $r['exception_preview'] ?? null,
                is_string($r['context'] ?? null) ? $r['context'] : json_encode($r['context'] ?? null),
                $this->eventCreatedAt($r, $nowTs),
            ];
        }

        $this->copyBatch('nightowl_scheduled_tasks', $columns, $rows);

        $this->checkThresholds('scheduled_task', $records, ['name', 'command']);
    }

    // ─── Performance Threshold Checking ──────────────────────────────

    /**
     * Check route thresholds using composite "GET|HEAD /path" target keys.
     */
    private function checkRouteThresholds(array $records): void
    {
        $thresholds = $this->getThresholds();
        if (empty($thresholds['route'] ?? [])) {
            return;
        }

        // Build composite name for each record so it matches the threshold target format
        foreach ($records as &$r) {
            $methods = $r['route_methods'] ?? [];
            if (is_string($methods)) {
                try {
                    $methods = json_decode($methods, true, 8, JSON_THROW_ON_ERROR);
                } catch (\JsonException) {
                    $methods = [];
                }
                if (! is_array($methods)) {
                    $methods = [];
                }
            }
            $prefix = ! empty($methods) ? implode('|', $methods).' ' : '';
            $r['_route_composite'] = $prefix.($r['route_path'] ?? '');
        }
        unset($r);

        $this->checkThresholds('route', $records, '_route_composite');
    }

    /**
     * Load thresholds from nightowl_settings, cached for threshold_cache_ttl seconds.
     *
     * Every 30s a lightweight poll checks whether updated_at changed in the DB.
     * If it did, the cache is invalidated and thresholds are reloaded immediately.
     * This lets users update thresholds from the dashboard without restarting the agent.
     *
     * @return array<string, list<array{target?: string, duration_ms: int}>>
     */
    private function getThresholds(): array
    {
        $now = microtime(true);

        if ($now < $this->thresholdCacheExpiry) {
            // Periodically poll updated_at to detect dashboard-side changes
            if ($now < $this->thresholdVersionCheckAt) {
                return $this->thresholdCache;
            }

            $this->thresholdVersionCheckAt = $now + 30;

            try {
                $updatedAt = $this->pdo()->query(
                    "SELECT updated_at FROM nightowl_settings WHERE key = 'thresholds'"
                )->fetchColumn() ?: null;

                if ($updatedAt === $this->thresholdUpdatedAt) {
                    return $this->thresholdCache;
                }
                // updated_at changed — fall through to full reload
            } catch (\Throwable) {
                return $this->thresholdCache;
            }
        }

        $this->thresholdCache = [];
        $this->thresholdCacheExpiry = $now + $this->thresholdCacheTtl;
        $this->thresholdVersionCheckAt = $now + 30;

        try {
            $row = $this->pdo()->query(
                "SELECT value, updated_at FROM nightowl_settings WHERE key = 'thresholds'"
            )->fetch(PDO::FETCH_ASSOC);

            $this->thresholdUpdatedAt = is_array($row) ? ($row['updated_at'] ?? null) : null;

            if (! is_array($row) || empty($row['value'])) {
                return $this->thresholdCache;
            }

            try {
                $items = json_decode((string) $row['value'], true, 16, JSON_THROW_ON_ERROR);
            } catch (\JsonException) {
                return $this->thresholdCache;
            }
            if (! is_array($items)) {
                return $this->thresholdCache;
            }

            foreach ($items as $item) {
                $type = $item['type'] ?? 'route';
                $this->thresholdCache[$type][] = [
                    'target' => $item['target'] ?? $item['route'] ?? null,
                    'duration_ms' => (int) ($item['duration_ms'] ?? 0),
                ];
            }
        } catch (\Throwable) {
            // Table may not exist yet — silently ignore
        }

        return $this->thresholdCache;
    }

    /**
     * Find the matching threshold for a record.
     * Specific target match takes priority over global (no target).
     *
     * @return int|null Duration threshold in microseconds, or null if no threshold matches
     */
    private function findThreshold(string $type, ?string $target): ?int
    {
        $thresholds = $this->getThresholds();
        $typeThresholds = $thresholds[$type] ?? [];

        if (empty($typeThresholds)) {
            return null;
        }

        $globalThreshold = null;
        $specificThreshold = null;

        foreach ($typeThresholds as $t) {
            if (empty($t['target'])) {
                $globalThreshold = $t['duration_ms'] * 1000; // ms → μs
            } elseif ($target !== null && $t['target'] === $target) {
                $specificThreshold = $t['duration_ms'] * 1000; // ms → μs
            }
        }

        return $specificThreshold ?? $globalThreshold;
    }

    /**
     * Check records against thresholds and upsert performance issues.
     *
     * @param  string  $type  Threshold type: 'route', 'job', 'command', 'scheduled_task', etc.
     * @param  array  $records  Raw records from the batch
     * @param  string|string[]  $nameKeys  Record field(s) containing the name, tried in order
     * @param  string  $groupKey  Record field containing the group hash
     */
    private function checkThresholds(string $type, array $records, string|array $nameKeys, string $groupKey = '_group'): void
    {
        $thresholds = $this->getThresholds();
        if (empty($thresholds[$type] ?? [])) {
            return;
        }

        $nameKeys = (array) $nameKeys;
        $issueGroups = [];

        foreach ($records as $r) {
            $duration = $r['duration'] ?? null;
            if ($duration === null) {
                continue;
            }

            $name = null;
            foreach ($nameKeys as $key) {
                if (! empty($r[$key])) {
                    $name = $r[$key];
                    break;
                }
            }
            $threshold = $this->findThreshold($type, $name);

            if ($threshold === null || $duration < $threshold) {
                continue;
            }

            $groupHash = $r[$groupKey] ?? ($name !== null ? md5($name) : null);
            if ($groupHash === null) {
                continue;
            }
            $deploy = $r['deploy'] ?? null;
            $compositeKey = $groupHash.'|'.$this->environment;

            if (! isset($issueGroups[$compositeKey])) {
                $issueGroups[$compositeKey] = [
                    'fingerprint' => $groupHash,
                    'deploy' => $deploy,
                    'environment' => $this->environment,
                    'name' => $name ?? 'Unknown',
                    'subtype' => $type,
                    'count' => 0,
                    'users' => [],
                    'timestamps' => [],
                    'threshold_us' => $threshold,
                    'max_duration_us' => $duration,
                ];
            }
            $issueGroups[$compositeKey]['count']++;
            $issueGroups[$compositeKey]['threshold_us'] = $threshold;
            if ($duration > $issueGroups[$compositeKey]['max_duration_us']) {
                $issueGroups[$compositeKey]['max_duration_us'] = $duration;
            }
            if (! empty($r['user'])) {
                $issueGroups[$compositeKey]['users'][$r['user']] = true;
            }
            if (! empty($r['timestamp'])) {
                $issueGroups[$compositeKey]['timestamps'][] = $r['timestamp'];
            }
        }

        if (empty($issueGroups)) {
            return;
        }

        $snapshot = $this->notifier->snapshotExistingIssues($this->pdo(), $issueGroups, 'performance');
        $this->upsertPerformanceIssues($issueGroups, $snapshot['reopen'] ?? []);
        $this->notifier->queueIssueNotifications($this->appName, $issueGroups, 'performance', $snapshot);
    }

    /**
     * Upsert performance issues — same pattern as syncIssuesToExceptions.
     */
    /**
     * @param  array<string, int>  $reopenIds  Composite key → issue id for fingerprints
     *                                         transitioning resolved → open in this batch.
     */
    private function upsertPerformanceIssues(array $issueGroups, array $reopenIds = []): void
    {
        // Performance issues use GREATEST for users_count instead of addition.
        // Unlike exceptions (which have a dedicated table for accurate counting),
        // performance issue users come from various source tables, so we use
        // GREATEST to prevent unbounded inflation while keeping the high-water mark.
        $upsertStmt = $this->pdo()->prepare('
            INSERT INTO nightowl_issues (
                type, deploy, environment, subtype, status, exception_class, exception_message, group_hash,
                first_seen_at, last_seen_at, occurrences_count, users_count,
                threshold_ms, triggered_duration_ms,
                created_at, updated_at
            ) VALUES (
                :type, :deploy, :environment, :subtype, :status, :exception_class, :exception_message, :group_hash,
                :first_seen_at, :last_seen_at, :occurrences_count, :users_count,
                :threshold_ms, :triggered_duration_ms,
                :created_at, :updated_at
            )
            ON CONFLICT (group_hash, type, environment) DO UPDATE SET
                subtype = COALESCE(EXCLUDED.subtype, nightowl_issues.subtype),
                last_seen_at = GREATEST(nightowl_issues.last_seen_at, EXCLUDED.last_seen_at),
                occurrences_count = nightowl_issues.occurrences_count + EXCLUDED.occurrences_count,
                users_count = GREATEST(nightowl_issues.users_count, EXCLUDED.users_count),
                threshold_ms = COALESCE(EXCLUDED.threshold_ms, nightowl_issues.threshold_ms),
                triggered_duration_ms = GREATEST(
                    COALESCE(nightowl_issues.triggered_duration_ms, 0),
                    COALESCE(EXCLUDED.triggered_duration_ms, 0)
                ),
                status = CASE
                    WHEN :should_reopen::boolean AND nightowl_issues.status = \'resolved\'
                        THEN \'open\'
                    ELSE nightowl_issues.status
                END,
                updated_at = EXCLUDED.updated_at
        ');

        $now = gmdate('Y-m-d H:i:s');

        foreach ($issueGroups as $key => $group) {
            $timestamps = $group['timestamps'];
            sort($timestamps);
            $firstSeen = ! empty($timestamps) ? gmdate('Y-m-d H:i:s', (int) $timestamps[0]) : $now;
            $lastSeen = ! empty($timestamps) ? gmdate('Y-m-d H:i:s', (int) end($timestamps)) : $now;
            $userCount = count($group['users']);

            $thresholdUs = $group['threshold_us'] ?? null;
            $maxDurationUs = $group['max_duration_us'] ?? null;
            $thresholdMs = $thresholdUs !== null ? (int) round($thresholdUs / 1000) : null;
            $triggeredMs = $maxDurationUs !== null ? (int) round($maxDurationUs / 1000) : null;

            $upsertStmt->execute([
                'type' => 'performance',
                'deploy' => $group['deploy'] ?? null,
                'environment' => $group['environment'] ?? $this->environment,
                'subtype' => $group['subtype'] ?? null,
                'status' => 'open',
                'exception_class' => $group['name'],
                'exception_message' => 'Duration exceeded threshold',
                'group_hash' => $group['fingerprint'],
                'first_seen_at' => $firstSeen,
                'last_seen_at' => $lastSeen,
                'occurrences_count' => $group['count'],
                'users_count' => $userCount,
                'threshold_ms' => $thresholdMs,
                'triggered_duration_ms' => $triggeredMs,
                'created_at' => $now,
                'updated_at' => $now,
                'should_reopen' => isset($reopenIds[$key]) ? 'true' : 'false',
            ]);
        }

        $this->logReopenActivity($reopenIds, $now);
    }

    /**
     * Append a status_changed activity row for each issue auto-reopened by the agent.
     * Only inserts rows whose nightowl_issues.status actually flipped resolved → open
     * (the upsert may have skipped the flip if a concurrent writer changed status
     * between snapshot and upsert), keeping the activity log honest.
     *
     * @param  array<string, int>  $reopenIds  Composite key → issue id
     */
    private function logReopenActivity(array $reopenIds, string $now): void
    {
        if (empty($reopenIds)) {
            return;
        }

        try {
            $insert = $this->pdo()->prepare('
                INSERT INTO nightowl_issue_activity
                    (issue_id, user_id, user_name, actor_type, actor_meta,
                     action, old_value, new_value, created_at)
                SELECT :issue_id, NULL, NULL, \'agent\', NULL,
                       \'status_changed\', \'resolved\', \'open\', :created_at
                WHERE EXISTS (
                    SELECT 1 FROM nightowl_issues
                    WHERE id = :id_check AND status = \'open\'
                )
            ');

            foreach ($reopenIds as $issueId) {
                $insert->execute([
                    'issue_id' => $issueId,
                    'id_check' => $issueId,
                    'created_at' => $now,
                ]);
            }
        } catch (\Throwable $e) {
            // Activity log is best-effort — don't fail the whole drain over it
            error_log('[NightOwl Agent] Failed to log reopen activity: '.$e->getMessage());
        }
    }
}
