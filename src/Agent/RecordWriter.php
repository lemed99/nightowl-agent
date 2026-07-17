<?php

namespace NightOwl\Agent;

use NightOwl\Support\DDSketchHistogram;
use NightOwl\Support\QueryHistogram;
use NightOwl\Support\RawPartitions;
use NightOwl\Support\RollupSpec;
use NightOwl\Support\RollupSpecs;
use NightOwl\Support\RollupTiers;
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

    /** @var array<string, array<string, int>> Cached per table: column name => varchar character limit. */
    private array $columnLimits = [];

    /** @var array<string, true> Table.column pairs already warned about, so a repeat offender can't storm the log. */
    private array $clampWarned = [];

    /**
     * Built once per table (base query rollup + its hour/day tiers): the queries
     * rollup upsert's row-count-invariant parts — the VALUES tuple, and the text
     * either side of the tuple list (which includes the generated hist_NN columns).
     *
     * Keyed on the table ALONE. array_chunk leaves a tail of any size from 1 to
     * ROLLUP_UPSERT_CHUNK, so a row count in the key would cache one whole statement
     * string (~75KB at 500 rows) per distinct tail size per table, for the life of a
     * drain worker whose RSS is already back-pressure-gated. Only the tuple
     * REPETITION varies with the row count, and repeating a cached string is cheap —
     * the prepared statement was never cached anyway.
     *
     * @var array<string, array{tuple: string, prefix: string, suffix: string}>
     */
    private array $rollupUpsertSqlCache = [];

    /**
     * Built once per rollup table: the generic spec-driven upsert's
     * row-count-invariant parts. Same shape and same table-only keying as
     * rollupUpsertSqlCache.
     *
     * @var array<string, array{tuple: string, prefix: string, suffix: string}>
     */
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
        // Drain network deadline. New params sit at position 11+ so every existing
        // positional call site (production and tests) is unaffected.
        private bool $timeoutsEnabled = true,
        private int $connectTimeout = 10,
        private int $tcpUserTimeoutMs = 20000,
        private int $keepalivesIdle = 10,
        private int $keepalivesInterval = 5,
        private int $keepalivesCount = 3,
        private int $lockTimeoutMs = 10000,
        private ?DrainHeartbeat $heartbeat = null,
        private int $idleTxnTimeoutMs = 30000,
    ) {
        $this->notifier = $notifier ?? new AlertNotifier;
    }

    /**
     * Pre-1.2.14 DSN connect_timeout. Kept ONLY for the kill-switch path: it never
     * governed connect (PDO_PGSQL's ATTR_TIMEOUT-derived value wins, libpq being
     * last-key-wins) but it DOES govern the SSL handshake, so the legacy DSN has to
     * reproduce it byte-for-byte.
     */
    private const LEGACY_CONNECT_TIMEOUT = 5;

    /**
     * Whether THIS PROCESS's libpq understands `tcp_user_timeout` (libpq >= 12).
     * Probed once; a property of the linked libpq, not of the host.
     */
    private static ?bool $tcpUserTimeoutSupported = null;

    private function connect(): void
    {
        $this->heartbeat?->enter('pg:connect');

        $options = [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION];

        if ($this->timeoutsEnabled) {
            // The ONLY control over the connect bound. PDO_PGSQL appends its own
            // connect_timeout derived from this and libpq is last-key-wins, so a
            // connect_timeout in the DSN is dead code. Never leave it unset (the old
            // 30s bound was an accident of PDO's default) and never 0 (hangs
            // unbounded). It bounds connect ONLY — a pg_sleep(3) under
            // ATTR_TIMEOUT=10 completes in 3.01s, so it cannot cut a live drain.
            $options[PDO::ATTR_TIMEOUT] = max(1, $this->connectTimeout);
        }

        $this->pdo = new PDO($this->dsn(), $this->username, $this->password, $options);

        // NOTE: `SET synchronous_commit = off` used to run HERE, session-scoped.
        // A plain SET survives commit, so through a transaction-mode pooler it leaks
        // onto the shared server connection and silently weakens durability for
        // whatever OTHER application borrows it next. It now runs as SET LOCAL inside
        // doWrite()'s transaction (applyTransactionGuards), which still governs this
        // transaction's own commit, so the 2-5x write throughput is unchanged.
    }

    /**
     * Client-side socket deadline.
     *
     * BOTH knobs are required; they cover disjoint regimes:
     *   send-blocked (a COPY in flight, unacked data): keepalives do NOTHING,
     *     because a socket with unacked data is not idle and the retransmit timer
     *     governs. tcp_user_timeout reaps it.
     *   idle-read (awaiting a result, nothing unacked): tcp_user_timeout does
     *     NOTHING — it only counts unacked time. Keepalives reap it (~25s).
     *
     * Measured against a true iptables partition (packets DROPped, so nothing ACKs
     * on the client's behalf — a frozen proxy cannot emulate this):
     *   no knobs               still wedged at 111s, and the old in-process SIGALRM
     *                          NEVER dispatched — it was never a deadline.
     *   tcp_user_timeout=5000  -> 5.22s
     *   tcp_user_timeout=20000 -> 20.32s   (the default)
     *   tcp_user_timeout=40000 -> 40.64s
     *
     * SCOPE, honestly: this bounds an UNREACHABLE peer, not an unresponsive one.
     * Keepalive probes are answered by the peer's KERNEL with no application
     * involvement, and tcp_user_timeout needs unacked data — so a reachable-but-
     * wedged backend or pooler is NOT bounded here. That regime belongs to
     * lock_timeout and to the operator, not to this.
     */
    private function dsn(): string
    {
        if (! $this->timeoutsEnabled) {
            // Kill switch: byte-identical to <= 1.2.13.
            return 'pgsql:'."host={$this->host};port={$this->port};dbname={$this->database}"
                .';connect_timeout='.self::LEGACY_CONNECT_TIMEOUT.";sslmode={$this->sslmode}";
        }

        $parts = [
            'host='.$this->host,
            'port='.$this->port,
            'dbname='.$this->database,
            'sslmode='.$this->sslmode,
        ];

        // Ancient libpq params — accepted by every libpq in support range, so they
        // ship without detection.
        if ($this->keepalivesIdle > 0 && $this->keepalivesInterval > 0 && $this->keepalivesCount > 0) {
            $parts[] = 'keepalives=1';
            $parts[] = 'keepalives_idle='.$this->keepalivesIdle;
            $parts[] = 'keepalives_interval='.$this->keepalivesInterval;
            $parts[] = 'keepalives_count='.$this->keepalivesCount;
        }

        if ($this->tcpUserTimeoutMs > 0 && self::libpqSupportsTcpUserTimeout()) {
            $parts[] = 'tcp_user_timeout='.$this->tcpUserTimeoutMs;
        }

        return 'pgsql:'.implode(';', $parts);
    }

    /**
     * Does THIS PROCESS's libpq understand `tcp_user_timeout` (libpq >= 12)?
     *
     * It must be feature-detected, never concatenated on faith: libpq rejects an
     * unknown conninfo keyword FATALLY. Verified against a real libpq 11.22:
     * `invalid connection option "tcp_user_timeout"` — connect fails outright, it
     * does not degrade. composer.json pins php ^8.2 with no libpq floor, so an older
     * client is a supported configuration.
     *
     * Ask libpq itself rather than a version number. libpq parses conninfo BEFORE
     * opening any socket, so point two probes at a target that can NEVER connect — a
     * Unix-socket directory that does not exist, so no DNS, no TCP, no firewall — and
     * compare the errors:
     *   same error => the keyword parsed; we failed at the socket, as the control
     *                 did => supported
     *   diff error => libpq rejected the keyword => unsupported
     *
     * Comparing two errors rather than matching message text keeps this locale-proof
     * (libpq's "invalid connection option" is a libpq_gettext string), and because it
     * never touches the real host, a Postgres outage can never mis-detect the flag off.
     *
     * Measured: false on libpq 11.22, true on 13.23 / 17.10 / 18.3; negative control
     * `zzz_bogus=1` false on all four. 0.04-1.07ms, once per process.
     */
    private static function libpqSupportsTcpUserTimeout(): bool
    {
        if (self::$tcpUserTimeoutSupported !== null) {
            return self::$tcpUserTimeoutSupported;
        }

        $probe = static function (string $extra): string {
            try {
                new PDO('pgsql:host=/nonexistent-nightowl-probe;dbname=nightowl_probe'.$extra, '', '');

                return '<connected>'; // impossible against a nonexistent socket dir
            } catch (\PDOException $e) {
                return $e->getMessage();
            }
        };

        try {
            self::$tcpUserTimeoutSupported = $probe('') === $probe(';tcp_user_timeout=1000');
        } catch (\Throwable) {
            self::$tcpUserTimeoutSupported = false; // inconclusive -> pre-1.2.14 behaviour
        }

        if (! self::$tcpUserTimeoutSupported) {
            error_log('[NightOwl Drain] libpq does not support tcp_user_timeout (needs libpq 12+) — '
                .'a network stall while writing a batch will be bounded only by the kernel '
                .'(net.ipv4.tcp_retries2, ~15 min by default), not by the agent. '
                .'Upgrade libpq, or lower tcp_retries2 on this host.');
        } elseif (PHP_OS_FAMILY !== 'Linux') {
            // libpq's setsockopt sits inside #ifdef TCP_USER_TIMEOUT, so the param is
            // ACCEPTED and INERT here (libpq 18.3 on macOS parses it and connects).
            // Without this line the probe's `true` reads as "protected" and a local
            // repro would look like the fix works.
            error_log('[NightOwl Drain] tcp_user_timeout is accepted but INERT on '
                .PHP_OS_FAMILY.' (Linux-only) — keepalives still bound an idle-read stall, '
                .'but a send-blocked stall falls back to the kernel. Dev machines only; '
                .'do not use this host to validate stall handling.');
        }

        return self::$tcpUserTimeoutSupported;
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
            // Top-level 'drain_connection', NOT nested under 'database':
            // mergeConfigFrom() is a shallow array_merge, so a published config's
            // 'database' array wholly replaces the package's and would silently
            // swallow any new sub-key there (and its env var with it).
            (bool) config('nightowl.drain_connection.timeouts_enabled', true),
            (int) config('nightowl.drain_connection.connect_timeout', 10),
            (int) config('nightowl.drain_connection.tcp_user_timeout_ms', 20000),
            (int) config('nightowl.drain_connection.keepalives_idle', 10),
            (int) config('nightowl.drain_connection.keepalives_interval', 5),
            (int) config('nightowl.drain_connection.keepalives_count', 3),
            (int) config('nightowl.drain_connection.lock_timeout_ms', 10000),
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
     *
     * Also drops the cached rollup upsert SQL. Each cached statement bakes in a
     * column set derived from the sketch/hist probes, which fail OPEN and UNCACHED
     * (see sketchEnabled/histEnabled) — so a statement built while a probe was
     * momentarily failing can carry a column count that no longer matches the
     * per-call flattened params once the probe recovers, and the cache is not
     * transactional so a rolled-back batch leaves it populated. Rebuilding it
     * against the fresh connection keeps the SQL and its bound params in lock-step.
     * The probe caches store only SUCCESSFUL probes, so they stay valid and are kept.
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
        $this->rollupSqlCache = [];
        $this->rollupUpsertSqlCache = [];
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

        $this->heartbeat?->enter('pg:begin');

        // beginTransaction() is INSIDE the try. It is a network round trip like any
        // other, so a stall can land on it — measured against a true partition, that
        // is exactly where a stalled batch blocks, because BEGIN is the first thing on
        // the wire and the socket dies with only a few hundred bytes sent. With BEGIN
        // outside the try, that throw bypassed this catch entirely: lastWriteError was
        // never stamped, so the health report lost the SQLSTATE and the failing table
        // and the drain worker had to fall back to scanning the raw message. The catch
        // is safe for a BEGIN failure: inTransaction() is false, so the guarded
        // rollBack() is skipped.
        try {
            $pdo->beginTransaction();
            $this->applyTransactionGuards($pdo);

            foreach ($grouped as $type => $typeRecords) {
                $this->heartbeat?->enter("pg:write:{$type}");
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

            $this->heartbeat?->enter('pg:commit');
            $pdo->commit();
            // All writes in this batch committed — publish the landed tables so the
            // drain worker can clear those tables' poison-breaker streaks.
            $this->lastWrittenTables = array_keys($this->pendingWrittenTables);
        } catch (\Throwable $e) {
            $this->notifier->clearPending(); // Discard — data was rolled back

            // Classify BEFORE touching the connection. rollBack() on a handle the
            // socket deadline just killed THROWS (inTransaction() reports true, then
            // PDOException SQLSTATE[HY000] "no connection to the server"), and an
            // exception thrown inside a catch abandons the rest of the block — so
            // with the old ordering lastWriteError was never assigned and `throw $e`
            // never ran. write() then classified the ROLLBACK's exception instead of
            // the real one, the health report lost the SQLSTATE and the failing
            // table, and with quarantine enabled a null lastWriteError is neither
            // whole-target nor transient, so the batch bisected and quarantined good
            // rows.
            //
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

            try {
                if ($pdo->inTransaction()) {
                    $pdo->rollBack();
                }
            } catch (\Throwable) {
                // The handle is already dead — the server has reaped the backend and
                // there is nothing to roll back. Never let this mask the real error.
                // Mirrors reconnect()'s existing guard.
            }

            throw $e;
        }

        // Dispatch notifications AFTER commit — no blocking I/O inside the transaction
        $this->notifier->flushNotifications($pdo);
    }

    /**
     * Transaction-scoped guards. These apply to EVERY statement in the batch — the
     * to_regclass probes, the settings SELECT, all 10 COPYs, the 14 rollup upserts,
     * the issues/users upserts and the COMMIT — which is coverage the per-call-site
     * SIGALRM never had.
     *
     * SET LOCAL, not SET, and for synchronous_commit that is a bugfix as much as
     * hardening: the plain SET at connect() ran OUTSIDE any transaction, so through a
     * TRANSACTION-MODE pooler it leaked onto the shared server connection and silently
     * weakened durability for whatever OTHER application borrowed it next (PgBouncer
     * only runs DISCARD ALL in session mode by default). SET LOCAL reverts at both
     * COMMIT and ROLLBACK, so nothing escapes the batch, and it still governs this
     * transaction's own commit — throughput is unchanged. One exec, one round trip.
     *
     * lock_timeout is deliberately NOT exempted around the rollup advisory lock. With
     * a backfill chunk holding the paired EXCLUSIVE lock, the drain's shared lock
     * aborts with 55P03 — and 55P03 hits DrainWorker::isTransientFailure(), checked
     * before the bisect, which DEFERS the batch and leaves the rows in SQLite. That is
     * already correct: a drain blocked on the lock and a drain deferring on 55P03 both
     * land zero rows for the chunk's duration, but only the deferring one keeps its
     * loop responsive. Exempting it (SET LOCAL lock_timeout = 0) would reintroduce an
     * unbounded wait AND, via a finally-restore, throw 25P02 over the real exception —
     * which is neither whole-target nor transient, so with quarantine on it would
     * bisect and quarantine good rows.
     *
     * idle_in_transaction_session_timeout is the ORPHAN reaper. When this process
     * abandons a batch mid-transaction (reconnect after a false-dead verdict, a
     * kill, a pooler blip), the server-side session can survive holding the batch's
     * uncommitted inserts — and the retry then collides with its own ghost's unique-
     * index entries and dies on 55P03 (observed live: nightowl_issues, index tuple
     * (0,1), through Supavisor). SET LOCAL is sufficient AND the point: the timeout
     * only ever fires while idle INSIDE a transaction, which is exactly SET LOCAL's
     * scope — and an orphan never commits, so the LOCAL value governs it for life.
     * Nothing leaks through transaction-mode poolers, and other applications on the
     * customer's database are never touched. It cannot reap a HEALTHY drain: it
     * counts only idle-between-statements time (measured ~27ms in a live batch),
     * never time inside a slow statement — a stuck-active COPY is tcp_user_timeout's
     * job, not this one's.
     */
    private function applyTransactionGuards(PDO $pdo): void
    {
        // Unconditional: the pooler leak fix is a bug fix, not a tunable.
        $sets = ['SET LOCAL synchronous_commit = off'];

        if ($this->timeoutsEnabled && $this->lockTimeoutMs > 0) {
            $sets[] = 'SET LOCAL lock_timeout = '.$this->lockTimeoutMs;
        }

        if ($this->timeoutsEnabled && $this->idleTxnTimeoutMs > 0) {
            $sets[] = 'SET LOCAL idle_in_transaction_session_timeout = '.$this->idleTxnTimeoutMs;
        }

        $pdo->exec(implode('; ', $sets));
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
        //
        // HY000 is the one exception, and it is load-bearing. It is PDO's GENERIC code,
        // not a Postgres SQLSTATE — it is what pdo_pgsql reports for a libpq TRANSPORT
        // failure with no server-side error, and it is exactly what the socket deadline
        // produces: a tcp_user_timeout-killed COPY throws SQLSTATE[HY000] "could not
        // receive data from server: Connection timed out", and rollBack() on that dead
        // handle throws SQLSTATE[HY000] "no connection to the server". Both must fall
        // through to the message scan below.
        //
        // Without this carve-out the deadline is WORSE than the wedge it replaces: the
        // stall classifies as a write error, write() skips reconnect(), the dead handle
        // stays cached (copyBatch's `$this->pdo = null` sits on the returns-false
        // branch, which a THROWING COPY never takes) and every later batch reuses it —
        // an infinite failure loop instead of an infinite wedge.
        //
        // The scan stays safe for HY000 specifically because a transport error carries
        // no server-reported row context; anything with a real SQLSTATE still returns
        // false here without ever touching the message.
        if (is_string($sqlstate) && $sqlstate !== '' && $sqlstate !== 'HY000') {
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
    /**
     * Character limits of $table's length-constrained columns, keyed by column name.
     *
     * Introspected from the live target rather than hardcoded from the migrations:
     * the tenant owns their database, so a column they widened themselves (the
     * documented varchar(n) → text unstick for a poison row) must not still be
     * clamped to 255 by us. `text` columns have a NULL character_maximum_length and
     * are absent from the map — i.e. never clamped.
     *
     * A SUCCESSFUL probe is cached for the process lifetime, mirroring
     * rollupColumnsChecked: a column's width only changes under a migration, which
     * restarts the agent. A cached empty map therefore means "this table genuinely
     * has no length-constrained columns" and never "the probe failed".
     *
     * @return array<string, int>
     */
    private function columnLimits(string $table): array
    {
        if (array_key_exists($table, $this->columnLimits)) {
            return $this->columnLimits[$table];
        }

        try {
            $stmt = $this->pdo()->prepare(
                'SELECT column_name, character_maximum_length FROM information_schema.columns
                 WHERE table_schema = \'public\' AND table_name = ? AND character_maximum_length IS NOT NULL'
            );
            $stmt->execute([$table]);
            $limits = [];
            foreach ($stmt->fetchAll(\PDO::FETCH_ASSOC) as $row) {
                $limits[(string) $row['column_name']] = (int) $row['character_maximum_length'];
            }
        } catch (\Throwable) {
            // Probe failed — clamp nothing rather than guessing a width and
            // silently mangling good data. A genuine overflow still surfaces as
            // SQLSTATE 22001, which is the pre-clamping behaviour, not a regression.
            //
            // Never cached. This probe rides the drain connection, so a PG restart or
            // a network blip fails it; caching that would leave clamping off for the
            // rest of the process and hand the next over-long value a 22001 that
            // head-of-line-blocks the drain. Fails open UNCACHED, the same contract as
            // the api's TenantTableProbe::exists().
            return [];
        }

        return $this->columnLimits[$table] = $limits;
    }

    /**
     * Clamp one value to its column's width, or return it untouched.
     *
     * Postgres counts varchar(n) in CHARACTERS, not bytes, so the authoritative
     * check is mb_strlen. strlen() is the fast path: UTF-8 never uses fewer bytes
     * than characters, so strlen() <= $max proves the value fits and lets the
     * overwhelming majority of values skip mb_* entirely — this runs on every
     * string of every row of every batch (~250k values at drain_batch_size 5000).
     */
    private function clampToColumn(string $table, string $column, mixed $value, int $max): mixed
    {
        if (! is_string($value) || strlen($value) <= $max) {
            return $value;
        }

        if (mb_strlen($value, 'UTF-8') <= $max) {
            return $value;
        }

        $key = $table.'.'.$column;
        if (! isset($this->clampWarned[$key])) {
            $this->clampWarned[$key] = true;
            // Length only — never the value, which is customer data (the same
            // reason lastWriteError withholds the raw libpq message).
            error_log(sprintf(
                '[NightOwl Agent] %s exceeds its varchar(%d) width — truncating to fit. '.
                'Widen it (ALTER TABLE %s ALTER COLUMN %s TYPE text) to store the full value.',
                $key, $max, $table, $column
            ));
        }

        return mb_substr($value, 0, $max, 'UTF-8');
    }

    /**
     * Clamp every length-constrained column in a named-parameter bind array.
     * Keys with no matching column (e.g. the upserts' `should_reopen` flag) are
     * left alone, so this is safe to drop over any execute() payload.
     *
     * @param  array<string, mixed>  $params
     * @return array<string, mixed>
     */
    private function clampParams(string $table, array $params): array
    {
        $limits = $this->columnLimits($table);
        if (empty($limits)) {
            return $params;
        }

        foreach ($params as $column => $value) {
            if (isset($limits[$column])) {
                $params[$column] = $this->clampToColumn($table, (string) $column, $value, $limits[$column]);
            }
        }

        return $params;
    }

    private function copyBatch(string $table, array $columns, array $rows): void
    {
        if (empty($rows)) {
            return;
        }

        $this->heartbeat?->enter("pg:copy:{$table}");
        $this->markWriteTarget($table);

        // Clamp before the forceInsert/Swoole branch below so the COPY and INSERT
        // paths write byte-identical values. An over-long field is otherwise
        // rejected with SQLSTATE 22001, and with quarantine off (the default) that
        // rejection head-of-line-blocks the ENTIRE drain forever: the batch is
        // retried intact every loop, the poison row fails again, and the buffer
        // fills until back-pressure starts refusing payloads. Truncating one field
        // is a far smaller loss than losing the pipeline. Same rationale as
        // eventEpoch()'s range guard on poison timestamps.
        //
        // Kept inside the heartbeat window: columnLimits() is a (once-per-table)
        // round trip on the drain connection, so a stall there must count against
        // the same wedge detector as the COPY itself.
        $limits = $this->columnLimits($table);
        $limited = [];
        foreach ($columns as $i => $column) {
            if (isset($limits[$column])) {
                $limited[$i] = $limits[$column];
            }
        }

        if (! empty($limited)) {
            foreach ($rows as &$row) {
                foreach ($limited as $i => $max) {
                    // isset() skips nulls, which need no clamping.
                    if (isset($row[$i])) {
                        $row[$i] = $this->clampToColumn($table, (string) $columns[$i], $row[$i], $max);
                    }
                }
            }
            unset($row);
        }

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
        // No SIGALRM backstop here. An async signal cannot preempt a blocked libpq
        // call: PHP dispatches async signals only at VM opcode boundaries, and libpq
        // retries EINTR internally, so the handler only ever ran the instant libpq
        // returned on its own — it never timed anything out. Reproduced against a true
        // partition: a 95s alarm around a stalled COPY NEVER dispatched, and the
        // process was still blocked when killed externally at 111s. The deadline now
        // comes from the socket (tcp_user_timeout + keepalives, see dsn()), which
        // bounds this COPY *and* every other statement on the connection, and surfaces
        // as a normal catchable exception.
        //
        // When pgsqlCopyFromArray does return, it returns false rather than
        // throwing (even under ERRMODE_EXCEPTION) on some errors. Convert to
        // an exception so the drain loop rolls back and retries the batch.
        $ok = $this->pdo()->pgsqlCopyFromArray($table.' ('.$colList.')', $tsvRows);

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
     * Keep future daily partitions pre-created on partitioned raw tables.
     * Called from the drain's maintenance tick; a non-blocking advisory lock
     * makes concurrent workers skip rather than queue, and any failure is
     * logged, never allowed to break draining. No-op on unpartitioned tenants.
     *
     * The lock is TRANSACTION-scoped (like lockRollupForWriteShared's, and the
     * backfill's), and the transaction spans the sweep it guards. A SESSION-scoped
     * pg_try_advisory_lock cannot be used here: the agent supports transaction-mode
     * poolers, where the lock and its pg_advisory_unlock can land on DIFFERENT server
     * connections, stranding the lock on a shared one forever. Every later tick would
     * then fail to acquire, maintenance would stop silently, and after DAYS_AHEAD days
     * every drained row would route to {t}_pdefault — which prune can only row-DELETE,
     * never DROP.
     *
     * A sweep that fails on some tables still COMMITS the children the others got.
     * RawPartitions savepoint-isolates each table and sweeps the rest, so those children
     * are real work: discarding them strands those tables' rows in {t}_pdefault exactly
     * as above, and one persistently failing table would do it on every tick, forever.
     * ensureFutureChildren therefore RETURNS its per-table failures (already isolated and
     * logged) rather than throwing, so the commit lands the healthy children and this tick
     * only logs the summary. The outer catch is for a failure that ESCAPES isolation — a
     * dead connection, or a BEGIN/guard/commit fault — which aborts the transaction, and
     * Postgres degrades the commit to a rollback on its own.
     *
     * The guards carry lock_timeout, which matters more here than on a write batch:
     * CREATE ... PARTITION OF takes ACCESS EXCLUSIVE on the parent, so a sweep queued
     * behind a long COPY would stall every reader that queues behind IT. Timing out one
     * table instead costs that table an hour (isolated, reported, retried next tick).
     */
    public function maintainRawPartitions(): void
    {
        $pdo = null;

        try {
            $pdo = $this->pdo();
            $pdo->beginTransaction();
            $this->applyTransactionGuards($pdo);

            $failures = [];
            $got = (bool) $pdo
                ->query("SELECT pg_try_advisory_xact_lock(hashtext('nightowl_partition_maintenance'))")
                ->fetchColumn();

            if ($got) {
                // Returns (never throws) the per-table failures it already isolated and
                // logged; the commit still lands every healthy table's children.
                $failures = RawPartitions::ensureFutureChildren($pdo);
            }

            $pdo->commit();

            if ($failures !== []) {
                error_log(sprintf(
                    '[NightOwl Drain] partition maintenance incomplete: %d partition(s) not created (will retry next tick)',
                    count($failures),
                ));
            }
        } catch (\Throwable $e) {
            try {
                if ($pdo?->inTransaction()) {
                    $pdo->rollBack();
                }
            } catch (\Throwable) {
                // Handle already dead — never let this mask the real error.
                // Mirrors doWrite()'s guarded rollback.
            }

            error_log('[NightOwl Drain] partition maintenance failed (will retry next tick): '.$e->getMessage());
        }
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
    private function rollupColumnsPresent(RollupSpec $spec, array $histColumns, ?string $table = null): bool
    {
        $table ??= $spec->table;
        if (array_key_exists($table, $this->rollupColumnsChecked)) {
            return $this->rollupColumnsChecked[$table];
        }

        // Mirror rollupSql()'s insert column set so the probe matches what we write.
        $expected = [...$spec->groupColumnNames(), 'bucket_start', 'environment', 'call_count', ...$spec->counterColumns()];
        if ($spec->hasDuration) {
            $expected = [...$expected, 'total_duration', 'min_duration', 'max_duration'];
        }
        if ($spec->hasHistogram && $this->histEnabled($table)) {
            $expected = [...$expected, ...$histColumns];
        }
        if ($spec->hasDurationCount && $this->durationCountEnabled($table)) {
            $expected[] = 'duration_count';
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

        // Clamp group-column values to their varchar width HERE, at the grouping layer,
        // not only before the INSERT: the group value IS the conflict key, so two values
        // sharing a 255-char prefix must land in the SAME additive group. Keyed by the
        // un-clamped value they would be two groups that clamp to one identical conflict
        // tuple, and the multi-row upsert then aborts the whole batch with 21000 ("cannot
        // affect row a second time") — a drain wedge, not a transient. Same clamp the raw
        // row gets (columnLimits agree across raw/rollup), so a row and its rollup still
        // key off the same value.
        $groupLimits = $this->columnLimits($spec->table);

        $groups = [];
        foreach ($records as $r) {
            // Bucket on the event's own time (matching created_at), so a backdated or
            // delayed drain lands in the correct minute instead of "now".
            $bucket = $this->eventBucket($r, $nowTs);
            $groupVals = [];
            $keyParts = [$bucket];
            foreach ($spec->groupColumns as $col => $def) {
                $value = (string) ($def['php'])($r);
                if (isset($groupLimits[$col])) {
                    $value = (string) $this->clampToColumn($spec->table, $col, $value, $groupLimits[$col]);
                }
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
                    'duration_count' => 0,
                    'min_duration' => null,
                    'max_duration' => null,
                    'hist' => $spec->hasHistogram ? array_fill(0, count($histColumns), 0) : [],
                    'sketch' => [],
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
                    // Exact by construction: incremented precisely where a
                    // duration folds in, so it always equals the histogram mass.
                    $groups[$key]['duration_count']++;
                    $groups[$key]['min_duration'] = $groups[$key]['min_duration'] === null
                        ? $duration : min($groups[$key]['min_duration'], $duration);
                    $groups[$key]['max_duration'] = $groups[$key]['max_duration'] === null
                        ? $duration : max($groups[$key]['max_duration'], $duration);
                    if ($spec->hasHistogram) {
                        $groups[$key]['hist'][QueryHistogram::binIndex($duration)]++;
                        // v2 sketch accumulates in parallel with the v1 bins
                        // (dual-write transition, specs/ddsketch_percentiles.md).
                        DDSketchHistogram::add($groups[$key]['sketch'], $duration);
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

        $this->upsertRollupGroups($spec, $histColumns, $groups, $spec->table);

        // Hour/day tiers: re-collapse the (already minute-collapsed) groups in
        // PHP and upsert the same additive shape into the sibling tables — no
        // extra per-record work, and each tier gates independently so an
        // un-migrated sibling just no-ops until nightowl:migrate + restart.
        foreach (RollupTiers::TIERS as $tier => $granularity) {
            $tierTable = RollupTiers::table($spec->table, $tier);

            if (! $this->rollupEnabled($tierTable)
                || ! $this->rollupColumnsPresent($spec, $histColumns, $tierTable)) {
                continue;
            }

            $this->markWriteTarget($tierTable);
            $this->lockRollupForWriteShared($tierTable);

            $this->upsertRollupGroups(
                $spec,
                $histColumns,
                RollupTiers::collapse($groups, $granularity, ['group']),
                $tierTable,
            );
        }
    }

    /**
     * Rows per multi-row upsert statement. Widest row is ~50 params (group cols
     * + counters + 39 hist bins + reps), so 500 rows stays far under PDO's
     * 65,535-parameter cap while collapsing a 200-group × 3-tier batch from
     * ~600 round trips into 3.
     */
    private const ROLLUP_UPSERT_CHUNK = 500;

    /**
     * Additive multi-row upsert into $table — shared by the base minute write
     * and the hour/day tier writes (identical column shape by construction:
     * tier tables are LIKE-clones of their base).
     *
     * Multi-row ON CONFLICT is safe from "cannot affect row a second time"
     * because $groups is keyed by exactly the conflict key AS CLAMPED (bucket +
     * group columns; environment is constant per writer). The grouping layer
     * (writeRollup / RollupTiers::collapse) applies the same varchar clamp these
     * tuples carry, so two group values that clamp equal already merged into ONE
     * additive group upstream — the emitted tuples stay unique even after the
     * per-column clamp below.
     *
     * @param  list<string>  $histColumns
     * @param  array<string, array<string, mixed>>  $groups
     */
    private function upsertRollupGroups(RollupSpec $spec, array $histColumns, array $groups, string $table): void
    {
        $withSketch = $spec->hasHistogram && $this->sketchEnabled($table);
        $withHist = $this->histEnabled($table);
        $withDurationCount = $spec->hasDurationCount && $this->durationCountEnabled($table);
        $insertCols = $this->rollupInsertColumns($spec, $histColumns, $withSketch, $withHist, $withDurationCount);

        // A rollup's group columns (user_id, group_hash, key, store, …) are the same
        // varchar(255) strings the raw tables carry, so they need the same clamp —
        // and the SAME one, or a raw row and its rollup would key off different
        // values. An unclamped rollup would head-of-line-block the drain exactly as
        // the raw row would (see copyBatch).
        $limits = $this->columnLimits($table);

        foreach (array_chunk($groups, self::ROLLUP_UPSERT_CHUNK) as $chunk) {
            $flat = [];
            foreach ($chunk as $g) {
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
                    if ($withDurationCount) {
                        $params['duration_count'] = $g['duration_count'];
                    }
                    if ($withHist) {
                        foreach ($histColumns as $i => $hc) {
                            $params[$hc] = $g['hist'][$i];
                        }
                    }
                }
                if ($withSketch) {
                    $params['sketch'] = bin2hex(DDSketchHistogram::pack($g['sketch'] ?? []));
                    $params['sketch_version'] = DDSketchHistogram::VERSION;
                }
                foreach ($g['reps'] as $rc => $value) {
                    $params[$rc] = $value;
                }

                // Flatten in insert-column order — the positional contract
                // with rollupSql()'s VALUES tuples.
                foreach ($insertCols as $col) {
                    $flat[] = isset($limits[$col])
                        ? $this->clampToColumn($table, $col, $params[$col], $limits[$col])
                        : $params[$col];
                }
            }

            $this->pdo()
                ->prepare($this->rollupSql($spec, $histColumns, $table, count($chunk)))
                ->execute($flat);
        }
    }

    /**
     * Build (and cache) the spec-driven upsert SQL. Counters, duration totals,
     * and histogram bins accumulate additively; min/max use LEAST/GREATEST;
     * representatives keep the first-seen value via COALESCE.
     *
     * @param  list<string>  $histColumns
     */
    /** @var array<string, bool> per-table: does it carry the DDSketch columns (000057 applied + CREATE FUNCTION allowed)? */
    private array $sketchColumnChecked = [];

    /** @var array<string, bool> per-table: does it still carry the v1 hist_NN columns (pre nightowl:drop-v1-histograms)? */
    private array $histColumnChecked = [];

    /** @var array<string, bool> per-table: does it carry the duration_count counter (000061 applied)? */
    private array $durationCountChecked = [];

    /**
     * Whether $table carries the duration_count column (migration 000061).
     * False on un-migrated tenants — the writer then omits the column instead
     * of disabling the whole rollup, and nightowl:migrate + a restart pick it
     * up (backfill-rollups closes the gap for rows written in between).
     */
    private function durationCountEnabled(string $table): bool
    {
        if (array_key_exists($table, $this->durationCountChecked)) {
            return $this->durationCountChecked[$table];
        }

        try {
            $stmt = $this->pdo()->prepare(
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ? AND column_name = 'duration_count'"
            );
            $stmt->execute([$table]);

            return $this->durationCountChecked[$table] = (int) $stmt->fetchColumn() > 0;
        } catch (\Throwable) {
            // Probe failed — omit the column for THIS call (always safe), but do NOT
            // cache it, so a transient blip doesn't downgrade the tenant for the whole
            // process life. Mirrors histEnabled()/sketchEnabled()/columnLimits().
            return false;
        }
    }

    /**
     * Whether $table still carries the v1 √2 histogram columns. False after
     * the operator runs nightowl:drop-v1-histograms (post-DDSketch cleanup) —
     * the writer then upserts sketch-only rows instead of failing on unknown
     * columns.
     */
    private function histEnabled(string $table): bool
    {
        if (array_key_exists($table, $this->histColumnChecked)) {
            return $this->histColumnChecked[$table];
        }

        try {
            $stmt = $this->pdo()->prepare(
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ? AND column_name = 'hist_00'"
            );
            $stmt->execute([$table]);

            return $this->histColumnChecked[$table] = (int) $stmt->fetchColumn() > 0;
        } catch (\Throwable) {
            // Probe failed — assume the pre-drop layout for THIS call (fail-open) but do
            // NOT cache it. This rides the drain connection, so a PG restart or a blip
            // fails it; caching hist=true against a post-drop tenant would emit hist_NN
            // columns on every later batch → 42703 → the whole drain wedges until restart.
            // Uncached, the next batch re-probes and self-corrects. Same contract as
            // columnLimits().
            return true;
        }
    }

    /**
     * Whether $table carries the v2 DDSketch columns. False on tenants that
     * haven't run 000057 or whose PG denied CREATE FUNCTION — the drain then
     * writes the v1 histogram only, and the reader stays on the √2 path.
     */
    private function sketchEnabled(string $table): bool
    {
        if (array_key_exists($table, $this->sketchColumnChecked)) {
            return $this->sketchColumnChecked[$table];
        }

        try {
            $stmt = $this->pdo()->prepare(
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ? AND column_name = 'sketch'"
            );
            $stmt->execute([$table]);

            return $this->sketchColumnChecked[$table] = (int) $stmt->fetchColumn() > 0;
        } catch (\Throwable) {
            // Probe failed — assume no sketch column for THIS call (safe: the drain
            // writes the v1 hist only) but do NOT cache it. Caching false on a blip would
            // silently downgrade a 000057-migrated tenant to v1 percentiles for the whole
            // process life; uncached, the next batch re-probes. Same contract as
            // columnLimits().
            return false;
        }
    }

    /**
     * The ordered insert-column list for a spec's rollup upsert — the contract
     * between rollupSql()'s VALUES tuples and the positional flattening in
     * upsertRollupGroups().
     *
     * @param  list<string>  $histColumns
     * @return list<string>
     */
    private function rollupInsertColumns(RollupSpec $spec, array $histColumns, bool $withSketch = false, bool $withHist = true, bool $withDurationCount = false): array
    {
        $insertCols = [...$spec->groupColumnNames(), 'bucket_start', 'environment', 'call_count', ...$spec->counterColumns()];
        if ($spec->hasDuration) {
            $insertCols = [...$insertCols, 'total_duration', 'min_duration', 'max_duration'];
        }
        if ($withDurationCount) {
            $insertCols[] = 'duration_count';
        }
        if ($spec->hasHistogram && $withHist) {
            $insertCols = [...$insertCols, ...$histColumns];
        }
        if ($withSketch) {
            $insertCols = [...$insertCols, 'sketch', 'sketch_version'];
        }

        return [...$insertCols, ...$spec->representativeColumns()];
    }

    private function rollupSql(RollupSpec $spec, array $histColumns, ?string $table = null, int $rowCount = 1): string
    {
        $table ??= $spec->table;

        if (! isset($this->rollupSqlCache[$table])) {
            $groupCols = $spec->groupColumnNames();
            $withSketch = $spec->hasHistogram && $this->sketchEnabled($table);
            $withHist = $this->histEnabled($table);
            $withDurationCount = $spec->hasDurationCount && $this->durationCountEnabled($table);
            $insertCols = $this->rollupInsertColumns($spec, $histColumns, $withSketch, $withHist, $withDurationCount);

            // Positional placeholders, one tuple per row: multi-row VALUES turns a
            // round trip per group into one statement per chunk. The sketch value
            // travels hex-encoded (PDO's flat positional execute() can't tag a
            // param as LOB) and decodes SQL-side.
            $placeholders = array_map(
                static fn (string $c): string => $c === 'sketch' ? "decode(?, 'hex')" : '?',
                $insertCols,
            );
            $pk = [...$groupCols, 'bucket_start', 'environment'];

            $t = $table;
            $set = ["call_count = {$t}.call_count + EXCLUDED.call_count"];
            foreach ($spec->counterColumns() as $c) {
                $set[] = "{$c} = {$t}.{$c} + EXCLUDED.{$c}";
            }
            if ($spec->hasDuration) {
                $set[] = "total_duration = {$t}.total_duration + EXCLUDED.total_duration";
                $set[] = "min_duration = LEAST({$t}.min_duration, EXCLUDED.min_duration)";
                $set[] = "max_duration = GREATEST({$t}.max_duration, EXCLUDED.max_duration)";
            }
            if ($withDurationCount) {
                $set[] = "duration_count = {$t}.duration_count + EXCLUDED.duration_count";
            }
            if ($spec->hasHistogram && $withHist) {
                foreach ($histColumns as $c) {
                    $set[] = "{$c} = {$t}.{$c} + EXCLUDED.{$c}";
                }
            }
            if ($withSketch) {
                // Row-lock-serialised SQL-side merge — no PHP read-modify-write.
                $set[] = "sketch = nightowl_ddsketch_merge({$t}.sketch, EXCLUDED.sketch)";
                $set[] = 'sketch_version = EXCLUDED.sketch_version';
            }
            foreach ($spec->representativeColumns() as $c) {
                $set[] = "{$c} = COALESCE({$t}.{$c}, EXCLUDED.{$c})";
            }

            $this->rollupSqlCache[$table] = [
                'tuple' => '('.implode(', ', $placeholders).')',
                'prefix' => sprintf('INSERT INTO %s (%s) VALUES ', $t, implode(', ', $insertCols)),
                'suffix' => sprintf(' ON CONFLICT (%s) DO UPDATE SET %s', implode(', ', $pk), implode(', ', $set)),
            ];
        }

        ['tuple' => $tuple, 'prefix' => $prefix, 'suffix' => $suffix] = $this->rollupSqlCache[$table];

        return $prefix.implode(', ', array_fill(0, $rowCount, $tuple)).$suffix;
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
    /**
     * The bespoke query-rollup twin of rollupColumnsPresent(). writeQueryRollups
     * carries no RollupSpec, so it needs its own gate: a query rollup table that
     * EXISTS but lacks a written column (a partial/failed migration) raises 42703
     * from the prepared upsert INSIDE the shared drain transaction, rolling the raw
     * COPY back and — because a prepared-statement failure can't be poison-row-
     * bisected — head-of-line-blocking the whole drain. Verify the full written set
     * once per table (same order queryRollupInsertColumns() builds it), disable the
     * rollup and keep draining raw if a column is missing. Probed via
     * information_schema and cached, sharing rollupColumnsChecked with writeRollup.
     *
     * @param  list<string>  $histColumns
     */
    private function queryRollupColumnsPresent(array $histColumns, string $table): bool
    {
        if (array_key_exists($table, $this->rollupColumnsChecked)) {
            return $this->rollupColumnsChecked[$table];
        }

        $expected = $this->queryRollupInsertColumns(
            $histColumns,
            $this->sketchEnabled($table),
            $this->histEnabled($table),
        );

        try {
            $stmt = $this->pdo()->prepare(
                'SELECT column_name FROM information_schema.columns WHERE table_schema = \'public\' AND table_name = ?'
            );
            $stmt->execute([$table]);
            $present = $stmt->fetchAll(\PDO::FETCH_COLUMN);
        } catch (\Throwable) {
            // Probe itself failed — the table-existence gate already passed, so stay
            // permissive rather than silently dropping every query rollup.
            return $this->rollupColumnsChecked[$table] = true;
        }

        $missing = array_diff($expected, $present);
        if (! empty($missing)) {
            error_log('[NightOwl Agent] '.$table.' missing column(s) '.implode(', ', $missing).' — rollups for it disabled until nightowl:migrate runs (restart the agent afterward)');

            return $this->rollupColumnsChecked[$table] = false;
        }

        return $this->rollupColumnsChecked[$table] = true;
    }

    private function writeQueryRollups(array $records, int $nowTs): void
    {
        $histColumns = QueryHistogram::columns();

        // The bespoke query path carries no RollupSpec, so it needs its own
        // column-presence gate — the same protection writeRollup gets from
        // rollupColumnsPresent(): a table that EXISTS but lacks a written column would
        // raise 42703 from the prepared upsert INSIDE the shared drain transaction,
        // rolling the raw COPY back and head-of-line-blocking the whole drain.
        if (! $this->queryRollupColumnsPresent($histColumns, 'nightowl_query_rollups')) {
            return;
        }

        $this->markWriteTarget('nightowl_query_rollups');
        $this->lockRollupForWriteShared('nightowl_query_rollups');

        // Clamp the conflict-key columns (group_hash, connection) at the grouping layer
        // so two values that clamp equal merge into ONE additive group. Keyed by the
        // un-clamped value they would emit duplicate conflict tuples in the multi-row
        // upsert and abort the batch with 21000. Mirrors writeRollup / copyBatch.
        $limits = $this->columnLimits('nightowl_query_rollups');
        $clampKey = fn (string $column, string $value): string => isset($limits[$column])
            ? (string) $this->clampToColumn('nightowl_query_rollups', $column, $value, $limits[$column])
            : $value;

        // Bucket per-record on the event's own time (same clock as created_at), so a
        // backdated/delayed drain spreads across the right minutes instead of "now".
        $groups = [];
        foreach ($records as $r) {
            $bucket = $this->eventBucket($r, $nowTs);
            $groupHash = $clampKey('group_hash', (string) ($r['_group'] ?? ''));
            $connection = $clampKey('connection', (string) ($r['connection'] ?? '')); // '' sentinel (see migration)
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
                    'sketch' => [],
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
                DDSketchHistogram::add($groups[$key]['sketch'], $duration);
            }
            if ($groups[$key]['sql_query'] === null && isset($r['sql']) && $r['sql'] !== '') {
                $groups[$key]['sql_query'] = $r['sql'];
            }
        }

        if (empty($groups)) {
            return;
        }

        $this->upsertQueryRollupGroups($histColumns, $groups, 'nightowl_query_rollups');

        // Hour/day tiers — same re-collapse as the generic writeRollup path,
        // keyed on (group_hash, connection) to preserve the query rollup's
        // 4-column PK identity.
        foreach (RollupTiers::TIERS as $tier => $granularity) {
            $tierTable = RollupTiers::table('nightowl_query_rollups', $tier);

            if (! $this->rollupEnabled($tierTable)
                || ! $this->queryRollupColumnsPresent($histColumns, $tierTable)) {
                continue;
            }

            $this->markWriteTarget($tierTable);
            $this->lockRollupForWriteShared($tierTable);

            $this->upsertQueryRollupGroups(
                $histColumns,
                RollupTiers::collapse($groups, $granularity, ['group_hash', 'connection']),
                $tierTable,
            );
        }
    }

    /**
     * Multi-row upsert for the bespoke query-rollup shape. Tuples within one
     * statement are unique on the conflict key by construction: $groups is keyed
     * bucket + group_hash + connection, both key columns already clamped at the
     * grouping layer (writeQueryRollups / RollupTiers::collapse), so two values
     * that clamp equal merged upstream and the emitted tuples stay unique after
     * the per-column clamp below. Chunked like upsertRollupGroups.
     *
     * @param  list<string>  $histColumns
     * @param  array<string, array<string, mixed>>  $groups
     */
    private function upsertQueryRollupGroups(array $histColumns, array $groups, string $table): void
    {
        $withSketch = $this->sketchEnabled($table);
        $withHist = $this->histEnabled($table);

        // Same clamp as upsertRollupGroups — this path is bespoke, so it needs its
        // own application. Only the string columns can overflow; the rest are ints.
        $limits = $this->columnLimits($table);
        $clamp = fn (string $column, mixed $value): mixed => isset($limits[$column])
            ? $this->clampToColumn($table, $column, $value, $limits[$column])
            : $value;

        foreach (array_chunk($groups, self::ROLLUP_UPSERT_CHUNK) as $chunk) {
            $flat = [];
            foreach ($chunk as $g) {
                $flat[] = $clamp('group_hash', $g['group_hash']);
                $flat[] = $g['bucket_start'];
                $flat[] = $clamp('environment', $this->environment);
                $flat[] = $clamp('connection', $g['connection']);
                $flat[] = $g['call_count'];
                $flat[] = $g['total_duration'];
                $flat[] = $g['min_duration'];
                $flat[] = $g['max_duration'];
                $flat[] = $clamp('sql_query', $g['sql_query']);
                if ($withHist) {
                    foreach ($histColumns as $i => $column) {
                        $flat[] = $g['hist'][$i];
                    }
                }
                if ($withSketch) {
                    $flat[] = bin2hex(DDSketchHistogram::pack($g['sketch'] ?? []));
                    $flat[] = DDSketchHistogram::VERSION;
                }
            }

            $this->pdo()
                ->prepare($this->rollupUpsertSql($histColumns, $table, count($chunk)))
                ->execute($flat);
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
    /**
     * Query-rollup insert-column order — the positional contract between
     * rollupUpsertSql()'s VALUES tuples and upsertQueryRollupGroups().
     *
     * @param  list<string>  $histColumns
     * @return list<string>
     */
    private function queryRollupInsertColumns(array $histColumns, bool $withSketch = false, bool $withHist = true): array
    {
        $cols = ['group_hash', 'bucket_start', 'environment', 'connection',
            'call_count', 'total_duration', 'min_duration', 'max_duration', 'sql_query',
            ...($withHist ? $histColumns : [])];

        return $withSketch ? [...$cols, 'sketch', 'sketch_version'] : $cols;
    }

    private function rollupUpsertSql(array $histColumns, string $table = 'nightowl_query_rollups', int $rowCount = 1): string
    {
        if (! isset($this->rollupUpsertSqlCache[$table])) {
            $withSketch = $this->sketchEnabled($table);
            $withHist = $this->histEnabled($table);
            $insertColumns = $this->queryRollupInsertColumns($histColumns, $withSketch, $withHist);
            $placeholders = array_map(
                static fn (string $c): string => $c === 'sketch' ? "decode(?, 'hex')" : '?',
                $insertColumns,
            );

            $setClauses = [
                "call_count     = {$table}.call_count     + EXCLUDED.call_count",
                "total_duration = {$table}.total_duration + EXCLUDED.total_duration",
                "min_duration   = LEAST({$table}.min_duration,  EXCLUDED.min_duration)",
                "max_duration   = GREATEST({$table}.max_duration, EXCLUDED.max_duration)",
                "sql_query      = COALESCE({$table}.sql_query, EXCLUDED.sql_query)",
            ];
            if ($withHist) {
                foreach ($histColumns as $column) {
                    $setClauses[] = "{$column} = {$table}.{$column} + EXCLUDED.{$column}";
                }
            }
            if ($withSketch) {
                $setClauses[] = "sketch = nightowl_ddsketch_merge({$table}.sketch, EXCLUDED.sketch)";
                $setClauses[] = 'sketch_version = EXCLUDED.sketch_version';
            }

            $this->rollupUpsertSqlCache[$table] = [
                'tuple' => '('.implode(', ', $placeholders).')',
                'prefix' => sprintf('INSERT INTO %s (%s) VALUES ', $table, implode(', ', $insertColumns)),
                'suffix' => sprintf(
                    ' ON CONFLICT (group_hash, bucket_start, environment, connection) DO UPDATE SET %s',
                    implode(', ', $setClauses),
                ),
            ];
        }

        ['tuple' => $tuple, 'prefix' => $prefix, 'suffix' => $suffix] = $this->rollupUpsertSqlCache[$table];

        return $prefix.implode(', ', array_fill(0, $rowCount, $tuple)).$suffix;
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

            $stmt->execute($this->clampParams('nightowl_exceptions', [
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
            ]));

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

            $upsertStmt->execute($this->clampParams('nightowl_issues', [
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
            ]));
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

        if ($this->rollupEnabled('nightowl_command_rollups')) {
            $this->writeRollup($records, RollupSpecs::commands(), $nowTs);
        }

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
        $nowTs = time();

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
                // nightowl_logs.created_at is a text column, so Postgres will not reject an
                // out-of-range date the way it does on the timestamp-typed tables. An unguarded
                // year > 9999 sorts above every prune cutoff and the row can never be deleted.
                $this->eventCreatedAt($r, $nowTs),
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

            $stmt->execute($this->clampParams('nightowl_users', [
                'v' => $r['v'] ?? null,
                'user_id' => (string) $userId,
                'name' => $r['name'] ?? null,
                'email' => $r['username'] ?? null,
                'timestamp' => $r['timestamp'] ?? null,
                'created_at' => $now,
                'updated_at' => $now,
            ]));
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

        if ($this->rollupEnabled('nightowl_scheduled_task_rollups')) {
            $this->writeRollup($records, RollupSpecs::scheduledTasks(), $nowTs);
        }

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

            $upsertStmt->execute($this->clampParams('nightowl_issues', [
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
            ]));
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
