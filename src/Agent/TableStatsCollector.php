<?php

namespace NightOwl\Agent;

use NightOwl\Support\RollupStaleness;

/**
 * Hourly per-table statistics for the NightOwl platform — the measurement
 * layer that exists so nobody ever has to ask a customer to run SQL against
 * their own database again.
 *
 * COVERAGE IS CATALOG-DRIVEN BY CONSTRUCTION: every query enumerates
 * pg_stat_user_tables / pg_class for relname LIKE 'nightowl%' at runtime, so
 * a table added by ANY future release — spec rollup, bespoke rollup, tier
 * sibling, partition child — is covered the moment it exists. There is no
 * list here to forget (this codebase has been bitten by hand-listed table
 * sets repeatedly; this class is the anti-pattern's antidote).
 *
 * PRIVACY IS STRUCTURAL: everything read comes from catalog and statistics
 * views — row counts, byte sizes, scan/write counters, vacuum timestamps,
 * rollup bucket bounds. No query here CAN read row contents. Row counts are
 * still information (nightowl_users' count is the customer's user count),
 * which is why this ships in the same release window as the privacy-policy
 * update that discloses exactly this list. NIGHTOWL_TABLE_STATS=false is the
 * off switch.
 *
 * ISOLATION: own short-lived connection per run, with its own
 * statement_timeout — never the drain's connection, so a slow catalog can't
 * stall a batch and a wedged drain can't be blamed on this. Detection of a
 * dead pipeline is PLATFORM-side (sample staleness + per-sample n_tup_ins
 * deltas), so this collector needs no view of the drain at all: if samples
 * stop arriving, that absence IS the signal.
 *
 * Fleet dedupe: a per-tenant advisory try-lock means one worker reports per
 * interval even with NIGHTOWL_DRAIN_WORKERS > 1 or multiple app servers
 * sharing a tenant DB.
 */
final class TableStatsCollector
{
    private const STATEMENT_TIMEOUT_MS = 10_000;

    private const CONNECT_TIMEOUT_S = 5;

    private const POST_TIMEOUT_S = 5;

    private const ADVISORY_KEY = 'nightowl_table_stats';

    /** Partition-child suffixes folded into their logical parent. */
    private const CHILD_SUFFIX = '/_p(\d{8}|default|historic|new)$/';

    public function __construct(
        private string $host,
        private int $port,
        private string $database,
        private string $username,
        private string $password,
        private string $sslmode,
        private string $apiUrl,
        private ?string $token,
        private bool $enabled = true,
    ) {}

    public static function fromConfig(): self
    {
        return new self(
            (string) config('nightowl.database.host', '127.0.0.1'),
            (int) config('nightowl.database.port', 5432),
            (string) config('nightowl.database.database', 'nightowl'),
            (string) config('nightowl.database.username', 'nightowl'),
            (string) config('nightowl.database.password', 'nightowl'),
            (string) config('nightowl.database.sslmode', 'prefer'),
            (string) config('nightowl.agent.api_url', 'https://api.usenightowl.com'),
            config('nightowl.agent.token'),
            // TOP-LEVEL key (shallow-merge rule — a published config's nested
            // arrays replace the package's wholesale).
            (bool) config('nightowl.table_stats', true),
        );
    }

    /**
     * Collect and report one sample. NEVER throws — runs on the drain's
     * cleanup tick, which must not lose its WAL checkpoint to a stats hiccup.
     *
     * Returns the frozen-rollup verdict derived from the same sample (see
     * RollupStaleness), or null when no sample was taken — a skipped interval,
     * a lost advisory lock, a failed connect. Null means "no opinion, keep the
     * last one", never "all clear": treating a failed collection as healthy
     * would make a tenant whose DB is unreachable look like its rollups are
     * fine, which is the one moment they are least likely to be.
     *
     * This rides the stats pass rather than probing on its own because the
     * bounds it grades are ALREADY in the sample — one catalog-driven pass, one
     * connection, one advisory lock, hourly. A second scanner would be 40-odd
     * extra MAX() probes an hour to recompute numbers this one already has.
     *
     * @return array<string, int>|null table => seconds behind its tier's leader
     */
    public function collectAndReport(int $nowTs): ?array
    {
        if (! $this->enabled || $this->token === null || $this->token === '') {
            return null;
        }

        try {
            $pdo = $this->connect();
        } catch (\Throwable $e) {
            error_log('[NightOwl Agent] table-stats: connect failed (retried next interval): '.$e->getMessage());

            return null;
        }

        try {
            // One reporter per tenant per interval; the session lock releases
            // on disconnect, so a crashed run can never strand it.
            $locked = $pdo->query(
                "SELECT pg_try_advisory_lock(hashtext('".self::ADVISORY_KEY."'))"
            )->fetchColumn();
            if (! $locked) {
                return null;
            }

            $payload = $this->gather($pdo, $nowTs);
        } catch (\Throwable $e) {
            error_log('[NightOwl Agent] table-stats: collection failed (retried next interval): '.$e->getMessage());

            return null;
        } finally {
            // Close regardless — short-lived by contract.
            unset($pdo);
        }

        $this->post($payload);

        // After the post, not before: the platform copy of the sample is the
        // forensic record, and a staleness verdict is worth much less without
        // the numbers behind it. post() never throws.
        return RollupStaleness::detect($payload['tables']);
    }

    /**
     * Everything the sample carries, from catalog/stat views only.
     *
     * @return array{v: int, sampled_at: int, db_bytes: int, tables: list<array<string, mixed>>}
     */
    public function gather(\PDO $pdo, int $nowTs): array
    {
        $pdo->exec('SET statement_timeout = '.self::STATEMENT_TIMEOUT_MS);

        $dbBytes = (int) $pdo->query('SELECT pg_database_size(current_database())')->fetchColumn();

        // Stats + sizes for every nightowl relation, partition children
        // included — folded into logical parents below.
        $rows = $pdo->query(
            "SELECT s.relname,
                    s.n_live_tup, s.n_dead_tup,
                    s.seq_scan, s.seq_tup_read, s.idx_scan,
                    s.n_tup_ins, s.n_tup_upd, s.n_tup_del,
                    s.n_tup_hot_upd, s.n_mod_since_analyze,
                    s.vacuum_count, s.autovacuum_count, s.analyze_count, s.autoanalyze_count,
                    EXTRACT(EPOCH FROM s.last_autovacuum)::bigint AS vac_at,
                    EXTRACT(EPOCH FROM s.last_autoanalyze)::bigint AS ana_at,
                    io.heap_blks_read, io.heap_blks_hit, io.idx_blks_read, io.idx_blks_hit,
                    io.toast_blks_read, io.toast_blks_hit,
                    age(c.relfrozenxid) AS frozen_age,
                    array_to_string(c.reloptions, ',') AS relopts,
                    (c.relpersistence = 'u')::int AS unlogged,
                    pg_total_relation_size(c.oid) AS bytes
             FROM pg_stat_user_tables s
             JOIN pg_class c ON c.oid = s.relid
             LEFT JOIN pg_statio_user_tables io ON io.relid = s.relid
             WHERE s.schemaname = 'public' AND s.relname LIKE 'nightowl%'"
        )->fetchAll(\PDO::FETCH_ASSOC);

        $tables = [];
        foreach ($rows as $r) {
            $logical = preg_replace(self::CHILD_SUFFIX, '', $r['relname']);
            $t = &$tables[$logical];
            $t ??= [
                'name' => $logical,
                'bytes' => 0, 'live' => 0, 'dead' => 0,
                'seq_scan' => 0, 'seq_read' => 0, 'idx_scan' => 0,
                'ins' => 0, 'upd' => 0, 'del' => 0,
                'vac_at' => null, 'ana_at' => null,
                'children' => 0,
            ];
            $t['bytes'] += (int) $r['bytes'];
            $t['live'] += (int) $r['n_live_tup'];
            $t['dead'] += (int) $r['n_dead_tup'];
            $t['seq_scan'] += (int) $r['seq_scan'];
            $t['seq_read'] += (int) $r['seq_tup_read'];
            $t['idx_scan'] += (int) $r['idx_scan'];
            $t['ins'] += (int) $r['n_tup_ins'];
            $t['upd'] += (int) $r['n_tup_upd'];
            $t['del'] += (int) $r['n_tup_del'];
            $t['vac_at'] = max($t['vac_at'] ?? 0, (int) $r['vac_at']) ?: null;
            $t['ana_at'] = max($t['ana_at'] ?? 0, (int) $r['ana_at']) ?: null;
            // Extended diagnostics ride the extra bag — the platform stores it
            // as jsonb, so future additions here need no schema change anywhere.
            $x = &$t['extra'];
            $x ??= ['hot_upd' => 0, 'mod_since_analyze' => 0, 'vacuum_count' => 0,
                'autovacuum_count' => 0, 'analyze_count' => 0, 'autoanalyze_count' => 0,
                'heap_read' => 0, 'heap_hit' => 0, 'idx_read' => 0, 'idx_hit' => 0,
                'frozen_age' => 0];
            $x['hot_upd'] += (int) $r['n_tup_hot_upd'];
            $x['mod_since_analyze'] += (int) $r['n_mod_since_analyze'];
            $x['vacuum_count'] += (int) $r['vacuum_count'];
            $x['autovacuum_count'] += (int) $r['autovacuum_count'];
            $x['analyze_count'] += (int) $r['analyze_count'];
            $x['autoanalyze_count'] += (int) $r['autoanalyze_count'];
            $x['heap_read'] += (int) $r['heap_blks_read'];
            $x['heap_hit'] += (int) $r['heap_blks_hit'];
            $x['idx_read'] += (int) $r['idx_blks_read'];
            $x['idx_hit'] += (int) $r['idx_blks_hit'];
            $x['toast_read'] = ($x['toast_read'] ?? 0) + (int) $r['toast_blks_read'];
            $x['toast_hit'] = ($x['toast_hit'] ?? 0) + (int) $r['toast_blks_hit'];
            $x['frozen_age'] = max($x['frozen_age'], (int) $r['frozen_age']);
            // Customer-applied per-table tuning of OUR tables (autovacuum
            // overrides, fillfactor changes) — settings, not content.
            if (($r['relopts'] ?? '') !== '' && ! isset($x['reloptions'])) {
                $x['reloptions'] = substr((string) $r['relopts'], 0, 200);
            }
            if ((int) $r['unlogged'] === 1) {
                $x['unlogged'] = 1;
            }
            unset($x);
            if ($logical !== $r['relname']) {
                $t['children']++;
            }
            unset($t);
        }

        // Rollup bucket bounds — the coverage-hole detector. Enumerated from
        // the SAME catalog result (never a hand list), one guarded index-backed
        // probe per rollup table; a single failure skips that table only.
        foreach ($tables as $name => &$t) {
            if (! str_ends_with($name, '_rollups')) {
                continue;
            }
            try {
                $b = $pdo->query(
                    "SELECT EXTRACT(EPOCH FROM MIN(bucket_start))::bigint AS min_b,
                            EXTRACT(EPOCH FROM MAX(bucket_start))::bigint AS max_b
                     FROM {$name}"
                )->fetch(\PDO::FETCH_ASSOC);
                $t['min_bucket'] = $b['min_b'] !== null ? (int) $b['min_b'] : null;
                $t['max_bucket'] = $b['max_b'] !== null ? (int) $b['max_b'] : null;
            } catch (\Throwable) {
                // Timeout or odd schema — this table's bounds are absent, the
                // rest of the sample still ships.
            }
        }
        unset($t);

        // Per-index usage + size + validity — "is THIS index earning its
        // keep" is not answerable from the per-table aggregate. Children fold
        // into the logical parent index by the same suffix rule; INVALID
        // shells (killed CONCURRENTLY builds) surface as invalid > 0.
        try {
            $idxRows = $pdo->query(
                "SELECT i.relname AS table_name, i.indexrelname, i.idx_scan,
                        pg_relation_size(i.indexrelid) AS bytes,
                        (NOT x.indisvalid)::int AS invalid
                 FROM pg_stat_user_indexes i
                 JOIN pg_index x ON x.indexrelid = i.indexrelid
                 WHERE i.schemaname = 'public' AND i.relname LIKE 'nightowl%'"
            )->fetchAll(\PDO::FETCH_ASSOC);

            foreach ($idxRows as $r) {
                $logicalTable = preg_replace(self::CHILD_SUFFIX, '', $r['table_name']);
                $logicalIdx = preg_replace('/_p\d{8}|_pdefault|_phistoric|_pnew/', '', $r['indexrelname']);
                $key = 'index:'.$logicalTable.':'.substr($logicalIdx, 0, 80);
                $t = &$tables[$key];
                $t ??= ['name' => $key, 'bytes' => 0, 'idx_scan' => 0, 'children' => 0,
                    'extra' => ['invalid' => 0]];
                $t['bytes'] += (int) $r['bytes'];
                $t['idx_scan'] += (int) $r['idx_scan'];
                $t['extra']['invalid'] += (int) $r['invalid'];
                if ($logicalTable !== $r['table_name']) {
                    $t['children']++;
                }
                unset($t);
            }
        } catch (\Throwable) {
            // Index stats missing beats the whole sample missing.
        }

        ksort($tables);

        return [
            'v' => 2,
            'sampled_at' => $nowTs,
            'db_bytes' => $dbBytes,
            'tables' => array_values($tables),
            // Diagnostic sections — each independently guarded (a managed PG
            // may deny a view, an old version may lack it); an absent section
            // means "couldn't read", never "was fine".
            'sections' => array_filter([
                'cluster' => $this->clusterSection($pdo),
                'activity' => $this->activitySection($pdo),
                'replication' => $this->replicationSection($pdo),
                'settings' => $this->settingsSection($pdo),
                'connection' => $this->connectionSection($pdo),
                'extensions' => $this->extensionsSection($pdo),
            ], static fn ($v): bool => $v !== null),
        ];
    }

    /** One guarded scalar-map query; null on any failure. */
    private function section(\PDO $pdo, string $sql): ?array
    {
        try {
            $row = $pdo->query($sql)->fetch(\PDO::FETCH_ASSOC);

            return $row === false ? null : array_map(
                static fn ($v) => is_numeric($v) ? +$v : $v,
                $row,
            );
        } catch (\Throwable) {
            return null;
        }
    }

    /**
     * Database + cluster I/O pressure: cache hit vs read, temp spills
     * (work_mem exhaustion), deadlocks, checkpoint pressure, WAL volume,
     * wraparound age. pg_stat_bgwriter's checkpoint columns moved to
     * pg_stat_checkpointer in PG17 — take whichever answers.
     */
    private function clusterSection(\PDO $pdo): ?array
    {
        $db = $this->section($pdo, "SELECT xact_commit, xact_rollback, blks_read, blks_hit,
                tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted,
                conflicts, temp_files, temp_bytes, deadlocks,
                EXTRACT(EPOCH FROM stats_reset)::bigint AS stats_reset_at
             FROM pg_stat_database WHERE datname = current_database()");
        if ($db === null) {
            return null;
        }

        $checkpoints = $this->section($pdo, 'SELECT num_timed AS checkpoints_timed, num_requested AS checkpoints_req,
                write_time AS checkpoint_write_time, sync_time AS checkpoint_sync_time
             FROM pg_stat_checkpointer')
            ?? $this->section($pdo, 'SELECT checkpoints_timed, checkpoints_req,
                checkpoint_write_time, checkpoint_sync_time, buffers_backend_fsync
             FROM pg_stat_bgwriter');

        $wal = $this->section($pdo, 'SELECT wal_records, wal_fpi, wal_bytes, wal_buffers_full FROM pg_stat_wal');

        $age = $this->section($pdo, 'SELECT age(datfrozenxid) AS datfrozenxid_age
             FROM pg_database WHERE datname = current_database()');

        // Guarded separately: these columns are version- or setting-gated, and
        // folding them into the main query would null the WHOLE section on an
        // older server (track_io_timing off just reads as zeros).
        $ioTiming = $this->section($pdo, 'SELECT blk_read_time, blk_write_time
             FROM pg_stat_database WHERE datname = current_database()');
        $sessions = $this->section($pdo, 'SELECT sessions, sessions_abandoned, sessions_fatal, sessions_killed,
                session_time, active_time, idle_in_transaction_time
             FROM pg_stat_database WHERE datname = current_database()');
        $io = $this->section($pdo, "SELECT SUM(reads)::bigint AS io_reads, SUM(writes)::bigint AS io_writes,
                SUM(extends)::bigint AS io_extends, SUM(fsyncs)::bigint AS io_fsyncs,
                SUM(read_time)::float8 AS io_read_time, SUM(write_time)::float8 AS io_write_time
             FROM pg_stat_io");

        // WAL archiving health (self-managed backup pipelines); bgwriter's
        // cleaning-pressure counters (still in pg_stat_bgwriter on PG17); and
        // our own SQL functions' cost (nightowl_ddsketch_* — zeros when
        // track_functions is off, the view itself always exists).
        $archiver = $this->section($pdo, 'SELECT archived_count, failed_count AS archive_failed_count
             FROM pg_stat_archiver');
        $bgwriter = $this->section($pdo, 'SELECT buffers_clean, maxwritten_clean, buffers_alloc
             FROM pg_stat_bgwriter');
        $functions = $this->section($pdo, "SELECT COALESCE(SUM(calls), 0)::bigint AS fn_calls,
                COALESCE(SUM(total_time), 0)::float8 AS fn_total_time
             FROM pg_stat_user_functions WHERE funcname LIKE 'nightowl%'");

        return $db + ($checkpoints ?? []) + ($wal ?? []) + ($age ?? [])
            + ($ioTiming ?? []) + ($sessions ?? []) + ($io ?? [])
            + ($archiver ?? []) + ($bgwriter ?? []) + ($functions ?? []);
    }

    /**
     * Connection + lock pressure, WITHOUT query text — on BYOD the tenant DB
     * can be shared with the customer's own app, and pg_stat_activity.query is
     * content. Counts by state, wait-event TYPES, and worst-case ages only.
     * (pg_stat_statements is excluded outright for the same reason.)
     */
    private function activitySection(\PDO $pdo): ?array
    {
        $states = $this->section($pdo, "SELECT
                count(*) AS conns_total,
                count(*) FILTER (WHERE state = 'active') AS conns_active,
                count(*) FILTER (WHERE state = 'idle') AS conns_idle,
                count(*) FILTER (WHERE state LIKE 'idle in transaction%') AS conns_idle_in_txn,
                count(*) FILTER (WHERE wait_event_type = 'Lock') AS waiting_on_locks,
                count(*) FILTER (WHERE wait_event_type = 'IO') AS waiting_on_io,
                count(*) FILTER (WHERE wait_event_type = 'LWLock') AS waiting_on_lwlocks,
                COALESCE(EXTRACT(EPOCH FROM max(now() - xact_start))::bigint, 0) AS oldest_txn_s,
                COALESCE(EXTRACT(EPOCH FROM
                    max(now() - state_change) FILTER (WHERE state LIKE 'idle in transaction%')
                )::bigint, 0) AS oldest_idle_txn_s
             FROM pg_stat_activity WHERE datname = current_database()");
        if ($states === null) {
            return null;
        }

        $extras = $this->section($pdo, "SELECT
                (SELECT count(*) FROM pg_locks WHERE NOT granted) AS ungranted_locks,
                (SELECT count(*) FROM pg_prepared_xacts) AS prepared_xacts,
                (SELECT count(*) FROM pg_stat_progress_vacuum) AS vacuums_running,
                (SELECT count(*) FROM pg_locks WHERE NOT granted AND locktype = 'relation') AS ungranted_relation,
                (SELECT count(*) FROM pg_locks WHERE NOT granted AND locktype = 'tuple') AS ungranted_tuple,
                (SELECT count(*) FROM pg_locks WHERE NOT granted AND locktype = 'transactionid') AS ungranted_xid,
                (SELECT count(*) FROM pg_locks WHERE NOT granted AND locktype = 'advisory') AS ungranted_advisory,
                (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') AS max_connections");

        return $states + ($extras ?? []);
    }

    /**
     * Replication slots are a top disk-filler (an abandoned slot retains WAL
     * forever); replica lag contextualises a customer's slow reads. Fields a
     * non-superuser can't see come back null — shipped as-is.
     */
    private function replicationSection(\PDO $pdo): ?array
    {
        return $this->section($pdo, "SELECT
                (SELECT count(*) FROM pg_replication_slots) AS slots_total,
                (SELECT count(*) FROM pg_replication_slots WHERE NOT active) AS slots_inactive,
                (SELECT COALESCE(max(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)), 0)
                 FROM pg_replication_slots)::bigint AS slots_retained_wal_bytes,
                (SELECT count(*) FROM pg_stat_replication) AS replicas");
    }

    /**
     * The knobs that explain behavior differences across the fleet, plus the
     * server version. Values only — operational configuration, not content.
     */
    private function settingsSection(\PDO $pdo): ?array
    {
        try {
            $rows = $pdo->query("SELECT name, setting, source FROM pg_settings WHERE name IN (
                    'server_version_num', 'max_connections', 'shared_buffers', 'work_mem',
                    'maintenance_work_mem', 'effective_cache_size', 'max_wal_size',
                    'checkpoint_timeout', 'synchronous_commit', 'wal_level', 'random_page_cost',
                    'statement_timeout', 'idle_in_transaction_session_timeout', 'lock_timeout',
                    'autovacuum', 'autovacuum_vacuum_scale_factor', 'autovacuum_naptime',
                    'autovacuum_max_workers', 'autovacuum_freeze_max_age',
                    'track_io_timing', 'track_wal_io_timing', 'track_functions',
                    'shared_preload_libraries', 'max_worker_processes',
                    'max_parallel_workers', 'jit')"
            )->fetchAll(\PDO::FETCH_ASSOC);
        } catch (\Throwable) {
            return null;
        }

        $out = [];
        foreach ($rows as $r) {
            $out[$r['name']] = $r['setting'];
            // Changed-from-default matters as much as the value.
            if (! in_array($r['source'], ['default', 'override'], true)) {
                $out[$r['name'].'__set'] = 1;
            }
        }

        return $out === [] ? null : $out;
    }

    /**
     * What the AGENT can observe that SQL alone cannot — it sits on the
     * connection. Clock skew (signed; two past incident classes were UTC
     * misconfigs), best-of-3 ping RTT + connect time (decomposes network path
     * from server work — pg_latency_ms alone cannot), transaction-pooler
     * detection (pg_backend_pid stability, the RawPartitions probe technique),
     * standby detection (agent pointed at a read replica presents as
     * mysterious write failures), and whether OUR connection runs over SSL.
     */
    private function connectionSection(\PDO $pdo): ?array
    {
        try {
            $agentNow = microtime(true);
            $serverNow = (float) $pdo->query('SELECT EXTRACT(EPOCH FROM now())')->fetchColumn();
            $skew = round($serverNow - $agentNow, 3);

            $ping = null;
            for ($i = 0; $i < 3; $i++) {
                $t0 = microtime(true);
                $pdo->query('SELECT 1')->fetchColumn();
                $rtt = (microtime(true) - $t0) * 1000;
                $ping = $ping === null ? $rtt : min($ping, $rtt);
            }

            $pid1 = (int) $pdo->query('SELECT pg_backend_pid()')->fetchColumn();
            $pid2 = (int) $pdo->query('SELECT pg_backend_pid()')->fetchColumn();

            $extra = $this->section($pdo, 'SELECT pg_is_in_recovery()::int AS in_recovery,
                    (SELECT ssl::int FROM pg_stat_ssl WHERE pid = pg_backend_pid()) AS ssl');

            return [
                'clock_skew_s' => $skew,
                'ping_rtt_ms' => round($ping, 3),
                'connect_ms' => $this->lastConnectMs,
                'backend_pid_stable' => (int) ($pid1 === $pid2),
            ] + ($extra ?? []);
        } catch (\Throwable) {
            return null;
        }
    }

    /** Installed extensions, names + versions only — support gold on exotic
     *  BYOD targets (timescale, citus, missing pg_stat_statements, ...). */
    private function extensionsSection(\PDO $pdo): ?array
    {
        try {
            $rows = $pdo->query('SELECT extname, extversion FROM pg_extension ORDER BY extname LIMIT 64')
                ->fetchAll(\PDO::FETCH_KEY_PAIR);

            return $rows === [] ? null : $rows;
        } catch (\Throwable) {
            return null;
        }
    }

    /** Milliseconds the last connect() took (TCP+TLS+auth); null in harnesses
     *  that hand gather() an external PDO. */
    private ?float $lastConnectMs = null;

    private function connect(): \PDO
    {
        $dsn = sprintf(
            'pgsql:host=%s;port=%d;dbname=%s;sslmode=%s',
            $this->host, $this->port, $this->database, $this->sslmode,
        );

        $t0 = microtime(true);
        $pdo = new \PDO($dsn, $this->username, $this->password, [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::ATTR_TIMEOUT => self::CONNECT_TIMEOUT_S,
        ]);
        $this->lastConnectMs = round((microtime(true) - $t0) * 1000, 2);

        return $pdo;
    }

    /** Blocking POST with a hard timeout — same shape as AlertNotifier's. */
    private function post(array $payload): void
    {
        $body = json_encode($payload);
        if ($body === false) {
            return;
        }

        $url = rtrim($this->apiUrl, '/').'/agent/table-stats';
        $context = stream_context_create([
            'http' => [
                'method' => 'POST',
                'header' => "Content-Type: application/json\r\n"
                    .'Content-Length: '.strlen($body)."\r\n"
                    ."Authorization: Bearer {$this->token}\r\n",
                'content' => $body,
                'timeout' => self::POST_TIMEOUT_S,
                'ignore_errors' => true,
            ],
        ]);

        @file_get_contents($url, false, $context);
    }
}
