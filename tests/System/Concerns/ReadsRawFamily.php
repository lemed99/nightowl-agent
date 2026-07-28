<?php

namespace NightOwl\Tests\System\Concerns;

use NightOwl\Support\StorageV2;
use PDO;

/**
 * Family-agnostic PostgreSQL reads for the System suite.
 *
 * These tests boot the real agent as a subprocess, so the storage family is
 * whatever NIGHTOWL_STORAGE_V2 tells that subprocess to write — the v1 tables
 * or the `{table}_v2` twins. The assertions have to follow it. Reading
 * nightowl_requests while the daemon writes nightowl_requests_v2 does not
 * error; it returns zero rows, which every one of these tests reports as a
 * drain timeout. Silence like that is precisely why the suite is worth running
 * in both families rather than pinning one.
 *
 * Three things differ between the families, and all three live here:
 *
 *  - the physical table name — rawTable();
 *  - identity columns: trace_id/execution_id are native `uuid` in v2 and
 *    fingerprint is `bytea`, so predicates get built rather than concatenated
 *    — traceEq()/traceIn()/execEq()/fingerprintEq();
 *  - label columns (method, status, queue) are dictionary ids in v2, so
 *    fetch() joins them back to their v1 spelling.
 *
 * One consequence is worth stating outright: v2 stores trace_id as a real
 * uuid, so this suite's record ids must BE uuids. That is the shape a real
 * Nightwatch SDK emits — StorageV2::uuidOrNull() drops anything else to NULL —
 * so the old 'sys-req-<uniqid>' tags were exercising an input production never
 * sends. uuid()/uuids() replace them, and group membership that used to be a
 * `LIKE 'tag-%'` prefix scan is now an explicit id list: a stronger assertion,
 * since it names the rows it expects instead of matching a shape.
 */
trait ReadsRawFamily
{
    /**
     * Columns fetch() must decode back to their v1 spelling on the v2 arm.
     * Deliberately only the ones this suite asserts on — StorageV2::COMPAT is
     * the rollup projection and is kept narrow for its own reasons; widening
     * it to serve tests would put test needs into a production contract.
     */
    private const V2_DECODE = [
        'nightowl_requests' => ['method' => 'label:method_id'],
        'nightowl_exceptions' => ['fingerprint' => 'hex'],
        'nightowl_jobs' => ['status' => 'label:status_id', 'queue' => 'label:queue_id'],
    ];

    /** Logical raw tables every System class clears between tests. */
    private const RAW_TABLES = [
        'nightowl_requests', 'nightowl_queries', 'nightowl_exceptions',
        'nightowl_commands', 'nightowl_jobs', 'nightowl_cache_events',
        'nightowl_mail', 'nightowl_notifications', 'nightowl_outgoing_requests',
        'nightowl_scheduled_tasks', 'nightowl_logs',
    ];

    /** Non-raw tables that have no v2 twin and are cleared as-is. */
    private const FLAT_TABLES = [
        'nightowl_issue_activity', 'nightowl_issue_comments', 'nightowl_issues',
        'nightowl_users', 'nightowl_settings', 'nightowl_alert_channels',
    ];

    /** physical table => does it carry an execution_id column. Memoized. */
    private static array $execIdColumn = [];

    /** The class's live connection. Each System class already holds one. */
    abstract protected static function pdo(): PDO;

    // ─── Family resolution ────────────────────────────────────

    /**
     * Mirrors agent-harness-async.php's rule exactly: unset means v2 (the
     * production default), anything else goes through FILTER_VALIDATE_BOOLEAN.
     * If these two ever disagree the tests read one family while the daemon
     * writes the other, so the duplication is deliberate and must stay in step.
     */
    protected static function storageV2(): bool
    {
        $raw = getenv('NIGHTOWL_STORAGE_V2');

        return $raw === false ? true : filter_var($raw, FILTER_VALIDATE_BOOLEAN);
    }

    /** Physical table the ACTIVE family writes for a logical raw table. */
    protected static function rawTable(string $v1Table): string
    {
        return self::storageV2() && isset(StorageV2::TABLES[$v1Table])
            ? StorageV2::TABLES[$v1Table]
            : $v1Table;
    }

    // ─── Ids ──────────────────────────────────────────────────

    /** A v4 uuid — the only trace/execution id shape v2 keeps. */
    protected static function uuid(): string
    {
        return sprintf(
            '%04x%04x-%04x-4%03x-%04x-%04x%04x%04x',
            mt_rand(0, 0xFFFF), mt_rand(0, 0xFFFF),
            mt_rand(0, 0xFFFF),
            mt_rand(0, 0x0FFF),
            mt_rand(0, 0x3FFF) | 0x8000,
            mt_rand(0, 0xFFFF), mt_rand(0, 0xFFFF), mt_rand(0, 0xFFFF),
        );
    }

    /** @return list<string> */
    protected static function uuids(int $count): array
    {
        $out = [];
        for ($i = 0; $i < $count; $i++) {
            $out[] = self::uuid();
        }

        return $out;
    }

    // ─── Predicates ───────────────────────────────────────────

    /**
     * The expression that reconstructs a record's trace id.
     *
     * v2 stores NULL in trace_id when it equals execution_id
     * (StorageV2::traceIdFor) and readers COALESCE it back. On v1 both columns
     * are stored verbatim, so the same COALESCE is a no-op — which is what
     * lets one expression serve both families. Tables without an execution_id
     * column (requests, commands) get the bare column.
     */
    protected static function traceExpr(string $v1Table): string
    {
        return self::hasExecutionId($v1Table) ? 'COALESCE(trace_id, execution_id)' : 'trace_id';
    }

    protected static function traceEq(string $v1Table, string $uuid): string
    {
        return self::traceExpr($v1Table).' = '.self::uuidLiteral($uuid);
    }

    /**
     * @param  list<string>  $uuids
     */
    protected static function traceIn(string $v1Table, array $uuids): string
    {
        $list = implode(', ', array_map(self::uuidLiteral(...), $uuids));

        return self::traceExpr($v1Table).' IN ('.$list.')';
    }

    protected static function execEq(string $uuid): string
    {
        return 'execution_id = '.self::uuidLiteral($uuid);
    }

    /** 32-char hex digest: varchar on v1, bytea on v2. */
    protected static function fingerprintEq(string $hex): string
    {
        if (preg_match('/^[0-9a-f]{32}$/', $hex) !== 1) {
            throw new \InvalidArgumentException("Not a 32-char lowercase hex digest: {$hex}");
        }

        return self::storageV2()
            ? "fingerprint = decode('{$hex}', 'hex')"
            : "fingerprint = '{$hex}'";
    }

    // ─── Reads ────────────────────────────────────────────────

    protected static function rowCount(string $table, string $where = '1=1'): int
    {
        $physical = self::rawTable($table);

        return (int) self::pdo()->query("SELECT COUNT(*) FROM {$physical} WHERE {$where}")->fetchColumn();
    }

    protected static function distinctTraceCount(string $table, string $where = '1=1'): int
    {
        $physical = self::rawTable($table);
        $expr = self::traceExpr($table);

        return (int) self::pdo()->query("SELECT COUNT(DISTINCT {$expr}) FROM {$physical} WHERE {$where}")->fetchColumn();
    }

    /**
     * One row, with the v2 dictionary/bytea columns this suite asserts on
     * aliased back to their v1 names.
     *
     * The decoded aliases are appended AFTER `t.*` on purpose: on the v2 arm
     * `t.*` also yields the raw form (fingerprint as bytea), and PDO's
     * FETCH_ASSOC keeps the last column of a duplicated name — so the caller
     * always reads the decoded value under the v1 spelling.
     */
    protected static function fetch(string $table, string $where): ?array
    {
        $physical = self::rawTable($table);
        $decode = self::storageV2() ? (self::V2_DECODE[$table] ?? []) : [];

        $selects = ['t.*'];
        $joins = [];
        $n = 0;

        foreach ($decode as $column => $kind) {
            if ($kind === 'hex') {
                $selects[] = "encode(t.{$column}, 'hex') AS \"{$column}\"";

                continue;
            }

            $src = substr($kind, strlen('label:'));
            $alias = 'd'.$n++;
            $joins[] = "LEFT JOIN nightowl_dict_string {$alias} ON {$alias}.id = t.{$src}";
            $selects[] = "{$alias}.value AS \"{$column}\"";
        }

        $sql = 'SELECT '.implode(', ', $selects)." FROM {$physical} t "
            .implode(' ', $joins)
            ." WHERE {$where}";

        $row = self::pdo()->query($sql)->fetch(PDO::FETCH_ASSOC);

        return $row ?: null;
    }

    /**
     * Poll PostgreSQL until a condition is met or timeout expires.
     * The drain worker runs in a separate process with its own schedule,
     * so we must wait for it to flush SQLite → PG.
     */
    protected function waitForDrain(string $table, string $where, int $expectedCount, float $timeout = self::DRAIN_TIMEOUT): void
    {
        $deadline = microtime(true) + $timeout;
        $actual = 0;

        while (microtime(true) < $deadline) {
            $actual = self::rowCount($table, $where);
            if ($actual >= $expectedCount) {
                return;
            }
            usleep(200_000); // 200ms poll
        }

        $physical = self::rawTable($table);
        $this->fail(
            "Drain timeout after {$timeout}s: expected {$expectedCount} rows in {$physical} WHERE {$where}, got {$actual}."
        );
    }

    // ─── Cleanup ──────────────────────────────────────────────

    /**
     * Clear both families, not just the active one. The inactive family is
     * empty in a normal run, but truncating only the one we read would let a
     * mode switch (or a daemon that fell back) leave rows behind that the next
     * test counts as its own.
     *
     * The dictionaries are deliberately left alone: the running daemon caches
     * their ids, so emptying them mid-run would have it write raw rows
     * pointing at dictionary entries that no longer exist.
     */
    protected static function truncateAllTables(): void
    {
        foreach (self::FLAT_TABLES as $table) {
            self::pdo()->exec("TRUNCATE TABLE {$table} CASCADE");
        }

        foreach (self::RAW_TABLES as $table) {
            self::pdo()->exec("TRUNCATE TABLE {$table} CASCADE");
            self::pdo()->exec('TRUNCATE TABLE '.StorageV2::TABLES[$table].' CASCADE');
        }
    }

    // ─── Internals ────────────────────────────────────────────

    private static function uuidLiteral(string $uuid): string
    {
        if (preg_match('/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/', $uuid) !== 1) {
            throw new \InvalidArgumentException("Not a uuid: {$uuid}");
        }

        return "'{$uuid}'";
    }

    private static function hasExecutionId(string $v1Table): bool
    {
        $physical = self::rawTable($v1Table);

        if (! array_key_exists($physical, self::$execIdColumn)) {
            $stmt = self::pdo()->prepare(
                "SELECT 1 FROM information_schema.columns
                 WHERE table_name = ? AND column_name = 'execution_id'"
            );
            $stmt->execute([$physical]);
            self::$execIdColumn[$physical] = (bool) $stmt->fetchColumn();
        }

        return self::$execIdColumn[$physical];
    }
}
