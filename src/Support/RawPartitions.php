<?php

namespace NightOwl\Support;

use PDO;

/**
 * Native declarative partitioning for the RAW telemetry tables, by created_at,
 * daily children. Turns nightowl:prune's daily multi-million-row DELETE storm
 * (WAL + vacuum debt + bloat, forever) into instant DROP PARTITION, and gives
 * window scans partition pruning.
 *
 * Scope: all 11 raw tables. nightowl_logs needs a created_at varchar→timestamp
 * rewrite first — normalizeCreatedAtType() handles it inside convert().
 *
 * Children: {table}_pYYYYMMDD (one UTC day), {table}_phistoric (the original
 * table attached as-is under a validated CHECK — zero row copying), and
 * {table}_pdefault (catch-all for backdated drains, which event-time bucketing
 * permits up to ~366d).
 *
 * The PK becomes (id, created_at) — Postgres requires the partition key in
 * every unique constraint. `id` is synthetic everywhere (audited 2026-07-17:
 * the only reader is the deletion chunker, which needs the column, not
 * uniqueness-per-table), so widening it changes nothing observable.
 */
final class RawPartitions
{
    public const TABLES = [
        'nightowl_requests',
        'nightowl_queries',
        'nightowl_exceptions',
        'nightowl_commands',
        'nightowl_jobs',
        'nightowl_cache_events',
        'nightowl_mail',
        'nightowl_notifications',
        'nightowl_outgoing_requests',
        'nightowl_scheduled_tasks',
        // Logs joins via a type pre-step: its created_at is historically a
        // nullable varchar (the writer always stamped valid UTC strings), so
        // convert() rewrites the column to timestamp first. On a populated
        // table that rewrite is a full-table ACCESS EXCLUSIVE pass — the
        // reason nightowl:partition is operator-run.
        'nightowl_logs',
    ];

    /** How many future daily children to keep pre-created. */
    public const DAYS_AHEAD = 7;

    private static int $savepointSeq = 0;

    public static function isPartitioned(PDO $conn, string $table): bool
    {
        $stmt = $conn->prepare(
            "SELECT relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'public' AND c.relname = ?"
        );
        $stmt->execute([$table]);
        $relkind = $stmt->fetchColumn();

        return $relkind === 'p';
    }

    public static function childName(string $table, int $dayEpoch): string
    {
        return $table.'_p'.gmdate('Ymd', $dayEpoch);
    }

    /**
     * Create the daily child for $dayEpoch (UTC midnight) if absent. Idempotent.
     *
     * Rows for that day already sitting in the DEFAULT child — a drain that ran
     * while the day had no child of its own — make Postgres reject the CREATE
     * with 23514: the new partition's constraint would be violated by rows the
     * default holds. They are moved into it instead (adoptDefaultRows).
     */
    public static function ensureDailyChild(PDO $conn, string $table, int $dayEpoch): void
    {
        $day = intdiv($dayEpoch, 86400) * 86400;
        $child = self::childName($table, $day);
        $from = gmdate('Y-m-d 00:00:00', $day);
        $to = gmdate('Y-m-d 00:00:00', $day + 86400);

        try {
            self::isolated($conn, fn () => $conn->exec(
                "CREATE TABLE IF NOT EXISTS {$child} PARTITION OF {$table} FOR VALUES FROM ('{$from}') TO ('{$to}')"
            ));

            return;
        } catch (\PDOException $e) {
            if ($e->getCode() !== '23514') {
                throw $e;
            }
        }

        self::adoptDefaultRows($conn, $table, $child, $from, $to);
    }

    /**
     * Pre-create today's child plus DAYS_AHEAD future days for every
     * partitioned raw table. Cheap no-op when everything exists; safe to call
     * from the drain's maintenance tick under its advisory lock.
     *
     * Never throws — one table that cannot be fixed must not cost the other ten
     * their children, on this tick or on any tick after it. Callers sweep inside
     * a transaction they own, where an exception is one rollBack() away from
     * discarding every child the healthy tables just got, and a table that stays
     * broken repeats that forever: the DEFAULT child ends up swallowing every raw
     * row, and prune can only row-DELETE it, never DROP it. Failures are logged
     * and returned instead; the next tick retries them.
     *
     * @return list<string> one entry per day that could not be created
     */
    public static function ensureFutureChildren(PDO $conn, ?array $tables = null): array
    {
        $today = intdiv(time(), 86400) * 86400;
        $failures = [];

        foreach ($tables ?? self::TABLES as $table) {
            try {
                $failures = array_merge(
                    $failures,
                    self::isolated($conn, fn () => self::ensureChildWindow($conn, $table, $today)),
                );
            } catch (\Throwable $e) {
                $failures[] = $table.': '.$e->getMessage();
            }
        }

        self::logFailures($failures);

        return $failures;
    }

    /**
     * Convert one raw table to a partitioned parent.
     *
     * Empty table (fresh installs, run from the migration): drop + recreate as
     * partitioned — nothing to preserve.
     *
     * Populated table (operator-run via nightowl:partition): rename the
     * original to {t}_phistoric and ATTACH it under a validated CHECK — no row
     * copying; the brief exclusive locks are at rename/attach, and the agent's
     * SQLite buffer absorbs drain retries through them.
     */
    public static function convert(PDO $conn, string $table): void
    {
        if (self::isPartitioned($conn, $table)) {
            return;
        }

        self::normalizeCreatedAtType($conn, $table);

        $hasRows = (bool) $conn->query("SELECT EXISTS (SELECT 1 FROM {$table} LIMIT 1)")->fetchColumn();
        $historic = "{$table}_phistoric";
        $tmp = "{$table}_pnew";

        // Crash-hardening: a previous conversion killed mid-run (deploy
        // pipelines do this) leaves a disposable pre-swap {tmp} — the swap
        // itself is transactional, so {tmp} existing alongside {table} always
        // means "retry from scratch". A killed CONCURRENTLY build likewise
        // leaves an INVALID unique index that would satisfy IF NOT EXISTS but
        // not the ATTACH — drop it so the rebuild is clean.
        $conn->exec("DROP TABLE IF EXISTS {$tmp}");
        $invalid = $conn->query(
            "SELECT NOT x.indisvalid FROM pg_index x JOIN pg_class c ON c.oid = x.indexrelid
             WHERE c.relname = '{$table}_id_created_at_pt'"
        )->fetchColumn();
        if ($invalid === true || $invalid === 't') {
            $conn->exec("DROP INDEX IF EXISTS {$table}_id_created_at_pt");
        }

        // Boundary: the start of TOMORROW (UTC) — everything already in the
        // table sorts below it, and the first daily child starts there. The
        // historic partition covers (MINVALUE, boundary).
        $today = intdiv(time(), 86400) * 86400;
        $boundary = gmdate('Y-m-d 00:00:00', $today + 86400);

        // New parent: same columns/defaults, partitioned. Indexes are replayed
        // from the catalog below (renamed; definitions cascade to children).
        $conn->exec(
            "CREATE TABLE {$tmp} (LIKE {$table} INCLUDING DEFAULTS INCLUDING STORAGE) PARTITION BY RANGE (created_at)"
        );
        $conn->exec("ALTER TABLE {$tmp} ADD PRIMARY KEY (id, created_at)");

        foreach (self::indexDefs($conn, $table) as $def) {
            // Replay: point the definition at the parent under a fresh name
            // (sequential replaces — the two patterns share the " ON " token).
            $sql = str_replace("INDEX {$def->indexname} ON", "INDEX {$def->indexname}_pt ON", $def->indexdef);
            $sql = str_replace("ON public.{$table}", "ON public.{$tmp}", $sql);
            $conn->exec($sql);
        }

        if ($hasRows) {
            // The parent PK (id, created_at) needs a matching unique index on
            // the attached partition or ATTACH would build one under lock.
            $conn->exec(
                "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {$table}_id_created_at_pt ON {$table} (id, created_at)"
            );
            // Validated CHECK lets ATTACH skip its full-table scan. IS NOT NULL
            // is folded in because the parent's PK forces created_at NOT NULL
            // and the original column is nullable — the validated CHECK then
            // lets SET NOT NULL skip ITS scan too (PG 12+). A legacy row with
            // NULL created_at fails validation here with a clear error; such
            // rows are invisible to every time-filtered reader anyway and must
            // be fixed (or deleted) before partitioning.
            $conn->exec(
                "ALTER TABLE {$table} ADD CONSTRAINT {$table}_hist_ck CHECK (created_at IS NOT NULL AND created_at < '{$boundary}') NOT VALID"
            );
            $conn->exec("ALTER TABLE {$table} VALIDATE CONSTRAINT {$table}_hist_ck");
            $conn->exec("ALTER TABLE {$table} ALTER COLUMN created_at SET NOT NULL");

            // A partition cannot carry its own PRIMARY KEY next to the
            // parent's composite one (42P16). Demote the old (id) PK — the
            // (id, created_at) unique index built above carries uniqueness
            // through the attach, and id keeps its NOT NULL column attribute.
            $pkStmt = $conn->prepare(
                "SELECT conname FROM pg_constraint WHERE conrelid = ?::regclass AND contype = 'p'"
            );
            $pkStmt->execute([$table]);
            $pkName = $pkStmt->fetchColumn();
            if ($pkName !== false) {
                $conn->exec("ALTER TABLE {$table} DROP CONSTRAINT {$pkName}");
            }
        }

        // The id sequence is OWNED BY the original table's column; re-own it to
        // the parent so dropping the historic partition later can't cascade it.
        $seqStmt = $conn->prepare("SELECT pg_get_serial_sequence(?, 'id')");
        $seqStmt->execute([$table]);
        $seq = $seqStmt->fetchColumn() ?: null;

        $conn->beginTransaction();
        try {
            // Re-own the id sequence to the new parent FIRST: the clone's id
            // DEFAULT references it, and it is OWNED BY the old table — a DROP
            // before re-owning raises 2BP01 (dependent objects).
            if ($seq !== null) {
                $conn->exec("ALTER SEQUENCE {$seq} OWNED BY {$tmp}.id");
            }

            if ($hasRows) {
                $conn->exec("ALTER TABLE {$table} RENAME TO {$historic}");
            } else {
                $conn->exec("DROP TABLE {$table}");
            }
            $conn->exec("ALTER TABLE {$tmp} RENAME TO {$table}");

            if ($hasRows) {
                $conn->exec(
                    "ALTER TABLE {$table} ATTACH PARTITION {$historic} FOR VALUES FROM (MINVALUE) TO ('{$boundary}')"
                );
            }

            $conn->exec(
                "CREATE TABLE {$table}_pdefault PARTITION OF {$table} DEFAULT"
            );
            $conn->commit();
        } catch (\Throwable $e) {
            $conn->rollBack();
            throw $e;
        }

        self::logFailures(self::ensureChildWindow($conn, $table, $today));
    }

    /**
     * Droppable children strictly older than the cutoff: daily children whose
     * whole range is expired. The historic and default partitions are never
     * dropped here (they get the row-DELETE path).
     *
     * @return list<string>
     */
    public static function expiredChildren(PDO $conn, string $table, string $cutoff): array
    {
        $stmt = $conn->prepare(
            "SELECT c.relname
             FROM pg_inherits i
             JOIN pg_class c ON c.oid = i.inhrelid
             JOIN pg_class p ON p.oid = i.inhparent
             WHERE p.relname = ?"
        );
        $stmt->execute([$table]);
        $rows = $stmt->fetchAll(PDO::FETCH_OBJ);

        $expired = [];
        foreach ($rows as $row) {
            if (! preg_match('/^'.preg_quote($table, '/').'_p(\d{8})$/', $row->relname, $m)) {
                continue; // historic/default — row-DELETE territory
            }
            $upper = gmdate('Y-m-d 00:00:00', strtotime($m[1].' +1 day UTC'));
            if ($upper <= $cutoff) {
                $expired[] = $row->relname;
            }
        }

        return $expired;
    }

    /**
     * Drop the historic partition once prune has emptied it — the moment a
     * converted tenant's entire pre-conversion bloat returns to the OS in one
     * unlink. Emptiness is the trigger (not boundary math): its upper bound
     * was frozen at conversion, it receives no new rows, so zero rows after a
     * row-DELETE pass is unambiguous. The DEFAULT child is never dropped — a
     * concurrent backdated drain would fail with "no partition for row".
     */
    public static function dropEmptyHistoric(PDO $conn, string $table): bool
    {
        $historic = "{$table}_phistoric";

        $isChild = (bool) $conn->query(
            "SELECT EXISTS (
                SELECT 1 FROM pg_inherits i
                JOIN pg_class c ON c.oid = i.inhrelid
                JOIN pg_class p ON p.oid = i.inhparent
                WHERE p.relname = '{$table}' AND c.relname = '{$historic}'
            )"
        )->fetchColumn();
        if (! $isChild) {
            return false;
        }

        if ((bool) $conn->query("SELECT EXISTS (SELECT 1 FROM {$historic} LIMIT 1)")->fetchColumn()) {
            return false;
        }

        $conn->exec("DROP TABLE {$historic}");

        return true;
    }

    /**
     * Partitionable raw tables that are still plain (relkind 'r') AND hold
     * rows — the set the visible "run nightowl:partition" warnings report.
     *
     * @return list<string>
     */
    public static function unpartitionedPopulated(PDO $conn, ?array $tables = null): array
    {
        $out = [];
        foreach ($tables ?? self::TABLES as $table) {
            $stmt = $conn->prepare(
                "SELECT relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'public' AND c.relname = ?"
            );
            $stmt->execute([$table]);
            if ($stmt->fetchColumn() !== 'r') {
                continue; // partitioned already, or absent
            }
            if ((bool) $conn->query("SELECT EXISTS (SELECT 1 FROM {$table} LIMIT 1)")->fetchColumn()) {
                $out[] = $table;
            }
        }

        return $out;
    }

    /**
     * Rewrite a varchar created_at (nightowl_logs' historical accident) to a
     * proper timestamp so it can key the range partition. Full-table rewrite
     * under ACCESS EXCLUSIVE on populated tables — why nightowl:partition is
     * operator-run. Rows with NULL/empty created_at (already invisible to
     * every time-filtered reader) become epoch-dated and age out via prune.
     */
    private static function normalizeCreatedAtType(PDO $conn, string $table): void
    {
        $stmt = $conn->prepare(
            "SELECT data_type FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = ? AND column_name = 'created_at'"
        );
        $stmt->execute([$table]);
        $type = $stmt->fetchColumn();

        if ($type === false || $type === 'timestamp without time zone') {
            return;
        }

        $conn->exec(
            "ALTER TABLE {$table} ALTER COLUMN created_at TYPE timestamp
             USING COALESCE(NULLIF(created_at, '')::timestamp, '1970-01-01 00:00:00'::timestamp)"
        );
    }

    /** @return list<object{indexname: string, indexdef: string}> non-PK indexes on $table */
    private static function indexDefs(PDO $conn, string $table): array
    {
        $stmt = $conn->prepare(
            "SELECT i.indexname, i.indexdef
             FROM pg_indexes i
             WHERE i.schemaname = 'public' AND i.tablename = ?
               AND i.indexname NOT IN (
                   SELECT c.relname FROM pg_index x
                   JOIN pg_class c ON c.oid = x.indexrelid
                   JOIN pg_class t ON t.oid = x.indrelid
                   WHERE t.relname = ? AND x.indisprimary
               )"
        );
        $stmt->execute([$table, $table]);

        return $stmt->fetchAll(PDO::FETCH_OBJ);
    }

    /**
     * Today's child plus DAYS_AHEAD future days for one table, skipping days the
     * historic child still covers — a child inside its range is rejected as an
     * overlap (42P17), and IF NOT EXISTS only suppresses a name clash.
     *
     * Each day stands alone: one that cannot be created must not cost the days
     * after it their children.
     *
     * @return list<string> one entry per day that could not be created
     */
    private static function ensureChildWindow(PDO $conn, string $table, int $today): array
    {
        if (! self::isPartitioned($conn, $table)) {
            return [];
        }

        $historic = self::historicConstraint($conn, $table);
        $failures = [];

        for ($d = 0; $d <= self::DAYS_AHEAD; $d++) {
            $day = $today + $d * 86400;

            try {
                self::isolated($conn, function () use ($conn, $table, $historic, $day) {
                    if ($historic !== null && self::historicCovers($conn, $historic, $day)) {
                        return;
                    }

                    self::ensureDailyChild($conn, $table, $day);
                });
            } catch (\Throwable $e) {
                $failures[] = self::childName($table, $day).': '.$e->getMessage();
            }
        }

        return $failures;
    }

    /**
     * The partition constraint of {table}_phistoric — the predicate a row must
     * satisfy to belong to it — or null when there is no historic child (an
     * empty table converts by rebuild, so fresh installs never get one and
     * today's child is theirs to create).
     */
    private static function historicConstraint(PDO $conn, string $table): ?string
    {
        $stmt = $conn->prepare(
            "SELECT pg_get_partition_constraintdef(c.oid)
             FROM pg_inherits i
             JOIN pg_class c ON c.oid = i.inhrelid
             JOIN pg_class p ON p.oid = i.inhparent
             WHERE p.relname = ? AND c.relname = ?"
        );
        $stmt->execute([$table, "{$table}_phistoric"]);
        $def = $stmt->fetchColumn();

        return is_string($def) && $def !== '' ? $def : null;
    }

    /**
     * Whether the historic child still owns $day. Postgres evaluates its own
     * partition constraint against the candidate timestamp, so nothing here
     * depends on how the catalog renders a bound. That bound is frozen at
     * conversion — this stops excluding anything once the day passes, and the
     * sweep runs on every tick forever, not only on conversion day.
     */
    private static function historicCovers(PDO $conn, string $constraint, int $day): bool
    {
        $stmt = $conn->prepare("SELECT ({$constraint}) FROM (SELECT ?::timestamp AS created_at) probe");
        $stmt->execute([gmdate('Y-m-d 00:00:00', $day)]);
        $covered = $stmt->fetchColumn();

        return $covered === true || $covered === 't';
    }

    /**
     * Move the DEFAULT child's rows for this day into a standalone table, then
     * ATTACH that as the day's partition.
     *
     * The rows travel while the parent is held at ACCESS SHARE only. A
     * CREATE ... PARTITION OF takes ACCESS EXCLUSIVE on the parent and keeps it
     * until commit, which would block every concurrent drain COPY for as long as
     * the move runs — and the tick's transaction can outlive the move by ten more
     * tables. ATTACH takes SHARE UPDATE EXCLUSIVE, which the drain's ROW
     * EXCLUSIVE does not conflict with.
     *
     * The CHECK is what lets ATTACH skip re-scanning the rows just moved; the
     * partition bound subsumes it afterwards. CONSTRAINTS must come across in
     * the LIKE — ATTACH refuses a child missing any of the parent's CHECKs.
     */
    private static function adoptDefaultRows(PDO $conn, string $table, string $child, string $from, string $to): void
    {
        // Emptying the default and attaching the rows' new home must be atomic
        // or a crash strands them in a table nothing reads. Inside a caller's
        // transaction that is already guaranteed (and beginTransaction would
        // throw — PDO has no nesting); the enclosing savepoint unwinds it.
        $ownTransaction = ! $conn->inTransaction();

        if ($ownTransaction) {
            $conn->beginTransaction();
        }

        try {
            $conn->exec(
                "CREATE TABLE {$child} (LIKE {$table} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE)"
            );
            $conn->exec(
                "ALTER TABLE {$child} ADD CONSTRAINT {$child}_adopt_ck
                 CHECK (created_at >= '{$from}' AND created_at < '{$to}')"
            );
            $conn->exec(
                "WITH moved AS (
                     DELETE FROM {$table}_pdefault
                     WHERE created_at >= '{$from}' AND created_at < '{$to}'
                     RETURNING *
                 )
                 INSERT INTO {$child} SELECT * FROM moved"
            );
            $conn->exec(
                "ALTER TABLE {$table} ATTACH PARTITION {$child} FOR VALUES FROM ('{$from}') TO ('{$to}')"
            );
            $conn->exec("ALTER TABLE {$child} DROP CONSTRAINT {$child}_adopt_ck");

            if ($ownTransaction) {
                $conn->commit();
            }
        } catch (\Throwable $e) {
            if ($ownTransaction) {
                $conn->rollBack();
            }
            throw $e;
        }
    }

    /**
     * Run $work so that its failure poisons neither the work already done nor
     * the work still to come. Autocommit gives that per statement; inside a
     * caller's transaction — the drain's maintenance tick holds a
     * transaction-scoped advisory lock and runs the whole sweep in one — the
     * first error aborts the block and every later statement dies 25P02, so
     * only a SAVEPOINT can. Names are sequenced because these nest.
     */
    private static function isolated(PDO $conn, callable $work): mixed
    {
        if (! $conn->inTransaction()) {
            return $work();
        }

        $savepoint = 'nightowl_rp_'.(++self::$savepointSeq);
        $conn->exec("SAVEPOINT {$savepoint}");

        try {
            $result = $work();
        } catch (\Throwable $e) {
            // Unwinding throws too on a dead connection, and the cause being
            // unwound is the one worth reporting.
            try {
                $conn->exec("ROLLBACK TO SAVEPOINT {$savepoint}");
                $conn->exec("RELEASE SAVEPOINT {$savepoint}");
            } catch (\Throwable) {
            }

            throw $e;
        }

        $conn->exec("RELEASE SAVEPOINT {$savepoint}");

        return $result;
    }

    /**
     * @param  list<string>  $failures
     */
    private static function logFailures(array $failures): void
    {
        if ($failures === []) {
            return;
        }

        error_log(sprintf(
            '[NightOwl Support] %d daily partition(s) not created (retried next tick) — %s',
            count($failures),
            implode('; ', array_slice($failures, 0, 3)).(count($failures) > 3 ? ' (…)' : ''),
        ));
    }
}
