<?php

namespace NightOwl\Support;

use Illuminate\Database\ConnectionInterface;

/**
 * Rebuilds one cache rollup table with its keys collapsed onto CacheKeyTemplate
 * patterns, by building a correct table alongside the broken one and swapping
 * names — the alternative to rewriting every row in place.
 *
 * Why a second table at all. The in-place merge (RepairCacheRollupKeysCommand's
 * --in-place path) rewrites tens of millions of rows through the drain's own
 * advisory lock, leaves the freed space as dead tuples the table keeps until a
 * pg_repack, and skips whatever bucket the drain may still be writing — which on
 * the daily tier means the operator has to come back the next day. A rebuild
 * touches the source table only to READ it: the new table is built unlocked, the
 * indexes are built on it before it is visible to anything, and the only blocking
 * moment is the rename. There is no dead space because nothing was deleted, and
 * no skipped tail because the writes that land mid-rebuild are captured rather
 * than avoided.
 *
 * How the mid-rebuild writes are captured. This is the part that makes it safe
 * against a LIVE drain, and it is the standard pg_repack shape:
 *
 *  1. Under the drain's advisory lock (so no drain write is in flight), create
 *     the delta table and an AFTER INSERT/UPDATE/DELETE trigger on the source,
 *     then `pg_export_snapshot()` — all inside one still-open transaction.
 *  2. Open the BUILD transaction on a SECOND connection at REPEATABLE READ and
 *     `SET TRANSACTION SNAPSHOT` to that id, so it reads the table exactly as it
 *     was at the instant the trigger was attached.
 *  3. Commit the first transaction. The drain unblocks; from here every write it
 *     makes is both invisible to the build (older snapshot) and recorded in the
 *     delta table. No write can fall in the gap, and none can be counted twice —
 *     which is the whole reason for the snapshot export. Without it, a row the
 *     drain updates between the trigger going live and the build's scan reaching
 *     that row would land in BOTH the build and the delta.
 *  4. Build unlocked, then take the advisory lock once more, fold the delta in,
 *     and rename.
 *
 * Deletes are captured as NEGATIVE deltas rather than tombstones, which keeps the
 * whole fold additive: a tombstone would have to delete a whole templated group,
 * and a group is the sum of many raw keys, so a partial delete (retention pruning
 * part of a bucket) would take the surviving keys' counts with it. Subtracting
 * cannot over-remove. The one thing subtraction cannot repair is min/max — a
 * deleted row's contribution to a group's extremes stays until the group empties
 * — which is the same approximation the drain's own additive upsert already makes.
 *
 * Cost to be aware of: the build transaction holds a snapshot for its whole run,
 * so vacuum cannot reclaim rows deleted anywhere in the database while it lasts.
 * That is minutes for a maintenance pass, not the hours the in-place merge takes,
 * but it is why this is a command and not something the agent does on its own.
 */
final class CacheRollupSwap
{
    /** Scratch objects, suffixed off the table name so an aborted run is identifiable and re-droppable. */
    private const SUFFIXES = ['__nw_new', '__nw_delta', '__nw_map'];

    /** @var list<string> additive counter columns (call_count + spec counters + duration totals) */
    private array $sums = [];

    /** @var list<string> LEAST/GREATEST columns */
    private array $minmax = [];

    /** @var list<string> full column list in insert order */
    private array $columns = [];

    /**
     * Test seam. Called with a phase name at the two moments a concurrent write
     * has to be proven captured: "captured" (trigger live, snapshot taken, build
     * not started) and "built" (new table complete, swap not started). Production
     * never sets it.
     *
     * @var null|\Closure(string): void
     */
    private ?\Closure $phaseHook = null;

    public function __construct(
        /** DDL, locking and the swap. Autocommit except where noted. */
        private ConnectionInterface $control,
        /** The snapshot build. A separate session because a transaction can only import a snapshot another OPEN transaction exported. */
        private ConnectionInterface $work,
        private int $lockTimeoutMs,
        /** @var \Closure(string): void */
        private \Closure $log,
    ) {}

    /** @param \Closure(string): void $hook */
    public function onPhase(\Closure $hook): void
    {
        $this->phaseHook = $hook;
    }

    /**
     * @return array{rows_before: int, rows_after: int, keys_mapped: int, delta_rows: int, swap_seconds: float}
     */
    public function rebuild(string $table, int $chunkDays = 7): array
    {
        $new = $table.'__nw_new';
        $delta = $table.'__nw_delta';
        $map = $table.'__nw_map';
        $trigger = $table.'__nw_trg';
        $fn = $table.'__nw_cap';

        $this->resolveColumns($table);
        $schema = $this->schemaOf($table);
        $pk = $this->primaryKeyColumns($table);
        $indexes = $this->secondaryIndexes($table);
        $grants = $this->grants($schema, $table);
        $this->assertOwned($schema, $table);

        // A previous run that died leaves scratch behind; the trigger among it
        // would double-count into a stale delta table. Clear it first.
        $this->dropScratch($table, $trigger, $fn);

        $rowsBefore = (int) $this->control->selectOne("SELECT count(*) AS n FROM {$table}")->n;

        try {
            $snapshot = $this->attachCapture($table, $delta, $trigger, $fn);
            $this->beginSnapshotTransaction($snapshot);
            $this->control->commit();
            ($this->phaseHook ?? static fn () => null)('captured');

            $mapped = $this->buildKeyMap($table, $map);
            ($this->log)("    key map: {$mapped} of the table's distinct keys collapse onto a pattern.");

            $this->buildNewTable($table, $new, $map, $pk, $chunkDays);
            $this->work->commit();

            $this->buildIndexes($new, $indexes);
            $this->applyGrants($new, $grants);
            ($this->phaseHook ?? static fn () => null)('built');

            $swapStart = microtime(true);
            $deltaRows = $this->swap($table, $new, $delta, $map, $pk, $trigger, $fn, $indexes);
            $swapSeconds = microtime(true) - $swapStart;
        } catch (\Throwable $e) {
            $this->abort($table, $trigger, $fn);

            throw $e;
        }

        return [
            'rows_before' => $rowsBefore,
            'rows_after' => (int) $this->control->selectOne("SELECT count(*) AS n FROM {$table}")->n,
            'keys_mapped' => $mapped,
            'delta_rows' => $deltaRows,
            'swap_seconds' => $swapSeconds,
        ];
    }

    /**
     * Column roles come from the rollup spec, not a hardcoded list, so a future
     * counter column is carried by both halves of the rebuild automatically.
     * duration_count is probed rather than assumed — it is optional on the table
     * (migration 000061), exactly as the drain and the backfill treat it.
     */
    private function resolveColumns(string $table): void
    {
        $spec = RollupSpecs::cacheEvents();

        $sums = array_merge(['call_count'], $spec->counterColumns());
        if ($spec->hasDuration) {
            $sums[] = 'total_duration';
        }
        if ($spec->hasDurationCount && $this->hasColumn($table, 'duration_count')) {
            $sums[] = 'duration_count';
        }

        $this->sums = $sums;
        $this->minmax = $spec->hasDuration ? ['min_duration', 'max_duration'] : [];
        $this->columns = array_merge(
            array_keys($spec->groupColumns),
            ['bucket_start', 'environment'],
            $this->sums,
            $this->minmax,
        );

        // A rebuild writes an explicit column list, so a column this class does
        // not know about would come out of the swap holding its DEFAULT — real
        // data replaced by zeroes, silently, on the tenant's only copy. Refuse
        // instead: a migration that adds a counter has to teach this class how to
        // aggregate it, and the failure says so at the start of the pass rather
        // than after the rename.
        $actual = array_map(
            static fn ($r): string => (string) $r->column_name,
            $this->control->select(
                'SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position',
                [$table],
            ),
        );
        $unknown = array_diff($actual, $this->columns);
        if ($unknown !== []) {
            throw new \RuntimeException(
                "{$table} has column(s) this rebuild does not know how to aggregate: "
                .implode(', ', $unknown)
                .'. Teach NightOwl\\Support\\CacheRollupSwap (and the rollup spec) about them, or run the repair with --in-place.'
            );
        }

        $missing = array_diff($this->columns, $actual);
        if ($missing !== []) {
            throw new \RuntimeException("{$table} is missing expected column(s): ".implode(', ', $missing).'.');
        }
    }

    private function hasColumn(string $table, string $column): bool
    {
        return (bool) $this->control->selectOne(
            'SELECT count(*) > 0 AS e FROM information_schema.columns WHERE table_name = ? AND column_name = ?',
            [$table, $column],
        )->e;
    }

    private function schemaOf(string $table): string
    {
        return (string) $this->control->selectOne(
            'SELECT n.nspname AS s FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = ?::regclass',
            [$table],
        )->s;
    }

    /** @return list<string> */
    private function primaryKeyColumns(string $table): array
    {
        $rows = $this->control->select(
            'SELECT a.attname AS c
             FROM pg_index i
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
             WHERE i.indrelid = ?::regclass AND i.indisprimary
             ORDER BY array_position(i.indkey, a.attnum)',
            [$table],
        );

        return array_map(static fn ($r): string => (string) $r->c, $rows);
    }

    /**
     * Every non-primary-key index, kept verbatim rather than assumed: the two
     * tier tables do not even agree on their bucket index's NAME
     * (…_bucket_idx vs …_bucket_start_idx), and a customer or a later migration
     * may have added others. Rebuilding a table that silently loses an index is
     * a slow-query incident a week later.
     *
     * @return list<array{name: string, unique: bool, using: string}>
     */
    private function secondaryIndexes(string $table): array
    {
        $rows = $this->control->select(
            'SELECT c.relname AS name, i.indisunique AS uniq, pg_get_indexdef(i.indexrelid) AS def
             FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
             WHERE i.indrelid = ?::regclass AND NOT i.indisprimary',
            [$table],
        );

        $out = [];
        foreach ($rows as $row) {
            // Reuse only the part after USING — the definition's own name and
            // table reference are schema-qualified for the OLD table.
            if (! preg_match('/ USING (.+)$/s', (string) $row->def, $m)) {
                continue;
            }
            $out[] = ['name' => (string) $row->name, 'unique' => (bool) $row->uniq, 'using' => $m[1]];
        }

        return $out;
    }

    /**
     * The swap ends in DROP TABLE, which only the owner may do. Postgres would
     * otherwise report that at the very end — after a rebuild that can take
     * minutes on a 45M-row table — so ask up front.
     */
    private function assertOwned(string $schema, string $table): void
    {
        $owner = $this->control->selectOne(
            'SELECT tableowner FROM pg_tables WHERE schemaname = ? AND tablename = ?',
            [$schema, $table],
        )?->tableowner;

        $isOwner = (bool) $this->control->selectOne(
            'SELECT pg_has_role(current_user, ?, \'USAGE\') AS ok',
            [$owner],
        )->ok;

        if (! $isOwner) {
            throw new \RuntimeException(
                "{$table} is owned by [{$owner}] and this connection is not a member of that role, so the "
                .'rebuild could not drop the old table at the end. Grant the role, or run the repair with --in-place.'
            );
        }
    }

    /**
     * A tenant whose dashboard reads through a role separate from the agent's
     * would lose its SELECT when the table is replaced by one this role owns.
     *
     * @return list<array{grantee: string, privilege: string, grantable: bool}>
     */
    private function grants(string $schema, string $table): array
    {
        $rows = $this->control->select(
            "SELECT grantee, privilege_type AS priv, is_grantable AS g
             FROM information_schema.role_table_grants
             WHERE table_schema = ? AND table_name = ? AND grantee <> current_user",
            [$schema, $table],
        );

        return array_map(static fn ($r): array => [
            'grantee' => (string) $r->grantee,
            'privilege' => (string) $r->priv,
            'grantable' => ((string) $r->g) === 'YES',
        ], $rows);
    }

    /**
     * Creates the delta table + capture trigger and exports the snapshot, leaving
     * the transaction OPEN — the snapshot is importable only while its exporter
     * lives, and the trigger must not become visible to the drain until the build
     * transaction is anchored to the pre-trigger snapshot.
     */
    private function attachCapture(string $table, string $delta, string $trigger, string $fn): string
    {
        $this->control->beginTransaction();
        $this->control->statement("SET LOCAL lock_timeout = '{$this->lockTimeoutMs}ms'");
        $this->control->statement('SELECT pg_advisory_xact_lock(hashtext(?))', ['nightowl_rollup:'.$table]);

        $this->control->statement("CREATE UNLOGGED TABLE {$delta} (op \"char\" NOT NULL, LIKE {$table})");

        $cols = implode(', ', $this->columns);
        $newVals = implode(', ', array_map(static fn ($c): string => "NEW.{$c}", $this->columns));
        $oldKeys = implode(', ', array_map(
            static fn ($c): string => "OLD.{$c}",
            array_slice($this->columns, 0, count($this->columns) - count($this->sums) - count($this->minmax)),
        ));
        $negSums = implode(', ', array_map(static fn ($c): string => "-OLD.{$c}", $this->sums));
        $nullMinMax = implode(', ', array_fill(0, count($this->minmax), 'NULL'));
        $deltaSums = implode(', ', array_map(static fn ($c): string => "NEW.{$c} - OLD.{$c}", $this->sums));
        $newMinMax = implode(', ', array_map(static fn ($c): string => "NEW.{$c}", $this->minmax));

        $this->control->statement("
            CREATE FUNCTION {$fn}() RETURNS trigger LANGUAGE plpgsql AS \$fn\$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    INSERT INTO {$delta} (op, {$cols}) VALUES ('-', {$oldKeys}, {$negSums}".
            ($nullMinMax === '' ? '' : ", {$nullMinMax}").");
                    RETURN OLD;
                ELSIF TG_OP = 'UPDATE' THEN
                    INSERT INTO {$delta} (op, {$cols}) VALUES ('~', {$oldKeys}, {$deltaSums}".
            ($newMinMax === '' ? '' : ", {$newMinMax}").");
                    RETURN NEW;
                ELSE
                    INSERT INTO {$delta} (op, {$cols}) VALUES ('+', {$newVals});
                    RETURN NEW;
                END IF;
            END
            \$fn\$");

        $this->control->statement(
            "CREATE TRIGGER {$trigger} AFTER INSERT OR UPDATE OR DELETE ON {$table}
             FOR EACH ROW EXECUTE FUNCTION {$fn}()"
        );

        return (string) $this->control->selectOne('SELECT pg_export_snapshot() AS s')->s;
    }

    private function beginSnapshotTransaction(string $snapshot): void
    {
        $this->work->beginTransaction();
        $this->work->statement('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');
        $this->work->statement("SET TRANSACTION SNAPSHOT '{$snapshot}'");
    }

    /**
     * Templating is PHP-only by design (CacheKeyTemplate is the single
     * implementation; the SQL side never re-derives a pattern), so the raw→pattern
     * mapping has to make one trip through PHP. Only keys that actually CHANGE are
     * stored — on a healthy table that is nothing, and the join below coalesces.
     *
     * Paginated on the primary key's leading column so memory is bounded by the
     * batch, not by the table: a 45M-row table's distinct keys will not fit in a
     * buffered result set.
     */
    private function buildKeyMap(string $table, string $map): int
    {
        $this->work->statement("CREATE UNLOGGED TABLE {$map} (raw text PRIMARY KEY, pattern text NOT NULL)");

        $last = null;
        $mapped = 0;
        $buffer = [];

        while (true) {
            $rows = $last === null
                ? $this->work->select("SELECT DISTINCT key FROM {$table} ORDER BY key LIMIT 5000")
                : $this->work->select("SELECT DISTINCT key FROM {$table} WHERE key > ? ORDER BY key LIMIT 5000", [$last]);

            if ($rows === []) {
                break;
            }

            foreach ($rows as $row) {
                $key = (string) $row->key;
                $last = $key;
                $pattern = CacheKeyTemplate::template($key);
                if ($pattern !== $key) {
                    $buffer[] = [$key, $pattern];
                    $mapped++;
                }
            }

            if (count($buffer) >= 5000) {
                $this->flushMap($map, $buffer);
                $buffer = [];
            }
        }

        $this->flushMap($map, $buffer);
        $this->work->statement("ANALYZE {$map}");

        return $mapped;
    }

    /** @param list<array{0: string, 1: string}> $rows */
    private function flushMap(string $map, array $rows): void
    {
        if ($rows === []) {
            return;
        }

        foreach (array_chunk($rows, 1000) as $chunk) {
            $values = implode(', ', array_fill(0, count($chunk), '(?, ?)'));
            $bind = [];
            foreach ($chunk as $pair) {
                $bind[] = $pair[0];
                $bind[] = $pair[1];
            }
            // ON CONFLICT because DISTINCT is per batch, and a key could in
            // principle be seen twice across a batch boundary.
            $this->work->statement("INSERT INTO {$map} (raw, pattern) VALUES {$values} ON CONFLICT (raw) DO NOTHING", $bind);
        }
    }

    /**
     * Chunked by bucket range so the aggregate's working set stays bounded — a
     * single GROUP BY over 45M rows is one enormous sort or hash. Every chunk
     * merges additively, so the chunk size changes nothing about the result.
     */
    private function buildNewTable(string $table, string $new, string $map, array $pk, int $chunkDays): void
    {
        $this->work->statement("CREATE TABLE {$new} (LIKE {$table} INCLUDING DEFAULTS INCLUDING COMMENTS INCLUDING STORAGE)");
        $this->work->statement("ALTER TABLE {$new} ADD PRIMARY KEY (".implode(', ', $pk).')');

        $bounds = $this->work->selectOne("SELECT min(bucket_start) AS lo, max(bucket_start) AS hi FROM {$table}");
        if ($bounds->lo === null) {
            return;
        }

        $columns = implode(', ', $this->columns);
        $sums = implode(', ', array_map(static fn ($c): string => "SUM(t.{$c})", $this->sums));
        $extremes = implode(', ', array_map(
            static fn ($c): string => str_starts_with($c, 'min') ? "MIN(t.{$c})" : "MAX(t.{$c})",
            $this->minmax,
        ));
        $select = "COALESCE(m.pattern, t.key), t.store, t.bucket_start, t.environment, {$sums}"
            .($extremes === '' ? '' : ", {$extremes}");

        $cursor = new \DateTimeImmutable((string) $bounds->lo, new \DateTimeZone('UTC'));
        $end = (new \DateTimeImmutable((string) $bounds->hi, new \DateTimeZone('UTC')))->modify('+1 second');
        $updates = $this->conflictUpdates($new);

        while ($cursor < $end) {
            $next = $cursor->modify("+{$chunkDays} days");
            $this->work->statement(
                "INSERT INTO {$new} ({$columns})
                 SELECT {$select}
                 FROM {$table} t LEFT JOIN {$map} m ON m.raw = t.key
                 WHERE t.bucket_start >= ? AND t.bucket_start < ?
                 GROUP BY 1, 2, 3, 4
                 ON CONFLICT (".implode(', ', $pk).") DO UPDATE SET {$updates}",
                [$cursor->format('Y-m-d H:i:s'), $next->format('Y-m-d H:i:s')],
            );
            $cursor = $next;
        }
    }

    private function conflictUpdates(string $target): string
    {
        $updates = array_map(static fn ($c): string => "{$c} = {$target}.{$c} + EXCLUDED.{$c}", $this->sums);
        foreach ($this->minmax as $c) {
            $fn = str_starts_with($c, 'min') ? 'LEAST' : 'GREATEST';
            $updates[] = "{$c} = {$fn}({$target}.{$c}, EXCLUDED.{$c})";
        }

        return implode(', ', $updates);
    }

    /** @param list<array{name: string, unique: bool, using: string}> $indexes */
    private function buildIndexes(string $new, array $indexes): void
    {
        foreach ($indexes as $i => $index) {
            $unique = $index['unique'] ? 'UNIQUE ' : '';
            $this->control->statement("CREATE {$unique}INDEX {$new}_i{$i} ON {$new} USING {$index['using']}");
        }
    }

    /** @param list<array{grantee: string, privilege: string, grantable: bool}> $grants */
    private function applyGrants(string $new, array $grants): void
    {
        foreach ($grants as $grant) {
            $grantee = '"'.str_replace('"', '""', $grant['grantee']).'"';
            $option = $grant['grantable'] ? ' WITH GRANT OPTION' : '';
            $this->control->statement("GRANT {$grant['privilege']} ON {$new} TO {$grantee}{$option}");
        }
    }

    /**
     * The only blocking step. Under the drain's advisory lock: fold in everything
     * the trigger captured, then replace the table. The delta is bounded by the
     * writes that happened during the build, so this is milliseconds even when
     * the build took minutes.
     *
     * @param  list<array{name: string, unique: bool, using: string}>  $indexes
     */
    private function swap(string $table, string $new, string $delta, string $map, array $pk, string $trigger, string $fn, array $indexes): int
    {
        $this->control->beginTransaction();
        $this->control->statement("SET LOCAL lock_timeout = '{$this->lockTimeoutMs}ms'");
        $this->control->statement('SELECT pg_advisory_xact_lock(hashtext(?))', ['nightowl_rollup:'.$table]);

        // Keys the drain wrote during the build may not be in the map. Bounded by
        // the delta, and inside the lock so nothing can arrive after the top-up.
        $unmapped = $this->control->select(
            "SELECT DISTINCT d.key FROM {$delta} d LEFT JOIN {$map} m ON m.raw = d.key WHERE m.raw IS NULL"
        );
        $pairs = [];
        foreach ($unmapped as $row) {
            $key = (string) $row->key;
            $pattern = CacheKeyTemplate::template($key);
            if ($pattern !== $key) {
                $pairs[] = [$key, $pattern];
            }
        }
        foreach (array_chunk($pairs, 1000) as $chunk) {
            $values = implode(', ', array_fill(0, count($chunk), '(?, ?)'));
            $bind = [];
            foreach ($chunk as $pair) {
                $bind[] = $pair[0];
                $bind[] = $pair[1];
            }
            $this->control->statement("INSERT INTO {$map} (raw, pattern) VALUES {$values} ON CONFLICT (raw) DO NOTHING", $bind);
        }

        $deltaRows = (int) $this->control->selectOne("SELECT count(*) AS n FROM {$delta}")->n;

        if ($deltaRows > 0) {
            $columns = implode(', ', $this->columns);
            $sums = implode(', ', array_map(static fn ($c): string => "SUM(d.{$c})", $this->sums));
            $extremes = implode(', ', array_map(
                static fn ($c): string => str_starts_with($c, 'min') ? "MIN(d.{$c})" : "MAX(d.{$c})",
                $this->minmax,
            ));
            $select = "COALESCE(m.pattern, d.key), d.store, d.bucket_start, d.environment, {$sums}"
                .($extremes === '' ? '' : ", {$extremes}");

            $this->control->statement(
                "INSERT INTO {$new} ({$columns})
                 SELECT {$select}
                 FROM {$delta} d LEFT JOIN {$map} m ON m.raw = d.key
                 GROUP BY 1, 2, 3, 4
                 ON CONFLICT (".implode(', ', $pk).') DO UPDATE SET '.$this->conflictUpdates($new)
            );

            // A group the deltas emptied (retention pruned it mid-rebuild) is not
            // a zero row, it is an absent row.
            $this->control->statement("DELETE FROM {$new} WHERE call_count <= 0");
        }

        $this->control->statement("DROP TABLE {$table}");
        $this->control->statement("ALTER TABLE {$new} RENAME TO {$table}");
        $this->control->statement("ALTER INDEX {$new}_pkey RENAME TO {$table}_pkey");
        foreach ($indexes as $i => $index) {
            $this->control->statement("ALTER INDEX {$new}_i{$i} RENAME TO {$index['name']}");
        }

        $this->control->statement("DROP TABLE IF EXISTS {$delta}");
        $this->control->statement("DROP TABLE IF EXISTS {$map}");
        $this->control->statement("DROP FUNCTION IF EXISTS {$fn}()");
        $this->control->commit();

        return $deltaRows;
    }

    private function abort(string $table, string $trigger, string $fn): void
    {
        foreach ([$this->work, $this->control] as $conn) {
            try {
                if ($conn->transactionLevel() > 0) {
                    $conn->rollBack();
                }
            } catch (\Throwable) {
                // A rolled-back-but-still-open handle is worse than a noisy one:
                // dispose of it so the next statement gets a clean session.
                try {
                    $conn->disconnect();
                } catch (\Throwable) {
                }
            }
        }

        $this->dropScratch($table, $trigger, $fn);
    }

    private function dropScratch(string $table, string $trigger, string $fn): void
    {
        try {
            $this->control->statement("DROP TRIGGER IF EXISTS {$trigger} ON {$table}");
            $this->control->statement("DROP FUNCTION IF EXISTS {$fn}()");
            foreach (self::SUFFIXES as $suffix) {
                $this->control->statement("DROP TABLE IF EXISTS {$table}{$suffix}");
            }
        } catch (\Throwable $e) {
            ($this->log)('    could not clear scratch objects: '.$e->getMessage());
        }
    }
}
