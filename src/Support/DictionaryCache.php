<?php

namespace NightOwl\Support;

use PDO;
use RuntimeException;

/**
 * Per-drain-worker LRU over the four storage-v2 dictionaries.
 *
 * Transaction discipline (the invariant everything here serves): ids enter
 * the LONG-LIVED cache only from OUTSIDE the batch transaction. The warm pass
 * runs in autocommit before doWrite()'s BEGIN — its inserts are durable the
 * moment they return, so a later batch rollback cannot invalidate a cached
 * id (a rolled-back batch's dict rows are merely unreferenced values in an
 * append-only table — harmless). The in-transaction fallback (`idOrResolve`)
 * exists only as defense in depth: anything it resolves is staged in
 * $pending and promoted at the same point doWrite() publishes
 * lastWrittenTables (post-commit), or discarded in the catch.
 *
 * Eviction discipline: the maps are trimmed to their caps ONLY at the point a
 * batch's outcome is published (promotePending / discardPending) — never during
 * the warm pass or the write. Within one batch nothing may be dropped: the
 * collector (RecordWriter::collectDictMisses) omits every value the LRU already
 * holds, so an id evicted mid-batch is not re-warmed and the write reads back
 * null into a NULLABLE sql_id/route_id/trace_ref. The dict row exists, the link
 * is gone for good (blank SQL card, routeless request, traceless exception),
 * and nothing errors — which is why the trim cannot live in warmTable's chunk
 * loop, where a batch carrying more distinct values than a cap ate its own
 * earliest-warmed ids. Peak map size is therefore cap + one batch's distinct
 * values — and that overshoot is NOT bounded by NIGHTOWL_DRAIN_BATCH_SIZE.
 * That knob bounds the SQLite payload ROWS a worker claims (DrainWorker's
 * claimBatch/fetchPending); each row json_decodes into an ARRAY of records — a
 * Nightwatch digest carries many events per payload — and they are flattened
 * into the single $records array handed to write(). The real ceiling is
 * batch rows × records per payload × the distinct values one record
 * contributes, across all four maps. It is transient, not a leak: the trim at
 * the batch's outcome frees it and nothing accumulates into the next batch.
 *
 * Concurrent workers: INSERT ... ON CONFLICT DO NOTHING makes worker B's
 * speculative insert wait on worker A's in-flight tuple; after A commits,
 * B's insert no-ops and B's follow-up SELECT (fresh READ COMMITTED snapshot)
 * returns A's id — both workers converge with no advisory lock. Ids never
 * renumber: the dictionaries are append-only by contract.
 */
final class DictionaryCache
{
    private const STRING_CAP = 16384;

    private const SQL_CAP = 4096;

    private const ROUTE_CAP = 4096;

    private const TRACE_CAP = 2048;

    /** map property → cap, so trim() is the single place any of them applies. */
    private const CAPS = [
        'strings' => self::STRING_CAP,
        'sql' => self::SQL_CAP,
        'routes' => self::ROUTE_CAP,
        'traces' => self::TRACE_CAP,
    ];

    /** Chunk VALUES lists well under the 65 535 bind-param cap. */
    private const INSERT_CHUNK = 500;

    /** @var array<string, int> "kind\0value" → id */
    private array $strings = [];

    /** @var array<string, int> 32-char hex hash → id */
    private array $sql = [];

    /** @var array<string, int> */
    private array $routes = [];

    /** @var array<string, int> */
    private array $traces = [];

    /** @var array<int, array{0: string, 1: string, 2: int}> [map, key, id] staged inside the batch txn */
    private array $pending = [];

    // ---------------------------------------------------------------- lookups

    public function stringId(string $kind, string $value): ?int
    {
        return $this->touch($this->strings, $kind."\0".$value);
    }

    public function sqlId(string $hash): ?int
    {
        return $this->touch($this->sql, $hash);
    }

    public function routeId(string $hash): ?int
    {
        return $this->touch($this->routes, $hash);
    }

    public function traceId(string $hash): ?int
    {
        return $this->touch($this->traces, $hash);
    }

    // ------------------------------------------------------------------ warm

    /**
     * Autocommit warm pass. $misses shape:
     *  [
     *    'string' => [['kind', 'value'], ...],
     *    'sql'    => [['hash32hex', 'sql', 'file'|null, line|null], ...],
     *    'route'  => [['hash32hex', method, domain, path, name, action, methods], ...],
     *    'trace'  => [['hash32hex', deflatedByteaWireForm], ...],
     *  ]
     * Values already present in the LRU are skipped by the caller's collector;
     * re-sending them is merely wasted work, never wrong.
     */
    public function warm(PDO $pdo, array $misses): void
    {
        if ($pdo->inTransaction()) {
            // The invariant above would break silently; fail loudly in tests,
            // visibly in the field.
            throw new RuntimeException('DictionaryCache::warm must run outside the batch transaction');
        }

        if (($rows = $misses['string'] ?? []) !== []) {
            $this->warmTable(
                $pdo, $rows,
                'INSERT INTO nightowl_dict_string (kind, value) VALUES %s ON CONFLICT (kind, value) DO NOTHING',
                '(?, ?)',
                fn (array $r): array => [$r[0], $r[1]],
                'SELECT id, kind, value FROM nightowl_dict_string WHERE (kind, value) IN (%s)',
                fn (object $row): array => [$this->stringKey($row->kind, $row->value), (int) $row->id],
                $this->strings,
            );
        }

        if (($rows = $misses['sql'] ?? []) !== []) {
            $this->warmTable(
                $pdo, $rows,
                'INSERT INTO nightowl_dict_sql (hash, sql, file, line) VALUES %s ON CONFLICT (hash) DO NOTHING',
                "(decode(?, 'hex'), ?, ?, ?)",
                fn (array $r): array => [$r[0], $r[1], $r[2], $r[3]],
                "SELECT id, encode(hash, 'hex') AS hash FROM nightowl_dict_sql WHERE hash IN (%s)",
                fn (object $row): array => [$row->hash, (int) $row->id],
                $this->sql,
                hashPlaceholder: "decode(?, 'hex')",
            );
        }

        if (($rows = $misses['route'] ?? []) !== []) {
            $this->warmTable(
                $pdo, $rows,
                'INSERT INTO nightowl_dict_route (hash, method, domain, path, name, action, methods) VALUES %s ON CONFLICT (hash) DO NOTHING',
                "(decode(?, 'hex'), ?, ?, ?, ?, ?, ?)",
                fn (array $r): array => $r,
                "SELECT id, encode(hash, 'hex') AS hash FROM nightowl_dict_route WHERE hash IN (%s)",
                fn (object $row): array => [$row->hash, (int) $row->id],
                $this->routes,
                hashPlaceholder: "decode(?, 'hex')",
            );
        }

        if (($rows = $misses['trace'] ?? []) !== []) {
            // Traces are the one GC'd dictionary (nightowl:gc-dict-traces), so
            // their warm differs from the other three in two ways:
            //
            //  1. ON CONFLICT DO UPDATE SET created_at = now() — every reference
            //     bumps the last-referenced clock the GC's `created_at <
            //     now() - quarantine` predicate reads, so an actively-used
            //     trace can never age into GC range. (The collector also stops
            //     trusting the LRU for traces — see RecordWriter::collectDict-
            //     Misses — so this touch fires on EVERY batch that references
            //     the trace, not just the first.) If the GC won a race and
            //     deleted the row, the INSERT re-creates it (append-only) and
            //     the SELECT-back returns the fresh id, so the exception write
            //     never lands a dangling trace_ref.
            //
            //  2. Rows sorted by hash before the multi-row INSERT. DO UPDATE
            //     takes a row lock per conflicting tuple (DO NOTHING mostly
            //     did not), so two workers touching an overlapping trace set in
            //     different orders could deadlock; a global hash order makes
            //     every worker lock in the same sequence. Autocommit warm, so a
            //     single statement is a single txn — sorting within it suffices.
            usort($rows, fn (array $a, array $b): int => strcmp($a[0], $b[0]));
            $this->warmTable(
                $pdo, $rows,
                'INSERT INTO nightowl_dict_trace (hash, trace_z) VALUES %s ON CONFLICT (hash) DO UPDATE SET created_at = now()',
                "(decode(?, 'hex'), ?)",
                fn (array $r): array => [$r[0], $r[1]],
                "SELECT id, encode(hash, 'hex') AS hash FROM nightowl_dict_trace WHERE hash IN (%s)",
                fn (object $row): array => [$row->hash, (int) $row->id],
                $this->traces,
                hashPlaceholder: "decode(?, 'hex')",
            );
        }
    }

    // ------------------------------------------------- in-txn fallback (rare)

    /**
     * Defensive in-transaction resolution: the shared extractor map makes
     * warm-vs-write disagreement impossible by construction, but if a value
     * still misses mid-batch, resolve it and STAGE the id — promotePending()
     * publishes to the long-lived LRU only after the batch commits.
     */
    public function resolveStringInTxn(PDO $pdo, string $kind, string $value): ?int
    {
        $stmt = $pdo->prepare(
            'WITH ins AS (
                INSERT INTO nightowl_dict_string (kind, value) VALUES (?, ?)
                ON CONFLICT (kind, value) DO NOTHING
                RETURNING id
            )
            SELECT id FROM ins
            UNION ALL
            SELECT id FROM nightowl_dict_string WHERE kind = ? AND value = ?
            LIMIT 1'
        );
        $stmt->execute([$kind, $value, $kind, $value]);
        $id = $stmt->fetchColumn();
        if ($id === false) {
            return null;
        }

        $this->pending[] = ['strings', $this->stringKey($kind, $value), (int) $id];

        return (int) $id;
    }

    /** Publish staged ids — call at the same point lastWrittenTables publishes. */
    public function promotePending(): void
    {
        foreach ($this->pending as [$map, $key, $id]) {
            $this->{$map}[$key] = $id;
        }
        $this->pending = [];
        $this->trim();
    }

    /** Drop staged ids — call in doWrite()'s catch, before/after rollback. */
    public function discardPending(): void
    {
        $this->pending = [];
        $this->trim();
    }

    // ------------------------------------------------------------- internals

    private function stringKey(string $kind, string $value): string
    {
        return $kind."\0".$value;
    }

    /** @param array<string, int> $map */
    private function touch(array &$map, string $key): ?int
    {
        if (! array_key_exists($key, $map)) {
            return null;
        }

        $id = $map[$key];
        unset($map[$key]);
        $map[$key] = $id; // LRU key-touch: re-append so first key is coldest

        return $id;
    }

    /** @param array<string, int> $map */
    private function evict(array &$map, int $cap): void
    {
        while (count($map) > $cap) {
            $coldest = array_key_first($map);
            unset($map[$coldest]);
        }
    }

    /**
     * Every map back to its cap. The ONLY eviction point — reached solely from
     * the two outcome publishers, i.e. between batches (see the class's eviction
     * discipline: a trim anywhere from the collect to the write drops links).
     */
    private function trim(): void
    {
        foreach (self::CAPS as $map => $cap) {
            $this->evict($this->{$map}, $cap);
        }
    }

    /**
     * Insert-then-select warm for one dict table, chunked. $rowValues maps a
     * miss row to its bind params (insert order); $intoCache maps a fetched
     * row to [cacheKey, id]. Chunking is a bind-param limit, not a memory one —
     * the map grows past its cap here and is trimmed only between batches.
     */
    private function warmTable(
        PDO $pdo,
        array $rows,
        string $insertSql,
        string $tuplePlaceholder,
        callable $rowValues,
        string $selectSql,
        callable $intoCache,
        array &$map,
        string $hashPlaceholder = '',
    ): void {
        foreach (array_chunk($rows, self::INSERT_CHUNK) as $chunk) {
            $tuples = implode(', ', array_fill(0, count($chunk), $tuplePlaceholder));
            $params = [];
            foreach ($chunk as $row) {
                foreach ($rowValues($row) as $v) {
                    $params[] = $v;
                }
            }
            $stmt = $pdo->prepare(sprintf($insertSql, $tuples));
            $stmt->execute($params);

            // Fresh snapshot: sees our rows AND any concurrent worker's.
            if ($hashPlaceholder !== '') {
                $in = implode(', ', array_fill(0, count($chunk), $hashPlaceholder));
                $selectParams = array_map(fn (array $r): string => $r[0], $chunk);
            } else {
                $in = implode(', ', array_fill(0, count($chunk), '(?, ?)'));
                $selectParams = [];
                foreach ($chunk as $row) {
                    $selectParams[] = $row[0];
                    $selectParams[] = $row[1];
                }
            }
            $stmt = $pdo->prepare(sprintf($selectSql, $in));
            $stmt->execute($selectParams);

            foreach ($stmt->fetchAll(PDO::FETCH_OBJ) as $fetched) {
                [$key, $id] = $intoCache($fetched);
                $map[$key] = $id;
            }
        }
    }
}
