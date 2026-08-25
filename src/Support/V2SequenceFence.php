<?php

namespace NightOwl\Support;

use PDO;

/**
 * Keeps every storage-v2 raw id sequence ABOVE its v1 twin's id range for as
 * long as BOTH families can still be written. (Not the cutover fence — that one
 * is the `v2_fence` timestamp in nightowl_settings, StorageV2::FENCE_KEY.)
 *
 * Migration 000067 sets each v2 sequence to MAX(v1.id)+1 inside its
 * table-creation guard, so it runs exactly once per tenant, at v2-table
 * creation. Every v1 insert after that instant overtakes the frozen value and
 * mints ids the v2 family will hand out again. The mixed fleet is the case the
 * codebase already plans for (PruneCommand's v1-EOL gate 4): app server A still
 * on a 1.x agent drains into nightowl_requests while server B on 2.x drains
 * into nightowl_requests_v2. A NIGHTOWL_STORAGE_V2=false excursion and flip
 * back does it too, from a v1 sequence that restarts at 1.
 *
 * Why that matters beyond tidiness: the API pages the v1+v2 union with
 * orderByDesc('created_at')->orderByDesc('id')->cursorPaginate(), and a COPY
 * batch stamps ONE shared created_at across its rows, so `id` is the sole
 * tiebreak. Duplicate (created_at, id) tuples make "Next" silently skip or
 * repeat rows on the Requests and exception-occurrence lists.
 *
 * So the fence is not a creation-time detail — it is re-applied on every
 * nightowl:migrate (deploys, nightowl:install, and the daemon's boot
 * auto-migrate when it fires), which is what makes it self-healing for as long
 * as the mixed-fleet window lasts. setval only moves the generator: it takes no
 * table lock and touches no row, so re-applying it while the drain writes is
 * safe.
 */
final class V2SequenceFence
{
    /**
     * Headroom reserved between the v1 high-water mark and the v2 sequence.
     *
     * It must exceed the rows a legacy 1.x agent can push into v1 BETWEEN two
     * re-fences, and a re-fence only happens on migrate/boot — days apart on a
     * tenant that is not deploying. Peak measured ingest is ~13,400 payloads/s,
     * so 1e6 buys 75 seconds and is provably too small; 1e9 buys ~21 hours at
     * that same peak and is unreachable in practice (a tenant sustaining it for
     * a day would be redeploying inside the window anyway). Both id columns are
     * bigint (v1 `$table->id()`, v2 `id bigserial`), so the whole cost is
     * cosmetically large ids against 9.2e18 of range.
     *
     * Applied even when v1 is EMPTY, and that is deliberate: the fence exists
     * against FUTURE v1 inserts as much as existing rows — the excursion case
     * starts an empty v1 at id 1, which is exactly where an unfenced v2 also
     * sits.
     */
    public const GAP = 1_000_000_000;

    /**
     * Re-fence only once headroom has fallen below this. Hysteresis, not
     * frugality: setval is not atomic against a concurrent nextval, so a move
     * smaller than the ids the drain can consume between our read and our
     * setval would hand the same id out twice INSIDE v2. Every move this admits
     * is at least GAP/2 = 5e8 ids ≈ 10h of peak ingest, which no
     * sub-millisecond window can cross. Above the floor there is nothing to
     * fix — v2 is still 5e8 ids clear of v1 — only margin to restore, and that
     * can wait for the next migrate.
     */
    public const MIN_HEADROOM = self::GAP / 2;

    /**
     * Re-apply the fence. Idempotent and strictly forward-only: a sequence is
     * never moved backwards, whatever the v1 high-water mark does (prune and
     * nightowl:clear can empty v1 entirely, so MAX(v1.id) is NOT monotonic and
     * cannot be trusted as the sole input).
     *
     * Pairs whose v1 parent is absent are skipped: prune's v1-EOL DROPs the
     * parent once the fence is past retention and v1 is empty, and a retired
     * family can never be written again — there is nothing left to collide
     * with, and naming a dropped table would 42P01 the whole routine.
     *
     * Per-pair isolation: one unreadable sequence must never cost the other
     * ten their fence. Callers render `failures`; nothing here throws for a
     * single table.
     *
     * @param  array<string, string>|null  $pairs  test hook (v1 => v2); production uses StorageV2::TABLES
     * @return array{refenced: array<string, int>, overlapping: list<string>, failures: array<string, string>}
     *                                                                                                       keyed/valued by the LOGICAL (v1) table name; refenced maps it to the sequence's new next id
     */
    public static function apply(PDO $conn, ?array $pairs = null): array
    {
        $refenced = [];
        $overlapping = [];
        $failures = [];

        $present = self::presentPairs($conn, $pairs ?? StorageV2::TABLES);
        if ($present === []) {
            return ['refenced' => [], 'overlapping' => [], 'failures' => []];
        }

        // Two round trips for the whole fleet of pairs — the states of every
        // pair in one statement, then every sequence's next value in one — on
        // the healthy path, which is every path but a broken one. It was two
        // PER pair, and this runs on every nightowl:migrate, so on a tenant
        // whose Postgres is a ~100ms hop away the fence alone cost ~2s of
        // deploy. The batch is an optimisation, not a change of contract: if
        // it fails at all, the per-pair reads below take over unchanged, so
        // one unreadable table still costs only its own fence.
        $states = self::readStates($conn, $present);
        $nexts = $states === null ? null : self::nextValues($conn, array_filter(
            array_column($states, 'seq'),
            static fn (?string $seq): bool => $seq !== null,
        ));

        foreach ($present as $v1 => $v2) {
            try {
                $state = $states[$v1] ?? self::readState($conn, $v1, $v2);

                if ($state === false || $state['seq'] === null) {
                    // Partitioned parents DO resolve here (000067's own fence
                    // relies on it); a NULL means the column carries no owned
                    // sequence at all, which we cannot fence.
                    $failures[$v1] = "{$v2}.id owns no sequence";

                    continue;
                }

                $v1Max = (int) $state['v1_max'];

                // Ids already issued cannot be un-collided — surface it and
                // move on, never renumber. The test is MIN(v2.id) rather than
                // MAX: once the fence lifts new v2 rows above v1, MAX clears
                // it immediately while the OVERLAPPING low ids are still the
                // ones the union pages. It self-clears as those rows age out at
                // retention.
                if ($state['v2_min'] !== null && (int) $state['v2_min'] <= $v1Max) {
                    $overlapping[] = $v1;
                }

                $next = $nexts[$state['seq']] ?? self::nextValue($conn, $state['seq']);
                if ($next - $v1Max >= self::MIN_HEADROOM) {
                    continue;
                }

                $target = $v1Max + self::GAP;
                $stmt = $conn->prepare('SELECT setval(?, ?, false)');
                $stmt->execute([$state['seq'], $target]);
                $refenced[$v1] = $target;
            } catch (\Throwable $e) {
                $failures[$v1] = $e->getMessage();
            }
        }

        return ['refenced' => $refenced, 'overlapping' => $overlapping, 'failures' => $failures];
    }

    /**
     * Everything the decision needs for one pair. MAX/MIN(id) are index-backed
     * via the PK's leading column (a MergeAppend of one descent per partition
     * child), so this stays instant on a populated tenant.
     */
    private static function stateSql(string $v1, string $v2): string
    {
        return "SELECT '{$v1}' AS v1,
                       pg_get_serial_sequence('{$v2}', 'id') AS seq,
                       (SELECT COALESCE(MAX(id), 0) FROM {$v1}) AS v1_max,
                       (SELECT MIN(id) FROM {$v2}) AS v2_min";
    }

    /** @return array<string, mixed>|false */
    private static function readState(PDO $conn, string $v1, string $v2): array|false
    {
        return $conn->query(self::stateSql($v1, $v2))->fetch(PDO::FETCH_ASSOC);
    }

    /**
     * All pairs' states in one statement, keyed by v1 name; null when the
     * batch itself fails, which hands every pair to the isolated per-pair path.
     *
     * @param  array<string, string>  $pairs
     * @return array<string, array<string, mixed>>|null
     */
    private static function readStates(PDO $conn, array $pairs): ?array
    {
        $legs = [];
        foreach ($pairs as $v1 => $v2) {
            $legs[] = self::stateSql($v1, $v2);
        }

        try {
            $rows = $conn->query(implode(' UNION ALL ', $legs))->fetchAll(PDO::FETCH_ASSOC);
        } catch (\Throwable) {
            return null;
        }

        $states = [];
        foreach ($rows as $row) {
            $states[$row['v1']] = $row;
        }

        return $states;
    }

    /**
     * Next id of every sequence in one statement (see nextValue for why the
     * relation is read directly); null on failure — per-sequence reads then
     * take over, isolated.
     *
     * @param  array<int|string, string>  $sequences
     * @return array<string, int>|null
     */
    private static function nextValues(PDO $conn, array $sequences): ?array
    {
        $sequences = array_values(array_unique($sequences));
        if ($sequences === []) {
            return [];
        }

        $legs = array_map(
            static fn (string $seq): string => "SELECT '{$seq}' AS seq, last_value + (CASE WHEN is_called THEN 1 ELSE 0 END) AS next FROM {$seq}",
            $sequences,
        );

        try {
            $rows = $conn->query(implode(' UNION ALL ', $legs))->fetchAll(PDO::FETCH_ASSOC);
        } catch (\Throwable) {
            return null;
        }

        $nexts = [];
        foreach ($rows as $row) {
            $nexts[$row['seq']] = (int) $row['next'];
        }

        return $nexts;
    }

    /**
     * The id the next nextval() will hand out.
     *
     * Read off the sequence relation instead of pg_sequence_last_value(), which
     * answers NULL for BOTH "never used" and "last touched by setval(…, false)"
     * — and 000067's fence leaves exactly the second state, so a NULL read as 0
     * would mistake a healthy fence for an absent one and move it backwards.
     */
    private static function nextValue(PDO $conn, string $sequence): int
    {
        return (int) $conn->query(
            "SELECT last_value + (CASE WHEN is_called THEN 1 ELSE 0 END) AS next FROM {$sequence}"
        )->fetchColumn();
    }

    /**
     * One round trip: the pairs whose BOTH tables exist. Both sides are probed
     * because both can legitimately be missing — a pre-cutover tenant has no v2
     * twins, a post-EOL tenant no v1 parents. Schema-relative to_regclass (no
     * `public.`): search_path-scoped tenants exist.
     *
     * @param  array<string, string>  $pairs
     * @return array<string, string>
     */
    private static function presentPairs(PDO $conn, array $pairs): array
    {
        $terms = [];
        foreach ($pairs as $v1 => $v2) {
            $terms[] = "SELECT '{$v1}' AS t WHERE to_regclass('{$v1}') IS NOT NULL";
            $terms[] = "SELECT '{$v2}' AS t WHERE to_regclass('{$v2}') IS NOT NULL";
        }

        $present = array_flip(
            $conn->query(implode(' UNION ALL ', $terms))->fetchAll(PDO::FETCH_COLUMN)
        );

        return array_filter(
            $pairs,
            static fn (string $v2, string $v1): bool => isset($present[$v1], $present[$v2]),
            ARRAY_FILTER_USE_BOTH,
        );
    }
}
