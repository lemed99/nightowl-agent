<?php

namespace NightOwl\Support;

use PDO;

/**
 * One round trip answering "which of these tables exist, and what kind?"
 *
 * Exists because the per-table probe is what made `nightowl:migrate` slow on a
 * customer whose Postgres sits ~100ms from the app server: Laravel's
 * `Schema::hasTable` and our own `to_regclass` checks are each one query, the
 * deploy path asked ~120 of them, and every one of them paid the full round
 * trip for a catalog lookup the server answers in microseconds. Measured
 * 2026-08-25 on a no-traffic tenant: 72s per deploy, on dev AND prod, because
 * the count is structural (14 rollup types × 3 tiers, 11 raw tables × 2
 * families) and nothing about it depends on data volume. (Reproduced at 80s
 * through a 100ms delay proxy against a local DB that answers in 0.6s direct.)
 *
 * Semantics match `to_regclass` exactly — schema-relative through search_path,
 * no `public.` — so a search_path-scoped tenant resolves the same names the
 * drain and the API resolve. Only ordinary and partitioned tables count
 * (`relkind` r/p), which is also what `Schema::hasTable` counts: a view or an
 * index that happens to share a name is not a table.
 */
final class TableCatalog
{
    /**
     * @param  list<string>  $names  candidate table names (identifiers, not user input)
     * @return array<string, string>  present name => relkind ('r' plain, 'p' partitioned)
     */
    public static function relkinds(PDO $pdo, array $names): array
    {
        $names = array_values(array_unique($names));
        if ($names === []) {
            return [];
        }

        // unnest over a bound text[] keeps this ONE statement whatever the
        // list length; to_regclass per element is what the per-table probes
        // asked, so a name resolves here iff it resolved there.
        $stmt = $pdo->prepare(
            'SELECT n.name, c.relkind
             FROM unnest(?::text[]) AS n(name)
             JOIN pg_class c ON c.oid = to_regclass(n.name)
             WHERE c.relkind IN (\'r\', \'p\')'
        );
        $stmt->execute([self::textArray($names)]);

        $present = [];
        foreach ($stmt->fetchAll(PDO::FETCH_NUM) as [$name, $relkind]) {
            $present[$name] = $relkind;
        }

        return $present;
    }

    /**
     * Postgres array literal for a list of identifiers. Identifiers only —
     * every caller passes constants from RollupSpecs / StorageV2 / RawPartitions
     * — so no element needs quoting inside the literal.
     *
     * @param  list<string>  $names
     */
    public static function textArray(array $names): string
    {
        return '{'.implode(',', $names).'}';
    }
}
