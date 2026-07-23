<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * The two detail-page composites 000044 missed. nightowl_mail and
     * nightowl_notifications were left off its INDEXES list, so their
     * per-mailable / per-notification detail lists filter group_hash off an
     * unindexed nullable column and order by the single-column created_at
     * index. That was slow under offset pagination and becomes load-bearing
     * under cursor pagination, whose seek predicate
     * (WHERE group_hash = ? AND (created_at, id) < (?, ?)) wants the
     * (group_hash, created_at) prefix to prune before the row comparison.
     *
     * Both tables are natively PARTITIONED (000058), and Postgres refuses
     * CREATE INDEX CONCURRENTLY on a partitioned parent (0A000). The online
     * pattern is three-phase, all still lock-light on a live tenant:
     *
     *   1. CREATE INDEX ... ON ONLY parent  — catalog-only shell, no build.
     *   2. Per existing child: CREATE INDEX CONCURRENTLY — the real builds,
     *      no write lock held.
     *   3. ATTACH PARTITION each child index — the parent index flips valid
     *      once every child is attached.
     *
     * PARTIAL-APPLY HAZARD this file is written around: boot-migrate runs in
     * a child process with a hard SIGKILL deadline, so any step can be the
     * last one that ran. Two consequences drive the shape of the loop below:
     *
     *  - Once the parent shell exists, every child the drain's hourly sweep
     *    creates (CREATE TABLE ... PARTITION OF) AUTO-INHERITS the index
     *    under Postgres's DEFAULT name — not ours. The attached check must
     *    therefore ask "does this CHILD have ANY index attached to the parent
     *    shell" (pg_inherits joined through pg_index on the child's oid),
     *    never "is an index with OUR name attached". A name-based check
     *    builds a duplicate and then dies on ATTACH with 55000 — permanently,
     *    on every retry, blocking every migration after this one.
     *  - A killed CONCURRENTLY leaves an INVALID index that IF NOT EXISTS
     *    silently keeps. Both branches (partitioned child AND plain table)
     *    drop-and-rebuild invalid indexes; a plain-table tenant is exposed to
     *    the same kill. Known limit: one child whose build alone outruns the
     *    boot deadline restarts from scratch every boot — daily children make
     *    that unlikely (a day of mail), and NIGHTOWL_AUTO_MIGRATE_TIMEOUT or
     *    a manual nightowl:migrate is the escape.
     */
    public $withinTransaction = false;

    /** @var list<array{0: string, 1: string, 2: string}> [index name, table, column list] */
    private const INDEXES = [
        ['nightowl_mail_group_hash_created_at_idx', 'nightowl_mail', 'group_hash, created_at'],
        ['nightowl_notifications_group_hash_created_at_idx', 'nightowl_notifications', 'group_hash, created_at'],
    ];

    public function up(): void
    {
        $conn = DB::connection($this->connection);

        foreach (self::INDEXES as [$name, $table, $columns]) {
            if (! Schema::connection($this->connection)->hasTable($table)) {
                continue;
            }

            $relkind = $conn->selectOne(
                'SELECT relkind FROM pg_class WHERE oid = to_regclass(?)', [$table]
            )->relkind ?? null;

            if ($relkind !== 'p') {
                // Plain (pre-partition) tenant. Same invalid-index discipline
                // as the children below: a killed CONCURRENTLY must not be
                // kept by IF NOT EXISTS.
                $this->dropIfInvalid($conn, $name);
                $conn->statement("CREATE INDEX CONCURRENTLY IF NOT EXISTS {$name} ON {$table} ({$columns})");

                continue;
            }

            $conn->statement("CREATE INDEX IF NOT EXISTS {$name} ON ONLY {$table} ({$columns})");

            $children = $conn->select(
                'SELECT c.relname FROM pg_inherits i
                 JOIN pg_class c ON c.oid = i.inhrelid
                 WHERE i.inhparent = to_regclass(?)
                 ORDER BY c.relname',
                [$table],
            );

            foreach ($children as $child) {
                // Attached check by CHILD, not by our index name: a child
                // created after the shell carries an auto-inherited index
                // under the default name, which satisfies the parent shell.
                $attached = $conn->selectOne(
                    'SELECT x.indexrelid::regclass::text AS idx
                     FROM pg_inherits i
                     JOIN pg_index x ON x.indexrelid = i.inhrelid
                     WHERE i.inhparent = to_regclass(?) AND x.indrelid = to_regclass(?)',
                    [$name, $child->relname],
                );

                $childIdx = "{$child->relname}_ghca_idx";

                if ($attached !== null) {
                    // Already served (possibly under the auto-inherited name).
                    // Sweep up a duplicate our earlier partial run may have
                    // built, but never the attached index itself.
                    if ($attached->idx !== $childIdx) {
                        $conn->statement("DROP INDEX CONCURRENTLY IF EXISTS {$childIdx}");
                    }

                    continue;
                }

                $this->dropIfInvalid($conn, $childIdx);
                $conn->statement("CREATE INDEX CONCURRENTLY IF NOT EXISTS {$childIdx} ON {$child->relname} ({$columns})");
                $conn->statement("ALTER INDEX {$name} ATTACH PARTITION {$childIdx}");
            }
        }
    }

    /** Drop a leftover INVALID index from a killed CONCURRENTLY build. */
    private function dropIfInvalid($conn, string $index): void
    {
        $invalid = $conn->selectOne(
            'SELECT NOT i.indisvalid AS invalid FROM pg_index i WHERE i.indexrelid = to_regclass(?)',
            [$index],
        )->invalid ?? false;

        if ($invalid) {
            $conn->statement("DROP INDEX CONCURRENTLY IF EXISTS {$index}");
        }
    }

    public function down(): void
    {
        foreach (self::INDEXES as [$name]) {
            // Dropping the parent shell cascades to attached children; plain
            // (non-partitioned) tenants just drop the one index.
            DB::connection($this->connection)->statement("DROP INDEX IF EXISTS {$name}");
        }
    }
};
