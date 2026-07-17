<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Convert EMPTY raw telemetry tables to native daily partitioning by
     * created_at — fresh installs get partitioned tables from day one, so
     * nightowl:prune drops whole expired children (instant) instead of running
     * multi-million-row DELETEs (WAL storms + vacuum debt at high volume).
     *
     * Populated tables are deliberately SKIPPED: converting live data is the
     * operator-run `nightowl:partition` command's job (rename + validated
     * CHECK + ATTACH, no row copying). This migration must also stay
     * idempotent under MigrateCommand's baseline adoption.
     *
     * Self-contained by design — this file is symlink-shared into nightowl-api,
     * which does not autoload the agent's classes (mirrors RawPartitions).
     *
     * nightowl_logs is excluded: its created_at is a nullable varchar and
     * cannot key a range partition until its own type migration lands.
     *
     * The PK becomes (id, created_at) — Postgres requires the partition key in
     * every unique constraint; raw `id` is synthetic (audited 2026-07-17), so
     * nothing observable changes.
     */
    private const TABLES = [
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
    ];

    private const DAYS_AHEAD = 7;

    public function up(): void
    {
        $schema = Schema::connection($this->connection);
        $conn = DB::connection($this->connection);

        foreach (self::TABLES as $table) {
            if (! $schema->hasTable($table)) {
                continue;
            }

            $relkind = $conn->selectOne(
                "SELECT relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = ?",
                [$table],
            )?->relkind;
            if ($relkind === 'p') {
                continue; // already partitioned
            }

            $hasRows = (bool) $conn->selectOne("SELECT EXISTS (SELECT 1 FROM {$table} LIMIT 1) AS e")->e;
            if ($hasRows) {
                continue; // live data — nightowl:partition owns that conversion
            }

            $tmp = "{$table}_pnew";

            $conn->statement("CREATE TABLE {$tmp} (LIKE {$table} INCLUDING DEFAULTS INCLUDING STORAGE) PARTITION BY RANGE (created_at)");
            $conn->statement("ALTER TABLE {$tmp} ADD PRIMARY KEY (id, created_at)");

            // Replay the (post-index-diet) secondary indexes onto the parent;
            // definitions cascade to every child. Names get a _pt suffix.
            $defs = $conn->select(
                "SELECT i.indexname, i.indexdef FROM pg_indexes i
                 WHERE i.schemaname = 'public' AND i.tablename = ?
                   AND i.indexname NOT IN (
                       SELECT c.relname FROM pg_index x
                       JOIN pg_class c ON c.oid = x.indexrelid
                       JOIN pg_class t ON t.oid = x.indrelid
                       WHERE t.relname = ? AND x.indisprimary
                   )",
                [$table, $table],
            );
            foreach ($defs as $def) {
                $sql = str_replace("INDEX {$def->indexname} ON", "INDEX {$def->indexname}_pt ON", $def->indexdef);
                $sql = str_replace("ON public.{$table}", "ON public.{$tmp}", $sql);
                $conn->statement($sql);
            }

            // Re-own the id sequence to the new parent BEFORE dropping the old
            // table: the clone's id DEFAULT references the sequence, which is
            // OWNED BY the old table — dropping first raises 2BP01.
            $seq = $conn->selectOne("SELECT pg_get_serial_sequence(?, 'id') AS s", [$table])?->s;
            if ($seq !== null) {
                $conn->statement("ALTER SEQUENCE {$seq} OWNED BY {$tmp}.id");
            }

            $conn->statement("DROP TABLE {$table}");
            $conn->statement("ALTER TABLE {$tmp} RENAME TO {$table}");

            $conn->statement("CREATE TABLE {$table}_pdefault PARTITION OF {$table} DEFAULT");

            $today = intdiv(time(), 86400) * 86400;
            for ($d = 0; $d <= self::DAYS_AHEAD; $d++) {
                $day = $today + $d * 86400;
                $child = $table.'_p'.gmdate('Ymd', $day);
                $from = gmdate('Y-m-d 00:00:00', $day);
                $to = gmdate('Y-m-d 00:00:00', $day + 86400);
                $conn->statement("CREATE TABLE IF NOT EXISTS {$child} PARTITION OF {$table} FOR VALUES FROM ('{$from}') TO ('{$to}')");
            }
        }
    }

    public function down(): void
    {
        // Intentionally a no-op: un-partitioning would require a full rebuild
        // and data copy. Partitioned and unpartitioned schemas are both fully
        // supported by every reader and writer.
    }
};
