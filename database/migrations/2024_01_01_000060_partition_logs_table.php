<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Bring nightowl_logs into the raw-table partitioning scheme (000058) —
     * it was deferred there because its created_at is a NULLABLE VARCHAR
     * (historical accident; the writer has always stamped valid UTC
     * 'Y-m-d H:i:s' strings) and a varchar can't key a range partition.
     *
     * EMPTY table only (fresh installs): convert the column type in place
     * (instant on zero rows) and rebuild partitioned — same inline recipe as
     * 000058. Populated tables are skipped: the type change is a full table
     * rewrite under ACCESS EXCLUSIVE, which is an operator decision — run
     * `nightowl:partition` (its logs path performs the rewrite; the agent's
     * SQLite buffer absorbs drain retries and readers degrade to the 57014
     * handler for the duration).
     *
     * Self-contained: symlink-shared into nightowl-api, no agent classes.
     */
    private const DAYS_AHEAD = 7;

    public function up(): void
    {
        $schema = Schema::connection($this->connection);
        $conn = DB::connection($this->connection);
        $table = 'nightowl_logs';

        if (! $schema->hasTable($table)) {
            return;
        }

        $relkind = $conn->selectOne(
            "SELECT relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = ?",
            [$table],
        )?->relkind;
        if ($relkind === 'p') {
            return;
        }

        if ((bool) $conn->selectOne("SELECT EXISTS (SELECT 1 FROM {$table} LIMIT 1) AS e")->e) {
            return; // populated — nightowl:partition owns the rewrite
        }

        // Type first (instant on an empty table), then the 000058 recipe.
        $type = $conn->selectOne(
            "SELECT data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ? AND column_name = 'created_at'",
            [$table],
        )?->data_type;
        if ($type !== 'timestamp without time zone') {
            $conn->statement(
                "ALTER TABLE {$table} ALTER COLUMN created_at TYPE timestamp USING NULLIF(created_at, '')::timestamp"
            );
        }

        $tmp = "{$table}_pnew";
        $conn->statement("CREATE TABLE {$tmp} (LIKE {$table} INCLUDING DEFAULTS INCLUDING STORAGE) PARTITION BY RANGE (created_at)");
        $conn->statement("ALTER TABLE {$tmp} ADD PRIMARY KEY (id, created_at)");

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

    public function down(): void
    {
        // No-op, like 000058: both layouts are fully supported.
    }
};
