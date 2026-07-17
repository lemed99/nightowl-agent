<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Second pass of the raw-index diet (first: 000056): the two "weak trio"
     * members whose only readers are list filters/sorts that always carry a
     * created_at range — the planner drives those off the time index, so these
     * singles tax every drain COPY for nothing:
     *
     * - nightowl_outgoing_requests.status_code — DeletionFilters equality +
     *   list sortable, both created_at-co-bounded; unlike requests.status_code
     *   it backs NO loose index scan.
     * - nightowl_scheduled_tasks.status — list filter/sort, co-bounded; not in
     *   distinctViaIndex (only jobs.status is).
     *
     * The third member, nightowl_exceptions.environment, is deliberately KEPT:
     * the environment filter is pervasive (HasEnvironmentFilter applies it to
     * most tenant reads), and a tenant filtering on a RARE environment over a
     * wide window is exactly the selectivity regime where a standalone env
     * index beats the time index. Cheap insurance on the issues feature.
     *
     * Both names are dropped in their unpartitioned and partitioned (_pt
     * replay) forms. CONCURRENTLY only works on non-partitioned indexes, so
     * the _pt variants take a plain DROP (brief lock; metadata-only).
     */
    public $withinTransaction = false;

    /** @var array<string, array{0: string, 1: string}> index name => [table, column] */
    private const WEAK_INDEXES = [
        'nightowl_outgoing_requests_status_code_index' => ['nightowl_outgoing_requests', 'status_code'],
        'nightowl_scheduled_tasks_status_index' => ['nightowl_scheduled_tasks', 'status'],
    ];

    public function up(): void
    {
        foreach (array_keys(self::WEAK_INDEXES) as $name) {
            DB::connection($this->connection)->statement("DROP INDEX CONCURRENTLY IF EXISTS {$name}");
            DB::connection($this->connection)->statement("DROP INDEX IF EXISTS {$name}_pt");
        }
    }

    public function down(): void
    {
        $schema = Schema::connection($this->connection);

        foreach (self::WEAK_INDEXES as $name => [$table, $column]) {
            if (! $schema->hasTable($table)) {
                continue;
            }

            // Partitioned parents can't take CONCURRENTLY; plain CREATE is
            // metadata + per-child builds.
            $relkind = DB::connection($this->connection)->selectOne(
                "SELECT relkind FROM pg_class WHERE relname = ?",
                [$table],
            )?->relkind;

            if ($relkind === 'p') {
                DB::connection($this->connection)->statement(
                    "CREATE INDEX IF NOT EXISTS {$name}_pt ON {$table} ({$column})"
                );
            } else {
                DB::connection($this->connection)->statement(
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS {$name} ON {$table} ({$column})"
                );
            }
        }
    }
};
