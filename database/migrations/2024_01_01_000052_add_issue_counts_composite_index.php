<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Covering composite index for the sidebar issues-badge poll
     * (GET /data/{app}/issues/counts, IssueController::counts). That endpoint
     * runs a conditional-SUM aggregate over nightowl_issues filtered by
     * environment, bucketed by status/type, plus the unassigned/mine sub-counts
     * on assigned_to. On a large issues table over a remote BYO Postgres the
     * aggregate scanned the full wide heap (exception_message TEXT and friends)
     * and blew past the statement_timeout — surfacing as a 504, or on tenants
     * whose pooler drops the timeout, a worker-killing 30s FatalError.
     *
     * Leading with environment (the sole WHERE) narrows the scan; carrying
     * status, type and assigned_to lets the aggregate run index-only, off a
     * b-tree far narrower than the heap rows. All four columns the query reads
     * live in the index, so no heap access is needed for all-visible rows.
     *
     * CONCURRENTLY (hence $withinTransaction = false): runs on customers' live
     * production DBs while the agent drains — a plain blocking CREATE INDEX would
     * hold a write lock and stall the drain.
     */
    public $withinTransaction = false;

    private const INDEX = 'nightowl_issues_env_status_type_assigned_idx';

    public function up(): void
    {
        if (! Schema::connection($this->connection)->hasTable('nightowl_issues')) {
            return;
        }

        $conn = DB::connection($this->connection);

        // A CONCURRENTLY build cancelled mid-scan (a role/db statement_timeout, a
        // pooler idle-cancel, or a dropped connection — all likely on the large,
        // cancellation-prone table this index targets) leaves an INVALID index
        // behind. CREATE INDEX ... IF NOT EXISTS matches on NAME only, so it would
        // then skip forever, reporting success while the planner ignores the dead
        // index (which still adds write overhead on every upsert). Drop any invalid
        // leftover first so a re-run self-heals instead of silently no-op'ing.
        $invalid = $conn->selectOne(
            'SELECT 1 FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid '.
            'WHERE c.relname = ? AND NOT i.indisvalid',
            [self::INDEX]
        );

        if ($invalid !== null) {
            $conn->statement('DROP INDEX CONCURRENTLY IF EXISTS '.self::INDEX);
        }

        $conn->statement(
            'CREATE INDEX CONCURRENTLY IF NOT EXISTS '.self::INDEX.
            ' ON nightowl_issues (environment, status, type, assigned_to)'
        );
    }

    public function down(): void
    {
        DB::connection($this->connection)->statement('DROP INDEX CONCURRENTLY IF EXISTS '.self::INDEX);
    }
};
