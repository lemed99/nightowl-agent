<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Additive count of occurrences carrying a user_id, so the exception detail
     * "authenticated vs guest" split reads SUM(authenticated_count) off the
     * fingerprint rollup (guest = call_count - authenticated_count) instead of a
     * full-window SUM(CASE WHEN user_id ...) scan of raw nightowl_exceptions.
     *
     * NULLABLE with no default — deliberately. Adding the column back-stamps every
     * pre-existing (pre-upgrade) minute bucket as NULL, which the drain and
     * nightowl:backfill-rollups later overwrite with real counts. A default of 0
     * would make an un-backfilled bucket indistinguishable from a genuinely
     * all-guest one, so the read path would silently report "all guest"; the NULL
     * sentinel instead lets the API detect an un-backfilled window and fall back to
     * the exact raw split. Until this column exists the agent's rollupColumnsPresent
     * guard skips writing nightowl_exception_rollups, so run nightowl:migrate then
     * nightowl:backfill-rollups on upgrade.
     */
    public function up(): void
    {
        $schema = Schema::connection($this->connection);

        if (! $schema->hasTable('nightowl_exception_rollups')) {
            return;
        }
        if ($schema->hasColumn('nightowl_exception_rollups', 'authenticated_count')) {
            return;
        }

        $schema->table('nightowl_exception_rollups', function (Blueprint $table): void {
            $table->bigInteger('authenticated_count')->nullable()->after('unhandled_count');
        });
    }

    public function down(): void
    {
        $schema = Schema::connection($this->connection);

        if ($schema->hasColumn('nightowl_exception_rollups', 'authenticated_count')) {
            $schema->table('nightowl_exception_rollups', function (Blueprint $table): void {
                $table->dropColumn('authenticated_count');
            });
        }
    }
};
