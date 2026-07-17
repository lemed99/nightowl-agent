<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Hourly + daily siblings for every minute-granular rollup table. Wide-range
     * reads (7d/30d charts, facet windows) aggregate every minute-row in the
     * window — ~8.6M wide rows for 30d on a 200-route tenant — which trips the
     * tenant statement timeout. The coarser tiers cut that 60× / 1440×; counters
     * and √2 histogram bins are additive, so the collapse is lossless.
     *
     * `LIKE ... INCLUDING ALL` copies columns, defaults, the PK (including the
     * query rollup's 4-column PK with `connection`), and the bucket_start index,
     * with auto-generated names — the tier tables must stay structurally
     * identical to their base so the drain's shared upsert SQL and the API's
     * readers work against any tier unchanged. reloptions are NOT copied by
     * LIKE, so fillfactor is set explicitly (see migration 000053 for why 70).
     *
     * The tier word sits BEFORE the `_rollups` suffix (request_hourly_rollups,
     * not request_rollups_hourly) so the api's RollupCoverageTest name guard
     * (`nightowl_\w+_rollups`) keeps matching every rollup table.
     */
    private const BASE_TABLES = [
        'nightowl_query_rollups',
        'nightowl_request_rollups',
        'nightowl_job_rollups',
        'nightowl_outgoing_request_rollups',
        'nightowl_cache_rollups',
        'nightowl_user_rollups',
        'nightowl_user_job_rollups',
        'nightowl_user_exception_rollups',
        'nightowl_exception_rollups',
        'nightowl_exception_server_rollups',
        'nightowl_mail_rollups',
        'nightowl_notification_rollups',
        'nightowl_command_rollups',
        'nightowl_scheduled_task_rollups',
    ];

    private const TIER_SUFFIXES = ['hourly', 'daily'];

    public function up(): void
    {
        $schema = Schema::connection($this->connection);
        $conn = DB::connection($this->connection);

        foreach (self::BASE_TABLES as $base) {
            if (! $schema->hasTable($base)) {
                continue;
            }

            foreach (self::TIER_SUFFIXES as $tier) {
                $table = str_replace('_rollups', "_{$tier}_rollups", $base);

                $conn->statement(
                    "CREATE TABLE IF NOT EXISTS {$table} (LIKE {$base} INCLUDING ALL)"
                );
                $conn->statement(
                    "ALTER TABLE {$table} SET (fillfactor = 70)"
                );
            }
        }
    }

    public function down(): void
    {
        $conn = DB::connection($this->connection);

        foreach (self::BASE_TABLES as $base) {
            foreach (self::TIER_SUFFIXES as $tier) {
                $table = str_replace('_rollups', "_{$tier}_rollups", $base);
                $conn->statement("DROP TABLE IF EXISTS {$table}");
            }
        }
    }
};
