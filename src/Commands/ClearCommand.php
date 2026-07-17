<?php

namespace NightOwl\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use NightOwl\Support\RollupSpecs;
use NightOwl\Support\RollupTiers;

class ClearCommand extends Command
{
    protected $signature = 'nightowl:clear
        {--force : Skip confirmation}';

    protected $description = 'Clear all NightOwl monitoring data';

    /**
     * Raw telemetry tables — kept byte-identical to PruneCommand::TABLES so clear
     * is just prune with zero retention over the same event set. Triage state
     * (issues/activity/comments), config (settings/alert_channels) and user
     * identity are intentionally NOT cleared: this wipes telemetry, not setup.
     */
    private const RAW_TABLES = [
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
        'nightowl_logs',
    ];

    public function handle(): int
    {
        if (! $this->option('force') && ! $this->confirm('This will delete ALL NightOwl monitoring data. Continue?')) {
            return self::SUCCESS;
        }

        $conn = DB::connection('nightowl');
        $schema = $conn->getSchemaBuilder();

        $cleared = 0;
        foreach (self::tables() as $table) {
            // A tenant on an older schema may not have every rollup table yet —
            // skip absent tables rather than aborting the whole clear.
            if (! $schema->hasTable($table)) {
                continue;
            }

            $conn->table($table)->truncate();
            $cleared++;
        }

        $this->info("All NightOwl data cleared ({$cleared} tables).");

        return self::SUCCESS;
    }

    /**
     * Every table clear truncates: raw telemetry + every rollup table. The rollup
     * names are derived from the single RollupSpecs registry the drain, backfill,
     * and prune all read, so a newly added rollup type can never be silently left
     * un-cleared (the bug where clear covered only the raw tables and none of the
     * rollups, leaving wide-range views populated from stale rollups after a reset).
     *
     * @return list<string>
     */
    public static function tables(): array
    {
        $rollups = [];
        foreach (RollupSpecs::all() as $spec) {
            $rollups[$spec->table] = true;
            foreach (RollupTiers::tierTables($spec->table) as $tierTable) {
                $rollups[$tierTable] = true;
            }
        }

        return array_values(array_unique([
            ...self::RAW_TABLES,
            ...array_keys($rollups),
        ]));
    }
}
