<?php

namespace NightOwl\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use NightOwl\Support\RollupSpecs;

class PruneCommand extends Command
{
    protected $signature = 'nightowl:prune
        {--days= : Number of days to retain raw telemetry}
        {--hours= : Number of HOURS to retain raw telemetry (overrides --days; for aggressive demo-feeder retention on a sub-day cadence)}
        {--rollup-days= : Number of days to retain query rollups (defaults to far longer than raw)}';

    protected $description = 'Prune old NightOwl monitoring data';

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
        'nightowl_logs',
    ];

    public function handle(): int
    {
        // --hours wins when given (fine-grained, for the demo feeder which keeps
        // only a few hours of raw telemetry and prunes every 15-30 min). Otherwise
        // fall back to --days / the configured day-granularity retention.
        $hoursOption = $this->option('hours');
        if ($hoursOption !== null && $hoursOption !== '') {
            $hours = max(1, (int) $hoursOption);
            // created_at is stored in UTC (gmdate), so the cutoff MUST be UTC too — a
            // non-UTC host app TZ would otherwise offset it (deleting fresh rows, or
            // never pruning), and the sub-day --hours cadence makes the offset dominate.
            $cutoff = now()->utc()->subHours($hours)->toDateTimeString();
            $window = "{$hours} hours";
        } else {
            $days = (int) ($this->option('days') ?? config('nightowl.database.retention_days', 14));
            $cutoff = now()->utc()->subDays($days)->toDateTimeString();
            $window = "{$days} days";
        }

        $conn = DB::connection('nightowl');

        $this->info("Pruning records older than {$window} (before {$cutoff})...");

        $totalDeleted = 0;

        foreach (self::TABLES as $table) {
            $deleted = $conn->table($table)->where('created_at', '<', $cutoff)->delete();
            $totalDeleted += $deleted;

            if ($deleted > 0) {
                $this->line("  {$table}: {$deleted} records deleted");
            }
        }

        // Rollups are tiny, so they're retained far longer than raw telemetry —
        // pruning raw aggressively while keeping rollups gives long-range trend
        // charts without storing raw rows. Every rollup table is pruned on its
        // own bucket_start, with a separate (longer) retention.
        $rollupDays = (int) ($this->option('rollup-days') ?? config('nightowl.database.rollup_retention_days', 90));
        $rollupCutoff = now()->utc()->subDays($rollupDays)->toDateTimeString();
        $schema = $conn->getSchemaBuilder();

        $rollupTables = [];
        foreach (RollupSpecs::all() as $spec) {
            $rollupTables[$spec->table] = true;
        }

        foreach (array_keys($rollupTables) as $table) {
            if (! $schema->hasTable($table)) {
                continue;
            }

            $deleted = $conn->table($table)->where('bucket_start', '<', $rollupCutoff)->delete();
            $totalDeleted += $deleted;

            if ($deleted > 0) {
                $this->line("  {$table}: {$deleted} records deleted (older than {$rollupDays} days)");
            }
        }

        $this->newLine();
        $this->info("Pruned {$totalDeleted} records total.");

        return self::SUCCESS;
    }
}
