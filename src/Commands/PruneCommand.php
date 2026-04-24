<?php

namespace NightOwl\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class PruneCommand extends Command
{
    protected $signature = 'nightowl:prune
        {--days= : Number of days to retain data}';

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
        $days = (int) ($this->option('days') ?? config('nightowl.database.retention_days', 14));
        $cutoff = now()->subDays($days)->toDateTimeString();
        $conn = DB::connection('nightowl');

        $this->info("Pruning records older than {$days} days (before {$cutoff})...");

        $totalDeleted = 0;

        foreach (self::TABLES as $table) {
            $deleted = $conn->table($table)->where('created_at', '<', $cutoff)->delete();
            $totalDeleted += $deleted;

            if ($deleted > 0) {
                $this->line("  {$table}: {$deleted} records deleted");
            }
        }

        $this->newLine();
        $this->info("Pruned {$totalDeleted} records total.");

        return self::SUCCESS;
    }
}
