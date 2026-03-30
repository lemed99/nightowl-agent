<?php

namespace NightOwl\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class ClearCommand extends Command
{
    protected $signature = 'nightowl:clear
        {--force : Skip confirmation}';

    protected $description = 'Clear all NightOwl monitoring data';

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
        'nightowl_alerts',
    ];

    public function handle(): int
    {
        if (! $this->option('force') && ! $this->confirm('This will delete ALL NightOwl monitoring data. Continue?')) {
            return self::SUCCESS;
        }

        $conn = DB::connection('nightowl');

        foreach (self::TABLES as $table) {
            $conn->table($table)->truncate();
        }

        $this->info('All NightOwl data cleared.');

        return self::SUCCESS;
    }
}
