<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Tenant list endpoints filter by `created_at > $since` on every request
     * (HasTimeRangeFilter::applyTimeFilter defaults to `created_at`). Without
     * these indexes Postgres falls back to seq scans on tables that grow into
     * the millions of rows.
     */
    private const TABLES = [
        'nightowl_requests',
        'nightowl_jobs',
        'nightowl_exceptions',
        'nightowl_queries',
        'nightowl_commands',
        'nightowl_cache_events',
        'nightowl_mail',
        'nightowl_outgoing_requests',
        'nightowl_scheduled_tasks',
        'nightowl_notifications',
    ];

    public function up(): void
    {
        foreach (self::TABLES as $table) {
            if (! Schema::connection($this->connection)->hasTable($table)) {
                continue;
            }

            Schema::connection($this->connection)->table($table, function (Blueprint $blueprint) use ($table): void {
                $blueprint->index('created_at', $this->indexName($table));
            });
        }
    }

    public function down(): void
    {
        foreach (self::TABLES as $table) {
            if (! Schema::connection($this->connection)->hasTable($table)) {
                continue;
            }

            Schema::connection($this->connection)->table($table, function (Blueprint $blueprint) use ($table): void {
                $blueprint->dropIndex($this->indexName($table));
            });
        }
    }

    private function indexName(string $table): string
    {
        return $table.'_created_at_index';
    }
};
