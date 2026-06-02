<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Every telemetry table declared trace_id as NOT NULL, but the agent
     * writes `$r['trace_id'] ?? null` for all of them. Any record emitted
     * outside a request trace (logs from artisan commands, boot-time events,
     * queue workers before trace context exists) carries a null trace_id and
     * makes the whole COPY batch fail the NOT NULL constraint — stalling the
     * drain. Make trace_id nullable across all telemetry tables to match what
     * the writer actually sends.
     */
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

    public function up(): void
    {
        foreach (self::TABLES as $table) {
            Schema::connection($this->connection)->table($table, function (Blueprint $t) {
                $t->string('trace_id')->nullable()->change();
            });
        }
    }

    public function down(): void
    {
        foreach (self::TABLES as $table) {
            Schema::connection($this->connection)->table($table, function (Blueprint $t) {
                $t->string('trace_id')->nullable(false)->change();
            });
        }
    }
};
