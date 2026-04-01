<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    public function up(): void
    {
        $schema = Schema::connection($this->connection);

        // Helper: add column only if it doesn't already exist (idempotent for fresh installs)
        $addIfMissing = function (string $table, string $column, callable $define) use ($schema) {
            if (! $schema->hasColumn($table, $column)) {
                $schema->table($table, function (Blueprint $t) use ($column, $define) {
                    $define($t);
                });
            }
        };

        // Add sensor version (v) to all tables
        $allTables = [
            'nightowl_requests', 'nightowl_queries', 'nightowl_exceptions',
            'nightowl_commands', 'nightowl_jobs', 'nightowl_cache_events',
            'nightowl_mail', 'nightowl_notifications', 'nightowl_outgoing_requests',
            'nightowl_scheduled_tasks', 'nightowl_logs', 'nightowl_users',
        ];

        foreach ($allTables as $tableName) {
            $addIfMissing($tableName, 'v', fn (Blueprint $t) => $t->smallInteger('v')->nullable());
        }

        $addIfMissing('nightowl_queries', 'execution_preview', fn (Blueprint $t) => $t->string('execution_preview')->nullable());

        $addIfMissing('nightowl_exceptions', 'group_hash', fn (Blueprint $t) => $t->string('group_hash')->nullable());
        $addIfMissing('nightowl_exceptions', 'execution_preview', fn (Blueprint $t) => $t->string('execution_preview')->nullable());

        $addIfMissing('nightowl_cache_events', 'group_hash', fn (Blueprint $t) => $t->string('group_hash')->nullable());
        $addIfMissing('nightowl_cache_events', 'execution_preview', fn (Blueprint $t) => $t->string('execution_preview')->nullable());

        $addIfMissing('nightowl_commands', 'class', fn (Blueprint $t) => $t->string('class')->nullable());
        $addIfMissing('nightowl_commands', 'name', fn (Blueprint $t) => $t->string('name')->nullable());
        $addIfMissing('nightowl_commands', 'bootstrap', fn (Blueprint $t) => $t->integer('bootstrap')->nullable());
        $addIfMissing('nightowl_commands', 'action', fn (Blueprint $t) => $t->integer('action')->nullable());
        $addIfMissing('nightowl_commands', 'terminating', fn (Blueprint $t) => $t->integer('terminating')->nullable());
        $addIfMissing('nightowl_commands', 'context', fn (Blueprint $t) => $t->text('context')->nullable());

        $addIfMissing('nightowl_jobs', 'job_id', fn (Blueprint $t) => $t->string('job_id')->nullable());
        $addIfMissing('nightowl_jobs', 'attempt_id', fn (Blueprint $t) => $t->string('attempt_id')->nullable());
        $addIfMissing('nightowl_jobs', 'attempt', fn (Blueprint $t) => $t->integer('attempt')->nullable());
        $addIfMissing('nightowl_jobs', 'execution_stage', fn (Blueprint $t) => $t->string('execution_stage')->nullable());
        $addIfMissing('nightowl_jobs', 'execution_preview', fn (Blueprint $t) => $t->string('execution_preview')->nullable());
        $addIfMissing('nightowl_jobs', 'context', fn (Blueprint $t) => $t->text('context')->nullable());

        $addIfMissing('nightowl_mail', 'group_hash', fn (Blueprint $t) => $t->string('group_hash')->nullable());
        $addIfMissing('nightowl_mail', 'execution_preview', fn (Blueprint $t) => $t->string('execution_preview')->nullable());
        $addIfMissing('nightowl_mail', 'cc', fn (Blueprint $t) => $t->integer('cc')->default(0));
        $addIfMissing('nightowl_mail', 'bcc', fn (Blueprint $t) => $t->integer('bcc')->default(0));
        $addIfMissing('nightowl_mail', 'attachments', fn (Blueprint $t) => $t->integer('attachments')->default(0));
        $addIfMissing('nightowl_mail', 'failed', fn (Blueprint $t) => $t->boolean('failed')->default(false));

        $addIfMissing('nightowl_notifications', 'group_hash', fn (Blueprint $t) => $t->string('group_hash')->nullable());
        $addIfMissing('nightowl_notifications', 'execution_preview', fn (Blueprint $t) => $t->string('execution_preview')->nullable());
        $addIfMissing('nightowl_notifications', 'failed', fn (Blueprint $t) => $t->boolean('failed')->default(false));

        $addIfMissing('nightowl_outgoing_requests', 'group_hash', fn (Blueprint $t) => $t->string('group_hash')->nullable());
        $addIfMissing('nightowl_outgoing_requests', 'host', fn (Blueprint $t) => $t->string('host')->nullable());
        $addIfMissing('nightowl_outgoing_requests', 'execution_preview', fn (Blueprint $t) => $t->string('execution_preview')->nullable());

        $addIfMissing('nightowl_scheduled_tasks', 'timezone', fn (Blueprint $t) => $t->string('timezone')->nullable());
        $addIfMissing('nightowl_scheduled_tasks', 'repeat_seconds', fn (Blueprint $t) => $t->integer('repeat_seconds')->default(0));
        $addIfMissing('nightowl_scheduled_tasks', 'without_overlapping', fn (Blueprint $t) => $t->boolean('without_overlapping')->default(false));
        $addIfMissing('nightowl_scheduled_tasks', 'on_one_server', fn (Blueprint $t) => $t->boolean('on_one_server')->default(false));
        $addIfMissing('nightowl_scheduled_tasks', 'run_in_background', fn (Blueprint $t) => $t->boolean('run_in_background')->default(false));
        $addIfMissing('nightowl_scheduled_tasks', 'even_in_maintenance_mode', fn (Blueprint $t) => $t->boolean('even_in_maintenance_mode')->default(false));
        $addIfMissing('nightowl_scheduled_tasks', 'context', fn (Blueprint $t) => $t->text('context')->nullable());

        $addIfMissing('nightowl_logs', 'extra', fn (Blueprint $t) => $t->text('extra')->nullable());
        $addIfMissing('nightowl_logs', 'execution_preview', fn (Blueprint $t) => $t->string('execution_preview')->nullable());

        $addIfMissing('nightowl_users', 'timestamp', fn (Blueprint $t) => $t->string('timestamp')->nullable());
    }

    public function down(): void
    {
        $schema = Schema::connection($this->connection);

        $dropIfExists = function (string $table, array $columns) use ($schema) {
            $existing = array_filter($columns, fn ($col) => $schema->hasColumn($table, $col));
            if (! empty($existing)) {
                $schema->table($table, function (Blueprint $t) use ($existing) {
                    $t->dropColumn($existing);
                });
            }
        };

        $allTables = [
            'nightowl_requests', 'nightowl_queries', 'nightowl_exceptions',
            'nightowl_commands', 'nightowl_jobs', 'nightowl_cache_events',
            'nightowl_mail', 'nightowl_notifications', 'nightowl_outgoing_requests',
            'nightowl_scheduled_tasks', 'nightowl_logs', 'nightowl_users',
        ];

        foreach ($allTables as $tableName) {
            $dropIfExists($tableName, ['v']);
        }

        $dropIfExists('nightowl_queries', ['execution_preview']);
        $dropIfExists('nightowl_exceptions', ['group_hash', 'execution_preview']);
        $dropIfExists('nightowl_cache_events', ['group_hash', 'execution_preview']);
        $dropIfExists('nightowl_commands', ['class', 'name', 'bootstrap', 'action', 'terminating', 'context']);
        $dropIfExists('nightowl_jobs', ['job_id', 'attempt_id', 'attempt', 'execution_stage', 'execution_preview', 'context']);
        $dropIfExists('nightowl_mail', ['group_hash', 'execution_preview', 'cc', 'bcc', 'attachments', 'failed']);
        $dropIfExists('nightowl_notifications', ['group_hash', 'execution_preview', 'failed']);
        $dropIfExists('nightowl_outgoing_requests', ['group_hash', 'host', 'execution_preview']);
        $dropIfExists('nightowl_scheduled_tasks', ['timezone', 'repeat_seconds', 'without_overlapping', 'on_one_server', 'run_in_background', 'even_in_maintenance_mode', 'context']);
        $dropIfExists('nightowl_logs', ['extra', 'execution_preview']);
        $dropIfExists('nightowl_users', ['timestamp']);
    }
};
