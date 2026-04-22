<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Telemetry tables that need an environment column.
     *
     * Excludes nightowl_users (dimension table, no deploy/environment).
     * Includes nightowl_issues so the unique dedup key can pivot to environment.
     */
    private array $telemetryTables = [
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
        $schema = Schema::connection($this->connection);

        foreach ($this->telemetryTables as $table) {
            $schema->table($table, function (Blueprint $t) {
                $t->string('environment')->nullable()->after('deploy');
                $t->index('environment');
            });
        }

        $schema->table('nightowl_issues', function (Blueprint $t) {
            $t->string('environment')->nullable()->after('deploy');
            $t->index('environment');
        });

        // Backfill existing rows from the customer's APP_ENV. This is the
        // semantically correct value for historical telemetry since the column
        // represents "where was this running" — which hasn't changed. Deploy
        // stays populated separately for release tracking.
        $env = config('app.env', 'production');
        $connection = DB::connection($this->connection);

        foreach ([...$this->telemetryTables, 'nightowl_issues'] as $table) {
            $connection->table($table)
                ->whereNull('environment')
                ->update(['environment' => $env]);
        }

        // The old unique key was (group_hash, type, deploy). The same fingerprint
        // could legitimately exist across deploys with different statuses (e.g.
        // resolved under deploy=abc, open under deploy=def). Backfilling a single
        // environment value collapses those into duplicates that would violate
        // the new unique. Keep the most-recent row per (group_hash, type, env) —
        // matches runtime upsert semantics (a new occurrence reopens/updates the
        // existing row) — and delete the rest.
        $connection->statement("
            DELETE FROM nightowl_issues
            WHERE id IN (
                SELECT id FROM (
                    SELECT id, ROW_NUMBER() OVER (
                        PARTITION BY group_hash, type, environment
                        ORDER BY last_seen_at DESC NULLS LAST, id DESC
                    ) AS rn
                    FROM nightowl_issues
                ) ranked
                WHERE ranked.rn > 1
            )
        ");

        $schema->table('nightowl_issues', function (Blueprint $t) {
            $t->dropUnique(['group_hash', 'type', 'deploy']);
            $t->unique(['group_hash', 'type', 'environment']);
        });
    }

    public function down(): void
    {
        $schema = Schema::connection($this->connection);

        $schema->table('nightowl_issues', function (Blueprint $t) {
            $t->dropUnique(['group_hash', 'type', 'environment']);
            $t->unique(['group_hash', 'type', 'deploy']);
            $t->dropIndex(['environment']);
            $t->dropColumn('environment');
        });

        foreach ($this->telemetryTables as $table) {
            $schema->table($table, function (Blueprint $t) {
                $t->dropIndex(['environment']);
                $t->dropColumn('environment');
            });
        }
    }
};
