<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    private const TABLES = [
        'nightowl_requests',
        'nightowl_commands',
        'nightowl_jobs',
        'nightowl_scheduled_tasks',
    ];

    private const COLUMNS = [
        'lazy_loads',
        'hydrated_models',
        'files_read',
        'files_written',
    ];

    public function up(): void
    {
        $schema = Schema::connection($this->connection);

        foreach (self::TABLES as $table) {
            if (! $schema->hasTable($table)) {
                continue;
            }
            $existing = array_values(array_filter(
                self::COLUMNS,
                fn ($col) => $schema->hasColumn($table, $col),
            ));
            if ($existing === []) {
                continue;
            }
            $schema->table($table, function (Blueprint $t) use ($existing) {
                $t->dropColumn($existing);
            });
        }
    }

    public function down(): void
    {
        $schema = Schema::connection($this->connection);

        foreach (self::TABLES as $table) {
            if (! $schema->hasTable($table)) {
                continue;
            }
            $missing = array_values(array_filter(
                self::COLUMNS,
                fn ($col) => ! $schema->hasColumn($table, $col),
            ));
            if ($missing === []) {
                continue;
            }
            $schema->table($table, function (Blueprint $t) use ($missing) {
                foreach ($missing as $col) {
                    $t->integer($col)->default(0);
                }
            });
        }
    }
};
