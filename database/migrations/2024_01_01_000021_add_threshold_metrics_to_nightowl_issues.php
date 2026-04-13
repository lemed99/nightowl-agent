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

        $schema->table('nightowl_issues', function (Blueprint $table) use ($schema) {
            if (! $schema->hasColumn('nightowl_issues', 'threshold_ms')) {
                $table->unsignedInteger('threshold_ms')->nullable()->after('users_count');
            }
            if (! $schema->hasColumn('nightowl_issues', 'triggered_duration_ms')) {
                $table->unsignedInteger('triggered_duration_ms')->nullable()->after('threshold_ms');
            }
        });
    }

    public function down(): void
    {
        $schema = Schema::connection($this->connection);

        $schema->table('nightowl_issues', function (Blueprint $table) use ($schema) {
            if ($schema->hasColumn('nightowl_issues', 'triggered_duration_ms')) {
                $table->dropColumn('triggered_duration_ms');
            }
            if ($schema->hasColumn('nightowl_issues', 'threshold_ms')) {
                $table->dropColumn('threshold_ms');
            }
        });
    }
};
