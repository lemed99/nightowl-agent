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

        if (! $schema->hasColumn('nightowl_issues', 'subtype')) {
            $schema->table('nightowl_issues', function (Blueprint $table) {
                $table->string('subtype')->nullable()->after('type');
                $table->index('subtype');
            });
        }
    }

    public function down(): void
    {
        $schema = Schema::connection($this->connection);

        if ($schema->hasColumn('nightowl_issues', 'subtype')) {
            $schema->table('nightowl_issues', function (Blueprint $table) {
                $table->dropIndex(['subtype']);
                $table->dropColumn('subtype');
            });
        }
    }
};
