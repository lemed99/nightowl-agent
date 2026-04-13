<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    public function up(): void
    {
        Schema::connection($this->connection)->table('nightowl_issues', function (Blueprint $table) {
            $table->string('deploy')->nullable()->after('type');
            $table->dropUnique(['group_hash', 'type']);
            $table->unique(['group_hash', 'type', 'deploy']);
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->table('nightowl_issues', function (Blueprint $table) {
            $table->dropUnique(['group_hash', 'type', 'deploy']);
            $table->unique(['group_hash', 'type']);
            $table->dropColumn('deploy');
        });
    }
};
