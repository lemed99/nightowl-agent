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
            $table->dropIndex(['group_hash']);
            $table->dropIndex(['type']);
            $table->unique(['group_hash', 'type']);
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->table('nightowl_issues', function (Blueprint $table) {
            $table->dropUnique(['group_hash', 'type']);
            $table->index('group_hash');
            $table->index('type');
        });
    }
};
