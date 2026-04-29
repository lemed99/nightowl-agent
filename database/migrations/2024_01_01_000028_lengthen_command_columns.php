<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    public function up(): void
    {
        Schema::connection($this->connection)->table('nightowl_commands', function (Blueprint $table) {
            $table->text('command')->change();
        });

        Schema::connection($this->connection)->table('nightowl_scheduled_tasks', function (Blueprint $table) {
            $table->text('command')->change();
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->table('nightowl_commands', function (Blueprint $table) {
            $table->string('command', 255)->change();
        });

        Schema::connection($this->connection)->table('nightowl_scheduled_tasks', function (Blueprint $table) {
            $table->string('command', 255)->change();
        });
    }
};
