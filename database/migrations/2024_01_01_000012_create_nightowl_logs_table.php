<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    public function up(): void
    {
        Schema::connection('nightowl')->create('nightowl_logs', function (Blueprint $table) {
            $table->id();
            $table->smallInteger('v')->nullable();
            $table->string('trace_id');
            $table->string('timestamp')->nullable();
            $table->string('deploy')->nullable();
            $table->string('server')->nullable();
            $table->string('execution_source')->nullable();
            $table->string('execution_id')->nullable();
            $table->string('execution_stage')->nullable();
            $table->string('execution_preview')->nullable();
            $table->string('user_id')->nullable();
            $table->string('level')->default('info');
            $table->text('message')->nullable();
            $table->text('context')->nullable();
            $table->text('extra')->nullable();
            $table->string('channel')->nullable();
            $table->string('created_at')->nullable();

            $table->index('trace_id');
            $table->index('execution_id');
            $table->index('level');
            $table->index('channel');
            $table->index('created_at');
        });
    }

    public function down(): void
    {
        Schema::connection('nightowl')->dropIfExists('nightowl_logs');
    }
};
