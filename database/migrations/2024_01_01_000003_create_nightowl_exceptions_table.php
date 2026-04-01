<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    public function up(): void
    {
        Schema::connection($this->connection)->create('nightowl_exceptions', function (Blueprint $table) {
            $table->id();
            $table->smallInteger('v')->nullable();
            $table->string('trace_id');
            $table->string('timestamp')->nullable();
            $table->string('deploy')->nullable();
            $table->string('server')->nullable();
            $table->string('group_hash')->nullable();

            // Execution context
            $table->string('execution_source')->nullable();
            $table->string('execution_id')->nullable();
            $table->string('execution_stage')->nullable();
            $table->string('execution_preview')->nullable();
            $table->string('user_id')->nullable();

            // Exception data
            $table->string('class');
            $table->text('message')->nullable();
            $table->string('code')->nullable();
            $table->string('file')->nullable();
            $table->integer('line')->nullable();
            $table->text('trace')->nullable();
            $table->string('php_version')->nullable();
            $table->string('laravel_version')->nullable();
            $table->boolean('handled')->default(false);

            // Grouping
            $table->string('fingerprint');

            $table->timestamp('created_at')->useCurrent();

            $table->index('trace_id');
            $table->index('fingerprint');
            $table->index('class');
            $table->index('timestamp');
            $table->index('execution_id');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_exceptions');
    }
};
