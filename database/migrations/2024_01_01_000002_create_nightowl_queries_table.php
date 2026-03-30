<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    public function up(): void
    {
        Schema::connection($this->connection)->create('nightowl_queries', function (Blueprint $table) {
            $table->id();
            $table->string('trace_id');
            $table->string('timestamp')->nullable();
            $table->string('deploy')->nullable();
            $table->string('server')->nullable();
            $table->string('group_hash')->nullable();

            // Execution context
            $table->string('execution_source')->nullable();
            $table->string('execution_id')->nullable();
            $table->string('execution_stage')->nullable();
            $table->string('user_id')->nullable();

            // Query data
            $table->text('sql_query');
            $table->string('file')->nullable();
            $table->integer('line')->nullable();
            $table->integer('duration')->nullable();
            $table->string('connection')->nullable();
            $table->string('connection_type')->nullable();

            $table->timestamp('created_at')->useCurrent();

            $table->index('trace_id');
            $table->index('execution_id');
            $table->index('timestamp');
            $table->index('duration');
            $table->index('group_hash');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_queries');
    }
};
