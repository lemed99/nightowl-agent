<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    public function up(): void
    {
        Schema::connection($this->connection)->create('nightowl_commands', function (Blueprint $table) {
            $table->id();
            $table->string('trace_id');
            $table->string('timestamp')->nullable();
            $table->string('deploy')->nullable();
            $table->string('server')->nullable();
            $table->string('group_hash')->nullable();
            $table->string('user_id')->nullable();

            $table->string('command');
            $table->integer('exit_code')->nullable();
            $table->integer('duration')->nullable();

            // Child event counts
            $table->integer('exceptions')->default(0);
            $table->integer('logs')->default(0);
            $table->integer('queries')->default(0);
            $table->integer('lazy_loads')->default(0);
            $table->integer('jobs_queued')->default(0);
            $table->integer('mail')->default(0);
            $table->integer('notifications')->default(0);
            $table->integer('outgoing_requests')->default(0);
            $table->integer('files_read')->default(0);
            $table->integer('files_written')->default(0);
            $table->integer('cache_events')->default(0);
            $table->integer('hydrated_models')->default(0);
            $table->integer('peak_memory_usage')->nullable();
            $table->text('exception_preview')->nullable();

            $table->timestamp('created_at')->useCurrent();

            $table->index('trace_id');
            $table->index('timestamp');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_commands');
    }
};
