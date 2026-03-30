<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    public function up(): void
    {
        Schema::connection($this->connection)->create('nightowl_requests', function (Blueprint $table) {
            $table->id();
            $table->string('trace_id');
            $table->string('timestamp')->nullable();
            $table->string('deploy')->nullable();
            $table->string('server')->nullable();
            $table->string('group_hash')->nullable();
            $table->string('user_id')->nullable();

            // Request basics
            $table->string('method');
            $table->text('url');
            $table->string('route_name')->nullable();
            $table->text('route_methods')->nullable();
            $table->string('route_domain')->nullable();
            $table->string('route_path')->nullable();
            $table->string('route_action')->nullable();
            $table->string('ip')->nullable();
            $table->integer('duration')->nullable();
            $table->integer('status_code');
            $table->integer('request_size')->nullable();
            $table->integer('response_size')->nullable();

            // Execution stage timings (microseconds)
            $table->integer('bootstrap')->nullable();
            $table->integer('before_middleware')->nullable();
            $table->integer('action')->nullable();
            $table->integer('render')->nullable();
            $table->integer('after_middleware')->nullable();
            $table->integer('sending')->nullable();
            $table->integer('terminating')->nullable();

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

            // Context
            $table->text('exception_preview')->nullable();
            $table->text('context')->nullable();
            $table->text('headers')->nullable();
            $table->text('payload')->nullable();

            $table->timestamp('created_at')->useCurrent();

            $table->index('trace_id');
            $table->index('timestamp');
            $table->index('status_code');
            $table->index('duration');
            $table->index('group_hash');
            $table->index(['method', 'url'], 'idx_requests_method_url');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_requests');
    }
};
