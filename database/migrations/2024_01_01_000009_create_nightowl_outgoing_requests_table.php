<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    public function up(): void
    {
        Schema::connection($this->connection)->create('nightowl_outgoing_requests', function (Blueprint $table) {
            $table->id();
            $table->smallInteger('v')->nullable();
            $table->string('trace_id');
            $table->string('timestamp')->nullable();
            $table->string('deploy')->nullable();
            $table->string('server')->nullable();
            $table->string('group_hash')->nullable();
            $table->string('execution_source')->nullable();
            $table->string('execution_id')->nullable();
            $table->string('execution_stage')->nullable();
            $table->string('execution_preview')->nullable();
            $table->string('user_id')->nullable();

            $table->string('host')->nullable();
            $table->string('method');
            $table->text('url');
            $table->integer('status_code')->nullable();
            $table->integer('duration')->nullable();
            $table->integer('request_size')->nullable();
            $table->integer('response_size')->nullable();
            $table->text('request_headers')->nullable();

            $table->timestamp('created_at')->useCurrent();

            $table->index('trace_id');
            $table->index('execution_id');
            $table->index('timestamp');
            $table->index('status_code');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_outgoing_requests');
    }
};
