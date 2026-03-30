<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    public function up(): void
    {
        Schema::connection($this->connection)->create('nightowl_mail', function (Blueprint $table) {
            $table->id();
            $table->string('trace_id');
            $table->string('timestamp')->nullable();
            $table->string('deploy')->nullable();
            $table->string('server')->nullable();
            $table->string('execution_source')->nullable();
            $table->string('execution_id')->nullable();
            $table->string('execution_stage')->nullable();
            $table->string('user_id')->nullable();

            $table->string('mailer')->nullable();
            $table->text('recipients')->nullable();
            $table->string('subject')->nullable();
            $table->string('mailable')->nullable();
            $table->integer('duration')->nullable();
            $table->boolean('queued')->default(false);

            $table->timestamp('created_at')->useCurrent();

            $table->index('trace_id');
            $table->index('execution_id');
            $table->index('timestamp');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_mail');
    }
};
