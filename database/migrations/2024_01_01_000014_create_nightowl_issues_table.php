<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    public function up(): void
    {
        Schema::connection($this->connection)->create('nightowl_issues', function (Blueprint $table) {
            $table->id();
            $table->string('type'); // exception, performance
            $table->string('status')->default('open'); // open, resolved, ignored
            $table->string('priority')->nullable(); // urgent, high, medium, low
            $table->string('exception_class')->nullable();
            $table->text('exception_message')->nullable();
            $table->string('group_hash')->nullable(); // link to exception fingerprint
            $table->timestamp('first_seen_at')->nullable();
            $table->timestamp('last_seen_at')->nullable();
            $table->unsignedInteger('occurrences_count')->default(0);
            $table->unsignedInteger('users_count')->default(0);
            $table->string('assigned_to')->nullable();
            $table->timestamps();

            $table->index('type');
            $table->index('status');
            $table->index('group_hash');
            $table->index('assigned_to');
            $table->index('last_seen_at');
            $table->index('first_seen_at');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_issues');
    }
};
