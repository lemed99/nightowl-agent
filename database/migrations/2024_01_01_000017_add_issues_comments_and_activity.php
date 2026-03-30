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
            $table->text('description')->nullable()->after('assigned_to');
        });

        Schema::connection($this->connection)->create('nightowl_issue_comments', function (Blueprint $table) {
            $table->id();
            $table->foreignId('issue_id')->constrained('nightowl_issues')->cascadeOnDelete();
            $table->unsignedBigInteger('user_id')->nullable();
            $table->string('user_name')->nullable();
            $table->string('user_email')->nullable();
            $table->text('body');
            $table->timestamps();

            $table->index('issue_id');
        });

        Schema::connection($this->connection)->create('nightowl_issue_activity', function (Blueprint $table) {
            $table->id();
            $table->foreignId('issue_id')->constrained('nightowl_issues')->cascadeOnDelete();
            $table->unsignedBigInteger('user_id')->nullable();
            $table->string('user_name')->nullable();
            $table->string('action', 50);
            $table->string('old_value', 255)->nullable();
            $table->string('new_value', 255)->nullable();
            $table->timestamp('created_at')->nullable();

            $table->index('issue_id');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_issue_activity');
        Schema::connection($this->connection)->dropIfExists('nightowl_issue_comments');

        Schema::connection($this->connection)->table('nightowl_issues', function (Blueprint $table) {
            $table->dropColumn('description');
        });
    }
};
