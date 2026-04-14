<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    public function up(): void
    {
        Schema::connection($this->connection)->table('nightowl_issue_activity', function (Blueprint $table) {
            $table->string('actor_type', 20)->default('user')->after('user_name');
            $table->json('actor_meta')->nullable()->after('actor_type');

            $table->index(['issue_id', 'actor_type']);
        });

        Schema::connection($this->connection)->table('nightowl_issue_comments', function (Blueprint $table) {
            $table->string('actor_type', 20)->default('user')->after('user_email');
            $table->json('actor_meta')->nullable()->after('actor_type');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->table('nightowl_issue_activity', function (Blueprint $table) {
            $table->dropIndex(['issue_id', 'actor_type']);
            $table->dropColumn(['actor_type', 'actor_meta']);
        });

        Schema::connection($this->connection)->table('nightowl_issue_comments', function (Blueprint $table) {
            $table->dropColumn(['actor_type', 'actor_meta']);
        });
    }
};
