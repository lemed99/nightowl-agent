<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

/**
 * The platform migrated user IDs to KSUIDs (strings). Tenant activity/comment
 * tables must accept those strings instead of UNSIGNED BIGINT.
 */
return new class extends Migration
{
    protected $connection = 'nightowl';

    public function up(): void
    {
        Schema::connection($this->connection)->table('nightowl_issue_activity', function (Blueprint $table) {
            $table->string('user_id')->nullable()->change();
        });

        Schema::connection($this->connection)->table('nightowl_issue_comments', function (Blueprint $table) {
            $table->string('user_id')->nullable()->change();
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->table('nightowl_issue_activity', function (Blueprint $table) {
            $table->unsignedBigInteger('user_id')->nullable()->change();
        });

        Schema::connection($this->connection)->table('nightowl_issue_comments', function (Blueprint $table) {
            $table->unsignedBigInteger('user_id')->nullable()->change();
        });
    }
};
