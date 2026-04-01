<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    public function up(): void
    {
        Schema::connection('nightowl')->create('nightowl_users', function (Blueprint $table) {
            $table->string('user_id')->primary();
            $table->smallInteger('v')->nullable();
            $table->string('name')->nullable();
            $table->string('email')->nullable();
            $table->string('timestamp')->nullable();
            $table->timestamp('created_at')->useCurrent();
            $table->timestamp('updated_at')->useCurrent();
        });
    }

    public function down(): void
    {
        Schema::connection('nightowl')->dropIfExists('nightowl_users');
    }
};
