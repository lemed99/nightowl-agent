<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    public function up(): void
    {
        Schema::connection('nightowl')->create('nightowl_reports', function (Blueprint $table) {
            $table->id();                                  // serial — tenant IDs are serial (cross-repo contract)
            $table->timestamp('period_start');
            $table->timestamp('period_end');
            $table->json('payload');                       // frozen aggregate snapshot
            $table->timestamp('created_at')->useCurrent(); // = generated_at
            $table->index('period_start');
        });
    }

    public function down(): void
    {
        Schema::connection('nightowl')->dropIfExists('nightowl_reports');
    }
};
