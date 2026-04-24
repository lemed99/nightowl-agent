<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    public function up(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_alerts');
    }

    public function down(): void
    {
        // Intentionally empty — the original create migration is retained in the
        // history, but the table is considered removed. Re-running the create
        // would require restoring that migration first.
    }
};
