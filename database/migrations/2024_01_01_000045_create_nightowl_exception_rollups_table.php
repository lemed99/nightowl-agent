<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Per-minute pre-aggregated summaries of nightowl_exceptions keyed by
     * FINGERPRINT (the exception group the Exceptions section + dashboard group by),
     * NOT route group_hash. Additive call_count (occurrences) + handled/unhandled
     * bands. Count-only: exceptions carry no duration.
     *
     * The list's class/message/file/line and affected_users (COUNT DISTINCT user_id,
     * non-additive) stay a bounded raw enrichment on the page's ≤50 fingerprints
     * (served by the new (fingerprint, created_at) index) — the rollup only carries
     * the additive occurrence/handled counts + supports COUNT(DISTINCT fingerprint)
     * for the group totals. PK collapses on the '' environment/fingerprint sentinels.
     */
    public function up(): void
    {
        if (Schema::connection($this->connection)->hasTable('nightowl_exception_rollups')) {
            return;
        }

        Schema::connection($this->connection)->create('nightowl_exception_rollups', function (Blueprint $table): void {
            $table->string('fingerprint')->default('');
            $table->timestamp('bucket_start');
            $table->string('environment')->default('');

            $table->bigInteger('call_count')->default(0);
            $table->bigInteger('handled_count')->default(0);
            $table->bigInteger('unhandled_count')->default(0);

            $table->primary(['fingerprint', 'bucket_start', 'environment'], 'nightowl_exception_rollups_pk');
            $table->index('bucket_start', 'nightowl_exception_rollups_bucket_idx');
        });
    }

    public function down(): void
    {
        Schema::connection($this->connection)->dropIfExists('nightowl_exception_rollups');
    }
};
