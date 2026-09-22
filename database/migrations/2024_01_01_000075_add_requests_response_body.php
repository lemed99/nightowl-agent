<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * The captured response body of a request (NIGHTOWL_CAPTURE_RESPONSE_BODY),
     * deflated like every other v2 blob.
     *
     * Nightwatch has no notion of a response body, so the column exists only on
     * the v2 table: v1 is the legacy family the drain writes solely under
     * NIGHTOWL_STORAGE_V2=false, and a v1 drain simply does not store bodies.
     *
     * The column is added unconditionally but written only when the app has
     * capture turned on, so this is one null column on an install that never
     * does. The drain probes for it before naming it (a batch carrying bodies
     * against an un-migrated tenant writes without them rather than 42703-ing
     * the whole batch), and so does the API before reading it.
     *
     * nightowl_requests_v2 is daily-partitioned; ADD COLUMN on the parent
     * propagates to every child and to partitions created later, and takes only
     * a brief ACCESS EXCLUSIVE lock (no table rewrite — the added column is
     * nullable with no default).
     */
    public function up(): void
    {
        $schema = Schema::connection($this->connection);

        if (! $schema->hasTable('nightowl_requests_v2')) {
            return; // pre-v2 tenant — nothing to extend
        }
        if ($schema->hasColumn('nightowl_requests_v2', 'response_z')) {
            return;
        }

        DB::connection($this->connection)->statement(
            'ALTER TABLE nightowl_requests_v2 ADD COLUMN response_z bytea'
        );
    }

    public function down(): void
    {
        DB::connection($this->connection)->statement(
            'ALTER TABLE nightowl_requests_v2 DROP COLUMN IF EXISTS response_z'
        );
    }
};
