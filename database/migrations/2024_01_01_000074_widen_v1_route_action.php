<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Widen nightowl_requests.route_action (v1) from varchar(255) to text.
     *
     * 000073 widened the v2 dictionary's `action` to text; the v1 column stayed
     * at 255, so a tenant still writing v1 clipped actions mid-class-name where
     * a v2 tenant stored them whole. Nothing collapsed Livewire actions before
     * this release, so an over-255 v1 action was already routine; collapsing
     * makes them shorter, not longer. The widening is for parity with v2, and
     * for the promise that an ignore pattern matches what the dashboard shows —
     * a v1 dashboard showed the 255-byte value while matching saw the whole one.
     *
     * varchar(n) → text is binary-coercible: no rewrite, no index rebuild. The
     * column is not indexed (a guard below refuses if that ever changes, since a
     * text column in a btree would trade a 22001 for a 54000).
     */
    public function up(): void
    {
        $schema = Schema::connection($this->connection);
        $conn = DB::connection($this->connection);

        if (! $schema->hasTable('nightowl_requests') || ! $schema->hasColumn('nightowl_requests', 'route_action')) {
            return; // v1 family retired on this tenant, or never created
        }

        $col = $conn->selectOne(
            "SELECT format_type(a.atttypid, a.atttypmod) AS type,
                    EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = a.attrelid AND a.attnum = ANY (i.indkey)) AS indexed
             FROM pg_attribute a
             WHERE a.attrelid = to_regclass('nightowl_requests') AND a.attname = 'route_action'"
        );

        if ($col === null || $col->type === 'text') {
            return;
        }
        if ($col->indexed) {
            return; // a btree entry is capped at ~2704 bytes; widening would move the failure, not remove it
        }

        $conn->statement('ALTER TABLE nightowl_requests ALTER COLUMN route_action TYPE text');
    }

    public function down(): void
    {
        $schema = Schema::connection($this->connection);
        if (! $schema->hasTable('nightowl_requests') || ! $schema->hasColumn('nightowl_requests', 'route_action')) {
            return;
        }

        DB::connection($this->connection)->statement(
            'ALTER TABLE nightowl_requests ALTER COLUMN route_action TYPE varchar(255) USING left(route_action, 255)'
        );
    }
};
