<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Heal a v2 id-sequence fence that has already drifted.
     *
     * 000067 set each v2 sequence to MAX(v1.id)+1 INSIDE its table-creation
     * guard, so it ran exactly once per tenant, at creation. Every v1 insert
     * after that instant overtakes the frozen value and mints ids the v2 family
     * hands out again — a mixed fleet (one app server still on a 1.x agent
     * draining into v1 while another drains into v2), or a
     * NIGHTOWL_STORAGE_V2=false excursion and flip back. The API pages the
     * v1+v2 union on (created_at, id) and a COPY batch stamps ONE shared
     * created_at, so id is the only tiebreak: duplicate tuples make "Next" skip
     * or repeat rows.
     *
     * This file heals tenants already drifted. It is NOT the whole fix —
     * nightowl:migrate re-applies the fence on EVERY run (including the
     * daemon's boot auto-migrate), which is what closes the window while a
     * mixed fleet keeps writing v1. A once-applied migration cannot.
     *
     * Deliberately NOT self-contained, unlike 000058: the routine lives in
     * NightOwl\Support\V2SequenceFence, the single implementation
     * nightowl:migrate calls too — a sequence-arithmetic rule that must agree in
     * both places is worse duplicated than skipped where the class is absent.
     * This file is shared into nightowl-api, which does not autoload the agent's
     * classes and only ever runs these migrations against FRESH demo/test tenant
     * schemas, where 000067's creation-time fence is exact and no v1 writer
     * exists to overtake it. The agent — the only thing that ever writes v1 —
     * always has the class.
     *
     * $withinTransaction = false because there is nothing to make atomic (no
     * DDL, and setval is not rolled back anyway) and because the routine
     * swallows per-table failures: inside a migrator-wrapped transaction a
     * swallowed error only leaves an aborted transaction for the migrator to
     * COMMIT, turning an advisory skip into a failed migrate.
     */
    public $withinTransaction = false;

    public function up(): void
    {
        if (! class_exists(\NightOwl\Support\V2SequenceFence::class)) {
            return;
        }

        try {
            \NightOwl\Support\V2SequenceFence::apply(
                DB::connection($this->connection)->getPdo()
            );
        } catch (\Throwable $e) {
            // Never block a schema sync (or the boot-migrate behind it) over an
            // advisory fence. nightowl:migrate re-runs this on the next deploy
            // and reports per-table failures, which a migration cannot.
            error_log('[NightOwl] storage-v2 id sequence fence skipped: '.$e->getMessage());
        }
    }

    public function down(): void
    {
        // Nothing to undo: setval moved a generator forward and the ids it
        // skipped were never rows. Moving it back would re-create the collision.
    }
};
