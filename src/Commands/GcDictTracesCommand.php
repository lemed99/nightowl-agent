<?php

namespace NightOwl\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

/**
 * Garbage-collect the trace dictionary — the one dict that can be reclaimed.
 *
 * A row in nightowl_dict_trace holds one distinct deflated stack trace, and the
 * only thing that ever references it is a nightowl_exceptions_v2 row's
 * trace_ref. Exception rows age out at retention (nightowl:prune drops their
 * partitions), so once every exception referencing a trace is gone the trace is
 * dead weight. The other three dictionaries (string/sql/route) are NEVER GC'd:
 * their ids are unbounded-lifetime keys that any future telemetry row may point
 * at, so there is no "provably unreferenced" state for them.
 *
 * A trace is deleted only when BOTH hold:
 *   1. no nightowl_exceptions_v2 row references it (the anti-join), and
 *   2. its created_at is older than the quarantine window.
 *
 * Gate 2 is the race guard, and it is why this is safe without any lock. The
 * drain's dictionary warm pass (DictionaryCache) touches created_at = now()
 * on EVERY batch that references a trace, committing that touch in autocommit
 * BEFORE the exception row that points at it is written. So any trace an
 * in-flight batch is about to reference is already young, and the single-
 * statement DELETE re-checks created_at, whose predicate the concurrent touch
 * (created_at = now()) fails — the row is spared. And if this GC ever wins the
 * race and deletes a trace a batch still wants, that batch's warm INSERT ...
 * ON CONFLICT re-creates it (append-only) and hands the write the fresh id.
 * Either way no exception is ever left with a dangling trace_ref.
 *
 * On a tenant without the v2 twin (or without the dict) the command is a no-op.
 */
class GcDictTracesCommand extends Command
{
    protected $signature = 'nightowl:gc-dict-traces
        {--quarantine-days= : Delete only traces unreferenced AND older than this many days (defaults to config)}
        {--chunk=500 : Ids per DELETE statement}
        {--dry-run : Report how many traces would be reclaimed without deleting}';

    protected $description = 'Reclaim orphaned rows from the storage-v2 trace dictionary';

    public function handle(): int
    {
        $conn = DB::connection('nightowl');
        $pdo = $conn->getPdo();

        // Both relations are search_path-relative (schema-scoped tenants exist),
        // so probe with to_regclass and no `public.` prefix — matching every
        // other v2 existence check in the agent.
        $traceExists = (bool) $pdo->query("SELECT to_regclass('nightowl_dict_trace') IS NOT NULL AS e")->fetchColumn();
        $excExists = (bool) $pdo->query("SELECT to_regclass('nightowl_exceptions_v2') IS NOT NULL AS e")->fetchColumn();
        if (! $traceExists || ! $excExists) {
            $this->info('nightowl_dict_trace / nightowl_exceptions_v2 not present — nothing to GC.');

            return self::SUCCESS;
        }

        // Pre-000068 tenants (dict without the created_at clock) cannot be GC'd
        // safely: without the touch, an actively-referenced trace has no "young"
        // signal, so skip until the migration has run. Probe pg_attribute off
        // the resolved relation (to_regclass respects search_path) — not
        // information_schema.columns, whose table_name filter is schema-blind and
        // would read a same-name table in another schema on a scoped tenant.
        $hasClock = (bool) $pdo->query(
            "SELECT EXISTS (
                SELECT 1 FROM pg_attribute
                WHERE attrelid = to_regclass('nightowl_dict_trace')
                  AND attname = 'created_at' AND NOT attisdropped
            ) AS e"
        )->fetchColumn();
        if (! $hasClock) {
            $this->warn('nightowl_dict_trace.created_at is absent (migration 000068 pending) — skipping GC.');

            return self::SUCCESS;
        }

        $days = (int) ($this->option('quarantine-days')
            ?? config('nightowl.dict_trace_gc.quarantine_days', 7));
        $days = max(0, $days);
        // created_at is timestamptz written with now(). The cutoff MUST carry an
        // explicit offset: comparing a timestamptz against a BARE string literal
        // makes Postgres parse that literal in the session TimeZone GUC (a BYO
        // Postgres is rarely UTC), which would shift the cutoff by the offset.
        // Formatting with +00:00 pins the comparison to UTC on any session.
        $cutoff = now()->utc()->subDays($days)->format('Y-m-d H:i:sP');

        // ONE anti-join scan of exceptions_v2 collects the candidates. The dict
        // is small (distinct traces), so the candidate set is tiny even when
        // exceptions_v2 is large; the chunked DELETE below never touches
        // exceptions_v2 again — its created_at re-check alone is the race guard.
        $candidates = $pdo->prepare(
            'SELECT d.id FROM nightowl_dict_trace d
             WHERE d.created_at < ?
               AND NOT EXISTS (
                   SELECT 1 FROM nightowl_exceptions_v2 e WHERE e.trace_ref = d.id
               )'
        );
        $candidates->execute([$cutoff]);
        $ids = array_map('intval', $candidates->fetchAll(\PDO::FETCH_COLUMN));

        if ($ids === []) {
            $this->info("No orphaned traces older than {$days} days.");

            return self::SUCCESS;
        }

        if ($this->option('dry-run')) {
            $this->info(count($ids)." orphaned trace(s) older than {$days} days would be reclaimed (dry run).");

            return self::SUCCESS;
        }

        $chunk = max(1, (int) $this->option('chunk'));
        $deleted = 0;

        foreach (array_chunk($ids, $chunk) as $batch) {
            $placeholders = implode(', ', array_fill(0, count($batch), '?'));
            // Re-check created_at inside the DELETE: a trace touched (referenced)
            // between the candidate scan and now has created_at = now() > cutoff
            // and is spared — that is the whole race guard.
            $deleted += $conn->affectingStatement(
                "DELETE FROM nightowl_dict_trace WHERE created_at < ? AND id IN ({$placeholders})",
                array_merge([$cutoff], $batch),
            );
        }

        $this->info("Reclaimed {$deleted} orphaned trace(s) older than {$days} days.");

        return self::SUCCESS;
    }
}
