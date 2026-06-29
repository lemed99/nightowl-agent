<?php

namespace NightOwl\Commands;

use Carbon\Carbon;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use NightOwl\Support\QueryHistogram;
use NightOwl\Support\RollupSpec;
use NightOwl\Support\RollupSpecs;

class BackfillRollupsCommand extends Command
{
    protected $signature = 'nightowl:backfill-rollups
        {--since= : Start datetime (default: earliest source row)}
        {--until= : End datetime (default: now minus the safety margin)}
        {--chunk-days=1 : Days of source data processed per transaction}
        {--type= : Restrict to one rollup table (e.g. nightowl_request_rollups)}';

    protected $description = 'Backfill every nightowl_*_rollups table from existing raw telemetry';

    /**
     * Backfill never touches a bucket the live drain might still be writing.
     * Live drain only writes the current minute; keeping the ceiling this far
     * behind `now` guarantees the two write modes never collide, so backfill can
     * safely DELETE-then-INSERT (replace-per-bucket) without a watermark.
     */
    private const SAFETY_MARGIN_SECONDS = 600;

    public function handle(): int
    {
        $conn = DB::connection('nightowl');
        $schema = $conn->getSchemaBuilder();
        $only = $this->option('type');

        $specs = array_filter(
            RollupSpecs::all(),
            fn (RollupSpec $spec): bool => $only === null || $spec->table === $only,
        );

        if (empty($specs)) {
            $this->error($only ? "Unknown rollup table: {$only}" : 'No rollup specs registered.');

            return self::FAILURE;
        }

        $chunkDays = max(1, (int) $this->option('chunk-days'));

        foreach ($specs as $spec) {
            if (! $schema->hasTable($spec->table)) {
                $this->warn("Skipping {$spec->table} (table does not exist — run nightowl:migrate).");

                continue;
            }

            $this->backfillSpec($conn, $spec, $chunkDays);
        }

        return self::SUCCESS;
    }

    private function backfillSpec($conn, RollupSpec $spec, int $chunkDays): void
    {
        $safetyCeiling = now()->subSeconds(self::SAFETY_MARGIN_SECONDS);

        $until = $this->option('until') ? Carbon::parse($this->option('until')) : $safetyCeiling->copy();
        if ($until->greaterThan($safetyCeiling)) {
            $until = $safetyCeiling->copy();
        }

        $sinceOption = $this->option('since') ?: $conn->table($spec->source)->min('created_at');
        if ($sinceOption === null) {
            $this->line("  {$spec->table}: no source rows.");

            return;
        }
        $since = Carbon::parse($sinceOption);

        if ($since->greaterThanOrEqualTo($until)) {
            $this->line("  {$spec->table}: nothing to backfill.");

            return;
        }

        $this->info("Backfilling {$spec->table} from {$since->toDateTimeString()} to {$until->toDateTimeString()}...");

        // Precompute the INSERT…SELECT shape once per spec.
        $histCase = $spec->hasHistogram ? QueryHistogram::caseSql($spec->durationField) : [];
        $parts = $spec->backfillSql($histCase);
        $columns = implode(', ', $parts['columns']);
        $selects = implode(', ', $parts['selects']);
        $groupBy = implode(', ', range(1, $parts['groupByCount']));

        $cursor = $since->copy();
        $total = 0;

        while ($cursor->lessThan($until)) {
            $chunkEnd = $cursor->copy()->addDays($chunkDays);
            if ($chunkEnd->greaterThan($until)) {
                $chunkEnd = $until->copy();
            }

            $total += $this->backfillChunk($conn, $spec, $columns, $selects, $groupBy, $cursor->toDateTimeString(), $chunkEnd->toDateTimeString());
            $cursor = $chunkEnd;

            // Throttle so backfill doesn't compete with live drain on the
            // customer's DB.
            usleep(50_000);
        }

        $this->line("  {$spec->table}: {$total} rollup rows.");
    }

    /**
     * Replace-per-bucket for one source chunk, transactionally: DELETE the
     * chunk's bucket range, then INSERT recomputed aggregates. Idempotent.
     *
     * The INSERT is ON CONFLICT … DO UPDATE (replace) rather than a plain INSERT:
     * the live drain now buckets each row on its own EVENT timestamp, so a catch-up
     * drain after a PG outage can write rollups into buckets older than the safety
     * margin — i.e. inside this chunk's range. A concurrent drain UPSERT in the gap
     * between this DELETE and INSERT would otherwise abort the whole chunk on the
     * (group, bucket, env) unique key; instead we overwrite with the backfill's
     * freshly-computed value (the same replace semantics this command already has).
     *
     * To stop that replace from CLOBBERING a concurrent catch-up drain's rows with a
     * stale count (the backfill's recompute snapshot can straddle the drain's commit),
     * the chunk takes an EXCLUSIVE advisory lock on the rollup table; the drain takes
     * the matching SHARED lock around its additive UPSERT (RecordWriter::
     * lockRollupForWriteShared). The two then serialize and commute: drain-first →
     * our recompute reads its committed rows; backfill-first → its additive UPSERT
     * adds on top of our value. Shared locks don't block each other, so multi-worker
     * drains are unaffected except while a backfill on the same table is running.
     */
    private function backfillChunk($conn, RollupSpec $spec, string $columns, string $selects, string $groupBy, string $start, string $end): int
    {
        // A row's bucket truncates created_at down to the minute, so clear from
        // the minute containing $start (not $start) to avoid colliding with a
        // stale partial-minute bucket from an earlier run.
        $bucketLow = Carbon::parse($start)->startOfMinute()->toDateTimeString();

        $pk = [...$spec->groupColumnNames(), 'bucket_start', 'environment'];
        $updateCols = array_values(array_diff(array_map('trim', explode(',', $columns)), $pk));
        $onConflict = 'ON CONFLICT ('.implode(', ', $pk).') DO UPDATE SET '
            .implode(', ', array_map(static fn (string $c): string => "{$c} = EXCLUDED.{$c}", $updateCols));

        return $conn->transaction(function () use ($conn, $spec, $columns, $selects, $groupBy, $start, $end, $bucketLow, $onConflict): int {
            // EXCLUSIVE advisory lock paired with the drain's SHARED lock (same key),
            // so this DELETE+recompute can't interleave with a concurrent additive
            // drain UPSERT and overwrite it with a stale count. Released at commit.
            $conn->statement('SELECT pg_advisory_xact_lock(hashtext(?))', ['nightowl_rollup:'.$spec->table]);

            $conn->table($spec->table)
                ->where('bucket_start', '>=', $bucketLow)
                ->where('bucket_start', '<', $end)
                ->delete();

            $conn->statement(
                "INSERT INTO {$spec->table} ({$columns})
                 SELECT {$selects}
                 FROM {$spec->source}
                 WHERE created_at >= ? AND created_at < ?
                 GROUP BY {$groupBy}
                 {$onConflict}",
                [$start, $end]
            );

            return (int) $conn->table($spec->table)
                ->where('bucket_start', '>=', $bucketLow)
                ->where('bucket_start', '<', $end)
                ->count();
        });
    }
}
