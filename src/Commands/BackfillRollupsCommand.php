<?php

namespace NightOwl\Commands;

use Carbon\Carbon;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use NightOwl\Support\QueryHistogram;
use NightOwl\Support\RollupSpec;
use NightOwl\Support\RollupSpecs;
use NightOwl\Support\RollupTiers;
use RuntimeException;

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
        $failed = [];

        foreach ($specs as $spec) {
            if (! $schema->hasTable($spec->table)) {
                $this->warn("Skipping {$spec->table} (table does not exist — run nightowl:migrate).");

                continue;
            }

            // One type's failure must not strand the remaining types: the API
            // serves zeros off a rollup table that exists but is empty rather
            // than falling back to raw, so aborting the run here would leave
            // wide-range views blank for every type after this one. Chunks
            // commit individually and the pass is replace-per-bucket, so a
            // re-run after the fix resumes without duplicating work.
            try {
                $this->backfillSpec($conn, $spec, $chunkDays);
                $this->backfillTiers($conn, $schema, $spec, $chunkDays);
            } catch (\Throwable $e) {
                $failed[$spec->table] = $e;
                $this->error("  {$spec->table}: FAILED — {$e->getMessage()}");
            }
        }

        if ($failed !== []) {
            // Raising rather than returning FAILURE: nightowl:migrate backfills
            // through $this->call() and discards the exit code, so a returned
            // failure would let it report "Rollup tables populated" over tables
            // it just left existing-but-empty — the state the API reads as zeros
            // instead of falling back to raw.
            throw new RuntimeException(
                'Backfill incomplete for '.count($failed).' rollup table(s): '.implode(', ', array_keys($failed))
                .'. They may read empty until this is fixed and nightowl:backfill-rollups is re-run.',
                0,
                reset($failed),
            );
        }

        return self::SUCCESS;
    }

    /**
     * Which optional columns a tier copy can carry. The generated INSERT…SELECT
     * READS each one from $sourceColumns' table and WRITES it to $destColumns',
     * so a column survives the copy only when both sides have it — the tiers
     * can disagree (a partial nightowl:drop-v1-histograms, a migration that
     * reached one table and not the other).
     *
     * Dropping a column from the copy avoids an unknown-column abort but is not
     * free: the pass is DELETE-then-INSERT, so a column the DESTINATION still
     * has is rewritten to its default rather than left alone. Only a desynced
     * pair reaches that, and tierDesync() warns on it.
     *
     * @param  list<string>  $sourceColumns
     * @param  list<string>  $destColumns
     * @return array{hist: bool, sketch: bool, durationCount: bool}
     */
    public static function tierColumns(RollupSpec $spec, array $sourceColumns, array $destColumns): array
    {
        $shared = static fn (string $column): bool => in_array($column, $sourceColumns, true)
            && in_array($column, $destColumns, true);

        return [
            'hist' => $spec->hasHistogram && $shared('hist_00'),
            // 000057 adds both sketch columns together, but tierBackfillSql
            // emits both, so neither may be assumed from the other.
            'sketch' => $spec->hasHistogram && $shared('sketch') && $shared('sketch_version'),
            'durationCount' => $spec->hasDurationCount && $shared('duration_count'),
        ];
    }

    /**
     * Optional columns the two tiers disagree on — one side has it, the other
     * does not. Reached only by DDL that stopped half-way: nightowl:drop-v1-
     * histograms drops per-table outside a transaction, iterating base→hourly
     * →daily, so a lock timeout on the large hourly table leaves the pair
     * split. Both sides lacking a column is a settled state, not a desync.
     *
     * @param  list<string>  $sourceColumns
     * @param  list<string>  $destColumns
     * @return list<string>
     */
    public static function tierDesync(RollupSpec $spec, array $sourceColumns, array $destColumns): array
    {
        $optional = $spec->hasHistogram ? ['hist_00', 'sketch', 'sketch_version'] : [];
        if ($spec->hasDurationCount) {
            $optional[] = 'duration_count';
        }

        return array_values(array_filter(
            $optional,
            static fn (string $c): bool => in_array($c, $sourceColumns, true) !== in_array($c, $destColumns, true),
        ));
    }

    /**
     * Rollup-from-rollup tier passes: minute→hourly, then hourly→daily. Runs to
     * NOW rather than the raw safety ceiling — the finer rollup is complete for
     * every closed bucket (live drain + the minute pass above), and the
     * exclusive advisory lock on each tier table serializes the replace against
     * the drain's shared-lock additive upsert, so recomputing even the
     * current-hour/day bucket commutes with concurrent drain writes.
     */
    private function backfillTiers($conn, $schema, RollupSpec $spec, int $chunkDays): void
    {
        $sourceTable = $spec->table;

        foreach (RollupTiers::TIERS as $tier => $granularitySeconds) {
            $tierTable = RollupTiers::table($spec->table, $tier);

            if (! $schema->hasTable($tierTable)) {
                $this->warn("Skipping {$tierTable} (table does not exist — run nightowl:migrate).");

                continue;
            }

            $sinceOption = $conn->table($sourceTable)->min('bucket_start');
            if ($sinceOption === null) {
                $this->line("  {$tierTable}: no source rows.");

                continue;
            }

            $since = Carbon::parse($sinceOption);
            $until = now();
            $unit = RollupTiers::TRUNC_UNIT[$tier];

            $this->info("Backfilling {$tierTable} from {$sourceTable}...");

            $sourceColumns = $schema->getColumnListing($sourceTable);
            $destColumns = $schema->getColumnListing($tierTable);

            $desynced = self::tierDesync($spec, $sourceColumns, $destColumns);
            if ($desynced !== []) {
                $this->warn(sprintf(
                    '  %s and %s disagree on %s. Dropping it from the copy to keep the pass running, '
                    .'which resets that column to its default on whichever side still has it. Finish the '
                    .'interrupted DDL and re-run this command to restore it.',
                    $tierTable,
                    $sourceTable,
                    implode(', ', $desynced),
                ));
            }

            $optional = self::tierColumns($spec, $sourceColumns, $destColumns);
            $parts = $spec->tierBackfillSql(
                $unit,
                $optional['hist'] ? QueryHistogram::columns() : [],
                $optional['sketch'],
                $optional['durationCount'],
            );
            $columns = implode(', ', $parts['columns']);
            $selects = implode(', ', $parts['selects']);
            $groupBy = implode(', ', range(1, $parts['groupByCount']));

            // Rollup rows are ~60× / ~1440× sparser than raw, so tier chunks can
            // span much wider windows per transaction.
            $tierChunkDays = $chunkDays * ($tier === 'hourly' ? 7 : 30);

            $cursor = $since->copy();
            $total = 0;
            while ($cursor->lessThan($until)) {
                $chunkEnd = $cursor->copy()->addDays($tierChunkDays);
                if ($chunkEnd->greaterThan($until)) {
                    $chunkEnd = $until->copy();
                }

                $total += $this->backfillTierChunk(
                    $conn, $spec, $sourceTable, $tierTable, $unit, $granularitySeconds,
                    $columns, $selects, $groupBy,
                    $cursor->toDateTimeString(), $chunkEnd->toDateTimeString(),
                );
                $cursor = $chunkEnd;

                usleep(50_000);
            }

            $this->line("  {$tierTable}: {$total} rollup rows.");

            // The next tier aggregates from this one (day from hour) — but only
            // when this pass actually ran; a missing hourly table leaves daily
            // aggregating straight from the minute rollup.
            $sourceTable = $tierTable;
        }
    }

    /**
     * Replace-per-window for one tier chunk — same protocol as backfillChunk,
     * with the DELETE floor truncated to the TIER granularity so a partial
     * leading bucket from an earlier run can't survive alongside its recompute.
     */
    private function backfillTierChunk(
        $conn, RollupSpec $spec, string $sourceTable, string $tierTable, string $unit, int $granularitySeconds,
        string $columns, string $selects, string $groupBy, string $start, string $end,
    ): int {
        $bucketLow = RollupTiers::truncateBucket($start, $granularitySeconds);

        $pk = [...$spec->groupColumnNames(), 'bucket_start', 'environment'];
        $updateCols = array_values(array_diff(array_map('trim', explode(',', $columns)), $pk));
        $onConflict = 'ON CONFLICT ('.implode(', ', $pk).') DO UPDATE SET '
            .implode(', ', array_map(static fn (string $c): string => "{$c} = EXCLUDED.{$c}", $updateCols));

        return $conn->transaction(function () use ($conn, $sourceTable, $tierTable, $columns, $selects, $groupBy, $start, $end, $bucketLow, $onConflict): int {
            $conn->statement('SELECT pg_advisory_xact_lock(hashtext(?))', ['nightowl_rollup:'.$tierTable]);

            $conn->table($tierTable)
                ->where('bucket_start', '>=', $bucketLow)
                ->where('bucket_start', '<', $end)
                ->delete();

            $conn->statement(
                "INSERT INTO {$tierTable} ({$columns})
                 SELECT {$selects}
                 FROM {$sourceTable}
                 WHERE bucket_start >= ? AND bucket_start < ?
                 GROUP BY {$groupBy}
                 {$onConflict}",
                [$bucketLow, $end]
            );

            return (int) $conn->table($tierTable)
                ->where('bucket_start', '>=', $bucketLow)
                ->where('bucket_start', '<', $end)
                ->count();
        });
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
        // Empty when the table went sketch-only (nightowl:drop-v1-histograms).
        $histCase = $spec->hasHistogram && $conn->getSchemaBuilder()->hasColumn($spec->table, 'hist_00')
            ? QueryHistogram::caseSql($spec->durationField) : [];
        // Absent until migration 000061. The live drain probes the same column
        // before writing it (RecordWriter::durationCountEnabled), so the two
        // write paths agree on an un-migrated tenant instead of this one
        // aborting on an unknown column.
        $withDurationCount = $spec->hasDurationCount
            && $conn->getSchemaBuilder()->hasColumn($spec->table, 'duration_count');
        $parts = $spec->backfillSql($histCase, $withDurationCount);
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

            // v2 DDSketch recompute for the same window — SQL-side (index CASE
            // + nightowl_ddsketch_agg), because a PHP pass over raw durations
            // would not scale on exactly the tenants that need backfill.
            if ($spec->hasHistogram
                && $conn->getSchemaBuilder()->hasColumn($spec->table, 'sketch')) {
                $conn->statement($spec->sketchBackfillSql($spec->table)['sql'], [$start, $end]);
            }

            return (int) $conn->table($spec->table)
                ->where('bucket_start', '>=', $bucketLow)
                ->where('bucket_start', '<', $end)
                ->count();
        });
    }
}
