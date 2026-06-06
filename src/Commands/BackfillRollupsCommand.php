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
     */
    private function backfillChunk($conn, RollupSpec $spec, string $columns, string $selects, string $groupBy, string $start, string $end): int
    {
        // A row's bucket truncates created_at down to the minute, so clear from
        // the minute containing $start (not $start) to avoid colliding with a
        // stale partial-minute bucket from an earlier run.
        $bucketLow = Carbon::parse($start)->startOfMinute()->toDateTimeString();

        return $conn->transaction(function () use ($conn, $spec, $columns, $selects, $groupBy, $start, $end, $bucketLow): int {
            $conn->table($spec->table)
                ->where('bucket_start', '>=', $bucketLow)
                ->where('bucket_start', '<', $end)
                ->delete();

            $conn->statement(
                "INSERT INTO {$spec->table} ({$columns})
                 SELECT {$selects}
                 FROM {$spec->source}
                 WHERE created_at >= ? AND created_at < ?
                 GROUP BY {$groupBy}",
                [$start, $end]
            );

            return (int) $conn->table($spec->table)
                ->where('bucket_start', '>=', $bucketLow)
                ->where('bucket_start', '<', $end)
                ->count();
        });
    }
}
