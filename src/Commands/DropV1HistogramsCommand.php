<?php

namespace NightOwl\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use NightOwl\Support\V1HistogramCleanup;

class DropV1HistogramsCommand extends Command
{
    protected $signature = 'nightowl:drop-v1-histograms
        {--force : Skip the confirmation prompt}';

    protected $description = 'Drop the v1 hist_NN columns once every rollup row carries a v2 DDSketch (post-transition cleanup)';

    public function handle(): int
    {
        $pdo = DB::connection('nightowl')->getPdo();

        $offenders = V1HistogramCleanup::verify($pdo);
        if ($offenders !== []) {
            $this->error('Not safe to drop yet:');
            foreach (self::describeOffenders($offenders) as $line) {
                $this->line($line);
            }

            return self::FAILURE;
        }

        $this->error('DO NOT RUN THIS YET — nightowl_ddsketch_agg is too slow to be the only percentile source.');
        $this->warn('The bins are what every percentile read falls back to. Once they are gone, every percentile at every');
        $this->warn('tier goes through nightowl_ddsketch_agg, whose STYPE is bytea: each input row copies, re-decodes and');
        $this->warn('re-encodes the whole accumulated sketch state, so cost per row grows with the state rather than');
        $this->warn('staying flat. Measured on a 14d hourly profile (~380 distinct indices): ~6.5ms per input row — 500');
        $this->warn('rows 3.2s, 4000 rows 26.3s. A 20s statement_timeout is exhausted around 3000 rows, and a 14d');
        $this->warn('per-route read (16,800 rows) never returns at all. Wide ranges are NOT unaffected — they are the');
        $this->warn('case that breaks, and after the drop there is no cheaper path left to degrade to.');
        $this->newLine();
        $this->warn('This drops 39 columns from every duration-bearing rollup table (all tiers). Requires:');
        $this->warn('  1. An agent restart AFTER this command (running drains cache the column layout).');
        $this->warn('  2. A NightOwl API release with histogram-conditional reads — do not run against an API older than the one shipping this command.');
        $this->warn('  3. An agent release whose merge/aggregate is linear in its input. Until that ships, the disk win is not worth an unrecoverable regression: the bins can only be restored by re-running the rollups from raw telemetry, which is gone past retention.');

        if (! $this->option('force') && ! $this->confirm('Proceed?')) {
            return self::FAILURE;
        }

        foreach (V1HistogramCleanup::drop($pdo) as $table) {
            $this->line("  {$table}: hist_NN dropped");
        }

        $this->newLine();
        $this->info('Done. Restart the agent daemon now (nightowl:agent).');

        return self::SUCCESS;
    }

    /**
     * Operator-facing explanation for each verify() blocker. MISSING_COUNT_FN
     * is a database-global condition (migration 000062's coverage function is
     * absent for the whole DB) that verify() surfaces on every hist-bearing
     * table, so its remedy — nightowl:migrate — is a property of the database,
     * not of any single table.
     *
     * @param  array<string, int>  $offenders  from V1HistogramCleanup::verify
     * @return list<string>
     */
    public static function describeOffenders(array $offenders): array
    {
        $lines = [];

        if (in_array(V1HistogramCleanup::MISSING_COUNT_FN, $offenders, true)) {
            $lines[] = '  nightowl_ddsketch_count() is missing for this database (migration 000062) — run nightowl:migrate first, then re-run this command.';
        }

        foreach ($offenders as $table => $count) {
            if ($count === V1HistogramCleanup::MISSING_COUNT_FN) {
                continue;
            }

            $lines[] = match ($count) {
                V1HistogramCleanup::MISSING_SKETCH => "  {$table}: no sketch column (CREATE FUNCTION was denied on this DB — the drop is unavailable here)",
                V1HistogramCleanup::MISSING_DURATION_COUNT => "  {$table}: no duration_count column — its avg denominator still comes from the hist bins. Run nightowl:migrate first.",
                default => "  {$table}: {$count} row(s) whose sketch doesn't cover their bins. Rows still inside raw retention are fixed by nightowl:backfill-rollups; older ones can only age out.",
            };
        }

        return $lines;
    }
}
