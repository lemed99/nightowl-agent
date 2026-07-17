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

        $this->warn('This drops 39 columns from every duration-bearing rollup table (all tiers). Requires:');
        $this->warn('  1. An agent restart AFTER this command (running drains cache the column layout).');
        $this->warn('  2. A NightOwl API release with histogram-conditional reads — do not run against an API older than the one shipping this command.');
        $this->warn('  3. On high-volume apps (millions of req/day), 1h/24h percentile charts get slower after the drop: those windows read the minute tier, where percentiles then come from aggregating one sketch per row (~20µs each) instead of the C-speed bin sums. Measured at an 8M req/day profile: ~4.4s for the 24h chart vs ~0.9s on the bins. Wide ranges (7d/30d) are unaffected. Weigh the disk win against that before dropping.');

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
