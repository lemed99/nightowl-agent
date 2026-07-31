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

        $this->warn('This drops 39 columns from every duration-bearing rollup table (all tiers). It is irreversible:');
        $this->warn('the bins can only be restored by re-running the rollups from raw telemetry, which is gone past');
        $this->warn('retention. After it, every percentile at every tier is served by nightowl_ddsketch_agg alone —');
        $this->warn('there is no cheaper path left to degrade to, so a wide-range chart that used to be slow becomes');
        $this->warn('a chart that does not return. Requires:');
        $this->warn('  1. An agent restart AFTER this command (running drains cache the column layout).');
        $this->warn('  2. A NightOwl API release with histogram-conditional reads — do not run against an API older than the one shipping this command.');
        $this->warn('  3. Migration 000070, the linear aggregate (verify() blocks without it). On the 200-route x 336-hour');
        $this->warn('     profile this command was written against, a 14d 50-group percentile read goes from 103s to 0.5s on');
        $this->warn('     narrow sketches and from not returning inside 120s to 2s on wide ones; a sketch-only read path is');
        $this->warn('     only survivable with it.');

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
     * and MISSING_LINEAR_AGG are database-global conditions (migration 000062's
     * coverage function / 000070's linear aggregate are absent for the whole
     * DB) that verify() surfaces on every hist-bearing table, so their remedy —
     * nightowl:migrate — is a property of the database, not of any single
     * table.
     *
     * @param  array<string, int>  $offenders  from V1HistogramCleanup::verify
     * @return list<string>
     */
    public static function describeOffenders(array $offenders): array
    {
        $lines = [];

        if (in_array(V1HistogramCleanup::MISSING_LINEAR_AGG, $offenders, true)) {
            $lines[] = '  nightowl_ddsketch_agg is still the bytea-state fold for this database (migration 000070) — it cannot serve a wide range as the only percentile source. Run nightowl:migrate first, then re-run this command.';
        }

        if (in_array(V1HistogramCleanup::MISSING_COUNT_FN, $offenders, true)) {
            $lines[] = '  nightowl_ddsketch_count() is missing for this database (migration 000062) — run nightowl:migrate first, then re-run this command.';
        }

        foreach ($offenders as $table => $count) {
            if ($count === V1HistogramCleanup::MISSING_COUNT_FN || $count === V1HistogramCleanup::MISSING_LINEAR_AGG) {
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
