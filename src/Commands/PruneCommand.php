<?php

namespace NightOwl\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use NightOwl\Support\RollupSpecs;
use NightOwl\Support\RawPartitions;
use NightOwl\Support\RollupTiers;

class PruneCommand extends Command
{
    protected $signature = 'nightowl:prune
        {--days= : Number of days to retain raw telemetry}
        {--hours= : Number of HOURS to retain raw telemetry (overrides --days; for aggressive demo-feeder retention on a sub-day cadence)}
        {--rollup-days= : Number of days to retain query rollups (defaults to far longer than raw)}
        {--delete-chunk=100000 : Rows per DELETE statement for raw-table trims (smaller = shorter transactions, more statements)}';

    protected $description = 'Prune old NightOwl monitoring data';

    private const TABLES = [
        'nightowl_requests',
        'nightowl_queries',
        'nightowl_exceptions',
        'nightowl_commands',
        'nightowl_jobs',
        'nightowl_cache_events',
        'nightowl_mail',
        'nightowl_notifications',
        'nightowl_outgoing_requests',
        'nightowl_scheduled_tasks',
        'nightowl_logs',
    ];

    public function handle(): int
    {
        // --hours wins when given (fine-grained, for the demo feeder which keeps
        // only a few hours of raw telemetry and prunes every 15-30 min). Otherwise
        // fall back to --days / the configured day-granularity retention.
        $hoursOption = $this->option('hours');
        if ($hoursOption !== null && $hoursOption !== '') {
            $hours = max(1, (int) $hoursOption);
            // created_at is stored in UTC (gmdate), so the cutoff MUST be UTC too — a
            // non-UTC host app TZ would otherwise offset it (deleting fresh rows, or
            // never pruning), and the sub-day --hours cadence makes the offset dominate.
            $cutoff = now()->utc()->subHours($hours)->toDateTimeString();
            $window = "{$hours} hours";
        } else {
            $days = (int) ($this->option('days') ?? config('nightowl.database.retention_days', 14));
            $cutoff = now()->utc()->subDays($days)->toDateTimeString();
            $window = "{$days} days";
        }

        $conn = DB::connection('nightowl');

        $this->info("Pruning records older than {$window} (before {$cutoff})...");

        $totalDeleted = 0;

        foreach (self::TABLES as $table) {
            // Partitioned tables: DROP fully-expired daily children first —
            // instant, zero WAL amplification, zero vacuum debt — then let the
            // row-DELETE below clean only the boundary/historic/default
            // partitions. Unpartitioned tables take the DELETE path unchanged.
            if (RawPartitions::isPartitioned($conn->getPdo(), $table)) {
                foreach (RawPartitions::expiredChildren($conn->getPdo(), $table, $cutoff) as $child) {
                    $rows = (int) $conn->table($child)->count();
                    $conn->statement("DROP TABLE {$child}");
                    $totalDeleted += $rows;
                    $this->line("  {$table}: dropped partition {$child} ({$rows} records)");
                }
            }

            $deleted = $this->chunkedDelete($conn, $table, $cutoff, max(1, (int) $this->option('delete-chunk')));
            $totalDeleted += $deleted;

            if ($deleted > 0) {
                $this->line("  {$table}: {$deleted} records deleted");
            }

            // Once the row-DELETE above empties the historic partition (its
            // range is frozen at conversion, so this happens on the first
            // prune after retention passes it), drop it — the tenant's entire
            // pre-conversion bloat returns to the OS in one unlink.
            if (RawPartitions::isPartitioned($conn->getPdo(), $table)
                && RawPartitions::dropEmptyHistoric($conn->getPdo(), $table)) {
                $this->line("  {$table}: dropped empty historic partition (pre-conversion space reclaimed)");
            }
        }

        // Rollups are tiny, so they're retained far longer than raw telemetry —
        // pruning raw aggressively while keeping rollups gives long-range trend
        // charts without storing raw rows. Every rollup table is pruned on its
        // own bucket_start, with a separate (longer) retention.
        $rollupDays = (int) ($this->option('rollup-days') ?? config('nightowl.database.rollup_retention_days', 90));
        $rollupCutoff = now()->utc()->subDays($rollupDays)->toDateTimeString();
        $schema = $conn->getSchemaBuilder();

        $rollupTables = [];
        foreach (RollupSpecs::all() as $spec) {
            $rollupTables[$spec->table] = true;
        }

        foreach (array_keys($rollupTables) as $table) {
            if (! $schema->hasTable($table)) {
                continue;
            }

            $deleted = $conn->table($table)->where('bucket_start', '<', $rollupCutoff)->delete();
            $totalDeleted += $deleted;

            if ($deleted > 0) {
                $this->line("  {$table}: {$deleted} records deleted (older than {$rollupDays} days)");
            }
        }

        // Hour/day tiers are 60× / 1440× sparser than the minute rollups, so
        // they carry their own (much longer) retentions — that's what makes
        // long-range trend views survive the minute tier's cutoff.
        $tierDays = [
            'hourly' => (int) config('nightowl.rollup_tier_retention.hourly_days', 366),
            'daily' => (int) config('nightowl.rollup_tier_retention.daily_days', 1100),
        ];

        foreach ($tierDays as $tier => $days) {
            $cutoff = now()->utc()->subDays($days)->toDateTimeString();

            foreach (array_keys($rollupTables) as $base) {
                $table = RollupTiers::table($base, $tier);
                if (! $schema->hasTable($table)) {
                    continue;
                }

                $deleted = $conn->table($table)->where('bucket_start', '<', $cutoff)->delete();
                $totalDeleted += $deleted;

                if ($deleted > 0) {
                    $this->line("  {$table}: {$deleted} records deleted (older than {$days} days)");
                }
            }
        }

        $this->newLine();
        $this->info("Pruned {$totalDeleted} records total.");

        return $this->finish($conn);
    }

    /**
     * Trim raw rows older than $cutoff in bounded statements instead of one
     * mega-DELETE. The first prune after nightowl:partition trims the entire
     * pre-conversion backlog out of the historic partition — tens of GB on
     * exactly the tenants that needed partitioning most — and a single DELETE
     * of that size runs for many minutes with no output (reported from the
     * field as "prune gets stuck"), holds one long transaction (WAL and bloat
     * spike, blocks vacuum truncation), and hands autovacuum one giant
     * dead-tuple wave. Chunking bounds each statement's transaction, emits
     * progress the operator can see, and an interrupted prune resumes where
     * it stopped instead of rolling the whole trim back.
     *
     * The inner SELECT walks the created_at index; the outer created_at bound
     * repeats so partition pruning stays effective on partitioned parents (an
     * id-only semi-join would probe every daily child).
     */
    private function chunkedDelete($conn, string $table, string $cutoff, int $chunk): int
    {
        $total = 0;
        $lastReport = 0;

        do {
            $deleted = $conn->affectingStatement(
                "DELETE FROM {$table} WHERE created_at < ? AND id IN (
                    SELECT id FROM {$table} WHERE created_at < ? LIMIT {$chunk}
                )",
                [$cutoff, $cutoff],
            );
            $total += $deleted;

            // A heartbeat roughly every 10 chunks — enough to prove liveness
            // on a multi-GB trim without flooding the output.
            if ($total - $lastReport >= $chunk * 10) {
                $this->line("  {$table}: {$total} records deleted so far...");
                $lastReport = $total;
            }
        } while ($deleted >= $chunk);

        return $total;
    }

    private function finish($conn): int
    {
        // THE moment the false expectation forms: "Pruned 24M records" reads
        // as "disk freed", but on unpartitioned tables Postgres only reuses
        // that space — it never returns it to the OS. Say so right here, in
        // the output the operator is actually reading.
        try {
            $unpartitioned = RawPartitions::unpartitionedPopulated($conn->getPdo());
            if ($unpartitioned !== []) {
                $this->warn(sprintf(
                    'Note: %d table(s) (%s…) are unpartitioned — the space just pruned is reused by new '
                    .'telemetry, not returned to the OS. Run `php artisan nightowl:partition` once to make '
                    .'future prunes drop whole day-partitions and reclaim disk instantly.',
                    count($unpartitioned),
                    $unpartitioned[0],
                ));
            }
        } catch (\Throwable) {
            // Advisory only — never fail a completed prune over it.
        }

        return self::SUCCESS;
    }
}
