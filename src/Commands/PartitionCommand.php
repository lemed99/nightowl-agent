<?php

namespace NightOwl\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use NightOwl\Support\RawPartitions;

class PartitionCommand extends Command
{
    protected $signature = 'nightowl:partition
        {--table= : Restrict to one raw table (e.g. nightowl_queries)}';

    protected $description = 'Convert the raw telemetry tables to native daily partitioning (prune becomes instant DROP PARTITION)';

    public function handle(): int
    {
        $conn = DB::connection('nightowl');
        $schema = Schema::connection('nightowl');
        $only = $this->option('table');

        $tables = $only !== null ? [$only] : RawPartitions::TABLES;
        if ($only !== null && ! in_array($only, RawPartitions::TABLES, true)) {
            $this->error("{$only} is not a partitionable raw table.");

            return self::FAILURE;
        }

        $this->warn(
            'nightowl_logs conversion includes a created_at varchar→timestamp rewrite — a full-table '
            .'ACCESS EXCLUSIVE pass on populated tables. Ingest keeps buffering; log reads 504 gracefully '
            .'for the duration.'
        );

        foreach ($tables as $table) {
            if (! $schema->hasTable($table)) {
                $this->warn("Skipping {$table} (does not exist — run nightowl:migrate).");

                continue;
            }
            if (RawPartitions::isPartitioned($conn->getPdo(), $table)) {
                $this->line("  {$table}: already partitioned.");

                continue;
            }

            $this->info("Partitioning {$table}...");
            RawPartitions::convert($conn->getPdo(), $table);
            $this->line("  {$table}: partitioned (historic partition attached in place, daily children ahead).");
        }

        $this->newLine();
        $this->info('Done. The running agent picks up future-partition maintenance on its next tick; no restart needed.');

        return self::SUCCESS;
    }
}
