<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

/**
 * Widen 32-bit integer columns that store microsecond durations, byte sizes,
 * and peak memory usage to 64-bit. The original `integer` type tops out at
 * 2,147,483,647 — ~35 minutes in microseconds or ~2 GB in bytes — so any
 * request/job/command/query that ran longer (or any payload larger) than that
 * raised "Numeric value out of range" and failed to drain.
 *
 * Counters (event counts, status/exit codes, attempts) and the *_ms threshold
 * columns are left as-is — they're small or measured in milliseconds and can't
 * realistically overflow.
 *
 * NOTE: int -> bigint forces a full table rewrite under an ACCESS EXCLUSIVE
 * lock on PostgreSQL. On large telemetry tables this can take a while and block
 * concurrent reads/writes — run it during a maintenance/low-traffic window.
 */
return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * table => [columns to widen]. Every column below is nullable with no
     * default; `change()` requires re-declaring the modifiers we keep.
     *
     * @var array<string, array<int, string>>
     */
    private array $columns = [
        'nightowl_requests' => [
            'duration', 'bootstrap', 'before_middleware', 'action', 'render',
            'after_middleware', 'sending', 'terminating',
            'request_size', 'response_size', 'peak_memory_usage',
        ],
        'nightowl_queries' => ['duration'],
        'nightowl_commands' => [
            'duration', 'bootstrap', 'action', 'terminating', 'peak_memory_usage',
        ],
        'nightowl_jobs' => ['duration', 'peak_memory_usage'],
        'nightowl_cache_events' => ['duration'],
        'nightowl_mail' => ['duration'],
        'nightowl_notifications' => ['duration'],
        'nightowl_outgoing_requests' => [
            'duration', 'request_size', 'response_size',
        ],
        'nightowl_scheduled_tasks' => ['duration', 'peak_memory_usage'],
    ];

    public function up(): void
    {
        $this->retype(fn (Blueprint $table, string $column) => $table->bigInteger($column)->nullable()->change());
    }

    public function down(): void
    {
        $this->retype(fn (Blueprint $table, string $column) => $table->integer($column)->nullable()->change());
    }

    private function retype(callable $apply): void
    {
        $schema = Schema::connection($this->connection);

        foreach ($this->columns as $table => $columns) {
            if (! $schema->hasTable($table)) {
                continue;
            }

            $present = array_filter($columns, fn (string $column) => $schema->hasColumn($table, $column));

            if ($present === []) {
                continue;
            }

            $schema->table($table, function (Blueprint $blueprint) use ($apply, $present) {
                foreach ($present as $column) {
                    $apply($blueprint, $column);
                }
            });
        }
    }
};
