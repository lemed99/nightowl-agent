<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    /**
     * Widen the storage-v2 dictionary columns that carry application-shaped
     * strings from varchar(512) to text.
     *
     * 000066 sized these by eye. A route path or controller action can exceed
     * 512 characters in a real application (nested resource routes, long
     * namespaces, generated route groups), and a query's origin `file` is an
     * absolute path from a machine we do not control. Because
     * DictionaryCache::warm builds its own INSERTs, those values never met the
     * width clamp every other write path applies, so one of them rejected the
     * whole batch with SQLSTATE 22001 — before the batch transaction, with
     * quarantine off by default, retried intact every loop — and the tenant's
     * drain stopped for good (BrokerCentral, 2026-09-08). RecordWriter now
     * clamps the dictionary rows too, so no schema can wedge the drain; this
     * migration is what keeps a long path STORED WHOLE rather than truncated.
     *
     * varchar(n) → text is binary-coercible, so Postgres rewrites neither the
     * table nor any index. It takes a brief ACCESS EXCLUSIVE lock. (The dict
     * tables were sized for hundreds of rows; a Livewire tenant on a pre-2.4.3
     * agent can hold hundreds of thousands — still a metadata-only change.)
     *
     * nightowl_dict_string.value is deliberately NOT widened: it is half of the
     * (kind, value) UNIQUE constraint, and a btree tuple has a hard ~2704-byte
     * ceiling — text there would only trade 22001 for 54000. It stays clamped
     * instead, which is right for what it holds: closed-set labels (environment,
     * server, queue, channel, job class), not free-form application strings.
     */
    private const COLUMNS = [
        'nightowl_dict_route' => ['path', 'action'],
        'nightowl_dict_sql' => ['file'],
    ];

    public function up(): void
    {
        $this->convert('text');
    }

    /**
     * Values longer than the old width exist only if the agent ran on this
     * schema, so the reverse has to truncate rather than fail. left() over a
     * few hundred dictionary rows; the clamp keeps new writes inside it.
     */
    public function down(): void
    {
        $this->convert('varchar(512)', truncateTo: 512);
    }

    private function convert(string $type, ?int $truncateTo = null): void
    {
        $schema = Schema::connection($this->connection);
        $conn = DB::connection($this->connection);

        foreach (self::COLUMNS as $table => $columns) {
            if (! $schema->hasTable($table)) {
                continue; // pre-v2 tenant — 000066 never ran
            }

            foreach ($columns as $column) {
                if (! $schema->hasColumn($table, $column)) {
                    continue;
                }

                // Already the target type: skip rather than take a pointless
                // ACCESS EXCLUSIVE lock. Makes the migration re-runnable by hand
                // on a tenant whose migrations table was rebuilt.
                $current = $conn->selectOne(
                    'SELECT data_type FROM information_schema.columns
                      WHERE table_schema = current_schema()
                        AND table_name = ? AND column_name = ?',
                    [$table, $column]
                );
                $isText = ($current->data_type ?? null) === 'text';
                if ($isText === ($type === 'text')) {
                    continue;
                }

                $using = $truncateTo === null
                    ? ''
                    : sprintf(' USING left(%s, %d)', $column, $truncateTo);

                $conn->statement(sprintf(
                    'ALTER TABLE %s ALTER COLUMN %s TYPE %s%s',
                    $table, $column, $type, $using
                ));
            }
        }
    }
};
