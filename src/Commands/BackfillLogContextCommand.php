<?php

namespace NightOwl\Commands;

use Carbon\Carbon;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use NightOwl\Support\StorageV2;

/**
 * Convert already-stored log context from the deflated `context_z` to the
 * searchable plaintext `context`, so a tenant that turns
 * `nightowl.log_context_searchable` on can search its HISTORY and not just the
 * rows drained from that moment.
 *
 * Why a command and not a migration: this rewrites every log row in range, and
 * nightowl_logs_v2 is the highest-volume table there is. Boot-migrate has a
 * deadline and a customer deploy is the wrong place to discover that. It is
 * also genuinely optional — the flag alone gives a correct, narrower product,
 * and the dashboard says so out loud.
 *
 * Direction: NEWEST FIRST, walking the fence backwards.
 *
 * The searchable-log-context fence (StorageV2::LOG_CONTEXT_FENCE_KEY) is the
 * instant from which every row is plaintext, and the API only offers a context
 * search for windows entirely at or after it. Converting backwards from the
 * fence means each completed chunk extends that guarantee further into the
 * past, so:
 *  - the recent logs people actually search become searchable first;
 *  - an interrupted run leaves a SMALLER searchable window, never a false one;
 *  - a re-run resumes at the fence with no cursor to persist and no work
 *    repeated.
 * Converting forwards would do the opposite: rows would be converted but the
 * fence could not move until the very last chunk landed, so an interrupted run
 * would have paid the entire cost for nothing.
 *
 * Both columns stay readable throughout. Conversion is about SEARCH; a log's
 * context is rendered correctly at every point of this pass, including
 * halfway through a chunk.
 */
class BackfillLogContextCommand extends Command
{
    protected $signature = 'nightowl:backfill-log-context
        {--since= : Oldest datetime to convert, UTC unless it carries an offset (default: the oldest log row)}
        {--dry-run : Measure what conversion would cost on YOUR data and change nothing}
        {--sample=2000 : Rows to inflate when sizing a --dry-run}
        {--vacuum : VACUUM ANALYZE the table afterwards to reclaim the rewritten rows}';

    protected $description = 'Make stored log context searchable (converts deflated context_z to plaintext context)';

    /**
     * Chunk pacing, same controller as nightowl:backfill-rollups.
     *
     * The concern here is different but the shape is identical: this pass
     * competes with the live drain for the same table, and a chunk that runs
     * long holds row locks and piles up dead tuples faster than autovacuum
     * clears them. Chunks are therefore paced by MEASURED time, not by a
     * calendar span, because the same window is milliseconds on a small tenant
     * and half a minute on a large one.
     */
    private const TARGET_CHUNK_SECONDS = 1.0;

    private const INITIAL_CHUNK_SECONDS = 900;

    private const MIN_CHUNK_SECONDS = 1;

    private const MAX_CHUNK_SECONDS = 86400;

    /** Rows per UPDATE statement inside a chunk. */
    private const UPDATE_BATCH = 500;

    public function handle(): int
    {
        $conn = DB::connection('nightowl');

        if (! $conn->getSchemaBuilder()->hasTable('nightowl_logs_v2')) {
            $this->warn('nightowl_logs_v2 does not exist — nothing to convert (this tenant stores v1, whose context is already plaintext and already searchable).');

            return self::SUCCESS;
        }
        if (! $conn->getSchemaBuilder()->hasColumn('nightowl_logs_v2', 'context')) {
            $this->error('nightowl_logs_v2 has no `context` column — run `php artisan nightowl:migrate` first.');

            return self::FAILURE;
        }

        if ($this->option('dry-run')) {
            return $this->dryRun($conn);
        }

        $pdo = $conn->getPdo();

        // The fence is the precondition, not a detail: converting history while
        // the drain still writes COMPRESSED rows buys nothing, because the
        // window a user searches always reaches up to now and would still
        // contain unsearchable rows. Better to refuse than to spend an hour
        // rewriting a table for no visible change.
        $fence = StorageV2::logContextFence($pdo);

        if ($fence === null) {
            if (! (bool) config('nightowl.log_context_searchable', false)) {
                $this->error('Searchable log context is off, so converting history would have no effect.');
                $this->line('  Set NIGHTOWL_LOG_CONTEXT_SEARCHABLE=true, restart the agent, then run this again.');

                return self::FAILURE;
            }

            // Config says on but the drain has not opened the fence yet (an
            // idle app, or a daemon still running the old config). Opening it
            // here is safe in both cases: if the running agent is still writing
            // compressed contexts, its very next log batch closes the fence
            // again (RecordWriter::noteLogContextFence), which is exactly the
            // self-correction the invariant is built on.
            $now = gmdate('Y-m-d H:i:s');
            StorageV2::openLogContextFence($pdo, $now);
            $fence = StorageV2::logContextFence($pdo) ?? $now;

            $this->warn('No fence was open — opened one at '.$fence.' UTC.');
            $this->line('  If the running agent has not picked up the config yet, restart it: until it does,');
            $this->line('  its next drained log batch will close the fence again and this pass will be wasted.');
        }

        $floor = $this->floor($conn);
        if ($floor === null) {
            $this->info('No logs stored yet — nothing to do.');

            return self::SUCCESS;
        }

        $cursor = $this->parseUtc($fence);
        if ($cursor->lessThanOrEqualTo($floor)) {
            // Two different reasons land here and only one of them is good
            // news. Saying "already searchable" for a --since the fence has
            // not reached would be a plain lie: that range is still
            // compressed, the operator just asked us not to touch it.
            if ($this->option('since') !== null && $this->parseUtc((string) $this->option('since'))->greaterThanOrEqualTo($cursor)) {
                $this->warn('--since ('.$floor->toDateTimeString().' UTC) is at or after the searchable range, which already starts at '.$cursor->toDateTimeString().' UTC.');
                $this->line('  Nothing to do. Pass an EARLIER --since to extend coverage further back.');
            } else {
                $this->info('Everything from '.$floor->toDateTimeString().' UTC onwards is already searchable.');
            }

            return self::SUCCESS;
        }

        $this->info('Converting log context to searchable plaintext, newest first.');
        $this->line('  range : '.$floor->toDateTimeString().' → '.$cursor->toDateTimeString().' UTC');
        $this->line('  fence : moves backwards as each chunk lands; stop any time and re-run to resume.');
        $this->newLine();

        $window = (float) self::INITIAL_CHUNK_SECONDS;
        $converted = 0;
        $chunks = 0;
        $started = microtime(true);

        while ($cursor->greaterThan($floor)) {
            $chunkStart = $cursor->copy()->subSeconds(max(1, (int) round($window)));
            if ($chunkStart->lessThan($floor)) {
                $chunkStart = $floor->copy();
            }

            $t0 = microtime(true);
            $rows = $this->convertChunk($conn, $chunkStart, $cursor);
            $elapsed = microtime(true) - $t0;

            $converted += $rows;
            $chunks++;
            $cursor = $chunkStart;

            $window = $this->nextWindow($window, $elapsed);

            if ($chunks % 25 === 0 || $rows > 0) {
                $this->line(sprintf(
                    '  %s UTC  %s rows converted  (%.2fs, window %ds)',
                    $cursor->toDateTimeString(),
                    number_format($converted),
                    $elapsed,
                    (int) round($window),
                ));
            }
        }

        $this->newLine();
        $this->info(sprintf(
            'Converted %s rows in %s chunks (%.1fs). Log context is searchable from %s UTC.',
            number_format($converted),
            number_format($chunks),
            microtime(true) - $started,
            $cursor->toDateTimeString(),
        ));

        if ($converted > 0) {
            // An UPDATE writes a new row version and leaves the old one dead,
            // so a full pass doubles the table's physical size until autovacuum
            // catches up. Saying so beats a customer discovering it in a disk
            // alert; --vacuum reclaims it now rather than eventually.
            if ($this->option('vacuum')) {
                $this->line('Vacuuming (this can take a while on a large table) ...');
                $conn->statement('VACUUM (ANALYZE) nightowl_logs_v2');
                $this->info('Vacuum complete.');
            } else {
                $this->warn('Rewritten rows leave dead tuples behind. Autovacuum will reclaim them; pass --vacuum to do it now.');
            }
        }

        return self::SUCCESS;
    }

    /**
     * Convert every compressed row in [start, end) and move the fence to
     * $start, in ONE transaction.
     *
     * Atomic on purpose, in that direction only. Rows without the fence move
     * is merely wasted work that a re-run repeats harmlessly; the fence moving
     * without its rows is a LIE — the API would offer a search over a span
     * that still holds rows no predicate can see. So they commit together.
     */
    private function convertChunk($conn, Carbon $start, Carbon $end): int
    {
        return $conn->transaction(function () use ($conn, $start, $end): int {
            $from = $start->toDateTimeString();
            $to = $end->toDateTimeString();
            $converted = 0;
            $lastId = 0;

            while (true) {
                // Keyset paging by id: the rows we just converted no longer
                // match `context_z IS NOT NULL`, so an OFFSET would skip and a
                // bare re-select would spin. created_at bounds every statement
                // so the partitioned table prunes to the chunk's children.
                $batch = $conn->select(
                    'SELECT id, context_z FROM nightowl_logs_v2
                     WHERE created_at >= ? AND created_at < ? AND context_z IS NOT NULL AND id > ?
                     ORDER BY id
                     LIMIT '.self::UPDATE_BATCH,
                    [$from, $to, $lastId],
                );

                if ($batch === []) {
                    break;
                }

                $ids = [];
                $values = [];
                $bindings = [];

                foreach ($batch as $row) {
                    $lastId = (int) $row->id;
                    $ids[] = $lastId;

                    $plain = $this->inflate($row->context_z);
                    if ($plain === null) {
                        // A blob we cannot inflate is corrupt — it was already
                        // unreadable before this pass touched it. Leaving it
                        // compressed would strand it in the searchable window
                        // forever (nothing would ever convert it, and the fence
                        // would still claim to cover it), so it converts to a
                        // marker: visible, greppable, and honest about what
                        // happened. Same choice RowHydrator makes on the read
                        // side for the same bytes.
                        $plain = '[corrupt blob]';
                    }

                    $values[] = '(?::bigint, ?::text)';
                    $bindings[] = $lastId;
                    $bindings[] = $plain;
                }

                $conn->update(
                    'UPDATE nightowl_logs_v2 AS t
                     SET context = v.ctx, context_z = NULL
                     FROM (VALUES '.implode(', ', $values).') AS v(id, ctx)
                     WHERE t.id = v.id AND t.created_at >= ? AND t.created_at < ?',
                    array_merge($bindings, [$from, $to]),
                );

                $converted += count($ids);

                if (count($batch) < self::UPDATE_BATCH) {
                    break;
                }
            }

            StorageV2::extendLogContextFence($conn->getPdo(), $from);

            return $converted;
        });
    }

    /**
     * Measure conversion against the tenant's OWN rows instead of guessing.
     *
     * The honest answer to "what will this cost me" is not a rule of thumb:
     * deflate barely pays under ~256 bytes (a small context comes out BIGGER
     * than it went in) and pays well above it, so the delta depends entirely on
     * what this app logs. This inflates a real sample and reports both totals.
     */
    private function dryRun($conn): int
    {
        $sample = max(1, (int) $this->option('sample'));
        $since = $this->option('since');
        $where = 'context_z IS NOT NULL';
        $bindings = [];

        if ($since !== null) {
            $where .= ' AND created_at >= ?';
            $bindings[] = $this->parseUtc($since)->toDateTimeString();
        }

        $totals = $conn->selectOne(
            "SELECT count(*) AS rows, COALESCE(sum(octet_length(context_z)), 0) AS bytes
             FROM nightowl_logs_v2 WHERE {$where}",
            $bindings,
        );

        $rows = (int) $totals->rows;
        if ($rows === 0) {
            $this->info('No compressed log context in range — nothing to convert.');

            return self::SUCCESS;
        }

        // Newest rows rather than a random sample: TABLESAMPLE does not apply
        // to a partitioned parent, ORDER BY random() would scan the whole
        // table, and what an app logs TODAY is the better predictor of what
        // conversion costs going forward anyway.
        $rowsSampled = $conn->select(
            "SELECT context_z FROM nightowl_logs_v2 WHERE {$where}
             ORDER BY created_at DESC LIMIT {$sample}",
            $bindings,
        );

        $compressed = 0;
        $plain = 0;
        $corrupt = 0;

        foreach ($rowsSampled as $row) {
            $blob = $this->rawBlob($row->context_z);
            $compressed += strlen($blob);
            $inflated = @gzinflate($blob);
            if ($inflated === false) {
                $corrupt++;

                continue;
            }
            $plain += strlen($inflated);
        }

        if ($compressed === 0) {
            $this->error('Sampled rows carried no readable context — cannot size this.');

            return self::FAILURE;
        }

        $ratio = $plain / $compressed;
        $storedBytes = (int) $totals->bytes;
        $projected = (int) round($storedBytes * $ratio);

        $this->info('Searchable log context — sizing against your own rows');
        $this->newLine();
        $this->line('  compressed rows in range : '.number_format($rows));
        $this->line('  sampled                  : '.number_format(count($rowsSampled)));
        $this->line('  stored now (context_z)   : '.$this->bytes($storedBytes));
        $this->line('  after conversion         : '.$this->bytes($projected).sprintf('  (%.2fx)', $ratio));
        $this->line('  difference               : '.($projected >= $storedBytes ? '+' : '-').$this->bytes(abs($projected - $storedBytes)));

        if ($corrupt > 0) {
            $this->newLine();
            $this->warn('  '.$corrupt.' of the sampled blobs could not be inflated — they are already unreadable.');
        }

        $this->newLine();
        $this->line('Conversion also rewrites every row in range, which leaves dead tuples until');
        $this->line('autovacuum (or --vacuum) reclaims them — budget for transient disk use above the figure.');
        $this->newLine();
        $this->line('Nothing was changed. Re-run without --dry-run to convert.');

        return self::SUCCESS;
    }

    /**
     * How far back this run should carry the fence, honouring --since.
     *
     * The oldest LOG ROW, not the oldest compressed one. The distinction is
     * the whole recovery path: turning the setting off deletes the fence, and
     * a tenant who had already converted their history then has nothing left
     * to convert — only a fence to walk back. Deriving the floor from
     * `context_z IS NOT NULL` would find no rows, report "nothing to convert",
     * and leave that tenant permanently unsearchable over data that is sitting
     * right there in plaintext. Chunks over already-converted spans convert
     * zero rows and cost one indexed SELECT each, so the walk is cheap.
     */
    private function floor($conn): ?Carbon
    {
        $oldest = $conn->selectOne(
            'SELECT min(created_at) AS at FROM nightowl_logs_v2'
        );

        if ($oldest === null || $oldest->at === null) {
            return null;
        }

        $floor = $this->parseUtc((string) $oldest->at);
        $since = $this->option('since');

        if ($since !== null) {
            $requested = $this->parseUtc($since);
            if ($requested->greaterThan($floor)) {
                $floor = $requested;
            }
        }

        return $floor;
    }

    /**
     * PDO renders bytea as a `\x…` hex string on this driver, but a stream on
     * some builds. Normalize before inflating either way.
     */
    private function rawBlob(mixed $value): string
    {
        if (is_resource($value)) {
            $value = stream_get_contents($value);
        }
        $value = (string) $value;

        if (str_starts_with($value, '\x')) {
            return (string) hex2bin(substr($value, 2));
        }

        return $value;
    }

    private function inflate(mixed $value): ?string
    {
        $inflated = @gzinflate($this->rawBlob($value));

        return $inflated === false ? null : $inflated;
    }

    private function nextWindow(float $current, float $elapsed): float
    {
        $scale = $elapsed > 0.01 ? self::TARGET_CHUNK_SECONDS / $elapsed : 2.0;
        $scale = max(0.25, min(2.0, $scale));

        return max(self::MIN_CHUNK_SECONDS, min(self::MAX_CHUNK_SECONDS, $current * $scale));
    }

    private function parseUtc(string $value): Carbon
    {
        return Carbon::parse($value, 'UTC')->utc();
    }

    private function bytes(int $n): string
    {
        $units = ['B', 'KB', 'MB', 'GB', 'TB'];
        $i = 0;
        $v = (float) $n;
        while ($v >= 1024 && $i < count($units) - 1) {
            $v /= 1024;
            $i++;
        }

        return sprintf('%.1f %s', $v, $units[$i]);
    }
}
