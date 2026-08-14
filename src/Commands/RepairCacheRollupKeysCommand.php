<?php

namespace NightOwl\Commands;

use Carbon\Carbon;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use NightOwl\Support\CacheKeyTemplate;
use NightOwl\Support\CacheRollupSwap;
use NightOwl\Support\RollupTiers;

/**
 * Re-aggregate EXISTING cache rollup rows onto their templated key patterns.
 *
 * The cache rollup is the only one keyed on a raw value (migration 000037's own
 * warning: "`key` is high-cardinality, so this table can approach the raw row
 * count"). Templating (CacheKeyTemplate, config nightowl.cache_key_template)
 * bounds that at DRAIN time — but only for rows written after the upgrade.
 * Rows already on disk keep their literal keys forever, and the hourly/daily
 * tiers keep them for `rollup_tier_retention.hourly_days` / `daily_days`
 * (366 / 1100 by default).
 *
 * That legacy shape is not merely large, it is pathological: a literal cache
 * key is near-unique, so the tier backfill that built hourly/daily from the
 * minute table collapsed almost nothing — measured in the field at a 45.6M-row
 * minute table producing a 45.3M-row hourly and a 45.4M-row daily table, i.e.
 * ~0% collapse, tripling one bloated table into three. Once those tables (and
 * their 4-column PKs) no longer fit in cache, every rollup write goes to disk,
 * the drain slows below the ingest rate, the SQLite buffer fills past
 * NIGHTOWL_MAX_PENDING_ROWS, and the agent starts REJECTING live telemetry.
 *
 * This command repairs that in place, without dropping a single chart point:
 * for every key whose CacheKeyTemplate pattern differs from the key itself, its
 * rows are summed into the pattern's row for the same (store, bucket_start,
 * environment). Counters and total_duration add; min/max fold. Nothing is
 * discarded — only regrouped — so the cache list/charts show the same totals,
 * grouped by key SHAPE the way every post-templating row already is.
 *
 * Protocol, and why it is shaped this way:
 *
 *  - Keys are mapped in PHP. CacheKeyTemplate is the ONLY implementation of the
 *    rule (its own docblock says so); the rollup's SQL form merely reads the
 *    key_pattern column PHP wrote. Porting the rule to SQL would create a second
 *    implementation that can drift — so this command reads distinct keys out,
 *    maps them here, and hands the (key -> pattern) pairs back as a VALUES list.
 *
 *  - Work is batched by KEY, not by time window. The PK is
 *    (key, store, bucket_start, environment), so a batch of keys is an index
 *    prefix probe, and — because the INSERT merges additively via ON CONFLICT —
 *    splitting one pattern's keys across two batches yields the same totals as
 *    doing them together. That is what makes the batch size free to be paced,
 *    which a time window is not: a daily bucket cannot be subdivided.
 *
 *  - Each batch is one transaction holding the SAME advisory lock the drain
 *    pairs with (`nightowl_rollup:<table>`, EXCLUSIVE here, SHARED there under
 *    NIGHTOWL_DB_LOCK_TIMEOUT_MS). A long hold makes every concurrent drain
 *    batch abort 55P03; on a 2.0.0 upgrade a 24s-per-chunk hold had the drain
 *    rejecting 100% of payloads for two hours. So the batch is paced by MEASURED
 *    hold time toward TARGET_CHUNK_SECONDS, exactly as nightowl:backfill-rollups
 *    paces its windows.
 *
 *  - Buckets the drain may still be writing are excluded (SAFETY_MARGIN_SECONDS
 *    behind now, truncated to the tier's own granularity), so the live writer
 *    and this rewrite can never touch the same row.
 *
 *  - Rows whose key already equals its pattern are never rewritten. They are
 *    only ever merged INTO, which makes the pass idempotent: once a literal key
 *    is folded away it no longer exists to be found, and CacheKeyTemplate is
 *    idempotent on its own output (CacheKeyTemplateTest::test_idempotent).
 *
 * This rewrites rows; it does not shrink files. The freed space is reusable by
 * Postgres but stays allocated until a `pg_repack` (online) or `VACUUM FULL`
 * (ACCESS EXCLUSIVE — blocks the drain for the whole rewrite). The command
 * reports sizes and prints the follow-up rather than running either itself.
 *
 * Raw nightowl_cache_events rows are untouched: they keep the literal key by
 * design, and they age out at NIGHTOWL_RETENTION_DAYS regardless.
 */
class RepairCacheRollupKeysCommand extends Command
{
    protected $signature = 'nightowl:repair-cache-rollup-keys
        {--since= : Earliest bucket to repair, UTC unless it carries an offset (default: the table\'s earliest bucket)}
        {--until= : Latest bucket to repair, UTC unless it carries an offset (default: now minus the safety margin)}
        {--tier= : Restrict to one tier: minute, hourly or daily (default: every tier present)}
        {--key-batch=2000 : Distinct keys in the first transaction; paced from there by measured lock hold}
        {--sleep-ms=50 : Pause between transactions, giving the drain the lock}
        {--dry-run : Report the collapse each table would get, writing nothing}
        {--in-place : Merge rows in place instead of rebuilding and swapping — needs no spare disk, but is far slower, leaves dead space until a pg_repack, and skips the bucket the drain is still writing}
        {--chunk-days=7 : Days of buckets aggregated per statement while rebuilding}
        {--force : Skip the confirmation about the disk a rebuild needs}';

    protected $description = 'Collapse literal cache keys in existing cache rollups onto their templated key patterns';

    private const BASE_TABLE = 'nightowl_cache_rollups';

    /**
     * Buckets nearer than this to `now` may still be receiving drain writes, so
     * they are left alone — same margin, same reason, as
     * BackfillRollupsCommand::SAFETY_MARGIN_SECONDS.
     */
    private const SAFETY_MARGIN_SECONDS = 600;

    /** Target advisory-lock hold per transaction; the pacer converges on it. */
    private const TARGET_CHUNK_SECONDS = 1.0;

    private const MIN_KEY_BATCH = 100;

    /**
     * Ceiling on keys per transaction. Also a parameter-count guard: the merge
     * statement binds 2 placeholders per key plus the window bounds, and
     * Postgres caps a prepared statement at 65535 parameters — 20k keys is 40k
     * placeholders, comfortably clear of it. The pacer only ever reaches this
     * ceiling on a table whose batches are finishing well inside TARGET.
     */
    private const MAX_KEY_BATCH = 20_000;

    /** Consecutive failed batches that end a table's pass rather than grind on. */
    private const MAX_CONSECUTIVE_FAILURES = 5;

    /** Wall seconds between progress lines during a long pass. */
    private const PROGRESS_SECONDS = 60;

    /** Batches abandoned to a query error; a non-zero count fails the command. */
    private int $failedBatches = 0;

    /**
     * Ceiling actually applied per repaired table, so the footer can name the
     * buckets this pass deliberately left alone. On the daily tier the 10-minute
     * safety margin rounds down to the current day's bucket, which means a run
     * against a live agent always leaves today's daily row literal-keyed — the
     * operator has to come back for it, so say so instead of letting the counts
     * look complete.
     */
    private array $ceilings = [];

    /** Columns that merge by addition. min/max_duration fold separately. */
    private const SUM_COLUMNS = [
        'call_count', 'hits', 'misses', 'writes', 'deletes', 'fails',
        'delete_failures', 'write_failures', 'total_duration',
    ];

    /**
     * Proportional controller for the next batch size — the integer twin of
     * BackfillRollupsCommand::nextWindow(), clamped to [0.25x, 2x] per step so
     * one anomalous transaction (an autovacuum landing mid-batch, a pooler
     * stall) can neither collapse the batch to nothing nor let it run away.
     */
    private static function nextBatch(int $current, float $elapsed): int
    {
        $scale = $elapsed > 0.01 ? self::TARGET_CHUNK_SECONDS / $elapsed : 2.0;
        $scale = max(0.25, min(2.0, $scale));

        return (int) max(self::MIN_KEY_BATCH, min(self::MAX_KEY_BATCH, round($current * $scale)));
    }

    /** bucket_start is naive UTC wall time; every instant here is born UTC. */
    private function nowUtc(): Carbon
    {
        return Carbon::now('UTC');
    }

    private function parseUtc(string $value): Carbon
    {
        return Carbon::parse($value, 'UTC')->utc();
    }

    public function handle(): int
    {
        $conn = DB::connection('nightowl');
        $dryRun = (bool) $this->option('dry-run');

        $tables = RollupTiers::tables(self::BASE_TABLE);

        $tier = (string) ($this->option('tier') ?? '');
        if ($tier !== '') {
            $wanted = $tier === 'minute' ? self::BASE_TABLE : RollupTiers::table(self::BASE_TABLE, $tier);
            if (! array_key_exists($wanted, $tables)) {
                $this->error("Unknown tier [{$tier}] — expected minute, hourly or daily.");

                return self::FAILURE;
            }
            $tables = [$wanted => $tables[$wanted]];
        }

        // Tier tables are optional (migration 000054); a tenant without them
        // repairs only the minute table. search_path-relative probe, matching
        // every other existence check in the agent.
        $present = [];
        foreach ($tables as $table => $granularity) {
            if ($this->tableExists($conn, $table)) {
                $present[$table] = $granularity;
            }
        }

        if ($present === []) {
            $this->info('No cache rollup tables present — nothing to repair.');

            return self::SUCCESS;
        }

        if (! (bool) config('nightowl.cache_key_template', true)) {
            $this->warn('nightowl.cache_key_template is OFF: the drain keeps writing literal keys, so new rows will re-grow the cardinality this pass collapses. Turn it on before repairing.');
        }

        $this->line($dryRun
            ? 'Dry run — reporting the collapse, writing nothing.'
            : 'Repairing cache rollup key cardinality (safe to run against a live agent).');

        if (! $dryRun && ! (bool) $this->option('in-place')) {
            // A rebuild re-aggregates the WHOLE table — it has no time cursor to
            // bound, so a --since/--until here would be silently ignored and the
            // operator would believe a window was respected that never was.
            foreach (['since', 'until'] as $bound) {
                if ($this->option($bound) !== null) {
                    $this->error("--{$bound} only applies to --in-place; a rebuild re-aggregates the whole table.");

                    return self::FAILURE;
                }
            }

            return $this->rebuildAll($conn, array_keys($present));
        }

        $rewrote = false;
        foreach ($present as $table => $granularity) {
            $rewrote = $this->repairTable($conn, $table, $granularity, $dryRun) || $rewrote;
        }

        if ($rewrote) {
            $this->newLine();
            $this->line('Rows were rewritten in place. Postgres can reuse the freed space but will not');
            $this->line('return it to the filesystem until the tables are rewritten physically:');
            $width = max(array_map('strlen', array_keys($present)));
            foreach (array_keys($present) as $table) {
                $this->line(sprintf('  pg_repack -t %s   # online, no exclusive lock', str_pad($table, $width)));
            }
            $this->line('VACUUM FULL does the same without pg_repack, but takes ACCESS EXCLUSIVE and');
            $this->line('blocks the drain for the whole rewrite — schedule it, do not run it live.');

            if ($this->ceilings !== []) {
                $this->newLine();
                $this->line('Buckets the drain may still be writing were left untouched:');
                foreach ($this->ceilings as $table => $ceiling) {
                    $this->line("  {$table}: at or after {$ceiling} UTC");
                }
                $this->line('Re-run once those buckets close to collapse the tail — on the daily tier that');
                $this->line("is tomorrow, and until then today's daily row keeps its literal keys.");
            }
        }

        if ($this->failedBatches > 0) {
            $this->warn(sprintf(
                '%d batch(es) failed and were skipped. The pass is idempotent — re-run it to pick them up.',
                $this->failedBatches,
            ));

            return self::FAILURE;
        }

        return self::SUCCESS;
    }

    /**
     * Rebuild-and-swap: build a correct table beside each broken one and swap the
     * names. Default because the in-place merge's costs are all borne by the
     * customer — hours of paced transactions, a table still full of dead tuples
     * afterwards, and a second visit for the bucket the drain was writing.
     *
     * What it asks in return is disk: while a table is being rebuilt, the database
     * holds both copies. The agent cannot see the database server's filesystem
     * (BYO Postgres is frequently a different host), so this reports what will be
     * needed and asks, rather than pretending to have checked.
     *
     * @param  list<string>  $tables
     */
    private function rebuildAll($conn, array $tables): int
    {
        $sizes = [];
        $largest = 0;
        foreach ($tables as $table) {
            $sizes[$table] = $this->relationSize($conn, $table);
            $largest = max($largest, $sizes[$table]);
        }

        $this->line('Rebuilding each cache rollup table and swapping it in.');
        foreach ($tables as $table) {
            $this->line(sprintf('  %s is %s', $table, $this->humanBytes($sizes[$table])));
        }
        $this->line(sprintf(
            'Tables are rebuilt one at a time, so the database needs about %s free while this runs.',
            $this->humanBytes($largest),
        ));

        if (! (bool) $this->option('force')
            && $this->input->isInteractive()
            && ! $this->confirm('Continue?', true)) {
            $this->line('Nothing was changed. Re-run with --in-place to repair without the extra space.');

            return self::SUCCESS;
        }

        $work = $this->workConnection();
        $chunkDays = max(1, (int) $this->option('chunk-days'));
        $lockTimeout = (int) config('nightowl.drain_connection.lock_timeout_ms', 10000);

        foreach ($tables as $table) {
            $this->line("  {$table}:");
            $swap = new CacheRollupSwap($conn, $work, $lockTimeout, function (string $line): void {
                $this->line($line);
            });

            $started = microtime(true);
            try {
                $stats = $swap->rebuild($table, $chunkDays);
            } catch (\Throwable $e) {
                $this->failedBatches++;
                $this->error("    rebuild failed, table left untouched: {$e->getMessage()}");

                continue;
            }

            $this->line(sprintf(
                '    %s rows → %s (%s captured from the live drain), %.1fs total, %.3fs holding the lock.',
                number_format($stats['rows_before']),
                number_format($stats['rows_after']),
                number_format($stats['delta_rows']),
                microtime(true) - $started,
                $stats['swap_seconds'],
            ));
            $this->line(sprintf(
                '    size %s → %s (reclaimed, no repack needed).',
                $this->humanBytes($sizes[$table]),
                $this->humanBytes($this->relationSize($conn, $table)),
            ));
        }

        if ($this->failedBatches > 0) {
            $this->warn(sprintf('%d table(s) could not be rebuilt and were left as they were.', $this->failedBatches));

            return self::FAILURE;
        }

        return self::SUCCESS;
    }

    /**
     * A second session, because a transaction can import a snapshot only while the
     * transaction that exported it is still open — one connection cannot be both.
     */
    private function workConnection()
    {
        $config = config('database.connections.nightowl');
        config(['database.connections.nightowl_rebuild' => $config]);
        DB::purge('nightowl_rebuild');

        return DB::connection('nightowl_rebuild');
    }

    /**
     * The key scan is the one query in this command whose plan is not obvious.
     * `SELECT DISTINCT key ... ORDER BY key LIMIT n` is meant to walk the PK's
     * leading column and stop after n distinct entries; if the planner instead
     * picks a sequential scan and a hash aggregate, EVERY page costs a full scan
     * of a table that is bloated by definition — which on the tables this
     * command exists to repair means hours per batch instead of a second.
     * Cheap to check, so check it and say so rather than let a pass grind.
     */
    private function warnOnUnindexedKeyScan($conn, string $table, int $batch): void
    {
        // Only on a table where the plan can actually hurt. Below this a
        // sequential scan is the CORRECT plan and the warning would be pure
        // noise on every small tenant.
        $estimated = (float) $conn->selectOne(
            'SELECT COALESCE(reltuples, 0) AS n FROM pg_class WHERE oid = ?::regclass',
            [$table],
        )->n;

        if ($estimated < 1_000_000) {
            return;
        }

        try {
            $rows = $conn->select("EXPLAIN SELECT DISTINCT key FROM {$table} ORDER BY key LIMIT {$batch}");
        } catch (\Throwable) {
            return;
        }

        $plan = implode("\n", array_map(
            static fn ($row): string => (string) (array_values((array) $row)[0] ?? ''),
            $rows,
        ));

        if (! str_contains($plan, 'Index Scan') && ! str_contains($plan, 'Index Only Scan')) {
            $this->warn("    {$table}: the distinct-key scan is not using an index — every batch will scan the whole table. Check that the primary key on (key, store, bucket_start, environment) is present and not invalid, then re-run.");
        }
    }

    private function tableExists($conn, string $table): bool
    {
        return (bool) $conn->selectOne('SELECT to_regclass(?) IS NOT NULL AS e', [$table])->e;
    }

    /** @return bool whether this table was actually rewritten */
    private function repairTable($conn, string $table, int $granularitySeconds, bool $dryRun): bool
    {
        $ceiling = RollupTiers::truncateBucket(
            $this->nowUtc()->subSeconds(self::SAFETY_MARGIN_SECONDS)->toDateTimeString(),
            $granularitySeconds,
        );

        $until = $this->option('until') ? $this->parseUtc((string) $this->option('until')) : $this->parseUtc($ceiling);
        if ($until->greaterThan($this->parseUtc($ceiling))) {
            $until = $this->parseUtc($ceiling);
        }

        $earliest = $this->option('since')
            ? (string) $this->option('since')
            : (string) ($conn->table($table)->min('bucket_start') ?? '');

        if ($earliest === '') {
            $this->line("  {$table}: empty — nothing to repair.");

            return false;
        }

        $since = $this->parseUtc($earliest);
        if ($since->greaterThanOrEqualTo($until)) {
            $this->line("  {$table}: window is empty after clamping to the drain safety margin — skipped.");

            return false;
        }

        $from = $since->toDateTimeString();
        $to = $until->toDateTimeString();
        $this->ceilings[$table] = $to;
        $sizeBefore = $this->relationSize($conn, $table);

        $batch = (int) max(self::MIN_KEY_BATCH, min(self::MAX_KEY_BATCH, (int) $this->option('key-batch')));
        $sleepUs = max(0, (int) $this->option('sleep-ms')) * 1000;

        $this->warnOnUnindexedKeyScan($conn, $table, $batch);

        // A repair over a table this command exists for runs for hours. Say so
        // periodically: silence is indistinguishable from a wedge.
        $lastReport = microtime(true);
        $lastKey = null;
        $consecutiveFailures = 0;
        $keysScanned = 0;
        $keysTemplated = 0;
        $rowsMerged = 0;
        $groupsWritten = 0;
        $transactions = 0;
        $longestHold = 0.0;

        while (true) {
            // Keyset pagination on the PK's LEADING column, deliberately without
            // the bucket_start predicate: `key` alone is an index prefix, so each
            // page is an index scan; adding a non-prefix filter would push a
            // narrow --since/--until window onto the bucket index and force a
            // sort of every matching row to satisfy DISTINCT ... ORDER BY key.
            // The window is applied where it must be — in the merge and DELETE.
            $keys = $lastKey === null
                ? $conn->select("SELECT DISTINCT key FROM {$table} ORDER BY key LIMIT {$batch}")
                : $conn->select("SELECT DISTINCT key FROM {$table} WHERE key > ? ORDER BY key LIMIT {$batch}", [$lastKey]);

            if ($keys === []) {
                break;
            }

            $lastKey = (string) $keys[array_key_last($keys)]->key;
            $keysScanned += count($keys);

            $pairs = [];
            foreach ($keys as $row) {
                $raw = (string) $row->key;
                $pattern = CacheKeyTemplate::template($raw);
                if ($pattern !== $raw) {
                    $pairs[] = [$raw, $pattern];
                }
            }

            if ($pairs === []) {
                // Already-templated (or literal-by-shape) keys: nothing to move,
                // no transaction, no lock. This is the whole steady state on a
                // repaired tenant, which is what makes re-running the pass cheap.
                continue;
            }

            // A batch that dies (lock timeout, statement timeout, a deadlock
            // with the drain) must not end a pass that may run for hours: the
            // transaction rolled back, so the rows are exactly as they were, and
            // the pass is idempotent — a re-run picks the batch up. Back the
            // batch size off so a size-driven timeout doesn't repeat, and give
            // up on the table only if the failures are relentless.
            $startedAt = microtime(true);
            try {
                $result = $dryRun
                    ? $this->projectBatch($conn, $table, $pairs, $from, $to)
                    : $this->mergeBatch($conn, $table, $pairs, $from, $to);
            } catch (\Throwable $e) {
                $this->failedBatches++;
                $consecutiveFailures++;
                $batch = (int) max(self::MIN_KEY_BATCH, floor($batch / 2));
                $this->warn(sprintf('    %s: batch ending at [%s] failed, skipped: %s', $table, $lastKey, $e->getMessage()));

                if ($consecutiveFailures >= self::MAX_CONSECUTIVE_FAILURES) {
                    $this->warn(sprintf('    %s: %d batches failed in a row — abandoning this table.', $table, $consecutiveFailures));

                    break;
                }

                continue;
            }
            $held = microtime(true) - $startedAt;
            $consecutiveFailures = 0;

            // Counted on SUCCESS only: a skipped batch's keys are still literal,
            // so reporting them as templated would overstate what the pass did.
            $keysTemplated += count($pairs);
            $rowsMerged += $result['rows'];
            $groupsWritten += $result['groups'];
            $transactions++;
            $longestHold = max($longestHold, $held);

            $batch = self::nextBatch($batch, $held);

            if (microtime(true) - $lastReport >= self::PROGRESS_SECONDS) {
                $this->line(sprintf(
                    '    %s: %d keys scanned, %d templated, %d rows merged, batch %d, at [%s]',
                    $table, $keysScanned, $keysTemplated, $rowsMerged, $batch, $lastKey,
                ));
                $lastReport = microtime(true);
            }

            if ($sleepUs > 0) {
                usleep($sleepUs);
            }
        }

        if ($keysTemplated === 0) {
            $this->line(sprintf('  %s: %d distinct keys, none templated — already repaired.', $table, $keysScanned));

            return false;
        }

        $this->line(sprintf(
            '  %s: %d/%d distinct keys templated, %d rows %s into at most %d pattern groups, %d transaction(s), longest lock hold %.2fs.',
            $table,
            $keysTemplated,
            $keysScanned,
            $rowsMerged,
            $dryRun ? 'would collapse' : 'collapsed',
            $groupsWritten,
            $transactions,
            $longestHold,
        ));

        // "At most": a pattern whose keys straddled two batches is counted once
        // per batch, and a group merging onto a row that already existed removes
        // one more row than this arithmetic shows. The real row count only ever
        // comes out lower, never higher.
        if (! $dryRun) {
            $this->line(sprintf(
                '    size %s → %s (dead space until repacked).',
                $this->humanBytes($sizeBefore),
                $this->humanBytes($this->relationSize($conn, $table)),
            ));
        }

        return ! $dryRun;
    }

    /**
     * One batch, transactionally: snapshot the merged groups, delete the literal
     * rows, then add the groups back.
     *
     * DELETE before INSERT, so the safety does not rest on an argument. Today no
     * mapped key can also be another key's pattern — template() is idempotent
     * (CacheKeyTemplateTest::test_idempotent), so a pattern is always its own
     * pattern and never enters the map — which means an INSERT-first order
     * happens to be safe too. But it is safe only BECAUSE of that property: were
     * it ever to weaken, inserting first would let the DELETE carry off counters
     * the INSERT had just merged onto a row. Deleting first cannot, whatever the
     * rule does: the merged totals are already materialised in the staging
     * table, read from this transaction's own snapshot.
     *
     * The three statements cannot be collapsed into one data-modifying CTE
     * either: a WITH-DELETE and its sibling INSERT share a command id, so the
     * INSERT's uniqueness check still sees the deleted rows as live and raises a
     * duplicate key error on exactly the rows being replaced.
     *
     * @param  list<array{0: string, 1: string}>  $pairs
     * @return array{rows: int, groups: int}
     */
    private function mergeBatch($conn, string $table, array $pairs, string $from, string $to): array
    {
        return $conn->transaction(function () use ($conn, $table, $pairs, $from, $to): array {
            $conn->statement('SELECT pg_advisory_xact_lock(hashtext(?))', ['nightowl_rollup:'.$table]);

            [$mapSql, $mapBind] = $this->mapValues($pairs);
            [$keySql, $keyBind] = $this->keyValues($pairs);

            $sums = implode(', ', array_map(
                static fn (string $c): string => "SUM(t.{$c})",
                self::SUM_COLUMNS,
            ));

            $columns = implode(', ', $this->allColumns());

            // Bare LIKE — columns, types and NOT NULLs, no index, no PK: the
            // GROUP BY already guarantees one row per conflict key, and the
            // staging table is written once and read once. Split from the
            // INSERT rather than written as CREATE TABLE AS SELECT so the
            // parameterised half stays an ordinary optimizable statement.
            $conn->statement("CREATE TEMP TABLE nightowl_cache_repair_merge (LIKE {$table}) ON COMMIT DROP");

            $conn->statement(
                "INSERT INTO nightowl_cache_repair_merge ({$columns})
                 SELECT m.pattern, t.store, t.bucket_start, t.environment,
                        {$sums},
                        MIN(t.min_duration),
                        MAX(t.max_duration)
                 FROM {$table} t
                 JOIN (VALUES {$mapSql}) AS m(raw_key, pattern) ON m.raw_key = t.key
                 WHERE t.bucket_start >= ? AND t.bucket_start < ?
                 GROUP BY 1, 2, 3, 4",
                [...$mapBind, $from, $to],
            );

            $rows = $conn->delete(
                "DELETE FROM {$table} t
                 USING (VALUES {$keySql}) AS m(raw_key)
                 WHERE t.key = m.raw_key AND t.bucket_start >= ? AND t.bucket_start < ?",
                [...$keyBind, $from, $to],
            );

            // Additive ON CONFLICT: a pattern row may already exist (written by a
            // post-templating drain, or by an earlier batch of this same pass),
            // and its counters must survive. LEAST/GREATEST ignore NULLs, which
            // is exactly the null-aware fold min/max_duration need.
            $updates = array_map(
                static fn (string $c): string => "{$c} = {$table}.{$c} + EXCLUDED.{$c}",
                self::SUM_COLUMNS,
            );
            $updates[] = "min_duration = LEAST({$table}.min_duration, EXCLUDED.min_duration)";
            $updates[] = "max_duration = GREATEST({$table}.max_duration, EXCLUDED.max_duration)";

            $groups = $conn->affectingStatement(
                "INSERT INTO {$table} ({$columns})
                 SELECT {$columns} FROM nightowl_cache_repair_merge
                 ON CONFLICT (key, store, bucket_start, environment) DO UPDATE SET ".implode(', ', $updates)
            );

            return ['rows' => (int) $rows, 'groups' => (int) $groups];
        });
    }

    /**
     * The same measurement without the write — and without the advisory lock,
     * since a dry run has no reason to make the drain wait.
     *
     * @param  list<array{0: string, 1: string}>  $pairs
     * @return array{rows: int, groups: int}
     */
    private function projectBatch($conn, string $table, array $pairs, string $from, string $to): array
    {
        [$mapSql, $mapBind] = $this->mapValues($pairs);

        $row = $conn->selectOne(
            "SELECT count(*) AS matched,
                    count(DISTINCT (m.pattern, t.store, t.bucket_start, t.environment)) AS groups
             FROM {$table} t
             JOIN (VALUES {$mapSql}) AS m(raw_key, pattern) ON m.raw_key = t.key
             WHERE t.bucket_start >= ? AND t.bucket_start < ?",
            [...$mapBind, $from, $to],
        );

        return ['rows' => (int) $row->matched, 'groups' => (int) $row->groups];
    }

    /** @return list<string> */
    private function allColumns(): array
    {
        return ['key', 'store', 'bucket_start', 'environment', ...self::SUM_COLUMNS, 'min_duration', 'max_duration'];
    }

    /**
     * (key, pattern) as a VALUES list. Explicitly ::text so the join against a
     * varchar(255) column resolves the same way on every server, and so an
     * all-numeric key can never be inferred as an integer.
     *
     * @param  list<array{0: string, 1: string}>  $pairs
     * @return array{0: string, 1: list<string>}
     */
    private function mapValues(array $pairs): array
    {
        $bind = [];
        foreach ($pairs as [$raw, $pattern]) {
            $bind[] = $raw;
            $bind[] = $pattern;
        }

        return [implode(', ', array_fill(0, count($pairs), '(?::text, ?::text)')), $bind];
    }

    /**
     * @param  list<array{0: string, 1: string}>  $pairs
     * @return array{0: string, 1: list<string>}
     */
    private function keyValues(array $pairs): array
    {
        $bind = array_map(static fn (array $pair): string => $pair[0], $pairs);

        return [implode(', ', array_fill(0, count($pairs), '(?::text)')), $bind];
    }

    private function relationSize($conn, string $table): int
    {
        return (int) $conn->selectOne('SELECT pg_total_relation_size(?::regclass) AS bytes', [$table])->bytes;
    }

    private function humanBytes(int $bytes): string
    {
        $units = ['B', 'KB', 'MB', 'GB', 'TB'];
        $i = 0;
        $value = (float) $bytes;
        while ($value >= 1024 && $i < count($units) - 1) {
            $value /= 1024;
            $i++;
        }

        return sprintf('%.1f %s', $value, $units[$i]);
    }
}
