<?php

namespace NightOwl\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

/**
 * One-off reclamation of orphaned rows in the route dictionary.
 *
 * A row in nightowl_dict_route holds one distinct route tuple, and the only thing
 * that references it is a nightowl_requests_v2 row's route_id. Request rows age
 * out at retention (nightowl:prune drops their partitions), so a route no request
 * points at any more is dead weight.
 *
 * ── THE SAFETY CONTRACT, STATED HONESTLY ────────────────────────────────────
 *
 * The agent must be stopped — everywhere it runs. A drain worker warms a
 * dictionary row in autocommit BEFORE its batch transaction and COPYs the
 * referencing request inside it, on EVERY batch — so a live drain the probes
 * cannot see mints rows while this command runs (any batch in flight when the
 * lock below lands included) that nothing references yet, and its LRU keeps
 * ids for the life of the process. No re-check inside this command can
 * protect either. A stopped agent has no cache and mints nothing; that is the
 * protection.
 *
 * Two in-band measures narrow the damage from an agent the probes cannot see:
 *
 *  - `LOCK TABLE nightowl_requests_v2 IN SHARE MODE` for the whole run. SHARE
 *    conflicts with the ROW EXCLUSIVE every INSERT/COPY takes, so a writer
 *    mid-batch makes the lock WAIT (it is a live-agent detector: a lock not
 *    granted within 30 seconds is treated as "something is writing" and the
 *    run refuses) and a writer arriving during the run has its COPY stalled,
 *    not raced. Its WARM is not stalled — autocommit, into nightowl_dict_route,
 *    which this lock does not cover — so a batch that warmed just before the
 *    lock was granted is the cached-id case above with a one-batch window.
 *    Under READ COMMITTED each statement sees a fresh snapshot, so
 *    without the lock a reference committed during the scan was deleted by the
 *    later DELETE — an earlier version removed the anti-join re-check on the
 *    reasoning that it "could not see uncommitted rows", which was true and
 *    beside the point.
 *  - Neither measure reaches the cached-id case above. Nothing can.
 *
 * `--force` skips the CONFIRMATION, never the probes and never the lock. The
 * probes are blind to the sync driver (`--driver=sync` writes straight to
 * Postgres: no health port, no drain worker, no buffer traffic) and to an
 * agent on another host; the confirmation text says so.
 *
 * ── WHY IT IS NOT ON A TIMER ────────────────────────────────────────────────
 *
 * nightowl:gc-dict-traces runs against a live daemon because dict_trace carries a
 * created_at clock the drain touches on every referencing batch, and because its
 * collector never trusts the LRU. Giving routes that treatment costs an
 * insert-with-touch plus a select-back on EVERY batch forever — ~0.5 ms against a
 * local socket, 0.2-0.4 s against a database a ~100 ms hop away — and puts row
 * locks on the handful of hot route rows every drain worker touches every batch.
 */
class GcDictRoutesCommand extends Command
{
    protected $signature = 'nightowl:gc-dict-routes
        {--dry-run : Report how many routes would be reclaimed without deleting}
        {--chunk=5000 : Rows per DELETE statement}
        {--force : Skip the confirmation (never skips the liveness probes or the lock)}
        {--sqlite-path= : Buffer path if the agent was started with --sqlite-path}';

    protected $description = 'Reclaim orphaned rows from the storage-v2 route dictionary (agent must be stopped)';

    /** Exit code for "the operator said no" — distinct from a failed run. */
    public const DECLINED = 3;

    /**
     * Upper bound on --chunk. The paging binds two parameters per statement
     * whatever the chunk, so this is not the bind-parameter ceiling an earlier
     * version hit at 65 535; it caps how many rows one DELETE may lock at once.
     */
    private const MAX_CHUNK = 50000;

    /**
     * How recently the agent must have touched something before this refuses.
     * Wider than it looks: DrainWorker stops writing its metrics file while it
     * is inside long work, and NIGHTOWL_DRAIN_WEDGE_WARN_SECONDS defaults to 180,
     * so a window shorter than that reads a WEDGED worker as stopped — and a
     * wedged worker is the worst case, because it flushes with pre-GC ids when it
     * unwedges. The cost is that a normal shutdown leaves files this fresh, so
     * the command refuses for this long after "stop the agent"; the refusal says
     * how long.
     */
    private const LIVENESS_WINDOW_SECONDS = 600;

    /**
     * How long the SHARE lock may wait before "something is writing" is the
     * verdict. A property, not a const, so a test can shorten it.
     */
    private int $lockWaitSeconds = 30;

    private const TMP_TABLE = 'nightowl_gc_route_orphans';

    /** Observability for tests: how many DELETE statements the last run issued, and the chunk it used. */
    private int $deleteStatements = 0;

    private int $effectiveChunk = 0;

    public function handle(): int
    {
        $conn = DB::connection('nightowl');
        $pdo = $conn->getPdo();

        // On the Laravel `nightowl` connection, which the provider pins to public
        // (`schema => public`) — unlike the drain's raw PDO. Fine: this command's
        // reads and deletes go through the same pinned connection.
        $dictExists = (bool) $pdo->query("SELECT to_regclass('nightowl_dict_route') IS NOT NULL AS e")->fetchColumn();
        $reqExists = (bool) $pdo->query("SELECT to_regclass('nightowl_requests_v2') IS NOT NULL AS e")->fetchColumn();
        if (! $dictExists || ! $reqExists) {
            $this->info('nightowl_dict_route / nightowl_requests_v2 not present — nothing to GC.');

            return self::SUCCESS;
        }

        $dryRun = (bool) $this->option('dry-run');

        // The probes run for a dry run too — it holds ACCESS SHARE on every request
        // partition for the length of its scan, which is enough to queue a pending
        // DROP from nightowl:prune ahead of the drain's inserts. A dry run only
        // WARNS on a live agent; the real run refuses.
        $alive = $this->evidenceOfLiveAgent();
        if ($alive !== null) {
            if (! $dryRun) {
                return $this->refuse($alive);
            }
            $this->warn("Note: {$alive}. A dry run does not delete, but its scan holds share locks on the request history while it runs.");
        } elseif (! $dryRun && ! $this->option('force')) {
            if (! $this->confirmAgentStopped()) {
                $this->line('Aborted: not confirmed. Nothing was changed.');

                return self::DECLINED;
            }
        }

        // One transaction: the temp table must stay on the backend that reads it
        // (a transaction-mode pooler guarantees that only inside a transaction),
        // the SHARE lock has to span scan and delete, and the reclamation is
        // all-or-nothing.
        $pdo->beginTransaction();

        try {
            if (! $dryRun) {
                $pdo->exec(sprintf("SET LOCAL lock_timeout = '%ds'", $this->lockWaitSeconds));
                try {
                    $pdo->exec('LOCK TABLE nightowl_requests_v2 IN SHARE MODE');
                } catch (\PDOException $e) {
                    if (($e->errorInfo[0] ?? '') === '55P03') {
                        $pdo->rollBack();

                        return $this->refuse(sprintf(
                            'a writer is holding nightowl_requests_v2 (the share lock was not granted within %ds)',
                            $this->lockWaitSeconds
                        ));
                    }
                    throw $e;
                }
            }

            $this->line('Scanning for unreferenced routes (one pass over the request history)…');
            $started = microtime(true);

            // Materialised ONCE, dropped with the transaction. COALESCE per column,
            // not around the sum: pg_column_size(NULL) is NULL and NULL propagates
            // through +, and domain/name are NULL on most routes — so an earlier
            // estimate printed "0 bytes" on every real tenant.
            $pdo->exec(
                'CREATE TEMP TABLE '.self::TMP_TABLE.' ON COMMIT DROP AS
                 SELECT d.id,
                        COALESCE(pg_column_size(d.hash), 0) + COALESCE(pg_column_size(d.method), 0)
                      + COALESCE(pg_column_size(d.domain), 0) + COALESCE(pg_column_size(d.path), 0)
                      + COALESCE(pg_column_size(d.name), 0) + COALESCE(pg_column_size(d.action), 0)
                      + COALESCE(pg_column_size(d.methods), 0) AS bytes
                 FROM nightowl_dict_route d
                 WHERE NOT EXISTS (
                     SELECT 1 FROM nightowl_requests_v2 r WHERE r.route_id = d.id
                 )'
            );
            $pdo->exec('CREATE UNIQUE INDEX ON '.self::TMP_TABLE.' (id)');

            $this->line(sprintf('Scan finished in %.1fs.', microtime(true) - $started));

            $summary = $pdo->query(
                'SELECT COUNT(*) AS n, pg_size_pretty(COALESCE(SUM(bytes), 0)) AS freed FROM '.self::TMP_TABLE
            )->fetch(\PDO::FETCH_ASSOC);

            $orphans = (int) $summary['n'];
            $total = (int) $pdo->query('SELECT COUNT(*) FROM nightowl_dict_route')->fetchColumn();

            if ($orphans === 0) {
                $this->info("No orphaned routes ({$total} route(s) all still referenced).");
                $pdo->rollBack();

                return self::SUCCESS;
            }

            if ($dryRun) {
                $this->info(sprintf(
                    '%d of %d route(s) are unreferenced and would be reclaimed (dry run). '
                    .'About %s of column data, before row and index overhead.',
                    $orphans, $total, $summary['freed'],
                ));
                $pdo->rollBack();

                return self::SUCCESS;
            }

            $deleted = $this->deleteInChunks($pdo);

            $pdo->commit();

            $this->info(sprintf(
                'Reclaimed %d orphaned route(s) of %d (about %s). The rows were interleaved with live '
                .'ones, so plain VACUUM will not shrink the file — run VACUUM FULL nightowl_dict_route '
                .'(brief exclusive lock) or pg_repack to return the space.',
                $deleted, $total, $summary['freed'],
            ));

            return self::SUCCESS;
        } catch (\Throwable $e) {
            try {
                if ($pdo->inTransaction()) {
                    $pdo->rollBack();
                }
            } catch (\Throwable) {
                // Handle already dead; never mask the real error.
            }

            $this->error('Reclamation failed and was rolled back: '.$e->getMessage());

            return self::FAILURE;
        }
    }

    /**
     * Page over the temp table's OWN ids, so the number of statements is
     * proportional to the rows being deleted — not to the span of the dictionary's
     * id space. An earlier version walked the PK range in fixed steps and issued
     * ~20 000 round trips to delete 200 rows scattered across a 100M id space.
     * Two bound parameters per statement whatever the chunk size.
     */
    private function deleteInChunks(\PDO $pdo): int
    {
        $chunk = max(1, min(self::MAX_CHUNK, (int) $this->option('chunk')));
        $this->effectiveChunk = $chunk;
        $this->deleteStatements = 0;

        $page = $pdo->prepare(
            'SELECT MAX(id) AS hi, COUNT(*) AS n
             FROM (SELECT id FROM '.self::TMP_TABLE.' WHERE id > ? ORDER BY id LIMIT ?) p'
        );
        $delete = $pdo->prepare(
            'DELETE FROM nightowl_dict_route d
             WHERE d.id > ? AND d.id <= ?
               AND EXISTS (SELECT 1 FROM '.self::TMP_TABLE.' t WHERE t.id = d.id)'
        );

        $deleted = 0;
        $last = PHP_INT_MIN; // bigserial never mints <= 0, but a hand-inserted id must not be skipped
        while (true) {
            $page->execute([$last, $chunk]);
            $p = $page->fetch(\PDO::FETCH_ASSOC);
            if ((int) $p['n'] === 0) {
                break;
            }
            $hi = (int) $p['hi'];
            $delete->execute([$last, $hi]);
            $this->deleteStatements++;
            $deleted += $delete->rowCount();
            $last = $hi;
        }

        return $deleted;
    }

    /**
     * Something that says an agent is alive, or null. Three probes, each blind
     * somewhere: the health port is optional and never started by the sync
     * driver; the metrics file exists only for the async drain worker, and only
     * under this config's sqlite path; the buffer is written by ingest, so it
     * stays fresh even while the drain is wedged — but it is also written by a
     * normal shutdown, which is why the window is stated in the refusal.
     */
    private function evidenceOfLiveAgent(): ?string
    {
        if ($this->agentHealthResponds()) {
            return 'a NightOwl agent is answering on its health port';
        }

        $age = $this->fileAgeSeconds($this->sqlitePath().'.drain-metrics*.json');
        if ($age !== null && $age < self::LIVENESS_WINDOW_SECONDS) {
            return sprintf('a drain worker wrote metrics %ds ago (wait %ds after stopping the agent)', $age, self::LIVENESS_WINDOW_SECONDS - $age);
        }

        $age = $this->fileAgeSeconds($this->sqlitePath().'*');
        if ($age !== null && $age < self::LIVENESS_WINDOW_SECONDS) {
            return sprintf('the ingest buffer was written %ds ago (wait %ds after stopping the agent)', $age, self::LIVENESS_WINDOW_SECONDS - $age);
        }

        return null;
    }

    private function confirmAgentStopped(): bool
    {
        $this->warn('This deletes dictionary rows and must not run while the agent is draining.');
        $this->line('  A running agent caches route ids in memory. Deleting a row underneath it leaves');
        $this->line('  later requests pointing at a route that no longer exists. The checks above cannot');
        $this->line('  see an agent on ANOTHER host, nor one started with --driver=sync (it has no health');
        $this->line('  port, no drain worker and no buffer traffic), so confirm it yourself.');

        return $this->confirm('Is the NightOwl agent stopped everywhere it runs?', false);
    }

    private function refuse(string $because): int
    {
        $this->error('Refusing to run — '.$because.'.');
        $this->line('  Stop the agent everywhere it runs, then run this command, then start it again.');
        $this->line('  --force skips the confirmation, not this check.');

        return self::FAILURE;
    }

    private function sqlitePath(): string
    {
        $override = $this->option('sqlite-path');
        if (is_string($override) && $override !== '') {
            return $override;
        }

        return (string) config('nightowl.agent.sqlite_path', storage_path('nightowl/agent-buffer.sqlite'));
    }

    /**
     * Seconds since the newest file matching $prefix.$suffixGlob was written, or
     * null if there is none. The PREFIX is a path and is escaped: glob() treats
     * `[`, `]`, `*`, `?` and `{` as metacharacters, so a directory named
     * `app-[prod]` made every probe return nothing and the command proceed.
     */
    private function fileAgeSeconds(string $pattern): ?int
    {
        $dir = dirname($pattern);
        $base = basename($pattern);
        $escapedDir = preg_replace('/[\\[\\]*?{}\\\\]/', '\\\\$0', $dir);

        $newest = null;
        foreach (glob($escapedDir.DIRECTORY_SEPARATOR.$base) ?: [] as $file) {
            $mtime = @filemtime($file);
            if ($mtime !== false && ($newest === null || $mtime > $newest)) {
                $newest = $mtime;
            }
        }

        return $newest === null ? null : max(0, time() - $newest);
    }

    private function agentHealthResponds(): bool
    {
        if (! config('nightowl.agent.health_enabled', true)) {
            return false;
        }

        $host = (string) config('nightowl.agent.host', '127.0.0.1');
        // 0.0.0.0 is a bind address, not a destination.
        if ($host === '0.0.0.0' || $host === '::') {
            $host = '127.0.0.1';
        }

        $socket = @fsockopen($host, (int) config('nightowl.agent.health_port', 2409), $errno, $errstr, 1.0);
        if ($socket === false) {
            return false;
        }
        fclose($socket);

        return true;
    }
}
