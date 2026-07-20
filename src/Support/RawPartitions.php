<?php

namespace NightOwl\Support;

use PDO;

/**
 * Native declarative partitioning for the RAW telemetry tables, by created_at,
 * daily children. Turns nightowl:prune's daily multi-million-row DELETE storm
 * (WAL + vacuum debt + bloat, forever) into instant DROP PARTITION, and gives
 * window scans partition pruning.
 *
 * Scope: all 11 raw tables. nightowl_logs needs a created_at varchar→timestamp
 * rewrite first — normalizeCreatedAtType() handles it inside convert().
 *
 * Children: {table}_pYYYYMMDD (one UTC day), {table}_phistoric (the original
 * table attached as-is under a validated CHECK — zero row copying), and
 * {table}_pdefault (catch-all for backdated drains, which event-time bucketing
 * permits up to ~366d).
 *
 * The PK becomes (id, created_at) — Postgres requires the partition key in
 * every unique constraint. `id` is synthetic everywhere (audited 2026-07-17:
 * the only reader is the deletion chunker, which needs the column, not
 * uniqueness-per-table), so widening it changes nothing observable.
 */
final class RawPartitions
{
    public const TABLES = [
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
        // Logs joins via a type pre-step: its created_at is historically a
        // nullable varchar (the writer always stamped valid UTC strings), so
        // convert() rewrites the column to timestamp first. On a populated
        // table that rewrite is a full-table ACCESS EXCLUSIVE pass — the
        // reason nightowl:partition is operator-run.
        'nightowl_logs',
    ];

    /** How many future daily children to keep pre-created. */
    public const DAYS_AHEAD = 7;

    /**
     * Ceiling on every piece of DDL the DRAIN's maintenance ticks issue: the heal
     * sweep's ALTER TABLE ... DROP CONSTRAINT and DROP INDEX, and the hourly child
     * sweep's CREATE TABLE ... PARTITION OF. All of them need ACCESS EXCLUSIVE on
     * the table or its parent, and a PENDING ACCESS EXCLUSIVE queues every later
     * reader and writer behind it — the damage is not limited to the statement that
     * waits. The drain's guards normally carry a lock_timeout, but
     * NIGHTOWL_DRAIN_CONN_TIMEOUTS=false is a documented rollback switch and
     * RecordWriter::applyTransactionGuards gates BOTH SET LOCAL guards on it, so the
     * tick carries its own rather than inheriting the question. Timing out costs one
     * tick — a minute for the heal, an hour for a child, both isolated, reported and
     * retried; wedging the drain costs telemetry.
     *
     * It covers BOTH tick paths because both were the same hazard and only one was
     * given a bound: the child sweep kept calling ensureChildWindow raw while the
     * bounded wrapper was wired into the operator command only.
     *
     * A CEILING, applied through withLockTimeout — an operator who set
     * NIGHTOWL_DB_LOCK_TIMEOUT_MS tighter than this keeps their value.
     */
    private const MAINTENANCE_LOCK_TIMEOUT_MS = 3000;

    /**
     * Ceiling on how long any ACCESS EXCLUSIVE statement on the operator
     * command's path WAITS for its lock: the created_at rewrite, the invalid-
     * scaffold DROP INDEX, both prep constraint statements, the swap's LOCK
     * TABLE, the abort unwind, and the child-window sweep's CREATE ... PARTITION
     * OF. It caps the WAIT, never the hold, so nothing long-running is truncated.
     *
     * Two statements are deliberately outside it, both because waiting is their
     * job and neither takes ACCESS EXCLUSIVE: CREATE INDEX CONCURRENTLY (waits
     * out concurrent transactions by design — a timeout would fail it spuriously
     * on exactly the busy tables it exists for) and VALIDATE CONSTRAINT.
     *
     * nightowl:partition runs on Laravel's `nightowl` connection, registered by
     * NightOwlAgentServiceProvider with no `options` key at all, so it carries no
     * lock_timeout and no statement_timeout: without this, a swap queued behind
     * one long dashboard read parks a pending exclusive request in front of every
     * new reader and writer of the LIVE table for that read's whole duration, and
     * the conversion becomes the outage it exists to prevent. Generous enough to
     * win against drain COPY batches (5k rows, milliseconds); short enough that a
     * genuinely busy table gets a clean, retryable 55P03 instead.
     */
    private const SWAP_LOCK_TIMEOUT_MS = 15000;

    /**
     * How long nightowl:partition waits for a table's conversion lock before
     * reporting contention. Long enough to ride out the drain's heal sweep, which
     * takes the same key for a fraction of one tick; far too short to sit through
     * a real peer conversion, which the operator genuinely should be told about.
     */
    private const CONVERSION_LOCK_WAIT_MS = 6000;

    private static int $savepointSeq = 0;

    public static function isPartitioned(PDO $conn, string $table): bool
    {
        $stmt = $conn->prepare(
            "SELECT relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'public' AND c.relname = ?"
        );
        $stmt->execute([$table]);
        $relkind = $stmt->fetchColumn();

        return $relkind === 'p';
    }

    public static function childName(string $table, int $dayEpoch): string
    {
        return $table.'_p'.gmdate('Ymd', $dayEpoch);
    }

    /**
     * The frozen upper bound of {t}_phistoric, as a UTC-midnight epoch: the
     * SECOND midnight ahead, never the first.
     *
     * It has to be a midnight. A daily child spans one whole UTC day, so a bound
     * anywhere inside a day leaves that day's tail belonging to neither the
     * historic child nor a daily one — historicCovers() reports the day covered,
     * ensureChildWindow skips it, and every row after the bound falls to
     * {t}_pdefault, which prune can only row-DELETE, never DROP. That leaves only
     * WHICH midnight, and the first one is not safe to pick.
     *
     * The bound is frozen into {t}_hist_ck BEFORE VALIDATE CONSTRAINT scans the
     * table and before the swap transaction runs, and from the instant it passes
     * the CHECK rejects every drain row with 23514 — NOT VALID included, since
     * NOT VALID only skips validating the rows already there. A conversion
     * started at 23:56 UTC therefore used to freeze a boundary four minutes out:
     * ADD landed, VALIDATE began its full-table scan on a 40M-row table, midnight
     * passed mid-scan, every concurrent drain INSERT/COPY began failing 23514, and
     * VALIDATE itself then failed 23514 on the rows that had arrived while it ran.
     * Sampling the clock late rather than at entry shrank that window; it could
     * not remove it, because the window is not the prep BEFORE the sample — it is
     * the scan and the swap AFTER it.
     *
     * Two days out, unconditionally, gives 24h of headroom at worst and 48h at
     * best. Deliberately not "the next midnight unless it is within N hours":
     * VALIDATE sits outside withLockTimeout on purpose (waiting is its job), so
     * its wait is unbounded and no N is defensible — and a rule that behaves one
     * way at noon and another at 23:50 is the same class of latent time-of-day
     * bug this exists to remove, in the conversion AND in the suite that checks
     * it.
     *
     * What it costs: {t}_phistoric covers one extra UTC day, so that day gets no
     * daily child and prune row-DELETEs it instead of DROPping a partition, and
     * dropEmptyHistoric fires a day later. Against a table already holding the
     * tenant's whole pre-conversion history, that is noise.
     *
     * Public because this is the ONE place the rule lives: the gap report and the
     * tests derive from it instead of re-deriving "tomorrow" by hand.
     */
    public static function historicBoundary(int $now): int
    {
        return intdiv($now, 86400) * 86400 + 2 * 86400;
    }

    /**
     * Create the daily child for $dayEpoch (UTC midnight) if absent. Idempotent.
     *
     * Rows for that day already sitting in the DEFAULT child — a drain that ran
     * while the day had no child of its own — make Postgres reject the CREATE
     * with 23514: the new partition's constraint would be violated by rows the
     * default holds. They are moved into it instead (adoptDefaultRows).
     */
    public static function ensureDailyChild(PDO $conn, string $table, int $dayEpoch): void
    {
        $day = intdiv($dayEpoch, 86400) * 86400;
        $child = self::childName($table, $day);
        $from = gmdate('Y-m-d 00:00:00', $day);
        $to = gmdate('Y-m-d 00:00:00', $day + 86400);

        try {
            self::isolated($conn, fn () => $conn->exec(
                "CREATE TABLE IF NOT EXISTS {$child} PARTITION OF {$table} FOR VALUES FROM ('{$from}') TO ('{$to}')"
            ));

            return;
        } catch (\PDOException $e) {
            if ($e->getCode() !== '23514') {
                throw $e;
            }
        }

        self::adoptDefaultRows($conn, $table, $child, $from, $to);
    }

    /**
     * Pre-create today's child plus DAYS_AHEAD future days for every
     * partitioned raw table. Cheap no-op when everything exists; safe to call
     * from the drain's maintenance tick under its advisory lock.
     *
     * Never throws — one table that cannot be fixed must not cost the other ten
     * their children, on this tick or on any tick after it. Callers sweep inside
     * a transaction they own, where an exception is one rollBack() away from
     * discarding every child the healthy tables just got, and a table that stays
     * broken repeats that forever: the DEFAULT child ends up swallowing every raw
     * row, and prune can only row-DELETE it, never DROP it. Failures are logged
     * and returned instead; the next tick retries them.
     *
     * Children ONLY, and deliberately with NO heal out-param. This sweep no
     * longer strips an interrupted conversion's leftovers (see the comment in the
     * body for what folding the two together cost) and must not be given that job
     * back. The out-param it used to carry survived the split as pure plumbing:
     * it was reset to empties on entry here and never written to again, so the
     * caller's "cleaned up an interrupted run's leftovers" notice was unreachable
     * — while this docblock and RecordWriter's both still specified a live heal
     * contract, so a maintainer following either one would have filled an array
     * nothing reads and the operator would never have heard about a total 23514
     * write outage being cleared. healConversionLeftovers() owns the heal,
     * RecordWriter::healRawPartitionLeftovers() runs it every tick, and the
     * notice is that caller's to print.
     *
     * Every day here is a CREATE TABLE ... PARTITION OF, which takes ACCESS
     * EXCLUSIVE on its parent, so each table's window runs under
     * MAINTENANCE_LOCK_TIMEOUT_MS rather than whatever the caller's transaction
     * happens to carry. It carried nothing under NIGHTOWL_DRAIN_CONN_TIMEOUTS=false
     * (applyTransactionGuards gates both SET LOCALs on it), and an unbounded sweep
     * queued behind one long dashboard read parks a pending exclusive in front of
     * every later reader and writer of that table for the read's whole duration —
     * Postgres queues new requests behind a pending exclusive one, so the damage was
     * never limited to the sweep. The ceiling sits INSIDE the per-table try, and
     * inside isolated(): its own settings read must not be able to break the "never
     * throws" contract above, and the savepoint has to enclose the SET LOCAL so a
     * failed table reverts it for free.
     *
     * @param  list<string>|null  $tables
     * @return list<string> one entry per day that could not be created
     */
    public static function ensureFutureChildren(PDO $conn, ?array $tables = null): array
    {
        $today = intdiv(time(), 86400) * 86400;
        $failures = [];

        // The heal is NOT run from here. It used to be, and that quietly undid
        // the reason it was given its own short transaction: this sweep is the
        // HOURLY one, so a heal inside it held each table's conversion key from
        // the moment it healed until the whole 11-table × 8-day window (including
        // adoptDefaultRows' physical row moves) committed. An operator re-running
        // nightowl:partition during that window — the action the leftover state
        // prompts — was refused against a peer run that does not exist.
        // RecordWriter::healRawPartitionLeftovers owns the heal now, per tick.

        foreach ($tables ?? self::TABLES as $table) {
            try {
                $failures = array_merge(
                    $failures,
                    self::isolated($conn, fn (): array => self::ensureChildWindowBounded(
                        $conn, $table, $today, self::MAINTENANCE_LOCK_TIMEOUT_MS,
                    )),
                );
            } catch (\Throwable $e) {
                $failures[] = $table.': '.$e->getMessage();
            }
        }

        self::logFailures($failures);

        return $failures;
    }

    /**
     * Strip what an interrupted conversion left on a LIVE table: the
     * {t}_hist_ck boundary CHECK, and the INVALID {t}_id_created_at_pt index a
     * killed CREATE INDEX CONCURRENTLY leaves behind. Runs on the drain's
     * cleanup tick because the damage is otherwise invisible until it is
     * catastrophic, and nothing in-process can clean up after the case that
     * causes it.
     *
     * The CHECK is a conversion-internal scaffold: prep puts it on the live
     * table so ATTACH can skip its full-table scan, and the swap carries it onto
     * {t}_phistoric by rename. It is never meant to outlive the conversion on
     * anything else. Two ways it does:
     *
     * - **Plain table, boundary passed.** A run SIGKILLed between VALIDATE and
     *   the swap (a deploy step timing out and taking the container with it —
     *   the tinybit.farm sequence) leaves it VALIDATED on the live table. It is
     *   harmless until its frozen boundary passes, then it rejects every drain
     *   row with 23514 — a silent, total write outage for that table, hours
     *   after the command anyone would blame. NOT VALID counts too: NOT VALID
     *   only skips validating the rows already there, new inserts are checked
     *   either way, which is why the probe filters on name and contype and never
     *   on convalidated. convert() unwinds this on any abort it survives; only a
     *   killed process needs this sweep.
     * - **Partitioned parent.** Agents before the conversion lock ran prep
     *   unprotected, so a concurrent run could apply it to an already-converted
     *   parent, where it cascades to every child (verified against postgres:17).
     *   The parent is never a legitimate home for it — convert() clones the
     *   parent LIKE ... without INCLUDING CONSTRAINTS — so its presence there is
     *   always corruption, whatever its boundary. Dropping it also clears the
     *   copy Postgres merged onto the historic child, which costs nothing: that
     *   child's partition BOUND enforces the same range on its own.
     *
     * The invalid index is the OTHER leftover of the same kill, and the one
     * nothing used to collect unattended: an invalid unique index is never used
     * by the planner, so it is pure cost — an (id, created_at) btree on a 40M-row
     * raw table is over a gigabyte — and only a RETRY of nightowl:partition ever
     * removed it. A VALID one is deliberately left alone: it is completed prep,
     * and the retry reuses it via IF NOT EXISTS.
     *
     * **A live conversion is never disturbed, because this takes the table's
     * conversion lock before it touches anything.** The boundary argument ("its
     * boundary is in the future, so the plain-table rule cannot match it") is not
     * sound on its own and never was — it makes a live conversion's safety a race
     * against a clock. historicBoundary() now puts that clock at least 24 hours
     * out, which makes the race very hard to lose; "very hard to lose" is not
     * "cannot lose", and a CIC blocked behind a long transaction plus a VALIDATE
     * queued behind an anti-wraparound autovacuum is a real way to spend a day.
     * Losing it means the sweep drops a LIVE conversion's validated CHECK and
     * turns its pending ATTACH into a full-table scan under ACCESS EXCLUSIVE — or
     * kills it 23514. The lock is what makes that impossible rather than
     * unlikely. Advisory scopes share one lock space, so the xact lock taken
     * here conflicts with convert()'s SESSION lock in both directions (verified
     * against postgres:17). Xact-scoped, not session-scoped, for two reasons: the
     * drain runs behind transaction-mode poolers where a session lock and its
     * unlock can land on different backends, and — because it is taken inside
     * isolated()'s savepoint — a failed heal releases it on ROLLBACK TO SAVEPOINT,
     * where a session lock would survive the rollback and strand itself on a live
     * backend (both verified). When the caller has no transaction of its own this
     * opens one per candidate table, so the lock spans the decision AND the DDL —
     * which is now the drain's path too, not only the tests'. Wrapped in ONE
     * caller-owned transaction these per-candidate commits are skipped and every
     * healed table's ACCESS EXCLUSIVE is held until that single commit, so the
     * per-tick caller deliberately opens none.
     *
     * Probe-first is what keeps that lock out of anyone's way: the two catalog
     * reads cover the WHOLE table set in one round trip each, and the answer is
     * "nothing to do" on every tick except the one after a conversion died — so
     * on a healthy tenant no conversion key is ever taken and no DDL is ever
     * issued, and a concurrent nightowl:partition can never see spurious
     * contention. Round trips are the only real cost here (~0.09 ms of server
     * work per table against ~0.8 ms of loopback RTT), which is why the probe is
     * batched and per-table work only begins once a leftover exists.
     *
     * Never throws — healing is best-effort next to the children the tick exists
     * to create — but it is no longer SILENT. A heal that can never succeed (a
     * 55P03 against a live drain COPY, say) used to be invisible forever while
     * the table sat in a total 23514 write outage, and no diagnosis rule in
     * MetricsCollector can infer it: DRAIN_WRITE_FAILING fires on the CONSEQUENCE
     * and its 23* advice recommends quarantine, which would convert a buffered
     * outage into dropped telemetry.
     *
     * "Not silent" is not "one channel", and collapsing it into one was its own
     * incident. Three states reach $failures and each gets its OWN log line,
     * because the right remediation differs and the wrong one is destructive:
     *
     * - **Hard failure** — something threw: the batched probe, or one table's
     *   read or DDL. Retried next tick; if it persists, re-run
     *   nightowl:partition or drop the constraint by hand.
     * - **Stalled behind a held lock** — a peer holds the table's conversion key
     *   AND the leftover is already past its boundary, so the table is rejecting
     *   drain rows right now. Reported ONLY in that combination. A held key on
     *   its own is the normal, constant state of a healthy in-flight run (prep
     *   adds {t}_hist_ck, so the table is a candidate on every tick until the
     *   swap lands), and reporting THAT as a failure printed an error a minute
     *   whose advice — drop the constraint by hand — would have destroyed the
     *   very conversion in progress. leftoverIsProvableWreckage() draws the line;
     *   the remediation here is the opposite one.
     * - **Unreadable CHECK** — a {t}_hist_ck naming a column other than
     *   created_at. Never dropped, and not healable without a human; see
     *   isConversionWreckage() for why probing it was a permanent write outage.
     *
     * @param  list<string>|null  $tables
     * @param  list<string>  $failures  out: one entry per REPORT — hard failures,
     *                                  then stalled locks, then unreadable CHECKs.
     *                                  Not one per table: the batched probe's
     *                                  failure names the whole set. Assigned once
     *                                  at the end, never appended mid-sweep.
     * @return list<string> tables healed
     */
    public static function healConversionLeftovers(PDO $conn, ?array $tables = null, array &$failures = []): array
    {
        $tables = array_values($tables ?? self::TABLES);

        if ($tables === []) {
            return [];
        }

        $healed = [];
        $candidates = [];

        // Three report channels, not one. They all end up in the $failures
        // out-param, but never in the same log line: a held conversion lock and
        // an unreadable CHECK need the OPPOSITE remediation from a hard failure,
        // and printing the hard one over them is what made a healthy
        // nightowl:partition run emit an error a minute telling the operator to
        // drop the constraint it was in the middle of validating.
        $hardFailures = [];
        $stalledLocks = [];
        $unreadableChecks = [];

        try {
            $candidates = self::isolated($conn, static function () use ($conn, $tables): array {
                $checks = self::tablesWithLeftoverCheck($conn, $tables);
                $scaffolds = self::invalidScaffoldIndexes($conn, $tables);

                // Caller's order, so the healed list is stable and assertable.
                return array_values(array_filter(
                    $tables,
                    static fn (string $t): bool => in_array($t, $checks, true) || in_array($t, $scaffolds, true),
                ));
            });
        } catch (\Throwable $e) {
            // Batched, so one failure costs every table its heal this tick. Name
            // the set it covered rather than a table it cannot identify — but
            // this is now genuinely rare: the probe touches only catalog
            // relations, so a table lock cannot reach it.
            $hardFailures[] = sprintf(
                'interrupted-conversion probe over %s: %s',
                implode(', ', array_slice($tables, 0, 3)).(count($tables) > 3 ? ' (+'.(count($tables) - 3).' more)' : ''),
                $e->getMessage(),
            );
        }

        foreach ($candidates as $table) {
            // Outside a caller's transaction (tests, and any future direct
            // caller) an xact-scoped lock would die with the implicit
            // single-statement transaction that took it, protecting the decision
            // but not the DDL after it. Own the transaction so the guarantee is
            // the same everywhere. Mirrors adoptDefaultRows().
            $ownTransaction = ! $conn->inTransaction();
            $dropped = false;
            $peerHeld = false;
            $unreadable = false;
            $stalledNow = false;

            try {
                // beginTransaction is INSIDE the try: a throw here (a dead
                // handle, a transaction the caller left open unexpectedly) would
                // otherwise escape a function whose whole contract — and
                // ensureFutureChildren's above it — is "never throws", and inside
                // maintainRawPartitions that costs every healthy table its child
                // window for the hour.
                if ($ownTransaction) {
                    $conn->beginTransaction();
                }

                self::isolated($conn, static function () use ($conn, $table, &$dropped, &$peerHeld, &$unreadable) {
                    // The whole safety argument. Skipping costs one tick;
                    // racing a live conversion costs the table.
                    //
                    // A skip is NOT a failure, and reporting it as one was its
                    // own incident. Prep adds {t}_hist_ck, so every 60s tick of a
                    // perfectly healthy in-flight nightowl:partition run lands
                    // here — on a populated table that is minutes to hours of
                    // them — and the failure channel printed an error each time,
                    // carrying the hard-failure remediation: "re-run
                    // nightowl:partition or drop the constraint by hand". Acting
                    // on it would have dropped the live conversion's VALIDATED
                    // CHECK and turned its pending ATTACH into a full-table scan
                    // under ACCESS EXCLUSIVE — the sweep advising the operator to
                    // cause the outage the sweep exists to prevent.
                    //
                    // A PEER DRAIN WORKER is now a legitimate holder too. The
                    // per-tick heal no longer takes a process-wide maintenance
                    // key, so with NIGHTOWL_DRAIN_WORKERS>1 the workers do race
                    // here — for the length of one candidate's short transaction.
                    // The loser skips and finds nothing left to do next tick.
                    //
                    // The state the skip HIDES is still reported, just not from
                    // here: a transaction-mode pooler can strand convert()'s
                    // session key on a backend nobody can reach (the state
                    // convert()'s own unlock diagnostic describes), and then the
                    // sweep skips forever while the table sits in a permanent
                    // total 23514 write outage. Both look identical at this line,
                    // so the question that actually separates them is asked after
                    // the lock attempt — see leftoverIsProvableWreckage() below.
                    if (! self::tryConversionXactLock($conn, $table)) {
                        $peerHeld = true;

                        return;
                    }

                    // The ceiling covers the READS as well as the DDL, and that
                    // is not belt-and-braces: pg_get_expr() has to open the
                    // relation to deparse conbin into column names, so
                    // leftoverChecks below takes an ACCESS SHARE on the very
                    // table an in-flight ALTER may be holding ACCESS EXCLUSIVE
                    // on. Bound only around the DROP, the sweep waited out the
                    // blocker in full and then healed successfully, reporting
                    // nothing — measured at 8.01 s against a blocker that only
                    // ended because its own idle timeout killed it.
                    //
                    // It is bounded HERE, per table under that table's own
                    // conversion lock, and deliberately NOT around the batched
                    // candidate probe: that probe reads pg_class/pg_constraint
                    // only (tablesWithLeftoverCheck takes no expression), so no
                    // table lock can reach it, and one blocked table costs only
                    // itself — the other ten still heal on this tick.
                    self::withLockTimeout($conn, self::MAINTENANCE_LOCK_TIMEOUT_MS, static function () use ($conn, $table, &$dropped, &$unreadable) {
                        // Re-read under the lock: the batched probe ran before
                        // it, and a peer's swap can commit in between — its
                        // CHECK travels to {t}_phistoric by rename and the new
                        // parent, cloned LIKE without INCLUDING CONSTRAINTS,
                        // carries none. Acting on the stale read would report a
                        // heal that dropped nothing, or drop something a
                        // conversion just re-added.
                        $check = self::leftoverChecks($conn, [$table])[$table] ?? null;

                        if ($check !== null) {
                            // Three-valued on purpose: null is "this is not a
                            // constraint we wrote, and we cannot evaluate it".
                            // Dropping it would destroy someone else's work;
                            // probing it raised 42703, which aborted the
                            // statement, propagated to the per-table catch, and
                            // recurred identically on every tick — so the table
                            // could never be healed and sat in a permanent 23514
                            // write outage past its boundary. Leave it; report it.
                            $wreckage = self::isConversionWreckage($conn, $check);

                            if ($wreckage === null) {
                                $unreadable = true;
                            } elseif ($wreckage) {
                                $conn->exec("ALTER TABLE {$table} DROP CONSTRAINT IF EXISTS {$table}_hist_ck");
                                $dropped = true;
                            }
                        }

                        if (in_array($table, self::invalidScaffoldIndexes($conn, [$table]), true)) {
                            $conn->exec("DROP INDEX IF EXISTS {$table}_id_created_at_pt");
                            $dropped = true;
                        }
                    });
                });

                // OUTSIDE the block above, and only on the skip path. This is a
                // READ that can queue behind the peer's own ACCESS EXCLUSIVE, so
                // it carries its own savepoint and its own ceiling rather than
                // risking the decision we just made — and it reports nothing
                // unless the skip actually COST a heal that was due.
                $stalledNow = $peerHeld && self::leftoverIsProvableWreckage($conn, $table) === true;

                if ($ownTransaction) {
                    $conn->commit();
                }

                // Appended only once the DROP is DURABLE. Marking it healed
                // inside the closure meant a commit that then threw left the
                // table in BOTH lists, and RecordWriter announced "cleaned up an
                // interrupted nightowl:partition run's leftovers" for a statement
                // that rolled back — the same false-success line that moving the
                // announcement after the caller's commit exists to prevent, just
                // one transaction further in.
                if ($dropped) {
                    $healed[] = $table;
                }

                if ($stalledNow) {
                    $stalledLocks[] = $table.': its conversion lock is held by another session AND its '
                        .$table.'_hist_ck is already past its boundary, so this table is rejecting every drain '
                        .'row (23514) and the sweep cannot clear it.';
                }

                if ($unreadable) {
                    $unreadableChecks[] = $table.': a '.$table.'_hist_ck exists whose expression references a '
                        .'column other than created_at, so it is not one this agent wrote and cannot be '
                        .'evaluated safely — left in place.';
                }
            } catch (\Throwable $e) {
                if ($ownTransaction && $conn->inTransaction()) {
                    try {
                        $conn->rollBack();
                    } catch (\Throwable) {
                        // Handle already dead — never mask the real cause.
                    }
                }

                $hardFailures[] = $table.': stale conversion leftovers not stripped — '.$e->getMessage();
            }
        }

        // All three logged HERE, not by the caller, and deliberately not through
        // logFailures(): a failure statement is true whether the caller commits
        // or rolls back (unlike the success line, which the caller owns), and
        // logFailures' wording would report a heal as a partition that was not
        // created. Three lines rather than one because the remediation differs
        // and the wrong one is destructive.
        if ($hardFailures !== []) {
            error_log(sprintf(
                '[NightOwl Support] %d interrupted-conversion leftover(s) not stripped this tick (retried next '
                .'tick) — %s. Counted as REPORTS, not tables: a per-table entry names one table, while a probe '
                .'failure covers the whole set and establishes nothing about any of them. Past its frozen '
                .'boundary a {t}_hist_ck rejects every drain row for its table (23514); if this repeats, and no '
                .'nightowl:partition run is in flight, re-run nightowl:partition or drop the constraint by hand.',
                count($hardFailures),
                implode('; ', array_slice($hardFailures, 0, 3)).(count($hardFailures) > 3 ? ' (…)' : ''),
            ));
        }

        if ($stalledLocks !== []) {
            // A held conversion lock on its own is NORMAL and prints nothing.
            // This fires only for the subset where the leftover is already
            // rejecting writes — i.e. where the skip cost a heal that was due —
            // and its advice is the opposite of the line above: do not touch the
            // constraint while a run may still be in flight.
            error_log(sprintf(
                '[NightOwl Support] %d table(s) are rejecting drain writes (23514) behind a HELD conversion '
                .'lock — %s. Do NOT drop the constraint blind. If a nightowl:partition run is still in flight it '
                .'owns this table, and it has been in flight over 24 hours: the boundary it froze is the SECOND '
                .'UTC midnight ahead, so nothing shorter can reach this state. Check pg_stat_activity for it — a '
                .'CREATE INDEX CONCURRENTLY waiting out a long-lived transaction, or a VALIDATE over a very large '
                .'table. Let it finish if it is progressing: its swap moves the constraint onto the historic '
                .'child and ends the outage by itself. Kill it only if it is not, and drop the constraint after '
                .'it has exited, never while it runs. '
                .'A PEER DRAIN WORKER healing this same table is the third cause and the harmless one — the heal '
                .'takes no process-wide key, so workers race here — and it lasts one candidate transaction, so it '
                .'cannot repeat. If NO run exists and this persists, the session key is stranded on a server '
                .'backend this connection cannot reach '
                .'(a transaction-mode pooler such as PgBouncer/Supavisor): point the nightowl DB connection at '
                .'the database port and restart the pooler, and the next tick clears it.',
                count($stalledLocks),
                implode('; ', array_slice($stalledLocks, 0, 3)).(count($stalledLocks) > 3 ? ' (…)' : ''),
            ));
        }

        if ($unreadableChecks !== []) {
            // The one leftover state this sweep will NEVER resolve on its own, so
            // it says exactly that instead of retrying forever behind a failure
            // line that promises the next tick will fix it.
            error_log(sprintf(
                '[NightOwl Support] %d {t}_hist_ck constraint(s) cannot be interpreted and will NEVER be healed '
                .'automatically — %s. This agent only ever writes CHECK (created_at IS NOT NULL AND created_at < '
                .'<boundary>); one naming any other column was not written by it, and dropping a constraint this '
                .'agent cannot read would destroy work it does not own. Inspect it (\\d+ <table> in psql) and '
                .'drop it by hand if it is an interrupted conversion\'s leftover — once its boundary passes it '
                .'rejects every drain row for that table (23514) with no retry that can ever clear it.',
                count($unreadableChecks),
                implode('; ', array_slice($unreadableChecks, 0, 3)).(count($unreadableChecks) > 3 ? ' (…)' : ''),
            ));
        }

        $failures = array_merge($failures, $hardFailures, $stalledLocks, $unreadableChecks);

        return $healed;
    }

    /**
     * Which of $tables carry a {t}_hist_ck CHECK at all — the candidacy question,
     * in ONE catalog read that touches ONLY catalog relations.
     *
     * Deliberately does NOT select the expression. pg_get_expr() has to open the
     * constraint's relation to deparse conbin into column names, so it takes an
     * ACCESS SHARE on every table it reports on; a batch that did that blocked
     * behind any in-flight ALTER and cost EVERY table its heal for that tick,
     * even though the candidacy test only ever asked "is a CHECK present?".
     * Measured: with the expression fetched, one locked table left a second
     * table's expired CHECK in place and its 23514 write outage running; without
     * it, the second table healed and only the locked one was reported.
     *
     * Batched because this runs on every 60s cleanup tick and in steady state the
     * answer is "none": per-table it was 44 round trips (~15 ms on Docker
     * loopback, ~220 ms against a managed PG at 5 ms RTT — round-trip-bound, not
     * query-bound). Nothing is DECIDED here; presence only makes a table a
     * candidate, and whether it is wreckage or a live conversion's scaffold is
     * re-read — expression and all — under that table's conversion lock.
     *
     * @param  list<string>  $tables
     * @return list<string>
     */
    private static function tablesWithLeftoverCheck(PDO $conn, array $tables): array
    {
        $stmt = $conn->prepare(
            "SELECT c.relname
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             JOIN pg_constraint con
               ON con.conrelid = c.oid
              AND con.contype = 'c'
              AND con.conname = c.relname || '_hist_ck'
             WHERE n.nspname = 'public'
               AND c.relname = ANY(string_to_array(?, ','))"
        );
        $stmt->execute([implode(',', $tables)]);

        return $stmt->fetchAll(PDO::FETCH_COLUMN);
    }

    /**
     * Every raw table in $tables carrying a {t}_hist_ck CHECK, keyed by table
     * name, with its deparsed expression AND whether that expression references
     * created_at and nothing else — the deciding read.
     *
     * Called per-table under that table's conversion lock and inside the heal's
     * lock_timeout (and from leftoverIsProvableWreckage, under its own savepoint
     * and ceiling), because pg_get_expr opens the relation — see
     * tablesWithLeftoverCheck for what that cost when it ran unbounded in a batch.
     *
     * created_at_only comes from pg_constraint.conkey, the catalog's own list of
     * the columns a constraint references, and deliberately NOT from
     * pattern-matching the deparsed text, whose formatting is a Postgres
     * implementation detail: a strict pattern that a future release reformatted
     * would stop every tenant healing, which is worse than the bug it guards.
     * It exists because isConversionWreckage() evaluates the expression against a
     * probe row exposing exactly one column. A {t}_hist_ck naming any other
     * column — hand-repaired, or an older agent's — raised 42703 there, aborting
     * the statement and recurring identically on every tick, so the table could
     * NEVER be healed and sat in a permanent 23514 write outage past its
     * boundary.
     *
     * The test is conkey EQUALS [created_at's attnum] — a whitelist of the one
     * shape we write — and NOT "every attnum in conkey resolves to created_at".
     * That difference is a bug this file already shipped. conkey records a
     * WHOLE-ROW reference as attnum 0, which has no pg_attribute row at all, so
     * the aggregate form's inner JOIN silently DROPPED it and bool_and answered
     * true over the surviving created_at entry. Measured on postgres:17.9:
     * CHECK (created_at IS NOT NULL AND created_at < '…' AND (t.*) IS NOT NULL)
     * gives conkey {2,0} and the old form said "created_at only", after which
     * the probe raised 42P01 ("missing FROM-clause entry") instead of 42703 —
     * same wedge, different SQLSTATE. Equality has no such gap: {2} passes,
     * {2,3} and {2,0} and {} fail, and NULL conkey (a constraint referencing no
     * column) compares NULL and COALESCEs to false. Verified against
     * postgres:17.9 for all five, on a partitioned parent too.
     *
     * @param  list<string>  $tables
     * @return array<string, object{relname: string, relkind: string, expr: string, created_at_only: bool|string}>
     */
    private static function leftoverChecks(PDO $conn, array $tables): array
    {
        $stmt = $conn->prepare(
            "SELECT c.relname, c.relkind, pg_get_expr(con.conbin, con.conrelid) AS expr,
                    COALESCE(con.conkey = ARRAY[(
                        SELECT a.attnum
                        FROM pg_attribute a
                        WHERE a.attrelid = con.conrelid
                          AND a.attname = 'created_at'
                          AND NOT a.attisdropped
                    )], false) AS created_at_only
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             JOIN pg_constraint con
               ON con.conrelid = c.oid
              AND con.contype = 'c'
              AND con.conname = c.relname || '_hist_ck'
             WHERE n.nspname = 'public'
               AND c.relname = ANY(string_to_array(?, ','))"
        );
        $stmt->execute([implode(',', $tables)]);

        $out = [];
        foreach ($stmt->fetchAll(PDO::FETCH_OBJ) as $row) {
            if ($row->expr === null) {
                continue;
            }

            $out[$row->relname] = $row;
        }

        return $out;
    }

    /**
     * Every raw table in $tables whose {t}_id_created_at_pt scaffolding index
     * exists but is INVALID — a CREATE INDEX CONCURRENTLY killed mid-build. One
     * catalog read. Valid ones are excluded on purpose: a retry reuses them.
     *
     * @param  list<string>  $tables
     * @return list<string>
     */
    private static function invalidScaffoldIndexes(PDO $conn, array $tables): array
    {
        $stmt = $conn->prepare(
            "SELECT t.relname
             FROM pg_index x
             JOIN pg_class i ON i.oid = x.indexrelid
             JOIN pg_class t ON t.oid = x.indrelid
             JOIN pg_namespace n ON n.oid = t.relnamespace
             WHERE n.nspname = 'public'
               AND NOT x.indisvalid
               AND i.relname = t.relname || '_id_created_at_pt'
               AND t.relname = ANY(string_to_array(?, ','))"
        );
        $stmt->execute([implode(',', $tables)]);

        return $stmt->fetchAll(PDO::FETCH_COLUMN);
    }

    /**
     * Whether this {t}_hist_ck can only be wreckage: true = drop it, false =
     * leave it (a live conversion's scaffold), null = the agent cannot interpret
     * it and must never touch it.
     *
     * Called with the table's conversion lock held, or from
     * leftoverIsProvableWreckage() when a peer holds it and only a verdict — not
     * a DROP — is wanted.
     *
     * NULL comes first because it is the case that used to be a permanent
     * outage. The expression is evaluated against a probe row exposing exactly
     * one column, created_at, so a constraint referencing anything else raised
     * 42703 — which aborts the statement, propagates to the per-table catch, and
     * is recorded as a failure that recurs identically forever. The table could
     * then never be healed, and past its boundary it rejected every drain row
     * (23514) with no retry that could clear it. The catalog answers the question
     * instead (see leftoverChecks' created_at_only). Dropping a constraint we
     * cannot read is the one thing worse than leaving it, so we leave it and say
     * so loudly — see healConversionLeftovers' unreadable-CHECK report.
     *
     * TWO layers, not one, and the second is not belt-and-braces. The catalog
     * gate is a whitelist, and a whitelist can only be as exhaustive as what its
     * author knew: the first version of it admitted conkey's whole-row attnum 0
     * and the probe raised 42P01, wedging the table exactly as 42703 had. So the
     * probe carries its own savepoint and its own catch, and any SQLSTATE class
     * 42 (the statement is not something we can run) or 22 (the expression could
     * not be evaluated for the probe row) becomes the same null verdict the gate
     * would have produced. Every other class — 08 dead connection, 55P03 lock
     * timeout, 57P01 admin shutdown — is rethrown, because those are transient
     * and the next tick genuinely does retry them; reporting them as "will NEVER
     * be healed" would be its own wrong remediation. The savepoint is required,
     * not stylistic: swallowing an error inside the caller's transaction without
     * unwinding leaves it aborted and the very next statement dies 25P02.
     *
     * On a partitioned parent: wreckage whatever its boundary says (see
     * healConversionLeftovers) — but still only once we can read it. The NAME is
     * ours by convention; the expression is the only evidence the constraint is.
     *
     * On a plain table: only once the frozen boundary has passed. Ask Postgres
     * whether a row stamped NOW would still be accepted, rather than parsing the
     * boundary out of the expression — and BIND a PHP-computed UTC literal to do
     * it, the same technique historicCovers() has always used. now()::timestamp
     * renders in the session's TimeZone GUC, which on a BYO PostgreSQL is the
     * customer's, while the boundary was written by gmdate() in UTC. Measured
     * against postgres:17 on one constraint whose boundary was one hour ahead:
     * the naive form answers `true` under UTC and `false` under
     * Pacific/Kiritimati (+14) — i.e. it strips a live conversion's CHECK up to
     * fourteen hours early — and under Etc/GMT+12 it answers `true` for a
     * boundary that passed a minute ago, leaving genuine wreckage in place for
     * twelve hours after it started rejecting writes. The bound form answered
     * correctly at every offset; timestamp without time zone never consults
     * TimeZone. CI runs Etc/UTC and cannot catch a regression here.
     *
     * A NULL probe result means the expression decided nothing for the probe row
     * — a boundary literal that is itself NULL, say. That is uninterpretable too,
     * and now reports rather than silently leaving the constraint forever.
     */
    private static function isConversionWreckage(PDO $conn, object $check): ?bool
    {
        // FIRST, before the parent shortcut and before the probe. PDO_pgsql
        // hands booleans back as either a PHP bool or 't'/'f' depending on the
        // build — the same idiom every other boolean read in this file uses.
        if (! ($check->created_at_only === true || $check->created_at_only === 't')) {
            return null;
        }

        if ($check->relkind === 'p') {
            return true;
        }

        try {
            $accepts = self::isolated($conn, static function () use ($conn, $check): mixed {
                $stmt = $conn->prepare("SELECT ({$check->expr}) FROM (SELECT ?::timestamp AS created_at) probe");
                $stmt->execute([gmdate('Y-m-d H:i:s')]);

                return $stmt->fetchColumn();
            });
        } catch (\PDOException $e) {
            // Classes 42 and 22 only — see the docblock for why everything else
            // has to keep propagating as a retryable failure.
            if (in_array(substr((string) $e->getCode(), 0, 2), ['42', '22'], true)) {
                return null;
            }

            throw $e;
        }

        if ($accepts === null) {
            // The expression decided nothing for the probe row — a boundary
            // literal that is itself NULL, say. Uninterpretable, not "accepted":
            // returning false here left such a constraint in place silently and
            // forever, which is the outage this function's null exists to report.
            return null;
        }

        return ! ($accepts === true || $accepts === 't');
    }

    /**
     * The heal's verdict for one table WITHOUT acting on it — for the one path
     * that cannot act, where a peer holds the table's conversion key.
     *
     * This is the question that separates a normal skip from a damaging one, and
     * nothing cheaper answers it. A held key is the CONSTANT state of a healthy
     * in-flight nightowl:partition run: prep adds {t}_hist_ck, so the table is a
     * candidate on every 60s tick until the swap lands, and the verdict there is
     * "not wreckage" — its boundary is at least a day out and it is accepting
     * today's rows.
     * The verdict is "wreckage" in exactly the cases where the skip COST a heal
     * that was due: a key stranded behind a transaction-mode pooler, or a
     * conversion that has been in flight so long it has outlived its OWN frozen
     * boundary. historicBoundary() puts that bound at the SECOND UTC midnight
     * ahead, so the second case needs 24-48h of prep — a CREATE INDEX
     * CONCURRENTLY waiting out a long-lived transaction, or a VALIDATE over an
     * enormous table — and is not the everyday shape it once was. Note what is
     * NOT failing there: VALIDATE only scans the rows already present, which all
     * satisfy the bound, so it succeeds; it is the DRAIN that takes the 23514,
     * because a NOT VALID constraint still rejects new rows. Either way the table
     * is rejecting drain writes at this moment and the operator has to hear it.
     *
     * Deliberately NOT hoisted above tryConversionXactLock() in the caller.
     * pg_get_expr opens the relation to deparse, so this read takes ACCESS SHARE
     * and CAN queue behind the peer's own ACCESS EXCLUSIVE; reading before the
     * lock attempt would put that on the healthy path and turn a live swap into a
     * per-tick hard failure. The ceiling is the same MAINTENANCE_LOCK_TIMEOUT_MS
     * the DDL runs under, and withLockTimeout only ever TIGHTENS, so a session
     * already stricter keeps its own value here too.
     *
     * Never throws and never decides anything destructive. null is "could not
     * tell" and the caller treats it as "say nothing" — a blocked read, a dead
     * handle or an unreadable constraint costs a log line's worth of certainty,
     * never the tick. Savepoint-isolated for the same reason: a 55P03 here would
     * otherwise abort the caller's transaction and take every other table's heal
     * with it.
     */
    private static function leftoverIsProvableWreckage(PDO $conn, string $table): ?bool
    {
        try {
            return self::isolated($conn, static fn (): ?bool => self::withLockTimeout(
                $conn,
                self::MAINTENANCE_LOCK_TIMEOUT_MS,
                static function () use ($conn, $table): ?bool {
                    $check = self::leftoverChecks($conn, [$table])[$table] ?? null;

                    return $check === null ? null : self::isConversionWreckage($conn, $check);
                },
            ));
        } catch (\Throwable) {
            return null;
        }
    }

    /**
     * Run $work under a lock_timeout CEILING, restoring whatever the caller had.
     *
     * The callers are the drain tick's DDL (the heal, and the child-window sweep)
     * and the operator command's, none of which inherits a usable bound: the
     * drain's guards are switched off by NIGHTOWL_DRAIN_CONN_TIMEOUTS=false, and
     * the `nightowl` connection nightowl:partition runs on never had one.
     *
     * A CEILING, never an assignment, and the distinction is the whole point.
     * Callers pass the loosest bound THEIR statement can afford, but the session
     * may already be tighter: RecordWriter::applyTransactionGuards emits
     * NIGHTOWL_DB_LOCK_TIMEOUT_MS, and an operator who lowered that to 500 did it
     * because a 3 s ACCESS EXCLUSIVE wait is already an outage on their table.
     * Overwriting it RAISED the ceiling to 3000 on a per-minute cadence — the exact
     * hazard these constants exist to prevent, inflicted by the code preventing it.
     * Tighten only; a tighter session value is left alone, SET included.
     *
     * 0 is Postgres for "wait forever", so it is the LOOSEST value there is and has
     * to compare as LARGER than any finite one. Read as the smallest (a plain min())
     * it would disable the bound on precisely the connections that have none — the
     * `nightowl` connection and the drain under the rollback switch — which is every
     * caller here.
     *
     * The previous value comes from pg_settings, not SHOW: lock_timeout's base unit
     * there is ms, so it arrives as a plain integer that can be COMPARED and fed
     * straight back to SET (no quoting, no unit parsing). SHOW renders it ('10s',
     * '500ms', '0') — restorable, but useless for deciding whether the session is
     * already tighter than we are.
     *
     * Inside a transaction, SET LOCAL: it is savepoint-scoped, so ROLLBACK TO
     * SAVEPOINT reverts it for free on the failure path and only the success path
     * needs the explicit restore (RELEASE SAVEPOINT PRESERVES it — verified). A
     * finally would be actively wrong there: after a failed DDL the block is
     * aborted, so the restoring statement would die 25P02 and bury the real
     * error. Outside a transaction SET LOCAL is a no-op with a warning, so use a
     * session-scoped SET and undo it on both paths, which is safe because an
     * autocommit failure poisons nothing.
     */
    private static function withLockTimeout(PDO $conn, int $milliseconds, callable $work): mixed
    {
        $previous = self::currentLockTimeoutMs($conn);

        // Already at least as tight (0 excluded — see the docblock). Leave the
        // session entirely alone rather than spend two round trips restoring it to
        // what it already was.
        if (! self::tightensLockTimeout($previous, $milliseconds)) {
            return $work();
        }

        if ($conn->inTransaction()) {
            $conn->exec('SET LOCAL lock_timeout = '.$milliseconds);
            $result = $work();
            $conn->exec('SET LOCAL lock_timeout = '.$previous);

            return $result;
        }

        $conn->exec('SET lock_timeout = '.$milliseconds);

        try {
            return $work();
        } finally {
            try {
                $conn->exec('SET lock_timeout = '.$previous);
            } catch (\Throwable) {
                // Dead handle: the session's setting died with it.
            }
        }
    }

    /**
     * The session's lock_timeout in milliseconds, from pg_settings rather than
     * SHOW — see withLockTimeout's docblock for why the rendered form is useless
     * for comparing.
     */
    private static function currentLockTimeoutMs(PDO $conn): int
    {
        return (int) $conn->query(
            "SELECT setting FROM pg_settings WHERE name = 'lock_timeout'"
        )->fetchColumn();
    }

    /**
     * Whether setting lock_timeout to $milliseconds would TIGHTEN a session
     * currently at $previous — the ceiling rule, in ONE place.
     *
     * Extracted so the swap transaction can apply it too. That block emitted a
     * bare `SET LOCAL lock_timeout = SWAP_LOCK_TIMEOUT_MS` while its own
     * neighbours went through withLockTimeout, so an operator who had lowered
     * NIGHTOWL_DB_LOCK_TIMEOUT_MS to 500 (because a 3 s ACCESS EXCLUSIVE wait on
     * their raw tables is already an outage) had that RAISED thirtyfold for the
     * LOCK TABLE ... ACCESS EXCLUSIVE that follows — the precise hazard the
     * ceiling exists to prevent, on the one path that bypassed it. It cannot be
     * withLockTimeout itself there: the swap's work is a hundred lines with its
     * own commit and rollback, and withLockTimeout's post-work restore would run
     * after that commit, outside any transaction, where SET LOCAL is a warning
     * and a no-op. The transaction ends either way, so the restore is not owed —
     * only the decision is.
     *
     * 0 is Postgres for "wait forever", so it is the LOOSEST value there is and
     * always tightens.
     */
    private static function tightensLockTimeout(int $previous, int $milliseconds): bool
    {
        return $previous === 0 || $previous > $milliseconds;
    }

    /** The advisory key convert() holds for the whole conversion of one table. */
    private static function conversionLockKey(string $table): string
    {
        return 'nightowl_partition:'.$table;
    }

    /**
     * Transaction-scoped attempt on a table's conversion key. Conflicts with
     * convert()'s SESSION-scoped hold from another session — advisory scope
     * affects release timing only, never conflict detection (verified against
     * postgres:17 in both directions).
     */
    private static function tryConversionXactLock(PDO $conn, string $table): bool
    {
        $stmt = $conn->prepare('SELECT pg_try_advisory_xact_lock(hashtext(?))');
        $stmt->execute([self::conversionLockKey($table)]);
        $held = $stmt->fetchColumn();

        return $held === true || $held === 't';
    }

    /**
     * The child window, with a lock ceiling applied.
     *
     * Every day here is a CREATE TABLE ... PARTITION OF, which takes ACCESS
     * EXCLUSIVE on the parent, and NEITHER caller inherits a usable bound. The
     * old comment here said the drain's tick "inherits the drain's guards" — it
     * does not when NIGHTOWL_DRAIN_CONN_TIMEOUTS=false, which is a documented
     * rollback switch, and that assumption is why the sweep went unbounded while
     * only the command was wired through this.
     *
     * The ceiling is a PARAMETER because the two paths genuinely differ:
     * nightowl:partition is a one-shot the operator is watching and can afford to
     * wait SWAP_LOCK_TIMEOUT_MS for its lock, while the drain would rather lose one
     * day's child for an hour (isolated, reported, retried) than hold a pending
     * exclusive request in front of a live raw table — MAINTENANCE_LOCK_TIMEOUT_MS.
     *
     * withLockTimeout only TIGHTENS, so a drain running with a lower
     * NIGHTOWL_DB_LOCK_TIMEOUT_MS keeps the operator's value on this path too.
     *
     * @return list<string> one entry per day that could not be created
     */
    private static function ensureChildWindowBounded(PDO $conn, string $table, int $today, int $milliseconds): array
    {
        return self::withLockTimeout(
            $conn,
            $milliseconds,
            static fn (): array => self::ensureChildWindow($conn, $table, $today),
        );
    }

    /**
     * The child window as a list of GAPS, never as an exception.
     *
     * Every caller of this reaches it having already established that the table
     * is partitioned — either because our swap just committed, or because a peer's
     * had. Missing children are a gap the hourly tick closes; they are not a
     * failed conversion, and they must never be reported as one.
     *
     * ensureChildWindow only isolates the PER-DAY work: its isPartitioned() and
     * historicConstraint() reads, and withLockTimeout's own lock_timeout read, sit
     * outside that try, so a connection blip in the moments after the swap
     * committed threw straight out of convert(). PartitionCommand then caught it
     * as a generic failure and printed the whole "still readable and writable…
     * a failure rolls it back" summary over a table that is fully converted with
     * a valid PK — advice the operator would act on, contradicted by the very
     * next run reporting "already partitioned".
     *
     * $historicBoundary is the bound OUR OWN conversion just froze, when there is
     * one. It is what lets the gap list name the days that were actually DUE
     * without asking the connection that has just failed us — see
     * dueChildDaysBestEffort().
     *
     * @return list<string> one entry per day that could not be created
     */
    private static function childWindowOrGaps(PDO $conn, string $table, int $today, ?int $historicBoundary = null): array
    {
        try {
            return self::ensureChildWindowBounded($conn, $table, $today, self::SWAP_LOCK_TIMEOUT_MS);
        } catch (\Throwable $e) {
            // One entry per day that was DUE — not one for the whole failure, and
            // not one per day of the raw window. Callers print count($gaps) as a
            // number of partitions ("N daily child partition(s) could not be
            // created"), so both errors mislead in opposite directions.
            // Collapsing the whole-window failure into a single string told the
            // operator one day had slipped when the entire window was missing, and
            // every raw row for those days lands in {t}_pdefault, which prune can
            // only row-DELETE, never DROP. Looping over DAYS_AHEAD + 1 was the
            // same error the other way: the historic child covers the days below
            // its frozen boundary, ensureChildWindow never INTENDS a child for
            // them, and reporting them made a table owed six children read as one
            // permanently short — an operator who then counts children finds the
            // count "wrong" forever.
            $gaps = [];
            foreach (self::dueChildDaysBestEffort($conn, $table, $today, $historicBoundary) as $day) {
                $gaps[] = self::childName($table, $day)
                    .': daily children could not be created after conversion — '.$e->getMessage();
            }

            // A failure must never leave through here as an empty list. Callers
            // treat empty as "clean" — PartitionCommand::exitCode returns SUCCESS
            // on it and prints "Done" — so zero due days would turn a real
            // post-commit failure into a silent success over a parent with no
            // daily children. It cannot happen while historicBoundary() is
            // today+2d and DAYS_AHEAD is 7 (six days always survive the filter),
            // but that is a coupling between two constants a thousand lines
            // apart with nothing asserting it, and the loop above is the only
            // thing carrying the error message at all.
            if ($gaps === []) {
                $gaps[] = $table.': daily children could not be created after conversion, and the days that were '
                    .'due could not be determined — '.$e->getMessage();
            }

            return $gaps;
        }
    }

    /**
     * The days of the window a table is actually DUE a child: [today, today +
     * DAYS_AHEAD] minus every day {t}_phistoric still covers. A covered day
     * cannot get one — CREATE ... PARTITION OF is rejected as an overlap (42P17),
     * and IF NOT EXISTS only suppresses a name clash.
     *
     * The SINGLE derivation, deliberately: ensureChildWindow creates from it and
     * childWindowOrGaps reports gaps from it. Two answers to "which days are due"
     * is exactly how the gap count came to over-report by a day on every populated
     * conversion, and re-deriving it from the boundary rule instead of sharing it
     * would only move the drift somewhere new the next time that rule changes.
     *
     * THROWS on a failed read, on purpose. This runs on the drain's hourly tick,
     * where a dead connection has to surface as one table-level failure —
     * swallowing it would return the whole window, make the sweep attempt a
     * covered day, and report a fabricated 42P17 per covered day in place of the
     * real cause. dueChildDaysBestEffort() is what turns a throw into a report,
     * and only on the path that has to produce one anyway.
     *
     * @return list<int> day epochs, ascending
     */
    private static function dueChildDays(PDO $conn, string $table, int $today): array
    {
        $historic = self::historicConstraint($conn, $table);
        $due = [];

        for ($d = 0; $d <= self::DAYS_AHEAD; $d++) {
            $day = $today + $d * 86400;

            if ($historic !== null && self::historicCovers($conn, $historic, $day)) {
                continue;
            }

            $due[] = $day;
        }

        return $due;
    }

    /**
     * dueChildDays() on the failure path, where an answer is still owed even
     * though the connection may be the thing that failed. Three sources, most
     * trustworthy first:
     *
     * - $historicBoundary, the bound OUR OWN conversion froze a few statements
     *   ago. Exact, and needs no connection at all — which is the point: we are
     *   here because a read just died. The bound is a midnight and the historic
     *   child covers (MINVALUE, bound), so no day straddles it and a simple
     *   >= comparison is the whole test.
     * - The catalog, for a PEER's conversion (both of convert()'s
     *   already-partitioned returns), where the bound was never ours to know.
     *   isolated() so a failed read cannot poison a caller's transaction.
     * - The whole window, when neither answers. Over-reporting is the safe
     *   direction here — a day named that turns out to exist costs the operator a
     *   recount, a day omitted hides rows piling up in {t}_pdefault — and it is
     *   also the exactly-right answer for the empty-table rebuild, which attaches
     *   no historic child and is therefore due every day of the window.
     *
     * @return list<int> day epochs, ascending
     */
    private static function dueChildDaysBestEffort(PDO $conn, string $table, int $today, ?int $historicBoundary): array
    {
        $window = [];
        for ($d = 0; $d <= self::DAYS_AHEAD; $d++) {
            $window[] = $today + $d * 86400;
        }

        if ($historicBoundary !== null) {
            return array_values(array_filter(
                $window,
                static fn (int $day): bool => $day >= $historicBoundary,
            ));
        }

        try {
            return self::isolated($conn, static fn (): array => self::dueChildDays($conn, $table, $today));
        } catch (\Throwable) {
            // The read that would have told us is the read that just failed.
            return $window;
        }
    }

    /**
     * Convert one raw table to a partitioned parent.
     *
     * Empty table (fresh installs, run from the migration): drop + recreate as
     * partitioned — nothing to preserve.
     *
     * Populated table (operator-run via nightowl:partition): rename the
     * original to {t}_phistoric and ATTACH it under a validated CHECK — no row
     * copying; the brief exclusive locks are at rename/attach, and the agent's
     * SQLite buffer absorbs drain retries through them.
     *
     * Concurrency: two runs of this DO happen — a deploy pipeline's run still
     * in flight while the operator runs it by hand (field incident: the
     * "retry from scratch" DROP of a leftover {tmp} dropped a LIVE peer's
     * {tmp} and killed it 42P01 mid index-replay). The ENTIRE conversion, prep
     * included, therefore runs under a per-table SESSION advisory lock, and the
     * loser is refused with ConversionInProgressException instead of dueling.
     *
     * The lock has to span the prep, not just the swap: two prep statements
     * cannot be transactional at all — CREATE INDEX CONCURRENTLY (25001), and
     * ADD CONSTRAINT ... NOT VALID + VALIDATE, whose full-table scan must not run
     * under the swap's ACCESS EXCLUSIVE — so they run in autocommit against
     * whatever `{table}` names WHEN THEY RUN. A run that preps after a peer
     * commits its swap therefore hits the new PARTITIONED parent: the CHECK is
     * accepted, cascades to every child, and rejects every drain row past its
     * frozen boundary (23514) — silent, and verified against postgres:17. Only a
     * lock the prep also holds prevents it, and the same lock is what lets the
     * drain's heal sweep run unattended without racing a live conversion.
     *
     * Session-scoped (not xact) is forced by that span. The drain's maintenance
     * tick deliberately uses xact locks because it runs behind transaction-mode
     * poolers, where a session lock and its unlock can land on different server
     * connections — leaking the lock and wedging every later conversion. That
     * hazard is real here too, so it is DETECTED at both ends rather than assumed
     * away: the baseline pid is read in the SAME statement that takes the key, so
     * the one move that would otherwise be invisible — between acquiring the lock
     * and learning where it landed — cannot hide inside a round trip;
     * assertSameBackend() re-reads pg_backend_pid() at each phase boundary and
     * throws PoolerAffinityException, which nightowl:partition treats as
     * fatal to the whole run rather than per-table; and the unlock's own return
     * value is read, because pg_advisory_unlock answering false is the only
     * evidence that the key is now stranded on a backend we cannot reach. A
     * conversion is a one-shot operator command; pointing it at the direct
     * Postgres port is a reasonable ask, and failing loudly beats corrupting
     * silently.
     *
     * @return list<string> daily children that could not be created afterwards.
     *                      The conversion SUCCEEDED whenever this returns at all
     *                      — a non-empty list is the hourly tick's to retry — but
     *                      the caller must report it rather than print "daily
     *                      children ahead" over a table that has none.
     */
    public static function convert(PDO $conn, string $table): array
    {
        // Already partitioned is NOT already finished. A peer SIGKILLed between
        // its swap commit and its own child sweep leaves a parent whose only
        // children are _phistoric and _pdefault, and every row drained from then
        // on lands in the DEFAULT — which prune can only row-DELETE, never DROP.
        // Returning silently here (and again under the lock) is exactly how a run
        // reports success over that state. This is the REPAIR entry point, not a
        // race artefact: PartitionCommand delegates every existing table here
        // rather than short-circuiting on relkind, because a run over that parent
        // is the operator asking for exactly this.
        if (self::isPartitioned($conn, $table)) {
            return self::childWindowOrGaps($conn, $table, intdiv(time(), 86400) * 86400);
        }

        // Retry briefly rather than refusing on the first miss. The drain's heal
        // sweep takes this exact key (xact-scoped, released when its tick
        // commits), and it takes it ONLY when a table carries an interrupted
        // run's leftovers — i.e. precisely after a killed conversion, i.e.
        // precisely when the operator re-runs this command. Refusing instantly
        // therefore told them "another nightowl:partition run is converting
        // {table}" about a run that did not exist, and the correlation made that
        // the common case rather than a rare one. A few seconds separates the two
        // holders cleanly: the sweep's hold is bounded by one tick, a real peer
        // conversion runs for minutes.
        // The key AND the backend that now holds it, in ONE statement. Sampled in a
        // round trip of its own, the pid answered "where are we NOW", not "where did
        // the lock land" — and behind a transaction-mode pooler those differ exactly
        // once, right here. The lock would go to backend A, the next statement to B,
        // B would become the expected pid, and every assertSameBackend() afterwards
        // would compare against B and pass: the conversion runs its non-transactional
        // prep believing it is serialised while the key sits on a backend nobody can
        // reach, and the unlock at the end answers false over a key stranded there,
        // refusing every later run until that backend is recycled. A pooler cannot
        // split one statement across backends, so this is the only place the two
        // facts can be established together.
        $lockStmt = $conn->prepare('SELECT pg_try_advisory_lock(hashtext(?)) AS locked, pg_backend_pid() AS pid');
        $deadline = microtime(true) + self::CONVERSION_LOCK_WAIT_MS / 1000;
        $backend = 0;
        $locked = false;

        do {
            $lockStmt->execute([self::conversionLockKey($table)]);
            $row = $lockStmt->fetch(PDO::FETCH_OBJ);

            if ($row !== false && ($row->locked === true || $row->locked === 't')) {
                // The pid of the backend that just took the key. A losing attempt's
                // pid is discarded on purpose: if attempt 1 misses on A and attempt 2
                // wins on B, B is the holder and B is what everything after must match.
                $backend = (int) $row->pid;
                $locked = true;
                break;
            }

            usleep(250000);
        } while (microtime(true) < $deadline);

        if (! $locked) {
            throw new ConversionInProgressException(
                "{$table}'s conversion lock is held by another session and did not free within "
                .(int) (self::CONVERSION_LOCK_WAIT_MS / 1000).'s — usually another nightowl:partition run still '
                .'in flight (a deploy pipeline?), which will finish the table itself. The running agent also '
                .'takes this lock for a moment when it clears an interrupted run\'s leftovers, so if no other '
                .'run exists, re-running in a minute succeeds. Nothing here changed the table.'
            );
        }

        try {
            return self::convertLocked($conn, $table, $backend);
        } finally {
            try {
                $unlock = $conn->prepare('SELECT pg_advisory_unlock(hashtext(?))');
                $unlock->execute([self::conversionLockKey($table)]);
                $released = $unlock->fetchColumn();

                // pg_advisory_unlock answers false — with a server WARNING, never
                // an error, and without poisoning an open transaction — when this
                // session does not hold the key. We took it above and release it
                // only here, so false can mean one thing: a transaction-mode
                // pooler moved us off the backend that holds it. The key then
                // survives on that backend until it closes, and EVERY later run is
                // refused as "another run is converting", naming a peer that does
                // not exist. Discarding this boolean threw away the only evidence
                // of that, and it is the one state an operator cannot debug.
                if ($released !== true && $released !== 't') {
                    error_log(sprintf(
                        '[NightOwl Support] could not release the conversion lock for %s — this session no longer '
                        .'holds it, so the connection moved off server backend pid %d (a transaction-mode pooler '
                        .'such as PgBouncer/Supavisor). The lock stays held on that backend until it closes, and '
                        .'every nightowl:partition run until then is refused as "another run is converting". Point '
                        .'the nightowl DB connection at the database port, then restart the pooler (or terminate '
                        .'backend %d) to clear it.',
                        $table,
                        $backend,
                        $backend,
                    ));
                }
            } catch (\Throwable) {
                // Best-effort: on a dead connection the lock died with the backend.
            }
        }
    }

    /**
     * The conversion proper, with the table's conversion lock held.
     *
     * Two phases. PREP runs in autocommit. Two of its statements cannot be
     * transactional at all — CREATE INDEX CONCURRENTLY (25001 inside a
     * transaction) and the NOT VALID ADD + VALIDATE, whose scan would otherwise
     * hold the swap transaction, and the ACCESS EXCLUSIVE it ends with, open for
     * its whole duration. normalizeCreatedAtType is there for a different reason:
     * it IS transactional, but it is a full-table rewrite measured in minutes and
     * must not sit inside the exclusive window. Everything else is in the SWAP
     * transaction, where a rollback undoes it for free.
     *
     * That division is deliberate and was earned. ALTER COLUMN created_at
     * SET NOT NULL and the PK demote used to run in prep, so ANY abort after
     * them — a pooler abort, a {t}_phistoric name clash, disk-full, a lock
     * timeout, a deadlock — left the LIVE table with no PRIMARY KEY at all, and
     * silently: pg_dump simply stops emitting the constraint, relreplident still
     * reads DEFAULT with zero indexes behind it, psql prints no Replica Identity
     * line, and a table in a publication then refuses every UPDATE and DELETE
     * (all observed against postgres:17). Both are catalog-cheap inside the
     * transaction — SET NOT NULL skips its scan because the CHECK validated in
     * prep proves non-nullness (PG 12+; 2.5 ms vs 156 ms on 2M rows, with
     * seq_scan staying at 0), and DROP CONSTRAINT ... ROLLBACK leaves the PK
     * index at the same relfilenode and byte count, no rebuild.
     *
     * Prep therefore leaves three residues, and only one bites:
     * {t}_id_created_at_pt is additive and a retry reuses it (an INVALID one is
     * dropped by the retry and by the drain's heal sweep); nightowl_logs'
     * created_at rewrite is irreversible but is the intended end state either
     * way; and the validated {t}_hist_ck is a frozen boundary that rejects every
     * drain row with 23514 once it passes. Any abort we survive takes that one
     * back off ($checkAdded); a KILLED process cannot, which is what
     * healConversionLeftovers() is for.
     *
     * @return list<string> daily children that could not be created afterwards
     */
    private static function convertLocked(PDO $conn, string $table, int $backend): array
    {
        // Hoisted above every return: BOTH exits owe the caller a child window.
        // The historic boundary is NOT derived from it — it gets its own clock
        // read at the moment it is frozen (see below), because everything between
        // here and there can take minutes.
        $today = intdiv(time(), 86400) * 86400;

        // A peer may have converted this table between the entry probe and the
        // lock — its swap is only visible now that we hold what it held. Do NOT
        // return bare: the peer may have been SIGKILLed the instant after its
        // swap committed, leaving a partitioned parent with zero daily children.
        if (self::isPartitioned($conn, $table)) {
            return self::childWindowOrGaps($conn, $table, $today);
        }

        // Bounded like every other exclusive statement here: lock_timeout caps
        // how long we WAIT for the lock, never how long we hold it, so the
        // minutes-long rewrite itself is unaffected — only the queue in front of
        // it is. Unbounded, this parks a pending ACCESS EXCLUSIVE ahead of every
        // reader and writer of the live table for as long as one dashboard query
        // runs.
        self::withLockTimeout(
            $conn,
            self::SWAP_LOCK_TIMEOUT_MS,
            static fn () => self::normalizeCreatedAtType($conn, $table),
        );

        // The rewrite above is the longest and least reversible statement in the
        // conversion — a full-table ACCESS EXCLUSIVE pass on a populated
        // nightowl_logs, minutes on a large one — and it used to run with no
        // affinity check between it and the lock at all. Check the moment it
        // returns: if the connection moved, we performed it without the lock we
        // believe we hold, and everything after it would compound that. The check
        // convert() would have made immediately after acquiring the lock is
        // deliberately NOT here, and only now is that argument actually sound:
        // convert() reads $backend in the SAME statement that takes the key, so
        // there is no round trip between the two and a check there would guard a
        // genuinely zero-statement gap. It used to sample the pid one statement
        // LATER, which is a ONE-statement gap — and under transaction-mode pooling
        // a one-statement gap is precisely where the move happens, so the check
        // this comment declines to make would have been the only one that mattered.
        self::assertSameBackend($conn, $backend, $table);

        $hasRows = (bool) $conn->query("SELECT EXISTS (SELECT 1 FROM {$table} LIMIT 1)")->fetchColumn();
        $historic = "{$table}_phistoric";
        $tmp = "{$table}_pnew";

        // A killed CONCURRENTLY build leaves an INVALID unique index that
        // would satisfy IF NOT EXISTS but not the ATTACH — drop it so the
        // rebuild is clean. Safe under the lock: no peer can be mid-build.
        $invalid = $conn->query(
            "SELECT NOT x.indisvalid FROM pg_index x JOIN pg_class c ON c.oid = x.indexrelid
             WHERE c.relname = '{$table}_id_created_at_pt'"
        )->fetchColumn();
        if ($invalid === true || $invalid === 't') {
            self::withLockTimeout(
                $conn,
                self::SWAP_LOCK_TIMEOUT_MS,
                static fn () => $conn->exec("DROP INDEX IF EXISTS {$table}_id_created_at_pt"),
            );
        }

        // The ONLY live-table statement left outside a transaction, and the only
        // one still needing a hand-written unwind. Once the CHECK is on, ANY exit
        // without the swap leaves a frozen boundary that starts rejecting drain
        // rows (23514) the moment it passes — NOT VALID included, since NOT VALID
        // only skips validating the rows already there.
        $checkAdded = false;

        // The historic child's frozen upper bound, epoch and literal. Set on the
        // POPULATED path only: the empty-table rebuild DROPs the original and
        // ATTACHes nothing, so there is no historic child, no day of the child
        // window is covered, and the gap report below must filter nothing out.
        $boundaryEpoch = null;
        $boundary = null;

        try {
            // Un-nested from `if ($hasRows)`: an empty table used to make no
            // affinity check at all until the one before beginTransaction.
            self::assertSameBackend($conn, $backend, $table);

            if ($hasRows) {
                // The parent PK (id, created_at) needs a matching unique index on
                // the attached partition or ATTACH would build one under lock.
                $conn->exec(
                    "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {$table}_id_created_at_pt ON {$table} (id, created_at)"
                );

                // Sampled HERE — after CIC, immediately before the CHECK that
                // freezes it — so the only statements between the clock read and
                // the boundary going live are VALIDATE CONSTRAINT (one sequential
                // scan) and the swap transaction (catalog-only, its lock wait
                // capped at SWAP_LOCK_TIMEOUT_MS). CIC is deliberately OUTSIDE
                // that window: it waits out every concurrent transaction, twice,
                // so it is the one prep statement whose duration nothing bounds.
                // historicBoundary() then puts the bound a full day past that —
                // see it for why the next midnight never was enough.
                $boundaryEpoch = self::historicBoundary(time());
                $boundary = gmdate('Y-m-d 00:00:00', $boundaryEpoch);

                // A run killed between ADD and the swap leaves {t}_hist_ck behind:
                // re-ADDing it would die 42710, and worse, once its frozen boundary
                // passed, the validated CHECK would start rejecting every drain
                // INSERT. Rebuild it fresh so the boundary is always this run's.
                self::withLockTimeout(
                    $conn,
                    self::SWAP_LOCK_TIMEOUT_MS,
                    static fn () => $conn->exec("ALTER TABLE {$table} DROP CONSTRAINT IF EXISTS {$table}_hist_ck"),
                );
                // Validated CHECK lets ATTACH skip its full-table scan. IS NOT NULL
                // is folded in because the parent's PK forces created_at NOT NULL
                // and the original column is nullable — the validated CHECK then
                // lets the SET NOT NULL in the swap transaction skip ITS scan too
                // (PG 12+), which is what makes that statement cheap enough to sit
                // inside the transaction at all. A legacy row with NULL created_at
                // fails validation here with a clear error; such rows are invisible
                // to every time-filtered reader anyway and must be fixed (or
                // deleted) before partitioning.
                self::withLockTimeout(
                    $conn,
                    self::SWAP_LOCK_TIMEOUT_MS,
                    static fn () => $conn->exec(
                        "ALTER TABLE {$table} ADD CONSTRAINT {$table}_hist_ck CHECK (created_at IS NOT NULL AND created_at < '{$boundary}') NOT VALID"
                    ),
                );
                $checkAdded = true;
                $conn->exec("ALTER TABLE {$table} VALIDATE CONSTRAINT {$table}_hist_ck");
            }

            // The id sequence is OWNED BY the original table's column; re-own it to
            // the parent so dropping the historic partition later can't cascade it.
            // Read here, applied inside the transaction — see the swap for why it
            // must never be committed on its own.
            $seqStmt = $conn->prepare("SELECT pg_get_serial_sequence(?, 'id')");
            $seqStmt->execute([$table]);
            $seq = $seqStmt->fetchColumn() ?: null;

            self::assertSameBackend($conn, $backend, $table);

            $conn->beginTransaction();
            try {
                // Bound every lock this transaction waits for. Without it, a swap
                // queued behind a long dashboard read parks a PENDING ACCESS
                // EXCLUSIVE in front of every new reader and writer of the LIVE
                // table for the duration — Postgres queues new requests behind a
                // pending exclusive one, so the damage is not limited to us
                // (reproduced: a plain SELECT arriving afterwards blocked until
                // the holder committed and then died on its own timeout). The
                // `nightowl` connection sets no lock_timeout of its own, so this
                // is the only thing standing between a busy table and the
                // conversion BEING the outage. A timeout here changes nothing and
                // is retryable.
                //
                // A CEILING, exactly like every other lock-bounded statement in
                // this file. Unconditionally, this RAISED the bound for an
                // operator who had already set a stricter one — 500ms becoming
                // 15s in front of the LOCK TABLE below — which is the hazard the
                // constant exists to prevent. No restore is owed: SET LOCAL dies
                // with this transaction on both the commit and the rollback path.
                if (self::tightensLockTimeout(self::currentLockTimeoutMs($conn), self::SWAP_LOCK_TIMEOUT_MS)) {
                    $conn->exec('SET LOCAL lock_timeout = '.self::SWAP_LOCK_TIMEOUT_MS);
                }

                // Belt-and-braces against a pooler that broke session affinity
                // undetected: a peer holding the session lock on another backend
                // still blocks this, and our own session lock makes it a no-op
                // for us (advisory locks are re-entrant within a session). Safe
                // to throw here — nothing destructive has touched the live table
                // yet, so the rollback is complete.
                $xact = $conn->prepare('SELECT pg_try_advisory_xact_lock(hashtext(?))');
                $xact->execute([self::conversionLockKey($table)]);
                $held = $xact->fetchColumn();
                if ($held !== true && $held !== 't') {
                    throw new ConversionInProgressException(
                        "another nightowl:partition run is converting {$table} — let it finish, then re-run."
                    );
                }

                // A {tmp} left by a pre-lock agent version killed mid-run is
                // disposable; under the lock it can no longer be a live peer's.
                $conn->exec("DROP TABLE IF EXISTS {$tmp}");

                // BUILD THE NEW PARENT FIRST, BEFORE freezing the live table.
                // Every statement here is catalog-only — {tmp} holds no rows and
                // has no partitions, so ADD PRIMARY KEY and the index replays
                // build nothing, and the whole block took 4.8 ms on an 11-column,
                // 7-index, 300k-row table — but each one is a network ROUND TRIP,
                // and there are 5 + one per user index of them. Holding ACCESS
                // EXCLUSIVE across them charged the tenant that whole latency in
                // frozen reads and writes for nothing. (The comment that used to
                // justify the other order called this "one catalog-only
                // statement"; it never was.)
                //
                // The trade, stated plainly: CREATE TABLE ... LIKE takes ACCESS
                // SHARE on {table} and holds it for the transaction, so the LOCK
                // TABLE below is a lock UPGRADE. It queues and drains FIFO. It
                // CAN deadlock against the drain — see the ALTER SEQUENCE below
                // for the one cycle that exists and why the ordering there is
                // load-bearing — and when it does, Postgres kills this
                // transaction and the rollback leaves the live table pristine
                // (verified: PK intact, no stranded {t}_hist_ck, {tmp} or
                // advisory lock). Building inside the transaction (rather than in
                // autocommit, where HEAD built it) also means an abort here
                // strands no {tmp} at all.
                $conn->exec(
                    "CREATE TABLE {$tmp} (LIKE {$table} INCLUDING DEFAULTS INCLUDING STORAGE) PARTITION BY RANGE (created_at)"
                );
                $conn->exec("ALTER TABLE {$tmp} ADD PRIMARY KEY (id, created_at)");

                foreach (self::indexDefs($conn, $table) as $def) {
                    // Replay: point the definition at the parent under a fresh name
                    // (sequential replaces — the two patterns share the " ON " token).
                    // indexDefs excludes indisprimary, so the still-present (id) PK
                    // is filtered here exactly as it was when the demote ran in prep.
                    $sql = str_replace("INDEX {$def->indexname} ON", "INDEX {$def->indexname}_pt ON", $def->indexdef);
                    $sql = str_replace("ON public.{$table}", "ON public.{$tmp}", $sql);
                    $conn->exec($sql);
                }

                // Now freeze the table. RENAME/DROP need this anyway, and taking
                // it here makes the exclusive window the swap and nothing else.
                $conn->exec("LOCK TABLE {$table} IN ACCESS EXCLUSIVE MODE");

                // Re-own the id sequence to the new parent: the clone's id
                // DEFAULT references it, and it is OWNED BY the old table — a DROP
                // before re-owning raises 2BP01 (dependent objects). This statement
                // must NEVER run outside this transaction: committed on its own and
                // then aborted, the sequence stays OWNED BY {tmp}, and every later
                // run's DROP TABLE IF EXISTS {tmp} fails 2BP01 ("default value for
                // column id of table {table} depends on sequence") — a permanent
                // retry wedge needing manual repair. Reproduced on postgres:17.
                //
                // AFTER the LOCK TABLE, and that ordering is the whole fix for a
                // measured deadlock. ALTER SEQUENCE ... OWNED BY takes SHARE ROW
                // EXCLUSIVE on {t}_id_seq and holds it to commit; every drain COPY
                // omits `id`, so each batch calls nextval() and takes ROW
                // EXCLUSIVE on that same sequence while already holding ROW
                // EXCLUSIVE on the table. Run before the LOCK, that closes a
                // cycle — we hold the sequence and want the table, the drain holds
                // the table and wants the sequence — and lock_timeout does not
                // save it because deadlock_timeout (1s) fires first, so the
                // operator gets a bare 40P01 instead of a retryable 55P03.
                // Measured under concurrent drain load: 10/60 and 8/60
                // conversions deadlocked (killing whole drain batches) with this
                // statement before the LOCK, 0/60 with it after. Once ACCESS
                // EXCLUSIVE on the table is held no session can hold the sequence
                // — every nextval holder also holds ROW EXCLUSIVE on the table —
                // so here it is uncontended.
                if ($seq !== null) {
                    $conn->exec("ALTER SEQUENCE {$seq} OWNED BY {$tmp}.id");
                }

                // A table probed empty above can have received drain rows since,
                // and DROP would take them with it. Only meaningful under the lock.
                if (! $hasRows
                    && (bool) $conn->query("SELECT EXISTS (SELECT 1 FROM {$table} LIMIT 1)")->fetchColumn()) {
                    throw new \RuntimeException(
                        "{$table} received rows mid-conversion — re-run nightowl:partition."
                    );
                }

                if ($hasRows) {
                    // Relocated prep, now transactional. Both are catalog-only:
                    // SET NOT NULL skips its scan because {t}_hist_ck is a
                    // VALIDATED constraint proving non-nullness, and DROP
                    // CONSTRAINT on the PK is pure catalog — a rollback restores
                    // the identical relfilenode, byte for byte, with the index
                    // still enforcing uniqueness. Under ACCESS EXCLUSIVE they cost
                    // microseconds; in prep they cost the table its primary key on
                    // every abort.
                    $conn->exec("ALTER TABLE {$table} ALTER COLUMN created_at SET NOT NULL");

                    // A partition cannot carry its own PRIMARY KEY next to the
                    // parent's composite one (42P16). Demote the old (id) PK — the
                    // (id, created_at) unique index built in prep carries
                    // uniqueness through the attach, and id keeps its NOT NULL
                    // column attribute.
                    $pkStmt = $conn->prepare(
                        "SELECT conname FROM pg_constraint WHERE conrelid = ?::regclass AND contype = 'p'"
                    );
                    $pkStmt->execute([$table]);
                    $pkName = $pkStmt->fetchColumn();
                    if ($pkName !== false) {
                        $conn->exec("ALTER TABLE {$table} DROP CONSTRAINT {$pkName}");
                    }

                    $conn->exec("ALTER TABLE {$table} RENAME TO {$historic}");
                } else {
                    $conn->exec("DROP TABLE {$table}");
                }
                $conn->exec("ALTER TABLE {$tmp} RENAME TO {$table}");

                if ($hasRows) {
                    $conn->exec(
                        "ALTER TABLE {$table} ATTACH PARTITION {$historic} FOR VALUES FROM (MINVALUE) TO ('{$boundary}')"
                    );
                }

                $conn->exec(
                    "CREATE TABLE {$table}_pdefault PARTITION OF {$table} DEFAULT"
                );
                $conn->commit();
                // The CHECK now lives on the historic child, whose partition
                // bound says the same thing — it is no longer a live-table
                // time bomb, so the abort cleanup must not undo it.
                $checkAdded = false;
            } catch (\Throwable $e) {
                if ($conn->inTransaction()) {
                    // Guarded like every other rollback on this path
                    // (healConversionLeftovers, RecordWriter::healRawPartitionLeftovers,
                    // doWrite): the swap's most violent deaths take the BACKEND
                    // with them — the idle_in_transaction_session_timeout the
                    // agent itself sets on tenant DBs, an admin's
                    // pg_terminate_backend, a transaction-mode pooler dropping
                    // the connection — and PDO::inTransaction() still answers
                    // true against a dead handle, so ROLLBACK raises "server
                    // closed the connection unexpectedly" ON TOP of the cause.
                    // Unguarded, that replaced $e and skipped the rethrow below:
                    // the operator saw a connection error instead of the 55P03 /
                    // 40P01 / 57P01 that says whether this was a lock timeout, a
                    // deadlock or a dead server — and, worse, the exception TYPE
                    // went with it, so a ConversionInProgressException raised at
                    // this transaction's own xact-lock probe above stopped
                    // reaching PartitionCommand's contention branch and a
                    // retryable BUSY (3) was reported as a hard FAILURE (1).
                    // The rollback is best-effort; the diagnosis is not.
                    try {
                        $conn->rollBack();
                    } catch (\Throwable) {
                        // Never mask the real cause.
                    }
                }

                throw $e;
            }
        } catch (\Throwable $e) {
            if ($checkAdded) {
                // Leaving it is worse than any error we could raise here: past
                // its boundary it silently fails every drain write. Everything
                // else this conversion did to the live table was inside the swap
                // transaction and is already gone. If the unwind ITSELF fails,
                // the table is in that state RIGHT NOW and only the drain's heal
                // sweep will reach it — say so, rather than swallowing the one
                // line that turns a future silent 23514 outage into a warning.
                //
                // Bounded, and this is the sharpest case for it: we reach here
                // most often BECAUSE the swap's LOCK TABLE just timed out, and
                // the unwind then re-requests ACCESS EXCLUSIVE against the very
                // blocker that caused it — on a connection whose SET LOCAL died
                // with the rolled-back transaction. Unbounded, the clean
                // retryable 55P03 the swap ceiling exists to produce turned
                // straight into the table-wide write outage it exists to prevent
                // (measured: 25 s and climbing, with a drain INSERT queued 17 s
                // behind it, and the command never returning to print anything).
                try {
                    self::withLockTimeout(
                        $conn,
                        self::SWAP_LOCK_TIMEOUT_MS,
                        static fn () => $conn->exec("ALTER TABLE {$table} DROP CONSTRAINT IF EXISTS {$table}_hist_ck"),
                    );
                } catch (\Throwable $unwind) {
                    error_log(sprintf(
                        '[NightOwl Support] %s: could not remove the interrupted conversion\'s boundary CHECK (%s). '
                        .'It is harmless until its boundary passes — at least 24 hours from now, because the '
                        .'boundary is frozen at the SECOND UTC midnight ahead — and rejects every drain row for '
                        .'this table (23514) from that moment. A running agent strips it on the first cleanup '
                        .'tick AFTER the boundary passes — not before, because until then it is indistinguishable '
                        .'from a live conversion\'s scaffold — so the outage starts and ends within about a minute '
                        .'of that midnight. You have a day to avoid it entirely: drop %s_hist_ck by hand now.',
                        $table,
                        $unwind->getMessage(),
                        $table,
                    ));
                }
            }

            throw $e;
        }

        // Deliberately OUTSIDE the swap transaction. Folding it in would make
        // "converted implies has children" atomic, at the price of one ACCESS
        // EXCLUSIVE round trip per due day inside the exclusive window. Not worth
        // it: a crash here leaves a parent whose _phistoric still covers today and
        // tomorrow, so those days' rows keep routing correctly, later rows land in
        // _pdefault, and the hourly tick both creates the missing children and
        // ADOPTS those rows into them — self-healing within the hour, with no row
        // lost or duplicated.
        //
        // $boundaryEpoch, not a re-read: if the window dies here it is because the
        // connection did, and the gap list must still name the days that were DUE
        // rather than every day of the window. It is null on the empty-table path,
        // which attaches no historic child and is due every day.
        $failures = self::childWindowOrGaps($conn, $table, $today, $boundaryEpoch);
        self::logFailures($failures);

        return $failures;
    }

    /**
     * Fail loudly if the connection moved to a different server backend — the
     * transaction-mode-pooler case, where the session lock we believe we hold
     * lives on a backend we are no longer talking to (and our unlock will miss
     * it, wedging every later conversion). Cheap enough to call per phase, and
     * called at every phase boundary: what it protects is the long statements
     * BETWEEN the boundaries, so a check that only runs on some paths protects
     * only some conversions.
     *
     * Its own exception type because the RESPONSE differs: contention is
     * per-table and transient, this is a connection-wide misconfiguration and
     * nightowl:partition must stop rather than push prep DDL through the same
     * pooler for the ten remaining tables, stranding a session advisory lock on
     * each. Before it was typed, the plain RuntimeException escaped handle() and
     * stopped the run as a side effect; the per-table catch(\Throwable) turned
     * that into "log it and continue".
     */
    private static function assertSameBackend(PDO $conn, int $expected, string $table): void
    {
        $pid = (int) $conn->query('SELECT pg_backend_pid()')->fetchColumn();

        if ($pid !== $expected) {
            throw new PoolerAffinityException(
                "the connection moved between server backends mid-conversion of {$table} (pid {$expected} → {$pid}) "
                .'— nightowl:partition needs a direct PostgreSQL connection, not a transaction-mode pooler '
                .'(PgBouncer/Supavisor). Point the nightowl DB connection at the database port and re-run.'
            );
        }
    }

    /**
     * Whether $table has a {t}_phistoric child — i.e. whether it was converted
     * by the populated path (rename + ATTACH) rather than the empty-table
     * rebuild, which DROPs the original and attaches nothing.
     */
    public static function hasHistoricChild(PDO $conn, string $table): bool
    {
        // nspname-qualified like every other catalog probe here: a tenant DB with
        // same-named tables in another schema would otherwise match the wrong
        // pair and flip the command's success line.
        return (bool) $conn->query(
            "SELECT EXISTS (
                SELECT 1 FROM pg_inherits i
                JOIN pg_class c ON c.oid = i.inhrelid
                JOIN pg_class p ON p.oid = i.inhparent
                JOIN pg_namespace n ON n.oid = p.relnamespace
                WHERE n.nspname = 'public'
                  AND p.relname = '{$table}' AND c.relname = '{$table}_phistoric'
            )"
        )->fetchColumn();
    }

    /**
     * Droppable children strictly older than the cutoff: daily children whose
     * whole range is expired. The historic and default partitions are never
     * dropped here (they get the row-DELETE path).
     *
     * @return list<string>
     */
    public static function expiredChildren(PDO $conn, string $table, string $cutoff): array
    {
        $stmt = $conn->prepare(
            "SELECT c.relname
             FROM pg_inherits i
             JOIN pg_class c ON c.oid = i.inhrelid
             JOIN pg_class p ON p.oid = i.inhparent
             WHERE p.relname = ?"
        );
        $stmt->execute([$table]);
        $rows = $stmt->fetchAll(PDO::FETCH_OBJ);

        $expired = [];
        foreach ($rows as $row) {
            if (! preg_match('/^'.preg_quote($table, '/').'_p(\d{8})$/', $row->relname, $m)) {
                continue; // historic/default — row-DELETE territory
            }
            $upper = gmdate('Y-m-d 00:00:00', strtotime($m[1].' +1 day UTC'));
            if ($upper <= $cutoff) {
                $expired[] = $row->relname;
            }
        }

        return $expired;
    }

    /**
     * Drop the historic partition once prune has emptied it — the moment a
     * converted tenant's entire pre-conversion bloat returns to the OS in one
     * unlink. Emptiness is the trigger (not boundary math): its upper bound
     * was frozen at conversion, it receives no new rows, so zero rows after a
     * row-DELETE pass is unambiguous. The DEFAULT child is never dropped — a
     * concurrent backdated drain would fail with "no partition for row".
     */
    public static function dropEmptyHistoric(PDO $conn, string $table): bool
    {
        $historic = "{$table}_phistoric";

        $isChild = (bool) $conn->query(
            "SELECT EXISTS (
                SELECT 1 FROM pg_inherits i
                JOIN pg_class c ON c.oid = i.inhrelid
                JOIN pg_class p ON p.oid = i.inhparent
                WHERE p.relname = '{$table}' AND c.relname = '{$historic}'
            )"
        )->fetchColumn();
        if (! $isChild) {
            return false;
        }

        if ((bool) $conn->query("SELECT EXISTS (SELECT 1 FROM {$historic} LIMIT 1)")->fetchColumn()) {
            return false;
        }

        $conn->exec("DROP TABLE {$historic}");

        return true;
    }

    /**
     * Partitionable raw tables that are still plain (relkind 'r') AND hold
     * rows — the set the visible "run nightowl:partition" warnings report.
     *
     * @return list<string>
     */
    public static function unpartitionedPopulated(PDO $conn, ?array $tables = null): array
    {
        $out = [];
        foreach ($tables ?? self::TABLES as $table) {
            $stmt = $conn->prepare(
                "SELECT relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'public' AND c.relname = ?"
            );
            $stmt->execute([$table]);
            if ($stmt->fetchColumn() !== 'r') {
                continue; // partitioned already, or absent
            }
            if ((bool) $conn->query("SELECT EXISTS (SELECT 1 FROM {$table} LIMIT 1)")->fetchColumn()) {
                $out[] = $table;
            }
        }

        return $out;
    }

    /**
     * Rewrite a varchar created_at (nightowl_logs' historical accident) to a
     * proper timestamp so it can key the range partition. Full-table rewrite
     * under ACCESS EXCLUSIVE on populated tables — why nightowl:partition is
     * operator-run. Rows with NULL/empty created_at (already invisible to
     * every time-filtered reader) become epoch-dated and age out via prune.
     */
    private static function normalizeCreatedAtType(PDO $conn, string $table): void
    {
        $stmt = $conn->prepare(
            "SELECT data_type FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = ? AND column_name = 'created_at'"
        );
        $stmt->execute([$table]);
        $type = $stmt->fetchColumn();

        if ($type === false || $type === 'timestamp without time zone') {
            return;
        }

        $conn->exec(
            "ALTER TABLE {$table} ALTER COLUMN created_at TYPE timestamp
             USING COALESCE(NULLIF(created_at, '')::timestamp, '1970-01-01 00:00:00'::timestamp)"
        );
    }

    /**
     * Non-PK indexes on $table, for replay onto the new parent.
     *
     * {t}_id_created_at_pt is excluded: it is not a user index but the
     * conversion's OWN scaffolding, built CONCURRENTLY during prep so the
     * historic child already carries the unique index the parent's composite PK
     * requires. Replaying it would give the parent a second (id, created_at)
     * unique index as {t}_id_created_at_pt_pt — one the historic child does not
     * have, so ATTACH PARTITION would build it inline, full-table, under the
     * ACCESS EXCLUSIVE the whole design exists to keep brief (and every daily
     * child would then carry a duplicate btree forever).
     *
     * @return list<object{indexname: string, indexdef: string}>
     */
    private static function indexDefs(PDO $conn, string $table): array
    {
        $stmt = $conn->prepare(
            "SELECT i.indexname, i.indexdef
             FROM pg_indexes i
             WHERE i.schemaname = 'public' AND i.tablename = ?
               AND i.indexname <> ?
               AND i.indexname NOT IN (
                   SELECT c.relname FROM pg_index x
                   JOIN pg_class c ON c.oid = x.indexrelid
                   JOIN pg_class t ON t.oid = x.indrelid
                   WHERE t.relname = ? AND x.indisprimary
               )"
        );
        $stmt->execute([$table, "{$table}_id_created_at_pt", $table]);

        return $stmt->fetchAll(PDO::FETCH_OBJ);
    }

    /**
     * Today's child plus DAYS_AHEAD future days for one table, skipping days the
     * historic child still covers — a child inside its range is rejected as an
     * overlap (42P17), and IF NOT EXISTS only suppresses a name clash. WHICH days
     * those are is dueChildDays()' answer, shared with the gap reporting so the
     * two can never disagree about how many children were ever owed.
     *
     * The covered-day test therefore sits outside the per-day savepoint now, and
     * that is not a loss: the constraint expression is the same for every day, so
     * a historicCovers() failure was never a one-day event — it produced eight
     * identical per-day failures where one table-level failure was the truth.
     * ensureFutureChildren already isolates this whole call per table.
     *
     * Each day still stands alone: one that cannot be CREATED must not cost the
     * days after it their children.
     *
     * @return list<string> one entry per day that could not be created
     */
    private static function ensureChildWindow(PDO $conn, string $table, int $today): array
    {
        if (! self::isPartitioned($conn, $table)) {
            return [];
        }

        $failures = [];

        foreach (self::dueChildDays($conn, $table, $today) as $day) {
            try {
                self::isolated($conn, static fn () => self::ensureDailyChild($conn, $table, $day));
            } catch (\Throwable $e) {
                $failures[] = self::childName($table, $day).': '.$e->getMessage();
            }
        }

        return $failures;
    }

    /**
     * The partition constraint of {table}_phistoric — the predicate a row must
     * satisfy to belong to it — or null when there is no historic child (an
     * empty table converts by rebuild, so fresh installs never get one and
     * today's child is theirs to create).
     */
    private static function historicConstraint(PDO $conn, string $table): ?string
    {
        $stmt = $conn->prepare(
            "SELECT pg_get_partition_constraintdef(c.oid)
             FROM pg_inherits i
             JOIN pg_class c ON c.oid = i.inhrelid
             JOIN pg_class p ON p.oid = i.inhparent
             WHERE p.relname = ? AND c.relname = ?"
        );
        $stmt->execute([$table, "{$table}_phistoric"]);
        $def = $stmt->fetchColumn();

        return is_string($def) && $def !== '' ? $def : null;
    }

    /**
     * Whether the historic child still owns $day. Postgres evaluates its own
     * partition constraint against the candidate timestamp, so nothing here
     * depends on how the catalog renders a bound. That bound is frozen at
     * conversion — this stops excluding anything once the day passes, and the
     * sweep runs on every tick forever, not only on conversion day.
     */
    private static function historicCovers(PDO $conn, string $constraint, int $day): bool
    {
        $stmt = $conn->prepare("SELECT ({$constraint}) FROM (SELECT ?::timestamp AS created_at) probe");
        $stmt->execute([gmdate('Y-m-d 00:00:00', $day)]);
        $covered = $stmt->fetchColumn();

        return $covered === true || $covered === 't';
    }

    /**
     * Move the DEFAULT child's rows for this day into a standalone table, then
     * ATTACH that as the day's partition.
     *
     * The rows travel while the parent is held at ACCESS SHARE only. A
     * CREATE ... PARTITION OF takes ACCESS EXCLUSIVE on the parent and keeps it
     * until commit, which would block every concurrent drain COPY for as long as
     * the move runs — and the tick's transaction can outlive the move by ten more
     * tables. ATTACH takes SHARE UPDATE EXCLUSIVE, which the drain's ROW
     * EXCLUSIVE does not conflict with.
     *
     * The CHECK is what lets ATTACH skip re-scanning the rows just moved; the
     * partition bound subsumes it afterwards. CONSTRAINTS must come across in
     * the LIKE — ATTACH refuses a child missing any of the parent's CHECKs.
     */
    private static function adoptDefaultRows(PDO $conn, string $table, string $child, string $from, string $to): void
    {
        // Emptying the default and attaching the rows' new home must be atomic
        // or a crash strands them in a table nothing reads. Inside a caller's
        // transaction that is already guaranteed (and beginTransaction would
        // throw — PDO has no nesting); the enclosing savepoint unwinds it.
        $ownTransaction = ! $conn->inTransaction();

        if ($ownTransaction) {
            $conn->beginTransaction();
        }

        try {
            $conn->exec(
                "CREATE TABLE {$child} (LIKE {$table} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE)"
            );
            $conn->exec(
                "ALTER TABLE {$child} ADD CONSTRAINT {$child}_adopt_ck
                 CHECK (created_at >= '{$from}' AND created_at < '{$to}')"
            );
            $conn->exec(
                "WITH moved AS (
                     DELETE FROM {$table}_pdefault
                     WHERE created_at >= '{$from}' AND created_at < '{$to}'
                     RETURNING *
                 )
                 INSERT INTO {$child} SELECT * FROM moved"
            );
            $conn->exec(
                "ALTER TABLE {$table} ATTACH PARTITION {$child} FOR VALUES FROM ('{$from}') TO ('{$to}')"
            );
            $conn->exec("ALTER TABLE {$child} DROP CONSTRAINT {$child}_adopt_ck");

            if ($ownTransaction) {
                $conn->commit();
            }
        } catch (\Throwable $e) {
            if ($ownTransaction) {
                $conn->rollBack();
            }
            throw $e;
        }
    }

    /**
     * Run $work so that its failure poisons neither the work already done nor
     * the work still to come. Autocommit gives that per statement; inside a
     * caller's transaction — the drain's maintenance tick holds a
     * transaction-scoped advisory lock and runs the whole sweep in one — the
     * first error aborts the block and every later statement dies 25P02, so
     * only a SAVEPOINT can. Names are sequenced because these nest.
     */
    private static function isolated(PDO $conn, callable $work): mixed
    {
        if (! $conn->inTransaction()) {
            return $work();
        }

        $savepoint = 'nightowl_rp_'.(++self::$savepointSeq);
        $conn->exec("SAVEPOINT {$savepoint}");

        try {
            $result = $work();
        } catch (\Throwable $e) {
            // Unwinding throws too on a dead connection, and the cause being
            // unwound is the one worth reporting.
            try {
                $conn->exec("ROLLBACK TO SAVEPOINT {$savepoint}");
                $conn->exec("RELEASE SAVEPOINT {$savepoint}");
            } catch (\Throwable) {
            }

            throw $e;
        }

        $conn->exec("RELEASE SAVEPOINT {$savepoint}");

        return $result;
    }

    /**
     * @param  list<string>  $failures
     */
    private static function logFailures(array $failures): void
    {
        if ($failures === []) {
            return;
        }

        error_log(sprintf(
            '[NightOwl Support] %d daily partition(s) not created (retried next tick) — %s',
            count($failures),
            implode('; ', array_slice($failures, 0, 3)).(count($failures) > 3 ? ' (…)' : ''),
        ));
    }
}
