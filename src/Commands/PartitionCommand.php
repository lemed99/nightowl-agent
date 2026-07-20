<?php

namespace NightOwl\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use NightOwl\Support\ConversionInProgressException;
use NightOwl\Support\PoolerAffinityException;
use NightOwl\Support\RawPartitions;

class PartitionCommand extends Command
{
    protected $signature = 'nightowl:partition
        {--table= : Restrict to one raw table (e.g. nightowl_queries)}';

    protected $description = 'Convert the raw telemetry tables to native daily partitioning (prune becomes instant DROP PARTITION)';

    /**
     * Nothing converted here is broken — another run holds the lock and is
     * doing the same work. Distinct from FAILURE (1) so a caller can retry or
     * tolerate contention without also swallowing genuine conversion errors, and
     * from INCOMPLETE (4), where the conversions DID land and only their child
     * windows are missing; still non-zero, because tables the operator asked
     * about are unconverted.
     *
     * 3, NOT 2: Symfony declares Command::INVALID = 2 ("the command was invoked
     * incorrectly") and Illuminate\Console\Command inherits it, so a BUSY of 2
     * was a second name for a documented usage-error code. PHP warns about
     * neither the collision nor a test asserting Command::INVALID that a merely
     * contended run would satisfy. The constant has never shipped in a release,
     * so this costs nothing but the two doc lines that quote the number.
     */
    public const BUSY = 3;

    /**
     * Every conversion this run attempted landed, but at least one table came
     * back owing daily children. The child window runs OUTSIDE the swap
     * transaction on purpose — folding it in would add one ACCESS EXCLUSIVE round
     * trip per due day to the exclusive window — so it can fail over a table that
     * is fully, correctly converted.
     *
     * Non-zero because the tables are not in the state this command promises: a
     * day with no child sends its rows to {t}_pdefault, which prune can only
     * row-DELETE, never DROP. Distinct from FAILURE because nothing is broken and
     * there is nothing to repair first, and from BUSY because the conversions
     * themselves are DONE — no peer is going to finish them, and the thing that
     * closes the gap is a maintenance pass, not a retry of the conversion.
     *
     * 4, following BUSY = 3 for the same reason: 2 is Symfony's inherited
     * Command::INVALID.
     */
    public const INCOMPLETE = 4;

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

        $busy = [];
        $failed = [];
        $childGaps = [];

        foreach ($tables as $table) {
            if (! $schema->hasTable($table)) {
                $this->warn("Skipping {$table} (does not exist — run nightowl:migrate).");

                continue;
            }

            // Deliberately NOT a short-circuit — this probe picks the WORDING and
            // nothing else. "Already partitioned" is not "already finished": a
            // conversion SIGKILLed between its swap committing and its own child
            // sweep (the window RawPartitions::convertLocked calls out where it
            // puts childWindowOrGaps outside the swap transaction) leaves a parent
            // whose only children are _phistoric and _pdefault, and every row
            // drained from then on lands in the DEFAULT — which prune can only
            // row-DELETE, never DROP. convert() carries the recovery for exactly
            // that state, and this command is the tool an operator re-runs to get
            // it; skipping the call here made that branch reachable only by a run
            // that LOST a race, never by the run that was asked to do the repair,
            // while this one printed "already partitioned." and exited SUCCESS
            // over it. Cheap on a healthy table: the entry probe returns before
            // the advisory-lock wait (so this can never report contention that
            // isn't there), and the CREATE ... IF NOT EXISTS statements it does
            // issue are catalog no-ops under ensureChildWindowBounded's
            // lock_timeout ceiling — 23 ms for a whole table against a local
            // postgres:17.
            try {
                $already = RawPartitions::isPartitioned($conn->getPdo(), $table);
            } catch (\Throwable) {
                // Wording only, so it must not cost the table its run: convert()
                // re-probes under its own lock, and a read that fails here fails
                // again in there, where it is caught and reported per table.
                $already = false;
            }

            $this->info($already
                ? "Checking {$table} (already partitioned)..."
                : "Partitioning {$table}...");

            try {
                $gaps = RawPartitions::convert($conn->getPdo(), $table);
            } catch (ConversionInProgressException $e) {
                $this->warn("  {$table}: skipped — {$e->getMessage()}");
                $busy[] = $table;

                continue;
            } catch (PoolerAffinityException $e) {
                // Connection-wide, not per-table: the same pooler fronts every
                // remaining table, each would run prep DDL it cannot protect, and
                // each would strand a session advisory lock on a shared backend.
                // Stop the run — but BREAK rather than return, so the tables
                // already converted above still make it into the summary.
                $this->error("  {$table}: aborted — {$e->getMessage()}");
                $this->error(
                    '  Stopping without attempting the remaining tables: behind a transaction-mode pooler '
                    .'every one of them would abort the same way.'
                );
                $failed[] = $table;

                break;
            } catch (\Throwable $e) {
                // Every table converts independently, so one that cannot must
                // cost only itself — the tables after it in TABLES are still
                // worth converting, and an escaping throw would skip them all
                // and swallow the summary. convert() unwinds its own table.
                $this->error("  {$table}: failed — {$e->getMessage()}");
                $failed[] = $table;

                continue;
            }

            if ($gaps !== []) {
                // The conversion landed — or was already landed, on the repair
                // path — and the child window did not. Printing "daily children
                // ahead" here would be the exact false line this reporting exists
                // to remove: a day with no child sends its rows to {t}_pdefault,
                // which prune can only row-DELETE, never DROP.
                $this->warn(sprintf(
                    '  %s: %s, but %d daily child partition(s) could not be created (%s). '
                    .'A running agent retries them on its next maintenance pass.',
                    $table,
                    $already ? 'already partitioned' : 'partitioned',
                    count($gaps),
                    $gaps[0],
                ));
                $childGaps = array_merge($childGaps, $gaps);

                continue;
            }

            if ($already) {
                // No hasHistoricChild probe on this path: the attached-vs-rebuilt
                // distinction describes what THIS run did to the table, and this
                // run converted nothing. What it did do is verify the window, and
                // that is what the line may claim.
                $this->line("  {$table}: already partitioned (daily child window verified).");

                continue;
            }

            // Branching on the historic child rather than asserting it: the
            // empty-table path DROPs the table and rebuilds it partitioned, so
            // nothing is attached and nothing happens "in place". Most tenants
            // have at least one empty raw table, so the unconditional line was
            // false on nearly every run.
            //
            // Its own try, and a neutral line if it cannot be answered. This is
            // the first statement after a COMMITTED conversion, so a connection
            // blip here would otherwise escape handle() entirely: no summary, no
            // exit code, and the remaining ten tables silently skipped — over a
            // table that converted successfully. Cosmetic detail must never cost
            // the run its reporting.
            try {
                $attached = RawPartitions::hasHistoricChild($conn->getPdo(), $table);
            } catch (\Throwable) {
                $attached = null;
            }

            $this->line(match ($attached) {
                true => "  {$table}: partitioned (historic partition attached in place, daily children ahead).",
                false => "  {$table}: partitioned (table was empty — rebuilt as a partitioned parent, daily children ahead).",
                default => "  {$table}: partitioned (daily children ahead).",
            });
        }

        $this->newLine();

        if ($busy !== []) {
            $this->warn(sprintf(
                '%d table(s) skipped because their conversion lock is held elsewhere. Nothing is broken: a '
                .'refused table keeps its rows, its columns and its primary key, and if the holder is another '
                .'nightowl:partition run it is doing this same work and will finish it. Two exceptions, both '
                .'from a refusal detected mid-swap: on nightowl_logs the created_at varchar→timestamp rewrite '
                .'may already have happened (irreversible, but the intended end state), and if that run\'s own '
                .'cleanup then failed it can leave a {table}_hist_ck behind — see the agent error log. Re-run '
                .'nightowl:partition once the holder is done and every table reports "already partitioned". '
                .'Exit code %d means contention alone; a run where anything also FAILED exits 1 instead.',
                count($busy),
                self::BUSY,
            ));
        }

        if ($childGaps !== []) {
            $this->warn(sprintf(
                '%d daily partition(s) could not be created. The conversions themselves succeeded — every '
                .'table reported above as partitioned IS partitioned, with its rows and its primary key '
                .'intact — but until a maintenance pass creates the missing children, those days\' rows land '
                .'in the DEFAULT partition: never lost, but prune can only row-DELETE them, never DROP. A '
                .'running agent closes the window on its next hourly pass; with no agent running, re-run '
                .'nightowl:partition, which retries the child window for tables it finds already partitioned. '
                .'Exit code %d means exactly this and nothing worse: converted, not yet complete.',
                count($childGaps),
                self::INCOMPLETE,
            ));
        }

        if ($failed !== []) {
            $this->error(sprintf(
                '%d table(s) failed to convert: %s. Each is still readable and writable, with its rows and its '
                .'primary key intact — the swap runs in one transaction, so a failure rolls back everything it '
                .'did. Three leftovers are possible outside that transaction. Two are harmless: a '
                .'{table}_id_created_at_pt index (a retry reuses it, but if you abandon the conversion it stays — '
                .'an extra (id, created_at) btree on a live raw table, written on every drained row; drop it by '
                .'hand to be rid of it), and — on nightowl_logs only — the created_at column already rewritten '
                .'from varchar to timestamp, which is irreversible but is the intended end state and affects '
                .'neither reads nor writes. The third is not: a {table}_hist_ck CHECK. The run removes it on the '
                .'way out, but a process killed outright cannot, and neither can one whose removal itself fails '
                .'(a statement timeout, a dead connection) — that case goes to the AGENT ERROR LOG, not to this '
                .'output, so check there. While it is there the table behaves normally; once its boundary passes '
                .'it rejects every write with 23514, and a running agent strips it within a minute of that '
                .'boundary. The boundary is frozen at the SECOND UTC midnight ahead, so that is at least 24 hours '
                .'away — long enough to drop the constraint by hand instead of waiting for the outage. Fix the '
                .'reported cause and re-run nightowl:partition.',
                count($failed),
                implode(', ', $failed),
            ));
        }

        $exit = self::exitCode($failed, $busy, $childGaps);

        // Derived from the code rather than sitting under the early returns that
        // used to guard it: "Done" claims a finished window, and the outcome that
        // most needed the claim withheld — converted, children missing — was the
        // one that reached this line.
        if ($exit === self::SUCCESS) {
            $this->info('Done. The running agent picks up future-partition maintenance on its next tick; no restart needed.');
        }

        return $exit;
    }

    /**
     * The run's exit code, from the three outcome lists the loop collects.
     *
     * Pure and static so the ladder can be asserted without a database: it is the
     * one part of this command a deploy pipeline actually gates on, and every
     * outcome that feeds it is otherwise reachable only through a real conversion
     * against real PostgreSQL.
     *
     * Precedence, most severe first. FAILED dominates: a run with both a failure
     * and contention needs an operator, not a retry. BUSY outranks gaps because
     * its tables are not converted AT ALL, and the re-run it asks for also closes
     * any gap this run left — reporting the lesser outcome would send the caller
     * away with the greater one unmentioned.
     *
     * INCOMPLETE is not cosmetic. childGaps was collected and warned about but
     * never consulted here, so a run whose conversions landed while their child
     * windows failed — a lock_timeout against one long-running dashboard read is
     * the documented common cause — printed "Done… no restart needed" and exited
     * SUCCESS. A pipeline gating on the status recorded the step complete and
     * moved on, while every drained row for those days went to the DEFAULT child
     * that prune can only row-DELETE. A third exit code was added so callers could
     * tell outcomes apart; this one collapsed straight back into SUCCESS.
     *
     * @param  list<string>  $failed  tables whose conversion raised
     * @param  list<string>  $busy  tables whose conversion lock is held elsewhere
     * @param  list<string>  $childGaps  daily children that could not be created
     */
    public static function exitCode(array $failed, array $busy, array $childGaps): int
    {
        if ($failed !== []) {
            return self::FAILURE;
        }

        if ($busy !== []) {
            // Contention alone is not a failure of this run's work — the peer is
            // completing it — but the tables are not converted YET, so reporting
            // success would be a lie. BUSY lets a caller tell the two apart.
            return self::BUSY;
        }

        if ($childGaps !== []) {
            return self::INCOMPLETE;
        }

        return self::SUCCESS;
    }
}
