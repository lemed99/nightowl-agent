<?php

namespace NightOwl\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use NightOwl\Support\RollupSpec;
use NightOwl\Support\RollupSpecs;
use NightOwl\Support\RollupTiers;
use NightOwl\Support\StorageV2;
use NightOwl\Support\TableCatalog;
use NightOwl\Support\V2SequenceFence;

class MigrateCommand extends Command
{
    protected $signature = 'nightowl:migrate
        {--no-backfill : Skip auto-populating newly created rollup tables (backfill manually with nightowl:backfill-rollups)}';

    protected $description = 'Create or update the NightOwl tables (idempotent — safe to run on every environment and deploy)';

    /**
     * How far a rollup base may trail the raw ceiling before a deploy treats it
     * as STOPPED rather than lagging.
     *
     * Deliberately loose. The two sides are measured on different clocks —
     * `bucket_start` is event time, `created_at` is the drain-insert clock —
     * so a buffer working through a backlog legitimately widens the gap by the
     * whole length of that backlog while losing nothing. Two hours clears any
     * plausible backlog; the failure this catches is unbounded and grows every
     * minute (a frozen table trails by a day within a day), so nothing real is
     * missed by refusing to be twitchy.
     *
     * Fine-grained detection is not this check's job — the agent's ROLLUP_STALE
     * diagnosis grades against tier peers within 15 minutes and reports. This
     * one repairs, and repair costs a raw scan, so it waits for certainty.
     */
    private const TAIL_FREEZE_TOLERANCE_SECONDS = 7200;

    /** The bespoke rollup outside RollupSpecs::all() (000063). */
    private const CONCURRENCY_ROLLUP = 'nightowl_request_concurrency_rollups';

    /**
     * Which of the tables this command ever asks about exist (name => relkind),
     * read in ONE statement per phase (TableCatalog) instead of one
     * `hasTable` / `to_regclass` per question. Reset after the migrate step,
     * which is the only thing in this run that creates tables.
     *
     * @var array<string, string>|null
     */
    private ?array $catalog = null;

    public function handle(): int
    {
        // History is tracked in the *nightowl* database (--database=nightowl),
        // not the app's primary database. The nightowl_* tables live in one
        // (BYO) database that several app environments can share, so their
        // migration history must live there too. Otherwise each environment's
        // primary database keeps its own empty history and re-runs the table
        // creation — the second deploy fails with "relation already exists".
        // Tracking history in the shared database makes this command idempotent
        // across every environment: the first run creates the tables, the rest
        // are no-ops, and a package upgrade's new migrations apply on whichever
        // environment deploys first.
        //
        // --path points explicitly at the package migrations so this works even
        // when the service provider didn't register them (NIGHTOWL_ENABLED or
        // NIGHTOWL_RUN_MIGRATIONS off) — this command is an explicit opt-in.
        $path = realpath(__DIR__.'/../../database/migrations');

        $this->baselineExistingSchema($path);

        $exit = $this->call('migrate', [
            '--database' => 'nightowl',
            '--path' => $path,
            '--realpath' => true,
            '--force' => true,
        ]);

        // The step above is the only one that creates tables: re-read presence
        // once before anything below asks.
        $this->catalog = null;

        // Unconditional, and not gated on $exit: the v2 id sequences drift
        // whenever a v1 insert lands after the fence was set, which has nothing
        // to do with whether a migration was pending this run. Re-applying it
        // here on EVERY invocation — every deploy step, nightowl:install, and
        // the daemon's boot auto-migrate whenever that fires — is what heals a
        // mixed fleet while it lasts; the 000069 migration alone runs once.
        $this->refenceV2Sequences();

        if ($exit === self::SUCCESS && ! $this->option('no-backfill')) {
            $this->backfillEmptyRollups();

            // This run WAS the reconciliation the daemon's marker asks for, so
            // clear it — otherwise an operator who runs this by hand (the whole
            // point of NIGHTOWL_AUTO_BACKFILL=false) keeps getting the
            // "did not complete" warning on every boot, forever.
            @unlink(self::backfillMarkerPath());
        }

        if ($exit === self::SUCCESS) {
            $this->warnIfSketchUnavailable();
            $this->warnIfUnpartitioned();
        }

        return $exit;
    }

    /**
     * Presence of every table this command can ask about, one statement.
     *
     * @return array<string, string>  name => relkind
     */
    private function catalog(): array
    {
        return $this->catalog ??= TableCatalog::relkinds(
            DB::connection('nightowl')->getPdo(),
            self::catalogNames(),
        );
    }

    private function tableExists(string $table): bool
    {
        return isset($this->catalog()[$table]);
    }

    /**
     * Every name any phase of this run probes: rollup bases and tiers, the
     * concurrency rollup, both raw families, and the singletons.
     *
     * @return list<string>
     */
    public static function catalogNames(): array
    {
        $names = ['nightowl_settings', self::CONCURRENCY_ROLLUP];

        foreach (RollupSpecs::all() as $spec) {
            $names[] = $spec->table;
            foreach (RollupTiers::tierTables($spec->table) as $tier) {
                $names[] = $tier;
            }
        }

        foreach (StorageV2::TABLES as $v1 => $v2) {
            $names[] = $v1;
            $names[] = $v2;
        }

        return array_values(array_unique($names));
    }

    /**
     * "Rollup reconciliation is owed."
     *
     * Written by the daemon when it defers the backfill half of a boot upgrade
     * (AgentCommand::spawnBackgroundBackfill), removed by whoever actually
     * performs it — the detached subshell, or this command on any full run.
     * Lives here because both commands need it and this one is already
     * AgentCommand's static dependency.
     */
    public static function backfillMarkerPath(): string
    {
        return storage_path('nightowl/backfill-pending');
    }

    /**
     * Re-apply the storage-v2 id-sequence fence (V2SequenceFence): keep every
     * v2 sequence above its v1 twin's id range so the API's v1+v2 union pages
     * can never see two rows sharing a (created_at, id) cursor — a COPY batch
     * stamps one created_at across its rows, so a colliding id makes "Next"
     * skip or repeat rows.
     *
     * Migration 000067 fences at v2-table creation only; a v1 insert after that
     * instant (mixed fleet, or a NIGHTOWL_STORAGE_V2=false excursion) overtakes
     * it. Re-running it here is the self-healing half.
     *
     * Advisory work — a failure warns and continues, exactly like the
     * unpartitioned and sketch checks. Nothing here may fail a schema sync, and
     * the daemon's boot-migrate rides this same path.
     */
    private function refenceV2Sequences(): void
    {
        try {
            $result = V2SequenceFence::apply(DB::connection('nightowl')->getPdo());
        } catch (\Throwable $e) {
            $this->warn("Storage-v2 id sequence fence skipped ({$e->getMessage()}).");

            return;
        }

        foreach ($result['failures'] as $table => $message) {
            $this->warn("  {$table}: id sequence fence skipped ({$message})");
        }

        if ($result['refenced'] !== []) {
            $this->line(sprintf(
                'Re-fenced %d storage-v2 id sequence(s) above the v1 id range (%s).',
                count($result['refenced']),
                implode(', ', array_keys($result['refenced'])),
            ));
        }

        // Ids already handed out cannot be renumbered; say so once, by name.
        if ($result['overlapping'] !== []) {
            $this->warn(sprintf(
                'Storage-v2 and v1 id ranges already overlap on %s: rows written before this fence took '
                .'ids the other family also used, so wide list views (requests, exception occurrences) may '
                .'skip or repeat a row while both families still hold them. Those rows cannot be renumbered; '
                .'the fence stops it recurring and the overlap clears as they age out at retention.',
                implode(', ', $result['overlapping']),
            ));
        }
    }

    /**
     * Populated tables never auto-partition (the conversion's CONCURRENTLY
     * index build must not run inside a deploy pipeline that may kill it) — so
     * upgraded tenants stay on the row-DELETE prune path, which reuses disk
     * but never returns it, until an operator runs nightowl:partition. Say so
     * loudly; the Data Management panel carries the same flag in the UI.
     */
    private function warnIfUnpartitioned(): void
    {
        try {
            $tables = \NightOwl\Support\RawPartitions::unpartitionedPopulated(
                DB::connection('nightowl')->getPdo()
            );
        } catch (\Throwable) {
            return;
        }

        if ($tables !== []) {
            $this->warn(sprintf(
                '%d raw telemetry table(s) are not partitioned (%s…): prune row-deletes them, so disk is '
                .'reused but never returned. Run `php artisan nightowl:partition` once (operator action; '
                .'see docs) to switch prune to instant partition drops.',
                count($tables),
                $tables[0],
            ));
        }
    }

    /**
     * Surface the CREATE-FUNCTION-denied condition (migration 000057 skips the
     * DDSketch columns on such databases): percentiles silently stay on the v1
     * √2 histogram (~2.8% worst-case vs ≤1%) — correct, but the operator
     * should know why, and that granting CREATE + re-running migrate fixes it.
     */
    private function warnIfSketchUnavailable(): void
    {
        $conn = DB::connection('nightowl');

        if (! $this->tableExists('nightowl_query_rollups')) {
            return;
        }

        $fn = $conn->selectOne("SELECT to_regproc('nightowl_ddsketch_merge') IS NOT NULL AS present");
        if ($fn !== null && ! $fn->present) {
            $this->warn(
                'DDSketch percentiles unavailable: this database denied CREATE FUNCTION, so the '
                .'sketch columns were skipped and percentiles use the v1 histogram (~2.8% worst-case '
                .'accuracy instead of ≤1%). Grant CREATE on the schema and re-run nightowl:migrate to enable.'
            );
        }
    }

    /**
     * Populate any rollup table that exists but is empty or incomplete from
     * existing raw telemetry.
     *
     * This closes a trap the API's read path sets: it switches a section to its
     * rollup table the moment that table EXISTS (a per-tenant `to_regclass`
     * probe), falling back to raw only when the table is *absent*. So a rollup
     * table that's been created but not yet populated makes wide-range views
     * read zero — strictly worse than the raw fallback it replaced. That's
     * exactly what a bare `nightowl:migrate` produced: it created the rollup
     * tables and left them empty until someone remembered to run
     * `nightowl:backfill-rollups`. Doing it here makes migrate self-healing.
     *
     * Scoped to tables with something to heal so a routine re-deploy doesn't
     * re-scan the raw history behind already-maintained rollups: a populated,
     * current table is left to the live drain, and an empty table whose raw
     * source is ALSO empty is complete — there is nothing to roll up, and
     * treating it as a hole ran a pointless backfill sub-command and printed
     * the "restart the daemon" warning on every deploy of every app that
     * simply has no cache events / notifications / outgoing requests yet.
     * Backfill is idempotent (replace-per-bucket) and throttled, so this is
     * safe to run on every deploy.
     *
     * Beyond emptiness, migrate detects INCOMPLETE rollups, so no state this
     * release can produce needs a manual nightowl:backfill-rollups:
     *
     * - A base minute table whose earliest bucket is YOUNGER than the raw
     *   history (rollups enabled after raw already existed, or an aborted
     *   first backfill), or whose latest bucket has STOPPED while raw kept
     *   arriving (baseIsIncompleteFrom): full chain for that type.
     * - A tier whose call_count sum falls SHORT of its chain source's. The
     *   drain writes minute and tier rows in one transaction, so a complete
     *   tier always covers its source — any shortfall is a gap, wherever it
     *   sits (the classic one: migrate created the tiers and backfilled, but
     *   the daemon kept writing minute-only until its restart, leaving a
     *   mid-history hole). The tiers-only pass replaces per-window across the
     *   whole span, so it heals middle holes too. Retention asymmetry never
     *   false-triggers: tiers keep MORE history than their source (366d/1100d
     *   vs 90d), so pruning only ever pushes the tier sum ABOVE the source's.
     *
     * Every measurement above comes back in ONE statement (completenessSql):
     * bounds, raw floor and ceiling, and tier sums for all 15 bases at once.
     * It used to be ~200 statements — six per base plus a `hasTable` per
     * table — and on a tenant whose Postgres is a ~100ms hop away that was
     * the entire 72-second deploy step, on an idle dev box as much as on
     * prod. A single statement also reads under a single snapshot, which
     * retires the read-ordering arguments the per-query version needed
     * (rollup ceiling before raw ceiling, source sum before tier sum): a
     * drain batch can no longer commit BETWEEN two of these reads.
     *
     * Empty tiers never serve reads (the resolver degrades to the minute
     * tier), but on a high-volume tenant that fallback is exactly the
     * wide-range scan the tiers exist to prevent — measured at an 8M req/day
     * profile, a 30d chart on the minute tier runs past the statement
     * timeout. Hence detection here rather than trusting the operator to
     * remember a second command.
     */
    private function backfillEmptyRollups(): void
    {
        $conn = DB::connection('nightowl');

        // Chain order matters: hourly is judged against the minute table,
        // daily against hourly — mirroring how backfillTiers aggregates.
        $bases = [];
        foreach (RollupSpecs::all() as $spec) {
            if (! $this->tableExists($spec->table)) {
                continue;
            }
            $bases[$spec->table] = [
                'source' => $spec->source,
                'tiers' => array_values(array_filter(
                    RollupTiers::tierTables($spec->table),
                    fn (string $tier): bool => $this->tableExists($tier),
                )),
            ];
        }

        // The bespoke concurrency rollup sits outside RollupSpecs::all() and
        // would otherwise be created by 000063 and never populated — the API's
        // coverage gate then keeps peak reads on the clamped raw path for up
        // to raw retention after upgrade. Same incompleteness rule as the spec
        // bases; no tiers of its own.
        if ($this->tableExists(self::CONCURRENCY_ROLLUP)) {
            $bases[self::CONCURRENCY_ROLLUP] = ['source' => 'nightowl_requests', 'tiers' => []];
        }

        $states = $this->probeCompleteness($conn, $bases);

        $basesNeedingFull = [];
        $basesOk = [];
        $incompleteTiers = [];
        foreach ($bases as $table => $base) {
            $state = $states[$table];

            if (self::baseIsIncompleteFrom($state['lo'], $state['hi'], $state['raw_min'], $state['raw_max'])) {
                $basesNeedingFull[] = $table;
            } elseif ($table !== self::CONCURRENCY_ROLLUP) {
                $basesOk[] = $table;
            }

            $sourceSum = $state['sums'][$table];
            foreach ($base['tiers'] as $tierTable) {
                if (self::tierIsIncompleteFrom($sourceSum, $state['sums'][$tierTable])) {
                    $incompleteTiers[] = $tierTable;
                }
                $sourceSum = $state['sums'][$tierTable];
            }
        }

        $plan = self::backfillPlan($basesNeedingFull, $basesOk, $incompleteTiers);

        if ($plan['full'] !== []) {
            $this->newLine();
            $this->info(sprintf(
                'Populating %d empty or incomplete rollup table(s) from raw telemetry so wide-range views read correctly...',
                count($plan['full']),
            ));

            foreach ($plan['full'] as $table) {
                $this->call('nightowl:backfill-rollups', ['--type' => $table]);
            }
        }

        if ($plan['tiers_only'] !== []) {
            $this->newLine();
            $this->info(sprintf(
                'Rebuilding empty or incomplete hourly/daily tiers for %d rollup type(s) from their minute rollups...',
                count($plan['tiers_only']),
            ));

            foreach ($plan['tiers_only'] as $table) {
                $this->call('nightowl:backfill-rollups', ['--type' => $table, '--tiers-only' => true]);
            }
        }

        // Holes the live drain reported but could not fill itself. Independent of
        // the completeness checks above: a drain that skipped one batch's rollup
        // leaves a hole too narrow for a MIN or a SUM comparison to notice.
        $repaired = $this->repairMarkedRollups($conn, $plan['full']);

        if ($plan['full'] === [] && $plan['tiers_only'] === [] && $repaired === []) {
            return;
        }

        $this->newLine();
        $this->warn(
            'Rollup tables populated. If the agent daemon was already running before this migrate, '
            .'restart it (nightowl:agent) so it starts writing rollups for new telemetry — it caches '
            .'which rollup tables exist at boot.'
        );
    }

    /**
     * Fill the rollup holes the drain recorded in `rollup_repair_from`.
     *
     * The drain writes that key when it cannot take a rollup table's shared
     * advisory lock: it commits the raw rows and skips the rollup, per table with
     * the earliest bucket it skipped (RecordWriter::recordRollupRepairDebt). One
     * skipped batch is a minutes-wide hole in the middle of an otherwise complete
     * table — MIN(bucket_start) still predates raw and the tier sums still match,
     * so neither baseIsIncomplete() nor tierIsIncomplete() can see it. This is the
     * only thing that closes it.
     *
     * `--since` the recorded floor rather than the whole history: the floor is the
     * earliest bucket that could be affected, and replace-per-bucket makes
     * re-deriving everything after it correct regardless of how many holes there
     * are. Skips tables the completeness pass just rebuilt in full — that pass
     * already covered them, and it ran first.
     *
     * The key is cleared only after every table it names succeeds. A partial run
     * keeps the debt, so the next deploy retries the rest instead of dropping it.
     *
     * @param  array<int, string>  $alreadyFull
     * @return array<int, string>  tables repaired here
     */
    private function repairMarkedRollups($conn, array $alreadyFull): array
    {
        try {
            $raw = $conn->table('nightowl_settings')->where('key', 'rollup_repair_from')->value('value');
        } catch (\Throwable $e) {
            $this->warn('Could not read the rollup repair marker: '.$e->getMessage());

            return [];
        }

        if ($raw === null || $raw === '') {
            return [];
        }

        $debt = json_decode((string) $raw, true, 8);
        if (! is_array($debt) || $debt === []) {
            // Not our shape — say so rather than silently discarding a record of
            // missing data, and leave the row for a human to look at.
            $this->warn('The rollup repair marker is unreadable; leaving it in place. Value: '.substr((string) $raw, 0, 200));

            return [];
        }

        $known = array_map(fn (RollupSpec $spec): string => $spec->table, RollupSpecs::all());

        $repaired = [];
        $failed = [];
        foreach ($debt as $table => $floor) {
            if (! is_string($table) || ! in_array($table, $known, true) || ! is_string($floor)) {
                $failed[$table] = $floor; // unrecognised — keep it, don't act on it
                continue;
            }
            if (in_array($table, $alreadyFull, true)) {
                continue; // the full pass above already re-derived this table
            }

            // A floor inside the backfill's own safety margin cannot be repaired
            // yet: backfill-rollups caps --until at now minus the margin, so it
            // would report "nothing to backfill", succeed, and let the key be
            // cleared with the hole still open. Keep the debt for the next run
            // instead. Normal timing never lands here — the repair runs a deploy
            // or a reboot after the skip — so this is the belt for a migrate that
            // follows a contended drain within ten minutes.
            if (strtotime($floor.' UTC') > time() - BackfillRollupsCommand::SAFETY_MARGIN_SECONDS) {
                $this->line(sprintf(
                    '  %s: rollup repair from %s UTC is too recent to backfill; keeping the marker for the next run.',
                    $table,
                    $floor,
                ));
                $failed[$table] = $floor;

                continue;
            }

            $this->newLine();
            $this->info(sprintf(
                'Repairing %s from %s UTC — the live drain skipped its rollup while the table was locked.',
                $table,
                $floor,
            ));

            if ($this->call('nightowl:backfill-rollups', ['--type' => $table, '--since' => $floor]) === self::SUCCESS) {
                $repaired[] = $table;
            } else {
                $failed[$table] = $floor;
            }
        }

        try {
            if ($failed === []) {
                $conn->table('nightowl_settings')->where('key', 'rollup_repair_from')->delete();
            } else {
                $conn->table('nightowl_settings')
                    ->where('key', 'rollup_repair_from')
                    ->update(['value' => json_encode($failed), 'updated_at' => now()]);
            }
        } catch (\Throwable $e) {
            $this->warn('Could not update the rollup repair marker: '.$e->getMessage());
        }

        return $repaired;
    }

    /**
     * The concurrency-rollup twin of the spec-base check (bespoke, no spec).
     * Kept as its own seam: RollupTailFreezeTest reaches it by reflection to
     * pin the predicate's detection AND false-positive behaviour.
     */
    private function concurrencyIsIncomplete($conn): bool
    {
        $state = $this->probeCompleteness($conn, [
            self::CONCURRENCY_ROLLUP => ['source' => 'nightowl_requests', 'tiers' => []],
        ])[self::CONCURRENCY_ROLLUP];

        return self::baseIsIncompleteFrom($state['lo'], $state['hi'], $state['raw_min'], $state['raw_max']);
    }

    /**
     * Run completenessSql for the given bases and reshape its single row.
     *
     * @param  array<string, array{source: string, tiers: list<string>}>  $bases
     * @return array<string, array{lo: ?string, hi: ?string, raw_min: ?string, raw_max: ?string, sums: array<string, ?string>}>
     */
    private function probeCompleteness($conn, array $bases): array
    {
        if ($bases === []) {
            return [];
        }

        $rawPresent = array_keys(array_filter(
            $this->catalog(),
            static fn (string $kind, string $name): bool => isset(StorageV2::TABLES[$name])
                || in_array($name, StorageV2::TABLES, true),
            ARRAY_FILTER_USE_BOTH,
        ));

        $row = (array) $conn->selectOne(self::completenessSql($bases, $rawPresent));

        $states = [];
        $i = 0;
        foreach ($bases as $table => $base) {
            $key = 'b'.$i++;
            $sums = [$table => $row["{$key}_sum"]];
            foreach ($base['tiers'] as $j => $tierTable) {
                $sums[$tierTable] = $row["{$key}_t{$j}_sum"];
            }
            $states[$table] = [
                'lo' => $row["{$key}_lo"],
                'hi' => $row["{$key}_hi"],
                'raw_min' => $row["{$key}_raw_min"],
                'raw_max' => $row["{$key}_raw_max"],
                'sums' => $sums,
            ];
        }

        return $states;
    }

    /**
     * One statement, one row, every number the completeness pass needs.
     *
     * Per base `b{i}`: `_lo`/`_hi` (rollup bucket bounds), `_raw_min`/`_raw_max`
     * (raw floor and ceiling across BOTH storage families — pre-fence rows
     * live in v1, post-fence in the v2 twin — NULL when neither table exists
     * or both are empty), `_sum` (call_count over the base) and `_t{j}_sum`
     * per listed tier. The concurrency base has no call_count; its `_sum`
     * is emitted as NULL and never read.
     *
     * Pure so the shape is unit-testable: every name comes from a constant
     * (RollupSpecs / RollupTiers / StorageV2), never from input.
     *
     * @param  array<string, array{source: string, tiers: list<string>}>  $bases
     * @param  list<string>  $rawPresent  raw tables (either family) that exist
     */
    public static function completenessSql(array $bases, array $rawPresent): string
    {
        $cols = [];
        $i = 0;
        foreach ($bases as $table => $base) {
            $key = 'b'.$i++;

            $cols[] = "(SELECT MIN(bucket_start)::text FROM {$table}) AS {$key}_lo";
            $cols[] = "(SELECT MAX(bucket_start)::text FROM {$table}) AS {$key}_hi";

            $legs = array_values(array_filter(
                [$base['source'], StorageV2::v2Name($base['source'])],
                static fn (string $t): bool => in_array($t, $rawPresent, true),
            ));
            foreach (['MIN' => 'raw_min', 'MAX' => 'raw_max'] as $agg => $suffix) {
                if ($legs === []) {
                    $cols[] = "NULL::text AS {$key}_{$suffix}";

                    continue;
                }
                $union = implode(' UNION ALL ', array_map(
                    static fn (string $t): string => "SELECT {$agg}(created_at) AS m FROM {$t}",
                    $legs,
                ));
                $cols[] = "(SELECT {$agg}(m)::text FROM ({$union}) {$key}_{$suffix}_legs) AS {$key}_{$suffix}";
            }

            $cols[] = $table === self::CONCURRENCY_ROLLUP
                ? "NULL::text AS {$key}_sum"
                : "(SELECT SUM(call_count)::text FROM {$table}) AS {$key}_sum";
            foreach ($base['tiers'] as $j => $tierTable) {
                $cols[] = "(SELECT SUM(call_count)::text FROM {$tierTable}) AS {$key}_t{$j}_sum";
            }
        }

        return 'SELECT '.implode(', ', $cols);
    }

    /**
     * A minute rollup base is incomplete when raw history exists that it does
     * not cover: it is empty, it is missing history the raw table still holds
     * (HEAD), or it has stopped advancing while raw kept arriving (TAIL).
     *
     * No raw history at all is never incomplete — an empty base over an empty
     * source has nothing to roll up, and this runs on every deploy of every
     * tenant, so calling it a hole meant every app without, say, notifications
     * ran a no-op backfill and got told to restart its daemon each time.
     *
     * Head and tail are separate failures and neither implies the other. The
     * head arm has been here since the tiers shipped: a base whose earliest
     * bucket is younger than the earliest raw row started late, and normal
     * states never trigger it — a fresh install starts both at the same
     * instant, and raw pruning (14d) moves the raw floor ABOVE the rollup's
     * (90d) floor.
     *
     * The tail arm is what the 2026-07-31 Yomoney freeze needed and nothing
     * had: prune's v1-EOL dropped `nightowl_requests` under a running daemon,
     * the concurrency recompute named a relation that no longer existed, and
     * 42P01 stopped that one table dead while every other rollup stayed
     * current. Nothing noticed for thirteen hours. The head arm could not see
     * it — the floor was untouched, only the ceiling stopped — and the tier
     * `call_count` comparison could not either, because when a minute base
     * freezes its hourly child freezes with it and the two sums still agree.
     * Both sides stuck at the same instant is indistinguishable from both
     * sides being correct, unless something looks at raw.
     *
     * Repair is the same full raw→minute→hour→day chain either arm already
     * triggers, so a tail hole heals on the next deploy instead of persisting
     * for as long as raw retention allows it to be fixed at all.
     *
     * The tail arm assumes every raw row reaches a bucket — true today because
     * every RollupSpec groups on COALESCE(...), so rows with no user, server or
     * fingerprint land in the '' group rather than vanishing. A spec that
     * genuinely FILTERED rows would break that: its source ceiling would run
     * ahead of its rollup ceiling with nothing wrong, and every deploy would
     * rescan raw. Such a spec needs its own predicate, not this one.
     *
     * All four inputs are read in ONE statement (completenessSql), i.e. under
     * one snapshot, so no drain batch can land between them.
     *
     * @param  ?string  $lo  earliest rollup bucket (NULL: empty base)
     * @param  ?string  $hi  latest rollup bucket
     * @param  ?string  $rawMin  earliest raw created_at across both families (NULL: no raw)
     * @param  ?string  $rawMax  latest raw created_at across both families
     */
    public static function baseIsIncompleteFrom(?string $lo, ?string $hi, ?string $rawMin, ?string $rawMax): bool
    {
        if ($rawMin === null) {
            return false; // no raw history to be missing
        }

        if ($lo === null) {
            return true; // empty, with raw waiting to be rolled up
        }

        if (\Carbon\Carbon::parse($rawMin)->startOfMinute()->lessThan(\Carbon\Carbon::parse($lo))) {
            return true; // head: raw predates the earliest bucket
        }

        if ($rawMax === null || $hi === null) {
            return false;
        }

        return \Carbon\Carbon::parse($hi)->addSeconds(self::TAIL_FREEZE_TOLERANCE_SECONDS)
            ->lessThan(\Carbon\Carbon::parse($rawMax));
    }

    /**
     * A complete tier's call_count sum always covers its chain source's — the
     * drain writes both in one transaction and pruning only removes from the
     * shorter-retention source. Both sums come from the same snapshot.
     */
    public static function tierIsIncompleteFrom(?string $sourceSum, ?string $tierSum): bool
    {
        return (float) ($tierSum ?? 0) < (float) ($sourceSum ?? 0);
    }

    /**
     * Which rollup types need which backfill, from the observed table states.
     * Pure so the decision is unit-testable without a database.
     *
     * - full: bases that are empty or incomplete — the raw→minute→tier chain.
     * - tiers_only: sound bases with at least one incomplete tier sibling —
     *   re-aggregate the tiers from the minute rows, skip raw.
     *
     * A base in `full` never also appears in `tiers_only` (its full chain
     * already rebuilds the tiers).
     *
     * @param  list<string>  $basesNeedingFull  existing base tables, empty or missing raw history
     * @param  list<string>  $basesOk  existing base tables whose coverage is sound
     * @param  list<string>  $incompleteTiers  existing tier tables whose sum falls short of their source
     * @return array{full: list<string>, tiers_only: list<string>}
     */
    public static function backfillPlan(array $basesNeedingFull, array $basesOk, array $incompleteTiers): array
    {
        $tiersOnly = [];
        foreach ($basesOk as $base) {
            foreach (RollupTiers::tierTables($base) as $tierTable) {
                if (in_array($tierTable, $incompleteTiers, true)) {
                    $tiersOnly[] = $base;
                    break;
                }
            }
        }

        return ['full' => array_values($basesNeedingFull), 'tiers_only' => $tiersOnly];
    }

    /**
     * Reconcile the nightowl migration history so migrate doesn't recreate
     * tables that already exist.
     *
     * The history that tracks NightOwl's migrations has lived in different places
     * across versions: in the nightowl database (v1.0.0–1.0.10, via
     * `--database=nightowl`), then in the host app's primary database
     * (v1.0.11–1.0.12), and now back in the nightowl database. So a given install
     * may have a complete, partial/stale, or empty nightowl-side history, with
     * the rest recorded in the primary database.
     *
     * We record into the nightowl history every migration that's already applied
     * according to EITHER history but isn't tracked here yet. That covers all the
     * upgrade paths without recreating existing tables, while leaving genuinely
     * unapplied migrations for migrate to run. A fresh database (no tables) needs
     * nothing. If the tables exist but neither history knows anything, we adopt
     * the schema as-is and say so — a genuinely-missing migration can't be
     * detected in that case.
     */
    private function baselineExistingSchema(string $migrationsPath): void
    {
        $repository = app('migrator')->getRepository();
        $repository->setSource('nightowl');

        if (! $repository->repositoryExists()) {
            $repository->createRepository();
        }

        $all = $this->packageMigrationNames($migrationsPath);
        $nightowlHistory = $repository->getRan();
        $primaryHistory = self::primaryHistory();
        // Either family counts: a post-EOL tenant (v1 retired by prune) must
        // still baseline-adopt, or it would misread its own DB as fresh.
        $tableExists = $this->tableExists('nightowl_requests')
            || $this->tableExists('nightowl_requests_v2');

        $toRecord = self::migrationsToRecord($all, $nightowlHistory, $primaryHistory, $tableExists);

        if ($toRecord === []) {
            return;
        }

        // Tables exist but neither history knows anything about them → we're
        // adopting the schema blind. Say so, since a genuinely-missing migration
        // would be marked applied without being run.
        if ($tableExists && self::appliedSet($all, $nightowlHistory, $primaryHistory) === []) {
            $this->warn(
                'No prior NightOwl migration history found to adopt from. Assuming the existing '
                .'schema is current — if a later migration turns out to be missing, run your '
                .'previous version\'s `php artisan migrate` first, or drop the nightowl_* tables and re-run.'
            );
        }

        $batch = $repository->getNextBatchNumber();
        foreach ($toRecord as $migration) {
            $repository->log($migration, $batch);
        }

        $this->line(sprintf(
            'Adopted %d migration(s) already applied but not yet tracked in the nightowl database (baseline).',
            count($toRecord),
        ));
    }

    /** @return list<string> */
    private function packageMigrationNames(string $migrationsPath): array
    {
        return collect(glob($migrationsPath.'/*.php'))
            ->map(fn (string $file) => basename($file, '.php'))
            ->sort()
            ->values()
            ->all();
    }

    /**
     * Migration names recorded in the host app's primary migration history.
     *
     * That's where legacy installs (and the host app's `php artisan migrate`)
     * logged NightOwl's migrations. Best-effort: returns [] if the primary
     * database is unreachable or has no migrations table. Callers treat [] as
     * "nothing to learn from".
     *
     * @return list<string>
     */
    public static function primaryHistory(): array
    {
        try {
            $primary = app('db')->connection();

            if (! $primary->getSchemaBuilder()->hasTable('migrations')) {
                return [];
            }

            return $primary->table('migrations')->pluck('migration')->all();
        } catch (\Throwable) {
            return [];
        }
    }

    /**
     * Migrations to record as applied in the nightowl history without running them.
     *
     * Reconciles the nightowl-connection history with the true applied set
     * (nightowl history ∪ the host's primary history): any migration already
     * applied somewhere but not yet tracked in the nightowl database is recorded,
     * so migrate doesn't try to recreate it. Genuinely-unapplied migrations are
     * left out for migrate to run. A fresh database (no tables) needs nothing.
     * When the tables exist but neither history records anything, the whole
     * schema is adopted (the caller warns) rather than recreated.
     *
     * @param  list<string>  $allMigrations
     * @param  list<string>  $nightowlHistory
     * @param  list<string>  $primaryHistory
     * @return list<string>
     */
    public static function migrationsToRecord(array $allMigrations, array $nightowlHistory, array $primaryHistory, bool $canonicalTableExists): array
    {
        if (! $canonicalTableExists) {
            return [];
        }

        $applied = self::appliedSet($allMigrations, $nightowlHistory, $primaryHistory);

        if ($applied === []) {
            // Tables present but nothing recorded anywhere — adopt the lot.
            $applied = $allMigrations;
        }

        return array_values(array_diff($applied, $nightowlHistory));
    }

    /**
     * Of the package's migrations, which does the primary history say are applied?
     *
     * Intersection (ignoring any non-NightOwl rows in the primary history),
     * preserving the package's migration order.
     *
     * @param  list<string>  $allMigrations
     * @param  list<string>  $primaryRecorded
     * @return list<string>
     */
    public static function applicableFromPrimary(array $allMigrations, array $primaryRecorded): array
    {
        return array_values(array_intersect($allMigrations, $primaryRecorded));
    }

    /**
     * The full set of package migrations known to be applied, from either history.
     *
     * A migration counts as applied if it's recorded in the nightowl-connection
     * history (the DB-history model) OR in the host app's primary history (legacy
     * ride-along / old install). Both are intersected with the package's own
     * migrations so unrelated app migrations and a shared single-database
     * `migrations` table don't leak in. Used by the agent's drift check so a
     * legacy install that's fallen behind is still detected.
     *
     * @param  list<string>  $allMigrations
     * @param  list<string>  $nightowlHistory
     * @param  list<string>  $primaryHistory
     * @return list<string>
     */
    public static function appliedSet(array $allMigrations, array $nightowlHistory, array $primaryHistory): array
    {
        return array_values(array_unique(array_merge(
            array_values(array_intersect($allMigrations, $nightowlHistory)),
            self::applicableFromPrimary($allMigrations, $primaryHistory),
        )));
    }

    /**
     * Package migrations not yet recorded in the given history.
     *
     * @param  list<string>  $allMigrations
     * @param  list<string>  $recordedMigrations
     * @return list<string>
     */
    public static function pendingMigrations(array $allMigrations, array $recordedMigrations): array
    {
        return array_values(array_diff($allMigrations, $recordedMigrations));
    }

    /**
     * Is the recorded history live but missing newer migrations?
     *
     * This is the drift the agent warns about at startup. An *empty* history is
     * deliberately NOT drift: it means the schema is present but simply not
     * tracked in this database yet (a pre-DB-history install), which
     * nightowl:migrate adopts as a baseline rather than something that breaks
     * writes. Drift is when some migrations are recorded but the latest ones
     * haven't been applied.
     *
     * @param  list<string>  $allMigrations
     * @param  list<string>  $recordedMigrations
     */
    public static function isBehind(array $allMigrations, array $recordedMigrations): bool
    {
        return $recordedMigrations !== [] && self::pendingMigrations($allMigrations, $recordedMigrations) !== [];
    }
}
