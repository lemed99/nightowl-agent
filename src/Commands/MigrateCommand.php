<?php

namespace NightOwl\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use NightOwl\Support\RollupSpecs;
use NightOwl\Support\RollupTiers;

class MigrateCommand extends Command
{
    protected $signature = 'nightowl:migrate
        {--no-backfill : Skip auto-populating newly created rollup tables (backfill manually with nightowl:backfill-rollups)}';

    protected $description = 'Create or update the NightOwl tables (idempotent — safe to run on every environment and deploy)';

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

        if ($exit === self::SUCCESS && ! $this->option('no-backfill')) {
            $this->backfillEmptyRollups();
        }

        if ($exit === self::SUCCESS) {
            $this->warnIfSketchUnavailable();
            $this->warnIfUnpartitioned();
        }

        return $exit;
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
        $schema = Schema::connection('nightowl');

        if (! $schema->hasTable('nightowl_query_rollups')) {
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
     * Populate any rollup table that exists but is empty from existing raw
     * telemetry.
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
     * Scoped to *empty* tables so a routine re-deploy doesn't re-scan the raw
     * history behind already-maintained rollups: a populated table is left to
     * the live drain. An empty table on a no-traffic install is a cheap no-op
     * (backfill finds no source rows). Backfill is idempotent (replace-per-
     * bucket) and throttled, so this is safe to run on every deploy.
     *
     * Beyond emptiness, migrate detects INCOMPLETE rollups, so no state this
     * release can produce needs a manual nightowl:backfill-rollups:
     *
     * - A base minute table whose earliest bucket is YOUNGER than the raw
     *   history (rollups enabled after raw already existed, or an aborted
     *   first backfill): full chain for that type. Cheap — two index-backed
     *   MINs per type.
     * - A tier whose call_count sum falls SHORT of its chain source's. The
     *   drain writes minute and tier rows in one transaction, so a complete
     *   tier always covers its source — any shortfall is a gap, wherever it
     *   sits (the classic one: migrate created the tiers and backfilled, but
     *   the daemon kept writing minute-only until its restart, leaving a
     *   mid-history hole). The tiers-only pass replaces per-window across the
     *   whole span, so it heals middle holes too. Retention asymmetry never
     *   false-triggers: tiers keep MORE history than their source (366d/1100d
     *   vs 90d), so pruning only ever pushes the tier sum ABOVE the source's.
     *   Costs one SUM per rollup table per deploy — ms for typical tenants,
     *   ~1s per big minute table at an 8M req/day profile.
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
        $schema = Schema::connection('nightowl');
        $conn = DB::connection('nightowl');

        $basesNeedingFull = [];
        $basesOk = [];
        $incompleteTiers = [];
        foreach (RollupSpecs::all() as $spec) {
            if (! $schema->hasTable($spec->table)) {
                continue;
            }

            if ($this->baseIsIncomplete($conn, $spec)) {
                $basesNeedingFull[] = $spec->table;
            } else {
                $basesOk[] = $spec->table;
            }

            // Chain order matters: hourly is judged against the minute table,
            // daily against hourly — mirroring how backfillTiers aggregates.
            $sourceTable = $spec->table;
            foreach (RollupTiers::tierTables($spec->table) as $tierTable) {
                if (! $schema->hasTable($tierTable)) {
                    continue;
                }
                if ($this->tierIsIncomplete($conn, $sourceTable, $tierTable)) {
                    $incompleteTiers[] = $tierTable;
                }
                $sourceTable = $tierTable;
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

        if ($plan['full'] === [] && $plan['tiers_only'] === []) {
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
     * Empty, or missing history the raw table still holds (its earliest bucket
     * is younger than the earliest raw row's minute). Normal states never
     * trigger: a fresh install starts both at the same instant, and raw
     * pruning (14d) moves the raw floor ABOVE the rollup's (90d) floor.
     */
    private function baseIsIncomplete($conn, \NightOwl\Support\RollupSpec $spec): bool
    {
        $rollupMin = $conn->table($spec->table)->min('bucket_start');
        if ($rollupMin === null) {
            return true; // empty
        }

        $rawMin = $conn->table($spec->source)->min('created_at');
        if ($rawMin === null) {
            return false; // no raw history to be missing
        }

        return \Carbon\Carbon::parse($rawMin)->startOfMinute()
            ->lessThan(\Carbon\Carbon::parse($rollupMin));
    }

    /**
     * A complete tier's call_count sum always covers its chain source's — the
     * drain writes both in one transaction and pruning only removes from the
     * shorter-retention source. Read the SOURCE first: a drain batch committing
     * between the two reads then only inflates the tier side, which cannot
     * false-trigger.
     */
    private function tierIsIncomplete($conn, string $sourceTable, string $tierTable): bool
    {
        $sourceSum = (float) ($conn->table($sourceTable)->sum('call_count') ?? 0);
        $tierSum = (float) ($conn->table($tierTable)->sum('call_count') ?? 0);

        return $tierSum < $sourceSum;
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
        $tableExists = Schema::connection('nightowl')->hasTable('nightowl_requests');

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
