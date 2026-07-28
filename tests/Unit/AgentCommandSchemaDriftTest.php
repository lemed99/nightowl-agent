<?php

namespace NightOwl\Tests\Unit;

use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Database\DatabaseServiceProvider;
use Illuminate\Database\MigrationServiceProvider;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Filesystem\Filesystem;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use Illuminate\Support\Facades\Schema;
use NightOwl\Commands\AgentCommand;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

/**
 * The daemon's startup drift check has to recognise a post-EOL tenant as
 * initialised. `nightowl:prune` DROPs an empty v1 raw parent once the v2 fence
 * is older than retention (PruneCommand::retireEmptyV1Tables), so
 * `nightowl_requests` is gone while the schema is fully current — and a probe
 * that only knows the v1 name answers "cannot tell" (null) there. That is the
 * one return value syncOrWarnSchema() answers with total silence: no boot
 * auto-migrate AND no warning line. A customer upgrading to an agent that
 * ships a new rollup migration would never get it applied, and nothing in the
 * log would point at the un-run migration behind the empty dashboard panel.
 *
 * SQLite in-memory rather than the PostgreSQL harness on purpose: the probe is
 * Schema::hasTable, which is driver-agnostic, and staging the v2-only case on
 * the shared PG test database would mean DROPping nightowl_requests out from
 * under every other suite.
 */
final class AgentCommandSchemaDriftTest extends TestCase
{
    private ?Application $app = null;

    private ?Container $previousContainer = null;

    protected function setUp(): void
    {
        // Booting an Application makes it the global container, and whatever
        // resolves app()/config() next inherits it. MigrationRunner already
        // documents that cross-class leak from the other direction, so put the
        // previous instance back rather than adding to it.
        $this->previousContainer = Container::getInstance();
    }

    protected function tearDown(): void
    {
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);
        Container::setInstance($this->previousContainer);
        $this->app = null;
    }

    public function test_v2_only_schema_reports_drift_instead_of_cannot_tell(): void
    {
        $this->bootWithRawTables(['nightowl_requests_v2']);
        $this->recordNightowlHistory($this->appliedThroughSecondNewest());

        $pending = $this->pendingNightowlMigrations();

        $this->assertNotNull(
            $pending,
            'A post-EOL (v2-only) schema is initialised — null there silences both the auto-migrate and the warning.',
        );
        $this->assertSame([$this->newestPackageMigration()], $pending);
    }

    public function test_v1_only_schema_still_reports_drift(): void
    {
        // The pre-cutover fleet must behave exactly as it did before the probe
        // was widened.
        $this->bootWithRawTables(['nightowl_requests']);
        $this->recordNightowlHistory($this->appliedThroughSecondNewest());

        $this->assertSame([$this->newestPackageMigration()], $this->pendingNightowlMigrations());
    }

    public function test_current_v2_only_schema_is_up_to_date_not_behind(): void
    {
        // [] — "up to date" — is the second of the three meanings and must
        // survive the widened probe, or every boot would re-run migrate.
        $this->bootWithRawTables(['nightowl_requests_v2']);
        $this->recordNightowlHistory($this->packageMigrations());

        $this->assertSame([], $this->pendingNightowlMigrations());
    }

    public function test_neither_family_present_is_still_cannot_tell(): void
    {
        // Genuinely uninitialised: the onboarding path owns that, so the check
        // stays silent. The behind-by-one history is recorded anyway — with the
        // guard gone this case would answer [newest] like the two above, so the
        // assertion actually discriminates rather than passing on a fresh DB.
        $this->bootWithRawTables([]);
        $this->recordNightowlHistory($this->appliedThroughSecondNewest());

        $this->assertNull($this->pendingNightowlMigrations());
    }

    /**
     * Minimal app with a `nightowl` connection and a migrator, plus whichever
     * raw parents the scenario says exist.
     *
     * @param  list<string>  $rawTables
     */
    private function bootWithRawTables(array $rawTables): void
    {
        $this->app = new Application(sys_get_temp_dir().'/nightowl-agent-drift-test');
        $this->app->instance('config', new Repository([
            'database' => [
                // The primary connection is a SEPARATE in-memory database with
                // no `migrations` table, so MigrateCommand::primaryHistory()
                // contributes nothing and the verdict comes only from the
                // nightowl-side history the scenario records.
                'default' => 'primary',
                'migrations' => 'migrations',
                'connections' => [
                    'nightowl' => ['driver' => 'sqlite', 'database' => ':memory:', 'prefix' => ''],
                    'primary' => ['driver' => 'sqlite', 'database' => ':memory:', 'prefix' => ''],
                ],
            ],
        ]));

        (new DatabaseServiceProvider($this->app))->register();
        // The Migrator constructor pulls `files`, normally bound by
        // FilesystemServiceProvider — not one of the Application's base
        // providers. Only the repository half is exercised here.
        $this->app->instance('files', new Filesystem);
        (new MigrationServiceProvider($this->app))->register();
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication($this->app);

        foreach ($rawTables as $table) {
            Schema::connection('nightowl')->create($table, function (Blueprint $t) {
                $t->id();
            });
        }
    }

    /** @param  list<string>  $migrations */
    private function recordNightowlHistory(array $migrations): void
    {
        $repository = $this->app['migrator']->getRepository();
        $repository->setSource('nightowl');
        $repository->createRepository();

        foreach ($migrations as $migration) {
            $repository->log($migration, 1);
        }
    }

    private function pendingNightowlMigrations(): ?array
    {
        return (new ReflectionMethod(AgentCommand::class, 'pendingNightowlMigrations'))
            ->invoke(new AgentCommand);
    }

    /**
     * The package's real migration names, in the order the drift check globs
     * them — so "behind by one" means the genuinely newest migration.
     *
     * @return list<string>
     */
    private function packageMigrations(): array
    {
        $files = glob(realpath(__DIR__.'/../../database/migrations').'/*.php') ?: [];
        sort($files);

        return array_map(static fn (string $file): string => basename($file, '.php'), $files);
    }

    /** @return list<string> */
    private function appliedThroughSecondNewest(): array
    {
        return array_slice($this->packageMigrations(), 0, -1);
    }

    private function newestPackageMigration(): string
    {
        $all = $this->packageMigrations();

        return $all[count($all) - 1];
    }
}
