<?php

namespace NightOwl\Tests\Unit;

use Illuminate\Config\Repository;
use Illuminate\Console\OutputStyle;
use Illuminate\Container\Container;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use NightOwl\Commands\AgentCommand;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * NIGHTOWL_AUTO_BACKFILL is the escape hatch a customer gets after the 2.0.0
 * boot-backfill wedge: an operator who would rather own that pass themselves can
 * stop the daemon from ever starting it.
 *
 * The gate has two halves, and the second is the one that is easy to get wrong.
 * Off must still leave the debt RECORDED — the marker file is what makes the
 * reminder recur, and without it the boot after a successful schema run sees zero
 * pending migrations, says nothing, and leaves whatever rollup tables that run
 * created existing-but-empty. An existing-but-empty rollup is worse than an
 * absent one: the API read path prefers any rollup table that exists over raw, so
 * those tables serve ZEROS to every wide-range chart, indefinitely.
 */
final class AgentCommandAutoBackfillGateTest extends TestCase
{
    private ?Application $app = null;

    private ?Container $previousContainer = null;

    private string $basePath;

    protected function setUp(): void
    {
        // Same cross-class container leak AgentCommandSchemaDriftTest documents:
        // booting an Application makes it the global container.
        $this->previousContainer = Container::getInstance();
        $this->basePath = sys_get_temp_dir().'/nightowl-autobackfill-'.getmypid().'-'.uniqid();
        mkdir($this->basePath.'/storage/logs', 0755, true);
    }

    protected function tearDown(): void
    {
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);
        Container::setInstance($this->previousContainer);
        $this->app = null;

        exec('rm -rf '.escapeshellarg($this->basePath));
    }

    public function test_disabled_records_the_debt_warns_and_spawns_nothing(): void
    {
        $output = $this->boot(autoBackfill: false);

        $this->spawn();

        $this->assertFileExists(
            $this->basePath.'/storage/nightowl/backfill-pending',
            'Off must still record the debt — the marker is what makes the reminder recur every boot.',
        );
        $this->assertFileDoesNotExist($this->basePath.'/spawned', 'Nothing may be spawned when the gate is off.');

        $text = $output->fetch();
        $this->assertStringContainsString('NIGHTOWL_AUTO_BACKFILL=false', $text);
        $this->assertStringContainsString('nightowl:migrate', $text, 'The warning has to name the command that clears it.');
        $this->assertStringContainsString('ZERO', $text, 'And say what an unfilled rollup actually costs.');
    }

    public function test_enabled_is_the_default_and_spawns_the_reconciliation(): void
    {
        // Default-on deliberately: this ships in the same release as the chunk
        // pacing that made the pass safe, and defaulting off would hand every
        // upgrading tenant the empty-rollup-serves-zeros problem instead.
        $this->boot(autoBackfill: null);

        $this->spawn();

        $this->assertFileExists($this->basePath.'/storage/nightowl/backfill-pending');
        $this->assertTrue($this->waitForFile($this->basePath.'/spawned'), 'The detached reconciliation never ran.');

        // The subshell removes the marker only on a clean exit — that is what
        // stops the next boot from re-spawning.
        $this->assertTrue(
            $this->waitForAbsence($this->basePath.'/storage/nightowl/backfill-pending'),
            'A completed reconciliation must clear its own marker.',
        );
    }

    /** Minimal app rooted at a temp basePath, with a fake `artisan` to exec. */
    private function boot(?bool $autoBackfill): BufferedOutput
    {
        // Stands in for the host app's artisan: records that it was invoked and
        // exits 0, so the subshell's `&& rm -f <marker>` runs exactly as it
        // would after a real nightowl:migrate.
        file_put_contents(
            $this->basePath.'/artisan',
            '<?php file_put_contents(__DIR__."/spawned", implode(" ", array_slice($argv, 1)));'
        );

        $this->app = new Application($this->basePath);
        $this->app->instance('config', new Repository([
            'nightowl' => $autoBackfill === null ? [] : ['auto_backfill' => $autoBackfill],
        ]));
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication($this->app);

        return $this->output = new BufferedOutput;
    }

    private function spawn(): void
    {
        $command = new AgentCommand;
        $command->setLaravel($this->app);
        $command->setOutput(new OutputStyle(new ArrayInput([]), $this->output));

        (new ReflectionMethod(AgentCommand::class, 'spawnBackgroundBackfill'))->invoke($command);
    }

    private BufferedOutput $output;

    private function waitForFile(string $path, float $seconds = 5.0): bool
    {
        return $this->waitFor(fn (): bool => is_file($path), $seconds);
    }

    private function waitForAbsence(string $path, float $seconds = 5.0): bool
    {
        return $this->waitFor(fn (): bool => ! is_file($path), $seconds);
    }

    private function waitFor(callable $done, float $seconds): bool
    {
        $deadline = microtime(true) + $seconds;
        while (microtime(true) < $deadline) {
            clearstatcache();
            if ($done()) {
                return true;
            }
            usleep(50_000);
        }

        return false;
    }
}
