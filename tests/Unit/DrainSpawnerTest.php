<?php

namespace NightOwl\Tests\Unit;

use Closure;
use NightOwl\NightOwlAgentServiceProvider;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

/**
 * Guard logic for the drain-worker respawn strategy.
 *
 * makeDrainSpawner() decides whether the async server execs a fresh
 * `php artisan nightowl:drain-worker` (a clean interpreter isolated from the
 * parent agent's long-lived ReactPHP state) or falls back to the in-process
 * fork. It must return a Closure only when exec is actually usable: pcntl_exec
 * present AND an artisan entrypoint on disk. Otherwise null, so AsyncServer
 * degrades to the in-process path instead of execing something that isn't there.
 */
class DrainSpawnerTest extends TestCase
{
    private string $tmpDir;

    protected function setUp(): void
    {
        if (! function_exists('pcntl_exec')) {
            $this->markTestSkipped('pcntl_exec required to exercise the spawner path.');
        }

        $this->tmpDir = sys_get_temp_dir().'/nightowl-spawner-'.getmypid().'-'.uniqid();
        mkdir($this->tmpDir, 0755, true);
    }

    protected function tearDown(): void
    {
        @unlink($this->tmpDir.'/artisan');
        @rmdir($this->tmpDir);
    }

    public function testReturnsClosureWhenArtisanExists(): void
    {
        touch($this->tmpDir.'/artisan');

        $spawner = $this->makeSpawner($this->tmpDir);

        $this->assertInstanceOf(
            Closure::class,
            $spawner,
            'Expected a respawn closure when pcntl_exec and an artisan entrypoint are both available.',
        );
    }

    public function testReturnsNullWhenArtisanMissing(): void
    {
        // tmpDir intentionally has no artisan file.
        $spawner = $this->makeSpawner($this->tmpDir);

        $this->assertNull(
            $spawner,
            'Expected null (in-process fallback) when there is no artisan entrypoint to exec.',
        );
    }

    /**
     * Invoke the protected NightOwlAgentServiceProvider::makeDrainSpawner() with a
     * fake application whose basePath() points at $base.
     */
    private function makeSpawner(string $base): ?Closure
    {
        $app = new class($base)
        {
            public function __construct(private string $base) {}

            public function basePath(string $path = ''): string
            {
                return $this->base.($path !== '' ? DIRECTORY_SEPARATOR.$path : '');
            }
        };

        $provider = new NightOwlAgentServiceProvider($app);

        $method = new ReflectionMethod($provider, 'makeDrainSpawner');

        return $method->invoke($provider, $app);
    }
}
