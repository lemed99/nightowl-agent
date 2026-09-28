<?php

namespace NightOwl\Tests\Unit;

use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use NightOwl\Agent\AsyncServer;
use NightOwl\NightOwlAgentServiceProvider;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * NIGHTOWL_DRAIN_WORKERS has to reach the agent. It sat in the config from the
 * first release while the provider built AsyncServer without it, so every
 * install ran one drain worker whatever it asked for, and the multi-worker
 * claim path had never run outside the test harness, which sets the count
 * itself.
 */
final class DrainWorkerCountTest extends TestCase
{
    private ?Container $previousContainer = null;

    protected function setUp(): void
    {
        $this->previousContainer = Container::getInstance();
    }

    protected function tearDown(): void
    {
        Facade::clearResolvedInstances();
        Container::setInstance($this->previousContainer);
    }

    public function test_the_provider_passes_the_configured_worker_count_to_the_agent(): void
    {
        // env() hands the value over as a string.
        $this->assertSame(4, $this->resolveServer('4')->drainWorkerCount());
    }

    public function test_the_default_is_one_worker(): void
    {
        $this->assertSame(1, $this->resolveServer(null)->drainWorkerCount());
    }

    public function test_the_provider_clamps_an_out_of_range_value(): void
    {
        $this->assertSame(1, $this->resolveServer('0')->drainWorkerCount());
        $this->assertSame(1, $this->resolveServer('several')->drainWorkerCount());
        $this->assertSame(AsyncServer::MAX_DRAIN_WORKERS, $this->resolveServer('100')->drainWorkerCount());
    }

    /**
     * @return iterable<string, array{int, int}>
     */
    public static function clampCases(): iterable
    {
        yield 'negative' => [-3, 1];
        yield 'zero' => [0, 1];
        yield 'one' => [1, 1];
        yield 'in range' => [8, 8];
        yield 'the maximum' => [AsyncServer::MAX_DRAIN_WORKERS, AsyncServer::MAX_DRAIN_WORKERS];
        yield 'above the maximum' => [AsyncServer::MAX_DRAIN_WORKERS + 1, AsyncServer::MAX_DRAIN_WORKERS];
    }

    #[DataProvider('clampCases')]
    public function test_clamp(int $requested, int $expected): void
    {
        $previous = ini_set('error_log', '/dev/null');

        try {
            $this->assertSame($expected, AsyncServer::clampDrainWorkerCount($requested));
        } finally {
            ini_set('error_log', (string) $previous);
        }
    }

    private function resolveServer(?string $drainWorkers): AsyncServer
    {
        $base = sys_get_temp_dir().'/nightowl-drain-workers-'.getmypid();
        @mkdir($base, 0777, true);

        $agent = ['health_enabled' => false, 'health_report_enabled' => false];
        if ($drainWorkers !== null) {
            $agent['drain_workers'] = $drainWorkers;
        }

        $app = new Application($base);
        $app->instance('config', new Repository([
            'app' => ['name' => 'Test', 'env' => 'testing'],
            'nightowl' => [
                // Off: the ingest wiring is not what this test is about.
                'enabled' => false,
                'agent' => $agent,
                'update_check' => ['enabled' => false],
            ],
        ]));

        $app->register(new NightOwlAgentServiceProvider($app));

        $previous = ini_set('error_log', '/dev/null');

        try {
            return $app->make(AsyncServer::class);
        } finally {
            ini_set('error_log', (string) $previous);
        }
    }
}
