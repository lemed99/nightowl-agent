<?php

namespace NightOwl\Tests\Unit;

use Illuminate\Config\Repository;
use Illuminate\Foundation\Application;
use Laravel\Nightwatch\Core;
use Laravel\Nightwatch\Ingest;
use NightOwl\NightOwlAgentServiceProvider;
use PHPUnit\Framework\TestCase;

/**
 * The `nightowl.enabled` master switch (NIGHTOWL_ENABLED) must make the
 * package inert: with it off, the service provider's booted hook must NOT
 * rebind Nightwatch's `Core::$ingest`, so no telemetry is collected or
 * transmitted to the agent socket. This is the switch customers flip in the
 * `testing` environment.
 */
final class MasterSwitchTest extends TestCase
{
    public function test_config_is_enabled_by_default(): void
    {
        // The config file uses storage_path() for other keys, so it needs an
        // application bound for the global helpers to resolve.
        new Application(sys_get_temp_dir().'/nightowl-master-switch-test');

        $config = require __DIR__.'/../../config/nightowl.php';

        $this->assertTrue($config['enabled'], 'NightOwl must default to enabled when NIGHTOWL_ENABLED is unset.');
        $this->assertFalse($config['run_migrations'], 'Migrations are managed by nightowl:install/migrate, so the host-migrate ride-along is off by default.');
    }

    public function test_disabled_switch_does_not_rebind_nightwatch_ingest(): void
    {
        $core = $this->bootProviderWithSwitch(false);

        $this->assertSame(
            'SENTINEL',
            $core->ingest,
            'With NIGHTOWL_ENABLED=false the provider must leave Core::$ingest untouched.'
        );
    }

    public function test_enabled_switch_rebinds_nightwatch_ingest_to_the_agent(): void
    {
        $core = $this->bootProviderWithSwitch(true);

        $this->assertInstanceOf(
            Ingest::class,
            $core->ingest,
            'With NIGHTOWL_ENABLED=true the provider must redirect Core::$ingest at the NightOwl agent.'
        );
    }

    /**
     * Boot a minimal application with the provider registered, returning the
     * stand-in Core whose public $ingest the booted hook may (or may not)
     * rebind depending on the switch.
     */
    private function bootProviderWithSwitch(bool $enabled): object
    {
        $app = new Application(sys_get_temp_dir().'/nightowl-master-switch-test');
        $app->instance('config', new Repository([
            'app' => ['name' => 'Test', 'env' => 'testing'],
            'nightowl' => [
                'enabled' => $enabled,
                'agent' => ['port' => 2407, 'token' => 'test-token'],
                'parallel_with_nightwatch' => false,
            ],
        ]));

        // Stand in for the Nightwatch SDK: only the public, mutable $ingest
        // property is exercised by the provider's booted hook.
        $core = new class
        {
            public mixed $ingest = 'SENTINEL';
        };
        $app->instance(Core::class, $core);

        $app->register(new NightOwlAgentServiceProvider($app));
        $app->boot();

        return $core;
    }
}
