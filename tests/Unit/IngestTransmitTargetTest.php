<?php

namespace NightOwl\Tests\Unit;

use Illuminate\Config\Repository;
use Illuminate\Foundation\Application;
use Laravel\Nightwatch\Core;
use Laravel\Nightwatch\Ingest;
use NightOwl\NightOwlAgentServiceProvider;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

/**
 * The instrumented app must be able to transmit telemetry to a REMOTE agent,
 * not just a co-located one on loopback. This is what unlocks serverless hosts
 * (Laravel Vapor): AWS Lambda can't run the long-lived agent in-process, so the
 * agent runs on a separate box (EC2/Forge) in the same VPC and the Vapor app
 * ships to it via NIGHTOWL_INGEST_URI — mirroring nightwatch's NIGHTWATCH_INGEST_URI.
 *
 * These tests guard both the new remote path and backward compatibility: with
 * nothing configured, transmit MUST still target the loopback listener so every
 * existing single-host install is unaffected.
 */
final class IngestTransmitTargetTest extends TestCase
{
    public function test_config_defaults_ingest_uri_to_null_so_provider_resolves_the_port(): void
    {
        // The config file uses storage_path() for other keys, so it needs an
        // application bound for the global helpers to resolve.
        new Application(sys_get_temp_dir().'/nightowl-ingest-target-test');

        $config = require __DIR__.'/../../config/nightowl.php';

        // ingest_uri is deliberately NULL when NIGHTOWL_INGEST_URI is unset — the
        // service provider derives the loopback default from the RESOLVED
        // agent.port config (see the provider tests below). Hardcoding a port here
        // would ship to that port even when the listener bound a config-file port.
        $this->assertNull(
            $config['agent']['ingest_uri'],
            'ingest_uri must default to null so the provider follows the resolved agent.port, not a hardcoded env port.'
        );
        $this->assertSame(
            0.5,
            (float) $config['agent']['ingest_timeout'],
            'The transmit timeout must default to 0.5s, tuned for a loopback agent.'
        );
    }

    public function test_null_ingest_uri_follows_agent_port_config(): void
    {
        // Production shape: the config file loads ingest_uri as null (env unset)
        // while agent.port is set via the config file, not NIGHTOWL_AGENT_PORT.
        // Transmit must follow the config port — the #7 regression fix.
        $ingest = $this->bootIngest(['port' => 2500, 'ingest_uri' => null, 'token' => 'test-token']);

        $this->assertSame(
            'tcp://127.0.0.1:2500',
            $this->prop($ingest, 'transmitTo'),
            'A null ingest_uri must resolve to the loopback listener on the configured agent port.'
        );
    }

    public function test_default_transmits_to_loopback_on_configured_port(): void
    {
        // No ingest_uri set: transmit falls back to the loopback listener on
        // the configured agent port (backward-compatible with pre-remote wiring).
        $ingest = $this->bootIngest(['port' => 2407, 'token' => 'test-token']);

        $this->assertSame('tcp://127.0.0.1:2407', $this->prop($ingest, 'transmitTo'));
        $this->assertSame(0.5, $this->prop($ingest, 'connectionTimeout'));
        $this->assertSame(0.5, $this->prop($ingest, 'timeout'));
    }

    public function test_transmit_port_follows_agent_port_when_uri_unset(): void
    {
        $ingest = $this->bootIngest(['port' => 9999, 'token' => 'test-token']);

        $this->assertSame(
            'tcp://127.0.0.1:9999',
            $this->prop($ingest, 'transmitTo'),
            'When only NIGHTOWL_AGENT_PORT is customised, transmit must follow it — no separate port needed for the co-located case.'
        );
    }

    public function test_remote_ingest_uri_is_honored(): void
    {
        // The core fix: a Vapor app points at the agent on a private VPC
        // address, and the app must transmit THERE, not to 127.0.0.1.
        $ingest = $this->bootIngest([
            'port' => 2407,
            'token' => 'test-token',
            'ingest_uri' => '10.0.0.5:2407',
        ]);

        $this->assertSame(
            'tcp://10.0.0.5:2407',
            $this->prop($ingest, 'transmitTo'),
            'NIGHTOWL_INGEST_URI must redirect transmit to the remote agent — otherwise Vapor/serverless can never reach the agent.'
        );
    }

    public function test_bare_host_uri_falls_back_to_agent_port(): void
    {
        $ingest = $this->bootIngest([
            'port' => 9100,
            'token' => 'test-token',
            'ingest_uri' => '10.0.0.5',
        ]);

        $this->assertSame(
            'tcp://10.0.0.5:9100',
            $this->prop($ingest, 'transmitTo'),
            'A NIGHTOWL_INGEST_URI with no port must fall back to the configured agent port.'
        );
    }

    public function test_empty_ingest_uri_falls_back_to_loopback(): void
    {
        $ingest = $this->bootIngest([
            'port' => 2407,
            'token' => 'test-token',
            'ingest_uri' => '',
        ]);

        $this->assertSame(
            'tcp://127.0.0.1:2407',
            $this->prop($ingest, 'transmitTo'),
            'An empty NIGHTOWL_INGEST_URI must not produce a portless/hostless target — fall back to the loopback listener.'
        );
    }

    public function test_surrounding_whitespace_is_trimmed(): void
    {
        // A stray space in a .env value (NIGHTOWL_INGEST_URI="10.0.0.5:2407 ")
        // must not produce a malformed target that silently drops telemetry.
        $ingest = $this->bootIngest([
            'port' => 2407,
            'token' => 'test-token',
            'ingest_uri' => "  10.0.0.5:2407\t",
        ]);

        $this->assertSame('tcp://10.0.0.5:2407', $this->prop($ingest, 'transmitTo'));
    }

    public function test_bracketed_ipv6_literal_passes_through_with_its_port(): void
    {
        // IPv6 must be bracketed; the bracketed form already carries its port,
        // so it must pass through unchanged (no spurious port appended).
        $ingest = $this->bootIngest([
            'port' => 2407,
            'token' => 'test-token',
            'ingest_uri' => '[fd00::5]:2407',
        ]);

        $this->assertSame('tcp://[fd00::5]:2407', $this->prop($ingest, 'transmitTo'));
    }

    public function test_bracketed_ipv6_literal_without_port_gets_agent_port(): void
    {
        // A bracketed IPv6 literal with no port must get the configured agent
        // port appended. The colons INSIDE "[fd00::5]" must not be mistaken for
        // a port delimiter — otherwise the app ships to a portless, unconnectable
        // target and telemetry silently drops.
        $ingest = $this->bootIngest([
            'port' => 2407,
            'token' => 'test-token',
            'ingest_uri' => '[fd00::5]',
        ]);

        $this->assertSame('tcp://[fd00::5]:2407', $this->prop($ingest, 'transmitTo'));
    }

    public function test_ingest_timeout_is_configurable_for_the_network_hop(): void
    {
        $ingest = $this->bootIngest([
            'port' => 2407,
            'token' => 'test-token',
            'ingest_uri' => '10.0.0.5:2407',
            'ingest_timeout' => 2.5,
        ]);

        $this->assertSame(2.5, $this->prop($ingest, 'connectionTimeout'));
        $this->assertSame(2.5, $this->prop($ingest, 'timeout'));
    }

    /**
     * Boot a minimal application with the provider registered and return the
     * Nightwatch Ingest the booted hook binds onto Core::$ingest.
     */
    private function bootIngest(array $agentConfig): Ingest
    {
        $app = new Application(sys_get_temp_dir().'/nightowl-ingest-target-test');
        $app->instance('config', new Repository([
            'app' => ['name' => 'Test', 'env' => 'testing'],
            'nightowl' => [
                'enabled' => true,
                'agent' => $agentConfig,
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

        $this->assertInstanceOf(
            Ingest::class,
            $core->ingest,
            'The provider must redirect Core::$ingest at a Nightwatch Ingest.'
        );

        return $core->ingest;
    }

    private function prop(Ingest $ingest, string $name): mixed
    {
        return (new ReflectionClass($ingest))->getProperty($name)->getValue($ingest);
    }
}
