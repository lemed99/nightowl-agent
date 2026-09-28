<?php

namespace NightOwl\Tests\Unit;

use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Database\DatabaseServiceProvider;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use NightOwl\Agent\DrainWorker;
use NightOwl\NightOwlAgentServiceProvider;
use NightOwl\Tests\Fixtures\ScriptedServer;
use PHPUnit\Framework\TestCase;

/**
 * NIGHTOWL_DB_SSLMODE on the Laravel `nightowl` connection.
 *
 * The drain and the health alerts connect with it, but the connection the
 * service provider registers had no sslmode, so nightowl:install,
 * nightowl:migrate and every other artisan command used libpq's default. A
 * database that requires SSL refused them, or one reached over an untrusted
 * network got a connection that silently fell back to plain text.
 *
 * Only when it is set. 'database.sslmode' falls back to 'prefer', and passing
 * that on every install overrode a PGSSLMODE environment variable the
 * commands had honoured. Unset, the connection carries no sslmode and libpq
 * applies PGSSLMODE. The drain keeps 'prefer', as it always had.
 */
final class NightowlConnectionSslmodeTest extends TestCase
{
    private const ENV = ['PGSSLMODE', 'NIGHTOWL_DB_SSLMODE'];

    private ?Container $previousContainer = null;

    /** @var array<string, string|false> */
    private array $previousEnv = [];

    protected function setUp(): void
    {
        $this->previousContainer = Container::getInstance();

        foreach (self::ENV as $name) {
            $this->previousEnv[$name] = getenv($name);
            putenv($name);
        }
    }

    protected function tearDown(): void
    {
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);
        Container::setInstance($this->previousContainer);

        foreach ($this->previousEnv as $name => $value) {
            putenv($value === false ? $name : "{$name}={$value}");
        }
    }

    public function test_the_configured_sslmode_is_on_the_connection(): void
    {
        $app = $this->boot(['sslmode' => 'verify-full']);

        $this->assertSame('verify-full', $app['config']->get('database.connections.nightowl.sslmode'));
    }

    public function test_no_sslmode_is_set_when_none_is_configured(): void
    {
        // 'prefer' is what a published config holds when NIGHTOWL_DB_SSLMODE
        // is unset, and null the package's own 'database' array.
        foreach ([null, '', '  ', 'prefer'] as $sslmode) {
            $app = $this->boot(['sslmode' => $sslmode]);

            $this->assertArrayNotHasKey(
                'sslmode',
                $app['config']->get('database.connections.nightowl'),
                'an unset NIGHTOWL_DB_SSLMODE must leave the mode to libpq',
            );
        }

        $this->assertArrayNotHasKey('sslmode', $this->boot(null)['config']->get('database.connections.nightowl'));
    }

    /** Set explicitly, 'prefer' is passed like any other mode. */
    public function test_an_explicit_nightowl_db_sslmode_is_on_the_connection(): void
    {
        putenv('NIGHTOWL_DB_SSLMODE=prefer');

        $this->assertSame('prefer', $this->boot(['sslmode' => 'prefer'])['config']->get('database.connections.nightowl.sslmode'));
        $this->assertSame('prefer', $this->boot(null)['config']->get('database.connections.nightowl.sslmode'));
    }

    /** On the wire: with NIGHTOWL_DB_SSLMODE unset, libpq applies PGSSLMODE. */
    public function test_pgsslmode_applies_when_nightowl_db_sslmode_is_unset(): void
    {
        putenv('PGSSLMODE=require');
        $this->assertSame(['SSL'], $this->openingPackets('prefer'));

        putenv('PGSSLMODE=disable');
        $this->assertSame(['STARTUP'], $this->openingPackets('prefer'));
    }

    public function test_an_explicit_nightowl_db_sslmode_wins_over_pgsslmode(): void
    {
        putenv('PGSSLMODE=require');

        putenv('NIGHTOWL_DB_SSLMODE=disable');
        $this->assertSame(['STARTUP'], $this->openingPackets('disable'));

        putenv('NIGHTOWL_DB_SSLMODE=prefer');
        $this->assertSame(['SSL', 'STARTUP'], $this->openingPackets('prefer'));
    }

    /**
     * The drain has always connected with 'database.sslmode', 'prefer' when
     * nothing is set, whatever PGSSLMODE says. That does not change.
     */
    public function test_the_drain_keeps_its_sslmode(): void
    {
        putenv('PGSSLMODE=require');
        $sslmode = new \ReflectionProperty(DrainWorker::class, 'sslmode');

        $this->assertSame('prefer', $sslmode->getValue($this->boot(['sslmode' => 'prefer'])->make(DrainWorker::class)));
        $this->assertSame('prefer', $sslmode->getValue($this->boot(null)->make(DrainWorker::class)));
        $this->assertSame('prefer', $sslmode->getValue($this->boot(['host' => '127.0.0.1'])->make(DrainWorker::class)));
        $this->assertSame('verify-full', $sslmode->getValue($this->boot(['sslmode' => 'verify-full'])->make(DrainWorker::class)));
    }

    /**
     * On the wire, through Laravel's own connector: libpq opens with an
     * SSLRequest unless sslmode is 'disable'.
     */
    public function test_artisan_connections_use_the_configured_sslmode(): void
    {
        $this->assertSame(['STARTUP'], $this->openingPackets('disable'));
        $this->assertSame(['SSL'], $this->openingPackets('require'));
    }

    /** @return list<string> */
    private function openingPackets(string $sslmode): array
    {
        $server = ScriptedServer::start('fake-pg-startup.php');

        try {
            $app = $this->boot([
                'host' => '127.0.0.1',
                'port' => $server->port,
                'database' => 'x',
                'username' => 'u',
                'password' => 'p',
                'sslmode' => $sslmode,
            ]);

            try {
                $app['db']->connection('nightowl')->getPdo();
                $this->fail('the fake server hangs up, so the connection cannot succeed');
            } catch (\PDOException) {
                // Expected: all that matters is how the client opened.
            }

            return array_values(array_filter($server->transcript(), static fn (string $line): bool => $line !== 'GSSENC'));
        } finally {
            $server->cleanup();
        }
    }

    /**
     * @param  array<string, mixed>|null  $database  a published config's
     *                                                'database' array, or null
     *                                                for the package's own
     */
    private function boot(?array $database): Application
    {
        $nightowl = ['enabled' => true];
        if ($database !== null) {
            $nightowl['database'] = $database;
        }

        $app = new Application(sys_get_temp_dir().'/nightowl-connection-sslmode-test');
        $app->instance('config', new Repository([
            'app' => ['name' => 'Test', 'env' => 'testing'],
            'database' => ['default' => 'nightowl', 'connections' => []],
            'nightowl' => $nightowl,
        ]));
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication($app);

        $app->register(new DatabaseServiceProvider($app));
        $app->register(new NightOwlAgentServiceProvider($app));
        $app->boot();

        return $app;
    }
}
