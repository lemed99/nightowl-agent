<?php

namespace NightOwl\Tests\Unit;

use Illuminate\Foundation\Application;
use Illuminate\Foundation\Bootstrap\HandleExceptions;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Facade;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Route;
use Illuminate\Support\ServiceProvider;
use Laravel\Nightwatch\Core;
use Laravel\Nightwatch\NightwatchServiceProvider;
use Laravel\Nightwatch\RecordsBuffer;
use NightOwl\NightOwlAgentServiceProvider;
use NightOwl\Support\MultiIngest;
use NightOwl\Tests\Fixtures\ScriptedServer;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request as SymfonyRequest;

/**
 * Records written while the application boots must reach NightOwl.
 *
 * Nightwatch's listeners are live from its register(), so a log line, query or
 * cache call in any provider's boot() is recorded straight away. NightOwl used
 * to attach its ingest in booted(), after every boot() had run, so those
 * records went into the SDK's own ingest instead: alone, that ingest is never
 * sent; in parallel mode only Nightwatch got them. Reproduced on 2026-09-26
 * with a Log::warning in AppServiceProvider::boot().
 *
 * A real application with the real Nightwatch provider, a request through the
 * HTTP kernel, and fake agents on both ends of the socket: what is asserted is
 * what each agent received, exactly once.
 */
final class BootTimeRecordsTest extends TestCase
{
    private const MARKER = 'nightowl boot-time marker';

    /** @var array<string, string> */
    private array $env = [];

    /** @var list<ScriptedServer> */
    private array $agents = [];

    private string|false $previousErrorLog = false;

    protected function setUp(): void
    {
        BootTimeRecordsProvider::$onRegister = null;
        BootTimeRecordsProvider::$onBoot = null;
        // MultiIngest logs every refused connection to the closed port below.
        $this->previousErrorLog = ini_set('error_log', '/dev/null');
    }

    protected function tearDown(): void
    {
        foreach ($this->agents as $agent) {
            $agent->cleanup();
        }
        foreach (array_keys($this->env) as $key) {
            putenv($key);
            unset($_ENV[$key], $_SERVER[$key]);
        }
        BootTimeRecordsProvider::$onRegister = null;
        BootTimeRecordsProvider::$onBoot = null;
        Facade::clearResolvedInstances();
        HandleExceptions::flushState();
        ini_set('error_log', (string) $this->previousErrorLog);
    }

    public function test_alone_a_log_written_in_a_providers_boot_reaches_nightowl(): void
    {
        $nightowl = $this->startAgent();
        BootTimeRecordsProvider::$onBoot = fn () => Log::warning(self::MARKER);

        $this->handleRequest(nightowlPort: $nightowl->port, nightwatchPort: null);

        $this->assertSame(1, $this->markerCount($nightowl, 'log'), 'the boot-time log line must reach NightOwl, once');
    }

    public function test_alone_it_does_not_matter_which_package_registers_first(): void
    {
        $nightowl = $this->startAgent();
        BootTimeRecordsProvider::$onBoot = fn () => Log::warning(self::MARKER);

        $this->handleRequest(nightowlPort: $nightowl->port, nightwatchPort: null, nightowlFirst: true);

        $this->assertSame(1, $this->markerCount($nightowl, 'log'));
    }

    public function test_in_parallel_mode_it_reaches_both_agents_once(): void
    {
        $nightowl = $this->startAgent();
        $nightwatch = $this->startAgent();
        BootTimeRecordsProvider::$onBoot = fn () => Log::warning(self::MARKER);

        $this->handleRequest(nightowlPort: $nightowl->port, nightwatchPort: $nightwatch->port);

        $this->assertSame(1, $this->markerCount($nightowl, 'log'), 'NightOwl must get the boot-time log line');
        $this->assertSame(1, $this->markerCount($nightwatch, 'log'), 'Nightwatch must still get it, and only once');
    }

    public function test_an_exception_thrown_by_a_providers_boot_reaches_nightowl(): void
    {
        // booted() never fires after this, so a swap there never happened.
        $nightowl = $this->startAgent();
        BootTimeRecordsProvider::$onBoot = fn () => throw new \RuntimeException(self::MARKER);

        $status = $this->handleRequest(nightowlPort: $nightowl->port, nightwatchPort: null);

        $this->assertSame(500, $status);
        $this->assertSame(1, $this->markerCount($nightowl, 'exception'), 'the exception that stopped the boot must reach NightOwl');
    }

    public function test_a_record_written_before_nightowl_attached_is_handed_over(): void
    {
        if (! method_exists(RecordsBuffer::class, 'all')) {
            $this->markTestSkipped('This Nightwatch has no RecordsBuffer::all(); the hand-over is skipped by design.');
        }

        // A provider's register() runs before the swap; its record sits in the
        // SDK's buffer until NightOwl attaches.
        $nightowl = $this->startAgent();
        BootTimeRecordsProvider::$onRegister = fn () => Log::warning(self::MARKER);

        $this->handleRequest(nightowlPort: $nightowl->port, nightwatchPort: null);

        $this->assertSame(1, $this->markerCount($nightowl, 'log'));
    }

    public function test_in_parallel_mode_a_handed_over_record_still_reaches_nightwatch_once(): void
    {
        if (! method_exists(RecordsBuffer::class, 'all')) {
            $this->markTestSkipped('This Nightwatch has no RecordsBuffer::all(); the hand-over is skipped by design.');
        }

        $nightowl = $this->startAgent();
        $nightwatch = $this->startAgent();
        BootTimeRecordsProvider::$onRegister = fn () => Log::warning(self::MARKER);

        $this->handleRequest(nightowlPort: $nightowl->port, nightwatchPort: $nightwatch->port);

        $this->assertSame(1, $this->markerCount($nightowl, 'log'));
        $this->assertSame(1, $this->markerCount($nightwatch, 'log'));
    }

    // ─── Settings changed in a provider's boot() ───────────────

    /**
     * NightOwl attaches before any boot() runs, so a provider that turns it
     * off in boot() is only seen afterwards. It must still end up off: the
     * SDK's own ingest back on Core, sending what NightOwl had buffered, as if
     * NightOwl had never attached.
     */
    public function test_alone_nightowl_disabled_in_a_providers_boot_sends_nightowl_nothing(): void
    {
        $nightowl = $this->startAgent();
        $nightwatch = $this->startAgent();
        BootTimeRecordsProvider::$onBoot = function () {
            Log::warning(self::MARKER);
            config(['nightowl.enabled' => false]);
        };

        $this->handleRequest(nightowlPort: $nightowl->port, nightwatchPort: $nightwatch->port, parallel: false);

        $this->assertSame(0, $this->markerCount($nightowl, 'log'), 'NightOwl stayed attached after boot() disabled it');
        $this->assertSame(0, $this->recordCount($nightowl, 'request'));
        if (self::canHandOver()) {
            $this->assertSame(1, $this->markerCount($nightwatch, 'log'), 'the SDK must send what it would have without NightOwl, once');
        }
        $this->assertSame(1, $this->recordCount($nightwatch, 'request'));
    }

    public function test_in_parallel_mode_nightowl_disabled_in_a_providers_boot_leaves_nightwatch_alone(): void
    {
        $nightowl = $this->startAgent();
        $nightwatch = $this->startAgent();
        BootTimeRecordsProvider::$onBoot = function () {
            Log::warning(self::MARKER);
            config(['nightowl.enabled' => false]);
        };

        $this->handleRequest(nightowlPort: $nightowl->port, nightwatchPort: $nightwatch->port);

        $this->assertSame(0, $this->markerCount($nightowl, 'log'));
        $this->assertSame(0, $this->recordCount($nightowl, 'request'));
        $this->assertSame(1, $this->markerCount($nightwatch, 'log'), 'Nightwatch must get its record once, not twice');
        $this->assertSame(1, $this->recordCount($nightwatch, 'request'));
    }

    /**
     * An ingest setting changed in boot() is applied: the records go to the
     * new target, including those written before the change.
     */
    public function test_an_ingest_uri_set_in_a_providers_boot_is_used(): void
    {
        $fromEnv = $this->startAgent();
        $fromBoot = $this->startAgent();
        BootTimeRecordsProvider::$onBoot = function () use ($fromBoot) {
            Log::warning(self::MARKER);
            config(['nightowl.agent.ingest_uri' => '127.0.0.1:'.$fromBoot->port]);
        };

        $this->handleRequest(nightowlPort: $fromEnv->port, nightwatchPort: null);

        if (self::canHandOver()) {
            $this->assertSame(1, $this->markerCount($fromBoot, 'log'), 'the record must reach the target set in boot(), once');
        }
        $this->assertSame(1, $this->recordCount($fromBoot, 'request'));
        $this->assertSame(0, $this->markerCount($fromEnv, 'log'));
        $this->assertSame(0, $this->recordCount($fromEnv, 'request'));
    }

    public function test_parallel_mode_turned_on_in_a_providers_boot_reaches_both_agents_once(): void
    {
        $nightowl = $this->startAgent();
        $nightwatch = $this->startAgent();
        BootTimeRecordsProvider::$onBoot = function () {
            Log::warning(self::MARKER);
            config(['nightowl.parallel_with_nightwatch' => true]);
        };

        $this->handleRequest(nightowlPort: $nightowl->port, nightwatchPort: $nightwatch->port, parallel: false);

        if (self::canHandOver()) {
            $this->assertSame(1, $this->markerCount($nightowl, 'log'));
            $this->assertSame(1, $this->markerCount($nightwatch, 'log'));
        }
        $this->assertSame(1, $this->recordCount($nightowl, 'request'));
        $this->assertSame(1, $this->recordCount($nightwatch, 'request'));
    }

    /**
     * booted() re-checks the settings. When no boot() changed one, the ingest
     * attached in booting() must stay: rebuilding it would register the
     * capture hooks a second time on every boot.
     */
    public function test_a_boot_that_changes_no_setting_keeps_the_ingest(): void
    {
        $nightowl = $this->startAgent();
        $this->setEnv(['NIGHTOWL_CAPTURE_REQUEST_PAYLOAD' => 'true']);
        $seen = new \ArrayObject;
        BootTimeRecordsProvider::$onBoot = function () use ($seen) {
            $seen['boot'] = app(Core::class)->ingest;
            // Registered after NightOwl's own booted() check, so it runs after it.
            app()->booted(function () use ($seen) {
                $seen['booted'] = app(Core::class)->ingest;
            });
        };

        $this->handleRequest(nightowlPort: $nightowl->port, nightwatchPort: null);

        $this->assertInstanceOf(MultiIngest::class, $seen['boot'] ?? null);
        $this->assertSame($seen['boot'], $seen['booted'] ?? null, 'a boot that changed no setting rebuilt the ingest');
        $this->assertSame(1, $this->recordCount($nightowl, 'request'));
    }

    /** NightOwl off in the environment and turned on in boot() attaches in booted(). */
    public function test_nightowl_enabled_in_a_providers_boot_attaches(): void
    {
        $nightowl = $this->startAgent();
        $this->setEnv(['NIGHTOWL_ENABLED' => 'false']);
        BootTimeRecordsProvider::$onBoot = function () {
            Log::warning(self::MARKER);
            config(['nightowl.enabled' => true]);
        };

        $this->handleRequest(nightowlPort: $nightowl->port, nightwatchPort: null);

        if (self::canHandOver()) {
            $this->assertSame(1, $this->markerCount($nightowl, 'log'), 'the record written before NightOwl attached must be handed over, once');
        }
        $this->assertSame(1, $this->recordCount($nightowl, 'request'));
    }

    // ─── Harness ───────────────────────────────────────────────

    /**
     * Whether this Nightwatch lets a buffered record move from one ingest to
     * another. A setting changed in boot() re-wires the ingest in booted(), and
     * the boot-time records written before it move only through
     * RecordsBuffer::all(), which Nightwatch before about 1.29 lacks: there they are dropped
     * by design, and only the request records that follow can be asserted.
     */
    private static function canHandOver(): bool
    {
        return method_exists(RecordsBuffer::class, 'all');
    }

    private function startAgent(): ScriptedServer
    {
        return $this->agents[] = ScriptedServer::start('fake-ingest-agent.php', ['startup' => 5, 'idle' => 1]);
    }

    /**
     * Boot a real app through the HTTP kernel, GET /ping, terminate. Alone
     * (null $nightwatchPort) Nightwatch's own target is a closed port, so
     * nothing it sent could be mistaken for NightOwl's.
     */
    private function handleRequest(int $nightowlPort, ?int $nightwatchPort, bool $nightowlFirst = false, ?bool $parallel = null): int
    {
        $parallel ??= $nightwatchPort !== null;

        $this->setEnv([
            'NIGHTWATCH_TOKEN' => 'test-token',
            // Nightwatch picks request vs command state by runningInConsole(); PHPUnit is a console.
            'NIGHTWATCH_FORCE_REQUEST' => '1',
            'NIGHTWATCH_INGEST_URI' => '127.0.0.1:'.($nightwatchPort ?? 1),
            'NIGHTOWL_INGEST_URI' => '127.0.0.1:'.$nightowlPort,
            'NIGHTOWL_PARALLEL_WITH_NIGHTWATCH' => $parallel ? 'true' : 'false',
            'LOG_CHANNEL' => 'nightwatch',
            'APP_KEY' => 'base64:'.base64_encode(str_repeat('k', 32)),
        ]);

        $base = sys_get_temp_dir().'/nightowl-boot-time-'.getmypid();
        @mkdir($base.'/bootstrap/cache', 0777, true);
        @mkdir($base.'/storage/framework', 0777, true);

        $providers = $nightowlFirst
            ? [NightOwlAgentServiceProvider::class, NightwatchServiceProvider::class]
            : [NightwatchServiceProvider::class, NightOwlAgentServiceProvider::class];

        $app = Application::configure(basePath: $base)
            ->withRouting(using: function () {
                Route::get('/ping', fn () => 'pong');
            })
            ->withExceptions()
            // Registered last, as the host application's own providers are.
            ->withProviders([...$providers, BootTimeRecordsProvider::class])
            ->create();

        $kernel = $app->make(\Illuminate\Contracts\Http\Kernel::class);
        $request = Request::createFromBase(SymfonyRequest::create('/ping', 'GET'));

        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        return $response->getStatusCode();
    }

    /** @param array<string, string> $vars */
    private function setEnv(array $vars): void
    {
        foreach ($vars as $key => $value) {
            putenv("{$key}={$value}");
            $_ENV[$key] = $_SERVER[$key] = $value;
            $this->env[$key] = $value;
        }
    }

    /** How many records of type $type carrying the marker message the agent received. */
    private function markerCount(ScriptedServer $agent, string $type): int
    {
        return $this->recordCount($agent, $type, self::MARKER);
    }

    /** How many records of type $type the agent received, optionally only those whose message holds $marker. */
    private function recordCount(ScriptedServer $agent, string $type, ?string $marker = null): int
    {
        $count = 0;

        foreach ($agent->transcript() as $frame) {
            if ($frame === '') {
                continue;
            }

            // v1:<tokenHash>:<json>
            $records = json_decode(explode(':', $frame, 3)[2] ?? '[]', true);

            foreach (is_array($records) ? $records : [] as $record) {
                if (($record['t'] ?? null) === $type
                    && ($marker === null || str_contains((string) ($record['message'] ?? ''), $marker))) {
                    $count++;
                }
            }
        }

        return $count;
    }
}

/**
 * Stands in for the host application's own provider (AppServiceProvider).
 */
final class BootTimeRecordsProvider extends ServiceProvider
{
    public static ?\Closure $onRegister = null;

    public static ?\Closure $onBoot = null;

    public function register(): void
    {
        if (self::$onRegister !== null) {
            (self::$onRegister)();
        }
    }

    public function boot(): void
    {
        if (self::$onBoot !== null) {
            (self::$onBoot)();
        }
    }
}
