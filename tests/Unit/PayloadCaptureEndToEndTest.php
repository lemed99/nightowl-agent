<?php

namespace NightOwl\Tests\Unit;

use Illuminate\Foundation\Application;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Facade;
use Illuminate\Support\Facades\Route;
use Laravel\Nightwatch\Contracts\Ingest;
use Laravel\Nightwatch\Core;
use Laravel\Nightwatch\NightwatchServiceProvider;
use NightOwl\NightOwlAgentServiceProvider;
use NightOwl\Support\CapturingIngest;
use NightOwl\Support\MultiIngest;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request as SymfonyRequest;

/**
 * Capture against the REAL installed Nightwatch, not a stand-in: a real Laravel
 * application, both providers booted, a request through the HTTP kernel and
 * terminate. This is the only test that proves the two hooks PayloadCapture
 * relies on — the redactRequests() callback and RequestHandled — fire in the
 * order that pairs them with the request record Nightwatch writes. A stand-in
 * Core would pass whatever that order really is.
 */
final class PayloadCaptureEndToEndTest extends TestCase
{
    private const ENV = [
        'NIGHTWATCH_TOKEN' => 'test-token',
        // Nightwatch picks request vs command state by runningInConsole(); PHPUnit is a console.
        'NIGHTWATCH_FORCE_REQUEST' => '1',
    ];

    protected function setUp(): void
    {
        foreach (self::ENV as $k => $v) {
            putenv("{$k}={$v}");
            $_ENV[$k] = $_SERVER[$k] = $v;
        }
    }

    protected function tearDown(): void
    {
        foreach (array_keys(self::ENV) as $k) {
            putenv($k);
            unset($_ENV[$k], $_SERVER[$k]);
        }
        Facade::clearResolvedInstances();
        // The booted app installed Laravel's error/exception handlers.
        \Illuminate\Foundation\Bootstrap\HandleExceptions::flushState();
    }

    public function test_a_200_json_request_carries_its_payload_and_redacted_response(): void
    {
        $record = $this->requestRecord(['request_payload' => true, 'response_body' => true]);

        $this->assertSame(200, $record['status_code']);
        $this->assertSame(
            ['email' => 'a@b.c', 'password' => '[7 bytes redacted]', '_nightwatch_files' => []],
            json_decode($record['payload'], true),
            'a non-500 payload must be captured, with Nightwatch\'s own redaction list applied',
        );
        $this->assertSame(
            ['ok' => true, 'access_token' => '[6 bytes redacted]', 'user' => ['name' => 'Ada']],
            json_decode($record['response_body'], true),
        );
    }

    public function test_a_customer_redact_requests_callback_still_applies(): void
    {
        $record = $this->requestRecord(
            ['request_payload' => true],
            fn (Core $core) => \Laravel\Nightwatch\Facades\Nightwatch::redactRequests(
                fn ($r) => $r->payload->set('email', '[customer-redacted]'),
            ),
        );

        $this->assertSame('[customer-redacted]', json_decode($record['payload'], true)['email']);
    }

    public function test_a_request_nightwatch_sampled_out_is_not_captured(): void
    {
        $record = $this->requestRecord(
            ['request_payload' => true, 'response_body' => true],
            fn (Core $core) => $core->dontSample(),
        );

        $this->assertSame('', $record['payload'], 'a discarded request must not pay for capture');
        $this->assertArrayNotHasKey('response_body', $record);
    }

    public function test_the_status_filter_excludes_both_halves(): void
    {
        $record = $this->requestRecord(['request_payload' => true, 'response_body' => true, 'status_codes' => '5xx']);

        $this->assertSame('', $record['payload']);
        $this->assertArrayNotHasKey('response_body', $record);
    }

    public function test_capture_left_unset_leaves_nightwatchs_record_alone(): void
    {
        $record = $this->requestRecord([]);

        $this->assertSame('', $record['payload'], 'Nightwatch only captures 500s');
        $this->assertArrayNotHasKey('response_body', $record);
    }

    /**
     * Boot a real app, send POST /api/login through the kernel and terminate,
     * and return the request record as it reached the NightOwl-bound ingest's
     * buffer (the last stop before the socket).
     *
     * @param  array<string, mixed>  $capture
     * @return array<string, mixed>
     */
    private function requestRecord(array $capture, ?\Closure $beforeRequest = null): array
    {
        $base = sys_get_temp_dir().'/nightowl-capture-e2e-'.getmypid();
        @mkdir($base.'/bootstrap/cache', 0777, true);
        @mkdir($base.'/storage/framework', 0777, true);

        $app = Application::configure(basePath: $base)
            ->withRouting(using: function () {
                Route::post('/api/login', fn () => response()->json([
                    'ok' => true, 'access_token' => 'sekret', 'user' => ['name' => 'Ada'],
                ]))->name('login');
            })
            ->withProviders([NightwatchServiceProvider::class, NightOwlAgentServiceProvider::class])
            ->create();

        $app->booting(function ($app) use ($capture) {
            $app['config']->set('app.key', 'base64:'.base64_encode(str_repeat('k', 32)));
            $app['config']->set('nightowl.capture', $capture);
        });

        $kernel = $app->make(\Illuminate\Contracts\Http\Kernel::class);
        $request = Request::createFromBase(SymfonyRequest::create(
            '/api/login', 'POST', [], [], [], ['CONTENT_TYPE' => 'application/json'],
            json_encode(['email' => 'a@b.c', 'password' => 'hunter2']),
        ));
        $response = $kernel->handle($request);

        $core = $app->make(Core::class);
        if ($beforeRequest !== null) {
            // Runs between handle and terminate, i.e. before the record is
            // built. A redactRequests callback registered here lands AFTER
            // NightOwl's own — the worst case for ordering.
            $beforeRequest($core);
        }

        $this->assertInstanceOf(MultiIngest::class, $core->ingest);
        $target = (new \ReflectionClass($core->ingest))->getProperty('ingests')->getValue($core->ingest)[0];

        // Keep the records off the socket, without reaching into Nightwatch's
        // internals: re-wrap the capture the provider built around a recorder,
        // so the records arrive exactly as CapturingIngest hands them on. The
        // Ingest CONTRACT is all this touches, and that is identical across the
        // ^1.26 range — Nightwatch's own RecordsBuffer is not: reading
        // `buffer->all()` here passed on the locked 1.30 and was an undefined
        // method on 1.26, which the SDK swallowed as a reported exception.
        $recorder = new class implements Ingest
        {
            /** @var list<array<mixed>> */
            public array $records = [];

            public function write(array $record): void
            {
                $this->records[] = $record;
            }

            public function writeNow(array $record): void
            {
                $this->records[] = $record;
            }

            public function ping(): void {}

            public function shouldDigest(bool $bool = true): void {}

            public function shouldDigestWhenBufferIsFull(bool $bool = true): void {}

            public function digest(): void {}

            public function flush(): void {}
        };
        // With capture off entirely the provider leaves the ingest unwrapped —
        // that IS the assertion of the unset test, so record straight from it.
        $core->ingest = $target instanceof CapturingIngest
            ? new CapturingIngest($recorder, (new \ReflectionClass($target))->getProperty('capture')->getValue($target))
            : $recorder;

        $kernel->terminate($request, $response);

        foreach ($recorder->records as $record) {
            if (($record['t'] ?? null) === 'request') {
                return $record;
            }
        }

        $this->fail('Nightwatch wrote no request record');
    }
}
