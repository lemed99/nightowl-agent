<?php

namespace NightOwl\Tests\Unit;

use Laravel\Nightwatch\Contracts\Ingest as IngestContract;
use Laravel\Nightwatch\Core;
use Laravel\Nightwatch\Facades\Nightwatch;
use Laravel\Nightwatch\Ingest;
use Laravel\Nightwatch\RecordsBuffer;
use NightOwl\Support\MultiIngest;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use ReflectionProperty;
use RuntimeException;
use Throwable;

final class NightwatchCompatibilityTest extends TestCase
{
    public function test_ingest_constructor_accepts_named_args_used_by_provider(): void
    {
        $params = (new ReflectionClass(Ingest::class))
            ->getConstructor()
            ?->getParameters() ?? [];

        $names = array_map(fn ($p) => $p->getName(), $params);

        foreach (['transmitTo', 'connectionTimeout', 'timeout', 'streamFactory', 'buffer', 'tokenHash'] as $expected) {
            $this->assertContains(
                $expected,
                $names,
                "Laravel\\Nightwatch\\Ingest::__construct no longer accepts '{$expected}'. Provider wiring in NightOwlAgentServiceProvider needs updating."
            );
        }
    }

    public function test_records_buffer_accepts_length_arg(): void
    {
        $params = (new ReflectionClass(RecordsBuffer::class))
            ->getConstructor()
            ?->getParameters() ?? [];

        $names = array_map(fn ($p) => $p->getName(), $params);

        $this->assertContains('length', $names, 'RecordsBuffer::__construct no longer accepts named arg "length".');
    }

    public function test_core_ingest_is_public_mutable_property(): void
    {
        $prop = (new ReflectionClass(Core::class))->getProperty('ingest');

        $this->assertTrue($prop->isPublic(), 'Core::$ingest is no longer public — provider cannot rebind it.');
        $this->assertFalse($prop->isReadOnly(), 'Core::$ingest is now readonly — provider cannot rebind it.');
    }

    public function test_unrecoverable_exception_hook_is_still_reachable(): void
    {
        // MultiIngest forwards every transport failure it swallows here. The
        // method is @internal to Nightwatch, so pin it: without this the SDK
        // renaming it would show up only as customers quietly losing the hook
        // (MultiIngest guards the call, so nothing would break loudly).
        $this->assertTrue(
            method_exists(Nightwatch::class, 'unrecoverableExceptionOccurred'),
            'Nightwatch::unrecoverableExceptionOccurred() is gone — MultiIngest can no longer forward swallowed transport failures to the customer\'s hook.'
        );
    }

    public function test_multi_ingest_implements_contract(): void
    {
        $this->assertInstanceOf(
            IngestContract::class,
            new MultiIngest(),
            'MultiIngest must implement Laravel\\Nightwatch\\Contracts\\Ingest.'
        );
    }

    public function test_multi_ingest_flattens_and_dedupes_to_prevent_duplicate_writes(): void
    {
        $writes = 0;
        $counter = new class($writes) implements IngestContract {
            public function __construct(private int &$count) {}
            public function write(array $record): void { $this->count++; }
            public function writeNow(array $record): void {}
            public function ping(): void {}
            public function shouldDigest(bool $bool = true): void {}
            public function shouldDigestWhenBufferIsFull(bool $bool = true): void {}
            public function digest(): void {}
            public function flush(): void {}
        };

        // Simulate the boot-hook re-wrap: each "wrap" feeds the previous chain
        // back in alongside a freshly-constructed Nightwatch ingest pointing
        // at the same agent socket. Without flatten+dedupe this multiplies.
        $makeNightowl = fn () => new Ingest(
            transmitTo: '127.0.0.1:2407',
            connectionTimeout: 0.5,
            timeout: 0.5,
            streamFactory: fn ($a, $t) => fopen('php://memory', 'r+'),
            buffer: new RecordsBuffer(length: 500),
            tokenHash: 'abc1234',
        );

        $chain = new MultiIngest($counter, $makeNightowl());
        $chain = new MultiIngest($chain, $makeNightowl());
        $chain = new MultiIngest($chain, $makeNightowl());

        $chain->write(['v' => 1]);

        $this->assertSame(1, $writes, 'Re-wrapping MultiIngest must not multiply writes to the same downstream ingest.');
    }

    public function test_multi_ingest_swallows_transport_failures_so_they_cannot_break_the_host_app(): void
    {
        $log = $this->captureErrorLog(function () {
            $ingest = new MultiIngest($this->deadAgent());

            // Every method Nightwatch reaches on a request/command lifecycle.
            // A monitoring package must survive an unreachable agent socket on
            // all of them, not just the one that happened to be reported.
            $ingest->write(['v' => 1]);
            $ingest->writeNow(['v' => 1]);
            $ingest->digest();
            $ingest->flush();
            $ingest->shouldDigest(true);
            $ingest->shouldDigestWhenBufferIsFull(true);
        });

        foreach (['write', 'writeNow', 'digest', 'flush', 'shouldDigest', 'shouldDigestWhenBufferIsFull'] as $method) {
            $this->assertStringContainsString(
                "MultiIngest {$method} failed",
                $log,
                "A swallowed {$method} failure must still reach the operator's error log."
            );
        }
    }

    public function test_multi_ingest_ping_still_reports_an_unreachable_agent(): void
    {
        // `nightwatch:status` decides whether the agent is reachable purely by
        // whether ping() throws. Failing open here makes the one command a
        // customer runs to check connectivity report a healthy agent over a
        // dead socket.
        $ingest = new MultiIngest($this->deadAgent());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Connection refused');

        $ingest->ping();
    }

    public function test_multi_ingest_forwards_swallowed_failures_to_the_nightwatch_hook(): void
    {
        $handler = new ReflectionProperty(Nightwatch::class, 'handleUnrecoverableExceptionsUsing');
        $original = $handler->getValue();

        $seen = [];
        Nightwatch::handleUnrecoverableExceptionsUsing(function (Throwable $e) use (&$seen) {
            $seen[] = $e;
        });

        try {
            $this->captureErrorLog(fn () => (new MultiIngest($this->deadAgent()))->write(['v' => 1]));
        } finally {
            $handler->setValue(null, $original);
        }

        $this->assertCount(
            1,
            $seen,
            'Wrapping the ingest must not unregister Nightwatch::handleUnrecoverableExceptionsUsing() — '.
            'it is the documented hook for observing telemetry transport failures.'
        );
        $this->assertSame('Agent acknowledgment timed out.', $seen[0]->getMessage());
    }

    /**
     * An ingest standing in for an agent whose socket is gone: every transport
     * method throws, exactly as Nightwatch's own Ingest does when the
     * acknowledgment read fails.
     */
    private function deadAgent(): IngestContract
    {
        return new class implements IngestContract
        {
            public function write(array $record): void { throw new RuntimeException('Agent acknowledgment timed out.'); }
            public function writeNow(array $record): void { throw new RuntimeException('Agent acknowledgment timed out.'); }
            public function ping(): void { throw new RuntimeException('Connection refused [tcp://127.0.0.1:2407]'); }
            public function shouldDigest(bool $bool = true): void { throw new RuntimeException('Agent acknowledgment timed out.'); }
            public function shouldDigestWhenBufferIsFull(bool $bool = true): void { throw new RuntimeException('Agent acknowledgment timed out.'); }
            public function digest(): void { throw new RuntimeException('Agent acknowledgment timed out.'); }
            public function flush(): void { throw new RuntimeException('Agent acknowledgment timed out.'); }
        };
    }

    /**
     * Run $fn with error_log() redirected to a temp file, returning what it
     * wrote — the fail-open log lines otherwise land in the test runner's
     * stderr and read like failures.
     */
    private function captureErrorLog(callable $fn): string
    {
        $file = tempnam(sys_get_temp_dir(), 'nightowl-error-log-');
        $previous = ini_get('error_log');
        ini_set('error_log', $file);

        try {
            $fn();
        } finally {
            ini_set('error_log', $previous === false ? '' : $previous);
        }

        $contents = (string) file_get_contents($file);
        @unlink($file);

        return $contents;
    }
}
