<?php

namespace NightOwl\Tests\Unit;

use Laravel\Nightwatch\Contracts\Ingest as IngestContract;
use Laravel\Nightwatch\Core;
use Laravel\Nightwatch\Ingest;
use Laravel\Nightwatch\RecordsBuffer;
use NightOwl\Support\MultiIngest;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

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
}
