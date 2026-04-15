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
}
