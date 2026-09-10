<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\NightwatchIngestArguments;
use PHPUnit\Framework\TestCase;

final class SixArgIngest
{
    public function __construct(
        public string $transmitTo,
        public float $connectionTimeout,
        public float $timeout,
        public mixed $streamFactory,
        public mixed $buffer,
        public string $tokenHash,
    ) {}
}

final class SevenArgIngest
{
    public function __construct(
        public string $transmitTo,
        public float $connectionTimeout,
        public float $timeout,
        public mixed $streamFactory,
        public mixed $buffer,
        public string $tokenHash,
        public object $events,
    ) {}
}

/**
 * laravel/nightwatch 1.26-1.28 construct Ingest with six arguments, 1.29+ with
 * a seventh required `events`. The provider's list must construct BOTH shapes,
 * and must not build a dispatcher for a constructor that cannot take one.
 */
final class NightwatchIngestArgumentsTest extends TestCase
{
    private function base(): array
    {
        return [
            'transmitTo' => '127.0.0.1:2407',
            'connectionTimeout' => 0.5,
            'timeout' => 0.5,
            'streamFactory' => new \stdClass,
            'buffer' => new \stdClass,
            'tokenHash' => 'abc',
        ];
    }

    public function test_a_pre_1_29_constructor_gets_the_six_arguments_and_no_dispatcher_is_built(): void
    {
        $built = 0;
        $args = NightwatchIngestArguments::complete(SixArgIngest::class, $this->base(), function () use (&$built) {
            $built++;

            return new \stdClass;
        });

        $ingest = new SixArgIngest(...$args);
        $this->assertSame('127.0.0.1:2407', $ingest->transmitTo);
        $this->assertArrayNotHasKey('events', $args);
        $this->assertSame(0, $built, 'no dispatcher is resolved for a constructor that cannot take one');
    }

    public function test_a_1_29_constructor_gets_the_dispatcher(): void
    {
        $dispatcher = new \stdClass;
        $args = NightwatchIngestArguments::complete(SevenArgIngest::class, $this->base(), fn () => $dispatcher);

        $ingest = new SevenArgIngest(...$args);
        $this->assertSame($dispatcher, $ingest->events);
        $this->assertSame('abc', $ingest->tokenHash);
    }

    public function test_an_explicit_events_argument_is_not_overwritten(): void
    {
        $mine = new \stdClass;
        $args = NightwatchIngestArguments::complete(SevenArgIngest::class, $this->base() + ['events' => $mine], fn () => new \stdClass);

        $this->assertSame($mine, (new SevenArgIngest(...$args))->events);
    }
}
