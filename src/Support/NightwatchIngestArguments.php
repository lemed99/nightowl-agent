<?php

namespace NightOwl\Support;

use Closure;
use ReflectionClass;

/**
 * The constructor arguments for Nightwatch's Ingest, across the 1.29 change.
 *
 * laravel/nightwatch 1.29.0 added a required `Dispatcher $events` to
 * Ingest::__construct (the IngestingEvents veto hook). Our constraint `^1.26`
 * admitted it while the lock and CI stayed on 1.26, so the first customer to
 * `composer update` past it had package:discover die with an
 * ArgumentCountError from NightOwlAgentServiceProvider (BrokerCentral,
 * 2026-09-10). The provider builds its argument list here, and the dispatcher
 * is supplied only when the installed constructor declares it — 1.26 through
 * 1.28 do not accept the name at all.
 */
final class NightwatchIngestArguments
{
    /**
     * @param  class-string  $class
     * @param  array<string, mixed>  $arguments  named arguments the provider always passes
     * @param  Closure(): object  $events  builds the dispatcher, called only when needed
     * @return array<string, mixed>
     */
    public static function complete(string $class, array $arguments, Closure $events): array
    {
        $constructor = (new ReflectionClass($class))->getConstructor();

        foreach ($constructor?->getParameters() ?? [] as $parameter) {
            if ($parameter->getName() === 'events' && ! array_key_exists('events', $arguments)) {
                $arguments['events'] = $events();
            }
        }

        return $arguments;
    }
}
