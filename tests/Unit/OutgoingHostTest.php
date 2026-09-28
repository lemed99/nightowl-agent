<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\OutgoingHost;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Every spelling a host threshold can be saved in reduces to the host the SDK
 * records ($uri->getHost(): lowercase, no port).
 *
 * nightowl-api's PerformanceThresholds::normalizeHost() is held to the same
 * table by its own test; change both together.
 */
class OutgoingHostTest extends TestCase
{
    /** @return array<string, array{string, string}> */
    public static function spellings(): array
    {
        return [
            'recorded host' => ['api.kettlepay.test', 'api.kettlepay.test'],
            'dashboard form' => ['https://api.kettlepay.test', 'api.kettlepay.test'],
            'http' => ['http://api.kettlepay.test', 'api.kettlepay.test'],
            'mixed case' => ['HTTPS://API.KettlePay.test', 'api.kettlepay.test'],
            'trailing slash' => ['https://api.kettlepay.test/', 'api.kettlepay.test'],
            'bare with trailing slash' => ['api.kettlepay.test/', 'api.kettlepay.test'],
            'surrounding space' => ['  https://api.kettlepay.test  ', 'api.kettlepay.test'],
            'full url' => ['https://api.kettlepay.test/v1/charges?x=1#y', 'api.kettlepay.test'],
            'query without path' => ['https://api.kettlepay.test?x=1', 'api.kettlepay.test'],
            'default port' => ['https://api.kettlepay.test:443', 'api.kettlepay.test'],
            'other port' => ['http://localhost:8080', 'localhost'],
            'bare with port' => ['localhost:8080', 'localhost'],
            'userinfo' => ['https://user:secret@api.kettlepay.test', 'api.kettlepay.test'],
            'scheme-relative' => ['//api.kettlepay.test', 'api.kettlepay.test'],
            'ipv6 with port' => ['http://[::1]:8080/health', '[::1]'],
            'ipv6 bracketed' => ['[::1]', '[::1]'],
            'ipv6 bare' => ['::1', '[::1]'],
            'ipv4 with port' => ['http://10.0.0.5:9200', '10.0.0.5'],
        ];
    }

    #[DataProvider('spellings')]
    public function test_a_spelling_reduces_to_the_recorded_host(string $saved, string $host): void
    {
        $this->assertSame($host, OutgoingHost::normalize($saved));
    }

    public function test_different_hosts_stay_different(): void
    {
        $this->assertNotSame(
            OutgoingHost::normalize('https://api.kettlepay.test'),
            OutgoingHost::normalize('https://kettlepay.test'),
        );
        $this->assertNotSame(
            OutgoingHost::normalize('https://api.kettlepay.test'),
            OutgoingHost::normalize('https://api.kettlepay.test.example.com'),
        );
    }
}
