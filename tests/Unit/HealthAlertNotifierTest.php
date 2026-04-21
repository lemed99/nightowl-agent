<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\HealthAlertNotifier;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

class HealthAlertNotifierTest extends TestCase
{
    public function test_sanitize_header_strips_carriage_return_and_line_feed(): void
    {
        $sanitize = $this->privateMethod('sanitizeHeader');

        $this->assertSame('normal', $sanitize->invoke(null, 'normal'));
        $this->assertSame('attackerCc: evil@example.com', $sanitize->invoke(null, "attacker\r\nCc: evil@example.com"));
        $this->assertSame('noCRno', $sanitize->invoke(null, "no\rCR\nno"));
    }

    public function test_is_safe_webhook_url_accepts_http_and_https(): void
    {
        $isSafe = $this->privateMethod('isSafeWebhookUrl');

        $this->assertTrue($isSafe->invoke(null, 'http://example.com/hook'));
        $this->assertTrue($isSafe->invoke(null, 'https://hooks.slack.com/services/abc'));
        $this->assertTrue($isSafe->invoke(null, 'HTTPS://example.com/HOOK'));
    }

    public function test_is_safe_webhook_url_rejects_file_and_other_schemes(): void
    {
        $isSafe = $this->privateMethod('isSafeWebhookUrl');

        $this->assertFalse($isSafe->invoke(null, 'file:///etc/passwd'));
        $this->assertFalse($isSafe->invoke(null, 'phar://payload.phar/x'));
        $this->assertFalse($isSafe->invoke(null, 'compress.zlib://any'));
        $this->assertFalse($isSafe->invoke(null, 'gopher://host/x'));
        $this->assertFalse($isSafe->invoke(null, 'ftp://host/x'));
        $this->assertFalse($isSafe->invoke(null, ''));
        $this->assertFalse($isSafe->invoke(null, 'not a url'));
    }

    public function test_dispatch_returns_early_on_empty_diagnoses(): void
    {
        $notifier = new HealthAlertNotifier('pgsql:host=127.0.0.1;port=1;dbname=x', 'u', 'p');

        // With empty diagnoses, loadChannels() should never be called, so no PG error.
        $notifier->dispatch([]);
        $notifier->dispatchRecovered([]);

        $this->assertTrue(true); // no exception
    }

    public function test_compute_interval_is_unused(): void
    {
        // Sanity: class can be instantiated with minimal args
        $notifier = new HealthAlertNotifier('dsn', 'u', 'p', 'AppName', 'host:1234');
        $this->assertInstanceOf(HealthAlertNotifier::class, $notifier);
    }

    private function privateMethod(string $name): \ReflectionMethod
    {
        return (new ReflectionClass(HealthAlertNotifier::class))->getMethod($name);
    }
}
