<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\HealthAlertNotifier;
use PHPUnit\Framework\TestCase;

class HealthAlertNotifierTest extends TestCase
{
    // Header sanitization moved to SmtpClient, and scheme rejection / status
    // checking / URL redaction moved to WebhookClient, when this class and
    // AlertNotifier stopped carrying a private copy of each transport. See
    // SmtpClientTest, WebhookClientTest, and tests/System/*ConversationTest.

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
}
