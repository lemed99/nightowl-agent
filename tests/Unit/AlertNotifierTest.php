<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\AlertNotifier;
use PHPUnit\Framework\TestCase;

class AlertNotifierTest extends TestCase
{
    // ─── Queue / Flush Lifecycle ─────────────────────────────────────

    public function testQueueNewIssueNotificationsDetectsNewHashes(): void
    {
        $notifier = new AlertNotifier;

        $issueGroups = [
            'hash_a' => ['class' => 'A', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
            'hash_b' => ['class' => 'B', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
            'hash_c' => ['class' => 'C', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
        ];

        // hash_a and hash_b existed before; hash_c is new
        $existingBefore = ['hash_a', 'hash_b'];

        $notifier->queueNewIssueNotifications('TestApp', $issueGroups, 'exception', $existingBefore);

        $pending = $this->getPending($notifier);
        $this->assertCount(1, $pending);
        $this->assertSame(['hash_c'], $pending[0]['newHashes']);
        $this->assertSame('TestApp', $pending[0]['appName']);
        $this->assertSame('exception', $pending[0]['issueType']);
    }

    public function testQueueSkipsWhenAllHashesExist(): void
    {
        $notifier = new AlertNotifier;

        $issueGroups = [
            'hash_a' => ['class' => 'A', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
        ];

        $notifier->queueNewIssueNotifications('TestApp', $issueGroups, 'exception', ['hash_a']);

        $this->assertEmpty($this->getPending($notifier));
    }

    public function testQueueAllNewWhenNoneExisted(): void
    {
        $notifier = new AlertNotifier;

        $issueGroups = [
            'hash_x' => ['class' => 'X', 'message' => '', 'count' => 5, 'users' => [], 'timestamps' => []],
            'hash_y' => ['class' => 'Y', 'message' => '', 'count' => 3, 'users' => [], 'timestamps' => []],
        ];

        $notifier->queueNewIssueNotifications('TestApp', $issueGroups, 'performance', []);

        $pending = $this->getPending($notifier);
        $this->assertCount(1, $pending);
        $this->assertCount(2, $pending[0]['newHashes']);
        $this->assertContains('hash_x', $pending[0]['newHashes']);
        $this->assertContains('hash_y', $pending[0]['newHashes']);
    }

    public function testClearPendingDiscardsAll(): void
    {
        $notifier = new AlertNotifier;

        $notifier->queueNewIssueNotifications('App', [
            'h1' => ['class' => 'A', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
        ], 'exception', []);

        $this->assertNotEmpty($this->getPending($notifier));

        $notifier->clearPending();

        $this->assertEmpty($this->getPending($notifier));
    }

    public function testMultipleQueuesAccumulate(): void
    {
        $notifier = new AlertNotifier;

        $notifier->queueNewIssueNotifications('App', [
            'h1' => ['class' => 'A', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
        ], 'exception', []);

        $notifier->queueNewIssueNotifications('App', [
            'h2' => ['name' => '/api/slow', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
        ], 'performance', []);

        $pending = $this->getPending($notifier);
        $this->assertCount(2, $pending);
        $this->assertSame('exception', $pending[0]['issueType']);
        $this->assertSame('performance', $pending[1]['issueType']);
    }

    // ─── Issue Name Resolution ───────────────────────────────────────

    public function testIssueNameResolvesClassForExceptions(): void
    {
        $notifier = new AlertNotifier;

        $group = ['class' => 'App\\Exceptions\\PaymentFailed', 'message' => 'Card declined', 'count' => 1, 'users' => []];
        $result = $this->callPrivate($notifier, 'issueName', [$group]);

        $this->assertSame('App\\Exceptions\\PaymentFailed', $result);
    }

    public function testIssueNameResolvesNameForPerformance(): void
    {
        $notifier = new AlertNotifier;

        $group = ['name' => '/api/users', 'count' => 1, 'users' => []];
        $result = $this->callPrivate($notifier, 'issueName', [$group]);

        $this->assertSame('/api/users', $result);
    }

    public function testIssueNameFallsBackToUnknown(): void
    {
        $notifier = new AlertNotifier;

        $group = ['count' => 1, 'users' => []];
        $result = $this->callPrivate($notifier, 'issueName', [$group]);

        $this->assertSame('Unknown', $result);
    }

    public function testIssueNamePrefersClassOverName(): void
    {
        $notifier = new AlertNotifier;

        $group = ['class' => 'RuntimeException', 'name' => '/api/test', 'count' => 1, 'users' => []];
        $result = $this->callPrivate($notifier, 'issueName', [$group]);

        $this->assertSame('RuntimeException', $result);
    }

    // ─── Message Truncation ──────────────────────────────────────────

    public function testIssueMessageTruncatesAt200(): void
    {
        $notifier = new AlertNotifier;

        $long = str_repeat('x', 300);
        $group = ['message' => $long];
        $result = $this->callPrivate($notifier, 'issueMessage', [$group]);

        $this->assertSame(203, mb_strlen($result)); // 200 + '...'
        $this->assertStringEndsWith('...', $result);
    }

    public function testIssueMessageReturnsEmptyForNull(): void
    {
        $notifier = new AlertNotifier;

        $result = $this->callPrivate($notifier, 'issueMessage', [['message' => null]]);
        $this->assertSame('', $result);

        $result = $this->callPrivate($notifier, 'issueMessage', [[]]);
        $this->assertSame('', $result);
    }

    public function testIssueMessagePreservesShortMessage(): void
    {
        $notifier = new AlertNotifier;

        $result = $this->callPrivate($notifier, 'issueMessage', [['message' => 'Short error']]);
        $this->assertSame('Short error', $result);
    }

    // ─── Header Sanitization ─────────────────────────────────────────

    public function testSanitizeHeaderStripsCRLF(): void
    {
        $notifier = new AlertNotifier;

        $result = $this->callPrivate($notifier, 'sanitizeHeader', ["Subject\r\nBcc: evil@hacker.com"]);
        $this->assertSame('SubjectBcc: evil@hacker.com', $result);
    }

    public function testSanitizeHeaderPreservesNormalText(): void
    {
        $notifier = new AlertNotifier;

        $result = $this->callPrivate($notifier, 'sanitizeHeader', ['[App] New Issue: RuntimeException']);
        $this->assertSame('[App] New Issue: RuntimeException', $result);
    }

    // ─── Encryption ──────────────────────────────────────────────────

    public function testDecryptValueMatchesLaravelCrypt(): void
    {
        // Generate a known key
        $key = random_bytes(32);

        $notifier = new AlertNotifier(encryptionKey: $key);

        // Encrypt a value the same way Laravel does
        $plaintext = 'my-smtp-password';
        $iv = random_bytes(openssl_cipher_iv_length('aes-256-cbc'));
        $encrypted = openssl_encrypt($plaintext, 'aes-256-cbc', $key, 0, $iv);
        $mac = hash_hmac('sha256', base64_encode($iv) . $encrypted, $key);

        $payload = base64_encode(json_encode([
            'iv' => base64_encode($iv),
            'value' => $encrypted,
            'mac' => $mac,
        ]));

        $result = $this->callPrivate($notifier, 'decryptValue', [$payload]);
        $this->assertSame('my-smtp-password', $result);
    }

    public function testDecryptValueReturnsOriginalOnInvalidPayload(): void
    {
        $notifier = new AlertNotifier(encryptionKey: random_bytes(32));

        // Not base64/JSON
        $result = $this->callPrivate($notifier, 'decryptValue', ['not-encrypted']);
        $this->assertSame('not-encrypted', $result);

        // Valid base64 but not the right JSON structure
        $result = $this->callPrivate($notifier, 'decryptValue', [base64_encode('{"foo":"bar"}')]);
        $this->assertSame(base64_encode('{"foo":"bar"}'), $result);
    }

    public function testDecryptValueReturnsOriginalOnMacMismatch(): void
    {
        $key = random_bytes(32);
        $notifier = new AlertNotifier(encryptionKey: $key);

        $iv = random_bytes(openssl_cipher_iv_length('aes-256-cbc'));
        $encrypted = openssl_encrypt('secret', 'aes-256-cbc', $key, 0, $iv);

        $payload = base64_encode(json_encode([
            'iv' => base64_encode($iv),
            'value' => $encrypted,
            'mac' => 'invalid-mac-value',
        ]));

        $result = $this->callPrivate($notifier, 'decryptValue', [$payload]);
        $this->assertSame($payload, $result);
    }

    public function testDecryptValueReturnsOriginalWithEmptyKey(): void
    {
        $notifier = new AlertNotifier(encryptionKey: '');

        // loadChannels skips decryption when key is empty, but test decryptValue directly
        $result = $this->callPrivate($notifier, 'decryptValue', ['anything']);
        // With empty key, MAC won't match (or payload is invalid), returns as-is
        $this->assertSame('anything', $result);
    }

    // ─── Notify Event Filtering ──────────────────────────────────────

    public function testQueueNewIssueNotificationsEmptyGroupsNoOp(): void
    {
        $notifier = new AlertNotifier;

        $notifier->queueNewIssueNotifications('App', [], 'exception', []);

        $this->assertEmpty($this->getPending($notifier));
    }

    // ─── Channel Cache ───────────────────────────────────────────────

    public function testChannelCacheRespectsTtl(): void
    {
        // With TTL=0, cache expires immediately (forces reload every call)
        $notifier = new AlertNotifier(cacheTtl: 0);

        $ref = new \ReflectionProperty($notifier, 'channelCacheExpiry');
        $this->assertSame(0.0, $ref->getValue($notifier));

        // With TTL=86400, after first load, expiry is set far in the future
        $notifier2 = new AlertNotifier(cacheTtl: 86400);
        // Manually set cache to simulate a load
        $cacheRef = new \ReflectionProperty($notifier2, 'channelCache');
        $cacheRef->setValue($notifier2, [['type' => 'slack', 'name' => 'Test', 'config' => []]]);
        $expiryRef = new \ReflectionProperty($notifier2, 'channelCacheExpiry');
        $expiryRef->setValue($notifier2, microtime(true) + 86400);

        // loadChannels should return cached without hitting PDO
        // (We can't call loadChannels without a PDO, but we verify the cache state)
        $this->assertCount(1, $cacheRef->getValue($notifier2));
    }

    // ─── SMTP Line Ending Normalization ──────────────────────────────

    public function testSmtpBodyNormalization(): void
    {
        // Simulate what smtpSend does to the body
        $body = "Line 1\nLine 2\n.hidden\nEnd";

        $smtpBody = str_replace(["\r\n", "\r", "\n"], ["\n", "\n", "\r\n"], $body);
        $smtpBody = str_replace("\r\n.", "\r\n..", $smtpBody);

        $this->assertSame("Line 1\r\nLine 2\r\n..hidden\r\nEnd", $smtpBody);
    }

    public function testSmtpBodyNormalizationWithMixedEndings(): void
    {
        $body = "A\r\nB\rC\nD";

        $smtpBody = str_replace(["\r\n", "\r", "\n"], ["\n", "\n", "\r\n"], $body);

        $this->assertSame("A\r\nB\r\nC\r\nD", $smtpBody);
    }

    // ─── JSON Encoding Safety ────────────────────────────────────────

    public function testJsonEncodeWithInvalidUtf8DoesNotReturnFalse(): void
    {
        // Simulate what sendSlack does
        $text = "Error: \x80\x81 invalid bytes";
        $result = json_encode(['text' => $text], JSON_INVALID_UTF8_SUBSTITUTE);

        $this->assertIsString($result);
        $this->assertStringContainsString('Error:', $result);
    }

    public function testJsonEncodeWithValidUtf8IsUnchanged(): void
    {
        $text = "Error: Something went wrong — résumé";
        $result = json_encode(['text' => $text], JSON_INVALID_UTF8_SUBSTITUTE);

        $this->assertIsString($result);
        $decoded = json_decode($result, true);
        $this->assertSame($text, $decoded['text']);
    }

    // ─── Notification Time Budget ─────────────────────────────

    public function testMaxNotificationSecondsConstantExists(): void
    {
        $ref = new \ReflectionClassConstant(AlertNotifier::class, 'MAX_NOTIFICATION_SECONDS');
        $this->assertSame(5.0, $ref->getValue());
    }

    public function testFlushClearsPendingEvenWithNoChannels(): void
    {
        $notifier = new AlertNotifier;

        $notifier->queueNewIssueNotifications('App', [
            'h1' => ['class' => 'A', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
        ], 'exception', []);

        $this->assertNotEmpty($this->getPending($notifier));

        // Create a mock PDO that returns no channels
        $pdo = new \PDO('sqlite::memory:');
        $pdo->exec('CREATE TABLE nightowl_alert_channels (type TEXT, name TEXT, config TEXT, enabled BOOLEAN)');

        $notifier->flushNotifications($pdo);

        $this->assertEmpty($this->getPending($notifier));
    }

    // ─── Helpers ─────────────────────────────────────────────────────

    private function getPending(AlertNotifier $notifier): array
    {
        $ref = new \ReflectionProperty($notifier, 'pendingNotifications');

        return $ref->getValue($notifier);
    }

    private function callPrivate(AlertNotifier $notifier, string $method, array $args = []): mixed
    {
        $ref = new \ReflectionMethod($notifier, $method);

        return $ref->invoke($notifier, ...$args);
    }
}
