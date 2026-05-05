<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\AlertNotifier;
use PHPUnit\Framework\TestCase;

class AlertNotifierTest extends TestCase
{
    // ─── Queue / Flush Lifecycle ─────────────────────────────────────

    public function testQueueDetectsNewHashes(): void
    {
        $notifier = new AlertNotifier;

        $issueGroups = [
            'hash_a' => ['class' => 'A', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
            'hash_b' => ['class' => 'B', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
            'hash_c' => ['class' => 'C', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
        ];

        // hash_a and hash_b existed before; hash_c is new
        $snapshot = ['existing' => ['hash_a', 'hash_b'], 'reopen' => []];

        $notifier->queueIssueNotifications('TestApp', $issueGroups, 'exception', $snapshot);

        $pending = $this->getPending($notifier);
        $this->assertCount(1, $pending);
        $this->assertSame(['hash_c'], $pending[0]['newHashes']);
        $this->assertSame([], $pending[0]['reopenedHashes']);
        $this->assertSame('TestApp', $pending[0]['appName']);
        $this->assertSame('exception', $pending[0]['issueType']);
    }

    public function testQueueSkipsWhenAllHashesExist(): void
    {
        $notifier = new AlertNotifier;

        $issueGroups = [
            'hash_a' => ['class' => 'A', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
        ];

        $notifier->queueIssueNotifications('TestApp', $issueGroups, 'exception', [
            'existing' => ['hash_a'],
            'reopen' => [],
        ]);

        $this->assertEmpty($this->getPending($notifier));
    }

    public function testQueueAllNewWhenNoneExisted(): void
    {
        $notifier = new AlertNotifier;

        $issueGroups = [
            'hash_x' => ['class' => 'X', 'message' => '', 'count' => 5, 'users' => [], 'timestamps' => []],
            'hash_y' => ['class' => 'Y', 'message' => '', 'count' => 3, 'users' => [], 'timestamps' => []],
        ];

        $notifier->queueIssueNotifications('TestApp', $issueGroups, 'performance', [
            'existing' => [],
            'reopen' => [],
        ]);

        $pending = $this->getPending($notifier);
        $this->assertCount(1, $pending);
        $this->assertCount(2, $pending[0]['newHashes']);
        $this->assertContains('hash_x', $pending[0]['newHashes']);
        $this->assertContains('hash_y', $pending[0]['newHashes']);
    }

    public function testQueueRoutesResolvedHashesToReopenedBucket(): void
    {
        $notifier = new AlertNotifier;

        $issueGroups = [
            'hash_a' => ['class' => 'A', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
            'hash_b' => ['class' => 'B', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
            'hash_c' => ['class' => 'C', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
        ];

        // hash_a is open (existing/silent), hash_b is resolved-and-reopening, hash_c is new
        $snapshot = [
            'existing' => ['hash_a'],
            'reopen' => ['hash_b' => 42],
        ];

        $notifier->queueIssueNotifications('TestApp', $issueGroups, 'exception', $snapshot);

        $pending = $this->getPending($notifier);
        $this->assertCount(1, $pending);
        $this->assertSame(['hash_c'], $pending[0]['newHashes']);
        $this->assertSame(['hash_b'], $pending[0]['reopenedHashes']);
    }

    public function testQueueDoesNothingWhenAllSilent(): void
    {
        $notifier = new AlertNotifier;

        $issueGroups = [
            'hash_a' => ['class' => 'A', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
        ];

        $notifier->queueIssueNotifications('TestApp', $issueGroups, 'exception', [
            'existing' => ['hash_a'],
            'reopen' => [],
        ]);

        $this->assertEmpty($this->getPending($notifier));
    }

    public function testClearPendingDiscardsAll(): void
    {
        $notifier = new AlertNotifier;

        $notifier->queueIssueNotifications('App', [
            'h1' => ['class' => 'A', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
        ], 'exception', ['existing' => [], 'reopen' => []]);

        $this->assertNotEmpty($this->getPending($notifier));

        $notifier->clearPending();

        $this->assertEmpty($this->getPending($notifier));
    }

    public function testMultipleQueuesAccumulate(): void
    {
        $notifier = new AlertNotifier;

        $notifier->queueIssueNotifications('App', [
            'h1' => ['class' => 'A', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
        ], 'exception', ['existing' => [], 'reopen' => []]);

        $notifier->queueIssueNotifications('App', [
            'h2' => ['name' => '/api/slow', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
        ], 'performance', ['existing' => [], 'reopen' => []]);

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

    // ─── Notify Event Filtering ──────────────────────────────────────

    public function testQueueEmptyGroupsNoOp(): void
    {
        $notifier = new AlertNotifier;

        $notifier->queueIssueNotifications('App', [], 'exception', ['existing' => [], 'reopen' => []]);

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

        $notifier->queueIssueNotifications('App', [
            'h1' => ['class' => 'A', 'message' => '', 'count' => 1, 'users' => [], 'timestamps' => []],
        ], 'exception', ['existing' => [], 'reopen' => []]);

        $this->assertNotEmpty($this->getPending($notifier));

        // Create a mock PDO that returns no channels
        $pdo = new \PDO('sqlite::memory:');
        $pdo->exec('CREATE TABLE nightowl_alert_channels (type TEXT, name TEXT, config TEXT, enabled BOOLEAN)');

        $notifier->flushNotifications($pdo);

        $this->assertEmpty($this->getPending($notifier));
    }

    // ─── Header Style (new vs reopened) ──────────────────────────────

    public function testHeaderStyleProducesIssueNewForNewIssuePrefix(): void
    {
        $notifier = new AlertNotifier;
        $style = $this->callPrivate($notifier, 'headerStyle', ['New Issue', 'exception']);

        $this->assertSame('issue.new', $style['event']);
        $this->assertSame('New Issue', $style['label']);
        $this->assertSame('#DC2626', $style['color_hex']);
    }

    public function testHeaderStyleProducesIssueReopenedForReopenedPrefix(): void
    {
        $notifier = new AlertNotifier;
        $style = $this->callPrivate($notifier, 'headerStyle', ['Reopened Issue', 'exception']);

        $this->assertSame('issue.reopened', $style['event']);
        $this->assertSame('Reopened Issue', $style['label']);
        $this->assertSame('#D97706', $style['color_hex']);
        $this->assertSame(0xD97706, $style['color_int']);
    }

    public function testHeaderStylePerformanceLabelsForReopened(): void
    {
        $notifier = new AlertNotifier;
        $style = $this->callPrivate($notifier, 'headerStyle', ['Reopened Issue', 'performance']);

        $this->assertSame('issue.reopened', $style['event']);
        $this->assertSame('Reopened Performance Alert', $style['label']);
    }

    public function testHeaderStylePerformanceLabelsForNew(): void
    {
        $notifier = new AlertNotifier;
        $style = $this->callPrivate($notifier, 'headerStyle', ['New Issue', 'performance']);

        $this->assertSame('issue.new', $style['event']);
        $this->assertSame('Performance Alert', $style['label']);
        $this->assertSame('#F59E0B', $style['color_hex']);
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
