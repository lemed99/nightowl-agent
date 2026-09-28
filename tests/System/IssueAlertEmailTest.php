<?php

namespace NightOwl\Tests\System;

use NightOwl\Agent\AlertNotifier;
use NightOwl\Agent\SmtpClient;
use NightOwl\Tests\Fixtures\ScriptedServer;
use PHPUnit\Framework\TestCase;

/**
 * The drain's issue email as the relay receives it.
 *
 * AlertNotifier built an event-aware subject ("Reopened Issue") but rendered
 * the body without the event, so a reopened issue arrived with a "New Issue"
 * or "Performance Alert" badge under a "Reopened" subject. These drive the
 * real sendEmail() into a scripted SMTP server and read the badge off the wire.
 */
class IssueAlertEmailTest extends TestCase
{
    /** @var list<ScriptedServer> */
    private array $servers = [];

    protected function tearDown(): void
    {
        foreach ($this->servers as $server) {
            $server->cleanup();
        }

        $this->servers = [];
    }

    public function test_a_reopened_exception_email_carries_the_reopened_badge(): void
    {
        [$subject, $wire] = $this->send('Reopened Issue', ['class' => 'RuntimeException', 'message' => 'boom'], 'exception');

        $this->assertSame('Subject: [AcmeApp] Reopened Issue: RuntimeException', $subject);
        $this->assertStringContainsString('>Reopened</td>', $wire);
        $this->assertStringNotContainsString('>New Issue</td>', $wire);
    }

    public function test_a_reopened_performance_email_carries_the_reopened_badge(): void
    {
        [$subject, $wire] = $this->send('Reopened Issue', [
            'name' => 'GET /users',
            'subtype' => 'route',
            'threshold_ms' => 500,
            'duration_ms' => 900,
        ], 'performance');

        $this->assertSame('Subject: [AcmeApp] Reopened Performance Alert: GET /users', $subject);
        $this->assertStringContainsString('>Reopened</td>', $wire);
        $this->assertStringNotContainsString('>Performance Alert</td>', $wire);
    }

    public function test_a_new_issue_email_keeps_the_new_issue_badge(): void
    {
        [$subject, $wire] = $this->send('New Issue', ['class' => 'RuntimeException', 'message' => 'boom'], 'exception');

        $this->assertSame('Subject: [AcmeApp] New Issue: RuntimeException', $subject);
        $this->assertStringContainsString('>New Issue</td>', $wire);
        $this->assertStringNotContainsString('>Reopened</td>', $wire);
    }

    /**
     * @return array{0: string|null, 1: string} the Subject line and the decoded message body
     */
    private function send(string $prefix, array $group, string $issueType): array
    {
        $server = ScriptedServer::start('fake-smtp-server.php');
        $this->servers[] = $server;

        $notifier = new AlertNotifier(86400, 'https://dash.example', 'app_123', 0, new SmtpClient(null, 2.0, 2.0));

        (new \ReflectionMethod(AlertNotifier::class, 'sendEmail'))->invoke(
            $notifier,
            [
                'host' => '127.0.0.1',
                'port' => $server->port,
                'encryption' => 'none',
                'from_address' => 'alerts@acme.test',
                'to_addresses' => ['ops@acme.test'],
            ],
            'AcmeApp',
            $prefix,
            $group + ['count' => 2, 'users_count' => 0, 'issue_id' => 7],
            $issueType,
            null,
        );

        $transcript = $server->transcript();
        $subject = null;
        foreach ($transcript as $line) {
            if (str_starts_with($line, 'Subject: ')) {
                $subject = $line;
            }
        }

        // The body goes out quoted-printable; read it as a mail client would.
        $data = array_search('DATA', $transcript, true);
        $lines = array_slice($transcript, $data === false ? 0 : $data + 1);
        $start = array_search('', $lines, true);
        $end = array_search('.', $lines, true);
        $body = array_slice($lines, $start + 1, ($end === false ? count($lines) : $end) - $start - 1);
        $html = quoted_printable_decode(implode("\r\n", array_map(
            static fn (string $line): string => str_starts_with($line, '..') ? substr($line, 1) : $line,
            $body,
        )));

        return [$subject, $html];
    }
}
