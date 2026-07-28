<?php

namespace NightOwl\Tests\System;

use NightOwl\Agent\SmtpClient;
use NightOwl\Tests\Fixtures\ScriptedServer;
use PHPUnit\Framework\TestCase;

/**
 * SmtpClient against a real socket speaking real SMTP.
 *
 * Nothing else in the product exercises this transport. nightowl-api sends the
 * same channel config through Symfony Mailer — that is what the dashboard's
 * "send test" button and every triage alert use — while `issue.new`,
 * `issue.reopened` and the health alerts go out through this client and only
 * this client. A customer can therefore test green, receive triage mail, and
 * never get a drain-dispatched alert, with an error_log line as the only trace.
 *
 * So these assertions are on the WIRE, not on the return value: the scripted
 * server (tests/Fixtures/fake-smtp-server.php) records every line the client
 * sends. A send that "succeeds" while greeting the relay with a name it would
 * refuse, or omitting headers that get the message filed as spam, is the exact
 * failure this suite exists to catch — and it is invisible from the API side.
 *
 * No PostgreSQL and no pcntl: a subprocess and a loopback socket.
 */
class SmtpClientConversationTest extends TestCase
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

    // ─── Harness ─────────────────────────────────────────────────────

    /**
     * Run one send against a scripted server and return what the server heard.
     *
     * @param  array<string, mixed>  $script  Server behaviour — see the fixture's docblock
     * @param  array<string, mixed>  $config  Merged over a working email-channel config
     * @return array{transcript: list<string>, error: string|null}
     */
    private function converse(array $script = [], array $config = [], ?SmtpClient $client = null, string $body = 'Body text.'): array
    {
        $server = ScriptedServer::start('fake-smtp-server.php', $script);
        $this->servers[] = $server;

        $error = null;
        try {
            ($client ?? new SmtpClient)->send(array_merge([
                'host' => '127.0.0.1',
                'port' => $server->port,
                'username' => 'alerts@example.com',
                'password' => 's3cr3t',
                'encryption' => 'none',
                'from_address' => 'alerts@example.com',
                'from_name' => 'NightOwl',
                'to_addresses' => ['ops@example.com'],
            ], $config), 'Test subject', $body, false);
        } catch (\Throwable $e) {
            $error = $e->getMessage();
        }

        return [
            'transcript' => $server->transcript(),
            'error' => $error,
        ];
    }

    /** The single line matching a prefix, or null. */
    private function line(array $transcript, string $prefix): ?string
    {
        foreach ($transcript as $line) {
            if (str_starts_with($line, $prefix)) {
                return $line;
            }
        }

        return null;
    }

    // ─── HELO ────────────────────────────────────────────────────────

    /**
     * The client used to greet every relay with the literal `EHLO nightowl` —
     * neither an FQDN nor an address literal, so not a legal Domain under RFC
     * 5321 §4.1.1.1. Exchange/Office 365 and Postfix with
     * reject_non_fqdn_helo_hostname refuse the connection on it, and the
     * refusal only ever reached error_log.
     */
    public function test_the_greeting_name_is_a_legal_domain_or_address_literal(): void
    {
        $result = $this->converse();

        $this->assertNull($result['error']);

        $ehlo = $this->line($result['transcript'], 'EHLO ');
        $this->assertNotNull($ehlo, 'client never sent EHLO');

        $name = substr($ehlo, 5);
        $this->assertNotSame('nightowl', $name, 'greeting name regressed to the bare token strict relays refuse');

        $isFqdn = SmtpClient::normalizeDomain($name) !== null;
        $isLiteral = preg_match('/^\[(IPv6:)?[0-9a-fA-F:.]+\]$/', $name) === 1;

        $this->assertTrue($isFqdn || $isLiteral, "EHLO name '{$name}' is neither an FQDN nor an address literal");
    }

    public function test_a_configured_helo_name_is_used_verbatim(): void
    {
        $result = $this->converse(client: new SmtpClient('relay-client.example.com'));

        $this->assertNull($result['error']);
        $this->assertSame('EHLO relay-client.example.com', $this->line($result['transcript'], 'EHLO '));
    }

    // ─── AUTH ────────────────────────────────────────────────────────

    /**
     * PLAIN was never attempted: the client sent AUTH LOGIN blind, whatever the
     * server advertised, so a relay offering PLAIN alone failed the handshake.
     */
    public function test_auth_plain_is_used_when_the_server_advertises_it(): void
    {
        $result = $this->converse([
            'ehlo' => "250-fake.test\r\n250 AUTH PLAIN\r\n",
        ]);

        $this->assertNull($result['error']);

        $auth = $this->line($result['transcript'], 'AUTH PLAIN ');
        $this->assertNotNull($auth, 'client did not use the PLAIN mechanism the server advertised');

        // The credentials must actually be in there, in RFC 4616 form.
        $this->assertSame(
            "\0alerts@example.com\0s3cr3t",
            base64_decode(substr($auth, strlen('AUTH PLAIN ')), true),
        );
    }

    public function test_auth_login_is_used_when_the_server_advertises_only_login(): void
    {
        $result = $this->converse([
            'ehlo' => "250-fake.test\r\n250 AUTH LOGIN\r\n",
            'auth' => "334 VXNlcm5hbWU6\r\n",
        ]);

        $this->assertNull($result['error']);
        $this->assertSame('AUTH LOGIN', $this->line($result['transcript'], 'AUTH'));
        $this->assertContains(base64_encode('alerts@example.com'), $result['transcript']);
        $this->assertContains(base64_encode('s3cr3t'), $result['transcript']);
    }

    /**
     * Back-compat: some minimal relays accept AUTH without ever announcing it,
     * and LOGIN blind is what this client has always done. Dropping the attempt
     * would break those installs to no benefit.
     */
    public function test_login_is_still_attempted_when_the_server_advertises_no_auth(): void
    {
        $result = $this->converse([
            'ehlo' => "250-fake.test\r\n250 8BITMIME\r\n",
            'auth' => "334 VXNlcm5hbWU6\r\n",
        ]);

        $this->assertNull($result['error']);
        $this->assertSame('AUTH LOGIN', $this->line($result['transcript'], 'AUTH'));
    }

    public function test_no_auth_is_attempted_without_a_username(): void
    {
        $result = $this->converse(config: ['username' => '', 'password' => '']);

        $this->assertNull($result['error']);
        $this->assertNull($this->line($result['transcript'], 'AUTH'));
    }

    // ─── Message ─────────────────────────────────────────────────────

    /**
     * A message with no Date header is malformed under RFC 5322 §3.6. Relays
     * accept it at DATA and then drop or spam-file it — which reads to the
     * customer as "NightOwl doesn't alert" while the agent logs a clean 250.
     * Symfony Mailer adds both of these, which is exactly why the API's mail
     * arrives and the agent's did not.
     */
    public function test_the_message_carries_the_date_and_message_id_headers(): void
    {
        $result = $this->converse();

        $this->assertNull($result['error']);

        $date = $this->line($result['transcript'], 'Date: ');
        $this->assertNotNull($date, 'message had no Date header');
        $this->assertNotFalse(strtotime(substr($date, 6)), "unparseable Date header: {$date}");

        $messageId = $this->line($result['transcript'], 'Message-ID: ');
        $this->assertNotNull($messageId, 'message had no Message-ID header');
        $this->assertSame(1, preg_match('/^Message-ID: <[^@>]+@example\.com>$/', $messageId), $messageId);
    }

    public function test_message_ids_are_unique_across_sends(): void
    {
        $first = $this->line($this->converse()['transcript'], 'Message-ID: ');
        $second = $this->line($this->converse()['transcript'], 'Message-ID: ');

        $this->assertNotNull($first);
        $this->assertNotSame($first, $second, 'a repeated Message-ID gets the second message deduplicated away');
    }

    /**
     * RFC 5321 §4.5.2: a body line beginning with a period must be doubled, or
     * the relay reads it as the end-of-data terminator and truncates the
     * message there — taking the rest of the alert with it.
     */
    public function test_a_body_line_starting_with_a_period_is_dot_stuffed(): void
    {
        $result = $this->converse(body: "First line\n.hidden line\nLast line");

        $this->assertNull($result['error']);
        $this->assertContains('..hidden line', $result['transcript']);
        $this->assertContains('Last line', $result['transcript'], 'body was truncated at the stray period');
    }

    // ─── Failure reporting ───────────────────────────────────────────

    /**
     * The Exchange/Office 365 case. What matters is not that it fails but that
     * the server's own words survive to the operator — that sentence is the
     * whole diagnosis.
     */
    public function test_a_relay_refusing_the_greeting_reports_its_own_words(): void
    {
        $result = $this->converse([
            'ehlo' => "501 5.5.4 Invalid domain name\r\n",
        ]);

        $this->assertNotNull($result['error']);
        $this->assertStringContainsString('501 5.5.4 Invalid domain name', $result['error']);
    }

    public function test_a_rejected_password_does_not_leak_the_credentials(): void
    {
        $result = $this->converse([
            'ehlo' => "250-fake.test\r\n250 AUTH PLAIN\r\n",
            'auth' => "535 5.7.8 Authentication credentials invalid\r\n",
        ]);

        $this->assertNotNull($result['error']);
        $this->assertStringContainsString('535 5.7.8', $result['error']);

        // The AUTH PLAIN command line carries the base64'd password, so the
        // error must be labelled by context and never echo the command.
        $this->assertStringNotContainsString('s3cr3t', $result['error']);
        $this->assertStringNotContainsString(base64_encode("\0alerts@example.com\0s3cr3t"), $result['error']);
    }

    /**
     * A relay that accepts the connection and then says nothing is a different
     * problem from one that hangs up — a silently dropped port versus a
     * rejected client. Reporting both as "SMTP error: " sent people to the
     * wrong place.
     */
    public function test_a_silent_relay_is_reported_as_a_timeout(): void
    {
        $result = $this->converse(
            ['greeting_delay' => 3],
            client: new SmtpClient(null, 10.0, 0.5),
        );

        $this->assertNotNull($result['error']);
        $this->assertStringContainsString('timed out', $result['error']);
        $this->assertStringContainsString('the server greeting', $result['error']);
    }

    public function test_requesting_tls_on_a_relay_that_lacks_it_names_the_mismatch(): void
    {
        $result = $this->converse(
            ['ehlo' => "250-fake.test\r\n250 8BITMIME\r\n"],
            ['encryption' => 'tls'],
        );

        $this->assertNotNull($result['error']);
        $this->assertStringContainsString('does not advertise STARTTLS', $result['error']);
    }

    /**
     * Incomplete config used to `return` silently — the single most effective
     * way to hide a broken alert channel from the person who configured it.
     */
    public function test_incomplete_config_is_reported_rather_than_silently_skipped(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('incomplete');

        (new SmtpClient)->send([
            'host' => 'smtp.example.com',
            'from_address' => 'alerts@example.com',
            'to_addresses' => [],
        ], 'Subject', 'Body');
    }

    public function test_a_dead_port_reports_the_connection_failure(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('SMTP connect to 127.0.0.1:1 failed');

        (new SmtpClient(null, 0.5, 0.5))->send([
            'host' => '127.0.0.1',
            'port' => 1,
            'from_address' => 'alerts@example.com',
            'to_addresses' => ['ops@example.com'],
            'encryption' => 'none',
        ], 'Subject', 'Body');
    }

    // ─── Header injection ────────────────────────────────────────────

    /**
     * The recipient list is user-controlled config. A CR/LF in it would end the
     * RCPT TO line and let the rest be read as a new SMTP command.
     */
    public function test_crlf_in_a_recipient_cannot_inject_an_smtp_command(): void
    {
        $result = $this->converse(config: [
            'to_addresses' => ["ops@example.com>\r\nRCPT TO:<attacker@evil.test"],
        ]);

        $this->assertNull($result['error']);

        $rcptLines = array_values(array_filter(
            $result['transcript'],
            static fn (string $line): bool => str_starts_with($line, 'RCPT TO:'),
        ));

        $this->assertCount(1, $rcptLines, 'the injected recipient became a second RCPT command');
        $this->assertNotContains('RCPT TO:<attacker@evil.test>', $result['transcript']);

        // The payload survives, flattened onto the one line — stripping the
        // CR/LF is the fix, silently dropping the address would not be.
        $this->assertStringContainsString('attacker@evil.test', $rcptLines[0]);
    }
}
