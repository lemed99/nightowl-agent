<?php

namespace NightOwl\Tests\System;

use NightOwl\Agent\EmailTemplate;
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
    private function converse(array $script = [], array $config = [], ?SmtpClient $client = null, string $body = 'Body text.', bool $isHtml = false, string $subject = 'Test subject'): array
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
            ], $config), $subject, $body, $isHtml);
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

    /**
     * The API now saves email channels with no credentials, as null or as
     * blanks. None of those may produce an AUTH command, and a null password
     * must not break the send.
     */
    public function test_no_auth_is_attempted_with_a_null_or_blank_username(): void
    {
        foreach ([
            'null' => ['username' => null, 'password' => null],
            'blank' => ['username' => '   ', 'password' => null],
        ] as $case => $credentials) {
            $result = $this->converse(config: $credentials);

            $this->assertNull($result['error'], "{$case} username");
            $this->assertNull($this->line($result['transcript'], 'AUTH'), "{$case} username still sent AUTH");
            $this->assertNotNull($this->line($result['transcript'], 'MAIL FROM:'), "{$case} username: the message was never sent");
        }
    }

    /**
     * A username on a relay that takes no login used to fail with the relay's
     * bare "502 Command not implemented" after AUTH LOGIN, which pointed
     * nowhere. The error now names the cause and the fix, and still carries
     * the server's own reply.
     */
    public function test_a_username_on_a_relay_without_auth_fails_with_a_clear_error(): void
    {
        $result = $this->converse([
            'ehlo' => "250-fake.test\r\n250 8BITMIME\r\n",
            'auth' => "502 5.5.2 Command not implemented\r\n",
        ]);

        $this->assertNotNull($result['error']);
        $this->assertStringContainsString('a username is set but 127.0.0.1:', $result['error']);
        $this->assertStringContainsString('does not advertise AUTH', $result['error']);
        $this->assertStringContainsString('clear the username and password', $result['error']);
        $this->assertStringContainsString('502 5.5.2 Command not implemented', $result['error']);
        $this->assertStringNotContainsString("encryption to 'tls'", $result['error']);
        $this->assertNull($this->line($result['transcript'], 'MAIL FROM:'));
    }

    public function test_a_relay_that_hides_auth_behind_starttls_is_named_in_the_error(): void
    {
        $result = $this->converse([
            'ehlo' => "250-fake.test\r\n250 STARTTLS\r\n",
            'auth' => "530 5.7.0 Must issue a STARTTLS command first\r\n",
        ]);

        $this->assertNotNull($result['error']);
        $this->assertStringContainsString('does not advertise AUTH', $result['error']);
        $this->assertStringContainsString("set encryption to 'tls'", $result['error']);
    }

    /** An advertised mechanism that fails is a credentials problem, reported as before. */
    public function test_a_refused_advertised_login_keeps_the_servers_own_error(): void
    {
        $result = $this->converse([
            'ehlo' => "250-fake.test\r\n250 AUTH LOGIN\r\n",
            'auth' => "504 5.5.4 Unrecognized authentication type\r\n",
        ]);

        $this->assertNotNull($result['error']);
        $this->assertSame('SMTP error after AUTH LOGIN: 504 5.5.4 Unrecognized authentication type', $result['error']);
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

    /**
     * An app name like "Café Ünïcode" went into the Subject as raw UTF-8.
     * Headers are 7-bit: it has to go out as RFC 2047 encoded-words, the
     * display name in From too, and read back as the same text.
     */
    public function test_a_non_ascii_subject_and_sender_name_go_out_encoded(): void
    {
        $subject = '[Café Ünïcode] Agent Health: DRAIN_STOPPED';

        $result = $this->converse(config: ['username' => '', 'from_name' => 'Café Ünïcode'], subject: $subject);

        $this->assertNull($result['error']);

        $data = array_search('DATA', $result['transcript'], true);
        $lines = array_slice($result['transcript'], $data + 1);
        $headers = array_slice($lines, 0, (int) array_search('', $lines, true));
        foreach ($headers as $line) {
            $this->assertMatchesRegularExpression('/^[\x20-\x7E]*$/', $line, "a header line is not 7-bit: {$line}");
        }

        $subjectHeader = $this->unfold($headers, 'Subject: ');
        $this->assertSame($subject, self::decodeWords($subjectHeader));

        $from = $this->unfold($headers, 'From: ');
        $this->assertStringEndsWith(' <alerts@example.com>', $from);
        $this->assertSame('Café Ünïcode <alerts@example.com>', self::decodeWords($from));
    }

    /** Decode B-encoded words as a mail client does: adjacent words join, the folding between them goes. */
    private static function decodeWords(string $header): string
    {
        return (string) preg_replace_callback(
            '/=\?UTF-8\?B\?([A-Za-z0-9+\/=]*)\?=(?:\r\n )?/',
            static fn (array $m): string => base64_decode($m[1]),
            $header,
        );
    }

    /** A header and its folded continuation lines, as one value. */
    private function unfold(array $headers, string $prefix): string
    {
        $value = null;
        foreach ($headers as $line) {
            if ($value === null) {
                if (str_starts_with($line, $prefix)) {
                    $value = substr($line, strlen($prefix));
                }

                continue;
            }

            if (! str_starts_with($line, ' ') && ! str_starts_with($line, "\t")) {
                break;
            }

            $value .= "\r\n".$line;
        }

        $this->assertNotNull($value, "no {$prefix}header");

        return $value;
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

    /**
     * EmailTemplate renders each email as a single line of several KB, and
     * RFC 5321 §4.5.3.1.6 caps a line at 1000 octets, so a relay could bounce
     * the alert after answering 250. The body is sent quoted-printable and must
     * decode to exactly what was rendered, UTF-8 included.
     */
    public function test_a_long_html_body_goes_out_in_short_lines_and_decodes_unchanged(): void
    {
        $html = EmailTemplate::renderHealth('Café & Co', [
            'code' => 'DRAIN_STOPPED',
            'level' => 'critical',
            'message' => 'Drain has not written for 120s — check PostgreSQL',
            'recommendation' => '.leading dot, then a=b',
        ], 'degraded', 'web-1:2407', 'https://dash.example/dashboard/app_1/health', 'https://dash.example');
        $this->assertGreaterThan(998, strlen($html), 'the fixture no longer exercises the line limit');

        $result = $this->converse(config: ['username' => ''], body: $html, isHtml: true);

        $this->assertNull($result['error']);
        $this->assertContains('Content-Transfer-Encoding: quoted-printable', $result['transcript']);

        $data = array_search('DATA', $result['transcript'], true);
        $lines = array_slice($result['transcript'], $data + 1);
        $start = array_search('', $lines, true);
        $end = array_search('.', $lines, true);
        $body = array_slice($lines, $start + 1, $end - $start - 1);

        foreach ($body as $line) {
            $this->assertLessThanOrEqual(998, strlen($line), 'a body line is over the SMTP line limit');
        }

        $decoded = quoted_printable_decode(implode("\r\n", array_map(
            static fn (string $line): string => str_starts_with($line, '..') ? substr($line, 1) : $line,
            $body,
        )));
        $this->assertSame($html, $decoded);
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
