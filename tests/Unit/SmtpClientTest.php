<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\SmtpClient;
use PHPUnit\Framework\TestCase;

/**
 * The pure decisions SmtpClient makes before it says a word to a relay: what
 * name to greet with, and which AUTH mechanism to use.
 *
 * Both were previously constants — `EHLO nightowl` and AUTH LOGIN, always —
 * and both are refused by real relays. The protocol conversation itself is
 * covered in tests/System/SmtpClientConversationTest.php against a scripted
 * server; these are the choices that feed it.
 */
class SmtpClientTest extends TestCase
{
    // ─── HELO name ───────────────────────────────────────────────────

    public function test_a_dotted_hostname_is_accepted_as_a_helo_name(): void
    {
        $this->assertSame('mail.example.com', SmtpClient::normalizeDomain('mail.example.com'));
        $this->assertSame('worker-01.eu.example.com', SmtpClient::normalizeDomain('worker-01.eu.example.com'));
    }

    /**
     * The old value. RFC 5321 §4.1.1.1 wants a fully-qualified domain or an
     * address literal, and a bare token is neither — Exchange/Office 365 and
     * Postfix under reject_non_fqdn_helo_hostname refuse the connection on it.
     * Rejecting it here is what sends us to the address-literal fallback.
     */
    public function test_a_bare_token_is_rejected_as_a_helo_name(): void
    {
        $this->assertNull(SmtpClient::normalizeDomain('nightowl'));
        $this->assertNull(SmtpClient::normalizeDomain('localhost'));
        $this->assertNull(SmtpClient::normalizeDomain('a3f9c21b4e77'));
    }

    public function test_a_helo_name_carrying_crlf_or_spaces_is_rejected(): void
    {
        // Would otherwise inject a second SMTP command on the EHLO line.
        $this->assertNull(SmtpClient::normalizeDomain("mail.example.com\r\nMAIL FROM:<a@b.c>"));
        $this->assertNull(SmtpClient::normalizeDomain('mail example.com'));
        $this->assertNull(SmtpClient::normalizeDomain(''));
    }

    public function test_a_trailing_root_dot_is_trimmed(): void
    {
        $this->assertSame('mail.example.com', SmtpClient::normalizeDomain('mail.example.com.'));
    }

    // ─── Address literal ─────────────────────────────────────────────

    public function test_an_ipv4_local_name_becomes_a_bracketed_literal(): void
    {
        $this->assertSame('[10.0.1.7]', SmtpClient::addressLiteral('10.0.1.7:54321'));
    }

    /**
     * IPv6 arrives bracketed with the port appended — splitting on the FIRST
     * colon would cut the address in half.
     */
    public function test_an_ipv6_local_name_keeps_its_whole_address(): void
    {
        $this->assertSame('[IPv6:2001:db8::1]', SmtpClient::addressLiteral('[2001:db8::1]:54321'));
        $this->assertSame('[IPv6:::1]', SmtpClient::addressLiteral('[::1]:25'));
    }

    public function test_an_unparseable_local_name_yields_no_literal(): void
    {
        $this->assertNull(SmtpClient::addressLiteral('not-an-address'));
        $this->assertNull(SmtpClient::addressLiteral(''));
    }

    // ─── EHLO capabilities ───────────────────────────────────────────

    public function test_capabilities_are_parsed_from_a_multiline_ehlo_reply(): void
    {
        $capabilities = SmtpClient::parseCapabilities(
            "250-mail.example.com at your service\r\n".
            "250-SIZE 35882577\r\n".
            "250-STARTTLS\r\n".
            "250-AUTH LOGIN PLAIN XOAUTH2\r\n".
            "250 8BITMIME\r\n"
        );

        $this->assertArrayHasKey('STARTTLS', $capabilities);
        $this->assertArrayHasKey('8BITMIME', $capabilities);
        $this->assertSame('LOGIN PLAIN XOAUTH2', $capabilities['AUTH']);

        // The first line is the greeting text, not a capability — parsing it as
        // one would invent a keyword out of the server's hostname.
        $this->assertArrayNotHasKey('MAIL.EXAMPLE.COM', $capabilities);
    }

    public function test_a_single_line_ehlo_reply_advertises_nothing(): void
    {
        $this->assertSame([], SmtpClient::parseCapabilities("250 mail.example.com\r\n"));
    }

    // ─── AUTH mechanism ──────────────────────────────────────────────

    public function test_mechanisms_are_read_from_the_rfc_4954_spelling(): void
    {
        $this->assertSame(
            ['LOGIN', 'PLAIN', 'XOAUTH2'],
            SmtpClient::authMechanisms(['AUTH' => 'LOGIN PLAIN XOAUTH2']),
        );
    }

    /** Older servers still emit `AUTH=LOGIN` for pre-RFC clients. */
    public function test_mechanisms_are_read_from_the_legacy_equals_spelling(): void
    {
        $this->assertSame(['LOGIN'], SmtpClient::authMechanisms(['AUTH' => '=LOGIN']));
        $this->assertSame(['PLAIN', 'LOGIN'], SmtpClient::authMechanisms(['AUTH' => 'PLAIN=LOGIN']));
    }

    public function test_lowercase_mechanisms_are_normalised(): void
    {
        $this->assertSame(['PLAIN', 'LOGIN'], SmtpClient::authMechanisms(['AUTH' => 'plain login']));
    }

    /**
     * A server that advertises no AUTH at all still gets an attempt — that is
     * what this client has always done, and some minimal relays accept AUTH
     * without announcing it. Returning [] here is what routes to that fallback
     * rather than to a hard failure.
     */
    public function test_no_auth_capability_yields_no_mechanisms(): void
    {
        $this->assertSame([], SmtpClient::authMechanisms([]));
        $this->assertSame([], SmtpClient::authMechanisms(['STARTTLS' => '']));
    }

    // ─── Header sanitising ───────────────────────────────────────────

    /**
     * Moved here from AlertNotifierTest and HealthAlertNotifierTest, which each
     * had their own copy — and had drifted: AlertNotifier passed only the
     * subject and from-name through it, HealthAlertNotifier the addresses too.
     * The end-to-end case (a CR/LF recipient failing to become a second RCPT
     * command) is in tests/System/SmtpClientConversationTest.php.
     */
    public function test_crlf_is_stripped_from_header_values(): void
    {
        $sanitize = (new \ReflectionClass(SmtpClient::class))->getMethod('sanitizeHeader');

        $this->assertSame('SubjectBcc: evil@hacker.com', $sanitize->invoke(null, "Subject\r\nBcc: evil@hacker.com"));
        $this->assertSame('noCRno', $sanitize->invoke(null, "no\rCR\nno"));
    }

    public function test_ordinary_header_text_is_left_alone(): void
    {
        $sanitize = (new \ReflectionClass(SmtpClient::class))->getMethod('sanitizeHeader');

        $this->assertSame('[App] New Issue: RuntimeException', $sanitize->invoke(null, '[App] New Issue: RuntimeException'));
    }

    // ─── Non-ASCII headers (RFC 2047) ────────────────────────────────

    public function test_an_ascii_subject_is_sent_as_is(): void
    {
        $this->assertSame('[App] Agent Health: DRAIN_STOPPED', SmtpClient::encodeHeader('[App] Agent Health: DRAIN_STOPPED'));
    }

    /**
     * Headers are 7-bit. A raw UTF-8 app name in the subject reached some
     * clients as mojibake and was refused by strict relays.
     */
    public function test_a_non_ascii_subject_is_encoded_and_decodes_back(): void
    {
        $subject = '[Café Ünïcode] Agent Health: DRAIN_STOPPED';

        $encoded = SmtpClient::encodeHeader($subject);

        $this->assertMatchesRegularExpression('/^[\x20-\x7E\r\n]*$/', $encoded, 'the header is not 7-bit');
        $this->assertStringStartsWith('=?UTF-8?B?', $encoded);
        $this->assertSame($subject, self::decode($encoded));
    }

    /**
     * RFC 2047 caps an encoded-word at 75 characters and forbids splitting a
     * character across two of them.
     */
    public function test_a_long_subject_is_split_into_short_words_on_character_boundaries(): void
    {
        $subject = '[Café Ünïcode] '.str_repeat('日本語のアプリ ', 12);

        $encoded = SmtpClient::encodeHeader($subject);
        $lines = explode("\r\n", $encoded);

        $this->assertGreaterThan(1, count($lines));
        foreach ($lines as $i => $line) {
            $word = $i === 0 ? $line : substr($line, 1);
            $this->assertSame($i === 0 ? $line : ' '.$word, $line, 'continuation lines must be folded with a space');
            $this->assertLessThanOrEqual(75, strlen($word));
            $this->assertMatchesRegularExpression('/^=\?UTF-8\?B\?[A-Za-z0-9+\/=]+\?=$/', $word);
            $this->assertTrue(mb_check_encoding(base64_decode(substr($word, 10, -2)), 'UTF-8'), "a word splits a character: {$word}");
        }
        $this->assertSame($subject, self::decode($encoded));
    }

    public function test_invalid_utf8_is_still_encoded_rather_than_sent_raw(): void
    {
        $subject = "[App\xC3] Recovered";

        $encoded = SmtpClient::encodeHeader($subject);

        $this->assertMatchesRegularExpression('/^[\x20-\x7E\r\n]*$/', $encoded);
        $this->assertSame($subject, self::decode($encoded));
    }

    public function test_a_from_name_is_encoded_quoted_or_left_bare_as_it_needs(): void
    {
        $mailbox = (new \ReflectionClass(SmtpClient::class))->getMethod('mailbox');

        $this->assertSame('NightOwl <a@example.com>', $mailbox->invoke(null, 'NightOwl', 'a@example.com'));
        $this->assertSame('"Acme, Inc." <a@example.com>', $mailbox->invoke(null, 'Acme, Inc.', 'a@example.com'));
        $this->assertSame('"say \\"hi\\"" <a@example.com>', $mailbox->invoke(null, 'say "hi"', 'a@example.com'));
        $this->assertSame('<a@example.com>', $mailbox->invoke(null, '', 'a@example.com'));

        $encoded = $mailbox->invoke(null, 'Café Ünïcode', 'a@example.com');
        $this->assertStringEndsWith(' <a@example.com>', $encoded);
        $this->assertSame('Café Ünïcode', self::decode(substr($encoded, 0, -strlen(' <a@example.com>'))));
    }

    /** Decode B-encoded words the way a mail client does: adjacent words join, the folding between them goes. */
    private static function decode(string $header): string
    {
        return (string) preg_replace_callback(
            '/=\?UTF-8\?B\?([A-Za-z0-9+\/=]*)\?=(?:\r\n )?/',
            static fn (array $m): string => base64_decode($m[1]),
            $header,
        );
    }
}
