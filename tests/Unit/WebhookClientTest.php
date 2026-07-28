<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\WebhookClient;
use PHPUnit\Framework\TestCase;

/**
 * Pure-function coverage for the shared webhook poster. The socket half — a
 * real POST against a real listener, and what happens on a non-2xx — is in
 * tests/Integration/TestAlertCommandTest.php, which runs the whole command.
 */
class WebhookClientTest extends TestCase
{
    // ─── Scheme rejection ────────────────────────────────────────────

    public function test_is_safe_url_accepts_http_and_https(): void
    {
        $this->assertTrue(WebhookClient::isSafeUrl('http://example.com/hook'));
        $this->assertTrue(WebhookClient::isSafeUrl('https://hooks.slack.com/services/abc'));
        $this->assertTrue(WebhookClient::isSafeUrl('HTTPS://example.com/HOOK'));
    }

    /**
     * file:// and phar:// are not hypothetical: channel config is customer
     * input, and file_get_contents honours every registered stream wrapper.
     */
    public function test_is_safe_url_rejects_file_and_other_schemes(): void
    {
        $this->assertFalse(WebhookClient::isSafeUrl('file:///etc/passwd'));
        $this->assertFalse(WebhookClient::isSafeUrl('phar://payload.phar/x'));
        $this->assertFalse(WebhookClient::isSafeUrl('compress.zlib://any'));
        $this->assertFalse(WebhookClient::isSafeUrl('gopher://host/x'));
        $this->assertFalse(WebhookClient::isSafeUrl('ftp://host/x'));
        $this->assertFalse(WebhookClient::isSafeUrl(''));
        $this->assertFalse(WebhookClient::isSafeUrl('not a url'));
    }

    public function test_post_throws_on_a_rejected_scheme_before_touching_the_wrapper(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('scheme must be http/https');

        (new WebhookClient)->post('file:///etc/passwd', '{}');
    }

    // ─── Status parsing ──────────────────────────────────────────────

    public function test_response_status_reads_the_code_from_the_status_line(): void
    {
        $this->assertSame(200, WebhookClient::responseStatus(['HTTP/1.1 200 OK', 'Content-Type: text/plain']));
        $this->assertSame(404, WebhookClient::responseStatus(['HTTP/1.0 404 Not Found']));
        $this->assertSame(500, WebhookClient::responseStatus(['HTTP/2 500 Internal Server Error']));
    }

    /**
     * PHP appends the headers of every hop, so a followed redirect leaves two
     * status lines. The first is the one that describes what the request
     * finally did — reading the last would report the redirect, not the result.
     */
    public function test_response_status_takes_the_first_of_several_status_lines(): void
    {
        $this->assertSame(301, WebhookClient::responseStatus([
            'HTTP/1.1 301 Moved Permanently',
            'Location: https://elsewhere.test/hook',
            'HTTP/1.1 200 OK',
        ]));
    }

    public function test_response_status_is_null_when_no_status_line_is_present(): void
    {
        $this->assertNull(WebhookClient::responseStatus([]));
        $this->assertNull(WebhookClient::responseStatus(['Content-Type: application/json']));
    }

    // ─── Redaction ───────────────────────────────────────────────────

    /**
     * The path of a Slack or Discord webhook is the entire credential. These
     * strings land in error_log and in `nightowl:test-alert` output, which is
     * what a customer pastes into a support thread.
     */
    public function test_redact_url_keeps_the_host_and_drops_the_secret_path(): void
    {
        $redacted = WebhookClient::redactUrl('https://hooks.slack.com/services/T000/B000/XXXXXXXXXXXX');

        $this->assertSame('hooks.slack.com/…', $redacted);
        $this->assertStringNotContainsString('XXXXXXXXXXXX', $redacted);
    }

    public function test_redact_url_drops_query_strings_and_credentials_too(): void
    {
        $redacted = WebhookClient::redactUrl('https://user:pass@example.test/hook?token=abc123');

        $this->assertSame('example.test/…', $redacted);
        $this->assertStringNotContainsString('abc123', $redacted);
        $this->assertStringNotContainsString('pass', $redacted);
    }

    public function test_redact_url_never_falls_back_to_the_raw_url(): void
    {
        // No host to keep — the fallback must be a placeholder, not the input,
        // or an unparseable-but-secret-bearing string would leak whole.
        $this->assertSame('(unparseable url)', WebhookClient::redactUrl('not a url at all'));
    }

    // ─── Transport failures ──────────────────────────────────────────

    /**
     * The reason has to survive a host application's error handler.
     *
     * `@file_get_contents` + `error_get_last()` does not: error_get_last() is
     * only written by PHP's INTERNAL handler, and any userland handler that
     * returns non-false bypasses it. Laravel installs one. So the first manual
     * `php artisan nightowl:test-alert` against a dead port printed the
     * placeholder "connection failed" while the identical call outside Laravel
     * printed "Connection refused" — the whole diagnostic value, gone in
     * exactly the environment the command runs in.
     */
    public function test_the_real_reason_survives_a_host_error_handler(): void
    {
        set_error_handler(static fn (): bool => true); // stand-in for Laravel's

        try {
            (new WebhookClient(1.0))->post('http://127.0.0.1:1/hook', '{}');
            $this->fail('a POST to a dead port should throw');
        } catch (\RuntimeException $e) {
            $this->assertStringContainsString('Connection refused', $e->getMessage());
            $this->assertStringNotContainsString('connection failed', $e->getMessage());
        } finally {
            restore_error_handler();
        }
    }

    public function test_the_scoped_handler_is_restored_afterwards(): void
    {
        $sentinel = static fn (): bool => true;
        set_error_handler($sentinel);

        try {
            (new WebhookClient(1.0))->post('http://127.0.0.1:1/hook', '{}');
        } catch (\RuntimeException) {
            // expected
        }

        // set_error_handler returns the PREVIOUS handler — if ours had leaked,
        // this would hand back the closure from WebhookClient instead.
        $current = set_error_handler(static fn (): bool => true);
        restore_error_handler();
        restore_error_handler();

        $this->assertSame($sentinel, $current, 'WebhookClient leaked its error handler');
    }

    /**
     * PHP's warning embeds the URL verbatim, so reporting it raw would leak the
     * credential straight back out through the failure path — past the very
     * redaction the success path applies.
     */
    public function test_a_transport_failure_does_not_leak_the_url_through_phps_warning(): void
    {
        $secret = 'T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX';

        try {
            (new WebhookClient(1.0))->post("http://127.0.0.1:1/services/{$secret}", '{}');
            $this->fail('a POST to a dead port should throw');
        } catch (\RuntimeException $e) {
            $this->assertStringNotContainsString($secret, $e->getMessage());
            $this->assertStringNotContainsString('XXXXXXXXXXXXXXXXXXXXXXXX', $e->getMessage());
            $this->assertStringContainsString('127.0.0.1/…', $e->getMessage());
            $this->assertStringContainsString('Connection refused', $e->getMessage());
        }
    }
}
