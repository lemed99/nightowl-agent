<?php

namespace NightOwl\Agent;

/**
 * The agent's outbound webhook poster — Slack, Discord and plain webhooks.
 *
 * One implementation, shared by AlertNotifier (drain child) and
 * HealthAlertNotifier (parent process), for the same reason SmtpClient exists:
 * the two had their own copies, and copies drift. The SMTP pair had drifted on
 * three separate points before they were merged, and only one of the two was
 * ever reachable by a customer trying to test their setup.
 *
 * Two things it does that the copies did not:
 *
 *  1. **Reads the response.** Both copies called `@file_get_contents` and threw
 *     the result away, so a revoked Slack webhook, a mistyped URL 404ing, or a
 *     receiver returning 500 was indistinguishable from a delivered alert. The
 *     agent reported nothing and the customer heard nothing — which is exactly
 *     what a working alert channel also looks like from the outside.
 *
 *  2. **Redacts the URL in errors.** A Slack or Discord webhook URL IS the
 *     credential: its path segment is the only secret involved, and anyone
 *     holding it can post into the channel. Failures reach error_log and the
 *     `nightowl:test-alert` console — both places a customer pastes into a
 *     support thread — so the host survives and the path does not.
 *
 * Throwing is safe on every path: both notifiers isolate a channel's dispatch
 * in try/catch and log, so one broken channel still cannot take down a drain
 * flush or a health tick. What changes is that the failure now has a reason
 * attached to it.
 *
 * No Laravel facades or container — this runs in the forked drain child.
 */
final class WebhookClient
{
    /**
     * @param  float  $timeout  Seconds for the whole request. Was 3s, which a
     *                          cold Slack/Discord TLS handshake plus their own
     *                          latency can genuinely exceed on a loaded host —
     *                          a timeout there is a silently dropped alert.
     */
    public function __construct(private float $timeout = 10.0) {}

    /**
     * @param  array<string, string>  $extraHeaders
     *
     * @throws \RuntimeException on a rejected scheme, a transport failure, or a non-2xx reply
     */
    public function post(string $url, string $body, array $extraHeaders = []): void
    {
        if (! self::isSafeUrl($url)) {
            throw new \RuntimeException("rejected webhook URL (scheme must be http/https): {$url}");
        }

        $headers = "Content-Type: application/json\r\nContent-Length: ".strlen($body)."\r\n";
        foreach ($extraHeaders as $key => $value) {
            $headers .= "{$key}: {$value}\r\n";
        }

        $context = stream_context_create([
            'http' => [
                'method' => 'POST',
                'header' => $headers,
                'content' => $body,
                'timeout' => $this->timeout,
                // Without this a 4xx/5xx makes the wrapper return false with no
                // body, so the receiver's own explanation — the most useful part
                // of the diagnosis — would be lost.
                'ignore_errors' => true,
            ],
        ]);

        // Capture the wrapper's own warning instead of `@` + error_get_last().
        //
        // error_get_last() is only populated when PHP's INTERNAL error handler
        // runs. Any userland set_error_handler that returns non-false bypasses
        // it — and Laravel installs exactly such a handler, which returns early
        // for `@`-suppressed errors. So in a real host app error_get_last() is
        // empty here and the reason degraded to a placeholder: "connection
        // failed" whether the relay refused, timed out, or resolved nowhere.
        // Measured, not theorised — `php artisan nightowl:test-alert` printed
        // the placeholder while the same call outside Laravel printed
        // "Connection refused".
        //
        // A scoped handler works regardless of what the host installed, and is
        // restored before we leave. It swallows the warning because we are
        // about to report it ourselves, with the URL redacted.
        $failure = null;
        set_error_handler(static function (int $severity, string $message) use (&$failure): bool {
            $failure ??= $message;

            return true;
        });

        try {
            // $http_response_header materialises in local scope from the
            // wrapper — same function scope, so the try block is fine.
            $response = file_get_contents($url, false, $context);
        } finally {
            restore_error_handler();
        }

        if ($response === false) {
            throw new \RuntimeException(
                'HTTP POST to '.self::redactUrl($url).' failed: '
                .($failure !== null ? self::redactMessage($failure, $url) : 'connection failed')
            );
        }

        $status = self::responseStatus($http_response_header ?? []);

        if ($status !== null && ($status < 200 || $status >= 300)) {
            $detail = trim(substr($response, 0, 200));

            throw new \RuntimeException(
                'HTTP POST to '.self::redactUrl($url)." returned {$status}".($detail !== '' ? ": {$detail}" : '')
            );
        }
    }

    /**
     * PHP's URL wrappers include file://, phar://, compress.zlib:// and more —
     * a malicious or mistaken channel config could otherwise make the agent
     * read local files and POST is not a concept those wrappers honour.
     */
    public static function isSafeUrl(string $url): bool
    {
        $scheme = strtolower((string) parse_url($url, PHP_URL_SCHEME));

        return $scheme === 'http' || $scheme === 'https';
    }

    /** First status line wins — redirects can stack several. */
    public static function responseStatus(array $responseHeaders): ?int
    {
        foreach ($responseHeaders as $header) {
            if (preg_match('#^HTTP/\S+\s+(\d{3})#', (string) $header, $m) === 1) {
                return (int) $m[1];
            }
        }

        return null;
    }

    /**
     * Strip the URL back out of PHP's own warning text.
     *
     * The wrapper's message embeds the URL verbatim —
     * `file_get_contents(https://hooks.slack.com/services/T0/B0/SECRET):
     * Failed to open stream: Connection refused` — so surfacing it raw would
     * leak the very credential redactUrl exists to protect, through the back
     * door. The prefix is dropped entirely (the caller already says what it was
     * doing and to whom); what survives is the reason, which is the part worth
     * reading.
     */
    private static function redactMessage(string $message, string $url): string
    {
        $message = str_replace($url, self::redactUrl($url), $message);
        $message = preg_replace('/^file_get_contents\([^)]*\):\s*/', '', $message) ?? $message;

        return trim($message);
    }

    /** Keep the host, which identifies the channel; drop the path, which is the secret. */
    public static function redactUrl(string $url): string
    {
        $host = parse_url($url, PHP_URL_HOST);

        return is_string($host) && $host !== '' ? $host.'/…' : '(unparseable url)';
    }
}
