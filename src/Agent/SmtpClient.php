<?php

namespace NightOwl\Agent;

/**
 * The agent's raw SMTP client — the transport behind every email alert the
 * agent itself dispatches: `issue.new` / `issue.reopened` from the drain
 * (AlertNotifier) and health alerts from the parent process
 * (HealthAlertNotifier).
 *
 * It is hand-rolled on purpose. Neither caller can reach a Laravel mailer: the
 * drain path is a forked child holding raw PDO and PHP streams, and the health
 * path fires off the 10s diagnosis timer. Nothing here may touch a facade or
 * the container.
 *
 * Being a SECOND, independent SMTP implementation is what makes it dangerous.
 * nightowl-api sends the very same `nightowl_alert_channels` config through
 * Symfony Mailer — that is what the dashboard's "send test" button exercises,
 * and what every issue.resolved / issue.ignored alert goes out through. So a
 * customer can configure email, test green, receive triage alerts, and still
 * never get a single drain-dispatched alert, because only this client speaks
 * for that half. Nothing in the product tested this path until
 * `nightowl:test-alert`.
 *
 * Each divergence from Symfony Mailer fixed below is a way that split could
 * bite, in the order it bites:
 *
 *   1. HELO/EHLO name. This sent the literal `EHLO nightowl` — neither an FQDN
 *      nor an address literal, so not a legal Domain per RFC 5321 §4.1.1.1.
 *      Exchange/Office 365 and Postfix under `reject_non_fqdn_helo_hostname`
 *      refuse it outright, and the connection dies at the first command with a
 *      501/504 that only ever reached error_log.
 *   2. AUTH mechanism. LOGIN was the only one offered, and it was attempted
 *      blind. Servers advertising PLAIN alone (and some advertising nothing
 *      until after STARTTLS) refuse the handshake.
 *   3. Date and Message-ID headers. Absent entirely. A message with no Date is
 *      malformed per RFC 5322 §3.6 — plenty of relays accept it at DATA and
 *      then drop it, or spam-file it on arrival, which reads to the customer as
 *      "NightOwl doesn't alert" with a 250 in the agent's log.
 *   4. Timeouts. 3s connect + 3s read, against a real TLS handshake plus AUTH
 *      plus DATA. Now configurable and defaulted to 10s, but still clamped to
 *      the caller's dispatch budget so a hung relay can never stall the drain
 *      for longer than the caller already allows.
 *
 * Failures throw. Reporting "nothing to do" by returning quietly is how this
 * stayed invisible; the callers catch and log, and `nightowl:test-alert` prints
 * the server's own words.
 */
final class SmtpClient
{
    /** Floor for a clamped timeout — below this even a healthy relay can't answer. */
    private const MIN_TIMEOUT_SECONDS = 0.5;

    public function __construct(
        private ?string $heloName = null,
        private float $connectTimeout = 10.0,
        private float $timeout = 10.0,
    ) {}

    public static function fromConfig(): self
    {
        $helo = config('nightowl.smtp.helo');

        return new self(
            is_string($helo) && trim($helo) !== '' ? $helo : null,
            (float) config('nightowl.smtp.connect_timeout', 10),
            (float) config('nightowl.smtp.timeout', 10),
        );
    }

    /**
     * Send one message through an email channel's config.
     *
     * Takes the raw channel config rather than pre-extracted fields so that
     * key handling, header sanitising and the "is this even configured" check
     * live in ONE place — the two callers had drifted apart on all three.
     *
     * @param  array<string, mixed>  $config  An `email` row's decoded config
     * @param  float|null  $deadline  microtime(true) the caller's dispatch budget expires;
     *                                timeouts are clamped to fit inside it
     *
     * @throws \RuntimeException on incomplete config or any SMTP-level failure
     */
    public function send(array $config, string $subject, string $body, bool $isHtml = false, ?float $deadline = null): void
    {
        $host = trim((string) ($config['host'] ?? ''));
        $port = (int) ($config['port'] ?? 587);
        $username = (string) ($config['username'] ?? '');
        $password = (string) ($config['password'] ?? '');
        $encryption = strtolower((string) ($config['encryption'] ?? 'tls'));

        // Every user-controllable field that lands in a header or an SMTP
        // command goes through sanitizeHeader — a CR/LF in any of them would
        // otherwise inject commands or forge headers. AlertNotifier used to
        // sanitize only the subject and from-name; HealthAlertNotifier
        // sanitized the addresses too. Centralising settles it.
        $fromAddress = self::sanitizeHeader((string) ($config['from_address'] ?? ''));
        $fromName = self::sanitizeHeader((string) ($config['from_name'] ?? 'NightOwl'));
        $toAddresses = array_values(array_filter(array_map(
            static fn ($a) => self::sanitizeHeader((string) $a),
            (array) ($config['to_addresses'] ?? []),
        ), static fn (string $a): bool => $a !== ''));

        $subject = self::sanitizeHeader($subject);

        if ($host === '' || $fromAddress === '' || $toAddresses === []) {
            throw new \RuntimeException(
                'email channel config is incomplete — host, from_address and to_addresses are all required'
            );
        }

        $connectTimeout = $this->clamp($this->connectTimeout, $deadline);

        // ssl:// wraps the socket in TLS from the first byte (implicit TLS,
        // conventionally port 465). 'tls' and 'none' both start in the clear;
        // 'tls' upgrades via STARTTLS below.
        $remote = ($encryption === 'ssl' ? 'ssl://' : '').$host.':'.$port;

        $socket = @stream_socket_client($remote, $errno, $errstr, $connectTimeout);
        if (! $socket) {
            throw new \RuntimeException(
                "SMTP connect to {$host}:{$port} failed: ".($errstr !== '' ? $errstr : "errno {$errno}")
            );
        }

        $this->applyReadTimeout($socket, $deadline);

        try {
            $this->expect($socket, 2, 'the server greeting');

            $helo = $this->heloName($socket);
            $capabilities = self::parseCapabilities(
                $this->command($socket, "EHLO {$helo}", 2, "EHLO {$helo}")
            );

            if ($encryption === 'tls') {
                if (! isset($capabilities['STARTTLS'])) {
                    throw new \RuntimeException(
                        "encryption is set to 'tls' but {$host}:{$port} does not advertise STARTTLS — "
                        ."use 'ssl' for implicit TLS (usually port 465) or 'none' for an unencrypted relay"
                    );
                }

                $this->command($socket, 'STARTTLS', 2, 'STARTTLS');

                if (! @stream_socket_enable_crypto(
                    $socket,
                    true,
                    STREAM_CRYPTO_METHOD_TLSv1_2_CLIENT | STREAM_CRYPTO_METHOD_TLSv1_3_CLIENT
                )) {
                    throw new \RuntimeException(
                        "STARTTLS handshake with {$host}:{$port} failed (certificate or protocol mismatch)"
                    );
                }

                // RFC 3207 §4.2: the client MUST re-issue EHLO after the
                // upgrade, and the server MUST discard what it advertised
                // before it — AUTH in particular is commonly withheld until the
                // channel is encrypted, so the pre-STARTTLS capability list is
                // the wrong one to pick a mechanism from.
                $capabilities = self::parseCapabilities(
                    $this->command($socket, "EHLO {$helo}", 2, "EHLO {$helo} (post-STARTTLS)")
                );
            }

            if ($username !== '') {
                $this->authenticate($socket, $capabilities, $username, $password);
            }

            $this->command($socket, "MAIL FROM:<{$fromAddress}>", 2, 'MAIL FROM');
            foreach ($toAddresses as $to) {
                $this->command($socket, "RCPT TO:<{$to}>", 2, "RCPT TO:<{$to}>");
            }

            $this->command($socket, 'DATA', 3, 'DATA');

            fwrite($socket, self::buildMessage($fromAddress, $fromName, $toAddresses, $subject, $body, $isHtml));
            $this->expect($socket, 2, 'the message body');

            fwrite($socket, "QUIT\r\n");
        } finally {
            fclose($socket);
        }
    }

    // ─── Message ─────────────────────────────────────────────────────

    /**
     * Render the DATA payload, terminator included.
     *
     * Date and Message-ID are here because RFC 5322 §3.6 makes Date mandatory
     * and because their absence is invisible at the protocol level: the relay
     * answers 250, the agent logs a success, and the message is filed as spam
     * or dropped downstream. Symfony Mailer adds both, which is precisely why
     * the API's mail arrives and the agent's did not.
     */
    private static function buildMessage(
        string $fromAddress,
        string $fromName,
        array $toAddresses,
        string $subject,
        string $body,
        bool $isHtml,
    ): string {
        // Normalise to CRLF, then dot-stuff any line that begins with a period
        // so it can't be read as the end-of-data terminator (RFC 5321 §4.5.2).
        $smtpBody = str_replace(["\r\n", "\r", "\n"], ["\n", "\n", "\r\n"], $body);
        $smtpBody = str_replace("\r\n.", "\r\n..", $smtpBody);
        if (str_starts_with($smtpBody, '.')) {
            $smtpBody = '.'.$smtpBody;
        }

        $message = "From: {$fromName} <{$fromAddress}>\r\n";
        $message .= 'To: '.implode(', ', $toAddresses)."\r\n";
        $message .= "Subject: {$subject}\r\n";
        $message .= 'Date: '.date('r')."\r\n";
        $message .= 'Message-ID: <'.self::messageId($fromAddress).">\r\n";
        $message .= "MIME-Version: 1.0\r\n";
        $message .= 'Content-Type: '.($isHtml ? 'text/html' : 'text/plain')."; charset=UTF-8\r\n";
        $message .= "\r\n";
        $message .= $smtpBody;
        $message .= "\r\n.\r\n";

        return $message;
    }

    /** Right-hand side taken from the sender's domain — a Message-ID must be globally unique. */
    private static function messageId(string $fromAddress): string
    {
        $domain = substr(strrchr($fromAddress, '@') ?: '@localhost', 1);
        if ($domain === '') {
            $domain = 'localhost';
        }

        return bin2hex(random_bytes(16)).'@'.$domain;
    }

    // ─── HELO name ───────────────────────────────────────────────────

    /**
     * The name we greet the server with, best available first.
     *
     * RFC 5321 §4.1.1.1 allows a fully-qualified domain OR, when none is
     * available, an address literal. A bare token like "nightowl" is neither,
     * and strict relays reject it — which is the whole reason this method
     * exists.
     */
    private function heloName($socket): string
    {
        if ($this->heloName !== null && ($configured = self::normalizeDomain($this->heloName)) !== null) {
            return $configured;
        }

        $hostname = gethostname();
        if (is_string($hostname) && ($fqdn = self::normalizeDomain($hostname)) !== null) {
            return $fqdn;
        }

        // No FQDN anywhere — containers routinely have a bare hostname like a
        // 12-hex container id. The address literal for whichever local
        // interface actually carries this connection is always legal and always
        // true, so it beats guessing a domain.
        $local = @stream_socket_get_name($socket, false);
        if (is_string($local) && ($literal = self::addressLiteral($local)) !== null) {
            return $literal;
        }

        return '[127.0.0.1]';
    }

    /**
     * Accept a name only if it is a usable FQDN: dotted, and made of nothing
     * but host characters. `localhost`, a container id, or anything carrying
     * whitespace or CRLF is rejected — a bad name here is worse than the
     * address-literal fallback.
     */
    public static function normalizeDomain(string $name): ?string
    {
        $name = trim($name, " \t\r\n.");

        if ($name === '' || ! str_contains($name, '.')) {
            return null;
        }

        return preg_match('/^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$/', $name) === 1 ? $name : null;
    }

    /**
     * Turn `stream_socket_get_name()`'s `addr:port` into an RFC 5321 address
     * literal: `[192.0.2.1]` for IPv4, `[IPv6:2001:db8::1]` for IPv6.
     */
    public static function addressLiteral(string $localName): ?string
    {
        // IPv6 arrives bracketed — `[::1]:54321` — so strip the port from the
        // right and unwrap. Splitting on the first colon would cut the address.
        $lastColon = strrpos($localName, ':');
        $address = $lastColon === false ? $localName : substr($localName, 0, $lastColon);
        $address = trim($address, '[]');

        if (filter_var($address, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4)) {
            return "[{$address}]";
        }

        if (filter_var($address, FILTER_VALIDATE_IP, FILTER_FLAG_IPV6)) {
            return "[IPv6:{$address}]";
        }

        return null;
    }

    // ─── AUTH ────────────────────────────────────────────────────────

    /**
     * Pick a mechanism the server actually advertises, PLAIN first.
     *
     * PLAIN is one round trip to LOGIN's three and is the more widely required
     * of the two; LOGIN is the older Microsoft-era mechanism many relays still
     * offer alongside it. When the server advertises neither we still try
     * LOGIN, because that is what this client has always done and some minimal
     * relays accept AUTH without ever announcing it — removing that attempt
     * would break working installs to no benefit.
     *
     * @param  array<string, string>  $capabilities
     */
    private function authenticate($socket, array $capabilities, string $username, string $password): void
    {
        $mechanisms = self::authMechanisms($capabilities);

        if (in_array('PLAIN', $mechanisms, true)) {
            // The context label is what any failure is reported with — never
            // the command itself, which carries the base64'd password.
            $this->command(
                $socket,
                'AUTH PLAIN '.base64_encode("\0{$username}\0{$password}"),
                2,
                'AUTH PLAIN',
            );

            return;
        }

        $this->command($socket, 'AUTH LOGIN', 3, 'AUTH LOGIN');
        $this->command($socket, base64_encode($username), 3, 'AUTH LOGIN (username)');
        $this->command($socket, base64_encode($password), 2, 'AUTH LOGIN (password)');
    }

    /**
     * Mechanisms from an EHLO capability set, upper-cased.
     *
     * Both spellings are in the wild: RFC 4954's `AUTH PLAIN LOGIN` and the
     * older `AUTH=LOGIN` some servers still emit for pre-RFC clients.
     *
     * @param  array<string, string>  $capabilities
     * @return list<string>
     */
    public static function authMechanisms(array $capabilities): array
    {
        $raw = $capabilities['AUTH'] ?? null;
        if ($raw === null) {
            return [];
        }

        $mechanisms = preg_split('/[\s=]+/', strtoupper(trim($raw)), -1, PREG_SPLIT_NO_EMPTY);

        return $mechanisms === false ? [] : array_values($mechanisms);
    }

    /**
     * Split a multiline EHLO reply into keyword => arguments.
     *
     * @return array<string, string>
     */
    public static function parseCapabilities(string $response): array
    {
        $capabilities = [];

        foreach (preg_split('/\r\n|\n/', trim($response)) ?: [] as $index => $line) {
            // The first line is the greeting text ("250-mail.example.com at
            // your service"), not a capability.
            if ($index === 0) {
                continue;
            }

            $line = trim(substr($line, 4));
            if ($line === '') {
                continue;
            }

            $parts = explode(' ', $line, 2);
            $capabilities[strtoupper($parts[0])] = $parts[1] ?? '';
        }

        return $capabilities;
    }

    // ─── Protocol I/O ────────────────────────────────────────────────

    /**
     * @param  string  $context  What we are waiting on, for the error message. Must
     *                           never be the raw command — AUTH lines carry credentials.
     *
     * @throws \RuntimeException
     */
    private function command($socket, string $command, int $expectFirstDigit, string $context): string
    {
        fwrite($socket, $command."\r\n");

        return $this->expect($socket, $expectFirstDigit, $context);
    }

    /**
     * Read one complete reply and check its status class.
     *
     * A reply is complete at the first line whose 4th character is a space
     * rather than a hyphen (RFC 5321 §4.2.1). The check is anchored on three
     * leading digits so that a line longer than the read buffer — which comes
     * back as a fragment, not a line — can't be mistaken for the terminator.
     *
     * @throws \RuntimeException on timeout, disconnect, or an unexpected status
     */
    private function expect($socket, int $expectFirstDigit, string $context): string
    {
        $response = '';

        while (true) {
            $line = fgets($socket, 4096);

            if ($line === false) {
                // Distinguishing these two matters: a timeout means the relay
                // is slow or the port is silently dropped, a close means it
                // hung up on us. They send the customer to different places.
                $meta = stream_get_meta_data($socket);

                throw new \RuntimeException(
                    ! empty($meta['timed_out'])
                        ? "SMTP timed out waiting for a reply to {$context}"
                        : "SMTP connection closed while waiting for a reply to {$context}"
                );
            }

            $response .= $line;

            if (preg_match('/^\d{3} /', $line) === 1) {
                break;
            }
        }

        if ($response === '' || (int) $response[0] !== $expectFirstDigit) {
            throw new \RuntimeException("SMTP error after {$context}: ".trim($response));
        }

        return $response;
    }

    // ─── Timeouts ────────────────────────────────────────────────────

    private function applyReadTimeout($socket, ?float $deadline): void
    {
        $seconds = $this->clamp($this->timeout, $deadline);

        stream_set_timeout($socket, (int) $seconds, (int) (fmod($seconds, 1.0) * 1_000_000));
    }

    /**
     * Never exceed the caller's remaining dispatch budget.
     *
     * The drain's flush budget is a ceiling on the WHOLE flush, checked between
     * dispatches — so without this, raising the per-socket timeout would let a
     * single hung relay overrun it by the full timeout on every channel behind
     * it. Clamping keeps the ceiling meaningful no matter how generous the
     * timeout is.
     *
     * It bounds, it does not guarantee: the read timeout is applied once, at
     * connect, so a relay that goes silent late in the conversation can still
     * take the budget plus one timeout. Re-clamping per read would need the
     * deadline held as instance state across a send, and the residual overrun
     * is one timeout total, not one per reply.
     */
    private function clamp(float $seconds, ?float $deadline): float
    {
        if ($deadline === null) {
            return max(self::MIN_TIMEOUT_SECONDS, $seconds);
        }

        return max(self::MIN_TIMEOUT_SECONDS, min($seconds, $deadline - microtime(true)));
    }

    /** Strip CR/LF to prevent header and SMTP-command injection. */
    private static function sanitizeHeader(string $value): string
    {
        return str_replace(["\r", "\n"], '', $value);
    }
}
