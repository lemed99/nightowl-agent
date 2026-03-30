<?php

namespace NightOwl\Agent;

use PDO;

/**
 * Dispatches alert notifications for new issues directly from the drain worker.
 *
 * Reads alert channels from nightowl_alert_channels (cached), detects new issues
 * by querying existing hashes before the upsert, and dispatches via raw HTTP
 * (Slack/Discord/Webhook) or raw SMTP (Email) AFTER the transaction commits.
 *
 * No Laravel facades — runs in a forked child process with raw PDO and PHP streams.
 */
final class AlertNotifier
{
    /** @var array<int, array{type: string, name: string, config: array}> */
    private array $channelCache = [];
    private float $channelCacheExpiry = 0;
    private int $cacheTtl;
    private string $encryptionKey;

    /** Maximum total time for notification dispatch per flush (seconds) */
    private const MAX_NOTIFICATION_SECONDS = 5.0;

    /** @var list<array{appName: string, issueGroups: array, issueType: string, newHashes: string[]}> */
    private array $pendingNotifications = [];

    public function __construct(int $cacheTtl = 86400, string $encryptionKey = '')
    {
        $this->cacheTtl = $cacheTtl;
        $this->encryptionKey = $encryptionKey;
    }

    public static function fromConfig(): self
    {
        $appKey = config('app.key', '');

        // Strip the base64: prefix that Laravel uses
        if (str_starts_with($appKey, 'base64:')) {
            $appKey = base64_decode(substr($appKey, 7));
        }

        return new self(
            (int) config('nightowl.threshold_cache_ttl', 86400),
            $appKey,
        );
    }

    /**
     * Stage 1: Call BEFORE the issue upsert to snapshot existing hashes.
     * Returns the set of group_hashes that already exist, for diffing after upsert.
     *
     * @param  PDO      $pdo
     * @param  string[] $groupHashes  All group_hashes about to be upserted
     * @param  string   $issueType    'exception' or 'performance'
     * @return string[] Group hashes that already exist
     */
    public function snapshotExistingIssues(PDO $pdo, array $groupHashes, string $issueType): array
    {
        if (empty($groupHashes)) {
            return [];
        }

        $channels = $this->loadChannels($pdo);
        if (empty($channels)) {
            return [];
        }

        try {
            $placeholders = implode(',', array_fill(0, count($groupHashes), '?'));
            $stmt = $pdo->prepare("
                SELECT group_hash FROM nightowl_issues
                WHERE group_hash IN ({$placeholders}) AND type = ?
            ");
            $stmt->execute([...array_values($groupHashes), $issueType]);

            return $stmt->fetchAll(PDO::FETCH_COLUMN) ?: [];
        } catch (\Throwable) {
            return [];
        }
    }

    /**
     * Stage 2: Call AFTER the upsert (still inside transaction) to queue notifications.
     * Does NOT dispatch yet — just records what needs to be sent.
     *
     * @param string   $appName        Application name
     * @param array    $issueGroups    Groups keyed by group_hash
     * @param string   $issueType      'exception' or 'performance'
     * @param string[] $existingBefore Hashes that existed before the upsert (from snapshotExistingIssues)
     */
    public function queueNewIssueNotifications(string $appName, array $issueGroups, string $issueType, array $existingBefore): void
    {
        $newHashes = array_diff(array_keys($issueGroups), $existingBefore);

        if (empty($newHashes)) {
            return;
        }

        $this->pendingNotifications[] = [
            'appName' => $appName,
            'issueGroups' => $issueGroups,
            'issueType' => $issueType,
            'newHashes' => array_values($newHashes),
        ];
    }

    /**
     * Discard all pending notifications (e.g., on transaction rollback).
     */
    public function clearPending(): void
    {
        $this->pendingNotifications = [];
    }

    /**
     * Stage 3: Call AFTER the transaction commits. Dispatches all queued notifications.
     * This is the only method that does I/O (HTTP/SMTP).
     */
    public function flushNotifications(PDO $pdo): void
    {
        $pending = $this->pendingNotifications;
        $this->pendingNotifications = [];

        if (empty($pending)) {
            return;
        }

        $channels = $this->loadChannels($pdo);
        if (empty($channels)) {
            return;
        }

        $deadline = microtime(true) + self::MAX_NOTIFICATION_SECONDS;

        foreach ($pending as $batch) {
            foreach ($channels as $channel) {
                if (microtime(true) > $deadline) {
                    error_log('[NightOwl Agent] Notification dispatch budget exceeded (' . self::MAX_NOTIFICATION_SECONDS . 's), skipping remaining');
                    return;
                }

                $config = $channel['config'];
                $notifyEvents = $config['notify_events'] ?? null;

                if ($notifyEvents !== null && ! in_array('issue.new', $notifyEvents)) {
                    continue;
                }

                foreach ($batch['newHashes'] as $hash) {
                    if (microtime(true) > $deadline) {
                        error_log('[NightOwl Agent] Notification dispatch budget exceeded (' . self::MAX_NOTIFICATION_SECONDS . 's), skipping remaining');
                        return;
                    }

                    $group = $batch['issueGroups'][$hash] ?? null;
                    if ($group === null) {
                        continue;
                    }
                    $this->dispatchToChannel($channel, $batch['appName'], 'New Issue', $group);
                }
            }
        }
    }

    // ─── Channel Loading ─────────────────────────────────────────────

    private function loadChannels(PDO $pdo): array
    {
        $now = microtime(true);
        if ($now < $this->channelCacheExpiry) {
            return $this->channelCache;
        }

        $this->channelCache = [];
        $this->channelCacheExpiry = $now + $this->cacheTtl;

        try {
            $rows = $pdo->query(
                "SELECT type, name, config FROM nightowl_alert_channels WHERE enabled = true"
            )->fetchAll(PDO::FETCH_ASSOC);

            foreach ($rows as $row) {
                $config = json_decode($row['config'], true) ?? [];

                // Decrypt email password if present
                if ($row['type'] === 'email' && ! empty($config['password']) && $this->encryptionKey !== '') {
                    $config['password'] = $this->decryptValue($config['password']);
                }

                $this->channelCache[] = [
                    'type' => $row['type'],
                    'name' => $row['name'],
                    'config' => $config,
                ];
            }
        } catch (\Throwable) {
            // Table may not exist yet
        }

        return $this->channelCache;
    }

    // ─── Dispatch ────────────────────────────────────────────────────

    private function dispatchToChannel(array $channel, string $appName, string $prefix, array $group): void
    {
        try {
            match ($channel['type']) {
                'slack' => $this->sendSlack($channel['config'], $appName, $prefix, $group),
                'discord' => $this->sendDiscord($channel['config'], $appName, $prefix, $group),
                'webhook' => $this->sendWebhook($channel['config'], $appName, $prefix, $group),
                'email' => $this->sendEmail($channel['config'], $appName, $prefix, $group),
                default => null,
            };
        } catch (\Throwable $e) {
            error_log("[NightOwl Agent] Failed to notify via {$channel['type']} ({$channel['name']}): {$e->getMessage()}");
        }
    }

    /**
     * Resolve the display name for an issue group.
     * Exception groups have 'class', performance groups have 'name'.
     */
    private function issueName(array $group): string
    {
        return $group['class'] ?? $group['name'] ?? 'Unknown';
    }

    private function issueMessage(array $group): string
    {
        $message = $group['message'] ?? '';
        if ($message !== '' && mb_strlen($message) > 200) {
            $message = mb_substr($message, 0, 200) . '...';
        }

        return $message;
    }

    /**
     * Strip CR/LF from a string to prevent email header injection.
     */
    private function sanitizeHeader(string $value): string
    {
        return str_replace(["\r", "\n"], '', $value);
    }

    private function sendSlack(array $config, string $appName, string $prefix, array $group): void
    {
        $url = $config['webhook_url'] ?? '';
        if ($url === '') {
            return;
        }

        $name = $this->issueName($group);
        $message = $this->issueMessage($group);

        $text = "*[{$appName}] {$prefix}*\n*{$name}*\n";
        if ($message !== '') {
            $text .= "{$message}\n";
        }
        $text .= "Occurrences: {$group['count']} | Users: " . count($group['users']);

        $this->httpPost($url, json_encode(['text' => $text], JSON_INVALID_UTF8_SUBSTITUTE));
    }

    private function sendDiscord(array $config, string $appName, string $prefix, array $group): void
    {
        $url = $config['webhook_url'] ?? '';
        if ($url === '') {
            return;
        }

        $name = $this->issueName($group);
        $message = $this->issueMessage($group);

        $text = "**[{$appName}] {$prefix}**\n**{$name}**\n";
        if ($message !== '') {
            $text .= "{$message}\n";
        }
        $text .= "Occurrences: {$group['count']} | Users: " . count($group['users']);

        $this->httpPost($url, json_encode(['content' => $text], JSON_INVALID_UTF8_SUBSTITUTE));
    }

    private function sendWebhook(array $config, string $appName, string $prefix, array $group): void
    {
        $url = $config['url'] ?? '';
        if ($url === '') {
            return;
        }

        $payload = json_encode([
            'event' => 'issue.new',
            'app' => $appName,
            'issue' => [
                'class' => $this->issueName($group),
                'message' => $group['message'] ?? '',
                'occurrences' => $group['count'],
                'users' => count($group['users']),
            ],
            'timestamp' => date('c'),
        ], JSON_INVALID_UTF8_SUBSTITUTE);

        $headers = [];
        if (! empty($config['secret'])) {
            $headers['X-NightOwl-Signature'] = hash_hmac('sha256', $payload, $config['secret']);
        }

        $this->httpPost($url, $payload, $headers);
    }

    private function sendEmail(array $config, string $appName, string $prefix, array $group): void
    {
        $host = $config['host'] ?? '';
        $port = (int) ($config['port'] ?? 587);
        $username = $config['username'] ?? '';
        $password = $config['password'] ?? '';
        $encryption = $config['encryption'] ?? 'tls';
        $fromAddress = $config['from_address'] ?? '';
        $fromName = $config['from_name'] ?? 'NightOwl';
        $toAddresses = $config['to_addresses'] ?? [];

        if ($host === '' || $fromAddress === '' || empty($toAddresses)) {
            return;
        }

        $name = $this->issueName($group);
        $subject = $this->sanitizeHeader("[{$appName}] {$prefix}: {$name}");
        $fromName = $this->sanitizeHeader($fromName);

        $body = "{$prefix} in {$appName}\n\n";
        $body .= "{$name}\n";
        $message = $this->issueMessage($group);
        if ($message !== '') {
            $body .= "Message: {$message}\n";
        }
        $body .= "\nOccurrences: {$group['count']}\n";
        $body .= "Users affected: " . count($group['users']) . "\n";

        $this->smtpSend($host, $port, $username, $password, $encryption, $fromAddress, $fromName, $toAddresses, $subject, $body);
    }

    // ─── Raw HTTP ────────────────────────────────────────────────────

    private function httpPost(string $url, string $body, array $extraHeaders = []): void
    {
        $headers = "Content-Type: application/json\r\nContent-Length: " . strlen($body) . "\r\n";
        foreach ($extraHeaders as $key => $value) {
            $headers .= "{$key}: {$value}\r\n";
        }

        $context = stream_context_create([
            'http' => [
                'method' => 'POST',
                'header' => $headers,
                'content' => $body,
                'timeout' => 3,
                'ignore_errors' => true,
            ],
        ]);

        @file_get_contents($url, false, $context);
    }

    // ─── Raw SMTP ────────────────────────────────────────────────────

    private function smtpSend(
        string $host,
        int $port,
        string $username,
        string $password,
        string $encryption,
        string $fromAddress,
        string $fromName,
        array $toAddresses,
        string $subject,
        string $body,
    ): void {
        $transport = $encryption === 'ssl' ? "ssl://{$host}" : $host;

        $socket = @stream_socket_client("{$transport}:{$port}", $errno, $errstr, 3);
        if (! $socket) {
            error_log("[NightOwl Agent] SMTP connect failed: {$errstr}");

            return;
        }

        stream_set_timeout($socket, 3);

        try {
            $this->smtpExpect($socket, 2); // greeting
            $this->smtpCommand($socket, "EHLO nightowl", 2);

            if ($encryption === 'tls') {
                $this->smtpCommand($socket, "STARTTLS", 2);
                if (! stream_socket_enable_crypto($socket, true, STREAM_CRYPTO_METHOD_TLSv1_2_CLIENT | STREAM_CRYPTO_METHOD_TLSv1_3_CLIENT)) {
                    error_log('[NightOwl Agent] SMTP STARTTLS failed');

                    return;
                }
                $this->smtpCommand($socket, "EHLO nightowl", 2);
            }

            if ($username !== '') {
                $this->smtpCommand($socket, "AUTH LOGIN", 3);
                $this->smtpCommand($socket, base64_encode($username), 3);
                $this->smtpCommand($socket, base64_encode($password), 2);
            }

            $this->smtpCommand($socket, "MAIL FROM:<{$fromAddress}>", 2);
            foreach ($toAddresses as $to) {
                $this->smtpCommand($socket, "RCPT TO:<{$to}>", 2);
            }

            $this->smtpCommand($socket, "DATA", 3);

            $toHeader = implode(', ', $toAddresses);
            // Normalize body to CRLF line endings for SMTP
            $smtpBody = str_replace(["\r\n", "\r", "\n"], ["\n", "\n", "\r\n"], $body);
            // Dot-stuff lines starting with a period (SMTP transparency)
            $smtpBody = str_replace("\r\n.", "\r\n..", $smtpBody);

            $msg = "From: {$fromName} <{$fromAddress}>\r\n";
            $msg .= "To: {$toHeader}\r\n";
            $msg .= "Subject: {$subject}\r\n";
            $msg .= "MIME-Version: 1.0\r\n";
            $msg .= "Content-Type: text/plain; charset=UTF-8\r\n";
            $msg .= "\r\n";
            $msg .= $smtpBody;
            $msg .= "\r\n.\r\n";

            fwrite($socket, $msg);
            $this->smtpExpect($socket, 2);

            fwrite($socket, "QUIT\r\n");
        } finally {
            fclose($socket);
        }
    }

    /**
     * Send an SMTP command and verify the response code starts with the expected digit.
     *
     * @throws \RuntimeException on unexpected response
     */
    private function smtpCommand($socket, string $command, int $expectFirstDigit): string
    {
        fwrite($socket, $command . "\r\n");

        return $this->smtpExpect($socket, $expectFirstDigit);
    }

    /**
     * Read SMTP response and verify the first digit of the status code.
     *
     * @throws \RuntimeException on unexpected response
     */
    private function smtpExpect($socket, int $expectFirstDigit): string
    {
        $response = '';
        while ($line = fgets($socket, 512)) {
            $response .= $line;
            if (isset($line[3]) && $line[3] === ' ') {
                break;
            }
        }

        if ($response === '' || (int) $response[0] !== $expectFirstDigit) {
            throw new \RuntimeException("SMTP error: " . trim($response));
        }

        return $response;
    }

    // ─── Encryption ──────────────────────────────────────────────────

    /**
     * Decrypt a value encrypted by Laravel's Crypt::encryptString().
     * Uses AES-256-CBC with the app key, matching Laravel's Encrypter.
     */
    private function decryptValue(string $encrypted): string
    {
        try {
            $payload = json_decode(base64_decode($encrypted), true);
            if (! $payload || ! isset($payload['iv'], $payload['value'], $payload['mac'])) {
                return $encrypted; // Not encrypted, return as-is
            }

            $iv = base64_decode($payload['iv']);
            $value = base64_decode($payload['value']);

            // Verify MAC (computed on base64 values, matching Laravel)
            $mac = hash_hmac('sha256', $payload['iv'] . $payload['value'], $this->encryptionKey);
            if (! hash_equals($mac, $payload['mac'])) {
                return $encrypted; // MAC mismatch
            }

            $decrypted = openssl_decrypt($value, 'aes-256-cbc', $this->encryptionKey, OPENSSL_RAW_DATA, $iv);

            return $decrypted !== false ? $decrypted : $encrypted;
        } catch (\Throwable) {
            return $encrypted;
        }
    }
}
