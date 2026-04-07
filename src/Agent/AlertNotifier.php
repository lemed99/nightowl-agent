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

    /** Lightweight polling: detect channel changes without full reload */
    private float $channelVersionCheckAt = 0;
    private ?string $channelFingerprint = null;

    /** Maximum total time for notification dispatch per flush (seconds) */
    private const MAX_NOTIFICATION_SECONDS = 5.0;

    /** @var list<array{appName: string, issueGroups: array, issueType: string, newHashes: string[]}> */
    private array $pendingNotifications = [];

    public function __construct(int $cacheTtl = 86400)
    {
        $this->cacheTtl = $cacheTtl;
    }

    public static function fromConfig(): self
    {
        return new self(
            (int) config('nightowl.threshold_cache_ttl', 86400),
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
                WHERE group_hash IN ({$placeholders}) AND type = ? AND status != 'resolved'
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
            // Periodically poll for dashboard-side channel changes
            if ($now < $this->channelVersionCheckAt) {
                return $this->channelCache;
            }

            $this->channelVersionCheckAt = $now + 30;

            try {
                $fingerprint = $pdo->query(
                    "SELECT COUNT(*)::text || ':' || COALESCE(MAX(updated_at)::text, '') FROM nightowl_alert_channels WHERE enabled = true"
                )->fetchColumn();

                if ($fingerprint === $this->channelFingerprint) {
                    return $this->channelCache;
                }
                // Fingerprint changed — fall through to full reload
            } catch (\Throwable) {
                return $this->channelCache;
            }
        }

        $this->channelCache = [];
        $this->channelCacheExpiry = $now + $this->cacheTtl;
        $this->channelVersionCheckAt = $now + 30;

        try {
            $rows = $pdo->query(
                "SELECT type, name, config, updated_at FROM nightowl_alert_channels WHERE enabled = true"
            )->fetchAll(PDO::FETCH_ASSOC);

            $maxUpdatedAt = '';
            foreach ($rows as $row) {
                if (($row['updated_at'] ?? '') > $maxUpdatedAt) {
                    $maxUpdatedAt = $row['updated_at'];
                }
                $this->channelCache[] = [
                    'type' => $row['type'],
                    'name' => $row['name'],
                    'config' => json_decode($row['config'], true) ?? [],
                ];
            }
            $this->channelFingerprint = count($rows) . ':' . $maxUpdatedAt;
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
        $occurrences = $group['count'];
        $users = count($group['users']);

        $detail = "*{$name}*";
        if ($message !== '') {
            $detail .= "\n{$message}";
        }

        $payload = [
            'attachments' => [
                [
                    'color' => '#DC2626',
                    'blocks' => [
                        [
                            'type' => 'section',
                            'text' => [
                                'type' => 'mrkdwn',
                                'text' => "\xF0\x9F\x9A\xA8  *New Issue*  \xC2\xB7  {$appName}",
                            ],
                        ],
                        [
                            'type' => 'section',
                            'text' => [
                                'type' => 'mrkdwn',
                                'text' => $detail,
                            ],
                        ],
                        [
                            'type' => 'section',
                            'fields' => [
                                ['type' => 'mrkdwn', 'text' => "*Occurrences*\n{$occurrences}"],
                                ['type' => 'mrkdwn', 'text' => "*Users Affected*\n{$users}"],
                            ],
                        ],
                        [
                            'type' => 'context',
                            'elements' => [
                                ['type' => 'mrkdwn', 'text' => "\xF0\x9F\xA6\x89 NightOwl"],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $this->httpPost($url, json_encode($payload, JSON_INVALID_UTF8_SUBSTITUTE));
    }

    private function sendDiscord(array $config, string $appName, string $prefix, array $group): void
    {
        $url = $config['webhook_url'] ?? '';
        if ($url === '') {
            return;
        }

        $name = $this->issueName($group);
        $message = $this->issueMessage($group);
        $occurrences = $group['count'];
        $users = count($group['users']);

        $description = "**{$name}**";
        if ($message !== '') {
            $description .= "\n{$message}";
        }

        $payload = [
            'embeds' => [
                [
                    'title' => "\xF0\x9F\x9A\xA8  New Issue",
                    'description' => $description,
                    'color' => 0xDC2626,
                    'fields' => [
                        ['name' => 'Occurrences', 'value' => (string) $occurrences, 'inline' => true],
                        ['name' => 'Users Affected', 'value' => (string) $users, 'inline' => true],
                        ['name' => 'App', 'value' => $appName, 'inline' => true],
                    ],
                    'footer' => ['text' => "\xF0\x9F\xA6\x89 NightOwl"],
                    'timestamp' => date('c'),
                ],
            ],
        ];

        $this->httpPost($url, json_encode($payload, JSON_INVALID_UTF8_SUBSTITUTE));
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
        $message = $this->issueMessage($group);
        $occurrences = $group['count'];
        $users = count($group['users']);

        $subject = $this->sanitizeHeader("[{$appName}] New Issue: {$name}");
        $fromName = $this->sanitizeHeader($fromName);

        $body = $this->buildEmailHtml($appName, $name, $message, $occurrences, $users);

        $this->smtpSend($host, $port, $username, $password, $encryption, $fromAddress, $fromName, $toAddresses, $subject, $body, true);
    }

    private function buildEmailHtml(string $appName, string $class, string $message, int $occurrences, int $users): string
    {
        $escapedClass = htmlspecialchars($class, ENT_QUOTES | ENT_HTML5, 'UTF-8');
        $escapedMessage = htmlspecialchars($message, ENT_QUOTES | ENT_HTML5, 'UTF-8');
        $escapedApp = htmlspecialchars($appName, ENT_QUOTES | ENT_HTML5, 'UTF-8');

        $messageRow = '';
        if ($message !== '') {
            $messageRow = '<tr><td style="padding:6px 28px 0;"><div style="font-size:13px;color:#52525b;line-height:1.5;word-break:break-word;">' . $escapedMessage . '</div></td></tr>';
        }

        return '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>'
            . '<body style="margin:0;padding:0;background-color:#f4f4f5;font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',Roboto,Helvetica,Arial,sans-serif;">'
            . '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f4f5;padding:32px 16px;"><tr><td align="center">'
            . '<table role="presentation" width="560" cellpadding="0" cellspacing="0" style="max-width:560px;width:100%;background-color:#ffffff;border-radius:8px;overflow:hidden;">'
            // Color bar
            . '<tr><td style="height:4px;background-color:#DC2626;"></td></tr>'
            // Header
            . '<tr><td style="padding:24px 28px 0;"><table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>'
            . '<td><span style="display:inline-block;padding:3px 10px;background-color:#FEE2E2;color:#DC2626;border-radius:4px;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;">New Issue</span></td>'
            . '<td align="right" style="color:#a1a1aa;font-size:12px;">' . $escapedApp . '</td>'
            . '</tr></table></td></tr>'
            // Exception class
            . '<tr><td style="padding:16px 28px 0;"><div style="font-size:16px;font-weight:600;color:#18181b;word-break:break-all;">' . $escapedClass . '</div></td></tr>'
            // Message
            . $messageRow
            // Stats
            . '<tr><td style="padding:20px 28px;"><table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#fafafa;border-radius:6px;"><tr>'
            . '<td style="padding:14px 18px;width:50%;border-right:1px solid #e4e4e7;"><div style="font-size:11px;color:#a1a1aa;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:2px;">Occurrences</div><div style="font-size:22px;font-weight:700;color:#18181b;">' . $occurrences . '</div></td>'
            . '<td style="padding:14px 18px;width:50%;"><div style="font-size:11px;color:#a1a1aa;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:2px;">Users Affected</div><div style="font-size:22px;font-weight:700;color:#18181b;">' . $users . '</div></td>'
            . '</tr></table></td></tr>'
            // Footer
            . '<tr><td style="padding:14px 28px;border-top:1px solid #f4f4f5;"><span style="color:#a1a1aa;font-size:11px;">&#x1F989; NightOwl</span></td></tr>'
            . '</table></td></tr></table></body></html>';
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
        bool $isHtml = false,
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
            $contentType = $isHtml ? 'text/html' : 'text/plain';
            $msg .= "Content-Type: {$contentType}; charset=UTF-8\r\n";
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

}
