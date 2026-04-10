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

    private string $frontendUrl;

    public function __construct(int $cacheTtl = 86400, string $frontendUrl = '')
    {
        $this->cacheTtl = $cacheTtl;
        $this->frontendUrl = $frontendUrl;
    }

    public static function fromConfig(): self
    {
        return new self(
            (int) config('nightowl.threshold_cache_ttl', 86400),
            (string) config('app.frontend_url', ''),
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
     *
     * Enriches each new issue from the DB (now committed) before dispatching,
     * so notifications carry the full context: issue ID, timestamps, environment, etc.
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
            // Enrich groups from DB once per batch (not per channel)
            $enrichedGroups = [];
            foreach ($batch['newHashes'] as $hash) {
                $group = $batch['issueGroups'][$hash] ?? null;
                if ($group === null) {
                    continue;
                }
                $enrichedGroups[$hash] = $this->enrichFromDb($pdo, $group, $hash, $batch['issueType']);
            }

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

                foreach ($enrichedGroups as $group) {
                    if (microtime(true) > $deadline) {
                        error_log('[NightOwl Agent] Notification dispatch budget exceeded (' . self::MAX_NOTIFICATION_SECONDS . 's), skipping remaining');
                        return;
                    }

                    $this->dispatchToChannel($channel, $batch['appName'], 'New Issue', $group, $batch['issueType']);
                }
            }
        }
    }

    /**
     * Enrich a notification group with data from the now-committed DB rows.
     *
     * Queries nightowl_issues for: id, first_seen_at, last_seen_at, occurrences_count, users_count, subtype.
     * For exceptions, also queries nightowl_exceptions for: file, line, server, php_version, laravel_version, handled.
     */
    private function enrichFromDb(PDO $pdo, array $group, string $groupHash, string $issueType): array
    {
        try {
            $stmt = $pdo->prepare('
                SELECT id, first_seen_at, last_seen_at, occurrences_count, users_count, subtype
                FROM nightowl_issues
                WHERE group_hash = ? AND type = ?
                LIMIT 1
            ');
            $stmt->execute([$groupHash, $issueType]);
            $issue = $stmt->fetch(PDO::FETCH_ASSOC);

            if ($issue) {
                $group['issue_id'] = (int) $issue['id'];
                $group['first_seen_at'] = $issue['first_seen_at'];
                $group['last_seen_at'] = $issue['last_seen_at'];
                $group['count'] = (int) ($issue['occurrences_count'] ?? $group['count']);
                $group['users_count'] = (int) ($issue['users_count'] ?? count($group['users']));
                $group['subtype'] = $issue['subtype'] ?? ($group['subtype'] ?? null);
            }
        } catch (\Throwable) {
            // Best-effort — dispatch with whatever we have
        }

        if ($issueType === 'exception') {
            try {
                $stmt = $pdo->prepare('
                    SELECT file, line, server, php_version, laravel_version, handled
                    FROM nightowl_exceptions
                    WHERE fingerprint = ?
                    ORDER BY created_at DESC
                    LIMIT 1
                ');
                $stmt->execute([$groupHash]);
                $exc = $stmt->fetch(PDO::FETCH_ASSOC);

                if ($exc) {
                    $group['environment'] = ! empty($exc['server']) ? $exc['server'] : null;
                    $group['handled'] = isset($exc['handled']) ? (bool) $exc['handled'] : null;
                    $group['php_version'] = ! empty($exc['php_version']) ? $exc['php_version'] : null;
                    $group['laravel_version'] = ! empty($exc['laravel_version']) ? $exc['laravel_version'] : null;
                    if (! empty($exc['file'])) {
                        $group['location'] = $exc['file'] . (! empty($exc['line']) ? ':' . $exc['line'] : '');
                    }
                }
            } catch (\Throwable) {
                // Best-effort
            }
        }

        return $group;
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

    private function dispatchToChannel(array $channel, string $appName, string $prefix, array $group, string $issueType = 'exception'): void
    {
        try {
            match ($channel['type']) {
                'slack' => $this->sendSlack($channel['config'], $appName, $prefix, $group, $issueType),
                'discord' => $this->sendDiscord($channel['config'], $appName, $prefix, $group, $issueType),
                'webhook' => $this->sendWebhook($channel['config'], $appName, $prefix, $group, $issueType),
                'email' => $this->sendEmail($channel['config'], $appName, $prefix, $group, $issueType),
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

    private function logoUrl(): string
    {
        return rtrim($this->frontendUrl, '/') . '/logo.png';
    }

    private function buildViewUrl(?int $issueId): ?string
    {
        if ($issueId === null || $this->frontendUrl === '') {
            return null;
        }

        // Agent doesn't know the app ID in the dashboard, so link to the issues list
        return rtrim($this->frontendUrl, '/') . '/dashboard';
    }

    /**
     * Strip CR/LF from a string to prevent email header injection.
     */
    private function sanitizeHeader(string $value): string
    {
        return str_replace(["\r", "\n"], '', $value);
    }

    private function sendSlack(array $config, string $appName, string $prefix, array $group, string $issueType = 'exception'): void
    {
        $url = $config['webhook_url'] ?? '';
        if ($url === '') {
            return;
        }

        $name = $this->issueName($group);
        $message = $this->issueMessage($group);
        $occurrences = (int) ($group['count'] ?? 0);
        $users = (int) ($group['users_count'] ?? count($group['users'] ?? []));
        $subtype = $group['subtype'] ?? null;
        $issueId = $group['issue_id'] ?? null;
        $handled = $group['handled'] ?? null;
        $isException = $issueType === 'exception';

        $blocks = [];

        $blocks[] = [
            'type' => 'section',
            'text' => [
                'type' => 'mrkdwn',
                'text' => "\xF0\x9F\x9A\xA8  *New Issue*  \xC2\xB7  {$appName}",
            ],
        ];

        $detail = "*{$name}*";
        if ($message !== '') {
            $detail .= "\n{$message}";
        }
        $blocks[] = [
            'type' => 'section',
            'text' => ['type' => 'mrkdwn', 'text' => $detail],
        ];

        $fields = [];
        if ($issueId !== null) {
            $fields[] = ['type' => 'mrkdwn', 'text' => "*Issue*\n#{$issueId}"];
        }
        if ($isException) {
            $statusText = ($handled === true) ? 'Handled' : 'Unhandled';
            $fields[] = ['type' => 'mrkdwn', 'text' => "*Status*\n{$statusText}"];
            if (! empty($group['environment'])) {
                $fields[] = ['type' => 'mrkdwn', 'text' => "*Environment*\n{$group['environment']}"];
            }
            if (! empty($group['location'])) {
                $fields[] = ['type' => 'mrkdwn', 'text' => "*Location*\n`{$group['location']}`"];
            }
            if (! empty($group['laravel_version'])) {
                $fields[] = ['type' => 'mrkdwn', 'text' => "*Laravel*\n{$group['laravel_version']}"];
            }
            if (! empty($group['php_version'])) {
                $fields[] = ['type' => 'mrkdwn', 'text' => "*PHP*\n{$group['php_version']}"];
            }
        } else {
            $subtypeLabel = EmailTemplate::subtypeLabel($subtype);
            $fields[] = ['type' => 'mrkdwn', 'text' => "*{$subtypeLabel}*\n`{$name}`"];
        }
        $fields[] = ['type' => 'mrkdwn', 'text' => "*Occurrences*\n{$occurrences}"];
        $fields[] = ['type' => 'mrkdwn', 'text' => "*Users Affected*\n{$users}"];

        $blocks[] = [
            'type' => 'section',
            'fields' => array_slice($fields, 0, 10),
        ];

        // First/Last seen
        $contextElements = [];
        if (! empty($group['first_seen_at'])) {
            $contextElements[] = ['type' => 'mrkdwn', 'text' => "First seen: {$group['first_seen_at']}"];
        }
        if (! empty($group['last_seen_at'])) {
            $contextElements[] = ['type' => 'mrkdwn', 'text' => "Last seen: {$group['last_seen_at']}"];
        }
        if (! empty($contextElements)) {
            $blocks[] = ['type' => 'context', 'elements' => $contextElements];
        }

        $blocks[] = [
            'type' => 'context',
            'elements' => [['type' => 'mrkdwn', 'text' => 'NightOwl']],
        ];

        $payload = [
            'username' => 'NightOwl',
            'icon_url' => $this->logoUrl(),
            'attachments' => [
                [
                    'color' => '#DC2626',
                    'blocks' => $blocks,
                ],
            ],
        ];

        $this->httpPost($url, json_encode($payload, JSON_INVALID_UTF8_SUBSTITUTE));
    }

    private function sendDiscord(array $config, string $appName, string $prefix, array $group, string $issueType = 'exception'): void
    {
        $url = $config['webhook_url'] ?? '';
        if ($url === '') {
            return;
        }

        $name = $this->issueName($group);
        $message = $this->issueMessage($group);
        $occurrences = (int) ($group['count'] ?? 0);
        $users = (int) ($group['users_count'] ?? count($group['users'] ?? []));
        $subtype = $group['subtype'] ?? null;
        $issueId = $group['issue_id'] ?? null;
        $handled = $group['handled'] ?? null;
        $isException = $issueType === 'exception';

        $description = "**{$name}**";
        if ($message !== '') {
            $description .= "\n{$message}";
        }

        $fields = [
            ['name' => 'App', 'value' => $appName, 'inline' => true],
        ];

        if ($issueId !== null) {
            $fields[] = ['name' => 'Issue', 'value' => '#' . $issueId, 'inline' => true];
        }

        if (! empty($group['environment'])) {
            $fields[] = ['name' => 'Environment', 'value' => (string) $group['environment'], 'inline' => true];
        }

        if ($isException) {
            $statusText = ($handled === true) ? 'Handled' : 'Unhandled';
            $fields[] = ['name' => 'Status', 'value' => $statusText, 'inline' => true];
            if (! empty($group['location'])) {
                $fields[] = ['name' => 'Location', 'value' => '`' . $group['location'] . '`', 'inline' => false];
            }
            if (! empty($group['laravel_version'])) {
                $fields[] = ['name' => 'Laravel', 'value' => (string) $group['laravel_version'], 'inline' => true];
            }
            if (! empty($group['php_version'])) {
                $fields[] = ['name' => 'PHP', 'value' => (string) $group['php_version'], 'inline' => true];
            }
        } else {
            $subtypeLabel = EmailTemplate::subtypeLabel($subtype);
            $fields[] = ['name' => $subtypeLabel, 'value' => '`' . $name . '`', 'inline' => false];
        }

        $fields[] = ['name' => 'Occurrences', 'value' => (string) $occurrences, 'inline' => true];
        $fields[] = ['name' => 'Users Affected', 'value' => (string) $users, 'inline' => true];

        if (! empty($group['first_seen_at'])) {
            $fields[] = ['name' => 'First Seen', 'value' => (string) $group['first_seen_at'], 'inline' => true];
        }
        if (! empty($group['last_seen_at'])) {
            $fields[] = ['name' => 'Last Seen', 'value' => (string) $group['last_seen_at'], 'inline' => true];
        }

        $logoUrl = $this->logoUrl();

        $payload = [
            'username' => 'NightOwl',
            'avatar_url' => $logoUrl,
            'embeds' => [
                [
                    'author' => ['name' => 'NightOwl', 'icon_url' => $logoUrl],
                    'title' => "\xF0\x9F\x9A\xA8  New Issue",
                    'description' => $description,
                    'color' => 0xDC2626,
                    'fields' => array_slice($fields, 0, 25),
                    'footer' => ['text' => 'NightOwl', 'icon_url' => $logoUrl],
                    'timestamp' => date('c'),
                ],
            ],
        ];

        $this->httpPost($url, json_encode($payload, JSON_INVALID_UTF8_SUBSTITUTE));
    }

    private function sendWebhook(array $config, string $appName, string $prefix, array $group, string $issueType = 'exception'): void
    {
        $url = $config['url'] ?? '';
        if ($url === '') {
            return;
        }

        $issue = [
            'id' => $group['issue_id'] ?? null,
            'type' => $issueType,
            'subtype' => $group['subtype'] ?? null,
            'class' => $this->issueName($group),
            'message' => $group['message'] ?? '',
            'occurrences' => (int) ($group['count'] ?? 0),
            'users' => (int) ($group['users_count'] ?? count($group['users'] ?? [])),
            'first_seen_at' => $group['first_seen_at'] ?? null,
            'last_seen_at' => $group['last_seen_at'] ?? null,
        ];

        if ($issueType === 'exception') {
            $issue['handled'] = $group['handled'] ?? null;
            $issue['environment'] = $group['environment'] ?? null;
            $issue['location'] = $group['location'] ?? null;
            $issue['php_version'] = $group['php_version'] ?? null;
            $issue['laravel_version'] = $group['laravel_version'] ?? null;
        }

        $payload = json_encode([
            'event' => 'issue.new',
            'app' => $appName,
            'issue' => $issue,
            'timestamp' => date('c'),
        ], JSON_INVALID_UTF8_SUBSTITUTE);

        $headers = [];
        if (! empty($config['secret'])) {
            $headers['X-NightOwl-Signature'] = hash_hmac('sha256', $payload, $config['secret']);
        }

        $this->httpPost($url, $payload, $headers);
    }

    private function sendEmail(array $config, string $appName, string $prefix, array $group, string $issueType = 'exception'): void
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
        $group['view_url'] = $this->buildViewUrl($group['issue_id'] ?? null);

        $subject = $this->sanitizeHeader("[{$appName}] New Issue: {$name}");
        $fromName = $this->sanitizeHeader($fromName);

        $body = EmailTemplate::renderIssue($appName, $group, $issueType);

        $this->smtpSend($host, $port, $username, $password, $encryption, $fromAddress, $fromName, $toAddresses, $subject, $body, true);
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
