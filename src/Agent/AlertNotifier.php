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

    /** Lightweight polling: detect channel changes without full reload */
    private float $channelVersionCheckAt = 0;

    private ?string $channelFingerprint = null;

    /** Maximum total time for notification dispatch per flush (seconds) */
    private const MAX_NOTIFICATION_SECONDS = 5.0;

    /** @var list<array{appName: string, issueGroups: array, issueType: string, newHashes: string[]}> */
    private array $pendingNotifications = [];

    public function __construct(
        private int $cacheTtl = 86400,
        private string $frontendUrl = '',
        private ?string $appId = null,
    ) {}

    public static function fromConfig(): self
    {
        $appId = config('nightowl.agent.app_id');

        return new self(
            (int) config('nightowl.threshold_cache_ttl', 86400),
            'https://usenightowl.com',
            is_string($appId) && $appId !== '' ? $appId : null,
        );
    }

    /**
     * Stage 1: Call BEFORE the issue upsert to snapshot existing composite keys.
     * Returns the set of (group_hash|environment) keys that already exist, for diffing after upsert.
     *
     * @param  array  $issueGroups  Groups keyed by "fingerprint|environment", each carrying 'fingerprint' and 'environment'
     * @param  string  $issueType  'exception' or 'performance'
     * @return string[] Composite keys that already exist
     */
    public function snapshotExistingIssues(PDO $pdo, array $issueGroups, string $issueType): array
    {
        if (empty($issueGroups)) {
            return [];
        }

        $channels = $this->loadChannels($pdo);
        if (empty($channels)) {
            return [];
        }

        try {
            $existing = [];
            $stmt = $pdo->prepare("
                SELECT 1 FROM nightowl_issues
                WHERE group_hash = ? AND type = ? AND environment IS NOT DISTINCT FROM ? AND status != 'resolved'
                LIMIT 1
            ");

            foreach ($issueGroups as $key => $group) {
                $stmt->execute([$group['fingerprint'], $issueType, $group['environment'] ?? null]);
                if ($stmt->fetchColumn() !== false) {
                    $existing[] = $key;
                }
            }

            return $existing;
        } catch (\Throwable) {
            return [];
        }
    }

    /**
     * Stage 2: Call AFTER the upsert (still inside transaction) to queue notifications.
     * Does NOT dispatch yet — just records what needs to be sent.
     *
     * @param  string  $appName  Application name
     * @param  array  $issueGroups  Groups keyed by group_hash
     * @param  string  $issueType  'exception' or 'performance'
     * @param  string[]  $existingBefore  Hashes that existed before the upsert (from snapshotExistingIssues)
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
                    error_log('[NightOwl Agent] Notification dispatch budget exceeded ('.self::MAX_NOTIFICATION_SECONDS.'s), skipping remaining');

                    return;
                }

                $config = $channel['config'];
                $notifyEvents = $config['notify_events'] ?? null;

                if ($notifyEvents !== null && ! in_array('issue.new', $notifyEvents)) {
                    continue;
                }

                foreach ($enrichedGroups as $group) {
                    if (microtime(true) > $deadline) {
                        error_log('[NightOwl Agent] Notification dispatch budget exceeded ('.self::MAX_NOTIFICATION_SECONDS.'s), skipping remaining');

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
    private function enrichFromDb(PDO $pdo, array $group, string $compositeKey, string $issueType): array
    {
        $fingerprint = $group['fingerprint'] ?? $compositeKey;
        $environment = $group['environment'] ?? null;

        $issue = null;
        try {
            $stmt = $pdo->prepare('
                SELECT id, first_seen_at, last_seen_at, occurrences_count, users_count, subtype,
                       status, priority, threshold_ms, triggered_duration_ms
                FROM nightowl_issues
                WHERE group_hash = ? AND type = ? AND environment IS NOT DISTINCT FROM ?
                LIMIT 1
            ');
            $stmt->execute([$fingerprint, $issueType, $environment]);
            $issue = $stmt->fetch(PDO::FETCH_ASSOC);
        } catch (\Throwable) {
            // Threshold columns may not exist yet on customer DBs pending migration —
            // fall back to the legacy column set so enrichment still succeeds.
            try {
                $stmt = $pdo->prepare('
                    SELECT id, first_seen_at, last_seen_at, occurrences_count, users_count, subtype,
                           status, priority
                    FROM nightowl_issues
                    WHERE group_hash = ? AND type = ? AND environment IS NOT DISTINCT FROM ?
                    LIMIT 1
                ');
                $stmt->execute([$fingerprint, $issueType, $environment]);
                $issue = $stmt->fetch(PDO::FETCH_ASSOC);
            } catch (\Throwable) {
                // Best-effort — dispatch with whatever we have
            }
        }

        if ($issue) {
            $group['issue_id'] = (int) $issue['id'];
            $group['first_seen_at'] = $issue['first_seen_at'];
            $group['last_seen_at'] = $issue['last_seen_at'];
            $group['count'] = (int) ($issue['occurrences_count'] ?? $group['count']);
            $group['users_count'] = (int) ($issue['users_count'] ?? count($group['users']));
            $group['subtype'] = $issue['subtype'] ?? ($group['subtype'] ?? null);
            $group['status'] = $issue['status'] ?? null;
            $group['priority'] = $issue['priority'] ?? null;
            $group['threshold_ms'] = isset($issue['threshold_ms']) && $issue['threshold_ms'] !== null ? (int) $issue['threshold_ms'] : ($group['threshold_ms'] ?? null);
            $group['duration_ms'] = isset($issue['triggered_duration_ms']) && $issue['triggered_duration_ms'] !== null ? (int) $issue['triggered_duration_ms'] : ($group['duration_ms'] ?? null);
        }

        if ($issueType === 'exception') {
            try {
                $stmt = $pdo->prepare('
                    SELECT file, line, server, php_version, laravel_version, handled
                    FROM nightowl_exceptions
                    WHERE fingerprint = ? AND environment IS NOT DISTINCT FROM ?
                    ORDER BY created_at DESC
                    LIMIT 1
                ');
                $stmt->execute([$fingerprint, $environment]);
                $exc = $stmt->fetch(PDO::FETCH_ASSOC);

                if ($exc) {
                    $group['server'] = ! empty($exc['server']) ? $exc['server'] : null;
                    $group['handled'] = isset($exc['handled']) ? (bool) $exc['handled'] : null;
                    $group['php_version'] = ! empty($exc['php_version']) ? $exc['php_version'] : null;
                    $group['laravel_version'] = ! empty($exc['laravel_version']) ? $exc['laravel_version'] : null;
                    if (! empty($exc['file'])) {
                        $group['location'] = $exc['file'].(! empty($exc['line']) ? ':'.$exc['line'] : '');
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
                'SELECT type, name, config, updated_at FROM nightowl_alert_channels WHERE enabled = true'
            )->fetchAll(PDO::FETCH_ASSOC);

            $maxUpdatedAt = '';
            foreach ($rows as $row) {
                if (($row['updated_at'] ?? '') > $maxUpdatedAt) {
                    $maxUpdatedAt = $row['updated_at'];
                }
                try {
                    $cfg = json_decode((string) $row['config'], true, 32, JSON_THROW_ON_ERROR);
                } catch (\JsonException) {
                    continue;
                }
                $this->channelCache[] = [
                    'type' => $row['type'],
                    'name' => $row['name'],
                    'config' => is_array($cfg) ? $cfg : [],
                ];
            }
            $this->channelFingerprint = count($rows).':'.$maxUpdatedAt;
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
            $message = mb_substr($message, 0, 200).'...';
        }

        return $message;
    }

    private function logoUrl(): string
    {
        return rtrim($this->frontendUrl, '/').'/logo.png';
    }

    private function buildViewUrl(?int $issueId): ?string
    {
        if ($issueId === null || $this->frontendUrl === '') {
            return null;
        }

        $base = rtrim($this->frontendUrl, '/');

        if ($this->appId === null) {
            // No app_id configured (NIGHTOWL_APP_ID unset) — link to the
            // generic dashboard since we can't build the per-app issue URL.
            return $base.'/dashboard';
        }

        return "{$base}/dashboard/{$this->appId}/issues/{$issueId}";
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

        $headerEmoji = $isException ? "\xF0\x9F\x9A\xA8" : "\xE2\x9A\xA1"; // siren vs high-voltage bolt
        $headerLabel = $isException ? 'New Issue' : 'Performance Alert';

        $blocks = [];

        $blocks[] = [
            'type' => 'section',
            'text' => [
                'type' => 'mrkdwn',
                'text' => "{$headerEmoji}  *{$headerLabel}*  \xC2\xB7  {$appName}",
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
            $duration = $group['duration_ms'] ?? null;
            $threshold = $group['threshold_ms'] ?? null;
            if ($duration !== null) {
                $fields[] = ['type' => 'mrkdwn', 'text' => "*Duration*\n{$duration}ms"];
            }
            if ($threshold !== null) {
                $fields[] = ['type' => 'mrkdwn', 'text' => "*Threshold*\n{$threshold}ms"];
            }
            if ($duration !== null && $threshold !== null && $duration > $threshold) {
                $over = $duration - $threshold;
                $fields[] = ['type' => 'mrkdwn', 'text' => "*Over by*\n{$over}ms"];
            }
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

        $viewUrl = $this->buildViewUrl($issueId);
        if ($viewUrl !== null) {
            $blocks[] = [
                'type' => 'actions',
                'elements' => [
                    [
                        'type' => 'button',
                        'text' => ['type' => 'plain_text', 'text' => 'View issue'],
                        'url' => $viewUrl,
                        'style' => 'primary',
                    ],
                ],
            ];
        }

        $blocks[] = [
            'type' => 'context',
            'elements' => [['type' => 'mrkdwn', 'text' => 'NightOwl']],
        ];

        $attachmentColor = $isException ? '#DC2626' : '#F59E0B';

        $payload = [
            'username' => 'NightOwl',
            'icon_url' => $this->logoUrl(),
            'attachments' => [
                [
                    'color' => $attachmentColor,
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
            $fields[] = ['name' => 'Issue', 'value' => '#'.$issueId, 'inline' => true];
        }

        if (! empty($group['environment'])) {
            $fields[] = ['name' => 'Environment', 'value' => (string) $group['environment'], 'inline' => true];
        }

        if ($isException) {
            $statusText = ($handled === true) ? 'Handled' : 'Unhandled';
            $fields[] = ['name' => 'Status', 'value' => $statusText, 'inline' => true];
            if (! empty($group['location'])) {
                $fields[] = ['name' => 'Location', 'value' => '`'.$group['location'].'`', 'inline' => false];
            }
            if (! empty($group['laravel_version'])) {
                $fields[] = ['name' => 'Laravel', 'value' => (string) $group['laravel_version'], 'inline' => true];
            }
            if (! empty($group['php_version'])) {
                $fields[] = ['name' => 'PHP', 'value' => (string) $group['php_version'], 'inline' => true];
            }
        } else {
            $subtypeLabel = EmailTemplate::subtypeLabel($subtype);
            $fields[] = ['name' => $subtypeLabel, 'value' => '`'.$name.'`', 'inline' => false];
            $duration = $group['duration_ms'] ?? null;
            $threshold = $group['threshold_ms'] ?? null;
            if ($duration !== null) {
                $fields[] = ['name' => 'Duration', 'value' => $duration.'ms', 'inline' => true];
            }
            if ($threshold !== null) {
                $fields[] = ['name' => 'Threshold', 'value' => $threshold.'ms', 'inline' => true];
            }
            if ($duration !== null && $threshold !== null && $duration > $threshold) {
                $over = $duration - $threshold;
                $fields[] = ['name' => 'Over by', 'value' => $over.'ms', 'inline' => true];
            }
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

        $discordTitle = $isException ? "\xF0\x9F\x9A\xA8  New Issue" : "\xE2\x9A\xA1  Performance Alert";
        $discordColor = $isException ? 0xDC2626 : 0xF59E0B;

        $embed = [
            'author' => ['name' => 'NightOwl', 'icon_url' => $logoUrl],
            'title' => $discordTitle,
            'description' => $description,
            'color' => $discordColor,
            'fields' => array_slice($fields, 0, 25),
            'footer' => ['text' => 'NightOwl', 'icon_url' => $logoUrl],
            'timestamp' => date('c'),
        ];

        $viewUrl = $this->buildViewUrl($issueId);
        if ($viewUrl !== null) {
            $embed['url'] = $viewUrl;
        }

        $payload = [
            'username' => 'NightOwl',
            'avatar_url' => $logoUrl,
            'embeds' => [$embed],
        ];

        $this->httpPost($url, json_encode($payload, JSON_INVALID_UTF8_SUBSTITUTE));
    }

    private function sendWebhook(array $config, string $appName, string $prefix, array $group, string $issueType = 'exception'): void
    {
        $url = $config['url'] ?? '';
        if ($url === '') {
            return;
        }

        $title = $this->issueName($group);
        $message = $this->issueMessage($group);
        $issueId = $group['issue_id'] ?? null;
        $isException = $issueType === 'exception';

        // Canonical issue payload — same shape as the backend WebhookDispatcher
        // emits for issue.resolved/ignored/reopened. Every key is always
        // present; null for non-applicable fields. Receivers can key off
        // `event` to discriminate new issues (issue.new) vs status changes.
        $issue = [
            'issue_id' => $issueId,
            'type' => $issueType,
            'title' => $title,
            'message' => $message !== '' ? $message : null,
            'status' => $group['status'] ?? 'open',
            'priority' => $group['priority'] ?? null,
            'first_seen_at' => $group['first_seen_at'] ?? null,
            'last_seen_at' => $group['last_seen_at'] ?? null,
            'occurrences' => (int) ($group['count'] ?? 0),
            'users' => (int) ($group['users_count'] ?? count($group['users'] ?? [])),
            'handled' => $isException ? ($group['handled'] ?? null) : null,
            'environment' => $group['environment'] ?? null,
            'location' => $isException ? ($group['location'] ?? null) : null,
            'php_version' => $isException ? ($group['php_version'] ?? null) : null,
            'laravel_version' => $isException ? ($group['laravel_version'] ?? null) : null,
            'subtype' => $group['subtype'] ?? null,
            'route' => ! $isException ? $title : null,
            'action' => null,
            'threshold_ms' => ! $isException ? ($group['threshold_ms'] ?? null) : null,
            'duration_ms' => ! $isException ? ($group['duration_ms'] ?? null) : null,
        ];

        $payload = json_encode([
            'event' => 'issue.new',
            'app' => $appName,
            'app_id' => $this->appId,
            'view_url' => $this->buildViewUrl($issueId),
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

        $subjectPrefix = $issueType === 'performance' ? 'Performance Alert' : 'New Issue';
        $subject = $this->sanitizeHeader("[{$appName}] {$subjectPrefix}: {$name}");
        $fromName = $this->sanitizeHeader($fromName);

        $body = EmailTemplate::renderIssue($appName, $group, $issueType, $this->frontendUrl);

        $this->smtpSend($host, $port, $username, $password, $encryption, $fromAddress, $fromName, $toAddresses, $subject, $body, true);
    }

    // ─── Raw HTTP ────────────────────────────────────────────────────

    /**
     * Reject non-http(s) URLs before they reach file_get_contents. PHP's URL
     * wrappers include file://, phar://, compress.zlib:// etc. — a malicious
     * channel config could otherwise make the agent read local files.
     */
    private static function isSafeWebhookUrl(string $url): bool
    {
        $scheme = strtolower((string) parse_url($url, PHP_URL_SCHEME));

        return $scheme === 'http' || $scheme === 'https';
    }

    private function httpPost(string $url, string $body, array $extraHeaders = []): void
    {
        if (! self::isSafeWebhookUrl($url)) {
            error_log("[NightOwl Agent] Rejected webhook URL (scheme must be http/https): {$url}");

            return;
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
            $this->smtpCommand($socket, 'EHLO nightowl', 2);

            if ($encryption === 'tls') {
                $this->smtpCommand($socket, 'STARTTLS', 2);
                if (! stream_socket_enable_crypto($socket, true, STREAM_CRYPTO_METHOD_TLSv1_2_CLIENT | STREAM_CRYPTO_METHOD_TLSv1_3_CLIENT)) {
                    error_log('[NightOwl Agent] SMTP STARTTLS failed');

                    return;
                }
                $this->smtpCommand($socket, 'EHLO nightowl', 2);
            }

            if ($username !== '') {
                $this->smtpCommand($socket, 'AUTH LOGIN', 3);
                $this->smtpCommand($socket, base64_encode($username), 3);
                $this->smtpCommand($socket, base64_encode($password), 2);
            }

            $this->smtpCommand($socket, "MAIL FROM:<{$fromAddress}>", 2);
            foreach ($toAddresses as $to) {
                $this->smtpCommand($socket, "RCPT TO:<{$to}>", 2);
            }

            $this->smtpCommand($socket, 'DATA', 3);

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
        fwrite($socket, $command."\r\n");

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
            throw new \RuntimeException('SMTP error: '.trim($response));
        }

        return $response;
    }
}
