<?php

namespace NightOwl\Agent;

use PDO;

/**
 * Dispatches alert notifications when new health diagnoses cross the debounce
 * threshold (e.g., DRAIN_STOPPED, PG_LATENCY_CRITICAL).
 *
 * Reads alert channels from nightowl_alert_channels (cached), dispatches via
 * raw HTTP (Slack/Discord/Webhook) or raw SMTP (Email). Runs in the parent
 * process on the 10s diagnosis timer — uses blocking I/O but only fires when
 * a genuinely new diagnosis appears (rare).
 */
final class HealthAlertNotifier
{
    private ?PDO $pdo = null;

    /** @var array<int, array{type: string, name: string, config: array}> */
    private array $channelCache = [];

    private float $channelCacheExpiry = 0;

    /** Lightweight polling: detect channel changes without full reload */
    private float $channelVersionCheckAt = 0;

    private ?string $channelFingerprint = null;

    private const CHANNEL_CACHE_TTL = 3600;

    /**
     * Ceiling on a whole dispatch round, checked between sends; SmtpClient
     * clamps its socket timeouts to what remains, so this bounds the health
     * timer regardless of how generous the SMTP timeouts are. Raised from 5s
     * with those timeouts — a real TLS handshake plus AUTH plus DATA did not
     * fit in the old budget, let alone several channels of it.
     */
    private const MAX_DISPATCH_SECONDS = 30.0;

    private SmtpClient $smtp;

    private WebhookClient $webhook;

    public function __construct(
        private string $dsn,
        private string $username,
        private string $password,
        private string $appName = 'NightOwl',
        private string $instanceId = '',
        ?SmtpClient $smtp = null,
        ?WebhookClient $webhook = null,
    ) {
        $this->smtp = $smtp ?? new SmtpClient;
        $this->webhook = $webhook ?? new WebhookClient;
    }

    public static function fromConfig(string $instanceId = ''): self
    {
        $host = config('nightowl.database.host', '127.0.0.1');
        $port = (int) config('nightowl.database.port', 5432);
        $database = config('nightowl.database.database', 'nightowl');

        return new self(
            "pgsql:host={$host};port={$port};dbname={$database}",
            config('nightowl.database.username', 'nightowl'),
            config('nightowl.database.password', 'nightowl'),
            config('app.name', 'NightOwl'),
            $instanceId,
            SmtpClient::fromConfig(),
        );
    }

    /**
     * Dispatch alerts for newly active diagnoses.
     *
     * @param  array<int, array{code: string, level: string, message: string, recommendation: string, value: float|int}>  $diagnoses
     */
    public function dispatch(array $diagnoses): void
    {
        $this->sendAll($diagnoses, 'health.degraded', 'degraded');
    }

    /**
     * Dispatch recovery notifications for resolved diagnoses.
     *
     * @param  array<int, array{code: string, level: string, message: string, recommendation: string, value: float|int}>  $diagnoses
     */
    public function dispatchRecovered(array $diagnoses): void
    {
        $this->sendAll($diagnoses, 'health.recovered', 'recovered');
    }

    private function sendAll(array $diagnoses, string $event, string $variant): void
    {
        if (empty($diagnoses)) {
            return;
        }

        try {
            $channels = $this->loadChannels();
        } catch (\Throwable) {
            return; // PG unreachable — can't read channels
        }

        if (empty($channels)) {
            return;
        }

        $deadline = microtime(true) + self::MAX_DISPATCH_SECONDS;

        foreach ($diagnoses as $diagnosis) {
            foreach ($channels as $channel) {
                if (microtime(true) > $deadline) {
                    error_log('[NightOwl Agent] Health alert dispatch budget exceeded, skipping remaining');

                    return;
                }

                $notifyEvents = $channel['config']['notify_events'] ?? null;
                if ($notifyEvents !== null && ! in_array($event, $notifyEvents)) {
                    continue;
                }

                try {
                    match ($channel['type']) {
                        'slack' => $this->sendSlack($channel['config'], $diagnosis, $variant),
                        'discord' => $this->sendDiscord($channel['config'], $diagnosis, $variant),
                        'webhook' => $this->sendWebhook($channel['config'], $diagnosis, $event, $variant),
                        'email' => $this->sendEmail($channel['config'], $diagnosis, $variant, $deadline),
                        default => null,
                    };
                } catch (\Throwable $e) {
                    error_log("[NightOwl Agent] Health alert via {$channel['type']} ({$channel['name']}) failed: {$e->getMessage()}");
                }
            }
        }
    }

    // ─── Channel Loading ─────────────────────────────────────────────

    private function pdo(): PDO
    {
        if ($this->pdo === null) {
            $this->pdo = new PDO($this->dsn, $this->username, $this->password);
            $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        }

        return $this->pdo;
    }

    private function loadChannels(): array
    {
        $now = microtime(true);

        if ($now < $this->channelCacheExpiry) {
            // Periodically poll for dashboard-side channel changes
            if ($now < $this->channelVersionCheckAt) {
                return $this->channelCache;
            }

            $this->channelVersionCheckAt = $now + 30;

            try {
                $fingerprint = $this->pdo()->query(
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
        $this->channelCacheExpiry = $now + self::CHANNEL_CACHE_TTL;
        $this->channelVersionCheckAt = $now + 30;

        $rows = $this->pdo()->query(
            'SELECT type, name, config, updated_at FROM nightowl_alert_channels WHERE enabled = true'
        )->fetchAll(PDO::FETCH_ASSOC);

        $maxUpdatedAt = '';
        foreach ($rows as $row) {
            if (($row['updated_at'] ?? '') > $maxUpdatedAt) {
                $maxUpdatedAt = $row['updated_at'];
            }
            try {
                $config = json_decode((string) $row['config'], true, 32, JSON_THROW_ON_ERROR);
            } catch (\JsonException) {
                continue;
            }
            $this->channelCache[] = [
                'type' => $row['type'],
                'name' => $row['name'],
                'config' => is_array($config) ? $config : [],
            ];
        }
        $this->channelFingerprint = count($rows).':'.$maxUpdatedAt;

        return $this->channelCache;
    }

    // ─── Formatting ──────────────────────────────────────────────────

    private function severityEmoji(string $level): string
    {
        return match ($level) {
            'critical' => '🔴',
            'warning' => '🟡',
            default => 'ℹ️',
        };
    }

    private function instanceLabel(): string
    {
        return $this->instanceId !== '' ? " ({$this->instanceId})" : '';
    }

    // ─── Dispatch ────────────────────────────────────────────────────

    private function sendSlack(array $config, array $d, string $variant): void
    {
        $url = $config['webhook_url'] ?? '';
        if ($url === '') {
            throw new \RuntimeException('slack channel has no webhook_url configured');
        }

        if ($variant === 'recovered') {
            $text = "✅ *[{$this->appName}] Recovered*{$this->instanceLabel()}\n";
            $text .= "*{$d['code']}* — {$d['message']} (resolved)";
        } else {
            $emoji = $this->severityEmoji($d['level']);
            $text = "{$emoji} *[{$this->appName}] Agent Health Alert*{$this->instanceLabel()}\n";
            $text .= "*{$d['code']}* — {$d['message']}\n";
            if (! empty($d['recommendation'])) {
                $text .= "_{$d['recommendation']}_";
            }
        }

        $this->webhook->post($url, json_encode(['text' => $text], JSON_INVALID_UTF8_SUBSTITUTE));
    }

    private function sendDiscord(array $config, array $d, string $variant): void
    {
        $url = $config['webhook_url'] ?? '';
        if ($url === '') {
            throw new \RuntimeException('discord channel has no webhook_url configured');
        }

        if ($variant === 'recovered') {
            $text = "✅ **[{$this->appName}] Recovered**{$this->instanceLabel()}\n";
            $text .= "**{$d['code']}** — {$d['message']} (resolved)";
        } else {
            $emoji = $this->severityEmoji($d['level']);
            $text = "{$emoji} **[{$this->appName}] Agent Health Alert**{$this->instanceLabel()}\n";
            $text .= "**{$d['code']}** — {$d['message']}\n";
            if (! empty($d['recommendation'])) {
                $text .= "_{$d['recommendation']}_";
            }
        }

        $this->webhook->post($url, json_encode(['content' => $text], JSON_INVALID_UTF8_SUBSTITUTE));
    }

    private function sendWebhook(array $config, array $d, string $event, string $variant): void
    {
        $url = $config['url'] ?? '';
        if ($url === '') {
            throw new \RuntimeException('webhook channel has no url configured');
        }

        $payload = json_encode([
            'event' => $event,
            'app' => $this->appName,
            'instance' => $this->instanceId,
            'diagnosis' => [
                'code' => $d['code'],
                'level' => $d['level'],
                'message' => $d['message'],
                'recommendation' => $d['recommendation'],
                'status' => $variant === 'recovered' ? 'resolved' : 'active',
            ],
            'timestamp' => date('c'),
        ], JSON_INVALID_UTF8_SUBSTITUTE);

        $headers = [];
        if (! empty($config['secret'])) {
            $headers['X-NightOwl-Signature'] = hash_hmac('sha256', $payload, $config['secret']);
        }

        $this->webhook->post($url, $payload, $headers);
    }

    private function sendEmail(array $config, array $d, string $variant, ?float $deadline = null): void
    {
        if ($variant === 'recovered') {
            $subject = "[{$this->appName}] Recovered: {$d['code']}";
            $body = "Agent Health Recovered — {$this->appName}{$this->instanceLabel()}\n\n";
            $body .= "{$d['code']} — {$d['message']} (resolved)\n";
        } else {
            $subject = "[{$this->appName}] Agent Health: {$d['code']}";
            $body = "Agent Health Alert — {$this->appName}{$this->instanceLabel()}\n\n";
            $body .= strtoupper($d['level']).": {$d['code']}\n";
            $body .= "{$d['message']}\n";
            if (! empty($d['recommendation'])) {
                $body .= "\nRecommendation: {$d['recommendation']}\n";
            }
        }

        // Config extraction, header sanitising and the incomplete-config check
        // all live in SmtpClient, shared with the drain's AlertNotifier — the two
        // had drifted (this one sanitized addresses, that one did not) and only
        // one of them was ever reached by a customer testing their setup.
        $this->smtp->send($config, $subject, $body, false, $deadline);
    }
}
