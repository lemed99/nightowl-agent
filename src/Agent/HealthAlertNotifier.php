<?php

namespace NightOwl\Agent;

use NightOwl\Support\DatabaseSslMode;
use PDO;

/**
 * Dispatches alert notifications when new health diagnoses cross the debounce
 * threshold (e.g., DRAIN_STOPPED, PG_LATENCY_CRITICAL).
 *
 * Reads alert channels from nightowl_alert_channels (cached), dispatches via
 * raw HTTP (Slack/Discord/Webhook) or raw SMTP (Email).
 *
 * The channels live in the same database whose outage is often the thing
 * being reported, and each round runs in a fresh forked child, so the
 * in-memory cache below never survives from one round to the next. Every
 * successful load is therefore also written to a sidecar file next to the
 * SQLite buffer, and a round that cannot reach the database sends to that
 * saved list instead of sending nothing (see loadChannels()). A healthy agent
 * may never run a round, so AsyncServer also refreshes the file on a timer
 * through primeChannels(), from a forked child like the rounds.
 *
 * EVERY step of a round is blocking I/O — loadChannels() opens a Postgres
 * connection, the sends are raw SMTP and HTTP — bounded only by
 * MAX_DISPATCH_SECONDS below. This used to run inline on the 10s diagnosis
 * timer, on the assumption that a new diagnosis is rare; it is not, because the
 * dispatch fires on TRANSITIONS and a struggling agent flaps. AsyncServer now
 * calls this from a short-lived forked child (see dispatchHealthAlerts) and
 * NOTHING here may be moved back onto the loop.
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

    /** How long a failed load holds before the database is tried again. */
    private const CHANNEL_RETRY_SECONDS = 30;

    /**
     * Ceiling on a whole dispatch round, checked between sends; SmtpClient
     * clamps its socket timeouts to what remains, so this bounds the health
     * timer regardless of how generous the SMTP timeouts are. Raised from 5s
     * with those timeouts — a real TLS handshake plus AUTH plus DATA did not
     * fit in the old budget, let alone several channels of it.
     */
    private const MAX_DISPATCH_SECONDS = 30.0;

    /**
     * Connect bound for the channel query, in seconds. PDO_PGSQL passes
     * ATTR_TIMEOUT to libpq as connect_timeout and defaults it to 30, so an
     * unreachable host used to hold the round for 30s before it fell back to
     * the saved list, and AsyncServer skipped every transition meanwhile.
     */
    private const CONNECT_TIMEOUT_SECONDS = 5;

    /**
     * Bound on each channel query once connected, in milliseconds. A database
     * that took the connection and then did not answer (the query waiting on
     * a lock that a migration holds on the channel table, for one) kept the
     * round alive with no limit, and AsyncServer skipped every later
     * transition as still in flight. The round then falls back to the saved
     * list, as it does when the connect fails.
     */
    private const STATEMENT_TIMEOUT_MS = 5000;

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
        private string $dashboardUrl = '',
        private ?string $appId = null,
        private ?string $channelsFile = null,
        // Appended to the DSN only when connecting. $dsn alone names the
        // database a saved list belongs to (channelsSource()).
        private ?string $sslmode = null,
    ) {
        $this->smtp = $smtp ?? new SmtpClient;
        $this->webhook = $webhook ?? new WebhookClient;
    }

    public static function fromConfig(string $instanceId = ''): self
    {
        $host = config('nightowl.database.host', '127.0.0.1');
        $port = (int) config('nightowl.database.port', 5432);
        $database = config('nightowl.database.database', 'nightowl');

        // Same sources, and the same env() fallback, as AlertNotifier uses to
        // build an issue alert's view_url.
        $appId = config('nightowl.agent.app_id') ?? env('NIGHTOWL_APP_ID');

        // The buffer's own default, resolved only when the key is missing:
        // storage_path() throws under a bare container (the agent test
        // harness), and passed as config()'s default it would run every time.
        // With no path at all there is simply no saved list.
        $sqlitePath = config('nightowl.agent.sqlite_path');
        if (! is_string($sqlitePath) || $sqlitePath === '') {
            try {
                $sqlitePath = storage_path('nightowl/agent-buffer.sqlite');
            } catch (\Throwable) {
                $sqlitePath = null;
            }
        }

        return new self(
            "pgsql:host={$host};port={$port};dbname={$database}",
            config('nightowl.database.username', 'nightowl'),
            config('nightowl.database.password', 'nightowl'),
            config('app.name', 'NightOwl'),
            $instanceId,
            SmtpClient::fromConfig(),
            null,
            (string) config('nightowl.agent.dashboard_url', 'https://usenightowl.com'),
            is_string($appId) && $appId !== '' ? $appId : null,
            // Next to the buffer, named like the drain's .drain-metrics.json.
            is_string($sqlitePath) && $sqlitePath !== '' ? $sqlitePath.'.alert-channels.json' : null,
            // NIGHTOWL_DB_SSLMODE when it is set. Otherwise none, so libpq
            // applies PGSSLMODE or its own default (DatabaseSslMode).
            DatabaseSslMode::explicit(),
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

    /**
     * The diagnoses worth waking somebody for.
     *
     * `info` diagnoses report an expected, self-clearing state (a rollup backfill
     * still reconciling after an upgrade). They belong in the health payload and
     * on /status, not in a Slack message titled "degraded" — and suppressing the
     * alert has to suppress its RECOVERY too, or an all-clear arrives for
     * something that never raised. Both dispatch paths go through sendAll(), so
     * one filter there covers both.
     *
     * @param  array<int, array{code?: string, level?: string}>  $diagnoses
     * @return array<int, array<string, mixed>>
     */
    public static function alertable(array $diagnoses): array
    {
        return array_values(array_filter(
            $diagnoses,
            static fn (array $d): bool => ($d['level'] ?? '') !== 'info',
        ));
    }

    private function sendAll(array $diagnoses, string $event, string $variant): void
    {
        $diagnoses = self::alertable($diagnoses);

        if (empty($diagnoses)) {
            return;
        }

        try {
            $channels = $this->loadChannels();
        } catch (\Throwable $e) {
            // loadChannels() handles an unreachable database itself; this only
            // catches what it did not anticipate.
            error_log("[NightOwl Agent] Health alert not sent: could not load alert channels: {$e->getMessage()}");

            return;
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
            $this->pdo = new PDO($this->connectionDsn(), $this->username, $this->password, [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                // A connect_timeout in the DSN would be overridden: PDO_PGSQL
                // appends its own, taken from this attribute (see RecordWriter).
                PDO::ATTR_TIMEOUT => self::CONNECT_TIMEOUT_SECONDS,
            ]);
        }

        return $this->pdo;
    }

    /**
     * Run $read with STATEMENT_TIMEOUT_MS in force.
     *
     * SET LOCAL in a transaction of its own, rather than an `options` startup
     * parameter, which PgBouncer refuses at connect unless it is configured to
     * ignore it, or a session SET, which a transaction-mode pooler leaves on
     * the shared server connection for whoever uses it next. Postgres only:
     * the tests stand the channel table up in SQLite.
     *
     * Any failure drops the handle, so a transaction left open is never
     * reused, and closing the connection rolls it back.
     *
     * @template T
     *
     * @param  \Closure(PDO): T  $read
     * @return T
     */
    private function read(\Closure $read): mixed
    {
        $pdo = $this->pdo();

        if ($pdo->getAttribute(PDO::ATTR_DRIVER_NAME) !== 'pgsql') {
            return $read($pdo);
        }

        try {
            $pdo->beginTransaction();
            $pdo->exec('SET LOCAL statement_timeout = '.self::STATEMENT_TIMEOUT_MS);
            $result = $read($pdo);
            $pdo->commit();

            return $result;
        } catch (\Throwable $e) {
            $this->pdo = null;

            throw $e;
        }
    }

    private function connectionDsn(): string
    {
        if ($this->sslmode === null || trim($this->sslmode) === '') {
            return $this->dsn;
        }

        return $this->dsn.';sslmode='.trim($this->sslmode);
    }

    /**
     * Read the channel list from the database and save it to the sidecar file,
     * so a later round can still alert while the database is unreachable.
     *
     * AsyncServer calls this from a forked child shortly after startup and
     * then every 15 minutes. Without it, the file was written only by a health
     * alert round, and an agent that had never sent one lost its first alert
     * during an outage. Returns whether the database answered. On failure the
     * file keeps the last list that was saved.
     */
    public function primeChannels(): bool
    {
        if ($this->channelsFile === null) {
            return false;
        }

        try {
            $this->fetchChannels(microtime(true));

            return true;
        } catch (\Throwable $e) {
            $this->pdo = null;
            $reason = trim((string) preg_replace('/\s+/', ' ', $e->getMessage()));
            error_log("[NightOwl Agent] Could not refresh the saved alert channel list at {$this->channelsFile} ({$reason}). The last saved list is kept.");

            return false;
        }
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
                $fingerprint = $this->read(static fn (PDO $pdo) => $pdo->query(
                    "SELECT COUNT(*)::text || ':' || COALESCE(MAX(updated_at)::text, '') FROM nightowl_alert_channels WHERE enabled = true"
                )->fetchColumn());

                if ($fingerprint === $this->channelFingerprint) {
                    return $this->channelCache;
                }
                // Fingerprint changed — fall through to full reload
            } catch (\Throwable) {
                return $this->channelCache;
            }
        }

        try {
            return $this->fetchChannels($now);
        } catch (\Throwable $e) {
            // Drop the handle: a connection that failed mid-query is not one to
            // reuse on the retry.
            $this->pdo = null;

            // Held briefly, so the recovered pass right after the degraded one
            // in the same round does not wait on the same dead connection. No
            // fingerprint, so the next check after the hold reloads in full.
            $this->channelCache = $this->savedChannelsFallback($e);
            $this->channelCacheExpiry = $now + self::CHANNEL_RETRY_SECONDS;
            $this->channelVersionCheckAt = $now + self::CHANNEL_RETRY_SECONDS;
            $this->channelFingerprint = null;

            return $this->channelCache;
        }
    }

    /**
     * Query the enabled channels, cache them and save them to the sidecar
     * file. Throws when the database cannot be read.
     *
     * @return array<int, array{type: string, name: string, config: array}>
     */
    private function fetchChannels(float $now): array
    {
        $rows = $this->read(static fn (PDO $pdo) => $pdo->query(
            'SELECT type, name, config, updated_at FROM nightowl_alert_channels WHERE enabled = true'
        )->fetchAll(PDO::FETCH_ASSOC));

        $channels = [];
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
            $channels[] = [
                'type' => $row['type'],
                'name' => $row['name'],
                'config' => is_array($config) ? $config : [],
            ];
        }

        $this->channelCache = $channels;
        $this->channelCacheExpiry = $now + self::CHANNEL_CACHE_TTL;
        $this->channelVersionCheckAt = $now + 30;
        $this->channelFingerprint = count($rows).':'.$maxUpdatedAt;

        // The whole list, every time, empty included: a channel deleted or
        // disabled in the dashboard must stop receiving fallback alerts too.
        $this->saveChannels($channels);

        return $channels;
    }

    // ─── Saved channel list ──────────────────────────────────────────

    /**
     * Write the channel list to the sidecar file, atomically.
     *
     * The file holds what the table holds, SMTP passwords and webhook secrets
     * included, so it is created 0600 (umask set before the file exists, not
     * chmod after it has content) and swapped in with rename(). The tmp name
     * carries the pid because agents sharing one buffer path share this file.
     *
     * @param  array<int, array{type: string, name: string, config: array}>  $channels
     */
    private function saveChannels(array $channels): void
    {
        if ($this->channelsFile === null) {
            return;
        }

        $json = json_encode([
            'source' => $this->channelsSource(),
            'channels' => $channels,
        ], JSON_INVALID_UTF8_SUBSTITUTE);

        if ($json === false || @file_get_contents($this->channelsFile) === $json) {
            return;
        }

        $tmp = $this->channelsFile.'.'.getmypid().'.tmp';

        $umask = umask(0077);
        try {
            $written = @file_put_contents($tmp, $json);
        } finally {
            umask($umask);
        }

        // Covers a tmp file left over from a crashed write, which keeps its own
        // mode. Best-effort: some mounted volumes refuse chmod outright.
        @chmod($tmp, 0600);

        if ($written === false || ! @rename($tmp, $this->channelsFile)) {
            @unlink($tmp);
            error_log("[NightOwl Agent] Could not save alert channels to {$this->channelsFile}. Health alerts cannot fall back to them while the database is unreachable.");
        }
    }

    /**
     * The saved channel list, for a round that could not read the database.
     *
     * It does not expire: an outage can outlast any TTL, and the list is only
     * ever replaced by a successful load, which also drops channels that were
     * deleted or disabled. A list saved for a different database is ignored.
     *
     * @return array<int, array{type: string, name: string, config: array}>
     */
    private function savedChannelsFallback(\Throwable $cause): array
    {
        $reason = trim((string) preg_replace('/\s+/', ' ', $cause->getMessage()));
        $raw = $this->channelsFile !== null ? @file_get_contents($this->channelsFile) : false;

        if ($raw === false) {
            error_log("[NightOwl Agent] Could not read alert channels from the database ({$reason}) and no saved list exists. Health alerts for this round are not sent.");

            return [];
        }

        $saved = json_decode($raw, true);

        if (! is_array($saved) || ! is_array($saved['channels'] ?? null)) {
            error_log("[NightOwl Agent] Could not read alert channels from the database ({$reason}) and the saved list at {$this->channelsFile} is unreadable. Health alerts for this round are not sent.");

            return [];
        }

        if (($saved['source'] ?? null) !== $this->channelsSource()) {
            error_log("[NightOwl Agent] Could not read alert channels from the database ({$reason}). The saved list at {$this->channelsFile} is for a different database, so it is not used.");

            return [];
        }

        $channels = [];
        foreach ($saved['channels'] as $channel) {
            if (is_array($channel)
                && is_string($channel['type'] ?? null)
                && is_string($channel['name'] ?? null)
                && is_array($channel['config'] ?? null)) {
                $channels[] = [
                    'type' => $channel['type'],
                    'name' => $channel['name'],
                    'config' => $channel['config'],
                ];
            }
        }

        $count = count($channels);
        error_log("[NightOwl Agent] Could not read alert channels from the database ({$reason}). Using the saved list at {$this->channelsFile} ({$count} channel".($count === 1 ? '' : 's').').');

        return $channels;
    }

    /** Which database a saved list came from. The DSN names host, port and database, and holds no secret. */
    private function channelsSource(): string
    {
        return hash('sha256', $this->dsn);
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

    /**
     * The app's Agent Health page, built like AlertNotifier::buildViewUrl():
     * null with no dashboard URL, the generic dashboard with no app id.
     */
    private function healthUrl(): ?string
    {
        $base = rtrim($this->dashboardUrl, '/');

        if ($base === '') {
            return null;
        }

        if ($this->appId === null) {
            return $base.'/dashboard';
        }

        return $base.'/dashboard/'.rawurlencode($this->appId).'/health';
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

        if (($healthUrl = $this->healthUrl()) !== null) {
            $text = rtrim($text, "\n")."\n<{$healthUrl}|View agent health>";
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

        // Angle brackets stop Discord unfurling the dashboard into a preview.
        if (($healthUrl = $this->healthUrl()) !== null) {
            $text = rtrim($text, "\n")."\n[View agent health](<{$healthUrl}>)";
        }

        $this->webhook->post($url, json_encode(['content' => $text], JSON_INVALID_UTF8_SUBSTITUTE));
    }

    private function sendWebhook(array $config, array $d, string $event, string $variant): void
    {
        $url = $config['url'] ?? '';
        if ($url === '') {
            throw new \RuntimeException('webhook channel has no url configured');
        }

        // app_id and view_url are the keys the issue payload already uses for
        // the same things. view_url is null with no dashboard URL configured.
        $payload = json_encode([
            'event' => $event,
            'app' => $this->appName,
            'app_id' => $this->appId,
            'view_url' => $this->healthUrl(),
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
        $subject = $variant === 'recovered'
            ? "[{$this->appName}] Recovered: {$d['code']}"
            : "[{$this->appName}] Agent Health: {$d['code']}";

        // HTML in the issue emails' template family, with a button to the
        // Agent Health page. It was plain text with no link at all.
        $body = EmailTemplate::renderHealth(
            $this->appName,
            $d,
            $variant,
            $this->instanceId,
            $this->healthUrl(),
            $this->dashboardUrl,
        );

        // Config extraction, header sanitising and the incomplete-config check
        // all live in SmtpClient, shared with the drain's AlertNotifier — the two
        // had drifted (this one sanitized addresses, that one did not) and only
        // one of them was ever reached by a customer testing their setup.
        $this->smtp->send($config, $subject, $body, true, $deadline);
    }
}
