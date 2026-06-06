<?php

namespace NightOwl\Agent;

use React\EventLoop\LoopInterface;
use React\Socket\ConnectionInterface;
use React\Socket\Connector;

/**
 * Non-blocking health reporter that POSTs agent status to the api
 * with adaptive intervals based on health status.
 *
 * Uses react/socket Connector for async HTTP — never blocks the event loop.
 * Retries failed reports with exponential backoff (max 3 retries).
 */
final class HealthReporter
{
    private int $consecutiveFailures = 0;
    private const MAX_RETRIES = 3;
    private const RETRY_BASE_SECONDS = 2;

    /** @var array{healthy: int, degraded: int, critical: int} */
    private array $intervals;

    public function __construct(
        private string $apiUrl,
        private string $token,
        private string $tenantId = '',
        array $intervals = [],
    ) {
        $this->intervals = [
            'healthy' => $intervals['healthy'] ?? 30,
            'degraded' => $intervals['degraded'] ?? 10,
            'critical' => $intervals['critical'] ?? 5,
        ];
    }

    public function start(LoopInterface $loop, AsyncServer $agent): void
    {
        $connector = new Connector();
        $url = rtrim($this->apiUrl, '/') . '/agent/health';
        $instanceId = gethostname() . ':' . getmypid();

        $scheduleNext = function () use (&$scheduleNext, $connector, $url, $instanceId, $agent, $loop) {
            $status = $agent->getStatus();
            $interval = $this->computeInterval($status['status'] ?? 'healthy');

            $loop->addTimer($interval, function () use (&$scheduleNext, $connector, $url, $instanceId, $agent, $loop) {
                $status = $agent->getStatus();
                $reportId = bin2hex(random_bytes(16));

                $status['agent_instance_id'] = $instanceId;
                $status['report_id'] = $reportId;
                $status['tenant_id'] = $this->tenantId;

                $body = json_encode($status, JSON_THROW_ON_ERROR);

                $this->sendWithRetry($loop, $connector, $url, $body, $reportId, 0);

                $scheduleNext();
            });
        };

        $scheduleNext();
    }

    private function sendWithRetry(
        LoopInterface $loop,
        Connector $connector,
        string $url,
        string $body,
        string $reportId,
        int $attempt,
    ): void {
        $parsed = parse_url($url);
        $scheme = $parsed['scheme'] ?? 'http';
        $host = $parsed['host'] ?? 'localhost';
        $port = $parsed['port'] ?? ($scheme === 'https' ? 443 : 80);
        $path = ($parsed['path'] ?? '/') . (isset($parsed['query']) ? '?' . $parsed['query'] : '');
        $reactScheme = $scheme === 'https' ? 'tls' : 'tcp';

        $connector->connect("{$reactScheme}://{$host}:{$port}")->then(
            function (ConnectionInterface $connection) use ($loop, $connector, $url, $host, $path, $body, $reportId, $attempt) {
                $request = "POST {$path} HTTP/1.1\r\n"
                    . "Host: {$host}\r\n"
                    . "Content-Type: application/json\r\n"
                    . "Authorization: Bearer {$this->token}\r\n"
                    . "Content-Length: " . strlen($body) . "\r\n"
                    . "Connection: close\r\n"
                    . "\r\n"
                    . $body;

                $connection->write($request);

                $responseBuffer = '';
                $handled = false;
                $connection->on('data', function (string $chunk) use ($connection, &$responseBuffer, &$handled, $loop, $connector, $url, $body, $reportId, $attempt) {
                    if ($handled) {
                        return;
                    }

                    $responseBuffer .= $chunk;
                    if (! str_contains($responseBuffer, "\r\n")) {
                        return;
                    }

                    $handled = true;
                    $statusLine = strtok($responseBuffer, "\r\n");
                    $parts = explode(' ', $statusLine, 3);
                    $code = (int) ($parts[1] ?? 0);
                    $connection->close();

                    if ($code >= 200 && $code < 300) {
                        $this->consecutiveFailures = 0;

                        return;
                    }

                    // The report reached the API but was rejected. 5xx is
                    // transient (retry); a 4xx means a bad token or a payload
                    // the API won't accept — that won't fix itself, so surface
                    // it instead of silently dropping the report.
                    $this->handleFailure(
                        $loop, $connector, $url, $body, $reportId, $attempt,
                        "HTTP {$code} from health endpoint",
                        retryable: $code >= 500,
                    );
                });
            },
            function (\Throwable $e) use ($loop, $connector, $url, $body, $reportId, $attempt) {
                $this->handleFailure(
                    $loop, $connector, $url, $body, $reportId, $attempt,
                    $e->getMessage(),
                    retryable: true,
                );
            }
        );
    }

    /**
     * Handle a failed report: retry transient failures with backoff, and log
     * persistent ones. A non-retryable failure (e.g. 401/422) is logged on its
     * first occurrence so a misconfigured token or contract mismatch is visible
     * immediately rather than silently discarded.
     */
    private function handleFailure(
        LoopInterface $loop,
        Connector $connector,
        string $url,
        string $body,
        string $reportId,
        int $attempt,
        string $reason,
        bool $retryable,
    ): void {
        $this->consecutiveFailures++;

        if ($retryable && $attempt < self::MAX_RETRIES) {
            $backoff = self::RETRY_BASE_SECONDS * (2 ** $attempt);
            $loop->addTimer($backoff, function () use ($loop, $connector, $url, $body, $reportId, $attempt) {
                $this->sendWithRetry($loop, $connector, $url, $body, $reportId, $attempt + 1);
            });

            return;
        }

        if ($this->consecutiveFailures % 5 === 0 || (! $retryable && $this->consecutiveFailures === 1)) {
            error_log("[NightOwl Agent] Health report failed ({$this->consecutiveFailures} consecutive): {$reason}");
        }
    }

    private function computeInterval(string $status): int
    {
        return match ($status) {
            'degraded' => $this->intervals['degraded'],
            'critical' => $this->intervals['critical'],
            default => $this->intervals['healthy'],
        };
    }
}
