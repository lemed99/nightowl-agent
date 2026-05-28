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
            function (ConnectionInterface $connection) use ($host, $path, $body) {
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
                $connection->on('data', function (string $chunk) use ($connection, &$responseBuffer) {
                    $responseBuffer .= $chunk;
                    if (str_contains($responseBuffer, "\r\n\r\n")) {
                        $statusLine = strtok($responseBuffer, "\r\n");
                        $parts = explode(' ', $statusLine, 3);
                        $code = (int) ($parts[1] ?? 0);
                        if ($code >= 200 && $code < 300) {
                            $this->consecutiveFailures = 0;
                        }
                        $connection->close();
                    }
                });
            },
            function (\Throwable $e) use ($loop, $connector, $url, $body, $reportId, $attempt) {
                $this->consecutiveFailures++;

                if ($attempt < self::MAX_RETRIES) {
                    $backoff = self::RETRY_BASE_SECONDS * (2 ** $attempt);
                    $loop->addTimer($backoff, function () use ($loop, $connector, $url, $body, $reportId, $attempt) {
                        $this->sendWithRetry($loop, $connector, $url, $body, $reportId, $attempt + 1);
                    });
                } elseif ($this->consecutiveFailures % 5 === 0) {
                    error_log("[NightOwl Agent] Health report failed ({$this->consecutiveFailures} consecutive): {$e->getMessage()}");
                }
            }
        );
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
