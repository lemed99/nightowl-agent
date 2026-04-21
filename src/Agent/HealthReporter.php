<?php

namespace NightOwl\Agent;

use React\EventLoop\LoopInterface;
use React\Http\Browser;

/**
 * Non-blocking health reporter that POSTs agent status to the dashboard
 * with adaptive intervals based on health status.
 *
 * Uses react/http Browser for async HTTP — never blocks the event loop.
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
        private string $dashboardUrl,
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
        $browser = new Browser();
        $url = rtrim($this->dashboardUrl, '/') . '/agent/health';
        $instanceId = gethostname() . ':' . getmypid();

        $scheduleNext = function () use (&$scheduleNext, $browser, $url, $instanceId, $agent, $loop) {
            $status = $agent->getStatus();
            $interval = $this->computeInterval($status['status'] ?? 'healthy');

            $loop->addTimer($interval, function () use (&$scheduleNext, $browser, $url, $instanceId, $agent, $loop) {
                $status = $agent->getStatus();
                $reportId = bin2hex(random_bytes(16));

                $status['agent_instance_id'] = $instanceId;
                $status['report_id'] = $reportId;
                $status['tenant_id'] = $this->tenantId;

                $body = json_encode($status, JSON_THROW_ON_ERROR);

                $this->sendWithRetry($loop, $browser, $url, $body, $reportId, 0);

                // Schedule the next report
                $scheduleNext();
            });
        };

        $scheduleNext();
    }

    private function sendWithRetry(
        LoopInterface $loop,
        Browser $browser,
        string $url,
        string $body,
        string $reportId,
        int $attempt,
    ): void {
        $browser->post($url, [
            'Content-Type' => 'application/json',
            'Authorization' => 'Bearer ' . $this->token,
        ], $body)->then(
            function () {
                $this->consecutiveFailures = 0;
            },
            function (\Throwable $e) use ($loop, $browser, $url, $body, $reportId, $attempt) {
                $this->consecutiveFailures++;

                if ($attempt < self::MAX_RETRIES) {
                    $backoff = self::RETRY_BASE_SECONDS * (2 ** $attempt);
                    $loop->addTimer($backoff, function () use ($loop, $browser, $url, $body, $reportId, $attempt) {
                        $this->sendWithRetry($loop, $browser, $url, $body, $reportId, $attempt + 1);
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
