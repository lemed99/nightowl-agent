<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\HealthReporter;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

class HealthReporterTest extends TestCase
{
    public function test_default_intervals_for_status_levels(): void
    {
        $reporter = new HealthReporter('https://api.example.com', 'token');

        $this->assertSame(30, $this->computeInterval($reporter, 'healthy'));
        $this->assertSame(10, $this->computeInterval($reporter, 'degraded'));
        $this->assertSame(5, $this->computeInterval($reporter, 'critical'));
        $this->assertSame(30, $this->computeInterval($reporter, 'unknown'));
    }

    public function test_custom_intervals_override_defaults(): void
    {
        $reporter = new HealthReporter('https://api.example.com', 'token', '', [
            'healthy' => 60,
            'degraded' => 20,
            'critical' => 2,
        ]);

        $this->assertSame(60, $this->computeInterval($reporter, 'healthy'));
        $this->assertSame(20, $this->computeInterval($reporter, 'degraded'));
        $this->assertSame(2, $this->computeInterval($reporter, 'critical'));
    }

    public function test_partial_custom_intervals_fall_back_to_defaults(): void
    {
        $reporter = new HealthReporter('https://api.example.com', 'token', '', [
            'critical' => 1,
        ]);

        $this->assertSame(30, $this->computeInterval($reporter, 'healthy'));
        $this->assertSame(10, $this->computeInterval($reporter, 'degraded'));
        $this->assertSame(1, $this->computeInterval($reporter, 'critical'));
    }

    public function test_consecutive_failures_starts_at_zero(): void
    {
        $reporter = new HealthReporter('https://api.example.com', 'token');

        $prop = (new ReflectionClass($reporter))->getProperty('consecutiveFailures');

        $this->assertSame(0, $prop->getValue($reporter));
    }

    public function test_non_retryable_rejection_is_counted_and_logged(): void
    {
        // A non-2xx response (e.g. 401/422) must surface, not be silently
        // dropped: the first occurrence increments the failure counter and is
        // logged so a bad token or rejected payload is visible immediately.
        $reporter = new HealthReporter('https://api.example.com', 'token');

        $logFile = tempnam(sys_get_temp_dir(), 'nightowl-health-log');
        $previous = ini_set('error_log', $logFile);

        try {
            $this->handleFailure($reporter, 'HTTP 422 from health endpoint', false);
        } finally {
            ini_set('error_log', $previous === false ? '' : $previous);
        }

        $prop = (new ReflectionClass($reporter))->getProperty('consecutiveFailures');
        $this->assertSame(1, $prop->getValue($reporter));

        $logged = (string) file_get_contents($logFile);
        @unlink($logFile);

        $this->assertStringContainsString('Health report failed', $logged);
        $this->assertStringContainsString('HTTP 422 from health endpoint', $logged);
    }

    private function computeInterval(HealthReporter $reporter, string $status): int
    {
        $method = (new ReflectionClass($reporter))->getMethod('computeInterval');

        return $method->invoke($reporter, $status);
    }

    private function handleFailure(HealthReporter $reporter, string $reason, bool $retryable): void
    {
        // The non-retryable path returns before touching the loop/connector,
        // so real lightweight instances are safe (no connection is opened).
        $method = (new ReflectionClass($reporter))->getMethod('handleFailure');

        $method->invoke(
            $reporter,
            \React\EventLoop\Loop::get(),
            new \React\Socket\Connector(),
            'https://api.example.com/agent/health',
            '{}',
            'rpt-1',
            0,
            $reason,
            $retryable,
        );
    }
}
