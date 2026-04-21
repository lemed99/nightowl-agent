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

    private function computeInterval(HealthReporter $reporter, string $status): int
    {
        $method = (new ReflectionClass($reporter))->getMethod('computeInterval');

        return $method->invoke($reporter, $status);
    }
}
