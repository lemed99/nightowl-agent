<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\Sampler;
use PHPUnit\Framework\TestCase;

class SamplerTest extends TestCase
{
    public function testKeepsAllWithFullSampleRate(): void
    {
        $sampler = new Sampler(sampleRate: 1.0);

        for ($i = 0; $i < 100; $i++) {
            $this->assertTrue($sampler->shouldKeep([['t' => 'request', 'status_code' => 200]]));
        }
    }

    public function testRejectsAllWithZeroSampleRate(): void
    {
        $sampler = new Sampler(sampleRate: 0.0);

        // Without bypass records, all should be rejected
        $rejected = 0;
        for ($i = 0; $i < 100; $i++) {
            if (! $sampler->shouldKeep([['t' => 'request', 'status_code' => 200]])) {
                $rejected++;
            }
        }

        // With 0.0 rate, mt_rand()/mt_getrandmax() is always > 0, so all rejected
        $this->assertSame(100, $rejected);
    }

    public function testAlwaysKeepsExceptions(): void
    {
        $sampler = new Sampler(sampleRate: 0.0);

        $records = [
            ['t' => 'request', 'status_code' => 200],
            ['t' => 'exception', 'class' => 'RuntimeException'],
        ];

        for ($i = 0; $i < 50; $i++) {
            $this->assertTrue($sampler->shouldKeep($records));
        }
    }

    public function testAlwaysKeeps5xxRequests(): void
    {
        $sampler = new Sampler(sampleRate: 0.0);

        $records = [
            ['t' => 'request', 'status_code' => 500],
        ];

        for ($i = 0; $i < 50; $i++) {
            $this->assertTrue($sampler->shouldKeep($records));
        }
    }

    public function testKeeps502AsServerError(): void
    {
        $sampler = new Sampler(sampleRate: 0.0);

        $this->assertTrue($sampler->shouldKeep([
            ['t' => 'request', 'status_code' => 502],
        ]));
    }

    public function testDoesNotBypass4xxRequests(): void
    {
        $sampler = new Sampler(sampleRate: 0.0);

        $rejected = 0;
        for ($i = 0; $i < 100; $i++) {
            if (! $sampler->shouldKeep([['t' => 'request', 'status_code' => 404]])) {
                $rejected++;
            }
        }

        $this->assertSame(100, $rejected);
    }

    public function testDoesNotBypassRegularRecords(): void
    {
        $sampler = new Sampler(sampleRate: 0.0);

        $rejected = 0;
        for ($i = 0; $i < 100; $i++) {
            if (! $sampler->shouldKeep([['t' => 'query', 'sql' => 'SELECT 1']])) {
                $rejected++;
            }
        }

        $this->assertSame(100, $rejected);
    }

    public function testPartialSampleRateDropsSome(): void
    {
        $sampler = new Sampler(sampleRate: 0.5);

        $kept = 0;
        $iterations = 10_000;

        for ($i = 0; $i < $iterations; $i++) {
            if ($sampler->shouldKeep([['t' => 'request', 'status_code' => 200]])) {
                $kept++;
            }
        }

        // With 0.5 rate, expect roughly 50% kept (allow wide tolerance for randomness)
        $ratio = $kept / $iterations;
        $this->assertGreaterThan(0.35, $ratio);
        $this->assertLessThan(0.65, $ratio);
    }

    public function testHandlesNonArrayRecords(): void
    {
        $sampler = new Sampler(sampleRate: 1.0);

        // Non-array records should be skipped without error
        $this->assertTrue($sampler->shouldKeep(['not an array', 42, null]));
    }

    public function testHandlesEmptyRecords(): void
    {
        $sampler = new Sampler(sampleRate: 1.0);
        $this->assertTrue($sampler->shouldKeep([]));
    }

    public function testHandlesRecordWithoutType(): void
    {
        $sampler = new Sampler(sampleRate: 0.0);

        $rejected = 0;
        for ($i = 0; $i < 100; $i++) {
            if (! $sampler->shouldKeep([['url' => '/test']])) {
                $rejected++;
            }
        }
        $this->assertSame(100, $rejected);
    }

    public function testExceptionBypassWithMixedRecords(): void
    {
        $sampler = new Sampler(sampleRate: 0.0);

        // Exception anywhere in the array triggers bypass
        $records = [
            ['t' => 'query', 'sql' => 'SELECT 1'],
            ['t' => 'request', 'status_code' => 200],
            ['t' => 'exception', 'class' => 'Error'],
            ['t' => 'log', 'level' => 'info'],
        ];

        $this->assertTrue($sampler->shouldKeep($records));
    }

    // ─── Per-Event Sampling ──────────────────────────────────────────

    public function testRequestSampleRateOverridesDefault(): void
    {
        // Default keeps all, but request rate drops all
        $sampler = new Sampler(sampleRate: 1.0, requestRate: 0.0);

        $rejected = 0;
        for ($i = 0; $i < 100; $i++) {
            if (! $sampler->shouldKeep([['t' => 'request', 'status_code' => 200]])) {
                $rejected++;
            }
        }
        $this->assertSame(100, $rejected);
    }

    public function testCommandSampleRateOverridesDefault(): void
    {
        $sampler = new Sampler(sampleRate: 1.0, commandRate: 0.0);

        $rejected = 0;
        for ($i = 0; $i < 100; $i++) {
            if (! $sampler->shouldKeep([['t' => 'command', 'command' => 'migrate']])) {
                $rejected++;
            }
        }
        $this->assertSame(100, $rejected);
    }

    public function testScheduledTaskSampleRateOverridesDefault(): void
    {
        $sampler = new Sampler(sampleRate: 1.0, scheduledRate: 0.0);

        $rejected = 0;
        for ($i = 0; $i < 100; $i++) {
            if (! $sampler->shouldKeep([['t' => 'scheduled-task', 'command' => 'cleanup']])) {
                $rejected++;
            }
        }
        $this->assertSame(100, $rejected);
    }

    public function testPerTypeSampleRateDoesNotAffectOtherTypes(): void
    {
        // Request rate is 0, but commands should use the default (1.0)
        $sampler = new Sampler(sampleRate: 1.0, requestRate: 0.0);

        for ($i = 0; $i < 100; $i++) {
            $this->assertTrue($sampler->shouldKeep([['t' => 'command', 'command' => 'migrate']]));
        }
    }

    public function testExceptionBypassStillWorksWithPerTypeRate(): void
    {
        // Request rate is 0, but exceptions bypass
        $sampler = new Sampler(sampleRate: 0.0, requestRate: 0.0);

        $records = [
            ['t' => 'request', 'status_code' => 200],
            ['t' => 'exception', 'class' => 'Error'],
        ];

        $this->assertTrue($sampler->shouldKeep($records));
    }

    public function test5xxBypassStillWorksWithPerTypeRate(): void
    {
        $sampler = new Sampler(sampleRate: 0.0, requestRate: 0.0);

        $this->assertTrue($sampler->shouldKeep([['t' => 'request', 'status_code' => 500]]));
    }

    public function testNullPerTypeRateFallsBackToDefault(): void
    {
        // Explicit null = use default
        $sampler = new Sampler(sampleRate: 0.0, requestRate: null);

        $rejected = 0;
        for ($i = 0; $i < 100; $i++) {
            if (! $sampler->shouldKeep([['t' => 'request', 'status_code' => 200]])) {
                $rejected++;
            }
        }
        $this->assertSame(100, $rejected);
    }

    public function testPerTypeRateWithPartialSampling(): void
    {
        // Default is 0 (reject all), request rate is 1.0 (keep all requests)
        $sampler = new Sampler(sampleRate: 0.0, requestRate: 1.0);

        for ($i = 0; $i < 100; $i++) {
            $this->assertTrue($sampler->shouldKeep([['t' => 'request', 'status_code' => 200]]));
        }

        // Non-request types still use the default 0.0
        $rejected = 0;
        for ($i = 0; $i < 100; $i++) {
            if (! $sampler->shouldKeep([['t' => 'query', 'sql' => 'SELECT 1']])) {
                $rejected++;
            }
        }
        $this->assertSame(100, $rejected);
    }
}
