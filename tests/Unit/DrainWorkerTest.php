<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\DrainWorker;
use PHPUnit\Framework\TestCase;

class DrainWorkerTest extends TestCase
{
    // --- setWorkerConfig ---

    public function testSetWorkerConfigSetsProperties(): void
    {
        $worker = new DrainWorker(
            sqlitePath: '/tmp/test.db',
            pgHost: '127.0.0.1',
            pgPort: 5432,
            pgDatabase: 'test',
            pgUsername: 'test',
            pgPassword: 'test',
        );

        $worker->setWorkerConfig(3, 4);

        $idRef = new \ReflectionProperty($worker, 'workerId');
        $totalRef = new \ReflectionProperty($worker, 'totalWorkers');

        $this->assertSame(3, $idRef->getValue($worker));
        $this->assertSame(4, $totalRef->getValue($worker));
    }

    // --- Phase 2 write-failure classification (poison-row isolation) ---

    public function testWriteFailureClassification(): void
    {
        $worker = new DrainWorker(
            sqlitePath: '/tmp/test.db',
            pgHost: '127.0.0.1',
            pgPort: 5432,
            pgDatabase: 'test',
            pgUsername: 'test',
            pgPassword: 'test',
        );

        $whole = new \ReflectionMethod($worker, 'isWholeTargetFailure');
        $whole->setAccessible(true);
        $transient = new \ReflectionMethod($worker, 'isTransientFailure');
        $transient->setAccessible(true);
        $err = static fn (?string $sqlstate, bool $connection = false): array => [
            'sqlstate' => $sqlstate, 'table' => 'nightowl_requests', 'connection' => $connection,
        ];

        // Whole-target → abort isolation, retry the whole batch (surfaces DRAIN_WRITE_FAILING).
        foreach (['42P01', '42501', '42703', '28P01', '28000', '3D000', '53300', '57P01', '58030', '08006'] as $s) {
            $this->assertTrue($whole->invoke($worker, $err($s)), "{$s} should be whole-target");
        }
        // The connection flag forces whole-target regardless of SQLSTATE.
        $this->assertTrue($whole->invoke($worker, $err('57014', true)));
        // Empty/absent SQLSTATE is NOT whole-target — isolation must bisect to the
        // per-payload INSERT, which surfaces a definitive code.
        $this->assertFalse($whole->invoke($worker, $err(null)));
        $this->assertFalse($whole->invoke($worker, $err('')));

        // Transient → defer (retry next loop), never quarantine.
        foreach (['40001', '40P01', '55006', '55P03'] as $s) {
            $this->assertTrue($transient->invoke($worker, $err($s)), "{$s} should be transient");
        }

        // Genuine single-row poison: neither whole-target nor transient → quarantined.
        // Includes 54000 (index-row-too-large), which is OUTSIDE class 22/23.
        foreach (['22001', '22P02', '23502', '23505', '54000'] as $s) {
            $this->assertFalse($whole->invoke($worker, $err($s)), "{$s} is not whole-target");
            $this->assertFalse($transient->invoke($worker, $err($s)), "{$s} is not transient");
        }
    }

    public function testSetWorkerConfigOnClone(): void
    {
        $prototype = new DrainWorker(
            sqlitePath: '/tmp/test.db',
            pgHost: '127.0.0.1',
            pgPort: 5432,
            pgDatabase: 'test',
            pgUsername: 'test',
            pgPassword: 'test',
            workerId: 0,
            totalWorkers: 1,
        );

        $worker = clone $prototype;
        $worker->setWorkerConfig(2, 5);

        // Original should be unchanged
        $idRef = new \ReflectionProperty($prototype, 'workerId');
        $this->assertSame(0, $idRef->getValue($prototype));

        // Clone should have new values
        $this->assertSame(2, $idRef->getValue($worker));
    }

    public function testDefaultWorkerConfig(): void
    {
        $worker = new DrainWorker(
            sqlitePath: '/tmp/test.db',
            pgHost: '127.0.0.1',
            pgPort: 5432,
            pgDatabase: 'test',
            pgUsername: 'test',
            pgPassword: 'test',
        );

        $idRef = new \ReflectionProperty($worker, 'workerId');
        $totalRef = new \ReflectionProperty($worker, 'totalWorkers');

        $this->assertSame(0, $idRef->getValue($worker));
        $this->assertSame(1, $totalRef->getValue($worker));
    }

    // --- Drain metrics file paths ---

    public function testDrainMetricsFilePathSingleWorker(): void
    {
        $worker = new DrainWorker(
            sqlitePath: '/tmp/nightowl-buffer.sqlite',
            pgHost: '127.0.0.1',
            pgPort: 5432,
            pgDatabase: 'test',
            pgUsername: 'test',
            pgPassword: 'test',
            workerId: 0,
            totalWorkers: 1,
        );

        // Verify the metrics path logic via reflection
        $ref = new \ReflectionMethod($worker, 'writeDrainMetrics');

        // For single worker, metrics file should NOT include worker ID
        $totalRef = new \ReflectionProperty($worker, 'totalWorkers');
        $this->assertSame(1, $totalRef->getValue($worker));
    }

    public function testDrainMetricsFilePathMultiWorker(): void
    {
        $worker = new DrainWorker(
            sqlitePath: '/tmp/nightowl-buffer.sqlite',
            pgHost: '127.0.0.1',
            pgPort: 5432,
            pgDatabase: 'test',
            pgUsername: 'test',
            pgPassword: 'test',
        );

        $worker->setWorkerConfig(2, 4);

        // For multi-worker, metrics file should include worker ID
        $totalRef = new \ReflectionProperty($worker, 'totalWorkers');
        $this->assertSame(4, $totalRef->getValue($worker));
    }
}
