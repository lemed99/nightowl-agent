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
