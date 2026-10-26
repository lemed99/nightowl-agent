<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\MetricsCollector;
use NightOwl\Agent\RecordWriter;
use NightOwl\Agent\SqliteBuffer;
use PDO;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use ReflectionProperty;

/**
 * The drain worker's buffer failures reach BUFFER_UNWRITABLE end to end: a real
 * SqliteBuffer, a real DrainWorker, its real metrics file, read by the real
 * collector. No PostgreSQL needed — the failure under test happens before the
 * drain ever reaches it, which is exactly what made it invisible (Yomoney,
 * 2026-10-15: 19 hours of "check PostgreSQL connectivity" on a healthy database).
 *
 * The unwritable buffer is `PRAGMA query_only = ON` on the buffer's own
 * connection; two workers so the drain claims with an UPDATE (claimBatch) — the
 * single-worker fetch is a SELECT, which a read-only buffer still serves.
 */
class DrainWorkerBufferErrorSignalTest extends TestCase
{
    private string $path;

    private SqliteBuffer $buffer;

    protected function setUp(): void
    {
        $this->path = sys_get_temp_dir().'/nightowl_buffer_signal_'.uniqid().'.sqlite';
        $this->buffer = new SqliteBuffer($this->path);
        $this->buffer->appendRaw('[{"t":"request"}]');
        $this->buffer->appendRaw('[{"t":"request"}]');
    }

    protected function tearDown(): void
    {
        unset($this->buffer);
        foreach (glob($this->path.'*') ?: [] as $f) {
            @unlink($f);
        }
    }

    private function setBufferWritable(bool $writable): void
    {
        /** @var PDO $pdo */
        $pdo = (new ReflectionProperty(SqliteBuffer::class, 'pdo'))->getValue($this->buffer);
        $pdo->exec('PRAGMA query_only = '.($writable ? 'OFF' : 'ON'));
    }

    private function worker(): DrainWorker
    {
        // Port 1: nothing listens, so a drain that gets past the buffer fails
        // fast on the connection instead of reaching a database.
        $worker = new DrainWorker(
            sqlitePath: $this->path,
            pgHost: '127.0.0.1',
            pgPort: 1,
            pgDatabase: 'none',
            pgUsername: 'none',
            pgPassword: 'none',
        );
        $worker->setWorkerConfig(0, 2);

        return $worker;
    }

    private function writer(): RecordWriter
    {
        return new RecordWriter('127.0.0.1', 1, 'none', 'none', 'none', storageV2Config: false);
    }

    /**
     * Drain once, publish the worker's metrics file, and diagnose from it.
     *
     * @return array<string, array<string, mixed>> diagnoses keyed by code
     */
    private function drainAndDiagnose(DrainWorker $worker, RecordWriter $writer, MetricsCollector $collector): array
    {
        (new ReflectionMethod($worker, 'drainBatch'))->invoke($worker, $this->buffer, $writer);
        (new ReflectionMethod($worker, 'writeDrainMetrics'))->invoke($worker);

        $collector->readDrainMetrics($this->path, 2);
        $pending = $this->buffer->pendingCount();
        $collector->runDiagnosis(false, $pending, 0, 0);
        $collector->runDiagnosis(false, $pending, 0, 0);
        $status = $collector->getFullStatus(microtime(true) - 60, false, $pending, 0, 0);

        return array_column($status['diagnoses'], null, 'code');
    }

    public function test_an_unwritable_buffer_is_named_and_clears_once_writable(): void
    {
        $worker = $this->worker();
        $writer = $this->writer();
        $collector = new MetricsCollector;

        $this->setBufferWritable(false);
        $diagnoses = $this->drainAndDiagnose($worker, $writer, $collector);

        $this->assertArrayHasKey('BUFFER_UNWRITABLE', $diagnoses);
        $this->assertStringContainsString('attempt to write a readonly database', $diagnoses['BUFFER_UNWRITABLE']['message']);
        $this->assertStringContainsString('not reaching PostgreSQL', $diagnoses['BUFFER_UNWRITABLE']['message']);
        // A buffer failure stamps no Postgres clock: nothing claims the database is
        // unreachable or refusing writes.
        $this->assertArrayNotHasKey('DRAIN_UNREACHABLE', $diagnoses);
        $this->assertArrayNotHasKey('DRAIN_WRITE_FAILING', $diagnoses);
        // ...and it is not a failed Postgres batch: batches_failed feeds DRAIN_ERRORS'
        // 15-minute window, which kept saying "check PostgreSQL" long after the
        // buffer was fixed (found by review).
        $this->assertSame(0, (new ReflectionProperty($worker, 'batchesFailed'))->getValue($worker));

        // Writable again: the claim succeeds, so the buffer card clears, and the
        // drain moves on to the (unreachable) database — which is now the story.
        $this->setBufferWritable(true);
        $diagnoses = $this->drainAndDiagnose($worker, $writer, $collector);

        $this->assertArrayNotHasKey('BUFFER_UNWRITABLE', $diagnoses);
        $this->assertArrayHasKey('DRAIN_UNREACHABLE', $diagnoses);
    }

    /**
     * A running agent's open handles keep writing through a chmod or chown, so a
     * buffer owned by the wrong user only fails when a worker (re)opens it. That
     * open must report, not just crash: each respawn re-stamps the failure.
     */
    public function test_a_worker_that_cannot_open_the_buffer_reports_it(): void
    {
        if (function_exists('posix_geteuid') && posix_geteuid() === 0) {
            $this->markTestSkipped('root ignores file permissions');
        }
        // The parent's collector exists before any worker starts; a metrics file
        // older than it would be read as a previous run's.
        $collector = new MetricsCollector;
        unset($this->buffer);
        chmod($this->path, 0444);

        $worker = $this->worker();
        try {
            (new ReflectionMethod($worker, 'openBuffer'))->invoke($worker);
            $this->fail('opening a read-only buffer should throw');
        } catch (\PDOException) {
            // Expected: the respawn path is unchanged.
        } finally {
            chmod($this->path, 0644);
        }

        $collector->readDrainMetrics($this->path, 2);
        $collector->runDiagnosis(false, 2, 0, 0);
        $collector->runDiagnosis(false, 2, 0, 0);
        $diagnoses = array_column($collector->getFullStatus(microtime(true) - 60, false, 2, 0, 0)['diagnoses'], null, 'code');

        $this->assertArrayHasKey('BUFFER_UNWRITABLE', $diagnoses);
        $this->assertStringContainsString('attempt to write a readonly database', $diagnoses['BUFFER_UNWRITABLE']['message']);
    }

    /**
     * Found by review: with NIGHTOWL_DRAIN_QUARANTINE on, the worker refreshed its
     * dead-letter count before writing its metrics, unguarded. On a corrupt buffer
     * that count throws, so every respawn died before the file could carry the
     * buffer error, and the drain side never reported it.
     */
    public function test_a_failing_dead_letter_count_does_not_stop_the_metrics_file(): void
    {
        $collector = new MetricsCollector;
        $worker = new DrainWorker(
            sqlitePath: $this->path,
            pgHost: '127.0.0.1',
            pgPort: 1,
            pgDatabase: 'none',
            pgUsername: 'none',
            pgPassword: 'none',
            quarantineEnabled: true,
        );
        $worker->setWorkerConfig(0, 2);

        /** @var PDO $pdo */
        $pdo = (new ReflectionProperty(SqliteBuffer::class, 'pdo'))->getValue($this->buffer);
        $pdo->exec('DROP TABLE buffer'); // every buffer read and write now throws

        (new ReflectionMethod($worker, 'drainBatch'))->invoke($worker, $this->buffer, $this->writer());
        (new ReflectionMethod($worker, 'publishMetrics'))->invoke($worker, $this->buffer); // must not throw

        $collector->readDrainMetrics($this->path, 2);
        $collector->runDiagnosis(false, 0, 0, 0);
        $collector->runDiagnosis(false, 0, 0, 0);
        $diagnoses = array_column($collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0)['diagnoses'], null, 'code');

        $this->assertArrayHasKey('BUFFER_UNWRITABLE', $diagnoses);
    }
}
