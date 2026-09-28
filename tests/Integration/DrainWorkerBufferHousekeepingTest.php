<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\SqliteBuffer;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * One drain worker looks after the SQLite buffer file.
 *
 * Deleting drained rows, the WAL checkpoint and handing free pages back are
 * work on the one file every worker shares. With several workers each of them
 * repeated it every tick, and each pass takes the write lock the other
 * workers' claims and marks wait on. Worker 0 does it now; a single worker is
 * worker 0, so it behaves as it always did.
 *
 * The two steps of the cleanup tick that touch the buffer, called directly on
 * a real buffer file. SQLite only.
 */
final class DrainWorkerBufferHousekeepingTest extends TestCase
{
    private string $path;

    private string|false $previousErrorLog = false;

    protected function setUp(): void
    {
        $this->path = sys_get_temp_dir().'/nightowl-housekeeping-'.getmypid().'-'.uniqid().'.sqlite';
        // The TRUNCATE checkpoint logs a line each time.
        $this->previousErrorLog = ini_set('error_log', '/dev/null');
    }

    protected function tearDown(): void
    {
        ini_set('error_log', (string) $this->previousErrorLog);

        foreach (['', '-wal', '-shm'] as $suffix) {
            @unlink($this->path.$suffix);
        }
    }

    public function test_only_worker_0_deletes_drained_rows(): void
    {
        $buffer = $this->bufferWithOldDrainedRows(3);

        $this->invoke($this->worker(1), 'sweepBuffer', $buffer);
        $this->assertSame(3, $this->rowCount(), 'worker 1 swept the buffer');

        $this->invoke($this->worker(0), 'sweepBuffer', $buffer);
        $this->assertSame(0, $this->rowCount(), 'worker 0 must still sweep it');
    }

    public function test_a_single_worker_still_sweeps(): void
    {
        $buffer = $this->bufferWithOldDrainedRows(2);

        $this->invoke($this->worker(0, totalWorkers: 1), 'sweepBuffer', $buffer);

        $this->assertSame(0, $this->rowCount());
    }

    public function test_only_worker_0_checkpoints_the_wal(): void
    {
        $buffer = $this->bufferWithOldDrainedRows(50);
        $this->assertGreaterThan(0, $buffer->walSize(), 'the fixture wrote no WAL to checkpoint');
        $walBefore = $buffer->walSize();

        // A 1-byte threshold makes every checkpoint escalate to TRUNCATE,
        // which leaves an empty WAL: the visible trace of a checkpoint.
        $this->invoke($this->worker(1), 'checkpointBuffer', $buffer);
        $this->assertSame($walBefore, $buffer->walSize(), 'worker 1 checkpointed the buffer');

        $this->invoke($this->worker(0), 'checkpointBuffer', $buffer);
        $this->assertSame(0, $buffer->walSize(), 'worker 0 must still checkpoint it');
    }

    // ─── Helpers ─────────────────────────────────────────────────────

    private function worker(int $workerId, int $totalWorkers = 2): DrainWorker
    {
        return new DrainWorker(
            sqlitePath: $this->path,
            pgHost: '127.0.0.1',
            pgPort: 1,
            pgDatabase: 'x',
            pgUsername: 'x',
            pgPassword: 'x',
            workerId: $workerId,
            totalWorkers: $totalWorkers,
            checkpointTruncateBytes: 1,
        );
    }

    private function bufferWithOldDrainedRows(int $rows): SqliteBuffer
    {
        $buffer = new SqliteBuffer($this->path);
        for ($i = 0; $i < $rows; $i++) {
            $buffer->appendRaw((string) json_encode([['t' => 'log', 'i' => $i, 'pad' => str_repeat('x', 512)]]));
        }

        $ids = array_column($buffer->fetchPending($rows), 'id');
        $buffer->markSynced(array_map('intval', $ids));

        // Past cleanup()'s 300s retention.
        $this->pdo()->exec('UPDATE buffer SET created_at = 0');

        return $buffer;
    }

    private function rowCount(): int
    {
        return (int) $this->pdo()->query('SELECT COUNT(*) FROM buffer')->fetchColumn();
    }

    private function pdo(): PDO
    {
        $pdo = new PDO('sqlite:'.$this->path);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->exec('PRAGMA busy_timeout=5000');

        return $pdo;
    }

    private function invoke(DrainWorker $worker, string $method, SqliteBuffer $buffer): void
    {
        (new \ReflectionMethod(DrainWorker::class, $method))->invoke($worker, $buffer);
    }
}
