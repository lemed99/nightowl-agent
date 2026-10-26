<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\AsyncServer;
use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\PayloadParser;
use NightOwl\Agent\SqliteBuffer;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use ReflectionProperty;

/**
 * The parent keeps the WAL small.
 *
 * The buffer runs with wal_autocheckpoint=0, so nothing in SQLite copies the
 * WAL back any more. Drain worker 0 did it between batches at first, and a
 * benchmark at ~9k payloads/s took the WAL to 424 MB while that worker was busy
 * (2026-10-22): a worker can sit inside one PostgreSQL write for minutes. The
 * parent never waits on PostgreSQL, and its commits are where SQLite's own
 * checkpoint used to run.
 */
class AsyncServerWalCheckpointTest extends TestCase
{
    private string $dir;

    private string $path;

    /** @var resource|null */
    private $otherHandle = null;

    protected function setUp(): void
    {
        $this->dir = sys_get_temp_dir().'/nightowl-async-ckpt-'.getmypid().'-'.uniqid();
        mkdir($this->dir);
        $this->path = $this->dir.'/agent-buffer.sqlite';
    }

    protected function tearDown(): void
    {
        if ($this->otherHandle !== null) {
            flock($this->otherHandle, LOCK_UN);
            fclose($this->otherHandle);
        }
        foreach (glob($this->dir.'/*') ?: [] as $f) {
            @unlink($f);
        }
        @rmdir($this->dir);
    }

    /** A parent with its buffer open, as listen() leaves it. */
    private function server(): AsyncServer
    {
        $server = new AsyncServer(
            parser: new PayloadParser,
            sqlitePath: $this->path,
            drainWorker: new DrainWorker(
                sqlitePath: $this->path,
                pgHost: '127.0.0.1',
                pgPort: 1,
                pgDatabase: 'none',
                pgUsername: 'none',
                pgPassword: 'none',
            ),
            healthEnabled: false,
            healthReportEnabled: false,
        );
        (new ReflectionMethod($server, 'openBuffer'))->invoke($server);

        return $server;
    }

    private function buffer(AsyncServer $server): SqliteBuffer
    {
        return (new ReflectionProperty($server, 'buffer'))->getValue($server);
    }

    private function fill(SqliteBuffer $buffer, int $rows): void
    {
        $payload = '[{"t":"request","pad":"'.str_repeat('x', 3000).'"}]';
        for ($i = 0; $i < $rows; $i++) {
            $buffer->appendRaw($payload);
        }
    }

    private function tick(AsyncServer $server): void
    {
        (new ReflectionMethod($server, 'checkpointIfWalLarge'))->invoke($server);
    }

    public function test_the_parent_copies_a_large_wal_back_into_the_buffer(): void
    {
        $server = $this->server();
        $buffer = $this->buffer($server);
        $this->fill($buffer, 1500);
        $this->assertGreaterThan(4 * 1024 * 1024, $buffer->walSize());
        $before = $buffer->fileSize();

        $this->tick($server);

        $this->assertGreaterThan($before + 4 * 1024 * 1024, $buffer->fileSize(), 'the rows reached the main file');
    }

    public function test_a_small_wal_is_left_alone(): void
    {
        $server = $this->server();
        $buffer = $this->buffer($server);
        $this->fill($buffer, 20);
        $before = $buffer->fileSize();

        $this->tick($server);

        $this->assertSame($before, $buffer->fileSize());
    }

    /** As if the parent's last checkpoint ran $seconds ago. */
    private function ageLastCheckpoint(SqliteBuffer $buffer, float $seconds): void
    {
        (new ReflectionProperty($buffer, 'lastCheckpointAt'))->setValue($buffer, microtime(true) - $seconds);
    }

    /**
     * Past 4 MB the -wal file stays over the threshold for good, so the size
     * alone would have every pass take the gate and wait for its turn. A pass
     * with nothing new written is skipped, and one that missed its turn makes
     * the next wait a second.
     */
    public function test_passes_with_nothing_to_do_cost_the_event_loop_nothing(): void
    {
        $server = $this->server();
        $buffer = $this->buffer($server);
        $this->fill($buffer, 1500);
        $this->tick($server);

        // A drain worker in the middle of a long write: any pass that tried
        // would wait its full 100ms.
        $this->otherHandle = fopen(SqliteBuffer::checkpointLockPath($this->path), 'c+');
        $this->assertTrue(flock($this->otherHandle, LOCK_SH | LOCK_NB));

        $start = microtime(true);
        $this->tick($server);
        $this->assertLessThan(0.02, microtime(true) - $start, 'nothing was written since the checkpoint');

        // Another process writes: data_version moves, and once the last
        // checkpoint is old enough the pass tries.
        (new SqliteBuffer($this->path))->appendRaw('[{"t":"request"}]');
        $this->ageLastCheckpoint($buffer, 11);
        $start = microtime(true);
        $this->tick($server);
        $this->assertGreaterThan(0.08, microtime(true) - $start, 'a write by another process counts');

        // It missed its turn, so the next pass leaves it for a second.
        $start = microtime(true);
        $this->tick($server);
        $this->assertLessThan(0.02, microtime(true) - $start);
    }

    /**
     * A quiet app past the 4 MB high-water mark. Keyed on size, every pass with
     * a write in it checkpointed: at 5 payloads/s, ten checkpoints a second,
     * each two fsyncs inside the event loop. Now one is due when the parent's
     * writes add up to ~4 MB of WAL, or 10s after the last when anything moved.
     */
    public function test_a_trickle_of_writes_past_the_high_water_mark_does_not_checkpoint_every_pass(): void
    {
        $server = $this->server();
        $buffer = $this->buffer($server);
        $this->fill($buffer, 1500);
        $this->tick($server);
        $this->assertSame(0, $buffer->writesSinceCheckpoint());
        $this->assertGreaterThan(4 * 1024 * 1024, $buffer->walSize(), 'the -wal file keeps its size');

        for ($pass = 0; $pass < 20; $pass++) {
            $this->fill($buffer, 1);
            $this->tick($server);
        }
        $this->assertSame(20, $buffer->writesSinceCheckpoint(), 'twenty passes with a write each, no checkpoint');

        $this->ageLastCheckpoint($buffer, 11);
        $this->tick($server);
        $this->assertSame(0, $buffer->writesSinceCheckpoint(), 'due once the last checkpoint is 10s old');

        $this->fill($buffer, 256);
        $this->tick($server);
        $this->assertSame(0, $buffer->writesSinceCheckpoint(), 'due at once after ~4 MB of the parent\'s own writes');
    }

    /**
     * Found by review: counting only the parent's own writes missed a drain
     * catching up on a backlog while ingest is quiet (after a PostgreSQL
     * outage, say). A 5,000-row claim and mark is ~20 MB of WAL, and only the
     * 10s rule would have caught it, letting hundreds of MB pile up per worker.
     */
    public function test_heavy_writes_by_a_drain_worker_make_a_checkpoint_due(): void
    {
        $server = $this->server();
        $buffer = $this->buffer($server);
        $this->fill($buffer, 3000);
        $this->tick($server);
        $this->assertSame(0, $buffer->uncheckpointedWalBytes());

        // A drain worker, on its own connection, claims and marks a batch.
        $worker = new SqliteBuffer($this->path);
        $rows = $worker->claimBatch(0, 3000);
        $worker->markSynced(array_column($rows, 'id'));
        unset($worker);
        $this->assertSame(0, $buffer->writesSinceCheckpoint(), 'the parent itself wrote nothing');
        $this->assertGreaterThan(4 * 1024 * 1024, $buffer->uncheckpointedWalBytes());

        $this->tick($server);

        $this->assertSame(0, $buffer->uncheckpointedWalBytes(), 'checkpointed without waiting for the 10s rule');
    }

    public function test_the_parent_waits_only_briefly_for_a_worker_holding_the_lock(): void
    {
        $server = $this->server();
        $buffer = $this->buffer($server);
        $this->fill($buffer, 1500);
        $before = $buffer->fileSize();

        // A drain worker in the middle of a write.
        $this->otherHandle = fopen(SqliteBuffer::checkpointLockPath($this->path), 'c+');
        $this->assertTrue(flock($this->otherHandle, LOCK_SH | LOCK_NB));

        $start = microtime(true);
        $this->tick($server);
        $this->assertLessThan(0.2, microtime(true) - $start, 'the event loop waits 100ms at most for a worker');
        $this->assertSame($before, $buffer->fileSize(), 'no checkpoint ran while a write held the lock');

        // A second later (the back-off after a missed turn), the write done.
        flock($this->otherHandle, LOCK_UN);
        (new ReflectionProperty($server, 'walCheckpointRetryAt'))->setValue($server, 0.0);
        $this->tick($server);
        $this->assertGreaterThan($before, $buffer->fileSize());
    }
}
