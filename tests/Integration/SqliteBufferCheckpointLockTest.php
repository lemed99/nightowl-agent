<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\SqliteBuffer;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * Checkpoints and writes never run at the same time.
 *
 * SQLite's WAL-reset bug (sqlite.org/wal.html section 11, every version 3.7.0
 * to 3.51.2, which is what Ubuntu 22.04 and 24.04 ship) loses part of a
 * committed transaction when a checkpoint starts while another connection's
 * commit resets the WAL. The agent has two processes on one WAL, so the buffer
 * holds a lock file around both: writes shared, checkpoints exclusive, and
 * SQLite's own automatic checkpoint (which could not take the lock) is off.
 *
 * A lock held through a second file handle stands in for the other process:
 * flock locks belong to the open file, so two handles in one process conflict
 * exactly as two processes do.
 */
class SqliteBufferCheckpointLockTest extends TestCase
{
    private string $path;

    /** @var resource|null */
    private $otherHandle = null;

    protected function setUp(): void
    {
        $this->path = sys_get_temp_dir().'/nightowl-ckptlock-'.getmypid().'-'.uniqid().'.sqlite';
    }

    protected function tearDown(): void
    {
        if ($this->otherHandle !== null) {
            flock($this->otherHandle, LOCK_UN);
            fclose($this->otherHandle);
        }
        foreach (glob($this->path.'*') ?: [] as $f) {
            @unlink($f);
        }
    }

    /** @return resource */
    private function otherProcessHolds(int $operation)
    {
        $this->otherHandle = fopen(SqliteBuffer::checkpointLockPath($this->path), 'c+');
        $this->assertTrue(flock($this->otherHandle, $operation | LOCK_NB));

        return $this->otherHandle;
    }

    public function test_sqlite_never_checkpoints_on_its_own(): void
    {
        $buffer = new SqliteBuffer($this->path);
        $pdo = (new \ReflectionProperty($buffer, 'pdo'))->getValue($buffer);

        $this->assertSame(0, (int) $pdo->query('PRAGMA wal_autocheckpoint')->fetchColumn());

        // Past SQLite's default 1000-page trigger, the WAL keeps every frame
        // until checkpoint() runs.
        $payload = '{"pad":"'.str_repeat('x', 3000).'"}';
        for ($i = 0; $i < 1500; $i++) {
            $buffer->appendRaw($payload);
        }
        $this->assertGreaterThan(4 * 1024 * 1024, $buffer->walSize());
        $frames = $pdo->query('PRAGMA wal_checkpoint(PASSIVE)')->fetch(PDO::FETCH_NUM);
        $this->assertGreaterThan(1000, (int) $frames[1], 'the WAL held more than 1000 frames when the first checkpoint ran');
    }

    public function test_a_checkpoint_is_skipped_while_a_write_holds_the_lock(): void
    {
        $buffer = new SqliteBuffer($this->path);
        $buffer->appendRaw('{"a":1}');

        $this->otherProcessHolds(LOCK_SH);

        $this->assertFalse($buffer->checkpoint());
        $this->assertFalse($buffer->checkpointTruncate());

        flock($this->otherHandle, LOCK_UN);
        $this->assertTrue($buffer->checkpoint());
        $this->assertTrue($buffer->checkpointTruncate());
        $this->assertSame(0, $buffer->walSize());
    }

    public function test_a_checkpoint_with_no_wait_gives_up_at_once(): void
    {
        $buffer = new SqliteBuffer($this->path);
        $buffer->appendRaw('{"a":1}');

        $this->otherProcessHolds(LOCK_SH);

        $start = microtime(true);
        $this->assertFalse($buffer->checkpoint(lockWaitMs: 0));
        $this->assertLessThan(0.05, microtime(true) - $start, 'the agent\'s event loop calls this, and must not wait');

        flock($this->otherHandle, LOCK_UN);
        $this->assertTrue($buffer->checkpoint(lockWaitMs: 0));
    }

    public function test_writes_run_side_by_side_under_the_shared_lock(): void
    {
        $buffer = new SqliteBuffer($this->path);

        $this->otherProcessHolds(LOCK_SH);

        $start = microtime(true);
        $buffer->appendRaw('{"a":1}');
        $buffer->markSynced([1]);
        $this->assertLessThan(0.5, microtime(true) - $start, 'a shared hold elsewhere must not delay a write');
    }

    public function test_a_write_waits_for_a_running_checkpoint(): void
    {
        if (! function_exists('pcntl_fork')) {
            $this->markTestSkipped('needs pcntl to hold the lock from another process for a set time');
        }

        $buffer = new SqliteBuffer($this->path);
        $lockPath = SqliteBuffer::checkpointLockPath($this->path);
        $ready = $this->path.'.ready';

        $pid = pcntl_fork();
        if ($pid === 0) {
            $h = fopen($lockPath, 'c+');
            flock($h, LOCK_EX);
            touch($ready);
            usleep(600_000);
            flock($h, LOCK_UN);
            // SIGKILL, not exit(): exit would run destructors, closing the SQLite
            // handle this child inherited, which corrupts the WAL index
            // (sqlite.org/howtocorrupt.html 2.7, the agent's fork rule).
            posix_kill(getmypid(), SIGKILL);
        }

        while (! file_exists($ready)) {
            usleep(1000);
        }

        $start = microtime(true);
        $buffer->appendRaw('{"a":1}');
        $waited = microtime(true) - $start;
        pcntl_waitpid($pid, $status);

        $this->assertGreaterThan(0.4, $waited, 'the write went ahead while the checkpoint held the lock');
        $this->assertLessThan(5.0, $waited);
        $this->assertSame(1, $buffer->pendingCount());
    }

    /**
     * Two processes writing flat out always have one write in flight, so a
     * checkpoint that only waits for a gap never gets one: in a benchmark at
     * ~9k payloads/s the WAL reached 708 MB that way (2026-10-22). A waiting
     * checkpoint holds the gate, so new writes queue behind it and it only
     * waits for the ones already running.
     */
    public function test_a_checkpoint_gets_its_turn_between_busy_writers(): void
    {
        if (! function_exists('pcntl_fork')) {
            $this->markTestSkipped('needs pcntl for writers in other processes');
        }

        $buffer = new SqliteBuffer($this->path);
        $buffer->appendRaw('{"a":1}');
        $until = microtime(true) + 3.0;

        $children = [];
        for ($c = 0; $c < 2; $c++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                $writer = new SqliteBuffer($this->path);
                $payload = '{"pad":"'.str_repeat('x', 2000).'"}';
                while (microtime(true) < $until) {
                    $writer->appendRaw($payload);
                }
                // SIGKILL, not exit(): see test_a_write_waits_for_a_running_checkpoint.
                posix_kill(getmypid(), SIGKILL);
            }
            $children[] = $pid;
        }

        usleep(300_000);
        $tries = 0;
        $done = 0;
        while (microtime(true) < $until - 0.3) {
            $tries++;
            $done += $buffer->checkpoint() ? 1 : 0;
        }
        foreach ($children as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $this->assertGreaterThan(1000, $buffer->pendingCount(), 'the writers were writing');
        $this->assertSame($tries, $done, "{$done} of {$tries} checkpoints got the lock while two processes wrote");
    }

    public function test_reclaim_still_shrinks_the_file_through_the_lock(): void
    {
        $buffer = new SqliteBuffer($this->path);
        $payload = '{"pad":"'.str_repeat('x', 2000).'"}';
        for ($i = 0; $i < 3000; $i++) {
            $buffer->appendRaw($payload);
        }
        $buffer->checkpointTruncate();
        $buffer->markSynced(range(1, 3000));
        $buffer->cleanup(-1);
        $buffer->checkpointTruncate();

        $this->assertGreaterThan(0, $buffer->reclaim(2048));
    }
}
