<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\SqliteBuffer;
use PHPUnit\Framework\TestCase;

/**
 * One buffer, one agent. Two agents draining the same file can each write the
 * same rows to Postgres, so the second one to start must be refused, while the
 * first agent's own drain workers keep working.
 *
 * The contender always runs in a separate process: that is the case the lock
 * exists for, and it does not depend on how flock treats two handles inside
 * one process.
 */
final class SqliteBufferOwnerLockTest extends TestCase
{
    private string $dir;

    private string $bufferPath;

    protected function setUp(): void
    {
        $this->dir = sys_get_temp_dir().'/nightowl-owner-lock-'.getmypid().'-'.uniqid();
        mkdir($this->dir, 0755, true);
        $this->bufferPath = $this->dir.'/agent-buffer.sqlite';
    }

    protected function tearDown(): void
    {
        foreach (glob($this->dir.'/*') ?: [] as $file) {
            @unlink($file);
        }
        @rmdir($this->dir);
    }

    public function test_a_second_agent_cannot_take_a_held_buffer(): void
    {
        $lock = SqliteBuffer::acquireOwnerLock($this->bufferPath);
        $this->assertIsResource($lock);

        $this->assertSame(
            'held by '.getmypid(),
            $this->contend($this->bufferPath),
            'a second agent must be refused, and told which process holds the buffer',
        );

        fclose($lock);
    }

    public function test_the_lock_is_free_again_once_its_holder_closes_it(): void
    {
        $lock = SqliteBuffer::acquireOwnerLock($this->bufferPath);
        fclose($lock);

        $this->assertSame('acquired', $this->contend($this->bufferPath));
    }

    public function test_agents_on_different_buffers_do_not_contend(): void
    {
        $lock = SqliteBuffer::acquireOwnerLock($this->bufferPath);

        $this->assertSame('acquired', $this->contend($this->dir.'/second-agent.sqlite'));

        fclose($lock);
    }

    public function test_a_forked_drain_worker_exiting_does_not_release_the_agents_lock(): void
    {
        if (! function_exists('pcntl_fork')) {
            $this->markTestSkipped('pcntl required to fork.');
        }

        $lock = SqliteBuffer::acquireOwnerLock($this->bufferPath);

        $pid = pcntl_fork();
        if ($pid === 0) {
            // A worker's exit closes its copy of the descriptor. SIGKILL rather
            // than exit so PHPUnit's shutdown never runs in the child.
            fclose($lock);
            posix_kill(posix_getpid(), SIGKILL);
        }
        pcntl_waitpid($pid, $status);

        $this->assertSame('held by '.getmypid(), $this->contend($this->bufferPath));

        fclose($lock);
    }

    public function test_a_spawned_process_does_not_keep_the_lock_after_the_agent_lets_go(): void
    {
        // The agent spawns commands that can outlive it (the detached rollup
        // backfill runs for minutes) and execs its drain workers. Had they
        // inherited the descriptor, a restarted agent would refuse to start
        // until they finished.
        $lock = SqliteBuffer::acquireOwnerLock($this->bufferPath);

        $sleeper = proc_open([PHP_BINARY, '-r', 'sleep(10);'], [], $pipes);
        $this->assertIsResource($sleeper);

        try {
            fclose($lock);

            $this->assertSame('acquired', $this->contend($this->bufferPath));
        } finally {
            proc_terminate($sleeper, 9);
            proc_close($sleeper);
        }
    }

    public function test_it_creates_a_missing_buffer_directory(): void
    {
        $path = $this->dir.'/nested/agent-buffer.sqlite';

        $lock = SqliteBuffer::acquireOwnerLock($path);

        $this->assertIsResource($lock);
        $this->assertFileExists(SqliteBuffer::lockPath($path));

        fclose($lock);
        @unlink(SqliteBuffer::lockPath($path));
        @rmdir($this->dir.'/nested');
    }

    /**
     * Try the lock from a separate PHP process: 'acquired', or 'held by <pid>'.
     */
    private function contend(string $bufferPath): string
    {
        $autoload = realpath(__DIR__.'/../../vendor/autoload.php');

        $code = sprintf(
            'require %s; $lock = \NightOwl\Agent\SqliteBuffer::acquireOwnerLock(%s, $pid); echo $lock === null ? "held by ".$pid : "acquired";',
            var_export($autoload, true),
            var_export($bufferPath, true),
        );

        return trim((string) shell_exec(escapeshellarg(PHP_BINARY).' -r '.escapeshellarg($code).' 2>&1'));
    }
}
