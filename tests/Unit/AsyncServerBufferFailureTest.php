<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\AsyncServer;
use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\MetricsCollector;
use NightOwl\Agent\PayloadParser;
use NightOwl\Agent\SqliteBuffer;
use NightOwl\Support\SqliteFailure;
use PDOException;
use PHPUnit\Framework\TestCase;

/**
 * The ingest side of BUFFER_UNWRITABLE: what AsyncServer does with an append the
 * buffer refused. This is the change that closes the Yomoney gap (2026-10-15:
 * ingest 0, rejects 0, for 19 hours), so it is pinned here and not only in the
 * collector's own tests — swapping the call for a no-op must fail something.
 */
class AsyncServerBufferFailureTest extends TestCase
{
    private string $path;

    protected function setUp(): void
    {
        $this->path = sys_get_temp_dir().'/nightowl_async_bufferfail_'.uniqid().'.sqlite';
    }

    protected function tearDown(): void
    {
        foreach (glob($this->path.'*') ?: [] as $f) {
            @unlink($f);
        }
    }

    private function server(): AsyncServer
    {
        return new AsyncServer(
            parser: new PayloadParser,
            sqlitePath: $this->path,
            drainWorker: new DrainWorker(
                sqlitePath: $this->path,
                pgHost: '127.0.0.1',
                pgPort: 1,
                pgDatabase: 'x',
                pgUsername: 'u',
                pgPassword: 'p',
            ),
            healthEnabled: false,
            healthReportEnabled: false,
        );
    }

    private function metrics(AsyncServer $server): MetricsCollector
    {
        return (new \ReflectionProperty($server, 'metrics'))->getValue($server);
    }

    public function testARefusedAppendIsCountedAsARejectAndRaisesTheDiagnosis(): void
    {
        $server = $this->server();
        $refuse = new \ReflectionMethod($server, 'recordBufferWriteFailure');
        $e = new PDOException('SQLSTATE[HY000]: General error: 11 database disk image is malformed');
        $e->errorInfo = ['HY000', 11, 'database disk image is malformed'];

        for ($i = 0; $i < 3; $i++) {
            $refuse->invoke($server, $e);
        }
        $metrics = $this->metrics($server);
        $metrics->tick();
        $metrics->runDiagnosis(false, 0, 0, 0);
        $metrics->runDiagnosis(false, 0, 0, 0);
        $status = $metrics->getFullStatus(microtime(true) - 60, false, 0, 0, 0);

        $this->assertSame(3, $status['ingest']['rejected']);
        $codes = array_column($status['diagnoses'], null, 'code');
        $this->assertArrayHasKey('BUFFER_UNWRITABLE', $codes);
        $this->assertStringContainsString('database disk image is malformed', $codes['BUFFER_UNWRITABLE']['message']);
    }

    /**
     * Found by review: a COUNT that throws inside a loop timer ends the event loop,
     * health reports with it. The count falls back to the last one that answered.
     */
    public function testABacklogCountThatThrowsFallsBackToTheLastAnswer(): void
    {
        $server = $this->server();
        $buffer = new SqliteBuffer($this->path);
        $buffer->appendRaw('[{"t":"request"}]');
        (new \ReflectionProperty($server, 'buffer'))->setValue($server, $buffer);
        $count = new \ReflectionMethod($server, 'backlogCount');

        $this->assertSame(1, $count->invoke($server));

        // Drop the table out from under it: the COUNT now throws.
        $pdo = (new \ReflectionProperty(SqliteBuffer::class, 'pdo'))->getValue($buffer);
        $pdo->exec('DROP TABLE buffer');

        $this->assertSame(1, $count->invoke($server), 'the last count, not an exception');
    }

    /**
     * Found by review: the parent reopens its buffer after every fork, inside the
     * SIGCHLD handler or the restart timer, and an exception there ends the event
     * loop. A buffer made read-only under a running agent therefore stopped the
     * agent before any health report could name it. The handle now stays closed,
     * and each append retries the open and is refused with the real cause.
     */
    public function testABufferThatCannotBeReopenedIsRefusedNotFatal(): void
    {
        $server = $this->server();
        new SqliteBuffer($this->path); // the file exists, as on a running agent
        chmod($this->path, 0444);
        $open = new \ReflectionMethod($server, 'openBuffer');
        $append = new \ReflectionMethod($server, 'appendToBuffer');

        $open->invoke($server); // must not throw

        try {
            $append->invoke($server, '[{"t":"request"}]');
            $this->fail('the append should be refused');
        } catch (\Throwable $e) {
            $this->assertSame(SqliteFailure::READONLY, SqliteFailure::code($e));
        }

        // Fixed: the next append reopens the buffer and lands. All three files, as
        // the advice says: the failed open re-created -shm with the file's 0444.
        foreach (glob($this->path.'*') ?: [] as $f) {
            chmod($f, 0644);
        }
        (new \ReflectionProperty($server, 'lastBufferOpenAttempt'))->setValue($server, 0.0);
        $append->invoke($server, '[{"t":"request"}]');

        $this->assertSame(1, (new SqliteBuffer($this->path))->backlogCount());
    }

    /**
     * Found by review: releasing a dead worker's claimed rows was unguarded in the
     * SIGCHLD handler, so a worker dying on a corrupt buffer took the agent with it.
     */
    public function testReapingAWorkerSurvivesABufferThatRefusesTheRelease(): void
    {
        if (! function_exists('pcntl_fork')) {
            $this->markTestSkipped('pcntl required.');
        }
        $server = $this->server();
        $buffer = new SqliteBuffer($this->path);
        $pdo = (new \ReflectionProperty(SqliteBuffer::class, 'pdo'))->getValue($buffer);
        $pdo->exec('DROP TABLE buffer'); // releaseClaimed's UPDATE now throws
        (new \ReflectionProperty($server, 'buffer'))->setValue($server, $buffer);
        (new \ReflectionProperty($server, 'shuttingDown'))->setValue($server, true); // no respawn

        $pid = pcntl_fork();
        if ($pid === 0) {
            posix_kill(posix_getpid(), SIGKILL); // no destructors on the shared handle
        }
        (new \ReflectionProperty($server, 'drainChildPids'))->setValue($server, [0 => $pid]);
        usleep(200_000);

        (new \ReflectionMethod($server, 'reapChildren'))->invoke($server); // must not throw

        $this->assertSame([], (new \ReflectionProperty($server, 'drainChildPids'))->getValue($server));
    }
}
