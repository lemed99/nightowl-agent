<?php

namespace NightOwl\Tests\Integration;

use Illuminate\Config\Repository;
use Illuminate\Console\OutputStyle;
use Illuminate\Container\Container;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use NightOwl\Agent\AsyncServer;
use NightOwl\Agent\BufferRecovery;
use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\MetricsCollector;
use NightOwl\Agent\PayloadParser;
use NightOwl\Agent\SqliteBuffer;
use NightOwl\Commands\AgentCommand;
use NightOwl\Tests\Support\CorruptBufferFixture;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use ReflectionProperty;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * A buffer that is corrupt when the agent starts.
 *
 * The running agent repairs or sets aside a corrupt buffer on its 10-second
 * tick, but a restart (what most people do with an agent in trouble, and what
 * a redeploy does) stopped at the startup write check instead: "will not
 * start", advice about file ownership and disk space, and "to reset the
 * buffer, delete agent-buffer.sqlite*", rows not yet drained included.
 */
final class AgentCommandStartupRecoveryTest extends TestCase
{
    private ?Container $previousContainer = null;

    private string $dir;

    private string $path;

    private string|false $previousErrorLog = false;

    protected function setUp(): void
    {
        $this->previousContainer = Container::getInstance();
        $this->dir = sys_get_temp_dir().'/nightowl-startup-recovery-'.getmypid().'-'.uniqid();
        mkdir($this->dir);
        $this->path = $this->dir.'/agent-buffer.sqlite';
        $this->previousErrorLog = ini_set('error_log', '/dev/null');
    }

    protected function tearDown(): void
    {
        ini_set('error_log', (string) $this->previousErrorLog);
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);
        Container::setInstance($this->previousContainer);

        exec('rm -rf '.escapeshellarg($this->dir));
    }

    public function test_a_damaged_table_is_set_aside_and_the_agent_starts(): void
    {
        CorruptBufferFixture::tableDamaged($this->path);
        $command = $this->command();

        $this->assertTrue($this->guard($command, 'async'), $this->output);

        $copies = glob($this->path.'.corrupt-*') ?: [];
        $this->assertCount(1, array_filter($copies, fn ($f) => ! preg_match('/-(wal|shm)$/', $f)), 'the damaged file is kept, renamed');
        $buffer = new SqliteBuffer($this->path);
        $buffer->appendRaw('[{"t":"request"}]');
        $this->assertSame(1, $buffer->pendingCount(), 'a fresh buffer is in service');

        $recovery = (new ReflectionProperty($command, 'startupRecovery'))->getValue($command);
        $this->assertSame(BufferRecovery::MOVED_ASIDE, $recovery['outcome']);
        $this->assertNull(SqliteBuffer::acquireOwnerLock($this->path), 'the agent holds the buffer it recovered');
        $this->assertTrue((new ReflectionMethod($command, 'lockBuffer'))->invoke($command), 'and taking it again later is a no-op');
    }

    public function test_a_file_that_is_not_a_database_is_set_aside_and_the_agent_starts(): void
    {
        file_put_contents($this->path, str_repeat('not a database ', 400));
        $command = $this->command();

        $this->assertTrue($this->guard($command, 'async'), $this->output);
        $this->assertNotSame([], glob($this->path.'.corrupt-*'));
        $this->assertSame(0, (new SqliteBuffer($this->path))->pendingCount());
    }

    /**
     * Another agent on this buffer has it open. Repairing or renaming it under
     * that agent is what must never happen; that agent recovers it itself.
     */
    public function test_a_buffer_another_agent_holds_is_not_touched(): void
    {
        CorruptBufferFixture::tableDamaged($this->path);
        $other = SqliteBuffer::acquireOwnerLock($this->path);

        try {
            $this->assertFalse($this->guard($this->command(['buffer_lock_wait' => 0]), 'async'));
        } finally {
            fclose($other);
        }

        $this->assertSame([], glob($this->path.'.corrupt-*'), 'not renamed');
        $this->assertNotSame([], BufferRecovery::integrityProblems($this->path) ?? ['unreadable'], 'not repaired either');
        $this->assertStringContainsString('is already using this buffer', $this->output);
    }

    /** The sync driver never uses the buffer, and holds no owner lock to recover it under. */
    public function test_the_sync_driver_leaves_a_corrupt_buffer_alone_and_says_not_to_delete_it(): void
    {
        CorruptBufferFixture::tableDamaged($this->path);

        $this->assertFalse($this->guard($this->command(), 'sync'));

        $this->assertSame([], glob($this->path.'.corrupt-*'));
        $this->assertStringContainsString('the buffer file is corrupt', $this->output);
        $this->assertStringContainsString('move '.$this->path.'* aside', $this->output);
        $this->assertStringNotContainsString('delete '.$this->path, $this->output);
    }

    public function test_the_startup_recovery_reaches_the_health_report(): void
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

        $server->recordStartupBufferRecovery([
            'outcome' => BufferRecovery::MOVED_ASIDE,
            'problems' => ['database disk image is malformed'],
            'pending_rows' => 42,
            'moved_to' => $this->path.'.corrupt-20261026-101500',
        ]);

        /** @var MetricsCollector $metrics */
        $metrics = (new ReflectionProperty($server, 'metrics'))->getValue($server);
        $metrics->runDiagnosis(false, 0, 0, 0);
        $metrics->runDiagnosis(false, 0, 0, 0);
        $diagnoses = array_column($server->getStatus()['diagnoses'], null, 'code');

        $this->assertArrayHasKey('BUFFER_REPAIRED', $diagnoses);
        $this->assertSame(42, $diagnoses['BUFFER_REPAIRED']['value']);
        $this->assertStringContainsString('agent-buffer.sqlite.corrupt-20261026-101500', $diagnoses['BUFFER_REPAIRED']['message']);
        $this->assertStringNotContainsString($this->dir, $diagnoses['BUFFER_REPAIRED']['message'], 'the file name, never its path');
    }

    private string $output = '';

    private function guard(AgentCommand $command, string $driver): bool
    {
        $buffered = new BufferedOutput;
        $command->setOutput(new OutputStyle(new ArrayInput([]), $buffered));
        $result = (new ReflectionMethod($command, 'guardBufferWritable'))->invoke($command, $driver);
        $this->output = $buffered->fetch();

        return $result;
    }

    private function command(array $config = []): AgentCommand
    {
        $app = new Application($this->dir);
        $app->instance('config', new Repository([
            'nightowl' => ['agent' => ['sqlite_path' => $this->path]] + $config,
        ]));
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication($app);

        $command = new AgentCommand;
        $command->setLaravel($app);

        return $command;
    }
}
