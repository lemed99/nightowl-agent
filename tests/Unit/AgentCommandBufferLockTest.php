<?php

namespace NightOwl\Tests\Unit;

use Illuminate\Config\Repository;
use Illuminate\Console\OutputStyle;
use Illuminate\Container\Container;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use NightOwl\Agent\Server;
use NightOwl\Agent\SqliteBuffer;
use NightOwl\Commands\AgentCommand;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * A second nightowl:agent on a buffer another agent already uses must refuse
 * to start, and say how to give it a buffer of its own.
 *
 * Several agents on one host share storage/nightowl/agent-buffer.sqlite unless
 * each is given --sqlite-path or NIGHTOWL_AGENT_SQLITE_PATH. With one drain
 * worker the drain reads rows without claiming them, so two agents on one
 * buffer wrote the same rows to Postgres twice.
 */
final class AgentCommandBufferLockTest extends TestCase
{
    private ?Container $previousContainer = null;

    private string $basePath;

    private string $bufferPath;

    private string|false $previousErrorLog = false;

    protected function setUp(): void
    {
        $this->previousContainer = Container::getInstance();
        $this->basePath = sys_get_temp_dir().'/nightowl-agent-lock-'.getmypid().'-'.uniqid();
        mkdir($this->basePath, 0755, true);
        $this->bufferPath = $this->basePath.'/agent-buffer.sqlite';
        $this->previousErrorLog = ini_set('error_log', '/dev/null');
    }

    protected function tearDown(): void
    {
        ini_set('error_log', (string) $this->previousErrorLog);
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);
        Container::setInstance($this->previousContainer);

        exec('rm -rf '.escapeshellarg($this->basePath));
    }

    public function test_a_second_agent_on_the_same_buffer_refuses_to_start(): void
    {
        $firstAgent = SqliteBuffer::acquireOwnerLock($this->bufferPath);

        try {
            $start = microtime(true);
            [$status, $text] = $this->runAgent(['buffer_lock_wait' => 1]);
            $elapsed = microtime(true) - $start;
        } finally {
            fclose($firstAgent);
        }

        $this->assertSame(1, $status, "a second agent must not start on a buffer that is in use.\n{$text}");
        $this->assertStringContainsString('is still using this buffer after 1s.', $text);
        $this->assertStringContainsString('pid '.getmypid(), $text, 'the message must name the process that holds the buffer');
        $this->assertStringContainsString($this->bufferPath, $text);
        $this->assertStringContainsString('--sqlite-path', $text);
        $this->assertStringContainsString('NIGHTOWL_AGENT_SQLITE_PATH', $text);
        $this->assertStringContainsString('Waiting up to 1s for it to exit.', $text);
        $this->assertGreaterThanOrEqual(1.0, $elapsed, 'the agent refused before the wait was up');
    }

    public function test_with_no_wait_a_held_buffer_is_refused_at_once(): void
    {
        $firstAgent = SqliteBuffer::acquireOwnerLock($this->bufferPath);

        try {
            $start = microtime(true);
            [$status, $text] = $this->runAgent(['buffer_lock_wait' => 0]);
            $elapsed = microtime(true) - $start;
        } finally {
            fclose($firstAgent);
        }

        $this->assertSame(1, $status);
        $this->assertStringContainsString('is already using this buffer.', $text);
        $this->assertStringNotContainsString('Waiting', $text);
        $this->assertLessThan(1.0, $elapsed);
    }

    /**
     * A restart or rolling deploy starts the new agent while the old one is
     * still stopping. Refusing at once made the new one exit in a loop until
     * supervisord gave up on it (FATAL after startretries). It now waits for
     * the old agent to let go.
     */
    public function test_an_agent_that_exits_during_the_wait_hands_over_the_buffer(): void
    {
        if (! function_exists('pcntl_fork') || ! function_exists('posix_kill')) {
            $this->markTestSkipped('pcntl/posix required');
        }

        $ready = $this->basePath.'/old-agent.ready';
        $pid = pcntl_fork();

        if ($pid === 0) {
            // The old agent: holds the buffer for a second, then goes away.
            // SIGKILL rather than exit, so PHPUnit's shutdown never runs here.
            $lock = SqliteBuffer::acquireOwnerLock($this->bufferPath);
            if ($lock !== null) {
                touch($ready);
                usleep(1_000_000);
            }
            posix_kill(posix_getpid(), SIGKILL);
        }

        try {
            $deadline = microtime(true) + 5.0;
            while (! is_file($ready) && microtime(true) < $deadline) {
                usleep(20_000);
            }
            $this->assertFileExists($ready, 'the stand-in old agent never took the lock');

            $output = new BufferedOutput;
            $command = $this->command(['buffer_lock_wait' => 10]);
            $command->setOutput(new OutputStyle(new ArrayInput([]), $output));

            $start = microtime(true);
            $locked = (new ReflectionMethod(AgentCommand::class, 'lockBuffer'))->invoke($command);
            $elapsed = microtime(true) - $start;
        } finally {
            pcntl_waitpid($pid, $status);
        }

        $text = $output->fetch();

        $this->assertTrue($locked, "the buffer was not taken over once the old agent exited.\n{$text}");
        $this->assertLessThan(5.0, $elapsed, 'the agent waited long after the buffer was free');
        $this->assertStringContainsString("Another agent (pid {$pid}) is using the buffer", $text);
        $this->assertStringContainsString('The other agent exited after', $text);
        $this->assertNull(SqliteBuffer::acquireOwnerLock($this->bufferPath), 'the agent must hold the lock it took over');
    }

    // ─── .gitignore next to the buffer ───────────────────────────────

    /**
     * storage/nightowl holds the buffer and the saved alert-channel list, whose
     * SMTP passwords and webhook secrets are plain text. A default Laravel
     * .gitignore does not cover it, so `git add -A` committed them.
     */
    public function test_the_default_buffer_directory_gets_a_gitignore(): void
    {
        $this->bufferPath = $this->basePath.'/storage/nightowl/agent-buffer.sqlite';

        $this->runAgentUpToItsServer();

        $this->assertSame("*\n!.gitignore\n", (string) @file_get_contents($this->basePath.'/storage/nightowl/.gitignore'));
    }

    public function test_an_existing_default_directory_gets_one_too(): void
    {
        // Every install upgraded from a version without this.
        mkdir($this->basePath.'/storage/nightowl', 0755, true);
        $this->bufferPath = $this->basePath.'/storage/nightowl/agent-buffer.sqlite';

        $this->runAgentUpToItsServer();

        $this->assertFileExists($this->basePath.'/storage/nightowl/.gitignore');
    }

    public function test_an_existing_gitignore_is_never_replaced(): void
    {
        mkdir($this->basePath.'/storage/nightowl', 0755, true);
        file_put_contents($this->basePath.'/storage/nightowl/.gitignore', "agent-buffer.sqlite*\n");
        $this->bufferPath = $this->basePath.'/storage/nightowl/agent-buffer.sqlite';

        $this->runAgentUpToItsServer();

        $this->assertSame("agent-buffer.sqlite*\n", file_get_contents($this->basePath.'/storage/nightowl/.gitignore'));
    }

    public function test_a_custom_directory_the_agent_creates_inside_the_app_gets_one(): void
    {
        $this->bufferPath = $this->basePath.'/var/nightowl/agent-buffer.sqlite';

        $this->runAgentUpToItsServer();

        $this->assertFileExists($this->basePath.'/var/nightowl/.gitignore');
    }

    public function test_a_custom_directory_that_already_existed_is_left_alone(): void
    {
        mkdir($this->basePath.'/data', 0755, true);
        $this->bufferPath = $this->basePath.'/data/agent-buffer.sqlite';

        $this->runAgentUpToItsServer();

        $this->assertFileDoesNotExist($this->basePath.'/data/.gitignore');
    }

    public function test_a_custom_directory_outside_the_app_is_left_alone(): void
    {
        $outside = sys_get_temp_dir().'/nightowl-outside-'.getmypid().'-'.uniqid();
        $this->bufferPath = $outside.'/agent-buffer.sqlite';

        try {
            $this->runAgentUpToItsServer();

            $this->assertDirectoryExists($outside);
            $this->assertFileDoesNotExist($outside.'/.gitignore');
        } finally {
            exec('rm -rf '.escapeshellarg($outside));
        }
    }

    public function test_the_agent_holds_the_lock_it_takes(): void
    {
        $command = $this->command();
        $command->setOutput(new OutputStyle(new ArrayInput([]), new BufferedOutput));

        $locked = (new ReflectionMethod(AgentCommand::class, 'lockBuffer'))->invoke($command);

        $this->assertTrue($locked);
        $this->assertNull(
            SqliteBuffer::acquireOwnerLock($this->bufferPath),
            'the lock must stay held for as long as the agent runs, not just while it is checked',
        );
    }

    /**
     * Run nightowl:agent through startup to the point where it would build its
     * server, and stop there: far enough to have opened the buffer and
     * protected its directory. The sync driver, so no lock file is created
     * first, and a Server binding that throws, so nothing listens.
     */
    private function runAgentUpToItsServer(): void
    {
        $command = $this->command();
        $command->getLaravel()->bind(Server::class, static fn () => throw new \RuntimeException('stopped before the server'));
        $memoryLimit = ini_get('memory_limit');

        try {
            $command->run(new ArrayInput(['--driver' => 'sync']), new BufferedOutput);
            $this->fail('the agent never reached its server');
        } catch (\RuntimeException $e) {
            $this->assertSame('stopped before the server', $e->getMessage());
        } finally {
            ini_set('memory_limit', (string) $memoryLimit);
        }
    }

    /** @return array{0: int, 1: string} exit status and output */
    private function runAgent(array $config = []): array
    {
        $output = new BufferedOutput;
        $memoryLimit = ini_get('memory_limit');

        try {
            $status = $this->command($config)->run(new ArrayInput(['--driver' => 'async']), $output);
        } finally {
            // handle() lifts the limit for the daemon; keep it out of the suite.
            ini_set('memory_limit', (string) $memoryLimit);
        }

        return [$status, $output->fetch()];
    }

    private function command(array $config = []): AgentCommand
    {
        $app = new Application($this->basePath);
        $app->instance('config', new Repository([
            'nightowl' => ['agent' => ['sqlite_path' => $this->bufferPath]] + $config,
        ]));
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication($app);

        $command = new AgentCommand;
        $command->setLaravel($app);

        return $command;
    }
}
