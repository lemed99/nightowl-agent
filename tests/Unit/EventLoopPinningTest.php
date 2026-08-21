<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\AsyncServer;
use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\PayloadParser;
use PHPUnit\Framework\TestCase;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\EventLoop\StreamSelectLoop;
use React\EventLoop\TimerInterface;

/**
 * AsyncServer must run on StreamSelectLoop, whatever Loop::get() would pick.
 *
 * Loop::get() auto-selects a kernel-backed loop (ExtUvLoop et al.) whenever
 * its extension is loaded — ext-uv is common because Reverb's docs recommend
 * it. Those loops' backends (epoll fd / io_uring ring) are kernel objects
 * shared across pcntl_fork(), so the forked drain worker's close() of the
 * inherited servers deregisters the PARENT's listeners: the agent binds 2407
 * and 2409 but never accepts a connection, deterministically from boot, with
 * nothing in the log (GitHub issue #6). StreamSelectLoop keeps watcher state
 * in process-local PHP arrays and is the only loop the fork architecture is
 * sound on.
 *
 * CI has no ext-uv, so the deaf-agent repro itself can't run here; this pins
 * the invariant that prevents it instead. If it fails, re-read issue #6
 * before "fixing" the constructor.
 */
class EventLoopPinningTest extends TestCase
{
    protected function tearDown(): void
    {
        // Never leak the stub loop to later tests, even on assertion failure.
        Loop::set(new StreamSelectLoop);
    }

    public function test_constructor_replaces_a_kernel_backed_global_loop(): void
    {
        // Stands in for ExtUvLoop, which CI doesn't have: any LoopInterface
        // that is not StreamSelectLoop must be replaced.
        $kernelBacked = $this->stubLoop();
        Loop::set($kernelBacked);

        $server = $this->makeServer();

        $this->assertInstanceOf(StreamSelectLoop::class, Loop::get());
        $this->assertNotSame($kernelBacked, Loop::get());

        // The server must run on the loop it pinned, not a private orphan —
        // otherwise components resolving Loop::get() would tick a loop that
        // never runs.
        $property = new \ReflectionProperty(AsyncServer::class, 'loop');
        $this->assertSame(Loop::get(), $property->getValue($server));
    }

    public function test_an_existing_select_loop_is_kept_not_discarded(): void
    {
        // A harness may register watchers before constructing AsyncServer;
        // replacing an already-safe loop would silently drop them.
        $existing = new StreamSelectLoop;
        Loop::set($existing);

        $server = $this->makeServer();

        $this->assertSame($existing, Loop::get());

        $property = new \ReflectionProperty(AsyncServer::class, 'loop');
        $this->assertSame($existing, $property->getValue($server));
    }

    private function makeServer(): AsyncServer
    {
        $sqlitePath = sys_get_temp_dir().'/nightowl-loop-pinning-'.getmypid().'.sqlite';

        return new AsyncServer(
            parser: new PayloadParser,
            sqlitePath: $sqlitePath,
            drainWorker: new DrainWorker(
                sqlitePath: $sqlitePath,
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

    private function stubLoop(): LoopInterface
    {
        return new class implements LoopInterface
        {
            public function addReadStream($stream, $listener) {}

            public function addWriteStream($stream, $listener) {}

            public function removeReadStream($stream) {}

            public function removeWriteStream($stream) {}

            public function addTimer($interval, $callback)
            {
                return new class implements TimerInterface
                {
                    public function getInterval()
                    {
                        return 0.0;
                    }

                    public function getCallback()
                    {
                        return static function (): void {};
                    }

                    public function isPeriodic()
                    {
                        return false;
                    }
                };
            }

            public function addPeriodicTimer($interval, $callback)
            {
                return $this->addTimer($interval, $callback);
            }

            public function cancelTimer(TimerInterface $timer) {}

            public function futureTick($listener) {}

            public function addSignal($signal, $listener) {}

            public function removeSignal($signal, $listener) {}

            public function run() {}

            public function stop() {}
        };
    }
}
