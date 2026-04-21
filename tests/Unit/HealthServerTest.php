<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\HealthServer;
use PHPUnit\Framework\TestCase;
use React\EventLoop\Loop;

class HealthServerTest extends TestCase
{
    public function test_instantiates_with_loop(): void
    {
        $server = new HealthServer(Loop::get());

        $this->assertInstanceOf(HealthServer::class, $server);
    }

    public function test_close_on_unstarted_server_is_safe(): void
    {
        $server = new HealthServer(Loop::get());

        // close() on a never-started server must not throw.
        $server->close();
        $server->close(); // idempotent

        $this->assertTrue(true);
    }
}
