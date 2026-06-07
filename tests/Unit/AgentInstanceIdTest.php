<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\AgentInstanceId;
use PHPUnit\Framework\TestCase;

class AgentInstanceIdTest extends TestCase
{
    public function testShortHostnameIsUnchanged(): void
    {
        $this->assertSame('host1:1234', AgentInstanceId::build('host1', 1234));
    }

    public function testLongHostnameIsCappedToMaxLength(): void
    {
        // A 300-char hostname (e.g. an over-long FQDN) plus :pid must not exceed
        // the API's varchar(191) column, which would otherwise 422 the report.
        $id = AgentInstanceId::build(str_repeat('a', 300), 1048576);

        $this->assertLessThanOrEqual(AgentInstanceId::MAX_LENGTH, strlen($id));
        $this->assertStringEndsWith(':1048576', $id);
    }

    public function testKubernetesStyleHostnameFits(): void
    {
        $host = 'tapp-prod-http-worker-7d9f8b6c5d-xkq2p.eu-west-1.compute.internal';
        $id = AgentInstanceId::build($host, 1048576);

        $this->assertSame($host . ':1048576', $id);
        $this->assertLessThanOrEqual(AgentInstanceId::MAX_LENGTH, strlen($id));
    }

    public function testCurrentReturnsHostColonPid(): void
    {
        $id = AgentInstanceId::current();

        $this->assertStringEndsWith(':' . getmypid(), $id);
        $this->assertLessThanOrEqual(AgentInstanceId::MAX_LENGTH, strlen($id));
    }
}
