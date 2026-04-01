<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\PayloadParser;
use NightOwl\Agent\Redactor;
use NightOwl\Agent\Sampler;
use NightOwl\Tests\Simulator\NightwatchSimulator;
use PHPUnit\Framework\TestCase;

/**
 * Tests that the simulator produces payloads the agent can actually parse.
 * Validates the full pipeline: simulator → wire format → parser → sampler → redactor.
 */
class SimulatorPayloadTest extends TestCase
{
    private NightwatchSimulator $sim;
    private PayloadParser $parser;
    private string $token = 'test-token-abc123';

    protected function setUp(): void
    {
        $this->sim = new NightwatchSimulator($this->token);
        $this->parser = new PayloadParser(gzipEnabled: true);
    }

    /**
     * Build a wire-format string from records (same as simulator's send, but without TCP).
     */
    private function buildWirePayload(array $records): string
    {
        $json = json_encode($records, JSON_THROW_ON_ERROR);
        $tokenHash = substr(hash('xxh128', $this->token), 0, 7);
        $body = "v1:{$tokenHash}:{$json}";

        return strlen($body) . ':' . $body;
    }

    // ─── Record type tests ─────────────────────────────────

    public function testRequestRecordParsesCorrectly(): void
    {
        $record = $this->sim->makeRequest();
        $wire = $this->buildWirePayload([$record]);

        $result = $this->parser->parse($wire);

        $this->assertNotNull($result);
        $this->assertSame('json', $result['type']);
        $this->assertCount(1, $result['records']);
        $this->assertSame('request', $result['records'][0]['t']);
        $this->assertArrayHasKey('trace_id', $result['records'][0]);
        $this->assertArrayHasKey('method', $result['records'][0]);
        $this->assertArrayHasKey('url', $result['records'][0]);
        $this->assertArrayHasKey('status_code', $result['records'][0]);
        $this->assertArrayHasKey('duration', $result['records'][0]);
    }

    public function testQueryRecordParsesCorrectly(): void
    {
        $record = $this->sim->makeQuery();
        $wire = $this->buildWirePayload([$record]);

        $result = $this->parser->parse($wire);

        $this->assertSame('query', $result['records'][0]['t']);
        $this->assertArrayHasKey('sql', $result['records'][0]);
        $this->assertArrayHasKey('duration', $result['records'][0]);
        $this->assertArrayHasKey('connection', $result['records'][0]);
    }

    public function testExceptionRecordParsesCorrectly(): void
    {
        $record = $this->sim->makeException();
        $wire = $this->buildWirePayload([$record]);

        $result = $this->parser->parse($wire);

        $this->assertSame('exception', $result['records'][0]['t']);
        $this->assertArrayHasKey('class', $result['records'][0]);
        $this->assertArrayHasKey('message', $result['records'][0]);
        $this->assertArrayHasKey('trace', $result['records'][0]);
        $this->assertArrayHasKey('fingerprint', $result['records'][0]);
    }

    public function testJobRecordParsesCorrectly(): void
    {
        $record = $this->sim->makeJob();
        $wire = $this->buildWirePayload([$record]);

        $result = $this->parser->parse($wire);

        $this->assertSame('queued-job', $result['records'][0]['t']);
        $this->assertArrayHasKey('name', $result['records'][0]);
        $this->assertArrayHasKey('queue', $result['records'][0]);
        $this->assertArrayHasKey('status', $result['records'][0]);
    }

    public function testCommandRecordParsesCorrectly(): void
    {
        $record = $this->sim->makeCommand();
        $wire = $this->buildWirePayload([$record]);

        $result = $this->parser->parse($wire);

        $this->assertSame('command', $result['records'][0]['t']);
        $this->assertArrayHasKey('command', $result['records'][0]);
        $this->assertArrayHasKey('exit_code', $result['records'][0]);
    }

    public function testScheduledTaskRecordParsesCorrectly(): void
    {
        $record = $this->sim->makeScheduledTask();
        $wire = $this->buildWirePayload([$record]);

        $result = $this->parser->parse($wire);

        $this->assertSame('scheduled-task', $result['records'][0]['t']);
        $this->assertArrayHasKey('name', $result['records'][0]);
        $this->assertArrayHasKey('cron', $result['records'][0]);
    }

    public function testCacheEventRecordParsesCorrectly(): void
    {
        $record = $this->sim->makeCacheEvent();
        $wire = $this->buildWirePayload([$record]);

        $result = $this->parser->parse($wire);

        $this->assertSame('cache-event', $result['records'][0]['t']);
        $this->assertArrayHasKey('type', $result['records'][0]);
        $this->assertArrayHasKey('key', $result['records'][0]);
    }

    public function testMailRecordParsesCorrectly(): void
    {
        $record = $this->sim->makeMail();
        $wire = $this->buildWirePayload([$record]);

        $result = $this->parser->parse($wire);

        $this->assertSame('mail', $result['records'][0]['t']);
        $this->assertArrayHasKey('class', $result['records'][0]);
        $this->assertArrayHasKey('subject', $result['records'][0]);
    }

    public function testNotificationRecordParsesCorrectly(): void
    {
        $record = $this->sim->makeNotification();
        $wire = $this->buildWirePayload([$record]);

        $result = $this->parser->parse($wire);

        $this->assertSame('notification', $result['records'][0]['t']);
        $this->assertArrayHasKey('class', $result['records'][0]);
        $this->assertArrayHasKey('channel', $result['records'][0]);
    }

    public function testOutgoingRequestRecordParsesCorrectly(): void
    {
        $record = $this->sim->makeOutgoingRequest();
        $wire = $this->buildWirePayload([$record]);

        $result = $this->parser->parse($wire);

        $this->assertSame('outgoing-request', $result['records'][0]['t']);
        $this->assertArrayHasKey('method', $result['records'][0]);
        $this->assertArrayHasKey('url', $result['records'][0]);
        $this->assertArrayHasKey('status_code', $result['records'][0]);
    }

    public function testLogRecordParsesCorrectly(): void
    {
        $record = $this->sim->makeLog();
        $wire = $this->buildWirePayload([$record]);

        $result = $this->parser->parse($wire);

        $this->assertSame('log', $result['records'][0]['t']);
        $this->assertArrayHasKey('level', $result['records'][0]);
        $this->assertArrayHasKey('message', $result['records'][0]);
    }

    public function testUserRecordParsesCorrectly(): void
    {
        $record = $this->sim->makeUser('user_42');
        $wire = $this->buildWirePayload([$record]);

        $result = $this->parser->parse($wire);

        $this->assertSame('user', $result['records'][0]['t']);
        $this->assertSame('user_42', $result['records'][0]['id']);
    }

    // ─── Mixed payload tests ───────────────────────────────

    public function testMixedPayloadWithAllRecordTypes(): void
    {
        $records = [
            $this->sim->makeRequest(),
            $this->sim->makeQuery(),
            $this->sim->makeException(),
            $this->sim->makeJob(),
            $this->sim->makeCommand(),
            $this->sim->makeScheduledTask(),
            $this->sim->makeCacheEvent(),
            $this->sim->makeMail(),
            $this->sim->makeNotification(),
            $this->sim->makeOutgoingRequest(),
            $this->sim->makeLog(),
            $this->sim->makeUser('user_1'),
        ];

        $wire = $this->buildWirePayload($records);
        $result = $this->parser->parse($wire);

        $this->assertSame('json', $result['type']);
        $this->assertCount(12, $result['records']);

        $types = array_column($result['records'], 't');
        $this->assertContains('request', $types);
        $this->assertContains('query', $types);
        $this->assertContains('exception', $types);
        $this->assertContains('queued-job', $types);
        $this->assertContains('command', $types);
        $this->assertContains('scheduled-task', $types);
        $this->assertContains('cache-event', $types);
        $this->assertContains('mail', $types);
        $this->assertContains('notification', $types);
        $this->assertContains('outgoing-request', $types);
        $this->assertContains('log', $types);
        $this->assertContains('user', $types);
    }

    // ─── Sampler integration ───────────────────────────────

    public function testSamplerKeepsExceptionPayloads(): void
    {
        $sampler = new Sampler(sampleRate: 0.0);

        $records = [
            $this->sim->makeRequest(['status_code' => 200]),
            $this->sim->makeException(),
        ];

        $this->assertTrue($sampler->shouldKeep($records));
    }

    public function testSamplerKeeps5xxPayloads(): void
    {
        $sampler = new Sampler(sampleRate: 0.0);

        $records = [
            $this->sim->makeRequest(['status_code' => 500]),
        ];

        $this->assertTrue($sampler->shouldKeep($records));
    }

    public function testSamplerDropsNormalPayloadsAtZeroRate(): void
    {
        $sampler = new Sampler(sampleRate: 0.0);

        $records = [
            $this->sim->makeRequest(['status_code' => 200]),
            $this->sim->makeQuery(),
        ];

        $this->assertFalse($sampler->shouldKeep($records));
    }

    // ─── Redactor integration ──────────────────────────────

    public function testRedactorRedactsSimulatorPayloads(): void
    {
        $redactor = new Redactor(keys: ['ip', 'headers', 'context'], enabled: true);

        $records = [
            $this->sim->makeRequest(['ip' => '10.0.0.1']),
        ];

        [$result, $modified] = $redactor->redact($records);

        $this->assertTrue($modified);
        $this->assertSame('[REDACTED]', $result[0]['ip']);
        $this->assertSame('[REDACTED]', $result[0]['headers']);
    }

    // ─── Large payload stress test ─────────────────────────

    public function testLargePayloadParses(): void
    {
        $records = [];
        for ($i = 0; $i < 100; $i++) {
            $records[] = $this->sim->makeRequest();
        }

        $wire = $this->buildWirePayload($records);
        $result = $this->parser->parse($wire);

        $this->assertSame('json', $result['type']);
        $this->assertCount(100, $result['records']);
    }

    // ─── Token hash validation ─────────────────────────────

    public function testTokenHashMatchesExpected(): void
    {
        $records = [$this->sim->makeRequest()];
        $wire = $this->buildWirePayload($records);

        $result = $this->parser->parse($wire);

        $expectedHash = substr(hash('xxh128', $this->token), 0, 7);
        $this->assertSame($expectedHash, $result['tokenHash']);
    }
}
