<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\ConnectionHandler;
use NightOwl\Agent\PayloadParser;
use NightOwl\Agent\RecordWriter;
use PHPUnit\Framework\TestCase;

class ConnectionHandlerTest extends TestCase
{
    private string $token = 'test-token-123';

    /**
     * Create a handler with a RecordWriter that will fail on actual DB writes.
     * We test the pipeline logic by checking responses (2:OK vs 5:ERROR).
     * For tests where write() SHOULD be called, we expect a PDOException.
     * For tests where write() should NOT be called, we expect no exception.
     */
    private function makeHandler(
        ?string $token = 'USE_DEFAULT',
    ): ConnectionHandler {
        // RecordWriter with impossible DSN — write() will throw PDOException
        $writer = new RecordWriter('__invalid__', 0, '__none__', '__none__', '__none__');

        return new ConnectionHandler(
            parser: new PayloadParser(),
            writer: $writer,
            token: $token === 'USE_DEFAULT' ? $this->token : $token,
        );
    }

    private function buildPayload(string $content, ?string $token = null): string
    {
        $hash = substr(hash('xxh128', $token ?? $this->token), 0, 7);
        $body = "v1:{$hash}:{$content}";

        return strlen($body) . ':' . $body;
    }

    private function createStream(): mixed
    {
        return fopen('php://memory', 'r+');
    }

    private function readResponse(mixed $stream): string
    {
        rewind($stream);

        return stream_get_contents($stream);
    }

    // ─── PING (no write expected) ──────────────────────────

    public function testPingRespondsOk(): void
    {
        $handler = $this->makeHandler();
        $stream = $this->createStream();

        $handler->handle($stream, $this->buildPayload('PING'));

        $this->assertSame('2:OK', $this->readResponse($stream));
    }

    public function testPingDoesNotRequireAuth(): void
    {
        $wrongHash = 'aaaaaaa';
        $body = "v1:{$wrongHash}:PING";
        $wire = strlen($body) . ':' . $body;

        $handler = $this->makeHandler();
        $stream = $this->createStream();

        $handler->handle($stream, $wire);

        // PING bypasses token check
        $this->assertSame('2:OK', $this->readResponse($stream));
    }

    // ─── Token validation ──────────────────────────────────

    public function testValidTokenAttemptsWrite(): void
    {
        $handler = $this->makeHandler();
        $stream = $this->createStream();

        // Valid token + valid JSON → will try to write → PDOException from dummy DSN
        $this->expectException(\PDOException::class);
        $handler->handle($stream, $this->buildPayload('[{"t":"request"}]'));
    }

    public function testInvalidTokenRejectedBeforeWrite(): void
    {
        $handler = $this->makeHandler();
        $stream = $this->createStream();

        $wrongHash = 'bbbbbbb';
        $body = "v1:{$wrongHash}:" . json_encode([['t' => 'request']]);
        $wire = strlen($body) . ':' . $body;

        // Invalid token → rejected BEFORE write, no PDOException
        $handler->handle($stream, $wire);

        $this->assertSame('5:ERROR', $this->readResponse($stream));
    }

    public function testNoTokenConfiguredAttemptsWrite(): void
    {
        $handler = $this->makeHandler(token: null);
        $stream = $this->createStream();

        $body = 'v1:anyhash:' . json_encode([['t' => 'request']]);
        $wire = strlen($body) . ':' . $body;

        // No token configured → skip validation → try write → PDOException
        $this->expectException(\PDOException::class);
        $handler->handle($stream, $wire);
    }

    // ─── Malformed payloads (no write expected) ────────────

    public function testEmptyDataIgnored(): void
    {
        $handler = $this->makeHandler();
        $stream = $this->createStream();

        $handler->handle($stream, '');

        $this->assertSame('', $this->readResponse($stream));
    }

    public function testMalformedPayloadReturnsError(): void
    {
        $handler = $this->makeHandler();
        $stream = $this->createStream();

        $handler->handle($stream, 'not-a-valid-wire-payload');

        $this->assertSame('5:ERROR', $this->readResponse($stream));
    }

    public function testUnsupportedVersionReturnsError(): void
    {
        $handler = $this->makeHandler();
        $stream = $this->createStream();

        $body = 'v99:abc1234:[]';
        $wire = strlen($body) . ':' . $body;

        $handler->handle($stream, $wire);

        $this->assertSame('5:ERROR', $this->readResponse($stream));
    }

    public function testInvalidJsonReturnsError(): void
    {
        $handler = $this->makeHandler();
        $stream = $this->createStream();

        $handler->handle($stream, $this->buildPayload('{not json}'));

        $this->assertSame('5:ERROR', $this->readResponse($stream));
    }
}
