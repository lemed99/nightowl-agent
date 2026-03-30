<?php

namespace NightOwl\Agent;

final class ConnectionHandler
{
    private ?string $expectedTokenHash;

    public function __construct(
        private PayloadParser $parser,
        private RecordWriter $writer,
        private Sampler $sampler,
        private Redactor $redactor,
        ?string $token = null,
    ) {
        $this->expectedTokenHash = $token !== null
            ? substr(hash('xxh128', $token), 0, 7)
            : null;
    }

    /**
     * Handle a complete payload from a client connection.
     *
     * @param resource $stream  The client stream (for writing responses)
     * @param string   $data    The complete payload data already read by the server
     */
    public function handle($stream, string $data): void
    {
        if ($data === '') {
            return;
        }

        // Parse the payload
        $result = $this->parser->parse($data);

        if ($result === null) {
            // Malformed payload — reject
            fwrite($stream, '5:ERROR');

            return;
        }

        // Unsupported version — reject with descriptive error
        if ($result['type'] === 'error') {
            error_log("[NightOwl Agent] {$result['error']}");
            fwrite($stream, '5:ERROR');

            return;
        }

        // Handle PING — no auth needed
        if ($result['type'] === 'text' && $result['payload'] === 'PING') {
            fwrite($stream, '2:OK');

            return;
        }

        // Validate token hash if configured
        if ($this->expectedTokenHash !== null) {
            $receivedHash = $result['tokenHash'] ?? null;

            if ($receivedHash !== $this->expectedTokenHash) {
                error_log('[NightOwl Agent] Rejected payload: invalid token hash');
                fwrite($stream, '5:ERROR');

                return;
            }
        }

        // Write records to database
        if ($result['type'] === 'json') {
            // Sampling — drop non-critical payloads transparently
            if (! $this->sampler->shouldKeep($result['records'])) {
                fwrite($stream, '2:OK');
                return;
            }

            // Redaction — strip sensitive keys before storage
            [$records] = $this->redactor->redact($result['records']);

            $this->writer->write($records);
        }

        // Acknowledge
        fwrite($stream, '2:OK');
    }
}
