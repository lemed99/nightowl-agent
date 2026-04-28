<?php

namespace NightOwl\Agent;

final class ConnectionHandler
{
    private ?string $expectedTokenHash;

    public function __construct(
        private PayloadParser $parser,
        private RecordWriter $writer,
        ?string $token = null,
    ) {
        $this->expectedTokenHash = $token !== null
            ? substr(hash('xxh128', $token), 0, 7)
            : null;
    }

    /**
     * Handle a complete payload from a client connection.
     *
     * @param  resource  $stream  The client stream (for writing responses)
     * @param  string  $data  The complete payload data already read by the server
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

        // Validate token hash if configured. hash_equals is constant-time —
        // overkill for a 7-char local-port hash, but defense-in-depth costs
        // nothing and future-proofs the comparison.
        if ($this->expectedTokenHash !== null) {
            $receivedHash = (string) ($result['tokenHash'] ?? '');

            if (! hash_equals($this->expectedTokenHash, $receivedHash)) {
                error_log('[NightOwl Agent] Rejected payload: invalid token hash');
                fwrite($stream, '5:ERROR');

                return;
            }
        }

        if ($result['type'] === 'json') {
            $this->writer->write($result['records']);
        }

        // Acknowledge
        fwrite($stream, '2:OK');
    }
}
