<?php

namespace NightOwl\Agent;

final class PayloadParser
{
    private const SUPPORTED_VERSIONS = ['v1'];

    private const MAX_DECOMPRESSED_BYTES = 10 * 1024 * 1024 * 20; // 200 MB — 20x compressed limit

    public function __construct(
        private bool $gzipEnabled = true,
        private ?string $debugDumpPath = null,
    ) {
        if ($this->debugDumpPath !== null) {
            $dir = dirname($this->debugDumpPath);
            if (! is_dir($dir)) {
                @mkdir($dir, 0755, true);
            }
        }
    }

    /**
     * Parse a Nightwatch payload.
     *
     * Wire format: [length]:[version]:[tokenHash]:[payload]
     *
     * Where:
     * - length: integer, byte length of everything after the first colon
     * - version: "v1"
     * - tokenHash: 7-char xxh128 hash of the app token
     * - payload: JSON array of records, or plain text like "PING"
     *
     * Returns both the decoded records and the raw JSON string. The raw string
     * allows the async driver to store the payload in SQLite without re-encoding,
     * eliminating json_encode from the hot path.
     *
     * @return array{type: 'json'|'text'|'error', payload?: string, records?: array, rawPayload?: string, error?: string}|null
     */
    public function parse(string $raw): ?array
    {
        // Step 1: Extract the length prefix
        $firstColon = strpos($raw, ':');
        if ($firstColon === false) {
            return null;
        }

        $length = (int) substr($raw, 0, $firstColon);
        $body = substr($raw, $firstColon + 1, $length);

        if ($body === false || $body === '') {
            return null;
        }

        // Step 2: Extract version
        $secondColon = strpos($body, ':');
        if ($secondColon === false) {
            return null;
        }

        $version = substr($body, 0, $secondColon);
        $remaining = substr($body, $secondColon + 1);

        // Step 3: Validate version
        if (! in_array($version, self::SUPPORTED_VERSIONS, true)) {
            return [
                'type' => 'error',
                'error' => "Unsupported wire format version '{$version}'. Supported: ".implode(', ', self::SUPPORTED_VERSIONS),
            ];
        }

        // Step 4: Extract and validate token hash
        $thirdColon = strpos($remaining, ':');
        if ($thirdColon === false) {
            return null;
        }

        $tokenHash = substr($remaining, 0, $thirdColon);
        $payload = substr($remaining, $thirdColon + 1);

        // Step 5: Conditional gzip decompression
        // Detect gzip by magic bytes (0x1f 0x8b) — these can never appear at the
        // start of valid JSON, so the check is definitive with zero false positives.
        // No size threshold: a client may compress a 5KB payload to 800 bytes, and
        // skipping decompression would cause json_decode to fail on gzip binary data.
        //
        // Uses incremental decompression with a size limit to prevent gzip bombs:
        // a 10MB compressed payload could decompress to 1GB+ with repetitive data.
        if ($this->gzipEnabled && strlen($payload) >= 2 && $payload[0] === "\x1f" && $payload[1] === "\x8b") {
            $decompressed = $this->safeGzipDecode($payload);
            if ($decompressed === false) {
                return null;
            }
            $payload = $decompressed;
        }

        // Step 6: Determine payload type
        if ($payload === 'PING') {
            return ['type' => 'text', 'payload' => 'PING'];
        }

        // Step 7: Decode JSON
        try {
            $records = json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            return null;
        }

        if (! is_array($records)) {
            return null;
        }

        if ($this->debugDumpPath !== null) {
            $this->dumpDecoded($tokenHash, $payload, $records);
        }

        return [
            'type' => 'json',
            'records' => $records,
            'rawPayload' => $payload,
            'tokenHash' => $tokenHash,
        ];
    }

    /**
     * Debug-only: append a JSONL line with the decoded records so we can
     * inspect which record types (`t` field) the upstream collector emits.
     * Grep friendly: `grep lazy_load raw-payloads.jsonl`.
     * Aggregate friendly: `jq -r '.records[].t' raw-payloads.jsonl | sort | uniq -c`.
     */
    private function dumpDecoded(string $tokenHash, string $rawPayload, array $records): void
    {
        $entry = [
            'ts' => date('c'),
            'tokenHash' => $tokenHash,
            'bytes' => strlen($rawPayload),
            'recordCount' => count($records),
            'recordTypes' => array_count_values(array_map(
                fn ($r) => is_array($r) && isset($r['t']) ? (string) $r['t'] : '<untyped>',
                $records,
            )),
            'records' => $records,
        ];

        $line = json_encode($entry, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        if ($line === false) {
            return;
        }

        @file_put_contents($this->debugDumpPath, $line."\n", FILE_APPEND | LOCK_EX);
    }

    /**
     * Decompress gzip data with a size limit to prevent gzip bombs.
     *
     * Uses incremental decompression so a 10MB compressed payload that
     * would decompress to 1GB+ is rejected without allocating the full
     * decompressed buffer.
     *
     * @return string|false Decompressed data or false on failure/oversized
     */
    private function safeGzipDecode(string $data): string|false
    {
        $ctx = @inflate_init(ZLIB_ENCODING_GZIP);
        if ($ctx === false) {
            return @gzdecode($data) ?: false; // Fallback if inflate_init unavailable
        }

        $decompressed = '';
        $offset = 0;
        $chunkSize = 8192;

        while ($offset < strlen($data)) {
            $chunk = substr($data, $offset, $chunkSize);
            $offset += $chunkSize;

            $inflated = @inflate_add($ctx, $chunk);
            if ($inflated === false) {
                return false;
            }
            $decompressed .= $inflated;

            if (strlen($decompressed) > self::MAX_DECOMPRESSED_BYTES) {
                return false; // Reject oversized decompressed output
            }
        }

        return $decompressed;
    }

    /**
     * Get the list of supported wire format versions.
     *
     * @return string[]
     */
    public static function supportedVersions(): array
    {
        return self::SUPPORTED_VERSIONS;
    }
}
