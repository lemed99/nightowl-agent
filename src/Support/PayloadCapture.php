<?php

namespace NightOwl\Support;

use Laravel\Nightwatch\Records\Request as RequestRecord;
use Symfony\Component\HttpFoundation\BinaryFileResponse;
use Symfony\Component\HttpFoundation\File\UploadedFile;
use Symfony\Component\HttpFoundation\Response;
use Throwable;

/**
 * NightOwl's own request-payload and response-body capture.
 *
 * Nightwatch captures a request payload ONLY when the response is a 500, and
 * nothing configures that: the check is hard-coded in RequestSensor, which is
 * final and @internal. It never captures a response body at all. So NightOwl
 * rewrites the request record on its own side of the ingest (CapturingIngest),
 * after Nightwatch has resolved it and before it is buffered for the agent —
 * Nightwatch's hosted ingest, in parallel mode, receives its record untouched.
 *
 * The two inputs arrive by two hooks, both registered in the service provider:
 *  - the RequestRecord, via Nightwatch's own redactRequests() callback list.
 *    CapturesState::request() runs those callbacks and writes the record in the
 *    same call, so the pairing is exact. And because the record is read here at
 *    write time — after EVERY callback ran, whatever the registration order —
 *    a customer's Nightwatch::redactRequests() edits to the payload are honored.
 *  - the Response, via Laravel's RequestHandled event, which fires inside
 *    Kernel::handle() for every request, long before terminate writes the record.
 * Both are cleared on use, so an Octane worker never pairs a record with a
 * previous request's state; the method + status code must also agree.
 *
 * Settings (all under the top-level `capture` config key):
 *  - request_payload   null = leave Nightwatch's behaviour alone (500s only,
 *                      NIGHTWATCH_CAPTURE_REQUEST_PAYLOAD); true = capture per
 *                      the filters below; false = never capture.
 *  - response_body     capture per the filters below.
 *  - status_codes      "500,422,5xx" — exact codes or a class. Empty = every status.
 *  - routes            IgnoredRoutes patterns (path, name or action). Empty = every route.
 *  - max_bytes         per-body truncation; 0 = no limit.
 * A filter that was set but parsed to NOTHING valid captures nothing: a typo in
 * a filter meant to narrow capture must not widen it to every request.
 *
 * Regardless of max_bytes, a record whose captured bodies together exceed
 * WIRE_CEILING_BYTES has them replaced by a TOO_LARGE marker. The agent refuses
 * any frame over 10 MB (AsyncServer::MAX_PAYLOAD_BYTES), and a refused frame
 * loses the WHOLE request's telemetry — its queries, logs and exceptions —
 * not just the body. JSON escaping can roughly double a body on the wire.
 */
final class PayloadCapture
{
    public const WIRE_CEILING_BYTES = 4 * 1024 * 1024;

    /**
     * Used whenever the setting is ABSENT, not merely when the env var is: a
     * customer who published the config and wrote their own `capture` block
     * without this key would otherwise ship tokens in clear. Only an explicit
     * empty value turns response redaction off.
     */
    public const DEFAULT_REDACT_RESPONSE_FIELDS = [
        'password', 'password_confirmation', 'token', 'access_token', 'refresh_token',
        'api_key', 'secret', 'client_secret',
    ];

    /** Nightwatch's own encoding flags (RequestSensor), so both halves serialize alike. */
    private const JSON_FLAGS = JSON_INVALID_UTF8_SUBSTITUTE | JSON_PRESERVE_ZERO_FRACTION | JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE;

    private ?RequestRecord $record = null;

    private ?Response $response = null;

    /** @var array<string, true> lower-cased */
    private array $redactResponseLookup;

    /**
     * @param  list<string>|null  $statusRules  null = every status; [] = none
     * @param  list<string>|null  $routes  null = every route; [] = none
     * @param  list<string>  $redactPayloadFields  Nightwatch's list, matched exactly (its semantics)
     * @param  list<string>  $redactResponseFields  matched case-insensitively
     */
    public function __construct(
        private ?bool $requestPayload,
        private bool $responseBody,
        private ?array $statusRules = null,
        private ?array $routes = null,
        private int $maxBytes = 0,
        private array $redactPayloadFields = ['_token', 'password', 'password_confirmation'],
        array $redactResponseFields = [],
    ) {
        $this->redactResponseLookup = [];
        foreach ($redactResponseFields as $field) {
            $this->redactResponseLookup[strtolower($field)] = true;
        }
    }

    /**
     * Build from the `capture` config block, or null when capture is entirely
     * off — the provider then leaves the ingest unwrapped.
     *
     * @param  array<string, mixed>  $config
     * @param  list<string>  $redactPayloadFields
     */
    public static function fromConfig(array $config, array $redactPayloadFields): ?self
    {
        $requestPayload = self::bool($config['request_payload'] ?? null);
        $responseBody = self::bool($config['response_body'] ?? null) ?? false;

        if ($requestPayload === null && ! $responseBody) {
            return null;
        }

        return new self(
            requestPayload: $requestPayload,
            responseBody: $responseBody,
            statusRules: self::parseStatusRules($config['status_codes'] ?? null),
            routes: self::parseRoutes($config['routes'] ?? null),
            maxBytes: max(0, (int) ($config['max_bytes'] ?? 0)),
            redactPayloadFields: array_values(array_filter($redactPayloadFields, 'is_string')),
            redactResponseFields: ($config['redact_response_fields'] ?? null) === null
                ? self::DEFAULT_REDACT_RESPONSE_FIELDS
                : self::parseList($config['redact_response_fields']),
        );
    }

    public function remember(RequestRecord $record): void
    {
        $this->record = $record;
    }

    public function rememberResponse(Response $response): void
    {
        $this->response = $response;
    }

    /**
     * Rewrite a resolved Nightwatch record. Anything that is not the request
     * record this capture was primed for passes through unchanged.
     *
     * @param  array<mixed>  $record
     * @return array<mixed>
     */
    public function enrich(array $record): array
    {
        if (($record['t'] ?? null) !== 'request') {
            return $record;
        }

        $captured = $this->record;
        $response = $this->response;
        $this->record = null;
        $this->response = null;

        if ($captured === null
            || $captured->method !== ($record['method'] ?? null)
            || $captured->statusCode !== ($record['status_code'] ?? null)) {
            return $record;
        }

        $matches = $this->matches($record);

        if ($this->requestPayload !== null) {
            $record['payload'] = $this->requestPayload && $matches ? $this->serializePayload($captured) : '';
        }

        if ($this->responseBody && $matches && $response !== null && $response->getStatusCode() === $captured->statusCode) {
            $body = $this->serializeResponse($response);
            if ($body !== '') {
                $record['response_body'] = $body;
            }
        }

        return $this->fitWire($record);
    }

    /** @param  array<mixed>  $record */
    private function matches(array $record): bool
    {
        if ($this->statusRules !== null && ! self::statusMatches($this->statusRules, (int) ($record['status_code'] ?? 0))) {
            return false;
        }

        return $this->routes === null || IgnoredRoutes::matches($this->routes, $record);
    }

    /** @param  list<string>  $rules */
    public static function statusMatches(array $rules, int $status): bool
    {
        $code = (string) $status;

        foreach ($rules as $rule) {
            if ($rule === $code || (str_ends_with($rule, 'xx') && $rule[0] === $code[0] && strlen($code) === 3)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Nightwatch's serializePayload() minus its 500-only gate — same skip rules,
     * same markers, same `_nightwatch_files` shape, so the dashboard renders a
     * payload identically whichever side captured it. Nightwatch's 64 KB clip is
     * replaced by max_bytes.
     */
    private function serializePayload(RequestRecord $record): string
    {
        $empty = $record->payload->count() === 0 && $record->files->count() === 0;

        if ($empty && in_array($record->method, ['GET', 'HEAD', 'OPTIONS', 'TRACE'], true)) {
            return '';
        }

        if ($empty && ! self::isSupportedRequestType((string) $record->headers->get('content-type', ''))) {
            return '{"_nightwatch_error":"UNSUPPORTED_CONTENT_TYPE"}';
        }

        try {
            return $this->truncate(json_encode([
                ...$this->redactPayload($record->payload->all()),
                '_nightwatch_files' => $this->mapFiles($record->files->all()),
            ], self::JSON_FLAGS));
        } catch (Throwable) {
            return '{"_nightwatch_error":"SERIALIZATION_FAILED"}';
        }
    }

    private static function isSupportedRequestType(string $type): bool
    {
        $type = strtolower($type);

        return str_contains($type, '/json') || str_contains($type, '+json')
            || str_starts_with($type, 'application/x-www-form-urlencoded')
            || str_starts_with($type, 'multipart/form-data');
    }

    /**
     * JSON bodies are decoded, redacted and re-encoded; textual bodies are kept
     * as-is (and cannot be field-redacted — there are no fields); anything else
     * gets a marker instead of bytes the dashboard could not show.
     */
    private function serializeResponse(Response $response): string
    {
        if ($response instanceof BinaryFileResponse) {
            return '{"_nightowl_error":"FILE_RESPONSE"}';
        }

        $content = $response->getContent();
        if ($content === false) {
            return '{"_nightowl_error":"STREAMED_RESPONSE"}';
        }
        if ($content === '') {
            return '';
        }

        $type = strtolower((string) $response->headers->get('content-type', ''));

        if (str_contains($type, '/json') || str_contains($type, '+json')) {
            try {
                $decoded = json_decode($content, true, 512, JSON_THROW_ON_ERROR);
            } catch (Throwable) {
                return $this->truncate($content); // labelled JSON, isn't: show what was sent
            }

            try {
                return $this->truncate(json_encode(
                    is_array($decoded) ? $this->redactResponse($decoded) : $decoded,
                    self::JSON_FLAGS,
                ));
            } catch (Throwable) {
                return '{"_nightowl_error":"SERIALIZATION_FAILED"}';
            }
        }

        if (self::isTextualResponseType($type) || ($type === '' && mb_check_encoding($content, 'UTF-8'))) {
            return $this->truncate($content);
        }

        return '{"_nightowl_error":"UNSUPPORTED_CONTENT_TYPE"}';
    }

    private static function isTextualResponseType(string $type): bool
    {
        return str_starts_with($type, 'text/')
            || str_contains($type, '/xml') || str_contains($type, '+xml')
            || str_starts_with($type, 'application/javascript')
            || str_starts_with($type, 'application/x-www-form-urlencoded');
    }

    /**
     * Nightwatch's redactRecursively(): exact key match, string values only.
     *
     * @param  array<mixed>  $array
     * @return array<mixed>
     */
    private function redactPayload(array $array): array
    {
        foreach ($array as $key => $value) {
            if (is_array($value)) {
                $array[$key] = $this->redactPayload($value);
            } elseif (is_string($value) && in_array($key, $this->redactPayloadFields, true)) {
                $array[$key] = '['.strlen($value).' bytes redacted]';
            }
        }

        return $array;
    }

    /**
     * Case-insensitive, and ANY value under a listed key is redacted — a
     * response's `token` may well be an object, and a leak through a nested
     * field is exactly as bad.
     *
     * @param  array<mixed>  $array
     * @return array<mixed>
     */
    private function redactResponse(array $array): array
    {
        foreach ($array as $key => $value) {
            if (is_string($key) && isset($this->redactResponseLookup[strtolower($key)])) {
                $array[$key] = is_string($value) ? '['.strlen($value).' bytes redacted]' : '[redacted]';
            } elseif (is_array($value)) {
                $array[$key] = $this->redactResponse($value);
            }
        }

        return $array;
    }

    /**
     * @param  array<mixed>  $files
     * @return array<mixed>
     */
    private function mapFiles(array $files): array
    {
        return array_map(function ($file) {
            if (is_array($file)) {
                return $this->mapFiles($file);
            }
            if (! $file instanceof UploadedFile) {
                return null;
            }

            return [
                'originalName' => $file->getClientOriginalName(),
                'size' => $file->getSize(),
                'error' => $file->getError(),
            ];
        }, $files);
    }

    private function truncate(string $value): string
    {
        if ($this->maxBytes === 0 || strlen($value) <= $this->maxBytes) {
            return $value;
        }

        // Cut on a character boundary — a split multi-byte sequence would be
        // substituted on the wire and leave a replacement glyph at the end.
        return mb_strcut($value, 0, $this->maxBytes, 'UTF-8');
    }

    /**
     * @param  array<mixed>  $record
     * @return array<mixed>
     */
    private function fitWire(array $record): array
    {
        $payload = is_string($record['payload'] ?? null) ? $record['payload'] : '';
        $body = is_string($record['response_body'] ?? null) ? $record['response_body'] : '';

        if (strlen($payload) + strlen($body) <= self::WIRE_CEILING_BYTES) {
            return $record;
        }

        if ($body !== '') {
            $record['response_body'] = self::tooLarge(strlen($body));
            $body = $record['response_body'];
        }

        if (strlen($payload) + strlen($body) > self::WIRE_CEILING_BYTES) {
            $record['payload'] = self::tooLarge(strlen($payload));
        }

        return $record;
    }

    private static function tooLarge(int $bytes): string
    {
        return '{"_nightowl_error":"TOO_LARGE","bytes":'.$bytes.'}';
    }

    private static function bool(mixed $value): ?bool
    {
        if ($value === null || $value === '') {
            return null;
        }

        return is_bool($value) ? $value : filter_var($value, FILTER_VALIDATE_BOOLEAN, FILTER_NULL_ON_FAILURE);
    }

    /** @return list<string>|null */
    public static function parseStatusRules(mixed $value): ?array
    {
        $entries = self::parseList($value);
        if ($entries === []) {
            return null;
        }

        $rules = [];
        foreach ($entries as $entry) {
            $entry = strtolower($entry);
            if (preg_match('/^[1-5](\d\d|xx)$/', $entry) === 1) {
                $rules[] = $entry;
            } else {
                self::warnOnce('NIGHTOWL_CAPTURE_STATUS_CODE: "'.$entry.'" is not a status code (500) or class (5xx), ignored');
            }
        }

        if ($rules === []) {
            self::warnOnce('NIGHTOWL_CAPTURE_STATUS_CODE has no valid entry, so nothing is captured');
        }

        return array_values(array_unique($rules));
    }

    /** @return list<string>|null */
    private static function parseRoutes(mixed $value): ?array
    {
        if ($value === null || $value === false || $value === [] || (is_string($value) && trim($value, " \t,") === '')) {
            return null;
        }

        $patterns = IgnoredRoutes::parse($value, 'NIGHTOWL_CAPTURE_ROUTES');
        if ($patterns === []) {
            self::warnOnce('NIGHTOWL_CAPTURE_ROUTES has no valid pattern, so nothing is captured');
        }

        return $patterns;
    }

    /** @return list<string> */
    private static function parseList(mixed $value): array
    {
        if ($value === null || $value === false || $value === '') {
            return [];
        }

        $entries = is_array($value) ? $value : explode(',', (string) $value);

        return array_values(array_filter(
            // Ints too: a published config may well say `[500, 422]`.
            array_map(static fn ($e) => is_string($e) || is_int($e) ? trim((string) $e) : '', $entries),
            static fn (string $e) => $e !== '',
        ));
    }

    /** @var array<string, true> */
    private static array $warned = [];

    private static function warnOnce(string $message): void
    {
        if (! isset(self::$warned[$message])) {
            self::$warned[$message] = true;
            error_log('[NightOwl Agent] '.$message);
        }
    }
}
