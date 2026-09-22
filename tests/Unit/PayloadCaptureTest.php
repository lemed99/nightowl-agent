<?php

namespace NightOwl\Tests\Unit;

use Laravel\Nightwatch\Records\Request as RequestRecord;
use NightOwl\Support\CapturingIngest;
use NightOwl\Support\MultiIngest;
use NightOwl\Support\PayloadCapture;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\BinaryFileResponse;
use Symfony\Component\HttpFoundation\FileBag;
use Symfony\Component\HttpFoundation\HeaderBag;
use Symfony\Component\HttpFoundation\InputBag;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpFoundation\StreamedResponse;

final class PayloadCaptureTest extends TestCase
{
    public function test_capture_is_inert_unless_asked_for(): void
    {
        $this->assertNull(PayloadCapture::fromConfig([], []));
        $this->assertNull(PayloadCapture::fromConfig(['request_payload' => null, 'response_body' => false], []));
        $this->assertNotNull(PayloadCapture::fromConfig(['request_payload' => false], []), 'false is a decision, not "unset"');
        $this->assertNotNull(PayloadCapture::fromConfig(['response_body' => 'true'], []));
    }

    public function test_status_rules_take_codes_and_classes(): void
    {
        $rules = PayloadCapture::parseStatusRules(' 422, 5XX ,');

        $this->assertSame(['422', '5xx'], $rules);
        $this->assertTrue(PayloadCapture::statusMatches($rules, 422));
        $this->assertTrue(PayloadCapture::statusMatches($rules, 503));
        $this->assertFalse(PayloadCapture::statusMatches($rules, 404));
        $this->assertNull(PayloadCapture::parseStatusRules(''), 'blank = every status');
        $this->assertSame(['500', '404'], PayloadCapture::parseStatusRules([500, 404]), 'a published config may use ints');
    }

    public function test_a_filter_set_to_garbage_captures_nothing_rather_than_everything(): void
    {
        $capture = PayloadCapture::fromConfig(['request_payload' => true, 'status_codes' => 'errors'], []);

        $this->assertSame('', $this->enrich($capture, 500)['payload']);

        $capture = PayloadCapture::fromConfig(['request_payload' => true, 'routes' => ["bad\xC3("]], []);

        $this->assertSame('', $this->enrich($capture, 200)['payload']);
    }

    public function test_route_filter_matches_path_name_or_action(): void
    {
        $capture = fn () => PayloadCapture::fromConfig(['request_payload' => true, 'routes' => '/api/*'], []);

        $this->assertNotSame('', $this->enrich($capture(), 200, route: '/api/orders')['payload']);
        $this->assertSame('', $this->enrich($capture(), 200, route: '/admin/orders')['payload']);
    }

    public function test_unset_request_payload_leaves_nightwatchs_value_and_false_clears_it(): void
    {
        $record = $this->enrich(new PayloadCapture(null, true), 500, nightwatchPayload: '{"a":1}');
        $this->assertSame('{"a":1}', $record['payload']);

        $record = $this->enrich(new PayloadCapture(false, false), 500, nightwatchPayload: '{"a":1}');
        $this->assertSame('', $record['payload']);
    }

    public function test_get_without_a_body_captures_nothing(): void
    {
        $record = $this->enrich(new PayloadCapture(true, false), 200, method: 'GET', payload: []);

        $this->assertSame('', $record['payload']);
    }

    public function test_request_payload_uses_nightwatch_redaction_semantics(): void
    {
        $capture = new PayloadCapture(true, false, redactPayloadFields: ['password']);

        $record = $this->enrich($capture, 200, payload: ['password' => 'hunter2', 'Password' => 'kept', 'nested' => ['password' => 'x']]);

        $this->assertSame(
            ['password' => '[7 bytes redacted]', 'Password' => 'kept', 'nested' => ['password' => '[1 bytes redacted]'], '_nightwatch_files' => []],
            json_decode($record['payload'], true),
        );
    }

    public function test_response_redaction_is_case_insensitive_and_covers_any_value(): void
    {
        $capture = PayloadCapture::fromConfig(['response_body' => true], []);
        $response = new Response(json_encode([
            'Access_Token' => 'abc',
            'token' => ['id' => 1, 'secret' => 'x'],
            'data' => [['api_key' => 'k', 'name' => 'n']],
        ]), 200, ['Content-Type' => 'application/json']);

        $body = json_decode($this->enrich($capture, 200, response: $response)['response_body'], true);

        $this->assertSame('[3 bytes redacted]', $body['Access_Token']);
        $this->assertSame('[redacted]', $body['token']);
        $this->assertSame(['api_key' => '[1 bytes redacted]', 'name' => 'n'], $body['data'][0]);
    }

    public function test_an_explicitly_empty_redaction_list_turns_it_off(): void
    {
        $capture = PayloadCapture::fromConfig(['response_body' => true, 'redact_response_fields' => ''], []);
        $response = new Response('{"token":"abc"}', 200, ['Content-Type' => 'application/json']);

        $this->assertSame('{"token":"abc"}', $this->enrich($capture, 200, response: $response)['response_body']);
    }

    public function test_non_json_bodies(): void
    {
        $capture = fn () => new PayloadCapture(null, true);

        $html = new Response('<h1>Hi</h1>', 200, ['Content-Type' => 'text/html; charset=UTF-8']);
        $this->assertSame('<h1>Hi</h1>', $this->enrich($capture(), 200, response: $html)['response_body']);

        $pdf = new Response('%PDF-1.4', 200, ['Content-Type' => 'application/pdf']);
        $this->assertSame('{"_nightowl_error":"UNSUPPORTED_CONTENT_TYPE"}', $this->enrich($capture(), 200, response: $pdf)['response_body']);

        $streamed = new StreamedResponse(fn () => print ('x'), 200);
        $this->assertSame('{"_nightowl_error":"STREAMED_RESPONSE"}', $this->enrich($capture(), 200, response: $streamed)['response_body']);

        $file = new BinaryFileResponse(__FILE__);
        $this->assertSame('{"_nightowl_error":"FILE_RESPONSE"}', $this->enrich($capture(), 200, response: $file)['response_body']);

        $empty = new Response('', 204);
        $this->assertArrayNotHasKey('response_body', $this->enrich($capture(), 204, response: $empty));

        $lying = new Response('not json', 200, ['Content-Type' => 'application/json']);
        $this->assertSame('not json', $this->enrich($capture(), 200, response: $lying)['response_body']);
    }

    public function test_max_bytes_truncates_on_a_character_boundary(): void
    {
        $capture = new PayloadCapture(null, true, maxBytes: 4);
        $response = new Response('héllo', 200, ['Content-Type' => 'text/plain']);

        // 'h' + 'é' (2 bytes) = 3 bytes; the 4th byte would split nothing, 'l' fits.
        $this->assertSame('hél', $this->enrich($capture, 200, response: $response)['response_body']);

        $capture = new PayloadCapture(null, true, maxBytes: 2);
        $this->assertSame('h', $this->enrich($capture, 200, response: $response)['response_body'], 'never half a character');
    }

    public function test_no_limit_by_default_but_the_wire_ceiling_still_holds(): void
    {
        $big = str_repeat('a', PayloadCapture::WIRE_CEILING_BYTES + 1);

        $record = $this->enrich(new PayloadCapture(null, true), 200, response: new Response($big, 200, ['Content-Type' => 'text/plain']));
        $this->assertSame('{"_nightowl_error":"TOO_LARGE","bytes":'.strlen($big).'}', $record['response_body']);

        $fits = str_repeat('a', 1024 * 1024);
        $record = $this->enrich(new PayloadCapture(null, true), 200, response: new Response($fits, 200, ['Content-Type' => 'text/plain']));
        $this->assertSame($fits, $record['response_body'], 'no default truncation below the ceiling');
    }

    public function test_the_response_is_dropped_first_when_both_are_over_the_ceiling(): void
    {
        $half = str_repeat('a', intdiv(PayloadCapture::WIRE_CEILING_BYTES, 2) + 10);

        $record = $this->enrich(
            new PayloadCapture(true, true), 200,
            payload: ['blob' => $half],
            response: new Response($half, 200, ['Content-Type' => 'text/plain']),
        );

        $this->assertStringContainsString($half, $record['payload']);
        $this->assertStringStartsWith('{"_nightowl_error":"TOO_LARGE"', $record['response_body']);
    }

    public function test_a_record_that_does_not_match_the_primed_request_is_untouched(): void
    {
        $capture = new PayloadCapture(true, true);
        $capture->remember($this->requestRecord('POST', 200, ['a' => 1]));
        $capture->rememberResponse(new Response('{"b":2}', 200, ['Content-Type' => 'application/json']));

        $record = $capture->enrich(['t' => 'request', 'method' => 'POST', 'status_code' => 500, 'payload' => 'nw']);
        $this->assertSame('nw', $record['payload']);
        $this->assertArrayNotHasKey('response_body', $record);

        // And the state was consumed — the next request cannot inherit it.
        $record = $capture->enrich(['t' => 'request', 'method' => 'POST', 'status_code' => 200, 'payload' => 'nw']);
        $this->assertSame('nw', $record['payload']);
    }

    public function test_other_record_types_pass_through_without_consuming_state(): void
    {
        $capture = new PayloadCapture(true, false);
        $capture->remember($this->requestRecord('POST', 200, ['a' => 1]));

        $this->assertSame(['t' => 'query', 'sql' => 'x'], $capture->enrich(['t' => 'query', 'sql' => 'x']));
        $this->assertSame('{"a":1,"_nightwatch_files":[]}', $capture->enrich($this->record('POST', 200))['payload']);
    }

    public function test_capturing_ingest_fails_open_and_multi_ingest_dedupes_it(): void
    {
        $inner = new class implements \Laravel\Nightwatch\Contracts\Ingest
        {
            public array $written = [];

            public function write(array $record): void { $this->written[] = $record; }

            public function writeNow(array $record): void {}

            public function ping(): void {}

            public function shouldDigest(bool $bool = true): void {}

            public function shouldDigestWhenBufferIsFull(bool $bool = true): void {}

            public function digest(): void {}

            public function flush(): void {}
        };

        $capture = new PayloadCapture(true, true);
        $capture->remember($this->requestRecord('POST', 200, ['a' => 1]));
        $capture->rememberResponse(new class extends Response
        {
            public function getContent(): string|false
            {
                throw new \RuntimeException('boom');
            }
        });
        $ingest = new CapturingIngest($inner, $capture);
        $ingest->write($this->record('POST', 200, payload: 'nw'));
        $this->assertSame([$this->record('POST', 200, payload: 'nw')], $inner->written, 'a capture failure sends the record exactly as Nightwatch built it');

        $multi = new MultiIngest(new MultiIngest($ingest), $ingest);
        $ingests = (new \ReflectionClass($multi))->getProperty('ingests')->getValue($multi);
        $this->assertCount(1, $ingests);
    }

    /**
     * @param  array<string, mixed>  $payload
     * @return array<string, mixed>
     */
    private function enrich(
        PayloadCapture $capture,
        int $status,
        string $method = 'POST',
        array $payload = ['a' => 1],
        ?Response $response = null,
        string $route = '/api/orders',
        string $nightwatchPayload = '',
    ): array {
        $capture->remember($this->requestRecord($method, $status, $payload));
        $capture->rememberResponse($response ?? new Response('', $status));

        return $capture->enrich($this->record($method, $status, $route, $nightwatchPayload));
    }

    /** @return array<string, mixed> */
    private function record(string $method, int $status, string $route = '/api/orders', string $payload = ''): array
    {
        return [
            't' => 'request', 'method' => $method, 'status_code' => $status,
            'route_path' => $route, 'route_name' => '', 'route_action' => '', 'payload' => $payload,
        ];
    }

    /** @param  array<string, mixed>  $payload */
    private function requestRecord(string $method, int $status, array $payload): RequestRecord
    {
        return new RequestRecord(
            method: $method, url: 'https://x.test/api/orders', routeName: '', routeMethods: [$method],
            routeDomain: '', routePath: '/api/orders', routeAction: '', ip: '127.0.0.1',
            duration: 1, statusCode: $status, requestSize: 0, responseSize: 0,
            headers: new HeaderBag(['content-type' => 'application/json']),
            payload: new InputBag($payload),
            files: new FileBag,
        );
    }
}
