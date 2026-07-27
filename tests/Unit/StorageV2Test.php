<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\StorageV2;
use PHPUnit\Framework\TestCase;

/**
 * Pure-encoder tests for the storage-v2 value helpers. The invariants here
 * are load-bearing: a wrong tsMicros guard branch desyncs a row's partition
 * key from its event time; a wrong traceIdFor equality verdict silently
 * breaks the API's COALESCE reconstruction; a placeholder that survives
 * deflateOrNull re-inflates as garbage bytes the API would ship to clients.
 */
class StorageV2Test extends TestCase
{
    private const NOW = 1_753_400_000;

    // ------------------------------------------------------------- tsMicros

    public function test_ts_micros_keeps_subsecond_precision(): void
    {
        $r = ['timestamp' => '1753399999.123456'];

        $this->assertSame(1_753_399_999_123_456, StorageV2::tsMicros($r, self::NOW));
    }

    public function test_ts_micros_accepts_integer_seconds(): void
    {
        $this->assertSame(1_753_399_999_000_000, StorageV2::tsMicros(['timestamp' => 1_753_399_999], self::NOW));
    }

    public function test_ts_micros_falls_back_to_drain_clock_on_garbage(): void
    {
        foreach ([null, '', 'not-a-number', []] as $bad) {
            $this->assertSame(
                self::NOW * 1_000_000,
                StorageV2::tsMicros(['timestamp' => $bad], self::NOW),
                var_export($bad, true),
            );
        }
    }

    public function test_ts_micros_range_guard_matches_event_epoch_window(): void
    {
        // ~366d past and 1d future — same constants as RecordWriter::eventEpoch,
        // so created_at and ts_us always take the same guard branch.
        $tooOld = self::NOW - 31_622_401;
        $oldestOk = self::NOW - 31_622_400;
        $tooNew = self::NOW + 86_401;
        $newestOk = self::NOW + 86_400;

        $this->assertSame(self::NOW * 1_000_000, StorageV2::tsMicros(['timestamp' => $tooOld], self::NOW));
        $this->assertSame($oldestOk * 1_000_000, StorageV2::tsMicros(['timestamp' => $oldestOk], self::NOW));
        $this->assertSame(self::NOW * 1_000_000, StorageV2::tsMicros(['timestamp' => $tooNew], self::NOW));
        $this->assertSame($newestOk * 1_000_000, StorageV2::tsMicros(['timestamp' => $newestOk], self::NOW));
    }

    // ------------------------------------------------------------ uuidOrNull

    public function test_uuid_or_null_normalizes_case(): void
    {
        $this->assertSame(
            '9b2f1c4e-8a3d-4f6b-9c1e-2d5a7b8c9d0e',
            StorageV2::uuidOrNull('9B2F1C4E-8A3D-4F6B-9C1E-2D5A7B8C9D0E'),
        );
    }

    public function test_uuid_or_null_rejects_garbage_without_throwing(): void
    {
        foreach (['zz2f1c4e-8a3d-4f6b-9c1e-2d5a7b8c9d0e', 'short', 42, '', null] as $bad) {
            $this->assertNull(StorageV2::uuidOrNull($bad), var_export($bad, true));
        }
    }

    // ------------------------------------------------------------ traceIdFor

    public function test_trace_id_nulled_when_equal_to_execution_id(): void
    {
        $uuid = '9b2f1c4e-8a3d-4f6b-9c1e-2d5a7b8c9d0e';

        $this->assertNull(StorageV2::traceIdFor(['trace_id' => $uuid, 'execution_id' => $uuid]));
    }

    public function test_trace_id_equality_is_case_insensitive(): void
    {
        // The SDK stamps both from one uuid; a case difference between the two
        // wire fields must still count as "same id".
        $this->assertNull(StorageV2::traceIdFor([
            'trace_id' => '9B2F1C4E-8A3D-4F6B-9C1E-2D5A7B8C9D0E',
            'execution_id' => '9b2f1c4e-8a3d-4f6b-9c1e-2d5a7b8c9d0e',
        ]));
    }

    public function test_trace_id_kept_when_distinct(): void
    {
        $this->assertSame(
            '9b2f1c4e-8a3d-4f6b-9c1e-2d5a7b8c9d0e',
            StorageV2::traceIdFor([
                'trace_id' => '9b2f1c4e-8a3d-4f6b-9c1e-2d5a7b8c9d0e',
                'execution_id' => '00000000-0000-4000-8000-000000000001',
            ]),
        );
    }

    public function test_trace_id_kept_when_execution_id_absent(): void
    {
        // Parent rows (requests/commands) have no execution_id — trace_id stays.
        $this->assertSame(
            '9b2f1c4e-8a3d-4f6b-9c1e-2d5a7b8c9d0e',
            StorageV2::traceIdFor(['trace_id' => '9b2f1c4e-8a3d-4f6b-9c1e-2d5a7b8c9d0e']),
        );
    }

    // ----------------------------------------------------------- hex16OrNull

    public function test_hex16_produces_bytea_wire_form(): void
    {
        $this->assertSame(
            '\x00112233445566778899aabbccddeeff',
            StorageV2::hex16OrNull('00112233445566778899AABBCCDDEEFF'),
        );
    }

    public function test_hex16_rejects_wrong_length_and_non_hex(): void
    {
        foreach (['abc', str_repeat('a', 31), str_repeat('a', 33), str_repeat('g', 32), null, ''] as $bad) {
            $this->assertNull(StorageV2::hex16OrNull($bad), var_export($bad, true));
        }
    }

    // --------------------------------------------------------- deflateOrNull

    public function test_placeholders_deflate_to_null(): void
    {
        foreach (['', '{}', 'null', '[]', null] as $placeholder) {
            $this->assertNull(StorageV2::deflateOrNull($placeholder), var_export($placeholder, true));
        }
    }

    public function test_deflate_round_trips_real_content(): void
    {
        $headers = '{"accept":"application/json","x-request-id":"abc-123"}';

        $wire = StorageV2::deflateOrNull($headers);

        $this->assertNotNull($wire);
        $this->assertStringStartsWith('\x', $wire);
        $this->assertSame($headers, gzinflate(hex2bin(substr($wire, 2))));
    }

    public function test_deflate_json_encodes_arrays_first(): void
    {
        $wire = StorageV2::deflateOrNull(['a' => 1]);

        $this->assertSame('{"a":1}', gzinflate(hex2bin(substr($wire, 2))));
    }

    public function test_empty_array_encodes_to_placeholder_and_nulls(): void
    {
        // json_encode([]) === '[]' — must hit the placeholder set, not store 2 deflated bytes.
        $this->assertNull(StorageV2::deflateOrNull([]));
    }

    // ---------------------------------------------------------------- naming

    public function test_v2_name_covers_all_eleven_tables(): void
    {
        $this->assertCount(11, StorageV2::TABLES);
        $this->assertSame('nightowl_queries_v2', StorageV2::v2Name('nightowl_queries'));
        $this->assertSame('nightowl_logs_v2', StorageV2::v2Name('nightowl_logs'));
    }
}
