<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\DDSketchHistogram as Sketch;
use PHPUnit\Framework\TestCase;

final class DDSketchHistogramTest extends TestCase
{
    /**
     * The cross-repo contract: constants, mapping probes, and pack layout.
     * nightowl-api's twin test asserts the SAME literal — change one, change
     * both, and bump VERSION if the mapping moved.
     */
    public function test_checksum_is_frozen(): void
    {
        $this->assertSame('79c2877961a9b8667eecfa9c7b4b664d', Sketch::checksum());
    }

    public function test_index_bounds_and_collectors(): void
    {
        $this->assertSame(Sketch::UNDERFLOW_INDEX, Sketch::index(0));
        $this->assertSame(Sketch::UNDERFLOW_INDEX, Sketch::index(0.5));
        $this->assertSame(0, Sketch::index(1));
        $this->assertSame(Sketch::OVERFLOW_INDEX, Sketch::index(Sketch::MAX_VALUE + 1));
        // MAX_VALUE itself stays inside the bounded range.
        $this->assertLessThan(Sketch::OVERFLOW_INDEX, Sketch::index(Sketch::MAX_VALUE));
    }

    public function test_representative_value_is_within_alpha_of_any_bucket_member(): void
    {
        foreach ([128, 999, 12_345, 1_000_000, 46_999_999, 900_000_000] as $v) {
            $i = Sketch::index($v);
            $rep = Sketch::value($i);
            $this->assertLessThanOrEqual(
                Sketch::ALPHA + 1e-9,
                abs($rep - $v) / $v,
                "value({$i}) must be within α of member {$v}"
            );
        }
    }

    public function test_pack_unpack_round_trips(): void
    {
        $counts = [-1 => 2, 0 => 7, 3 => 1, 412 => 18, 1200 => 4, Sketch::OVERFLOW_INDEX => 3];
        $this->assertSame($counts, Sketch::unpack(Sketch::pack($counts)));

        $this->assertSame('', Sketch::pack([]));
        $this->assertSame([], Sketch::unpack(''));
        $this->assertSame([], Sketch::unpack(null));
    }

    public function test_pack_is_deterministic_regardless_of_insertion_order(): void
    {
        $a = Sketch::pack([412 => 18, 3 => 1, -1 => 2]);
        $b = Sketch::pack([-1 => 2, 412 => 18, 3 => 1]);
        $this->assertSame($a, $b);
    }

    public function test_merge_sums_counts(): void
    {
        $a = Sketch::pack([3 => 1, 10 => 5]);
        $b = Sketch::pack([10 => 2, 99 => 4]);

        $this->assertSame([3 => 1, 10 => 7, 99 => 4], Sketch::unpack(Sketch::merge($a, $b)));
        // Merging with the empty sketch is identity.
        $this->assertSame(Sketch::unpack($a), Sketch::unpack(Sketch::merge($a, '')));
    }

    public function test_truncated_payload_raises(): void
    {
        $packed = Sketch::pack([500 => 300]); // multi-byte varints
        $this->expectException(\RuntimeException::class);
        Sketch::unpack(substr($packed, 0, strlen($packed) - 1));
    }

    /**
     * Index 91 (~6µs) with 120 observations packs to the bytes 0x5C 0x78 — the
     * characters '\' and 'x' — and adding index 139 with 49 makes the rest read
     * as well-formed hex ('01'). pdo_pgsql hands bytea back as a stream of
     * already-unescaped bytes, so in both the payload is data, not an escape.
     */
    public function test_counts_from_db_reads_raw_bytes_that_open_like_a_hex_escape(): void
    {
        foreach ([[91 => 120], [91 => 120, 139 => 49]] as $counts) {
            $packed = Sketch::pack($counts);
            $this->assertStringStartsWith('\x', $packed);

            $stream = fopen('php://memory', 'r+');
            fwrite($stream, $packed);
            rewind($stream);

            $this->assertSame($counts, Sketch::countsFromDb($stream));
        }
    }

    /**
     * One row is read once per percentile (p50/p95/p99), so countsFromDb() must
     * be idempotent on a SINGLE handle: the second call over the same stream
     * has to yield the identical non-empty map. Without the rewind it reads back
     * a consumed (empty) stream on the second pass.
     */
    public function test_counts_from_db_is_idempotent_on_a_single_stream(): void
    {
        $counts = [91 => 120, 139 => 49];
        $stream = fopen('php://memory', 'r+');
        fwrite($stream, Sketch::pack($counts));
        rewind($stream);

        $this->assertSame($counts, Sketch::countsFromDb($stream));
        $this->assertSame($counts, Sketch::countsFromDb($stream));
    }

    /**
     * A raw-byte string is unpacked as-is, never sniffed as a '\x' hex escape.
     * pack([91 => 120, 139 => 49]) is the 4 bytes '\','x','0','1' — its tail
     * '01' reads as valid hex, so a hex-decode misfire would corrupt or drop it.
     */
    public function test_counts_from_db_takes_a_raw_byte_string_that_opens_like_a_hex_escape(): void
    {
        $counts = [91 => 120, 139 => 49];
        $packed = Sketch::pack($counts);
        $this->assertStringStartsWith('\x', $packed);

        $this->assertSame($counts, Sketch::countsFromDb($packed));
        $this->assertNull(Sketch::countsFromDb(''));
        $this->assertNull(Sketch::countsFromDb(null));
    }

    /** Percentiles from a sketch stay within α of the exact value. */
    public function test_percentile_within_alpha_on_synthetic_distributions(): void
    {
        // Log-uniform spread — the shape that exposed the √2 histogram's 2.8%.
        $values = [];
        for ($i = 0; $i < 5000; $i++) {
            $values[] = (int) round(100 * (10 ** (($i % 1000) / 250))); // 100µs .. 1s decades
        }
        sort($values);

        $counts = [];
        foreach ($values as $v) {
            Sketch::add($counts, $v);
        }

        foreach ([0.50, 0.95, 0.99] as $p) {
            $exact = $values[max(0, (int) ceil(count($values) * $p) - 1)];
            $est = Sketch::percentile($counts, $p, $values[0], $values[count($values) - 1]);
            $this->assertLessThanOrEqual(
                Sketch::ALPHA + 1e-9,
                abs($est - $exact) / $exact,
                sprintf('p%d: est %.1f vs exact %d', $p * 100, $est, $exact)
            );
        }
    }

    public function test_percentile_clamps_to_observed_range(): void
    {
        $counts = [];
        foreach ([500, 500, 500, 500] as $v) {
            Sketch::add($counts, $v);
        }

        $this->assertSame(500.0, Sketch::percentile($counts, 0.95, 500, 500));
    }

    public function test_upconvert_places_sqrt2_bins_in_sketch_space(): void
    {
        // All mass in one interior √2 bin: upconverted percentile must sit
        // within that bin's edges.
        $bins = array_fill(0, 39, 0);
        $bins[10] = 100;
        $counts = Sketch::upconvert($bins);

        $this->assertSame(100, array_sum($counts));

        $edges = \NightOwl\Support\QueryHistogram::EDGES;
        $est = Sketch::percentile($counts, 0.95);
        $this->assertGreaterThanOrEqual($edges[9], $est);
        $this->assertLessThanOrEqual($edges[10], $est);
    }
}
