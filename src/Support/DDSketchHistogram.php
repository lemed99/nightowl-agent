<?php

namespace NightOwl\Support;

/**
 * DDSketch-grade relative-error percentile sketch for rollup durations —
 * specs/ddsketch_percentiles.md. Supersedes the √2 histogram's RESOLUTION
 * only; the min/max clamp and geometric within-bucket interpolation carry
 * forward via percentile().
 *
 * MUST stay byte-identical to nightowl-api's App\Support\DDSketchHistogram —
 * the agent assigns indices at drain time, the API estimates from the merged
 * counts, and both sides' packed bytea layouts must agree exactly. Guarded by
 * checksum tests on both sides (like QueryHistogram::EDGES).
 *
 * Mapping (frozen as VERSION 2 — never re-edge in place; a new mapping is a
 * new version, per the spec's retention-gap constraint):
 *   α = 0.01, γ = (1+α)/(1−α); index(v) = ceil(ln v / ln γ) for v in µs.
 *   Guaranteed relative error of value(i) = 2γ^i/(γ+1) over its bucket is α.
 *   Reserved indices: UNDERFLOW (v < 1µs) and OVERFLOW (v > 1 day) bound the
 *   index range so a pathological duration can't mint unbounded indices; the
 *   stored exact min/max clamp recovers the tails.
 *
 * Packed layout (opaque bytea): unsigned-LEB128 varints, pairs of
 * (index_delta, count) over the ascending index list; the first "delta" is
 * index − UNDERFLOW (≥ 0). Empty sketch packs to ''.
 */
final class DDSketchHistogram
{
    public const VERSION = 2;

    public const ALPHA = 0.01;

    /** γ = (1+α)/(1−α) */
    public const GAMMA = 101 / 99;

    /** Values below 1µs are measurement noise → underflow collector. */
    public const MIN_VALUE = 1;

    /** Values above 1 day (µs) → overflow collector. */
    public const MAX_VALUE = 86_400_000_000;

    public const UNDERFLOW_INDEX = -1;

    /** ceil(ln(MAX_VALUE)/ln(GAMMA)) + 1; frozen, guarded by checksum test. */
    public const OVERFLOW_INDEX = 1261;

    /** Sketch index for a duration in microseconds. */
    public static function index(int|float $value): int
    {
        if ($value < self::MIN_VALUE) {
            return self::UNDERFLOW_INDEX;
        }
        if ($value > self::MAX_VALUE) {
            return self::OVERFLOW_INDEX;
        }

        $i = (int) ceil(log((float) $value) / log(self::GAMMA));

        // ceil() at an exact power of γ can land one past OVERFLOW-1 by a ulp.
        return min(max($i, 0), self::OVERFLOW_INDEX - 1);
    }

    /**
     * DDSketch representative for index i: 2γ^i/(γ+1) — worst-case relative
     * error over the bucket [γ^(i−1), γ^i) is exactly α at either edge.
     */
    public static function value(int $index): float
    {
        if ($index <= self::UNDERFLOW_INDEX) {
            return (float) self::MIN_VALUE;
        }
        if ($index >= self::OVERFLOW_INDEX) {
            return (float) self::MAX_VALUE;
        }

        return 2.0 * (self::GAMMA ** $index) / (self::GAMMA + 1.0);
    }

    /** Lower edge γ^(i−1) of bucket i, for geometric within-bucket refinement. */
    public static function lowerEdge(int $index): float
    {
        if ($index <= self::UNDERFLOW_INDEX) {
            return (float) self::MIN_VALUE;
        }

        return self::GAMMA ** ($index - 1);
    }

    /** Upper edge γ^i of bucket i. */
    public static function upperEdge(int $index): float
    {
        if ($index >= self::OVERFLOW_INDEX) {
            return (float) self::MAX_VALUE;
        }

        return self::GAMMA ** $index;
    }

    /**
     * Accumulate a duration into a sparse index=>count map (drain hot path).
     *
     * @param  array<int, int>  $counts
     */
    public static function add(array &$counts, int|float $duration): void
    {
        $i = self::index($duration);
        $counts[$i] = ($counts[$i] ?? 0) + 1;
    }

    /**
     * Sum two sparse maps — the PHP-side twin of nightowl_ddsketch_merge().
     *
     * @param  array<int, int>  $a
     * @param  array<int, int>  $b
     * @return array<int, int>
     */
    public static function mergeCounts(array $a, array $b): array
    {
        foreach ($b as $i => $c) {
            $a[$i] = ($a[$i] ?? 0) + $c;
        }

        return $a;
    }

    /**
     * Pack a sparse map to the varint bytea payload. Deterministic: ascending
     * index order, first delta measured from UNDERFLOW_INDEX.
     *
     * @param  array<int, int>  $counts
     */
    public static function pack(array $counts): string
    {
        $counts = array_filter($counts, static fn (int $c): bool => $c > 0);
        if ($counts === []) {
            return '';
        }
        ksort($counts);

        $out = '';
        $prev = self::UNDERFLOW_INDEX;
        foreach ($counts as $i => $c) {
            $out .= self::varint($i - $prev);
            $out .= self::varint($c);
            $prev = $i;
        }

        return $out;
    }

    /**
     * Unpack a payload back to the sparse map. Tolerant of '' / null-ish input
     * (empty sketch). Malformed trailing bytes raise — a corrupt sketch must
     * surface, not silently skew percentiles.
     *
     * @return array<int, int>
     */
    public static function unpack(?string $packed): array
    {
        if ($packed === null || $packed === '') {
            return [];
        }

        $counts = [];
        $pos = 0;
        $len = strlen($packed);
        $prev = self::UNDERFLOW_INDEX;

        while ($pos < $len) {
            $delta = self::readVarint($packed, $pos, $len);
            $count = self::readVarint($packed, $pos, $len);
            $prev += $delta;
            $counts[$prev] = ($counts[$prev] ?? 0) + $count;
        }

        return $counts;
    }

    /** merge() over packed payloads — unpack, sum, repack. */
    public static function merge(?string $a, ?string $b): string
    {
        return self::pack(self::mergeCounts(self::unpack($a), self::unpack($b)));
    }

    /**
     * Unpack a sketch as PDO pgsql hands it back — a bytea stream of raw
     * packed bytes — into the sparse map. Null for absent/empty (caller falls
     * back to the v1 histogram path).
     *
     * A bare string is taken as those same raw bytes: every read path selects
     * bytea (nightowl_ddsketch_agg), which pdo_pgsql always unescapes to a
     * stream, so the '\x…' text of a ::text-cast never reaches here.
     *
     * @return array<int, int>|null
     */
    public static function countsFromDb(mixed $value): ?array
    {
        if (is_resource($value)) {
            // pdo_pgsql unescapes bytea itself, so a stream carries raw sketch
            // bytes — which can themselves open with the characters '\' 'x'.
            // Callers read one row once per percentile (p50/p95/p99), so rewind
            // first: a consumed stream would read back as an empty sketch.
            if (stream_get_meta_data($value)['seekable'] ?? false) {
                rewind($value);
            }
            $value = stream_get_contents($value);
        }
        if (! is_string($value) || $value === '') {
            return null;
        }

        $counts = self::unpack($value);

        return $counts === [] ? null : $counts;
    }

    /**
     * Scatter a √2 histogram (v1 hist_NN counts) into sketch space so mixed
     * v1/v2 windows merge: each √2 bin's count lands at the index of the bin's
     * geometric-mean value. The v1 segment keeps ~√2 accuracy — bounded by the
     * coarser side, never worse than today (spec §Versioning).
     *
     * @param  array<int|string, int>  $binCounts  39 √2 bin counts (hist_00..hist_38 order)
     * @return array<int, int>
     */
    public static function upconvert(array $binCounts): array
    {
        $edges = QueryHistogram::EDGES;
        $n = count($edges) + 1;
        $counts = [];
        $values = array_values($binCounts);

        for ($bin = 0; $bin < $n; $bin++) {
            $c = (int) ($values[$bin] ?? 0);
            if ($c <= 0) {
                continue;
            }

            if ($bin === 0) {
                $rep = $edges[0] / 2; // underflow: below first edge
            } elseif ($bin >= $n - 1) {
                $rep = $edges[$n - 2]; // overflow: at last edge; clamp fixes the tail
            } else {
                $rep = sqrt((float) $edges[$bin - 1] * (float) $edges[$bin]);
            }

            $i = self::index($rep);
            $counts[$i] = ($counts[$i] ?? 0) + $c;
        }

        return $counts;
    }

    /**
     * Percentile estimate from a sparse map: rank walk to the crossing index,
     * geometric within-bucket refinement, clamped to the observed min/max
     * (exact tails) — the same shape as QueryHistogram::estimatePercentile.
     *
     * @param  array<int, int>  $counts
     */
    public static function percentile(array $counts, float $p, ?int $min = null, ?int $max = null): float
    {
        $total = array_sum($counts);
        if ($total <= 0) {
            return 0.0;
        }
        ksort($counts);

        $rank = max(1, (int) ceil($total * $p));
        $cumulative = 0;

        foreach ($counts as $i => $count) {
            if ($count <= 0) {
                continue;
            }
            if ($cumulative + $count < $rank) {
                $cumulative += $count;

                continue;
            }

            $lo = self::lowerEdge($i);
            $hi = self::upperEdge($i);
            $within = ($rank - $cumulative) / $count; // (0, 1]

            if ($min !== null && $min > $lo) {
                $lo = (float) $min;
            }
            if ($max !== null && $max < $hi) {
                $hi = (float) $max;
            }
            $hi = max($hi, $lo);

            // Geometric (log-linear) interpolation — the buckets are log-spaced.
            $estimate = $lo > 0 ? $lo * (($hi / $lo) ** $within) : $hi * $within;

            if ($min !== null) {
                $estimate = max($estimate, (float) $min);
            }
            if ($max !== null) {
                $estimate = min($estimate, (float) $max);
            }

            return $estimate;
        }

        return $max !== null ? (float) $max : 0.0;
    }

    /**
     * Contract checksum: constants + mapping probes + pack layout. The twin
     * class in nightowl-api must produce the identical string.
     */
    public static function checksum(): string
    {
        $probes = [];
        foreach ([1, 128, 1000, 128_000, 1_000_000, 47_000_000, self::MAX_VALUE] as $v) {
            $probes[] = self::index($v);
        }

        return md5(implode('|', [
            self::VERSION,
            sprintf('%.17g', self::ALPHA),
            sprintf('%.17g', self::GAMMA),
            self::MIN_VALUE,
            self::MAX_VALUE,
            self::UNDERFLOW_INDEX,
            self::OVERFLOW_INDEX,
            implode(',', $probes),
            bin2hex(self::pack([-1 => 2, 0 => 1, 412 => 18, self::OVERFLOW_INDEX => 3])),
        ]));
    }

    private static function varint(int $n): string
    {
        $out = '';
        while ($n >= 0x80) {
            $out .= chr(($n & 0x7F) | 0x80);
            $n >>= 7;
        }

        return $out.chr($n);
    }

    private static function readVarint(string $data, int &$pos, int $len): int
    {
        $result = 0;
        $shift = 0;
        while (true) {
            if ($pos >= $len) {
                throw new \RuntimeException('DDSketch payload truncated mid-varint');
            }
            $byte = ord($data[$pos++]);
            $result |= ($byte & 0x7F) << $shift;
            if (($byte & 0x80) === 0) {
                return $result;
            }
            $shift += 7;
        }
    }
}
