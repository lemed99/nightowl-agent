<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\QueryHistogram;
use PHPUnit\Framework\TestCase;

class QueryHistogramTest extends TestCase
{
    /**
     * The bin edges are a cross-repo contract: the agent assigns bins and the
     * API estimates percentiles from them, so both repos' QueryHistogram::EDGES
     * must be byte-identical. This frozen checksum trips if either side drifts.
     * The same assertion lives in nightowl-api/tests/Unit/QueryHistogramTest.
     */
    public function test_edges_match_frozen_checksum(): void
    {
        $this->assertSame(38, count(QueryHistogram::EDGES));
        $this->assertSame(39, QueryHistogram::binCount());
        $this->assertSame(
            '32995a8c86e2800f5037189580b0451f',
            md5(implode(',', QueryHistogram::EDGES)),
            'QueryHistogram::EDGES drifted — agent and API must stay byte-identical'
        );
    }

    public function test_edges_are_strictly_ascending(): void
    {
        $prev = -1;
        foreach (QueryHistogram::EDGES as $edge) {
            $this->assertGreaterThan($prev, $edge);
            $prev = $edge;
        }
    }

    public function test_columns_are_zero_padded_and_complete(): void
    {
        $columns = QueryHistogram::columns();
        $this->assertCount(39, $columns);
        $this->assertSame('hist_00', $columns[0]);
        $this->assertSame('hist_38', $columns[38]);
    }

    public function test_bin_index_assigns_underflow_overflow_and_middle(): void
    {
        // Below the first edge → underflow bin 0.
        $this->assertSame(0, QueryHistogram::binIndex(0));
        $this->assertSame(0, QueryHistogram::binIndex(127));

        // Exactly the first edge → bin 1.
        $this->assertSame(1, QueryHistogram::binIndex(128));

        // At/above the last edge → overflow bin 38.
        $this->assertSame(38, QueryHistogram::binIndex(47453133));
        $this->assertSame(38, QueryHistogram::binIndex(999999999));

        // Bin index == number of edges <= duration.
        $this->assertSame(QueryHistogram::binIndex(1000), $this->edgesAtMost(1000));
    }

    public function test_estimate_percentile_lands_in_crossing_bin(): void
    {
        // 100 calls all in one bin (index 10 → [EDGES[9], EDGES[10]) = [2896, 4096)).
        $bins = array_fill(0, QueryHistogram::binCount(), 0);
        $bins[10] = 100;

        $p50 = QueryHistogram::estimatePercentile($bins, 0.50);
        $this->assertGreaterThanOrEqual(2896, $p50);
        $this->assertLessThan(4096, $p50);
    }

    public function test_estimate_percentile_empty_is_zero(): void
    {
        $bins = array_fill(0, QueryHistogram::binCount(), 0);
        $this->assertSame(0.0, QueryHistogram::estimatePercentile($bins, 0.95));
    }

    public function test_estimate_percentile_overflow_returns_lower_edge(): void
    {
        $bins = array_fill(0, QueryHistogram::binCount(), 0);
        $bins[38] = 10; // overflow bin

        $this->assertSame((float) 47453133, QueryHistogram::estimatePercentile($bins, 0.95));
    }

    private function edgesAtMost(int $duration): int
    {
        $count = 0;
        foreach (QueryHistogram::EDGES as $edge) {
            if ($edge <= $duration) {
                $count++;
            }
        }

        return $count;
    }
}
