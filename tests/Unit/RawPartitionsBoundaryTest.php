<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\RawPartitions;
use PHPUnit\Framework\TestCase;

final class RawPartitionsBoundaryTest extends TestCase
{
    /**
     * The boundary frozen into {t}_hist_ck must still be in the future when
     * VALIDATE CONSTRAINT finishes scanning the table and when the swap commits.
     * Driven off an injected clock rather than time(), because the bug is a
     * time-of-day bug: a suite that only ever runs at noon cannot see it, and a
     * suite that runs at 23:56 would see it once and never again.
     */
    public function test_the_frozen_boundary_always_clears_a_full_day(): void
    {
        // Every minute of one UTC day.
        $midnight = 1_770_000_000 - (1_770_000_000 % 86400);

        for ($s = 0; $s < 86400; $s += 60) {
            $now = $midnight + $s;
            $boundary = RawPartitions::historicBoundary($now);

            $this->assertSame(0, $boundary % 86400,
                "at +{$s}s the boundary must be a UTC midnight — a bound inside a day leaves that day's "
                .'tail belonging to no partition, and those rows fall to {t}_pdefault');
            $this->assertGreaterThanOrEqual(86400, $boundary - $now,
                "at +{$s}s the boundary must clear a full day: VALIDATE CONSTRAINT's full-table scan and "
                .'the swap both run after it is frozen, and it rejects every drain row (23514) the moment '
                .'it passes');
            $this->assertLessThanOrEqual(2 * 86400, $boundary - $now,
                "at +{$s}s the boundary must not run past two days — every extra day is a day of rows "
                .'{t}_phistoric holds that prune can only row-DELETE');
        }
    }

    /** The bound is the SECOND midnight ahead, not the first, at every hour. */
    public function test_the_boundary_is_the_second_midnight_ahead(): void
    {
        $midnight = 1_770_000_000 - (1_770_000_000 % 86400);

        $this->assertSame($midnight + 2 * 86400, RawPartitions::historicBoundary($midnight));
        $this->assertSame($midnight + 2 * 86400, RawPartitions::historicBoundary($midnight + 43_200));
        $this->assertSame($midnight + 2 * 86400, RawPartitions::historicBoundary($midnight + 86_340));
    }
}
