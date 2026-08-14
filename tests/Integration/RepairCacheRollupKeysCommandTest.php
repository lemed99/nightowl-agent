<?php

namespace NightOwl\Tests\Integration;

/**
 * nightowl:repair-cache-rollup-keys --in-place — the merge-in-the-live-table
 * strategy, kept for the tenant whose disk cannot hold a second copy of the
 * table (the default path rebuilds and swaps; see RepairCacheRollupKeysRebuildTest).
 *
 * Everything both strategies owe is asserted in CacheRollupRepairTestCase. What
 * is only true here is the consequence of writing into the table the drain is
 * writing to: the pass has to leave the drain's live bucket alone, has to SAY it
 * left it alone, and — because it walks a time window rather than the whole
 * table — honours --since/--until.
 */
final class RepairCacheRollupKeysCommandTest extends CacheRollupRepairTestCase
{
    protected function strategyOptions(): array
    {
        return ['--in-place' => true];
    }

    public function test_says_already_repaired_when_every_key_is_its_own_pattern(): void
    {
        $this->insert('nightowl_cache_rollups', 'settings:global');
        $this->insert('nightowl_cache_rollups', 'user:{int}:profile');

        $this->assertStringContainsString('already repaired', $this->runRepair());
    }

    public function test_spares_buckets_inside_the_drain_safety_margin(): void
    {
        // The current minute belongs to the live drain; rewriting it would race
        // the writer this command deliberately never fights.
        $now = gmdate('Y-m-d H:i:00');
        $this->insert('nightowl_cache_rollups', 'user:1:profile', ['bucket_start' => $now]);
        $this->insert('nightowl_cache_rollups', 'user:2:profile');

        $this->runRepair(['--tier' => 'minute']);

        $this->assertSame(['user:1:profile', 'user:{int}:profile'], $this->keys());
    }

    public function test_names_the_buckets_the_safety_margin_left_behind(): void
    {
        // The counts alone read as "done": the pass reports what it collapsed and
        // says nothing about the tail it skipped. On the daily tier that tail is
        // the whole current day, so the operator has to be told to come back.
        $this->insert('nightowl_cache_rollups', 'user:1:profile');
        $this->insert('nightowl_cache_daily_rollups', 'user:1:profile');

        $output = $this->runRepair();

        $this->assertStringContainsString('Buckets the drain may still be writing were left untouched:', $output);
        $this->assertStringContainsString(
            'nightowl_cache_daily_rollups: at or after '.gmdate('Y-m-d').' 00:00:00 UTC',
            $output,
        );
    }

    public function test_since_and_until_bound_the_repair(): void
    {
        $old = gmdate('Y-m-d H:i:s', (int) (floor((time() - 10 * 86400) / 86400) * 86400));

        $this->insert('nightowl_cache_rollups', 'user:1:profile', ['bucket_start' => $old]);
        $this->insert('nightowl_cache_rollups', 'user:2:profile');

        $this->runRepair(['--since' => $this->oldBucket(4)]);

        $this->assertSame(['user:1:profile', 'user:{int}:profile'], $this->keys());
    }

}
