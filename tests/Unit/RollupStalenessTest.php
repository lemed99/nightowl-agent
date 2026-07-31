<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\RollupStaleness;
use PHPUnit\Framework\TestCase;

class RollupStalenessTest extends TestCase
{
    /** 2026-07-31 00:00:00 UTC — the hour the Yomoney freeze started. */
    private const T0 = 1785456000;

    public function test_detects_the_one_table_that_stopped_while_its_peers_advanced(): void
    {
        // The real signature: prune's v1-EOL dropped nightowl_requests under a
        // running daemon, the concurrency recompute 42P01'd every tick from
        // then on, and every other minute rollup carried on for another 11
        // hours. The only outward sign was four 504s on 14-day charts.
        $stale = RollupStaleness::detect([
            $this->rollup('nightowl_request_concurrency_rollups', self::T0),
            $this->rollup('nightowl_request_rollups', self::T0 + 11 * 3600),
            $this->rollup('nightowl_query_rollups', self::T0 + 11 * 3600),
            $this->rollup('nightowl_job_rollups', self::T0 + 11 * 3600 - 60),
        ]);

        $this->assertSame(['nightowl_request_concurrency_rollups' => 11 * 3600], $stale);
    }

    public function test_a_healthy_tenant_reports_nothing(): void
    {
        // Minute tables drift by a bucket or two between samples; that is not a
        // freeze and must never page anyone.
        $this->assertSame([], RollupStaleness::detect([
            $this->rollup('nightowl_request_rollups', self::T0),
            $this->rollup('nightowl_query_rollups', self::T0 - 60),
            $this->rollup('nightowl_cache_rollups', self::T0 - 180),
            $this->rollup('nightowl_mail_rollups', self::T0 - 600),
        ]));
    }

    public function test_tiers_are_graded_against_their_own_kind(): void
    {
        // A daily table's newest bucket is by construction up to a day behind a
        // minute table's. Graded across tiers, every healthy tenant on earth
        // reports its daily rollups as stale.
        $this->assertSame([], RollupStaleness::detect([
            $this->rollup('nightowl_request_rollups', self::T0),
            $this->rollup('nightowl_query_rollups', self::T0),
            $this->rollup('nightowl_request_hourly_rollups', self::T0 - 3600),
            $this->rollup('nightowl_query_hourly_rollups', self::T0 - 3600),
            $this->rollup('nightowl_request_daily_rollups', self::T0 - 86400),
            $this->rollup('nightowl_query_daily_rollups', self::T0 - 86400),
        ]));
    }

    public function test_a_frozen_hourly_table_is_caught_against_its_hourly_peers(): void
    {
        $stale = RollupStaleness::detect([
            $this->rollup('nightowl_request_rollups', self::T0),
            $this->rollup('nightowl_query_rollups', self::T0),
            $this->rollup('nightowl_request_hourly_rollups', self::T0 - 86400),
            $this->rollup('nightowl_query_hourly_rollups', self::T0 - 3600),
        ]);

        $this->assertSame(['nightowl_request_hourly_rollups' => 86400 - 3600], $stale);
    }

    public function test_an_empty_rollup_is_not_stale(): void
    {
        // max_bucket null = the table exists but holds nothing. That is a
        // backfill that has not run (ROLLUP_BACKFILL_PENDING's job) — every
        // tenant is in this state for the hour after a new rollup type ships.
        $this->assertSame([], RollupStaleness::detect([
            $this->rollup('nightowl_request_rollups', self::T0),
            $this->rollup('nightowl_query_rollups', self::T0),
            ['name' => 'nightowl_notification_rollups', 'max_bucket' => null],
            ['name' => 'nightowl_mail_rollups'], // bounds probe skipped this sample
        ]));
    }

    public function test_a_wholesale_freeze_is_left_to_the_drain_diagnoses(): void
    {
        // Drain stopped: everything froze together, so nothing is behind its
        // peers. DRAIN_STOPPED/DRAIN_WEDGED own this and say something useful;
        // "all your rollups are stale" would be noise on top.
        $this->assertSame([], RollupStaleness::detect([
            $this->rollup('nightowl_request_rollups', self::T0 - 86400),
            $this->rollup('nightowl_query_rollups', self::T0 - 86400),
            $this->rollup('nightowl_job_rollups', self::T0 - 86400),
        ]));
    }

    public function test_non_rollup_tables_are_ignored(): void
    {
        $this->assertSame([], RollupStaleness::detect([
            ['name' => 'nightowl_requests', 'max_bucket' => self::T0 - 999999],
            ['name' => 'index:nightowl_requests:some_idx', 'idx_scan' => 5],
            $this->rollup('nightowl_request_rollups', self::T0),
            $this->rollup('nightowl_query_rollups', self::T0),
        ]));
    }

    public function test_a_lone_table_in_a_tier_has_no_peers_to_be_behind(): void
    {
        // Silent by design: with one sample there is no self-calibrating
        // baseline, and an absolute deadline would have to assume this app
        // emits that telemetry type at all.
        $this->assertSame([], RollupStaleness::detect([
            $this->rollup('nightowl_request_rollups', self::T0 - 999999),
        ]));
    }

    public function test_worst_offender_is_listed_first(): void
    {
        $stale = RollupStaleness::detect([
            $this->rollup('nightowl_request_rollups', self::T0),
            $this->rollup('nightowl_query_rollups', self::T0 - 3600),
            $this->rollup('nightowl_job_rollups', self::T0 - 86400),
        ]);

        $this->assertSame(['nightowl_job_rollups', 'nightowl_query_rollups'], array_keys($stale));
    }

    /**
     * @return array<string, mixed>
     */
    private function rollup(string $name, int $maxBucket): array
    {
        return ['name' => $name, 'max_bucket' => $maxBucket, 'bytes' => 4096];
    }
}
