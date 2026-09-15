<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\RollupSpecs;
use NightOwl\Support\RollupStaleness;
use PHPUnit\Framework\TestCase;

class RollupStalenessTest extends TestCase
{
    /** 2026-07-31 00:00:00 UTC — the hour the Yomoney freeze started. */
    private const T0 = 1785456000;

    public function test_detects_the_one_table_that_stopped_while_its_source_kept_arriving(): void
    {
        // The real signature: prune's v1-EOL dropped nightowl_requests under a
        // running daemon, the concurrency recompute 42P01'd every tick from
        // then on, and requests kept landing in the v2 twin for another 11
        // hours. The only outward sign was four 504s on 14-day charts.
        $stale = RollupStaleness::detect([
            $this->row('nightowl_requests_v2', self::T0 + 11 * 3600 + 20),
            $this->row('nightowl_request_concurrency_rollups', self::T0),
            $this->row('nightowl_request_rollups', self::T0 + 11 * 3600),
        ]);

        $this->assertSame(['nightowl_request_concurrency_rollups' => 11 * 3600 + 20], $stale);
    }

    public function test_a_quiet_telemetry_type_is_not_stale(): void
    {
        // The false positive this rule replaced (2026-09-15, under one request
        // a second): no command for two hours, no mail for 89 minutes, no
        // exception for 37. Graded against the request rollup those read as
        // frozen; graded against their own raw tables they are current.
        $this->assertSame([], RollupStaleness::detect([
            $this->row('nightowl_requests_v2', self::T0 + 30),
            $this->row('nightowl_request_rollups', self::T0),
            $this->row('nightowl_commands_v2', self::T0 - 7200 + 12),
            $this->row('nightowl_command_rollups', self::T0 - 7200),
            $this->row('nightowl_mail_v2', self::T0 - 89 * 60 + 5),
            $this->row('nightowl_mail_rollups', self::T0 - 89 * 60),
            $this->row('nightowl_exceptions_v2', self::T0 - 37 * 60 + 41),
            $this->row('nightowl_exception_rollups', self::T0 - 37 * 60),
            $this->row('nightowl_user_exception_rollups', self::T0 - 37 * 60),
            $this->row('nightowl_exception_server_rollups', self::T0 - 37 * 60),
        ]));
    }

    public function test_tiers_are_allowed_their_own_bucket_width(): void
    {
        // An hourly bucket starts up to an hour before the newest event in it,
        // a daily one up to a day. Graded at the minute tolerance, every healthy
        // tenant on earth reports its coarse tiers as stale.
        $this->assertSame([], RollupStaleness::detect([
            $this->row('nightowl_requests_v2', self::T0 + 86399),
            $this->row('nightowl_request_rollups', self::T0 + 86340),
            $this->row('nightowl_request_hourly_rollups', self::T0 + 82800),
            $this->row('nightowl_request_daily_rollups', self::T0),
        ]));
    }

    public function test_a_frozen_hourly_table_is_caught_against_its_source(): void
    {
        $stale = RollupStaleness::detect([
            $this->row('nightowl_requests_v2', self::T0),
            $this->row('nightowl_request_rollups', self::T0),
            $this->row('nightowl_request_hourly_rollups', self::T0 - 86400),
        ]);

        $this->assertSame(['nightowl_request_hourly_rollups' => 86400], $stale);
    }

    public function test_the_minute_tolerance_is_the_boundary(): void
    {
        $this->assertSame([], RollupStaleness::detect([
            $this->row('nightowl_jobs_v2', self::T0 + 900),
            $this->row('nightowl_job_rollups', self::T0),
        ]));

        $this->assertSame(['nightowl_job_rollups' => 901], RollupStaleness::detect([
            $this->row('nightowl_jobs_v2', self::T0 + 901),
            $this->row('nightowl_job_rollups', self::T0),
        ]));
    }

    public function test_an_empty_rollup_is_not_stale(): void
    {
        // max_bucket null = the table exists but holds nothing. That is a
        // backfill that has not run (ROLLUP_BACKFILL_PENDING's job) — every
        // tenant is in this state for the hour after a new rollup type ships.
        $this->assertSame([], RollupStaleness::detect([
            $this->row('nightowl_notifications_v2', self::T0),
            $this->row('nightowl_mail_v2', self::T0),
            ['name' => 'nightowl_notification_rollups', 'max_bucket' => null],
            ['name' => 'nightowl_mail_rollups'], // bounds probe skipped this sample
        ]));
    }

    public function test_no_source_ceiling_means_no_verdict(): void
    {
        // The ceiling probe failed or the source is empty: nothing to grade
        // against, so no guess — even for a rollup that looks ancient.
        $this->assertSame([], RollupStaleness::detect([
            $this->row('nightowl_command_rollups', self::T0 - 999999),
            ['name' => 'nightowl_mail_v2', 'max_bucket' => null],
            $this->row('nightowl_mail_rollups', self::T0 - 999999),
        ]));
    }

    public function test_a_wholesale_freeze_is_left_to_the_drain_diagnoses(): void
    {
        // Drain stopped: raw froze with the rollups, so nothing trails its
        // source. DRAIN_STOPPED/DRAIN_WEDGED own this and say something useful;
        // "all your rollups are stale" would be noise on top.
        $this->assertSame([], RollupStaleness::detect([
            $this->row('nightowl_requests_v2', self::T0 - 86400 + 30),
            $this->row('nightowl_request_rollups', self::T0 - 86400),
            $this->row('nightowl_queries_v2', self::T0 - 86400 + 30),
            $this->row('nightowl_query_rollups', self::T0 - 86400),
        ]));
    }

    public function test_either_storage_family_can_hold_the_newest_row(): void
    {
        // Kill switch flipped back to v1: the v2 twin stopped, v1 is current.
        // Grading against the twin alone would call a healthy rollup frozen.
        $this->assertSame([], RollupStaleness::detect([
            $this->row('nightowl_requests_v2', self::T0 - 86400),
            $this->row('nightowl_requests', self::T0 + 10),
            $this->row('nightowl_request_rollups', self::T0),
        ]));

        // And the newer family wins in the other direction too.
        $this->assertSame(['nightowl_request_rollups' => 86400], RollupStaleness::detect([
            $this->row('nightowl_requests', self::T0),
            $this->row('nightowl_requests_v2', self::T0 + 86400),
            $this->row('nightowl_request_rollups', self::T0),
        ]));
    }

    public function test_a_lone_rollup_is_still_graded(): void
    {
        // The peer rule could never judge a table with no tier peers; its own
        // source can.
        $this->assertSame(['nightowl_request_rollups' => 7200], RollupStaleness::detect([
            $this->row('nightowl_requests_v2', self::T0),
            $this->row('nightowl_request_rollups', self::T0 - 7200),
        ]));
    }

    public function test_rollups_without_a_known_source_and_non_rollup_rows_are_ignored(): void
    {
        $this->assertSame([], RollupStaleness::detect([
            $this->row('nightowl_requests_v2', self::T0),
            $this->row('nightowl_zz_future_rollups', self::T0 - 999999),
            ['name' => 'index:nightowl_requests:some_idx', 'idx_scan' => 5],
            $this->row('nightowl_request_rollups', self::T0),
        ]));
    }

    public function test_worst_offender_is_listed_first(): void
    {
        $stale = RollupStaleness::detect([
            $this->row('nightowl_requests_v2', self::T0),
            $this->row('nightowl_queries_v2', self::T0),
            $this->row('nightowl_query_rollups', self::T0 - 3600),
            $this->row('nightowl_request_rollups', self::T0 - 86400),
        ]);

        $this->assertSame(['nightowl_request_rollups', 'nightowl_query_rollups'], array_keys($stale));
    }

    public function test_source_tables_cover_every_spec_in_both_families(): void
    {
        $sources = RollupStaleness::sourceTables();

        foreach (RollupSpecs::all() as $spec) {
            $this->assertContains($spec->source, $sources, "{$spec->table}'s source is never probed");
            $this->assertContains($spec->source.'_v2', $sources, "{$spec->table}'s v2 source is never probed");
        }
        $this->assertSame(count($sources), count(array_unique($sources)), 'a source is probed twice');
        $this->assertNotContains('nightowl_logs', $sources, 'no rollup summarises logs');
    }

    /**
     * @return array<string, mixed>
     */
    private function row(string $name, int $maxBucket): array
    {
        return ['name' => $name, 'max_bucket' => $maxBucket, 'bytes' => 4096];
    }
}
