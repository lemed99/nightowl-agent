<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\RollupTiers;
use PHPUnit\Framework\TestCase;

final class RollupTiersTest extends TestCase
{
    public function test_table_names_put_tier_before_rollups_suffix(): void
    {
        $this->assertSame(
            'nightowl_request_hourly_rollups',
            RollupTiers::table('nightowl_request_rollups', 'hourly')
        );
        $this->assertSame(
            'nightowl_user_exception_daily_rollups',
            RollupTiers::table('nightowl_user_exception_rollups', 'daily')
        );
    }

    public function test_tables_lists_finest_first_with_granularities(): void
    {
        $this->assertSame([
            'nightowl_job_rollups' => 60,
            'nightowl_job_hourly_rollups' => 3600,
            'nightowl_job_daily_rollups' => 86400,
        ], RollupTiers::tables('nightowl_job_rollups'));
    }

    public function test_tier_tables_excludes_base(): void
    {
        $this->assertSame(
            ['nightowl_job_hourly_rollups', 'nightowl_job_daily_rollups'],
            RollupTiers::tierTables('nightowl_job_rollups')
        );
    }

    public function test_truncate_bucket_hour_and_day_in_utc(): void
    {
        $this->assertSame('2026-07-16 17:00:00', RollupTiers::truncateBucket('2026-07-16 17:48:00', 3600));
        $this->assertSame('2026-07-16 00:00:00', RollupTiers::truncateBucket('2026-07-16 17:48:00', 86400));
        // Already-aligned buckets pass through unchanged.
        $this->assertSame('2026-07-16 17:00:00', RollupTiers::truncateBucket('2026-07-16 17:00:00', 3600));
    }

    /** The generic writeRollup group shape: nested 'group', counters, hist, reps. */
    public function test_collapse_merges_generic_groups_into_hour_buckets(): void
    {
        $minute = fn (string $bucket, int $calls, int $failed, ?int $min, ?int $max, int $total, array $hist, ?string $rep): array => [
            'group' => ['group_hash' => 'abc'],
            'bucket_start' => $bucket,
            'call_count' => $calls,
            'counters' => ['failed_count' => $failed],
            'total_duration' => $total,
            'duration_count' => array_sum($hist),
            'min_duration' => $min,
            'max_duration' => $max,
            'hist' => $hist,
            'reps' => ['job_class' => $rep],
        ];

        $collapsed = RollupTiers::collapse([
            $minute('2026-07-16 17:05:00', 3, 1, 100, 900, 1500, [2, 1, 0], 'App\Jobs\A'),
            $minute('2026-07-16 17:59:00', 2, 0, 50, 500, 550, [1, 0, 1], null),
            $minute('2026-07-16 18:00:00', 1, 1, null, null, 0, [0, 0, 0], 'App\Jobs\B'),
        ], 3600, ['group']);

        $this->assertCount(2, $collapsed);

        $hour17 = $collapsed["2026-07-16 17:00:00\0abc"];
        $this->assertSame(5, $hour17['call_count']);
        $this->assertSame(1, $hour17['counters']['failed_count']);
        $this->assertSame(2050, $hour17['total_duration']);
        $this->assertSame(5, $hour17['duration_count'], 'duration_count sums across collapsed minutes');
        $this->assertSame(50, $hour17['min_duration']);
        $this->assertSame(900, $hour17['max_duration']);
        $this->assertSame([3, 1, 1], $hour17['hist']);
        // First-seen representative survives the merge.
        $this->assertSame('App\Jobs\A', $hour17['reps']['job_class']);

        $hour18 = $collapsed["2026-07-16 18:00:00\0abc"];
        $this->assertSame(1, $hour18['call_count']);
        $this->assertNull($hour18['min_duration']);
    }

    /** Null-aware min/max folding: null on either side yields the other. */
    public function test_collapse_folds_null_durations(): void
    {
        $g = fn (string $bucket, ?int $min, ?int $max): array => [
            'group' => ['h' => 'x'],
            'bucket_start' => $bucket,
            'call_count' => 1,
            'counters' => [],
            'total_duration' => 0,
            'min_duration' => $min,
            'max_duration' => $max,
            'hist' => [],
            'reps' => [],
        ];

        $collapsed = RollupTiers::collapse([
            $g('2026-07-16 17:01:00', null, null),
            $g('2026-07-16 17:02:00', 40, 40),
            $g('2026-07-16 17:03:00', null, null),
        ], 3600, ['group']);

        $row = array_values($collapsed)[0];
        $this->assertSame(40, $row['min_duration']);
        $this->assertSame(40, $row['max_duration']);
    }

    public function test_tier_backfill_sql_reaggregates_rollup_columns(): void
    {
        $spec = \NightOwl\Support\RollupSpecs::jobs();
        $parts = $spec->tierBackfillSql('hour', ['hist_00', 'hist_01']);

        // Destination columns mirror the base rollup's insert set.
        $this->assertContains('bucket_start', $parts['columns']);
        $this->assertContains('hist_00', $parts['columns']);
        $this->assertContains('job_class', $parts['columns']);

        // Selects re-aggregate the finer rollup: SUM counters/bins, fold
        // min/max, truncate the bucket, keep MIN(representative). No FILTER —
        // the finer tier already applied the duration predicate.
        $joined = implode(' | ', $parts['selects']);
        $this->assertStringContainsString("date_trunc('hour', bucket_start)", $joined);
        $this->assertStringContainsString('SUM(call_count)', $joined);
        $this->assertStringContainsString('SUM(hist_00)', $joined);
        $this->assertStringContainsString('MIN(min_duration)', $joined);
        $this->assertStringContainsString('MAX(max_duration)', $joined);
        $this->assertStringContainsString('MIN(job_class)', $joined);
        $this->assertStringNotContainsString('FILTER', $joined);

        $this->assertSame(count($parts['columns']), count($parts['selects']));
        // group cols + bucket + environment lead the select list positionally.
        $this->assertSame(count($spec->groupColumnNames()) + 2, $parts['groupByCount']);
    }

    public function test_tier_backfill_sql_for_count_only_spec_has_no_duration_columns(): void
    {
        $spec = \NightOwl\Support\RollupSpecs::requestUsers();
        $parts = $spec->tierBackfillSql('day', []);

        $this->assertNotContains('total_duration', $parts['columns']);
        $this->assertNotContains('hist_00', $parts['columns']);
        $this->assertStringContainsString("date_trunc('day', bucket_start)", implode(' | ', $parts['selects']));
        $this->assertSame(count($parts['columns']), count($parts['selects']));
    }

    /** The bespoke query shape: flat group_hash/connection identity + sql_query rep. */
    public function test_collapse_merges_query_groups_keeping_connection_identity(): void
    {
        $q = fn (string $bucket, string $conn, int $calls, ?string $sql): array => [
            'group_hash' => 'qh',
            'connection' => $conn,
            'bucket_start' => $bucket,
            'call_count' => $calls,
            'total_duration' => 10,
            'min_duration' => 10,
            'max_duration' => 10,
            'sql_query' => $sql,
            'hist' => [1],
        ];

        $collapsed = RollupTiers::collapse([
            $q('2026-07-16 17:05:00', 'pgsql', 2, null),
            $q('2026-07-16 17:45:00', 'pgsql', 3, 'SELECT 1'),
            $q('2026-07-16 17:50:00', 'mysql', 1, 'SELECT 2'),
        ], 3600, ['group_hash', 'connection']);

        // Same hour, different connections → distinct tier rows.
        $this->assertCount(2, $collapsed);

        $pgsql = $collapsed["2026-07-16 17:00:00\0qh\0pgsql"];
        $this->assertSame(5, $pgsql['call_count']);
        $this->assertSame([2], $pgsql['hist']);
        // First non-null sql_query wins even when the first minute had none.
        $this->assertSame('SELECT 1', $pgsql['sql_query']);
    }
}
