<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\QueryHistogram;
use NightOwl\Support\RollupSpecs;
use PHPUnit\Framework\TestCase;

/**
 * Locks the exception-group / mail / notification rollup specs. The parity
 * invariant is the same as every rollup: the PHP drain predicate and the SQL
 * backfill condition must produce identical aggregates so the read path matches a
 * raw re-aggregation. Pure spec assertions (no DB).
 */
class RollupSpecsExpansionTest extends TestCase
{
    public function test_new_specs_registered_in_all(): void
    {
        $tables = array_map(static fn ($s) => $s->table, RollupSpecs::all());

        $this->assertContains('nightowl_exception_rollups', $tables);
        $this->assertContains('nightowl_mail_rollups', $tables);
        $this->assertContains('nightowl_notification_rollups', $tables);
    }

    public function test_exception_groups_keys_on_fingerprint_matching_write_path(): void
    {
        $spec = RollupSpecs::exceptionGroups();
        $this->assertSame('nightowl_exception_rollups', $spec->table);
        $this->assertSame(['fingerprint'], $spec->groupColumnNames());

        $php = $spec->groupColumns['fingerprint']['php'];
        // SDK _group wins when present...
        $this->assertSame('grp-123', $php(['_group' => 'grp-123', 'class' => 'X']));
        // ...else the local class|code|file|line hash, byte-identical to writeExceptions().
        $this->assertSame(
            md5('RuntimeException|42|/app/x.php|10'),
            $php(['class' => 'RuntimeException', 'code' => '42', 'file' => '/app/x.php', 'line' => 10]),
        );
        $this->assertSame("COALESCE(fingerprint, '')", $spec->groupColumns['fingerprint']['sql']);
    }

    public function test_exception_groups_handled_bands_match_controller_sql(): void
    {
        $spec = RollupSpecs::exceptionGroups();
        $handled = $spec->counters['handled_count'];
        $unhandled = $spec->counters['unhandled_count'];

        // Bands are complementary and match ExceptionController's raw SQL.
        $this->assertTrue(($handled['php'])(['handled' => true]));
        $this->assertFalse(($handled['php'])(['handled' => false]));
        $this->assertFalse(($handled['php'])([]), 'missing handled defaults to unhandled');
        $this->assertTrue(($unhandled['php'])(['handled' => false]));
        $this->assertTrue(($unhandled['php'])([]));
        $this->assertFalse(($unhandled['php'])(['handled' => true]));

        $this->assertSame('handled = true', $handled['sql']);
        $this->assertSame('handled != true OR handled IS NULL', $unhandled['sql']);

        // Count-only: no duration/histogram.
        $this->assertFalse($spec->hasDuration);
        $this->assertFalse($spec->hasHistogram);
        ['columns' => $columns, 'groupByCount' => $groupBy] = $spec->backfillSql([]);
        $this->assertSame(['fingerprint', 'bucket_start', 'environment', 'call_count', 'handled_count', 'unhandled_count'], $columns);
        $this->assertSame(3, $groupBy);
    }

    public function test_mail_spec_is_group_hash_with_duration_histogram(): void
    {
        $spec = RollupSpecs::mail();
        $this->assertSame('nightowl_mail_rollups', $spec->table);
        $this->assertSame(['group_hash'], $spec->groupColumnNames());
        $this->assertSame(['queued_count', 'failed_count'], $spec->counterColumns());
        $this->assertTrue($spec->hasDuration);
        $this->assertTrue($spec->hasHistogram);

        $this->assertTrue(($spec->counters['queued_count']['php'])(['queued' => true]));
        $this->assertFalse(($spec->counters['queued_count']['php'])(['queued' => false]));
        $this->assertSame('queued = true', $spec->counters['queued_count']['sql']);

        // Backfill projects duration totals + all 39 histogram bins + the representative.
        ['columns' => $columns] = $spec->backfillSql(QueryHistogram::caseSql('duration'));
        $this->assertContains('total_duration', $columns);
        $this->assertContains('hist_00', $columns);
        $this->assertContains('hist_38', $columns);
        $this->assertContains('mailable', $columns);
    }

    public function test_notification_spec_keys_on_group_hash_and_channel(): void
    {
        $spec = RollupSpecs::notifications();
        $this->assertSame('nightowl_notification_rollups', $spec->table);
        // Two-column group key — lets the list rebuild its DISTINCT channel set.
        $this->assertSame(['group_hash', 'channel'], $spec->groupColumnNames());
        $this->assertSame("COALESCE(channel, '')", $spec->groupColumns['channel']['sql']);
        $this->assertSame('mail', ($spec->groupColumns['channel']['php'])(['channel' => 'mail']));
        $this->assertSame('', ($spec->groupColumns['channel']['php'])([]));

        $this->assertTrue($spec->hasDuration);
        $this->assertTrue($spec->hasHistogram);

        // group by group_hash + channel + bucket_start + environment = 4 positions.
        ['groupByCount' => $groupBy] = $spec->backfillSql([]);
        $this->assertSame(4, $groupBy);
    }
}
