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

        // authenticated_count: occurrences carrying a user_id (guest = call_count -
        // authenticated_count). '0' is a valid user id, so the predicate tests for a
        // non-empty string, not empty() (which would treat '0' as absent).
        $authed = $spec->counters['authenticated_count'];
        $this->assertTrue(($authed['php'])(['user' => 'u1']));
        $this->assertTrue(($authed['php'])(['user' => '0']), "'0' is a real user id → authenticated");
        $this->assertFalse(($authed['php'])(['user' => '']));
        $this->assertFalse(($authed['php'])([]), 'missing user → guest');
        $this->assertSame("user_id IS NOT NULL AND user_id != ''", $authed['sql']);

        // Count-only: no duration/histogram.
        $this->assertFalse($spec->hasDuration);
        $this->assertFalse($spec->hasHistogram);
        ['columns' => $columns, 'groupByCount' => $groupBy] = $spec->backfillSql([]);
        $this->assertSame(['fingerprint', 'bucket_start', 'environment', 'call_count', 'handled_count', 'unhandled_count', 'authenticated_count'], $columns);
        $this->assertSame(3, $groupBy);
    }

    public function test_exception_servers_spec_is_fingerprint_server_keyed(): void
    {
        $spec = RollupSpecs::exceptionServers();
        $this->assertSame('nightowl_exception_server_rollups', $spec->table);
        $this->assertSame('nightowl_exceptions', $spec->source);
        $this->assertSame(['fingerprint', 'server'], $spec->groupColumnNames());
        $this->assertSame([], $spec->counterColumns(), 'count-only: server presence is the signal');
        $this->assertFalse($spec->hasDuration);
        $this->assertFalse($spec->hasHistogram);

        // fingerprint matches exceptionGroups (byte-identical to writeExceptions).
        $this->assertSame('grp-9', ($spec->groupColumns['fingerprint']['php'])(['_group' => 'grp-9']));
        $this->assertSame(
            md5('RuntimeException|42|/app/x.php|10'),
            ($spec->groupColumns['fingerprint']['php'])(['class' => 'RuntimeException', 'code' => '42', 'file' => '/app/x.php', 'line' => 10]),
        );
        // server dimension.
        $this->assertSame('web-1', ($spec->groupColumns['server']['php'])(['server' => 'web-1']));
        $this->assertSame('', ($spec->groupColumns['server']['php'])([]));
        $this->assertSame("COALESCE(server, '')", $spec->groupColumns['server']['sql']);

        ['columns' => $columns, 'groupByCount' => $groupBy] = $spec->backfillSql([]);
        $this->assertSame(['fingerprint', 'server', 'bucket_start', 'environment', 'call_count'], $columns);
        $this->assertSame(4, $groupBy);
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

    public function test_new_command_and_scheduled_task_specs_registered(): void
    {
        $tables = array_map(static fn ($s) => $s->table, RollupSpecs::all());
        $this->assertContains('nightowl_command_rollups', $tables);
        $this->assertContains('nightowl_scheduled_task_rollups', $tables);
    }

    public function test_command_spec_bands_match_exit_code_and_handle_null(): void
    {
        $spec = RollupSpecs::commands();
        $this->assertSame('nightowl_command_rollups', $spec->table);
        $this->assertSame('nightowl_commands', $spec->source);
        $this->assertSame(['group_hash'], $spec->groupColumnNames());
        $this->assertSame(['successful_count', 'unsuccessful_count'], $spec->counterColumns());
        $this->assertTrue($spec->hasDuration);
        $this->assertTrue($spec->hasHistogram);

        $successful = $spec->counters['successful_count'];
        $unsuccessful = $spec->counters['unsuccessful_count'];

        // SQL bands are `exit_code = 0` / `exit_code != 0`.
        $this->assertSame('exit_code = 0', $successful['sql']);
        $this->assertSame('exit_code != 0', $unsuccessful['sql']);

        // exit_code = 0 → successful only.
        $this->assertTrue(($successful['php'])(['exit_code' => 0]));
        $this->assertFalse(($unsuccessful['php'])(['exit_code' => 0]));
        // exit_code = 1 → unsuccessful only.
        $this->assertFalse(($successful['php'])(['exit_code' => 1]));
        $this->assertTrue(($unsuccessful['php'])(['exit_code' => 1]));
        // NULL / absent exit_code → NEITHER, matching SQL three-valued logic.
        $this->assertFalse(($successful['php'])(['exit_code' => null]));
        $this->assertFalse(($unsuccessful['php'])(['exit_code' => null]));
        $this->assertFalse(($successful['php'])([]));
        $this->assertFalse(($unsuccessful['php'])([]));

        ['columns' => $columns] = $spec->backfillSql(QueryHistogram::caseSql('duration'));
        $this->assertContains('total_duration', $columns);
        $this->assertContains('hist_38', $columns);
        $this->assertContains('command', $columns);
    }

    public function test_scheduled_task_spec_folds_success_into_processed(): void
    {
        $spec = RollupSpecs::scheduledTasks();
        $this->assertSame('nightowl_scheduled_task_rollups', $spec->table);
        $this->assertSame('nightowl_scheduled_tasks', $spec->source);
        $this->assertSame(['group_hash'], $spec->groupColumnNames());
        $this->assertSame(['failed_count', 'processed_count', 'skipped_count'], $spec->counterColumns());
        $this->assertTrue($spec->hasDuration);
        $this->assertTrue($spec->hasHistogram);

        $processed = $spec->counters['processed_count'];
        // The processed band folds in the legacy 'success' alias — both PHP and SQL.
        $this->assertSame("status = 'processed' OR status = 'success'", $processed['sql']);
        $this->assertTrue(($processed['php'])(['status' => 'processed']));
        $this->assertTrue(($processed['php'])(['status' => 'success']));
        $this->assertFalse(($processed['php'])(['status' => 'failed']));
        $this->assertFalse(($processed['php'])(['status' => 'skipped']));

        $this->assertTrue(($spec->counters['failed_count']['php'])(['status' => 'failed']));
        $this->assertTrue(($spec->counters['skipped_count']['php'])(['status' => 'skipped']));

        ['columns' => $columns] = $spec->backfillSql(QueryHistogram::caseSql('duration'));
        $this->assertContains('command', $columns);
        $this->assertContains('expression', $columns);
        $this->assertContains('repeat_seconds', $columns);
    }
}
