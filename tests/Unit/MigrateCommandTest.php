<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Commands\MigrateCommand;
use PHPUnit\Framework\TestCase;

/**
 * `nightowl:migrate` tracks migration history inside the nightowl database so
 * it's idempotent across environments. When a database already has the schema
 * but no history here (an install that predates history-in-the-nightowl-DB, or
 * one created via the host app's `php artisan migrate`), it must adopt the
 * existing migrations as a baseline rather than re-running the CREATE TABLE
 * statements and failing with "already exists".
 */
final class MigrateCommandTest extends TestCase
{
    public function test_fresh_database_records_no_baseline(): void
    {
        // No tables yet → migrate creates everything; nothing to adopt.
        $this->assertSame([], MigrateCommand::migrationsToRecord(['a', 'b', 'c'], [], [], false));
    }

    public function test_pure_primary_tracked_install_adopts_from_primary(): void
    {
        // Fresh v1.0.11/1.0.12 install: tables exist, nightowl history empty,
        // everything recorded in the primary database → adopt it all here.
        $this->assertSame(
            ['a', 'b', 'c'],
            MigrateCommand::migrationsToRecord(['a', 'b', 'c'], [], ['a', 'b', 'c'], true),
        );
    }

    public function test_complete_nightowl_history_records_nothing(): void
    {
        // ≤1.0.10 install, never upgraded: nightowl history already complete →
        // nothing to reconcile.
        $this->assertSame([], MigrateCommand::migrationsToRecord(['a', 'b', 'c'], ['a', 'b', 'c'], [], true));
    }

    public function test_stale_partial_nightowl_history_reconciles_the_gap(): void
    {
        // The mixed case (≤1.0.10 then 1.0.11/1.0.12): nightowl history is stale
        // (only 'a'), the rest is recorded in primary, tables fully present.
        // Reconcile records exactly the gap so migrate doesn't recreate b/c.
        $this->assertSame(
            ['b', 'c'],
            MigrateCommand::migrationsToRecord(['a', 'b', 'c'], ['a'], ['a', 'b', 'c'], true),
        );
    }

    public function test_genuinely_new_migration_is_left_for_migrate(): void
    {
        // 'c' is applied nowhere → it is NOT baselined, so migrate runs it.
        $this->assertSame([], MigrateCommand::migrationsToRecord(['a', 'b', 'c'], ['a', 'b'], ['a', 'b'], true));
    }

    public function test_no_record_anywhere_adopts_whole_schema(): void
    {
        // Tables exist but neither history knows anything → adopt all (caller warns).
        $this->assertSame(['a', 'b'], MigrateCommand::migrationsToRecord(['a', 'b'], [], [], true));
    }

    public function test_shared_single_database_history_records_nothing(): void
    {
        // nightowl connection == primary: the shared `migrations` table holds app
        // migrations too, but they're filtered out and the package ones are
        // already tracked → nothing to reconcile.
        $this->assertSame(
            [],
            MigrateCommand::migrationsToRecord(['a', 'b'], ['app_2019_users', 'a', 'b'], ['app_2019_users', 'a', 'b'], true),
        );
    }

    public function test_adopts_exactly_what_primary_history_recorded(): void
    {
        // The applied set is read from the host's primary history, so a migration
        // that primary never ran ('c') is NOT adopted — migrate will run it.
        $this->assertSame(
            ['a', 'b'],
            MigrateCommand::applicableFromPrimary(['a', 'b', 'c'], ['a', 'b']),
        );

        // Non-NightOwl rows in the primary history are ignored; package order kept.
        $this->assertSame(
            ['a', 'c'],
            MigrateCommand::applicableFromPrimary(['a', 'b', 'c'], ['some_app_migration', 'c', 'a']),
        );

        // Nothing recorded on primary → nothing to adopt from it.
        $this->assertSame([], MigrateCommand::applicableFromPrimary(['a', 'b'], []));
    }

    public function test_pending_migrations_are_those_not_recorded(): void
    {
        $this->assertSame(['b', 'c'], MigrateCommand::pendingMigrations(['a', 'b', 'c'], ['a']));
        $this->assertSame([], MigrateCommand::pendingMigrations(['a', 'b'], ['a', 'b']));
        $this->assertSame(['a', 'b'], MigrateCommand::pendingMigrations(['a', 'b'], []));
    }

    public function test_empty_history_is_not_drift(): void
    {
        // Schema present but untracked here (pre-DB-history install) → adopted
        // as a baseline by nightowl:migrate, not a break. No drift warning.
        $this->assertFalse(MigrateCommand::isBehind(['a', 'b'], []));
    }

    public function test_live_history_missing_newer_migrations_is_drift(): void
    {
        $this->assertTrue(MigrateCommand::isBehind(['a', 'b'], ['a']));
    }

    public function test_fully_applied_history_is_not_drift(): void
    {
        $this->assertFalse(MigrateCommand::isBehind(['a', 'b'], ['a', 'b']));
        // Recorded ahead of the package (downgrade) → nothing pending, not drift.
        $this->assertFalse(MigrateCommand::isBehind(['a', 'b'], ['a', 'b', 'c']));
    }

    public function test_applied_set_unions_both_histories(): void
    {
        // A migration applied per the nightowl history OR the primary history counts.
        $this->assertEqualsCanonicalizing(
            ['a', 'b'],
            MigrateCommand::appliedSet(['a', 'b', 'c'], ['a'], ['b']),
        );

        // Legacy install: nothing in the nightowl DB, everything in primary → current.
        $this->assertEqualsCanonicalizing(
            ['a', 'b', 'c'],
            MigrateCommand::appliedSet(['a', 'b', 'c'], [], ['a', 'b', 'c']),
        );

        // Unrelated app migrations and a shared `migrations` table don't leak in.
        $this->assertEqualsCanonicalizing(
            ['a', 'b'],
            MigrateCommand::appliedSet(['a', 'b'], ['app_2019_users', 'a'], ['app_2020_jobs', 'b']),
        );
    }

    public function test_legacy_install_behind_is_drift_via_primary_history(): void
    {
        // The gap this closes: nothing tracked in the nightowl DB, but primary
        // history shows the install is behind by one (a new migration 'c' was
        // never applied anywhere). Combined applied set = {a,b} → drift.
        $applied = MigrateCommand::appliedSet(['a', 'b', 'c'], [], ['a', 'b']);
        $this->assertTrue(MigrateCommand::isBehind(['a', 'b', 'c'], $applied));
        $this->assertSame(['c'], MigrateCommand::pendingMigrations(['a', 'b', 'c'], $applied));
    }

    public function test_legacy_install_current_is_not_drift(): void
    {
        // No nightowl-DB history, but primary shows all applied → not drift.
        $applied = MigrateCommand::appliedSet(['a', 'b', 'c'], [], ['a', 'b', 'c']);
        $this->assertFalse(MigrateCommand::isBehind(['a', 'b', 'c'], $applied));
    }

    public function test_no_record_anywhere_is_not_flagged(): void
    {
        // Tables present but no history in either place → unknowable, so we do
        // NOT false-alarm at startup (nightowl:migrate adopts it as a baseline).
        $applied = MigrateCommand::appliedSet(['a', 'b'], [], []);
        $this->assertFalse(MigrateCommand::isBehind(['a', 'b'], $applied));
    }

    public function test_backfill_plan_upgrade_path_gets_tiers_only(): void
    {
        // The tier-release upgrade: sound minute tables, tier siblings empty
        // or holed (sum short of the source). Without the tiers-only pass,
        // wide ranges stay on the minute tier — past the statement timeout at
        // 8M req/day scale.
        $plan = MigrateCommand::backfillPlan(
            basesNeedingFull: [],
            basesOk: ['nightowl_request_rollups', 'nightowl_query_rollups'],
            incompleteTiers: ['nightowl_request_hourly_rollups', 'nightowl_request_daily_rollups'],
        );

        $this->assertSame([], $plan['full']);
        $this->assertSame(['nightowl_request_rollups'], $plan['tiers_only'], 'only the type with incomplete tiers is re-aggregated');
    }

    public function test_backfill_plan_empty_base_takes_the_full_chain_only(): void
    {
        // A full pass already rebuilds the tiers — the same type must not also
        // be queued tiers-only.
        $plan = MigrateCommand::backfillPlan(
            basesNeedingFull: ['nightowl_request_rollups'],
            basesOk: [],
            incompleteTiers: ['nightowl_request_hourly_rollups'],
        );

        $this->assertSame(['nightowl_request_rollups'], $plan['full']);
        $this->assertSame([], $plan['tiers_only']);
    }

    public function test_backfill_plan_maintained_tiers_are_left_alone(): void
    {
        // Sound base, no incomplete tiers (live drain maintains them): no work.
        $plan = MigrateCommand::backfillPlan(
            basesNeedingFull: [],
            basesOk: ['nightowl_request_rollups'],
            incompleteTiers: [],
        );

        $this->assertSame([], $plan['full']);
        $this->assertSame([], $plan['tiers_only']);
    }

    // ------------------------------------------------- completeness predicate

    /**
     * The tinybit.farm shape (2026-08-25): an app that has produced no
     * outgoing requests, cache events or notifications has three empty rollup
     * bases over three empty raw sources. Nothing is missing — there is
     * nothing to roll up — yet every deploy ran three backfill sub-commands
     * that found "no source rows" and then told the operator to restart the
     * daemon. Empty over empty is complete.
     */
    public function test_empty_base_over_empty_raw_is_complete(): void
    {
        $this->assertFalse(MigrateCommand::baseIsIncompleteFrom(lo: null, hi: null, rawMin: null, rawMax: null));
    }

    public function test_empty_base_with_raw_waiting_is_incomplete(): void
    {
        $this->assertTrue(MigrateCommand::baseIsIncompleteFrom(
            lo: null, hi: null, rawMin: '2026-08-25 10:00:00', rawMax: '2026-08-25 11:00:00',
        ));
    }

    public function test_head_raw_predating_the_earliest_bucket_is_incomplete(): void
    {
        $this->assertTrue(MigrateCommand::baseIsIncompleteFrom(
            lo: '2026-08-25 10:00:00', hi: '2026-08-25 11:00:00',
            rawMin: '2026-08-25 09:00:00', rawMax: '2026-08-25 11:00:00',
        ));
    }

    public function test_tail_frozen_past_the_tolerance_is_incomplete(): void
    {
        $this->assertTrue(MigrateCommand::baseIsIncompleteFrom(
            lo: '2026-08-25 06:00:00', hi: '2026-08-25 06:00:00',
            rawMin: '2026-08-25 06:00:00', rawMax: '2026-08-25 12:00:00',
        ), 'six hours behind raw is a freeze');
    }

    public function test_drain_lag_inside_the_tolerance_is_complete(): void
    {
        $this->assertFalse(MigrateCommand::baseIsIncompleteFrom(
            lo: '2026-08-25 06:00:00', hi: '2026-08-25 10:20:00',
            rawMin: '2026-08-25 06:00:00', rawMax: '2026-08-25 12:00:00',
        ), '100 minutes behind is a backlog, not a freeze (tolerance is 120)');
    }

    public function test_populated_base_with_raw_pruned_away_is_complete(): void
    {
        $this->assertFalse(MigrateCommand::baseIsIncompleteFrom(
            lo: '2026-08-25 06:00:00', hi: '2026-08-25 12:00:00', rawMin: null, rawMax: null,
        ));
    }

    public function test_tier_short_of_its_source_is_incomplete(): void
    {
        $this->assertTrue(MigrateCommand::tierIsIncompleteFrom(sourceSum: '100', tierSum: '99'));
        $this->assertFalse(MigrateCommand::tierIsIncompleteFrom(sourceSum: '100', tierSum: '100'));
        $this->assertFalse(MigrateCommand::tierIsIncompleteFrom(sourceSum: '100', tierSum: '150'), 'retention asymmetry pushes the tier ABOVE its source');
        $this->assertFalse(MigrateCommand::tierIsIncompleteFrom(sourceSum: null, tierSum: null), 'empty over empty');
        $this->assertTrue(MigrateCommand::tierIsIncompleteFrom(sourceSum: '1', tierSum: null), 'an empty tier over a populated source is the classic hole');
    }

    // ---------------------------------------------------- one-statement probe

    /**
     * The whole reason migrate is fast again on a distant database: every
     * measurement for every base rides ONE statement. The SQL shape is pinned
     * here so a future per-table "just one more query" cannot creep back in
     * unnoticed — that is exactly how it reached ~200 statements per deploy.
     */
    public function test_completeness_sql_is_one_statement_covering_every_base(): void
    {
        $sql = MigrateCommand::completenessSql(
            [
                'nightowl_request_rollups' => [
                    'source' => 'nightowl_requests',
                    'tiers' => ['nightowl_request_hourly_rollups', 'nightowl_request_daily_rollups'],
                ],
                'nightowl_notification_rollups' => [
                    'source' => 'nightowl_notifications',
                    'tiers' => ['nightowl_notification_hourly_rollups'],
                ],
                'nightowl_request_concurrency_rollups' => [
                    'source' => 'nightowl_requests',
                    'tiers' => [],
                ],
            ],
            rawPresent: ['nightowl_requests', 'nightowl_requests_v2'],
        );

        $this->assertStringStartsWith('SELECT ', $sql);
        $this->assertSame(1, substr_count($sql, 'SELECT (SELECT MIN(bucket_start)'), 'a single SELECT list, not a batch of statements');

        // Base 0: both raw families present → both legs in the union.
        $this->assertStringContainsString('SELECT MIN(created_at) AS m FROM nightowl_requests UNION ALL SELECT MIN(created_at) AS m FROM nightowl_requests_v2', $sql);
        $this->assertStringContainsString('AS b0_raw_min', $sql);
        $this->assertStringContainsString('AS b0_raw_max', $sql);
        $this->assertStringContainsString('(SELECT SUM(call_count)::text FROM nightowl_request_rollups) AS b0_sum', $sql);
        $this->assertStringContainsString('(SELECT SUM(call_count)::text FROM nightowl_request_hourly_rollups) AS b0_t0_sum', $sql);
        $this->assertStringContainsString('(SELECT SUM(call_count)::text FROM nightowl_request_daily_rollups) AS b0_t1_sum', $sql);

        // Base 1: neither raw family exists → NULL, and no reference to the
        // absent tables (a 42P01 would take the whole statement down).
        $this->assertStringContainsString('NULL::text AS b1_raw_min', $sql);
        $this->assertStringContainsString('NULL::text AS b1_raw_max', $sql);
        $this->assertStringNotContainsString('FROM nightowl_notifications ', $sql);
        $this->assertStringContainsString('AS b1_t0_sum', $sql);
        $this->assertStringNotContainsString('b1_t1_sum', $sql);

        // Base 2: the concurrency rollup has no call_count column.
        $this->assertStringContainsString('NULL::text AS b2_sum', $sql);
        $this->assertStringNotContainsString('SUM(call_count)::text FROM nightowl_request_concurrency_rollups', $sql);
    }

    public function test_catalog_names_cover_every_table_the_run_asks_about(): void
    {
        $names = MigrateCommand::catalogNames();

        $this->assertSame($names, array_values(array_unique($names)), 'no duplicates');
        foreach ([
            'nightowl_settings',
            'nightowl_request_concurrency_rollups',
            'nightowl_query_rollups', 'nightowl_query_hourly_rollups', 'nightowl_query_daily_rollups',
            'nightowl_notification_rollups',
            'nightowl_requests', 'nightowl_requests_v2',
            'nightowl_logs', 'nightowl_logs_v2',
        ] as $expected) {
            $this->assertContains($expected, $names);
        }
    }
}
