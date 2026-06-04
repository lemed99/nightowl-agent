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
}
