<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Commands\ClearCommand;
use NightOwl\Support\RollupSpecs;
use PHPUnit\Framework\TestCase;

/**
 * `nightowl:clear` must truncate every telemetry AND rollup table. The old list
 * hardcoded 10 tables and silently missed nightowl_logs plus all 12 rollups, so a
 * "clear" left wide-range dashboard views populated from stale rollups. The rollup
 * set is now derived from RollupSpecs so it can never drift from what the drain
 * actually writes.
 */
final class ClearCommandTest extends TestCase
{
    public function test_covers_logs_which_the_old_list_missed(): void
    {
        $this->assertContains('nightowl_logs', ClearCommand::tables());
    }

    public function test_covers_every_rollup_table_from_the_registry(): void
    {
        $tables = ClearCommand::tables();

        foreach (RollupSpecs::all() as $spec) {
            $this->assertContains(
                $spec->table,
                $tables,
                "nightowl:clear must truncate the rollup table {$spec->table}",
            );
        }
    }

    public function test_table_list_has_no_duplicates(): void
    {
        $tables = ClearCommand::tables();

        $this->assertSame(array_values(array_unique($tables)), $tables);
    }

    public function test_preserves_triage_config_and_identity_tables(): void
    {
        $tables = ClearCommand::tables();

        foreach ([
            'nightowl_issues',
            'nightowl_issue_activity',
            'nightowl_issue_comments',
            'nightowl_settings',
            'nightowl_alert_channels',
            'nightowl_users',
        ] as $preserved) {
            $this->assertNotContains($preserved, $tables, "clear must not wipe {$preserved}");
        }
    }
}
