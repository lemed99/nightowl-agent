<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Commands\BackfillRollupsCommand;
use NightOwl\Support\RollupSpecs;
use PHPUnit\Framework\TestCase;

/**
 * The tier pass generates `INSERT INTO {daily} (…) SELECT …, SUM(hist_00),
 * nightowl_ddsketch_agg(sketch) … FROM {hourly}` — the optional columns are
 * READ from the finer tier and WRITTEN to the coarser one. Probing only the
 * destination made a desynced pair (a partial nightowl:drop-v1-histograms, e.g.
 * a lock timeout on the large hourly table) generate a SELECT over a column the
 * source lacks: 42703, the whole tier pass aborts, and the daily tier is left
 * un-backfilled — which the API renders as empty rather than falling back.
 */
final class BackfillRollupsCommandTest extends TestCase
{
    /** @return list<string> the full column set of a duration-bearing rollup table */
    private function columns(array $without = []): array
    {
        $cols = [
            'group_hash', 'bucket_start', 'environment', 'call_count',
            'queued_count', 'failed_count',
            'total_duration', 'min_duration', 'max_duration', 'duration_count',
            'sketch', 'sketch_version', 'mailable',
        ];
        foreach (range(0, 38) as $i) {
            $cols[] = sprintf('hist_%02d', $i);
        }

        return array_values(array_diff($cols, $without));
    }

    public function test_matched_tiers_carry_every_optional_column(): void
    {
        $optional = BackfillRollupsCommand::tierColumns(
            RollupSpecs::mail(),
            $this->columns(),
            $this->columns(),
        );

        $this->assertSame(['hist' => true, 'sketch' => true, 'durationCount' => true], $optional);
    }

    public function test_column_dropped_from_the_source_is_not_selected(): void
    {
        // Partial drop reached the hourly table (the SELECT side) but not the
        // daily one. Probing the destination alone said "hist" and generated
        // SUM(hist_00) over a source without it.
        $optional = BackfillRollupsCommand::tierColumns(
            RollupSpecs::mail(),
            $this->columns(without: ['hist_00']),
            $this->columns(),
        );

        $this->assertFalse($optional['hist']);
    }

    public function test_column_dropped_from_the_destination_is_not_inserted(): void
    {
        $optional = BackfillRollupsCommand::tierColumns(
            RollupSpecs::mail(),
            $this->columns(),
            $this->columns(without: ['hist_00']),
        );

        $this->assertFalse($optional['hist']);
    }

    public function test_sketch_needs_both_sides(): void
    {
        // 000057 skips the sketch columns when the database denies CREATE
        // FUNCTION; nightowl_ddsketch_agg(sketch) must not be emitted then.
        $this->assertFalse(BackfillRollupsCommand::tierColumns(
            RollupSpecs::mail(),
            $this->columns(without: ['sketch', 'sketch_version']),
            $this->columns(),
        )['sketch']);

        $this->assertFalse(BackfillRollupsCommand::tierColumns(
            RollupSpecs::mail(),
            $this->columns(),
            $this->columns(without: ['sketch', 'sketch_version']),
        )['sketch']);
    }

    public function test_duration_count_needs_both_sides(): void
    {
        // Un-migrated tenant (000061 absent): SUM(duration_count) over the
        // finer tier is 42703, and the column can't be written either.
        $this->assertFalse(BackfillRollupsCommand::tierColumns(
            RollupSpecs::mail(),
            $this->columns(without: ['duration_count']),
            $this->columns(without: ['duration_count']),
        )['durationCount']);

        $this->assertTrue(BackfillRollupsCommand::tierColumns(
            RollupSpecs::mail(),
            $this->columns(),
            $this->columns(),
        )['durationCount']);
    }

    public function test_sketch_version_alone_does_not_carry_the_sketch(): void
    {
        // tierBackfillSql emits nightowl_ddsketch_agg(sketch) AND
        // MAX(sketch_version); probing one and emitting both leaves the other
        // able to raise 42703.
        $this->assertFalse(BackfillRollupsCommand::tierColumns(
            RollupSpecs::mail(),
            $this->columns(without: ['sketch_version']),
            $this->columns(),
        )['sketch']);
    }

    public function test_desync_is_reported_in_both_directions(): void
    {
        $spec = RollupSpecs::mail();

        // Source ahead: the copy drops hist, so the destination's existing bins
        // get rewritten to their default — the operator has to hear about it.
        $this->assertSame(['hist_00'], BackfillRollupsCommand::tierDesync(
            $spec,
            $this->columns(without: ['hist_00']),
            $this->columns(),
        ));

        $this->assertSame(['hist_00'], BackfillRollupsCommand::tierDesync(
            $spec,
            $this->columns(),
            $this->columns(without: ['hist_00']),
        ));
    }

    public function test_matched_tiers_are_not_a_desync(): void
    {
        // Both sides carrying a column and both sides lacking one are settled
        // states — a pre-000061 tenant must not be nagged on every run.
        $this->assertSame([], BackfillRollupsCommand::tierDesync(
            RollupSpecs::mail(),
            $this->columns(),
            $this->columns(),
        ));

        $this->assertSame([], BackfillRollupsCommand::tierDesync(
            RollupSpecs::mail(),
            $this->columns(without: ['duration_count']),
            $this->columns(without: ['duration_count']),
        ));

        // A count-only type declares none of these columns, so a stray one on
        // the table is not its desync to report.
        $this->assertSame([], BackfillRollupsCommand::tierDesync(
            RollupSpecs::requestUsers(),
            $this->columns(without: ['hist_00']),
            $this->columns(),
        ));
    }

    public function test_spec_flags_still_gate_columns_the_type_never_writes(): void
    {
        // A count-only type has no duration columns at all — a stray hist_00 on
        // the table must not pull it into the copy.
        $optional = BackfillRollupsCommand::tierColumns(
            RollupSpecs::requestUsers(),
            $this->columns(),
            $this->columns(),
        );

        $this->assertSame(['hist' => false, 'sketch' => false, 'durationCount' => false], $optional);
    }
}
