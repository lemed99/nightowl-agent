<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Commands\DropV1HistogramsCommand;
use NightOwl\Support\V1HistogramCleanup;
use PHPUnit\Framework\TestCase;

/**
 * nightowl:drop-v1-histograms explains each verify() blocker to the operator.
 * MISSING_COUNT_FN means migration 000062's coverage function is absent (the
 * agent was upgraded but nightowl:migrate has not run) — a database-global fact
 * verify() reports on every hist-bearing table. Its remedy is nightowl:migrate,
 * NOT backfill or waiting out retention, and it must be stated once regardless
 * of how many tables carry the marker.
 */
final class DropV1HistogramsCommandTest extends TestCase
{
    public function test_missing_count_fn_renders_migrate_advice_exactly_once(): void
    {
        // The marker is global but surfaced per table — mimic verify()'s output.
        $offenders = [
            'nightowl_query_rollups' => V1HistogramCleanup::MISSING_COUNT_FN,
            'nightowl_request_rollups' => V1HistogramCleanup::MISSING_COUNT_FN,
            'nightowl_job_rollups' => V1HistogramCleanup::MISSING_COUNT_FN,
        ];

        $lines = DropV1HistogramsCommand::describeOffenders($offenders);

        $migrateLines = array_values(array_filter(
            $lines,
            static fn (string $l): bool => str_contains($l, 'nightowl:migrate'),
        ));
        $this->assertCount(1, $migrateLines, 'the migrate remedy must appear once, not once per table');

        $joined = implode("\n", $lines);
        // The wrong remedies from the old default arm must not appear for -3.
        $this->assertStringNotContainsString('nightowl:backfill-rollups', $joined);
        $this->assertStringNotContainsString('age past retention', $joined);
        // The global fact is not re-reported per table.
        $this->assertStringNotContainsString('nightowl_query_rollups', $joined);
    }

    public function test_positive_row_count_advice_is_honest_about_age_out_only_rows(): void
    {
        $lines = DropV1HistogramsCommand::describeOffenders(['nightowl_query_rollups' => 7]);

        $this->assertCount(1, $lines);
        $this->assertStringContainsString('nightowl_query_rollups: 7 row(s)', $lines[0]);
        // Backfill only helps rows still inside raw retention; the rest age out.
        $this->assertStringContainsString('nightowl:backfill-rollups', $lines[0]);
        $this->assertStringContainsString('age out', $lines[0]);
    }

    public function test_sketch_and_duration_count_markers_stay_per_table(): void
    {
        $lines = DropV1HistogramsCommand::describeOffenders([
            'nightowl_mail_hourly_rollups' => V1HistogramCleanup::MISSING_SKETCH,
            'nightowl_mail_daily_rollups' => V1HistogramCleanup::MISSING_DURATION_COUNT,
        ]);

        $this->assertCount(2, $lines);
        $this->assertStringContainsString('nightowl_mail_hourly_rollups: no sketch column', $lines[0]);
        $this->assertStringContainsString('nightowl_mail_daily_rollups: no duration_count column', $lines[1]);
    }
}
