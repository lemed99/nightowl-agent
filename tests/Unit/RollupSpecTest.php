<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\QueryHistogram;
use NightOwl\Support\RollupSpecs;
use PHPUnit\Framework\TestCase;

/**
 * Locks the job-duration durationPredicate. A queued-job (dispatch) row carries
 * enqueue time, not execution time, so it must be excluded from the job duration
 * total/min/max/histogram in BOTH the live drain (the php predicate, consumed by
 * RecordWriter::writeRollup) and the backfill (the SQL FILTER). Without it the
 * dispatch's small duration drags the rollup min ~280x low and skews p95.
 */
class RollupSpecTest extends TestCase
{
    public function test_jobs_duration_predicate_excludes_dispatch_rows(): void
    {
        $spec = RollupSpecs::jobs();
        $this->assertNotNull($spec->durationPredicate, 'jobs spec must carry a durationPredicate');

        $php = $spec->durationPredicate['php'];
        $this->assertTrue($php(['attempt_id' => 'a1']), 'attempt rows count toward duration');
        $this->assertFalse($php(['attempt_id' => null]), 'dispatch (queued) rows must NOT count toward duration');
        $this->assertFalse($php([]), 'rows without an attempt_id must NOT count toward duration');
    }

    public function test_jobs_backfill_sql_filters_duration_to_attempts(): void
    {
        $spec = RollupSpecs::jobs();
        ['selects' => $selects] = $spec->backfillSql([]);

        $this->assertStringContainsString(
            'FILTER (WHERE attempt_id IS NOT NULL)',
            implode(' ', $selects),
            'backfill duration aggregates must FILTER to attempt rows (exclude the dispatch row)',
        );
    }

    public function test_backfill_histogram_columns_are_coalesced_to_zero(): void
    {
        // A duration-predicate FILTER matching zero rows (a bucket with only a
        // queued dispatch, no attempt) makes `SUM(...) FILTER (...)` return NULL,
        // which violates the hist_NN NOT NULL constraint. Every hist select must be
        // COALESCE(..., 0) so such buckets backfill 0, matching the live drain.
        $spec = RollupSpecs::jobs();
        $histCase = QueryHistogram::caseSql('duration');
        ['selects' => $selects] = $spec->backfillSql($histCase);

        $histSelects = array_values(array_filter($selects, fn (string $s): bool => str_contains($s, 'CASE WHEN duration')));
        $this->assertNotEmpty($histSelects, 'the jobs backfill must emit histogram selects');
        foreach ($histSelects as $s) {
            $this->assertStringStartsWith('COALESCE(', $s, 'each hist backfill select must be COALESCE-wrapped so a zero-row FILTER yields 0, not NULL');
            $this->assertStringEndsWith(', 0)', $s);
        }
    }

    public function test_duration_count_flag_drives_backfill_and_tier_sql(): void
    {
        // The four hist-sum-denominator types carry duration_count (000061);
        // its raw backfill is COUNT(duration) — non-null count, matching the
        // live drain's fold exactly — and its tier backfill re-SUMs the column.
        foreach ([RollupSpecs::mail(), RollupSpecs::notifications(), RollupSpecs::commands(), RollupSpecs::scheduledTasks()] as $spec) {
            $this->assertTrue($spec->hasDurationCount, "{$spec->table} must carry duration_count");

            ['columns' => $columns, 'selects' => $selects] = $spec->backfillSql([], withDurationCount: true);
            $i = array_search('duration_count', $columns, true);
            $this->assertNotFalse($i, "{$spec->table} backfill must include duration_count");
            $this->assertSame('COUNT(duration)', $selects[$i]);

            ['columns' => $tierCols, 'selects' => $tierSelects] = $spec->tierBackfillSql('hour', [], withDurationCount: true);
            $ti = array_search('duration_count', $tierCols, true);
            $this->assertNotFalse($ti, "{$spec->table} tier backfill must include duration_count");
            $this->assertSame('SUM(duration_count)', $tierSelects[$ti]);
        }

        // Types with a call_count/attempts_count denominator don't pay for it.
        foreach ([RollupSpecs::requests(), RollupSpecs::jobs()] as $spec) {
            $this->assertFalse($spec->hasDurationCount);
            $this->assertNotContains('duration_count', $spec->backfillSql([], withDurationCount: true)['columns']);
        }
    }

    public function test_duration_count_is_omitted_when_the_column_is_absent(): void
    {
        // duration_count only exists once migration 000061 has run. Emitting it
        // off the spec flag alone made `nightowl:backfill-rollups` before
        // `nightowl:migrate` die with 42703 on the first of the four types,
        // leaving the rest un-backfilled — so the column must follow the
        // caller's probe, exactly as hist_NN and sketch already do.
        foreach ([RollupSpecs::mail(), RollupSpecs::notifications(), RollupSpecs::commands(), RollupSpecs::scheduledTasks()] as $spec) {
            ['columns' => $columns, 'selects' => $selects] = $spec->backfillSql([], withDurationCount: false);
            $this->assertNotContains('duration_count', $columns, "{$spec->table} backfill must omit duration_count when the table lacks it");
            $this->assertStringNotContainsString('duration_count', implode(' ', $selects));
            $this->assertSameSize($columns, $selects);

            ['columns' => $tierCols, 'selects' => $tierSelects] = $spec->tierBackfillSql('hour', [], withDurationCount: false);
            $this->assertNotContains('duration_count', $tierCols, "{$spec->table} tier backfill must omit duration_count when the table lacks it");
            $this->assertStringNotContainsString('duration_count', implode(' ', $tierSelects));
            $this->assertSameSize($tierCols, $tierSelects);
        }
    }

    public function test_requests_spec_has_no_duration_predicate(): void
    {
        // Requests are single-row — duration covers every row, so no predicate and
        // no FILTER (guards against the predicate leaking onto other rollup types).
        $spec = RollupSpecs::requests();
        $this->assertNull($spec->durationPredicate);

        ['selects' => $selects] = $spec->backfillSql([]);
        $this->assertStringNotContainsString('FILTER (WHERE', implode(' ', $selects));
    }
}
