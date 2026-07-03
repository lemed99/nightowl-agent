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
