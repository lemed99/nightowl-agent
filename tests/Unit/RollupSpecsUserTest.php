<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\RollupSpecs;
use PHPUnit\Framework\TestCase;

/**
 * Locks the three per-user rollup specs (requestUsers/jobUsers/exceptionUsers)
 * that power the users list. The load-bearing invariant is PARITY: each per-user
 * spec must reproduce, per user, exactly what its route-keyed sibling counts —
 * so the users list served from the rollup matches a raw re-aggregation. These
 * are pure PHP-predicate + backfill-SQL assertions (no DB).
 */
class RollupSpecsUserTest extends TestCase
{
    public function test_user_specs_registered_in_all(): void
    {
        $tables = array_map(static fn ($s) => $s->table, RollupSpecs::all());

        // Registration is what wires backfill + prune coverage for the new tables.
        $this->assertContains('nightowl_user_rollups', $tables);
        $this->assertContains('nightowl_user_job_rollups', $tables);
        $this->assertContains('nightowl_user_exception_rollups', $tables);
    }

    public function test_request_users_groups_by_user_from_wire_field(): void
    {
        $spec = RollupSpecs::requestUsers();

        $this->assertSame('nightowl_user_rollups', $spec->table);
        $this->assertSame('nightowl_requests', $spec->source);
        $this->assertSame(['user_id'], $spec->groupColumnNames());

        // The wire record carries the user under `user` (RecordWriter maps it to
        // the user_id column) — the group extractor must read `user`, not user_id.
        $php = $spec->groupColumns['user_id']['php'];
        $this->assertSame('u-42', $php(['user' => 'u-42']));
        $this->assertSame('', $php([]), 'anonymous rows collapse to the empty-string sentinel');

        $this->assertSame("COALESCE(user_id, '')", $spec->groupColumns['user_id']['sql']);
    }

    /**
     * The whole point of the per-user request rollup is that its status bands
     * reproduce the route rollup's bands. Compare predicate outputs head-to-head
     * across the status-code boundaries so the two can never silently drift.
     */
    public function test_request_users_status_bands_match_requests_spec(): void
    {
        $userBands = RollupSpecs::requestUsers()->counters;
        $routeBands = RollupSpecs::requests()->counters;

        $this->assertSame(['success_count', 'client_error_count', 'server_error_count'], array_keys($userBands));

        foreach ([200, 204, 301, 399, 400, 404, 499, 500, 503] as $status) {
            foreach (['success_count', 'client_error_count', 'server_error_count'] as $band) {
                $this->assertSame(
                    ($routeBands[$band]['php'])(['status_code' => $status]),
                    ($userBands[$band]['php'])(['status_code' => $status]),
                    "band {$band} diverges from requests() at status {$status}",
                );
            }
            // Exactly one band matches each status (no double-count, no gap).
            $hits = array_filter($userBands, static fn ($def) => ($def['php'])(['status_code' => $status]));
            $this->assertCount(1, $hits, "status {$status} must fall in exactly one band");
        }
    }

    public function test_request_users_is_count_only_no_duration_or_histogram(): void
    {
        $spec = RollupSpecs::requestUsers();
        $this->assertFalse($spec->hasDuration);
        $this->assertFalse($spec->hasHistogram);
        $this->assertNull($spec->durationPredicate);
        $this->assertSame([], $spec->representativeColumns());

        // Backfill projects only the group key + bucket + env + call_count + bands.
        ['columns' => $columns, 'groupByCount' => $groupBy] = $spec->backfillSql([]);
        $this->assertSame(
            ['user_id', 'bucket_start', 'environment', 'call_count', 'success_count', 'client_error_count', 'server_error_count'],
            $columns,
        );
        $this->assertSame(3, $groupBy, 'group by user_id + bucket_start + environment');
    }

    public function test_job_users_counts_attempts_matching_jobs_spec(): void
    {
        $spec = RollupSpecs::jobUsers();
        $this->assertSame('nightowl_user_job_rollups', $spec->table);
        $this->assertSame('nightowl_jobs', $spec->source);
        $this->assertSame(['attempts_count'], $spec->counterColumns());

        $userAttempts = $spec->counters['attempts_count']['php'];
        $jobsAttempts = RollupSpecs::jobs()->counters['attempts_count']['php'];

        // attempts = attempt_id IS NOT NULL — dispatch (queued) rows are excluded.
        // NULL-check (not empty()) so '' / '0' attempt ids still count as attempts,
        // byte-identical to the jobs() spec.
        foreach ([['attempt_id' => 'a1'], ['attempt_id' => null], [], ['attempt_id' => '0']] as $record) {
            $this->assertSame(
                $jobsAttempts($record),
                $userAttempts($record),
                'attempts predicate diverges from jobs() for '.json_encode($record),
            );
        }
        $this->assertTrue($userAttempts(['attempt_id' => 'a1']));
        $this->assertFalse($userAttempts(['attempt_id' => null]));

        $this->assertSame('attempt_id IS NOT NULL', $spec->counters['attempts_count']['sql']);
        $this->assertFalse($spec->hasDuration);
        $this->assertFalse($spec->hasHistogram);
    }

    public function test_exception_users_is_count_only(): void
    {
        $spec = RollupSpecs::exceptionUsers();
        $this->assertSame('nightowl_user_exception_rollups', $spec->table);
        $this->assertSame('nightowl_exceptions', $spec->source);
        $this->assertSame(['user_id'], $spec->groupColumnNames());

        // No explicit counters: the implicit call_count IS the exception count.
        $this->assertSame([], $spec->counterColumns());
        $this->assertFalse($spec->hasDuration);
        $this->assertFalse($spec->hasHistogram);

        ['columns' => $columns, 'selects' => $selects, 'groupByCount' => $groupBy] = $spec->backfillSql([]);
        $this->assertSame(['user_id', 'bucket_start', 'environment', 'call_count'], $columns);
        $this->assertSame(3, $groupBy);
        $this->assertContains('COUNT(*)', $selects);
        $this->assertStringNotContainsString('FILTER (WHERE', implode(' ', $selects));
    }
}
