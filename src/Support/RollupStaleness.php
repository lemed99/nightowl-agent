<?php

namespace NightOwl\Support;

/**
 * Which rollup tables have stopped advancing while their peers kept going.
 *
 * A frozen rollup is the quietest failure this agent has. Nothing errors on the
 * read side: the API's coverage gate sees a stale `max(bucket_start)`, decides
 * the rollup does not cover the requested window, and silently falls back to
 * sweeping raw — which on a wide range is exactly the 57014 statement timeout
 * the rollups exist to prevent. On the write side it is one `error_log` line
 * per tick, in a log nobody tails. (Yomoney, 2026-07-31: prune's v1-EOL dropped
 * `nightowl_requests` at 00:00 UTC under a running daemon, every concurrency
 * recompute since aborted with 42P01, and the first anyone knew of it was four
 * production 504s on 14-day charts.)
 *
 * The rule is PEER-RELATIVE, not absolute, and that is the whole design. An
 * absolute "max_bucket must be within N minutes of now" needs to know whether
 * this app emits mail, runs scheduled tasks, or has had a single cache miss
 * today — it does not, and every wrong guess is a false critical on a healthy
 * tenant. Peers answer it for free: all rollups are written by the same drain
 * pass, so on a healthy tenant they advance together, and a table that has
 * fallen hours behind the others has a problem specific to itself. That is the
 * exact shape of every cause worth alerting on — an aborting recompute, a
 * dropped source table, a permission loss on one relation.
 *
 * Comparison is WITHIN tier. Minute, hourly, and daily tables are legitimately
 * up to a minute, an hour, and a day apart from each other by construction, so
 * comparing across tiers would flag every daily table on every healthy tenant.
 *
 * When EVERY rollup freezes together — a stopped or wedged drain — nothing is
 * behind its peers and this reports nothing. That is correct: DRAIN_STOPPED and
 * DRAIN_WEDGED already own that failure and say something far more useful.
 */
final class RollupStaleness
{
    /**
     * How far behind its tier's leader a table may sit before it is stale.
     *
     * Generous multiples of each tier's own bucket width, because the leader is
     * a live number: the drain writes minute buckets continuously but hourly
     * and daily ones only roll over when their bucket does, so two tables in
     * the same tier can be a full bucket apart with nothing wrong. These
     * thresholds are 15 buckets (minute), 3 buckets (hourly), and 3 buckets
     * (daily) — well outside normal skew, well inside the hours-to-days it took
     * to notice the failures they are here to catch.
     */
    private const TIER_TOLERANCE = [
        'daily' => 3 * 86400,
        'hourly' => 3 * 3600,
        'minute' => 900,
    ];

    /**
     * @param  list<array<string, mixed>>  $tables  table-stats rows; each needs a
     *                                              `name` and, for rollup tables, a `max_bucket` epoch (null when empty)
     * @return array<string, int> table => seconds behind its tier's leader, worst first
     */
    public static function detect(array $tables): array
    {
        /** @var array<string, array<string, int>> $byTier */
        $byTier = [];

        foreach ($tables as $row) {
            $name = $row['name'] ?? null;
            if (! is_string($name) || ! str_ends_with($name, '_rollups')) {
                continue;
            }

            // Absent means the bounds probe was skipped or failed for this table
            // this sample; null means the table is EMPTY. Neither is staleness —
            // an empty rollup is ROLLUP_BACKFILL_PENDING's business, and calling
            // it stale here would fire on every tenant the hour a new rollup
            // type ships, before its backfill has run.
            $max = $row['max_bucket'] ?? null;
            if (! is_int($max)) {
                continue;
            }

            $byTier[self::tierOf($name)][$name] = $max;
        }

        $stale = [];

        foreach ($byTier as $tier => $peers) {
            // One table in a tier has no peers, so it has nothing to be behind.
            // Deliberately silent rather than guessing an absolute deadline.
            if (count($peers) < 2) {
                continue;
            }

            $leader = max($peers);
            $tolerance = self::TIER_TOLERANCE[$tier];

            foreach ($peers as $name => $max) {
                $behind = $leader - $max;
                if ($behind > $tolerance) {
                    $stale[$name] = $behind;
                }
            }
        }

        arsort($stale);

        return $stale;
    }

    /**
     * Tier from the table name. `*_daily_rollups` / `*_hourly_rollups` are the
     * coarse siblings migration 000054 creates; everything else ending in
     * `_rollups` is the minute base — including the ones with no tier siblings
     * at all (concurrency, the per-user tables), which is why this is a
     * default rather than a third explicit suffix.
     */
    private static function tierOf(string $name): string
    {
        return match (true) {
            str_ends_with($name, '_daily_rollups') => 'daily',
            str_ends_with($name, '_hourly_rollups') => 'hourly',
            default => 'minute',
        };
    }
}
