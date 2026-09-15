<?php

namespace NightOwl\Support;

/**
 * Which rollup tables have stopped being written while their raw telemetry
 * kept arriving.
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
 * The rule grades each rollup against ITS OWN RAW SOURCE. A rollup is written
 * in the same drain transaction as the raw rows it summarises, and both sides
 * carry the same event clock — RecordWriter stamps raw `created_at` with
 * eventCreatedAt() and derives `bucket_start` with eventBucket(), both from
 * eventEpoch(). So on a healthy tenant a rollup's newest bucket trails its
 * source's newest row by less than one bucket, and a table further behind than
 * that has missed rows that are sitting in raw. That is the shape of every
 * cause worth alerting on — an aborting recompute, a dropped source table, a
 * permission loss on one relation, a column the upsert needs gone missing.
 *
 * It used to be PEER-relative: a table far behind the newest rollup in its tier
 * was stale. That assumed every telemetry type arrives continuously, and a
 * quiet app breaks it. An app doing under one request a second can run no
 * command for hours and send no mail for an hour, so its command and mail
 * rollups sat hours "behind" the request rollup with nothing wrong, and the
 * health page raised a critical telling the customer to hunt for a maintenance
 * error that did not exist (a low-traffic app on 2026-09-15: seven tables named,
 * zero failed batches; Yomoney's three exception rollups before that). Only raw
 * can tell "nothing to write" from "failing to write it".
 *
 * When the drain stops, raw stops with the rollups and nothing trails its
 * source, so this reports nothing. That is correct: DRAIN_STOPPED and
 * DRAIN_WEDGED already own that failure and say something far more useful.
 */
final class RollupStaleness
{
    /**
     * How far a rollup's newest bucket may trail its source's newest row before
     * it is stale.
     *
     * A healthy table trails by under one bucket of its own width, because
     * `bucket_start` is the START of the bucket holding the newest event — up to
     * a minute, an hour, or a day. These are 15 buckets (minute), 3 buckets
     * (hourly), and 3 buckets (daily): well outside that, and outside the tick
     * the bespoke concurrency recompute runs on, well inside the hours-to-days
     * it took to notice the failures they are here to catch.
     */
    private const TIER_TOLERANCE = [
        'daily' => 3 * 86400,
        'hourly' => 3 * 3600,
        'minute' => 900,
    ];

    /** @var array<string, string>|null */
    private static ?array $sources = null;

    /**
     * @param  list<array<string, mixed>>  $tables  table-stats rows; each needs a `name`, and a
     *                                              `max_bucket` epoch (null when empty) — the newest `bucket_start` on
     *                                              a rollup table, the newest `created_at` on a raw source table
     * @return array<string, int> table => seconds its raw source runs ahead of it, worst first
     */
    public static function detect(array $tables): array
    {
        $rollups = [];
        $ceilings = [];

        foreach ($tables as $row) {
            $name = $row['name'] ?? null;
            $max = $row['max_bucket'] ?? null;

            // Absent means the probe was skipped or failed for this table this
            // sample; null means the table is EMPTY. Neither is staleness — an
            // empty rollup is ROLLUP_BACKFILL_PENDING's business, and an empty
            // source has nothing for its rollup to be missing.
            if (! is_string($name) || ! is_int($max)) {
                continue;
            }

            if (str_ends_with($name, '_rollups')) {
                $rollups[$name] = $max;
            } else {
                $ceilings[$name] = $max;
            }
        }

        $sources = self::sources();
        $stale = [];

        foreach ($rollups as $name => $max) {
            // A rollup no spec names has no known source. Silent rather than
            // guessing which raw table it summarises.
            $source = $sources[self::baseOf($name)] ?? null;
            if ($source === null) {
                continue;
            }

            // Either family can hold the newest row: v1 before the storage-v2
            // fence or with the kill switch off, the v2 twin otherwise.
            $legs = array_filter(
                [$ceilings[$source] ?? null, $ceilings[StorageV2::v2Name($source)] ?? null],
                'is_int',
            );
            if ($legs === []) {
                continue;
            }

            $behind = max($legs) - $max;
            if ($behind > self::TIER_TOLERANCE[self::tierOf($name)]) {
                $stale[$name] = $behind;
            }
        }

        arsort($stale);

        return $stale;
    }

    /**
     * Every raw table, both storage families, whose ceiling detect() grades
     * against — the list TableStatsCollector probes.
     *
     * @return list<string>
     */
    public static function sourceTables(): array
    {
        $tables = [];
        foreach (array_unique(self::sources()) as $v1Table) {
            $tables[] = $v1Table;
            $tables[] = StorageV2::v2Name($v1Table);
        }

        return $tables;
    }

    /**
     * Raw source (v1 name) per base rollup table. Built from RollupSpecs so a
     * new spec is covered the moment it exists; the concurrency rollup sits
     * outside RollupSpecs::all() and summarises requests, as MigrateCommand's
     * completeness pass also records.
     *
     * @return array<string, string>
     */
    private static function sources(): array
    {
        if (self::$sources === null) {
            self::$sources = [ConcurrencyRollup::TABLE => 'nightowl_requests'];
            foreach (RollupSpecs::all() as $spec) {
                self::$sources[$spec->table] = $spec->source;
            }
        }

        return self::$sources;
    }

    /** nightowl_request_hourly_rollups → nightowl_request_rollups */
    private static function baseOf(string $name): string
    {
        return str_replace(['_hourly_rollups', '_daily_rollups'], '_rollups', $name);
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
