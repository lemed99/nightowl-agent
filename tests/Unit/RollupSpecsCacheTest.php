<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\RollupSpecs;
use PHPUnit\Framework\TestCase;

/**
 * Locks the cache rollup counters to the event types the Nightwatch SDK sends
 * (CacheEventSensor: hit, miss, write, write-failure, delete, delete-failure)
 * plus the legacy spellings older rows carry. Each counter's PHP form (drain)
 * and SQL form (backfill) must accept exactly the same spellings. Pure spec
 * assertions (no DB); RecordWriterTest runs the SQL forms against Postgres.
 */
class RollupSpecsCacheTest extends TestCase
{
    /** Spelling => the counters it lands in. */
    private const EXPECTED = [
        'hit' => ['hits'],
        'miss' => ['misses'],
        'write' => ['writes'],
        'set' => ['writes'],
        'put' => ['writes'],
        'delete' => ['deletes'],
        'forget' => ['deletes'],
        'write-failure' => ['fails', 'write_failures'],
        'write_fail' => ['fails', 'write_failures'],
        'set_fail' => ['fails', 'write_failures'],
        'put_fail' => ['fails', 'write_failures'],
        'fail' => ['fails', 'write_failures'],
        'delete-failure' => ['delete_failures', 'fails'],
        'delete_fail' => ['delete_failures', 'fails'],
        'forget_fail' => ['delete_failures', 'fails'],
        'unknown' => [],
    ];

    public function test_each_spelling_lands_in_the_expected_counters(): void
    {
        $counters = RollupSpecs::cacheEvents()->counters;

        foreach (self::EXPECTED as $type => $expected) {
            $hit = array_keys(array_filter($counters, static fn (array $c): bool => ($c['php'])(['type' => $type])));
            sort($hit);
            $this->assertSame($expected, $hit, "event type '{$type}'");
        }
    }

    public function test_sql_form_lists_the_same_spellings_as_the_php_form(): void
    {
        foreach (RollupSpecs::cacheEvents()->counters as $column => $counter) {
            $this->assertMatchesRegularExpression("/^event_type (= '[^']+'|IN \\('[^']+'(, '[^']+')*\\))$/", $counter['sql'], $column);
            preg_match_all("/'([^']+)'/", $counter['sql'], $m);

            $php = array_keys(array_filter(self::EXPECTED, static fn (array $cols, string $type): bool => ($counter['php'])(['type' => $type]), ARRAY_FILTER_USE_BOTH));
            $sql = $m[1];
            sort($php);
            sort($sql);

            $this->assertSame($php, $sql, "counter {$column}");
        }
    }

    public function test_fails_is_write_failures_plus_delete_failures(): void
    {
        $counters = RollupSpecs::cacheEvents()->counters;

        foreach (array_keys(self::EXPECTED) as $type) {
            $r = ['type' => $type];
            $this->assertSame(
                (int) ($counters['write_failures']['php'])($r) + (int) ($counters['delete_failures']['php'])($r),
                (int) ($counters['fails']['php'])($r),
                "event type '{$type}'",
            );
        }
    }
}
