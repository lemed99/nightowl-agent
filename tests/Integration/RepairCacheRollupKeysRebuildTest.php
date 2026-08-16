<?php

namespace NightOwl\Tests\Integration;

use Illuminate\Support\Facades\DB;
use NightOwl\Support\CacheRollupSwap;
use PDO;

/**
 * nightowl:repair-cache-rollup-keys with no strategy flag — the DEFAULT path:
 * build the collapsed table beside the live one and swap it in under a lock held
 * for milliseconds.
 *
 * Everything both strategies owe is asserted in CacheRollupRepairTestCase. What
 * is only true here is the price of building a second table while the drain
 * keeps writing to the first, and it is the whole reason this strategy is not
 * obviously safe:
 *
 *  - a row the drain writes AFTER the build's snapshot must reach the new table
 *    exactly once — the snapshot hides it from the scan, the trigger carries it;
 *  - a row the drain writes BEFORE the snapshot but AFTER the trigger went live
 *    must reach it exactly once too — the scan sees it, so the delta must not
 *    add it a second time (this is the double-count the export/import of
 *    pg_export_snapshot() exists to prevent, and the one a trigger alone gets
 *    wrong);
 *  - a DELETE (retention pruning runs on its own schedule) must SUBTRACT, not
 *    tombstone: a templated group is the sum of many raw keys, so dropping the
 *    group would take the surviving keys' counts with it;
 *  - the table that comes out the other side must be the same object to every
 *    reader — same index names, same grants — because the API queries it by
 *    name and the agent's role is not the owner;
 *  - and a rebuild that dies must leave the live table exactly as it found it.
 *
 * The two moments are reached deterministically through CacheRollupSwap::onPhase()
 * rather than by racing a background writer, so these assertions cannot flake.
 */
final class RepairCacheRollupKeysRebuildTest extends CacheRollupRepairTestCase
{
    protected function strategyOptions(): array
    {
        return ['--force' => true];
    }

    private function swap(): CacheRollupSwap
    {
        $config = config('database.connections.nightowl');
        config(['database.connections.nightowl_rebuild' => $config]);
        DB::purge('nightowl_rebuild');

        return new CacheRollupSwap(
            DB::connection('nightowl'),
            DB::connection('nightowl_rebuild'),
            10000,
            static fn (string $line) => null,
        );
    }

    /** A write from a third session — the drain, as far as the rebuild is concerned. */
    private function drainWrites(string $sql): void
    {
        self::$pdo->exec($sql);
    }

    private function upsert(string $key, int $calls, int $duration): string
    {
        return 'INSERT INTO '.static::schema().'.nightowl_cache_rollups
            (key, store, bucket_start, environment, call_count, hits, misses, writes, deletes, fails,
             delete_failures, write_failures, total_duration, min_duration, max_duration)
            VALUES (\''.$key.'\', \'redis\', \''.$this->oldBucket().'\', \'production\', '.$calls.', '.$calls.', 0, 0, 0, 0, 0, 0, '.$duration.', '.$duration.', '.$duration.')
            ON CONFLICT (key, store, bucket_start, environment) DO UPDATE SET
                call_count = '.static::schema().'.nightowl_cache_rollups.call_count + EXCLUDED.call_count,
                hits = '.static::schema().'.nightowl_cache_rollups.hits + EXCLUDED.hits,
                total_duration = '.static::schema().'.nightowl_cache_rollups.total_duration + EXCLUDED.total_duration,
                min_duration = LEAST('.static::schema().'.nightowl_cache_rollups.min_duration, EXCLUDED.min_duration),
                max_duration = GREATEST('.static::schema().'.nightowl_cache_rollups.max_duration, EXCLUDED.max_duration)';
    }

    private function totals(): array
    {
        return self::$pdo->query('SELECT key, call_count, total_duration, min_duration, max_duration
            FROM '.static::schema().'.nightowl_cache_rollups ORDER BY key')->fetchAll(PDO::FETCH_ASSOC);
    }

    public function test_an_insert_after_the_snapshot_is_folded_in_exactly_once(): void
    {
        $this->insert('nightowl_cache_rollups', 'user:1:profile', ['call_count' => 2, 'total_duration' => 200, 'min_duration' => 90, 'max_duration' => 110]);

        $swap = $this->swap();
        $swap->onPhase(function (string $phase) {
            if ($phase === 'captured') {
                $this->drainWrites($this->upsert('user:2:profile', 3, 300));
            }
        });

        $result = $swap->rebuild('nightowl_cache_rollups');

        $this->assertGreaterThan(0, $result['delta_rows'], 'the trigger captured nothing');
        $this->assertSame([[
            'key' => 'user:{int}:profile', 'call_count' => 5, 'total_duration' => 500,
            'min_duration' => 90, 'max_duration' => 300,
        ]], $this->totals());
    }

    public function test_an_insert_before_the_snapshot_is_not_counted_twice(): void
    {
        // Trigger live, snapshot taken, scan not started: the row IS in the
        // snapshot, so the build finds it AND the trigger recorded it. Without
        // the exported snapshot both copies land and the counter doubles.
        $this->insert('nightowl_cache_rollups', 'user:1:profile', ['call_count' => 2, 'total_duration' => 200]);

        $swap = $this->swap();
        $seen = false;
        $swap->onPhase(function (string $phase) use (&$seen) {
            if ($phase === 'captured' && ! $seen) {
                $seen = true;
                $this->drainWrites($this->upsert('user:1:profile', 4, 400));
            }
        });

        $swap->rebuild('nightowl_cache_rollups');

        $this->assertSame([[
            'key' => 'user:{int}:profile', 'call_count' => 6, 'total_duration' => 600,
            'min_duration' => 100, 'max_duration' => 400,
        ]], $this->totals());
    }

    public function test_a_write_after_the_build_is_folded_in(): void
    {
        $this->insert('nightowl_cache_rollups', 'user:1:profile', ['call_count' => 2, 'total_duration' => 200]);

        $swap = $this->swap();
        $swap->onPhase(function (string $phase) {
            if ($phase === 'built') {
                $this->drainWrites($this->upsert('user:9:profile', 7, 700));
            }
        });

        $swap->rebuild('nightowl_cache_rollups');

        $this->assertSame(9, (int) $this->totals()[0]['call_count']);
    }

    public function test_a_delete_during_the_rebuild_subtracts_instead_of_dropping_the_group(): void
    {
        // Retention pruning deletes ONE raw key. The group it belongs to is the
        // sum of several, so a tombstone would take the survivors with it.
        $this->insert('nightowl_cache_rollups', 'user:1:profile', ['call_count' => 2, 'total_duration' => 200]);
        $this->insert('nightowl_cache_rollups', 'user:2:profile', ['call_count' => 5, 'total_duration' => 500]);

        $swap = $this->swap();
        $swap->onPhase(function (string $phase) {
            if ($phase === 'captured') {
                $this->drainWrites('DELETE FROM '.static::schema().".nightowl_cache_rollups WHERE key = 'user:1:profile'");
            }
        });

        $swap->rebuild('nightowl_cache_rollups');

        $rows = $this->totals();
        $this->assertCount(1, $rows);
        $this->assertSame('user:{int}:profile', $rows[0]['key']);
        $this->assertSame(5, (int) $rows[0]['call_count']);
        $this->assertSame(500, (int) $rows[0]['total_duration']);
    }

    public function test_a_group_emptied_by_deletes_leaves_no_zero_row_behind(): void
    {
        $this->insert('nightowl_cache_rollups', 'user:1:profile', ['call_count' => 2, 'total_duration' => 200]);

        $swap = $this->swap();
        $swap->onPhase(function (string $phase) {
            if ($phase === 'captured') {
                $this->drainWrites('DELETE FROM '.static::schema().'.nightowl_cache_rollups');
            }
        });

        $swap->rebuild('nightowl_cache_rollups');

        $this->assertSame([], $this->totals());
    }

    public function test_secondary_indexes_survive_under_their_original_names(): void
    {
        // The API queries these by name; the two tier tables do not even agree on
        // the bucket index's name (..._bucket_idx vs ..._bucket_start_idx), so the
        // rebuild has to carry the name over rather than regenerate it.
        self::$pdo->exec('CREATE INDEX nightowl_cache_rollups_bucket_idx ON '.static::schema().'.nightowl_cache_rollups (bucket_start)');
        $this->insert('nightowl_cache_rollups', 'user:1:profile');

        $this->swap()->rebuild('nightowl_cache_rollups');

        $defs = self::$pdo->query("SELECT indexname FROM pg_indexes
            WHERE schemaname = '".static::schema()."' AND tablename = 'nightowl_cache_rollups' ORDER BY 1")
            ->fetchAll(PDO::FETCH_COLUMN);

        $this->assertSame(['nightowl_cache_rollups_bucket_idx', 'nightowl_cache_rollups_pkey'], $defs);
    }

    public function test_refuses_a_table_carrying_a_column_it_cannot_aggregate(): void
    {
        // Adding a rollup counter without teaching the rebuild about it would
        // otherwise silently zero (or default) that column across the whole table.
        self::$pdo->exec('ALTER TABLE '.static::schema().'.nightowl_cache_rollups ADD COLUMN evictions bigint NOT NULL DEFAULT 0');
        $this->insert('nightowl_cache_rollups', 'user:1:profile');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/evictions.*--in-place/s');

        $this->swap()->rebuild('nightowl_cache_rollups');
    }

    public function test_a_rebuild_that_dies_leaves_the_live_table_untouched(): void
    {
        $this->insert('nightowl_cache_rollups', 'user:1:profile', ['call_count' => 2]);
        $this->insert('nightowl_cache_rollups', 'user:2:profile', ['call_count' => 3]);
        $before = $this->totals();

        $swap = $this->swap();
        $swap->onPhase(function (string $phase) {
            if ($phase === 'built') {
                throw new \RuntimeException('disk full');
            }
        });

        try {
            $swap->rebuild('nightowl_cache_rollups');
            $this->fail('rebuild swallowed the failure');
        } catch (\RuntimeException $e) {
            $this->assertSame('disk full', $e->getMessage());
        }

        $this->assertSame($before, $this->totals());

        $scratch = self::$pdo->query("SELECT tablename FROM pg_tables
            WHERE schemaname = '".static::schema()."' AND tablename LIKE '%\\_\\_nw\\_%'")
            ->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame([], $scratch, 'scratch objects left behind');

        $triggers = self::$pdo->query("SELECT tgname FROM pg_trigger t
            JOIN pg_class c ON c.oid = t.tgrelid
            WHERE c.relname = 'nightowl_cache_rollups' AND NOT t.tgisinternal")
            ->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame([], $triggers, 'capture trigger left attached to the live table');
    }

    public function test_scratch_left_by_an_earlier_crash_is_cleared_not_reused(): void
    {
        // A stale delta table still carrying a dead run's rows would be folded in
        // on top of a fresh scan — every row in it counted twice.
        $this->insert('nightowl_cache_rollups', 'user:1:profile', ['call_count' => 2]);
        self::$pdo->exec('CREATE TABLE '.static::schema().'.nightowl_cache_rollups__nw_delta (op "char" NOT NULL, LIKE '.static::schema().'.nightowl_cache_rollups)');
        self::$pdo->exec('CREATE TABLE '.static::schema().'.nightowl_cache_rollups__nw_new (LIKE '.static::schema().'.nightowl_cache_rollups)');

        $this->swap()->rebuild('nightowl_cache_rollups');

        $this->assertSame(2, (int) $this->totals()[0]['call_count']);
    }

    public function test_since_is_refused_because_a_rebuild_has_no_window(): void
    {
        $this->insert('nightowl_cache_rollups', 'user:1:profile');
        $this->insert('nightowl_cache_rollups', 'user:2:profile');

        $command = new \NightOwl\Commands\RepairCacheRollupKeysCommand;
        $command->setLaravel($this->app);
        $output = new \Symfony\Component\Console\Output\BufferedOutput;
        $exit = $command->run(new \Symfony\Component\Console\Input\ArrayInput(
            ['--since' => $this->oldBucket(4), '--force' => true],
        ), $output);

        $this->assertSame(1, $exit);
        $this->assertStringContainsString('--since only applies to --in-place', $output->fetch());
        $this->assertSame(['user:1:profile', 'user:2:profile'], $this->keys());
    }

    /**
     * The preflight's happy path. It is the only thing standing between an
     * operator and a doomed run, so it has to be right in BOTH directions —
     * a false positive here would push every healthy database onto the slow
     * strategy for nothing.
     */
    public function test_the_snapshot_preflight_passes_on_a_database_that_supports_it(): void
    {
        $swap = $this->probe();

        $this->assertSame('', $swap->snapshotUnavailable());
    }

    /**
     * The failure Eskie hit in production on 2026-08-15: three tables, each
     * dying at the import with
     *
     *   SQLSTATE[25001]: SET TRANSACTION SNAPSHOT must be called before any query
     *
     * Reproduced the only way the message can be produced — by letting the
     * second session run a query before the import — to prove the preflight
     * catches that shape of database instead of discovering it minutes into a
     * rebuild. The version goes in the message because "why here and not
     * anywhere else" is a question about the server.
     */
    public function test_the_snapshot_preflight_catches_a_session_that_refuses_the_import(): void
    {
        $work = $this->workConnection();
        $work->beginTransaction();
        $work->select('SELECT 1');

        $reason = $this->probe($work)->snapshotUnavailable();

        $this->assertStringContainsString('refused a cross-session snapshot', $reason);
        // Postgres refuses at whichever of the two statements it reaches
        // first — the isolation level on a dirty session, the import on a
        // clean one — so pin the half of the message both refusals share.
        $this->assertStringContainsString('must be called before any query', $reason);
        $this->assertMatchesRegularExpression('/Postgres \d+/', $reason);
        $this->assertStringContainsString('are separate, so something ran a query', $reason);
        // Nothing dirtied the session while the handshake was OPEN — the poison
        // above ran before it — so the recorder must say so rather than invent a
        // culprit. The negative is the useful half of that answer: it rules the
        // host app out and leaves the driver.
        $this->assertStringContainsString('Nothing this application ran through Laravel touched that session', $reason);
    }

    /**
     * The other half: when the host app DOES query the second session while the
     * handshake is open, the failure must name that query. Guessing which
     * listener dirtied a session is the expensive part of this diagnosis, and it
     * is the one thing the operator cannot work out from the Postgres error.
     *
     * The listener here reacts to a query on the FIRST connection by querying
     * the SECOND, which is the shape the field failure has, and holds it open so
     * the snapshot it takes is still there when the import arrives.
     */
    public function test_the_snapshot_preflight_names_the_query_that_dirtied_the_session(): void
    {
        $this->app->singleton('events', fn () => new \Illuminate\Events\Dispatcher($this->app));
        $this->app['db']->purge('nightowl');
        $this->app['db']->purge('probe_work');

        $control = $this->app['db']->connection('nightowl');
        $work = $this->workConnection();

        $this->app['events']->listen(\Illuminate\Database\Events\QueryExecuted::class, static function ($query) use ($work) {
            if (str_contains($query->sql, 'pg_export_snapshot')) {
                $work->beginTransaction();
                $work->select('SELECT 1 AS audit_probe');
            }
        });

        $reason = $this->probe($work)->snapshotUnavailable();

        $this->assertStringContainsString('refused a cross-session snapshot', $reason);
        $this->assertStringContainsString('This application ran it:', $reason);
        $this->assertStringContainsString('audit_probe', $reason);
    }

    /** Whichever way it answers, it must hand both sessions back clean. */
    public function test_the_snapshot_preflight_leaves_no_transaction_behind(): void
    {
        $control = $this->app['db']->connection('nightowl');

        $this->assertSame('', (new CacheRollupSwap($control, $this->workConnection(), 1000, static fn (string $l) => null))->snapshotUnavailable());

        $this->assertSame(0, $control->transactionLevel());
        $this->assertSame(1, (int) $control->selectOne('SELECT 1 AS n')->n);
    }

    /**
     * End to end: when the second session cannot be had at all, the command
     * must stop at the preflight — before the trigger, the delta table or the
     * scan — and say which strategy to use instead. Non-interactive, because
     * that is how it runs under a deploy script.
     */
    public function test_a_rebuild_stops_at_the_preflight_when_the_second_session_cannot_be_opened(): void
    {
        $this->insert('nightowl_cache_rollups', 'user:1:profile');
        $this->insert('nightowl_cache_rollups', 'user:2:profile');

        // Resolve (and so cache) the control connection on the good config,
        // then poison the config the work connection is cloned from.
        $this->app['db']->connection('nightowl');
        $this->app['config']->set('database.connections.nightowl.port', 1);

        $command = new \NightOwl\Commands\RepairCacheRollupKeysCommand;
        $command->setLaravel($this->app);
        $output = new \Symfony\Component\Console\Output\BufferedOutput;
        // Non-interactive, the way Symfony's Application configures a TTY-less
        // run — under a deploy script this must be an error, not a prompt.
        $input = new \Symfony\Component\Console\Input\ArrayInput(['--force' => true]);
        $input->setInteractive(false);
        $exit = $command->run($input, $output);
        $text = $output->fetch();

        $this->assertSame(1, $exit, $text);
        $this->assertStringContainsString('Cannot rebuild here', $text);
        $this->assertStringContainsString('--in-place', $text);
        $this->assertSame(['user:1:profile', 'user:2:profile'], $this->keys());
    }

    private function workConnection()
    {
        $this->app['config']->set('database.connections.probe_work', $this->app['config']->get('database.connections.nightowl'));
        $this->app['db']->purge('probe_work');

        return $this->app['db']->connection('probe_work');
    }

    private function probe($work = null): CacheRollupSwap
    {
        return new CacheRollupSwap(
            $this->app['db']->connection('nightowl'),
            $work ?? $this->workConnection(),
            1000,
            static fn (string $line) => null,
        );
    }
}
