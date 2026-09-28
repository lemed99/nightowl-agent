<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\SqliteBuffer;
use PHPUnit\Framework\TestCase;

/**
 * The claim path several drain workers use, as it behaves when a write fails
 * and when an agent dies with claims outstanding.
 *
 * NIGHTOWL_DRAIN_WORKERS used to be dropped before it reached the agent, so
 * these paths had never run on a customer host.
 */
final class SqliteBufferClaimsTest extends TestCase
{
    private string $dbPath;

    private SqliteBuffer $buffer;

    protected function setUp(): void
    {
        $this->dbPath = sys_get_temp_dir().'/nightowl_claims_'.uniqid().'.sqlite';
        $this->buffer = new SqliteBuffer($this->dbPath);
    }

    protected function tearDown(): void
    {
        unset($this->buffer);

        foreach ([$this->dbPath, $this->dbPath.'-wal', $this->dbPath.'-shm'] as $file) {
            @unlink($file);
        }
    }

    public function test_a_worker_retries_its_failed_batch_instead_of_claiming_another(): void
    {
        $this->append(10);

        $first = array_column($this->buffer->claimBatch(0, 3), 'id');

        // The write failed: nothing was marked, so the rows are still claimed.
        // The retry must be the same batch, not that batch plus three more.
        $retry = array_column($this->buffer->claimBatch(0, 3), 'id');

        $this->assertSame($first, $retry);
        $this->assertSame(7, $this->buffer->pendingCount(), 'the failed retry must not claim any more rows');
    }

    public function test_claims_do_not_grow_through_repeated_failures(): void
    {
        $this->append(50);

        // Twenty failed attempts, as through a Postgres outage with backoff.
        for ($attempt = 0; $attempt < 20; $attempt++) {
            $batch = $this->buffer->claimBatch(0, 5);
            $this->assertCount(5, $batch, 'every retry must be one batch');
        }

        $this->assertSame(45, $this->buffer->pendingCount());
        $this->assertSame(50, $this->buffer->backlogCount());
    }

    public function test_a_worker_claims_new_rows_once_its_batch_is_marked(): void
    {
        $this->append(6);

        $first = $this->buffer->claimBatch(0, 3);
        $this->buffer->markSynced(array_column($first, 'id'));

        $next = $this->buffer->claimBatch(0, 3);

        $this->assertCount(3, $next);
        $this->assertEmpty(array_intersect(array_column($first, 'id'), array_column($next, 'id')));
    }

    public function test_the_backlog_counts_claimed_rows(): void
    {
        $this->append(10);

        $this->buffer->claimBatch(0, 4);
        $this->buffer->claimBatch(1, 4);

        // pendingCount() is what a worker can still claim; the backlog is
        // what has not reached Postgres, and back-pressure must see all of it.
        $this->assertSame(2, $this->buffer->pendingCount());
        $this->assertSame(10, $this->buffer->backlogCount());

        $this->buffer->markSynced(array_column($this->buffer->claimBatch(0, 4), 'id'));
        $this->assertSame(6, $this->buffer->backlogCount());
    }

    public function test_the_backlog_leaves_out_synced_and_quarantined_rows(): void
    {
        $this->append(5);

        $rows = $this->buffer->fetchPending(5);
        $this->buffer->markSynced([$rows[0]['id']]);
        $this->buffer->quarantine([$rows[1]['id']]);

        $this->assertSame(3, $this->buffer->backlogCount());
    }

    public function test_release_all_claimed_frees_every_workers_claims(): void
    {
        $this->append(12);

        $this->buffer->claimBatch(0, 3);
        $this->buffer->claimBatch(1, 3);
        $this->buffer->claimBatch(5, 3);
        $synced = $this->buffer->fetchPending(1);
        $this->buffer->markSynced(array_column($synced, 'id'));

        $this->assertSame(9, $this->buffer->releaseAllClaimed());

        // A single worker reads with fetchPending(), which skips claimed rows:
        // before the release they were invisible to it.
        $this->assertSame(11, $this->buffer->pendingCount());
        $this->assertCount(11, $this->buffer->fetchPending(100));
    }

    public function test_release_all_claimed_leaves_quarantined_rows_alone(): void
    {
        $this->append(4);

        $claimed = $this->buffer->claimBatch(0, 4);
        $this->buffer->quarantine([$claimed[0]['id']]);

        $this->assertSame(3, $this->buffer->releaseAllClaimed());
        $this->assertSame(1, $this->buffer->quarantinedCount());
        $this->assertSame(3, $this->buffer->pendingCount());
    }

    private function append(int $rows): void
    {
        for ($i = 0; $i < $rows; $i++) {
            $this->buffer->appendRaw(json_encode([['t' => 'log', 'i' => $i]]));
        }
    }
}
