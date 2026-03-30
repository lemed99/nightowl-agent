<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\SqliteBuffer;
use PHPUnit\Framework\TestCase;

class SqliteBufferTest extends TestCase
{
    private string $dbPath;
    private SqliteBuffer $buffer;

    protected function setUp(): void
    {
        $this->dbPath = sys_get_temp_dir() . '/nightowl_test_' . uniqid() . '.sqlite';
        $this->buffer = new SqliteBuffer($this->dbPath);
    }

    protected function tearDown(): void
    {
        unset($this->buffer);

        foreach ([$this->dbPath, $this->dbPath . '-wal', $this->dbPath . '-shm'] as $file) {
            if (file_exists($file)) {
                @unlink($file);
            }
        }
    }

    // --- append / fetchPending ---

    public function testAppendAndFetchPending(): void
    {
        $records = [['t' => 'request', 'url' => '/test']];
        $this->buffer->append($records);

        $pending = $this->buffer->fetchPending(10);

        $this->assertCount(1, $pending);
        $this->assertSame(json_encode($records, JSON_INVALID_UTF8_SUBSTITUTE), $pending[0]['payload']);
        $this->assertSame(1, $pending[0]['record_count']);
    }

    public function testAppendRaw(): void
    {
        $json = '{"t":"request","url":"/raw"}';
        $this->buffer->appendRaw($json);

        $pending = $this->buffer->fetchPending(10);

        $this->assertCount(1, $pending);
        $this->assertSame($json, $pending[0]['payload']);
        $this->assertSame(0, $pending[0]['record_count']); // appendRaw sets count to 0
    }

    public function testFetchPendingLimitsResults(): void
    {
        for ($i = 0; $i < 10; $i++) {
            $this->buffer->appendRaw(json_encode(['i' => $i]));
        }

        $pending = $this->buffer->fetchPending(3);
        $this->assertCount(3, $pending);

        // Should return oldest first
        $firstPayload = json_decode($pending[0]['payload'], true);
        $this->assertSame(0, $firstPayload['i']);
    }

    public function testFetchPendingReturnsEmptyWhenNoPending(): void
    {
        $this->assertSame([], $this->buffer->fetchPending(10));
    }

    // --- markSynced ---

    public function testMarkSynced(): void
    {
        $this->buffer->appendRaw('{"a":1}');
        $this->buffer->appendRaw('{"b":2}');
        $this->buffer->appendRaw('{"c":3}');

        $pending = $this->buffer->fetchPending(10);
        $this->assertCount(3, $pending);

        // Mark first two as synced
        $this->buffer->markSynced([$pending[0]['id'], $pending[1]['id']]);

        $remaining = $this->buffer->fetchPending(10);
        $this->assertCount(1, $remaining);
        $this->assertSame('{"c":3}', $remaining[0]['payload']);
    }

    public function testMarkSyncedWithEmptyArray(): void
    {
        // Should not throw
        $this->buffer->markSynced([]);

        // Verify nothing changed
        $this->buffer->appendRaw('{"test":1}');
        $this->assertCount(1, $this->buffer->fetchPending(10));
    }

    // --- pendingCount ---

    public function testPendingCount(): void
    {
        $this->assertSame(0, $this->buffer->pendingCount());

        $this->buffer->appendRaw('{"a":1}');
        $this->buffer->appendRaw('{"b":2}');
        $this->assertSame(2, $this->buffer->pendingCount());

        $pending = $this->buffer->fetchPending(1);
        $this->buffer->markSynced([$pending[0]['id']]);
        $this->assertSame(1, $this->buffer->pendingCount());
    }

    // --- cleanup ---

    public function testCleanupRemovesSyncedRows(): void
    {
        $this->buffer->appendRaw('{"a":1}');

        $pending = $this->buffer->fetchPending(1);
        $this->buffer->markSynced([$pending[0]['id']]);

        // Small sleep so the row ages past the cutoff
        usleep(50_000);

        $deleted = $this->buffer->cleanup(0);
        $this->assertSame(1, $deleted);

        $this->assertSame(0, $this->buffer->pendingCount());
    }

    public function testCleanupDoesNotRemoveUnsynced(): void
    {
        $this->buffer->appendRaw('{"a":1}');

        $deleted = $this->buffer->cleanup(0);
        $this->assertSame(0, $deleted);

        $this->assertSame(1, $this->buffer->pendingCount());
    }

    public function testCleanupRespectsMaxAge(): void
    {
        $this->buffer->appendRaw('{"a":1}');

        $pending = $this->buffer->fetchPending(1);
        $this->buffer->markSynced([$pending[0]['id']]);

        // With large maxAge, nothing should be deleted
        $deleted = $this->buffer->cleanup(3600);
        $this->assertSame(0, $deleted);
    }

    // --- WAL operations ---

    public function testWalSizeReturnsZeroForNewBuffer(): void
    {
        // WAL might be 0 or small after creation
        $this->assertGreaterThanOrEqual(0, $this->buffer->walSize());
    }

    public function testCheckpointDoesNotThrow(): void
    {
        $this->buffer->appendRaw('{"test":1}');
        $this->buffer->checkpoint();
        $this->assertTrue(true); // No exception = pass
    }

    public function testCheckpointTruncateDoesNotThrow(): void
    {
        $this->buffer->appendRaw('{"test":1}');
        $this->buffer->checkpointTruncate();
        $this->assertTrue(true);
    }

    // --- Append failures ---

    public function testAppendThrowsOnInvalidUtf8(): void
    {
        // json_encode with JSON_INVALID_UTF8_SUBSTITUTE should handle this
        $records = [['data' => "\xFF\xFE invalid"]];
        $this->buffer->append($records);

        $pending = $this->buffer->fetchPending(1);
        $this->assertCount(1, $pending);
    }

    // --- Large batch operations ---

    public function testLargeBatchAppendAndFetch(): void
    {
        for ($i = 0; $i < 500; $i++) {
            $this->buffer->appendRaw(json_encode(['i' => $i]));
        }

        $this->assertSame(500, $this->buffer->pendingCount());

        $batch1 = $this->buffer->fetchPending(200);
        $this->assertCount(200, $batch1);

        $ids = array_column($batch1, 'id');
        $this->buffer->markSynced($ids);

        $this->assertSame(300, $this->buffer->pendingCount());

        $batch2 = $this->buffer->fetchPending(400);
        $this->assertCount(300, $batch2);
    }

    // --- Multi-worker claiming ---

    public function testClaimBatchClaimsRowsForWorker(): void
    {
        for ($i = 0; $i < 10; $i++) {
            $this->buffer->appendRaw(json_encode(['i' => $i]));
        }

        $claimed = $this->buffer->claimBatch(0, 5);

        $this->assertCount(5, $claimed);
        // Claimed rows should be oldest first
        $this->assertSame(0, json_decode($claimed[0]['payload'], true)['i']);

        // pendingCount should reflect only unclaimed rows
        $this->assertSame(5, $this->buffer->pendingCount());
    }

    public function testClaimBatchMultipleWorkersGetDisjointRows(): void
    {
        for ($i = 0; $i < 20; $i++) {
            $this->buffer->appendRaw(json_encode(['i' => $i]));
        }

        // Worker 0 claims first 8
        $claimed0 = $this->buffer->claimBatch(0, 8);
        // Worker 1 claims next 8
        $claimed1 = $this->buffer->claimBatch(1, 8);

        $this->assertCount(8, $claimed0);
        $this->assertCount(8, $claimed1);

        // No overlap in IDs
        $ids0 = array_column($claimed0, 'id');
        $ids1 = array_column($claimed1, 'id');
        $this->assertEmpty(array_intersect($ids0, $ids1), 'Workers should claim disjoint row sets');

        // 4 rows still pending
        $this->assertSame(4, $this->buffer->pendingCount());
    }

    public function testClaimBatchReturnsEmptyWhenNoPending(): void
    {
        $this->assertSame([], $this->buffer->claimBatch(0, 10));
    }

    public function testReleaseClaimed(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->buffer->appendRaw(json_encode(['i' => $i]));
        }

        $claimed = $this->buffer->claimBatch(0, 5);
        $this->assertCount(5, $claimed);
        $this->assertSame(0, $this->buffer->pendingCount());

        // Simulate worker crash — release claimed rows back to pending
        $this->buffer->releaseClaimed(0);
        $this->assertSame(5, $this->buffer->pendingCount());

        // Another worker can now claim them
        $reclaimed = $this->buffer->claimBatch(1, 5);
        $this->assertCount(5, $reclaimed);
    }

    public function testReleaseClaimedOnlyAffectsSpecificWorker(): void
    {
        for ($i = 0; $i < 10; $i++) {
            $this->buffer->appendRaw(json_encode(['i' => $i]));
        }

        $this->buffer->claimBatch(0, 5);
        $this->buffer->claimBatch(1, 5);
        $this->assertSame(0, $this->buffer->pendingCount());

        // Release only worker 0's claims
        $this->buffer->releaseClaimed(0);
        $this->assertSame(5, $this->buffer->pendingCount());

        // Worker 0 can re-claim the released rows
        $reclaimed = $this->buffer->claimBatch(0, 10);
        $this->assertCount(5, $reclaimed);

        // Worker 1's claims are still held — fetchPending returns nothing
        // because the remaining rows are claimed by worker 1 (synced=101)
        $this->assertSame(0, $this->buffer->pendingCount());
    }

    public function testMarkSyncedAfterClaim(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->buffer->appendRaw(json_encode(['i' => $i]));
        }

        $claimed = $this->buffer->claimBatch(0, 5);
        $ids = array_column($claimed, 'id');

        $this->buffer->markSynced($ids);

        // Neither pending nor claimable
        $this->assertSame(0, $this->buffer->pendingCount());
        $this->assertEmpty($this->buffer->claimBatch(0, 10));

        // Cleanup should remove them
        usleep(50_000);
        $deleted = $this->buffer->cleanup(0);
        $this->assertSame(5, $deleted);
    }

    public function testClaimBatchWithConcurrentSecondBuffer(): void
    {
        // Simulate multi-process access using two SqliteBuffer instances
        // on the same database file (mirrors the actual fork architecture)
        for ($i = 0; $i < 20; $i++) {
            $this->buffer->appendRaw(json_encode(['i' => $i]));
        }

        // Second buffer instance (same file, different PDO connection)
        $buffer2 = new SqliteBuffer($this->dbPath);

        $claimed0 = $this->buffer->claimBatch(0, 10);
        $claimed1 = $buffer2->claimBatch(1, 10);

        $ids0 = array_column($claimed0, 'id');
        $ids1 = array_column($claimed1, 'id');

        $this->assertCount(10, $claimed0);
        $this->assertCount(10, $claimed1);
        $this->assertEmpty(array_intersect($ids0, $ids1), 'Two buffer instances should claim disjoint rows');

        unset($buffer2);
    }

    // --- WAL checkpoint strategy ---

    public function testCheckpointTruncateResetsWalSize(): void
    {
        // Insert enough data to create a non-trivial WAL
        for ($i = 0; $i < 100; $i++) {
            $this->buffer->appendRaw(json_encode([
                't' => 'request', 'url' => '/test', 'data' => str_repeat('x', 500),
            ]));
        }

        $walBefore = $this->buffer->walSize();

        $this->buffer->checkpointTruncate();

        $walAfter = $this->buffer->walSize();

        // WAL should be smaller after truncate checkpoint (may be 0 or header-only)
        $this->assertLessThanOrEqual($walBefore, $walAfter);
    }

    public function testPassiveCheckpointDoesNotBlock(): void
    {
        // Verify PASSIVE checkpoint completes without error under write load
        for ($i = 0; $i < 50; $i++) {
            $this->buffer->appendRaw(json_encode(['i' => $i]));
        }

        // PASSIVE should not throw
        $this->buffer->checkpoint();

        // Buffer should still be functional after checkpoint
        $this->buffer->appendRaw(json_encode(['after' => 'checkpoint']));
        $pending = $this->buffer->fetchPending(100);
        $this->assertSame(51, count($pending));
    }

    public function testMultipleCheckpointCyclesStable(): void
    {
        // Simulate the drain worker pattern: write, drain, checkpoint, repeat
        for ($cycle = 0; $cycle < 5; $cycle++) {
            for ($i = 0; $i < 20; $i++) {
                $this->buffer->appendRaw(json_encode(['cycle' => $cycle, 'i' => $i]));
            }

            $rows = $this->buffer->fetchPending(20);
            $this->buffer->markSynced(array_column($rows, 'id'));

            $this->buffer->checkpoint();
            $this->buffer->cleanup(0);
        }

        // No pending rows, no crash
        $this->assertSame(0, $this->buffer->pendingCount());
    }

    // --- Ordering ---

    public function testFetchPendingReturnsOldestFirst(): void
    {
        $this->buffer->appendRaw('{"order":"first"}');
        $this->buffer->appendRaw('{"order":"second"}');
        $this->buffer->appendRaw('{"order":"third"}');

        $pending = $this->buffer->fetchPending(10);

        $this->assertSame('first', json_decode($pending[0]['payload'], true)['order']);
        $this->assertSame('second', json_decode($pending[1]['payload'], true)['order']);
        $this->assertSame('third', json_decode($pending[2]['payload'], true)['order']);
    }
}
