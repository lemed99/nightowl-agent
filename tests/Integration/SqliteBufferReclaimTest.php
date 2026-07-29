<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\SqliteBuffer;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * Handing drained buffer pages back to the filesystem.
 *
 * SQLite reuses the pages a DELETEd row occupied but never returns them, so the
 * buffer file is high-water-mark sized: the load test measured 3.8 GB of file
 * holding 126,728 pending rows, and that file filled the host disk, killed
 * Postgres, and wedged the drain. Checkpointing does not help — it only resets
 * the -wal.
 *
 * The subtle half is the pragma ORDER. auto_vacuum lives in the database header
 * and can only be set while that header does not yet exist. `PRAGMA
 * journal_mode=WAL` materializes the header, after which `PRAGMA
 * auto_vacuum=INCREMENTAL` is silently a no-op — it does not error, and `PRAGMA
 * auto_vacuum` keeps answering 0. The first version of this fix set the pragma
 * after WAL and therefore did nothing at all, on every buffer, with no symptom
 * beyond the file never shrinking. test_a_new_buffer_is_created_in_incremental_
 * mode is the guard against that regression returning.
 */
class SqliteBufferReclaimTest extends TestCase
{
    /** ~2 KB per payload, so a few thousand rows make a measurable file. */
    private const PAYLOAD_ROWS = 4000;

    private string $path;

    protected function setUp(): void
    {
        $this->path = sys_get_temp_dir().'/nightowl-reclaim-'.getmypid().'-'.uniqid().'.sqlite';
    }

    protected function tearDown(): void
    {
        foreach (['', '-wal', '-shm'] as $suffix) {
            $f = $this->path.$suffix;
            if (file_exists($f)) {
                @unlink($f);
            }
        }
    }

    // ─── Helpers ───────────────────────────────────────────────

    private function fill(SqliteBuffer $buffer, int $rows = self::PAYLOAD_ROWS): void
    {
        $payload = json_encode([[
            't' => 'request',
            'trace_id' => '00000000-0000-4000-8000-000000000001',
            'method' => 'GET',
            'route' => '/reclaim/'.str_repeat('x', 1800),
            'duration' => 1000,
        ]], JSON_THROW_ON_ERROR);

        for ($i = 0; $i < $rows; $i++) {
            $buffer->appendRaw($payload);
        }
    }

    /** Mark everything drained and delete it, leaving a large freelist. */
    private function drainAll(SqliteBuffer $buffer): void
    {
        do {
            $rows = $buffer->fetchPending(1000);
            if ($rows === []) {
                break;
            }
            $buffer->markSynced(array_column($rows, 'id'));
        } while (true);

        $buffer->cleanup(maxAge: 0);
        $buffer->checkpointTruncate();
    }

    private function autoVacuumMode(string $path): int
    {
        $pdo = new PDO('sqlite:'.$path);

        return (int) $pdo->query('PRAGMA auto_vacuum')->fetchColumn();
    }

    /**
     * A buffer file as it existed before this fix: WAL, no auto_vacuum. Written
     * with a raw PDO in exactly the order the old constructor used, so the fixture
     * is the real legacy artifact rather than an approximation of one.
     */
    private function createLegacyBufferFile(): void
    {
        $pdo = new PDO('sqlite:'.$this->path);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->exec('PRAGMA busy_timeout=5000');
        $pdo->exec('PRAGMA journal_mode=WAL');
        $pdo->exec('PRAGMA auto_vacuum=INCREMENTAL'); // the no-op that shipped
        $pdo->exec('CREATE TABLE IF NOT EXISTS buffer (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL,
            record_count INTEGER NOT NULL,
            created_at REAL NOT NULL,
            synced INTEGER NOT NULL DEFAULT 0
        )');
        $pdo = null;
    }

    // ─── Tests ─────────────────────────────────────────────────

    /**
     * THE ordering regression test. If the auto_vacuum pragma is ever moved below
     * journal_mode=WAL this drops to 0 and every other reclaim path silently stops
     * working.
     */
    public function test_a_new_buffer_is_created_in_incremental_mode(): void
    {
        $buffer = new SqliteBuffer($this->path);
        $this->fill($buffer, 10);

        $this->assertSame(
            2,
            $this->autoVacuumMode($this->path),
            'PRAGMA auto_vacuum should read 2 (INCREMENTAL) — 0 means the pragma ran after journal_mode=WAL and no-opped'
        );
        $this->assertFalse($buffer->needsCompaction(), 'a fresh buffer never needs the blocking VACUUM path');
    }

    public function test_the_legacy_fixture_really_is_legacy(): void
    {
        // Guards the fixture itself: if this ever reads 2, the "no-op after WAL"
        // premise no longer holds on the local SQLite and every legacy-path test
        // below would be silently exercising the modern path instead.
        $this->createLegacyBufferFile();

        $this->assertSame(
            0,
            $this->autoVacuumMode($this->path),
            'setting auto_vacuum after journal_mode=WAL is expected to be a silent no-op'
        );
        $this->assertTrue(
            (new SqliteBuffer($this->path))->needsCompaction(),
            'an already-deployed buffer must be detected as needing compaction'
        );
    }

    public function test_reclaim_returns_freed_space_to_the_filesystem(): void
    {
        $buffer = new SqliteBuffer($this->path);
        $this->fill($buffer);

        $peak = $buffer->fileSize();
        $this->assertGreaterThan(4 * 1024 * 1024, $peak, 'fixture should build a few MB of buffer');

        $this->drainAll($buffer);

        $this->assertGreaterThan(0, $buffer->freeBytes(), 'drained pages should be on the freelist');
        $this->assertGreaterThan(
            $peak * 0.9,
            $buffer->fileSize(),
            'the file must NOT have shrunk on its own — that is the bug being fixed'
        );

        // Unbounded here: the point is that the space comes back at all.
        $freed = 0;
        do {
            $slice = $buffer->reclaim(2048);
            $freed += $slice;
        } while ($slice > 0);

        $this->assertGreaterThan(0, $freed, 'reclaim() returned nothing');
        $this->assertLessThan(
            $peak / 2,
            $buffer->fileSize(),
            sprintf('file should be far below its %d-byte peak after full reclaim', $peak)
        );
    }

    /**
     * Bounded on purpose: incremental_vacuum holds the write lock for the length of
     * a slice, and the ingest process shares this file. One slice must move a
     * fraction of the freelist, not all of it.
     */
    public function test_reclaim_is_bounded_by_its_page_argument(): void
    {
        $buffer = new SqliteBuffer($this->path);
        $this->fill($buffer);
        $this->drainAll($buffer);

        $freeBefore = $buffer->freeBytes();
        $this->assertGreaterThan(0, $freeBefore);

        $buffer->reclaim(16);

        $this->assertGreaterThan(
            0,
            $buffer->freeBytes(),
            'a 16-page slice must not drain the whole freelist — the bound is what keeps ingest responsive'
        );
        $this->assertLessThan($freeBefore, $buffer->freeBytes(), 'the slice should still have freed something');
    }

    public function test_reclaim_is_a_noop_for_a_non_positive_page_count(): void
    {
        $buffer = new SqliteBuffer($this->path);
        $this->fill($buffer, 200);
        $this->drainAll($buffer);

        $size = $buffer->fileSize();

        $this->assertSame(0, $buffer->reclaim(0));
        $this->assertSame(0, $buffer->reclaim(-1));
        $this->assertSame($size, $buffer->fileSize());
    }

    /**
     * The already-deployed case. A legacy buffer cannot shrink incrementally at all
     * — incremental_vacuum is a no-op in mode 0 — so compact()'s full VACUUM is the
     * only thing that recovers it, and it also converts the file so every later
     * reclaim takes the cheap path.
     */
    public function test_compact_recovers_and_converts_a_legacy_buffer(): void
    {
        $this->createLegacyBufferFile();

        $buffer = new SqliteBuffer($this->path);
        $this->fill($buffer);
        $peak = $buffer->fileSize();
        $this->drainAll($buffer);

        $this->assertSame(0, $buffer->reclaim(2048), 'incremental_vacuum cannot free anything in mode 0');
        $this->assertGreaterThan($peak * 0.9, $buffer->fileSize(), 'so the legacy file is still at its peak');

        $freed = $buffer->compact();

        $this->assertGreaterThan(0, $freed, 'VACUUM should have released the drained pages');
        $this->assertLessThan($peak / 2, $buffer->fileSize());
        $this->assertSame(2, $this->autoVacuumMode($this->path), 'VACUUM should apply the pending INCREMENTAL mode');
        $this->assertFalse($buffer->needsCompaction(), 'and the buffer should never need the blocking path again');
    }

    /**
     * The drain worker only spends the blocking VACUUM on an IDLE legacy buffer.
     * Running it with a backlog would hold the write lock across a full file
     * rewrite while ingest is still appending.
     */
    public function test_the_drain_leaves_a_legacy_buffer_alone_while_rows_are_pending(): void
    {
        $this->createLegacyBufferFile();

        $buffer = new SqliteBuffer($this->path);
        $this->fill($buffer);

        // Drain everything, then put ONE row back so the buffer is non-idle while
        // still carrying a big freelist.
        $this->drainAll($buffer);
        $this->fill($buffer, 1);

        $sizeBefore = $buffer->fileSize();
        $this->reclaimVia($buffer, thresholdBytes: 1);

        $this->assertSame(
            $sizeBefore,
            $buffer->fileSize(),
            'a legacy buffer with pending rows must be left for a later, idle tick'
        );

        // Drain the straggler and the very next tick should compact.
        $this->drainAll($buffer);
        $this->reclaimVia($buffer, thresholdBytes: 1);

        $this->assertLessThan($sizeBefore / 2, $buffer->fileSize(), 'an idle legacy buffer should compact');
    }

    /**
     * Below the threshold the freelist is left alone — it is cheap reuse for the
     * next spike, and trimming it would cost a write lock for nothing.
     */
    public function test_the_drain_skips_reclamation_below_the_threshold(): void
    {
        $buffer = new SqliteBuffer($this->path);
        $this->fill($buffer);
        $this->drainAll($buffer);

        $sizeBefore = $buffer->fileSize();
        $this->reclaimVia($buffer, thresholdBytes: 1024 * 1024 * 1024);

        $this->assertSame($sizeBefore, $buffer->fileSize(), 'a sub-threshold freelist should be left in place');
    }

    /** Drive the worker's private cleanup-tick reclamation with a tuned threshold. */
    private function reclaimVia(SqliteBuffer $buffer, int $thresholdBytes): void
    {
        $worker = new DrainWorker(
            sqlitePath: $this->path,
            pgHost: '127.0.0.1',
            pgPort: 1,
            pgDatabase: 'unused',
            pgUsername: 'unused',
            pgPassword: 'unused',
            reclaimThresholdBytes: $thresholdBytes,
        );

        (new \ReflectionMethod($worker, 'reclaimBufferSpace'))->invoke($worker, $buffer);
    }
}
