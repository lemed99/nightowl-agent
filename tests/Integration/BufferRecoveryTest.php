<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\BufferRecovery;
use NightOwl\Agent\SqliteBuffer;
use NightOwl\Support\SqliteFailure;
use NightOwl\Tests\Support\CorruptBufferFixture;
use PHPUnit\Framework\TestCase;

/**
 * Bringing a corrupt buffer back into service, against real damaged files.
 *
 * Yomoney's buffer went corrupt twice in a week (2026-10-15, 2026-10-21) and
 * each time the agent stopped for good until someone moved the file by hand.
 * The two incidents were different damage: the second left the table intact
 * and only an index wrong, and REINDEX keeps every row that was waiting.
 */
class BufferRecoveryTest extends TestCase
{
    private string $dir;

    private string $path;

    protected function setUp(): void
    {
        $this->dir = sys_get_temp_dir().'/nightowl-recovery-'.getmypid().'-'.uniqid();
        mkdir($this->dir);
        $this->path = $this->dir.'/agent-buffer.sqlite';
    }

    protected function tearDown(): void
    {
        foreach (glob($this->dir.'/*') ?: [] as $f) {
            @unlink($f);
        }
        @rmdir($this->dir);
    }

    public function test_the_index_only_fixture_fails_the_way_yomoneys_buffer_did(): void
    {
        CorruptBufferFixture::indexOnly($this->path);

        $problems = BufferRecovery::integrityProblems($this->path);
        $this->assertContains('wrong # of entries in index idx_buffer_synced_id', $problems);

        // The drain is handed rows it already marked, then cannot mark them.
        $buffer = new SqliteBuffer($this->path);
        $rows = $buffer->fetchPending(5000);
        $this->assertSame(1, (int) $rows[0]['id'], 'row 1 was marked synced, but the stale index serves it again');

        try {
            $buffer->markSynced(array_column($rows, 'id'));
            $this->fail('marking through the broken index should fail');
        } catch (\PDOException $e) {
            $this->assertSame(SqliteFailure::CORRUPT, SqliteFailure::code($e));
        }
    }

    public function test_index_only_damage_is_repaired_in_place_without_losing_a_row(): void
    {
        CorruptBufferFixture::indexOnly($this->path);

        $result = BufferRecovery::recover($this->path);

        $this->assertSame(BufferRecovery::REINDEXED, $result['outcome']);
        $this->assertSame([], BufferRecovery::integrityProblems($this->path));
        $this->assertFileExists($this->path);
        $this->assertSame([], glob($this->path.'.corrupt-*'));

        // All 180 rows that had not been drained are still there, and the ones
        // already marked are no longer served.
        $buffer = new SqliteBuffer($this->path);
        $this->assertSame(180, $buffer->pendingCount());
        $rows = $buffer->fetchPending(5000);
        $this->assertSame(range(51, 230), array_map('intval', array_column($rows, 'id')));
        $buffer->markSynced(array_column($rows, 'id'));
        $this->assertSame(0, $buffer->pendingCount());
    }

    public function test_a_damaged_table_is_moved_aside_and_a_fresh_buffer_works(): void
    {
        CorruptBufferFixture::tableDamaged($this->path);

        $result = BufferRecovery::recover($this->path);

        $this->assertSame(BufferRecovery::MOVED_ASIDE, $result['outcome']);
        $this->assertNotNull($result['moved_to']);
        $this->assertFileExists($result['moved_to']);
        $this->assertStringStartsWith($this->path.'.corrupt-', $result['moved_to']);
        $this->assertFileDoesNotExist($this->path);
        $this->assertFileDoesNotExist($this->path.'-wal');
        $this->assertFileDoesNotExist($this->path.'-shm');

        $buffer = new SqliteBuffer($this->path);
        $buffer->appendRaw('{"fresh":true}');
        $this->assertSame(1, $buffer->pendingCount());
        $this->assertSame([], BufferRecovery::integrityProblems($this->path));
    }

    public function test_a_file_that_is_not_a_database_is_moved_aside(): void
    {
        file_put_contents($this->path, str_repeat('not a database ', 400));

        $result = BufferRecovery::recover($this->path);

        $this->assertSame(BufferRecovery::MOVED_ASIDE, $result['outcome']);
        $this->assertNull($result['pending_rows']);
        $this->assertFileDoesNotExist($this->path);
    }

    public function test_a_healthy_buffer_is_left_alone(): void
    {
        $buffer = new SqliteBuffer($this->path);
        $buffer->appendRaw('{"ok":true}');
        unset($buffer);

        $result = BufferRecovery::recover($this->path);

        $this->assertSame(BufferRecovery::CLEAN, $result['outcome']);
        $this->assertSame(1, (new SqliteBuffer($this->path))->pendingCount());
        $this->assertSame([], glob($this->path.'.corrupt-*'));
    }

    public function test_only_the_newest_corrupt_copies_are_kept(): void
    {
        // Older copies from earlier recoveries, named the way moveAside() names them.
        foreach (['20260101-000000', '20260102-000000', '20260103-000000'] as $stamp) {
            file_put_contents($this->path.'.corrupt-'.$stamp, 'old');
            file_put_contents($this->path.'.corrupt-'.$stamp.'-wal', 'old');
        }
        file_put_contents($this->path, 'not a database');

        $moved = BufferRecovery::moveAside($this->path);

        $kept = glob($this->path.'.corrupt-*') ?: [];
        $mains = array_values(array_filter($kept, fn ($f) => ! str_ends_with($f, '-wal') && ! str_ends_with($f, '-shm')));
        $this->assertCount(BufferRecovery::KEEP_CORRUPT_COPIES, $mains);
        $this->assertContains($moved, $mains);
        $this->assertContains($this->path.'.corrupt-20260103-000000', $mains);
        $this->assertFileDoesNotExist($this->path.'.corrupt-20260101-000000-wal');
    }

    public function test_index_messages_are_told_apart_from_table_damage(): void
    {
        $this->assertTrue(BufferRecovery::onlyIndexesDamaged([
            'wrong # of entries in index idx_buffer_synced_id',
            'row 7 missing from index idx_buffer_synced_id',
            'non-unique entry in index sqlite_autoindex_buffer_1',
        ]));

        // Oct 15's messages: none of these can be fixed by REINDEX.
        $this->assertFalse(BufferRecovery::onlyIndexesDamaged([
            'wrong # of entries in index idx_buffer_synced_id',
            '*** in database main ***'."\n".'Page 1359: btreeInitPage() returns error code 11',
        ]));
        $this->assertFalse(BufferRecovery::onlyIndexesDamaged(['Freelist: size is 12 but should be 9']));
        $this->assertFalse(BufferRecovery::onlyIndexesDamaged([]));
    }

    /**
     * A damaged buffer in a directory the agent cannot rename in. This was
     * reported as set aside, with BUFFER_REPAIRED saying a fresh buffer was in
     * service, while the same corrupt file stayed open; and the -wal of that
     * file, which holds rows the file itself does not have yet, was renamed
     * away or deleted whenever the directory allowed it.
     */
    public function test_a_damaged_buffer_that_cannot_be_renamed_is_reported_unchecked_and_left_whole(): void
    {
        if (function_exists('posix_geteuid') && posix_geteuid() === 0) {
            $this->markTestSkipped('root renames whatever the directory mode');
        }

        CorruptBufferFixture::tableDamaged($this->path);
        // What a process that died with the file open leaves next to it.
        touch($this->path.'-wal');
        touch($this->path.'-shm');
        chmod($this->dir, 0555);

        try {
            $result = BufferRecovery::recover($this->path);
        } finally {
            chmod($this->dir, 0755);
        }

        $this->assertSame(BufferRecovery::UNCHECKED, $result['outcome']);
        $this->assertStringContainsString('could not be renamed aside', $result['problems'][0]);
        $this->assertNull($result['moved_to']);
        $this->assertFileExists($this->path);
        $this->assertFileExists($this->path.'-wal');
        $this->assertSame([], glob($this->path.'.corrupt-*'));
    }

    /**
     * A check that cannot run is not damage. Before, any failure counted as a
     * buffer beyond repair, and a file the agent briefly could not open would
     * have been set aside with everything in it.
     */
    public function test_a_buffer_that_cannot_be_opened_is_left_alone(): void
    {
        if (function_exists('posix_geteuid') && posix_geteuid() === 0) {
            $this->markTestSkipped('root opens a file whatever its mode');
        }

        $buffer = new SqliteBuffer($this->path);
        $buffer->appendRaw('[{"t":"request"}]');
        $buffer->checkpointTruncate();
        unset($buffer);
        chmod($this->path, 0000);

        try {
            $result = BufferRecovery::recover($this->path);
        } finally {
            chmod($this->path, 0644);
        }

        $this->assertSame(BufferRecovery::UNCHECKED, $result['outcome']);
        $this->assertSame([], glob($this->path.'.corrupt-*'));
        $this->assertSame(1, (new SqliteBuffer($this->path))->pendingCount());
    }
}
