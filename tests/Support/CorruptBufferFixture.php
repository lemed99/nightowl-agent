<?php

namespace NightOwl\Tests\Support;

use NightOwl\Agent\SqliteBuffer;
use PDO;

/**
 * Real SQLite buffers damaged the two ways Yomoney's were.
 *
 * Built by writing old page images back over a live file, not by
 * `writable_schema` tricks, so the damage is what a lost write leaves: the
 * WAL-reset bug drops part of a committed transaction's pages, which is how a
 * table and its index end up disagreeing.
 */
final class CorruptBufferFixture
{
    /**
     * The 2026-10-21 shape: the table is intact, idx_buffer_synced_id is not.
     *
     * 200 rows are appended and the index's page is saved. Then rows 1-50 are
     * marked synced and 30 more rows appended, and the saved page is written
     * back. The index still lists rows 1-50 as pending and has never heard of
     * rows 201-230, so `fetchPending()` re-serves 50 rows the drain already
     * sent, and marking them fails with SQLite error 11: what Yomoney's drain
     * did for five minutes and then forever.
     *
     * Leaves 180 rows pending (ids 51-230) in the table.
     */
    public static function indexOnly(string $path): void
    {
        $buffer = new SqliteBuffer($path);
        for ($i = 0; $i < 200; $i++) {
            $buffer->appendRaw('[{"t":"request","n":'.$i.'}]');
        }
        $buffer->checkpointTruncate();
        unset($buffer);

        self::loseIndexWrite($path, range(1, 50), 30);
    }

    /**
     * Break idx_buffer_synced_id on an existing buffer, such as one a real
     * agent filled: save the index's page, mark $markIds synced and append
     * $appendRows more rows, then write the saved page back. The index must
     * still fit on its root page (a few hundred rows), or the restored page
     * would point at pages that changed underneath it.
     *
     * No other connection may have the buffer open.
     *
     * @param  int[]  $markIds
     */
    public static function loseIndexWrite(string $path, array $markIds, int $appendRows = 0): void
    {
        $buffer = new SqliteBuffer($path);
        $buffer->checkpointTruncate();
        unset($buffer);

        $page = self::rootPage($path, 'idx_buffer_synced_id');
        $saved = self::readPage($path, $page);

        $buffer = new SqliteBuffer($path);
        $buffer->markSynced($markIds);
        for ($i = 0; $i < $appendRows; $i++) {
            $buffer->appendRaw('[{"t":"request","m":'.$i.'}]');
        }
        $buffer->checkpointTruncate();
        unset($buffer);

        self::writePage($path, $page, $saved);
    }

    /**
     * The 2026-10-15 shape: a page of the table itself is unreadable, which no
     * index rebuild can fix. The table's root page is overwritten with bytes
     * that are not a b-tree page.
     */
    public static function tableDamaged(string $path): void
    {
        $buffer = new SqliteBuffer($path);
        for ($i = 0; $i < 300; $i++) {
            $buffer->appendRaw('[{"t":"request","n":'.$i.',"pad":"'.str_repeat('x', 200).'"}]');
        }
        $buffer->checkpointTruncate();
        unset($buffer);

        $page = self::rootPage($path, 'buffer');
        self::writePage($path, $page, str_repeat("\xAB", self::pageSize($path)));
    }

    private static function rootPage(string $path, string $name): int
    {
        $pdo = new PDO("sqlite:{$path}");
        $stmt = $pdo->prepare('SELECT rootpage FROM sqlite_schema WHERE name = ?');
        $stmt->execute([$name]);

        return (int) $stmt->fetchColumn();
    }

    private static function pageSize(string $path): int
    {
        return (int) (new PDO("sqlite:{$path}"))->query('PRAGMA page_size')->fetchColumn();
    }

    private static function readPage(string $path, int $page): string
    {
        $size = self::pageSize($path);
        $fh = fopen($path, 'rb');
        fseek($fh, ($page - 1) * $size);
        $bytes = fread($fh, $size);
        fclose($fh);

        return $bytes;
    }

    private static function writePage(string $path, int $page, string $bytes): void
    {
        $fh = fopen($path, 'r+b');
        fseek($fh, ($page - 1) * strlen($bytes));
        fwrite($fh, $bytes);
        fclose($fh);
    }
}
