<?php

namespace NightOwl\Agent;

use NightOwl\Support\SqliteFailure;
use PDO;

/**
 * Check a corrupt SQLite buffer and bring it back into service.
 *
 * Until this existed a corrupt buffer stopped the agent for good: every drain
 * batch failed with "database disk image is malformed", the buffer filled to
 * its ceiling, and from then on every payload was refused, until someone found
 * the file and moved it by hand. Yomoney lost 19 hours that way on 2026-10-15
 * and most of a day again on 2026-10-21.
 *
 * Those two incidents were different kinds of damage, and they want different
 * fixes:
 *
 * - 2026-10-21: the table was intact and only an index disagreed with it
 *   ("wrong # of entries in index idx_buffer_synced_id"). REINDEX rebuilds the
 *   index from the table, and all 100,043 waiting rows survive.
 * - 2026-10-15: freelist and pointer-map errors and unreadable b-tree pages.
 *   Nothing short of a new file fixes that. The old one is renamed, not
 *   deleted: rows that had not reached PostgreSQL are still in it.
 *
 * Only call this with no other connection open on the file (AsyncServer stops
 * its drain workers and closes its own handle first). REINDEX and the rename
 * both assume nobody else is writing.
 */
final class BufferRecovery
{
    /** integrity_check found nothing. */
    public const CLEAN = 'clean';

    /** Only indexes were damaged, and REINDEX fixed them. Nothing was lost. */
    public const REINDEXED = 'reindexed';

    /** The file was renamed aside; the agent starts a fresh buffer. */
    public const MOVED_ASIDE = 'moved_aside';

    /**
     * The check could not run for a reason that is not damage (the file busy,
     * the disk full, out of memory). Nothing was changed; the agent checks
     * again after its cooldown.
     */
    public const UNCHECKED = 'unchecked';

    /** Corrupt copies kept next to the buffer. Older ones are deleted, so a host that keeps corrupting cannot fill its disk with them. */
    public const KEEP_CORRUPT_COPIES = 2;

    // integrity_check lines that say an index disagrees with its table. Anything
    // else (a page, the freelist, the pointer map, a row's own record) means the
    // table itself is damaged, which REINDEX cannot repair.
    private const INDEX_ONLY_PATTERNS = [
        '/^wrong # of entries in index /',
        '/^row \d+ missing from index /',
        '/^non-unique entry in index /',
    ];

    // Lines integrity_check returns at most. Enough to classify the damage, and
    // it stops early on a badly broken file instead of listing every page.
    private const INTEGRITY_CHECK_LIMIT = 100;

    /**
     * Check the buffer at $path and repair it, or move it aside.
     *
     * pending_rows is the number of rows not yet drained that were left in the
     * moved file: null when the file could not be read, or was not moved.
     *
     * @return array{outcome: string, problems: list<string>, pending_rows: ?int, moved_to: ?string}
     */
    public static function recover(string $path): array
    {
        try {
            $problems = self::integrityProblems($path);
        } catch (\Throwable $e) {
            error_log("[NightOwl Agent] The buffer's integrity check could not run: {$e->getMessage()}");

            return self::result(self::UNCHECKED, [$e->getMessage()]);
        }

        if ($problems === []) {
            return self::result(self::CLEAN, []);
        }

        if ($problems !== null && self::onlyIndexesDamaged($problems)) {
            try {
                self::reindex($path);

                if (self::integrityProblems($path) === []) {
                    return self::result(self::REINDEXED, $problems);
                }
            } catch (\Throwable $e) {
                error_log("[NightOwl Agent] Rebuilding the buffer's indexes failed: {$e->getMessage()}");

                // Only damage justifies a new file. A busy or full disk is
                // not damage, and the file it would set aside is a good one.
                if (! SqliteFailure::isCorruption(SqliteFailure::code($e))) {
                    return self::result(self::UNCHECKED, $problems);
                }
            }
        }

        $pending = self::pendingRows($path);
        $movedTo = self::moveAside($path);

        // The file is still in service: reporting a fresh buffer would be
        // false, and the next recovery after the cooldown tries again.
        if ($movedTo === null) {
            return self::result(self::UNCHECKED, ['the damaged buffer could not be renamed aside (see the log line before this one)']);
        }

        return self::result(self::MOVED_ASIDE, $problems ?? ['the integrity check could not run'], $pending, $movedTo);
    }

    /**
     * One log line saying what recover() did.
     *
     * @param  array{outcome: string, problems: list<string>, pending_rows: ?int, moved_to: ?string}  $result
     */
    public static function describe(array $result): string
    {
        $problems = implode('; ', array_slice($result['problems'], 0, 3));

        return match ($result['outcome']) {
            self::CLEAN => 'The buffer passed its integrity check. Nothing to repair.',
            self::REINDEXED => "Rebuilt the buffer's indexes ({$problems}). No buffered rows were lost.",
            self::UNCHECKED => 'Could not check the buffer ('.($result['problems'][0] ?? 'unknown error').'). Left it as it is.',
            default => "The buffer is damaged beyond repair ({$problems}). Moved it aside as ".basename((string) $result['moved_to'])
                .($result['pending_rows'] !== null ? " with {$result['pending_rows']} rows not yet drained" : '')
                .'; starting a fresh buffer.',
        };
    }

    /**
     * What PRAGMA integrity_check reports, [] for a healthy file, or null when
     * the file is too damaged to check (SQLite says corrupt, or not a
     * database). Any other failure (busy, disk full, no file) is thrown: it
     * says nothing about the file.
     *
     * @return list<string>|null
     */
    public static function integrityProblems(string $path): ?array
    {
        try {
            $pdo = self::open($path);
            $rows = $pdo->query('PRAGMA integrity_check('.self::INTEGRITY_CHECK_LIMIT.')')->fetchAll(PDO::FETCH_COLUMN);
        } catch (\Throwable $e) {
            if (! SqliteFailure::isCorruption(SqliteFailure::code($e))) {
                throw $e;
            }
            error_log("[NightOwl Agent] The buffer is too damaged to check: {$e->getMessage()}");

            return null;
        }

        $rows = array_values(array_map('strval', $rows));

        return $rows === ['ok'] ? [] : $rows;
    }

    /**
     * Whether every problem integrity_check listed is an index disagreeing with
     * its table, the one kind of damage REINDEX repairs in place.
     *
     * @param  list<string>  $problems
     */
    public static function onlyIndexesDamaged(array $problems): bool
    {
        if ($problems === []) {
            return false;
        }

        foreach ($problems as $problem) {
            $matched = false;
            foreach (self::INDEX_ONLY_PATTERNS as $pattern) {
                if (preg_match($pattern, $problem) === 1) {
                    $matched = true;
                    break;
                }
            }

            if (! $matched) {
                return false;
            }
        }

        return true;
    }

    /**
     * Rename the buffer and its -wal and -shm to `<buffer>.corrupt-<UTC time>`
     * (with the same suffixes), then delete all but the newest
     * KEEP_CORRUPT_COPIES. Returns the new name of the main file, or null when
     * it could not be renamed, in which case nothing is touched: its -wal holds
     * committed rows the file itself does not have yet.
     */
    public static function moveAside(string $path): ?string
    {
        $target = $path.'.corrupt-'.gmdate('Ymd-His');
        // Two recoveries in one second would otherwise overwrite the first copy.
        for ($n = 2; file_exists($target); $n++) {
            $target = $path.'.corrupt-'.gmdate('Ymd-His').'-'.$n;
        }

        if (! @rename($path, $target)) {
            error_log("[NightOwl Agent] Could not rename {$path}: ".(error_get_last()['message'] ?? 'unknown error'));

            return null;
        }

        foreach (['-wal', '-shm'] as $suffix) {
            if (! file_exists($path.$suffix) || @rename($path.$suffix, $target.$suffix)) {
                continue;
            }

            // A -wal or -shm left behind would be read as part of the NEW
            // buffer: frames from the old file applied to a fresh one.
            // Deleting them is the only safe outcome if they cannot move.
            error_log("[NightOwl Agent] Could not rename {$path}{$suffix}: ".(error_get_last()['message'] ?? 'unknown error'));
            @unlink($path.$suffix);
        }

        self::pruneCorruptCopies($path);

        return $target;
    }

    /** Delete corrupt copies beyond the newest KEEP_CORRUPT_COPIES. */
    private static function pruneCorruptCopies(string $path): void
    {
        $copies = glob($path.'.corrupt-*') ?: [];
        $copies = array_values(array_filter($copies, fn (string $f) => ! preg_match('/-(wal|shm)$/', $f)));

        // The names carry a sortable UTC time, so the newest sort last.
        sort($copies, SORT_STRING);
        $stale = array_slice($copies, 0, max(0, count($copies) - self::KEEP_CORRUPT_COPIES));

        foreach ($stale as $copy) {
            foreach (['', '-wal', '-shm'] as $suffix) {
                if (file_exists($copy.$suffix)) {
                    @unlink($copy.$suffix);
                }
            }
        }
    }

    /** In a function of its own, so the connection closes even when REINDEX throws. */
    private static function reindex(string $path): void
    {
        self::open($path)->exec('REINDEX');
    }

    /** Rows still waiting to be drained, or null when the file cannot answer. */
    private static function pendingRows(string $path): ?int
    {
        try {
            // NOT INDEXED: the index may be the broken part, and the count should
            // come from the table.
            return (int) self::open($path)
                ->query('SELECT COUNT(*) FROM buffer NOT INDEXED WHERE synced = 0 OR synced >= 100')
                ->fetchColumn();
        } catch (\Throwable) {
            return null;
        }
    }

    private static function open(string $path): PDO
    {
        if (! is_file($path)) {
            throw new \RuntimeException("No buffer at {$path}");
        }

        $pdo = new PDO("sqlite:{$path}");
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->exec('PRAGMA busy_timeout=5000');

        return $pdo;
    }

    /**
     * @param  list<string>  $problems
     * @return array{outcome: string, problems: list<string>, pending_rows: ?int, moved_to: ?string}
     */
    private static function result(string $outcome, array $problems, ?int $pending = null, ?string $movedTo = null): array
    {
        return ['outcome' => $outcome, 'problems' => $problems, 'pending_rows' => $pending, 'moved_to' => $movedTo];
    }
}
