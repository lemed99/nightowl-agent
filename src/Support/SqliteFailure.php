<?php

namespace NightOwl\Support;

/**
 * The SQLite result code behind a local-buffer failure.
 *
 * pdo_sqlite reports every error as SQLSTATE HY000 and puts the useful part —
 * SQLite's own result code — in errorInfo[1]. That code is what tells a full
 * disk (13) from a read-only file or filesystem (8), a corrupt file (11) or a
 * lock held by another process (5), which is the whole difference between the
 * advice that fixes a stalled buffer and advice that sends someone to the
 * wrong machine.
 */
final class SqliteFailure
{
    public const BUSY = 5;

    public const LOCKED = 6;

    public const NOMEM = 7;

    public const READONLY = 8;

    public const IOERR = 10;

    public const CORRUPT = 11;

    public const FULL = 13;

    public const CANTOPEN = 14;

    public const MISUSE = 21;

    public const NOTADB = 26;

    /**
     * Primary result code, or 0 when the throwable carries none.
     *
     * Masked to the low byte: an extended code (SQLITE_IOERR_WRITE = 778) shares
     * its primary code's advice.
     */
    public static function code(\Throwable $e): int
    {
        if ($e instanceof \PDOException && is_array($e->errorInfo) && is_int($e->errorInfo[1] ?? null)) {
            return $e->errorInfo[1] & 0xFF;
        }

        // A PDOException raised by PDO itself rather than by a statement can carry
        // an empty errorInfo; the code is still in the text.
        if (preg_match('/General error: (\d+) /', $e->getMessage(), $m) === 1) {
            return ((int) $m[1]) & 0xFF;
        }

        return 0;
    }

    /**
     * Whether the code says something about the FILE. MISUSE does not: it is what
     * a statement reports when re-run after an earlier failure left it unusable.
     * On Yomoney's corrupt buffer (2026-10-15) the drain read "malformed" (11)
     * while ingest read only "misuse" (21).
     */
    public static function isInformative(int $code): bool
    {
        return $code !== 0 && $code !== self::MISUSE;
    }

    /** Whether the code says the file itself is damaged: corrupt, or not a database at all. */
    public static function isCorruption(int $code): bool
    {
        return $code === self::CORRUPT || $code === self::NOTADB;
    }

    /** Short name of the failure, for a diagnosis message. */
    public static function describe(int $code): string
    {
        return match ($code) {
            self::FULL => 'database or disk is full',
            self::READONLY => 'attempt to write a readonly database',
            self::IOERR => 'disk I/O error',
            self::CORRUPT => 'database disk image is malformed',
            self::NOTADB => 'file is not a database',
            self::CANTOPEN => 'unable to open database file',
            self::BUSY => 'database is locked',
            self::LOCKED => 'database table is locked',
            self::NOMEM => 'out of memory',
            self::MISUSE => 'bad parameter or other API misuse',
            0 => 'unknown error',
            default => "error {$code}",
        };
    }
}
