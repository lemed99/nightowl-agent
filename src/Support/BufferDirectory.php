<?php

namespace NightOwl\Support;

/**
 * Keeps the agent's working files out of the application's git repository.
 *
 * The buffer directory holds the SQLite buffer (raw telemetry), its WAL and
 * lock files, and the saved alert-channel list, which carries SMTP passwords
 * and webhook secrets in plain text. It defaults to storage/nightowl, and a
 * default Laravel .gitignore does not cover that directory, so a `git add -A`
 * on a developer machine committed all of it.
 */
final class BufferDirectory
{
    public const GITIGNORE = "*\n!.gitignore\n";

    /**
     * Whether the agent should write a .gitignore into $dir.
     *
     * Always for NightOwl's own storage/nightowl. For a custom buffer
     * directory only when the agent created it and it lies inside the
     * application: a directory someone made on purpose is theirs to manage,
     * and one outside the application is not in its repository.
     */
    public static function shouldProtect(string $dir, bool $createdByAgent, string $storageDir, string $basePath): bool
    {
        $real = realpath($dir);
        if ($real === false) {
            return false;
        }

        $storage = realpath($storageDir);
        if ($storage !== false && $real === $storage) {
            return true;
        }

        $base = realpath($basePath);
        if (! $createdByAgent || $base === false || $real === $base) {
            return false;
        }

        return str_starts_with($real.DIRECTORY_SEPARATOR, rtrim($base, DIRECTORY_SEPARATOR).DIRECTORY_SEPARATOR);
    }

    /**
     * Write $dir/.gitignore ignoring everything but itself, unless a file by
     * that name is already there: an existing one is never replaced. Returns
     * whether one exists afterwards. Never throws.
     */
    public static function ensureGitignore(string $dir): bool
    {
        $file = rtrim($dir, DIRECTORY_SEPARATOR).DIRECTORY_SEPARATOR.'.gitignore';

        if (file_exists($file) || is_link($file)) {
            return true;
        }

        if (! is_dir($dir)) {
            return false;
        }

        // 'x' creates the file only if it does not exist, so two agents
        // starting at once cannot overwrite each other or an operator's file.
        $handle = @fopen($file, 'x');
        if ($handle === false) {
            return file_exists($file);
        }

        $written = @fwrite($handle, self::GITIGNORE);
        fclose($handle);

        if ($written !== strlen(self::GITIGNORE)) {
            @unlink($file);

            return false;
        }

        return true;
    }
}
