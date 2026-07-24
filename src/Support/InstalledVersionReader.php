<?php

namespace NightOwl\Support;

use Composer\InstalledVersions;

/**
 * Reads the CURRENTLY INSTALLED nightowl/agent version straight off disk.
 *
 * This exists because every in-process version source is frozen at boot:
 * Composer\InstalledVersions caches vendor/composer/installed.php in a static
 * on first use, and MetricsCollector::agentVersion() memoizes on top of that.
 * A long-running daemon therefore can never observe the version change that a
 * `composer update` writes to disk — which is exactly what the auto-restart
 * watcher needs to observe.
 *
 * Two deliberate non-choices:
 *  - NOT include/require: with opcache.validate_timestamps=0 (common in prod)
 *    a re-require serves the stale compiled file forever. file_get_contents
 *    always sees the real bytes.
 *  - NOT MetricsCollector::agentVersion(): its 16-char truncation exists for
 *    the health-report wire contract and can mask drift (two long branch names
 *    truncating identically). This reader returns the raw untruncated tuple
 *    "pretty_version#reference" — reference catches re-tags and dev-branch
 *    moves that pretty_version alone would miss.
 *
 * Fork-safety: no persistent handles, no state. A forked child inheriting this
 * object owns no file descriptor.
 *
 * All failure modes (missing file, mid-composer-update partial write, format
 * change) return null — the watcher treats null as "cannot tell, do nothing".
 */
final class InstalledVersionReader
{
    public function __construct(private readonly string $installedPhpPath)
    {
    }

    /**
     * Whether a path was configured at all. An empty path (installed.php could
     * not be located — odd bundler setups, sentinel-only harness runs) means
     * every read() is null BY CONFIGURATION, which callers must distinguish
     * from "configured but currently unreadable" (composer mid-rewrite).
     */
    public function isConfigured(): bool
    {
        return $this->installedPhpPath !== '';
    }

    /**
     * The installed tuple "pretty_version#reference", or null when unreadable.
     *
     * Reference may be absent (path-repo installs) — the tuple is then
     * "version#", still comparable. The regex requires the exact quoted key
     * 'nightowl/agent' followed by => so it can match neither the root block's
     * `'name' => 'nightowl/agent',` line (key followed by a comma there) nor
     * the 'nightowl/agent-simulator' package (closing quote breaks the prefix).
     * The capture stops at the first ')' — inside the package block that is
     * `'aliases' => array()`, which Composer writes AFTER pretty_version and
     * reference, so both keys are always inside the capture; if Composer ever
     * reorders, the miss degrades to null, never to a wrong value.
     */
    public function read(): ?string
    {
        try {
            if ($this->installedPhpPath === '' || ! is_file($this->installedPhpPath)) {
                return null;
            }

            $contents = @file_get_contents($this->installedPhpPath);
            if ($contents === false || $contents === '') {
                return null;
            }

            if (! preg_match("/'nightowl\\/agent'\\s*=>\\s*array\\s*\\(([^)]*)/", $contents, $block)) {
                return null;
            }

            if (! preg_match("/'pretty_version'\\s*=>\\s*'([^']*)'/", $block[1], $version) || $version[1] === '') {
                return null;
            }

            $reference = preg_match("/'reference'\\s*=>\\s*'([^']*)'/", $block[1], $ref) ? $ref[1] : '';

            return $version[1].'#'.$reference;
        } catch (\Throwable) {
            return null;
        }
    }

    /**
     * Locate installed.php relative to the loaded InstalledVersions class file
     * rather than assuming base_path('vendor') — robust to COMPOSER_VENDOR_DIR.
     *
     * Known blind spot (documented in README): under symlinked-release deploys
     * (Envoyer/Deployer) the class file's realpath pins to the RELEASE dir that
     * was current at boot, so a new release never changes this file — those
     * setups keep restarting the daemon from their deploy hook.
     */
    public static function defaultPath(): ?string
    {
        try {
            $classFile = (new \ReflectionClass(InstalledVersions::class))->getFileName();
            if ($classFile === false) {
                return null;
            }

            $path = dirname($classFile).'/installed.php';

            return is_file($path) ? $path : null;
        } catch (\Throwable) {
            return null;
        }
    }
}
