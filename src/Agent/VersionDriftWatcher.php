<?php

namespace NightOwl\Agent;

use NightOwl\Support\InstalledVersionReader;

/**
 * Notices when `composer update` has replaced the installed nightowl/agent on
 * disk while this daemon keeps running the version it loaded at boot, and says
 * so in the log. It does NOT act on that — restarting the daemon is the
 * operator's call, made by whatever already restarts their processes.
 *
 * Why warn at all: a running daemon cannot pick up new code, and drain workers
 * respawn as fresh interpreters — so after an update a respawned worker runs
 * NEW code under an OLD parent, a combination nothing tests. That window is
 * worth naming in the log rather than leaving silent, because from the outside
 * everything looks healthy.
 *
 * Why only warn: acting on it means exiting and trusting the supervisor to
 * bring us back, which depends on how the customer configured supervision —
 * something the agent cannot verify and gets no feedback about. When that
 * assumption is wrong the agent simply stays down, which is far worse than
 * running slightly stale code. So this reports and leaves the decision alone.
 *
 * Two consecutive reads must agree before warning: composer rewrites the vendor
 * tree non-atomically over seconds, and a half-written file should never
 * produce a warning telling someone to restart mid-update.
 *
 * Clock-free: the caller owns the poll interval, which keeps this trivially
 * unit-testable.
 */
final class VersionDriftWatcher
{
    /**
     * Version on disk at boot — what this process is actually running. null
     * (installed.php unreadable or not locatable) disables the watcher: with
     * nothing to compare against, every later read would look like a change.
     */
    private readonly ?string $baseline;

    /** Candidate new version awaiting its confirming second read (debounce). */
    private ?string $pendingCandidate = null;

    /** The drift we have already warned about — warn once per distinct version, not once per tick. */
    private ?string $warnedAbout = null;

    /** Log an unreadable installed.php once, not once per tick. */
    private bool $loggedReadFailure = false;

    public function __construct(
        private readonly InstalledVersionReader $reader,
        private readonly float $pollSeconds = 300.0,
    ) {
        $this->baseline = $this->reader->read();
    }

    public function pollSeconds(): float
    {
        return max(1.0, $this->pollSeconds);
    }

    /** Whether a drift warning is currently outstanding (exposed on the status endpoint). */
    public function hasDrifted(): bool
    {
        return $this->warnedAbout !== null;
    }

    /** The version installed on disk once drift was confirmed, or null. */
    public function driftedTo(): ?string
    {
        return $this->warnedAbout;
    }

    /** The version this process is running, or null when it could not be determined. */
    public function runningVersion(): ?string
    {
        return $this->baseline;
    }

    /**
     * A human-readable warning the first time a new installed version is
     * confirmed, then null until the version on disk changes again.
     */
    public function check(): ?string
    {
        if ($this->baseline === null) {
            return null;
        }

        $current = $this->reader->read();

        if ($current === null) {
            // Mid-composer-update, or the file vanished (a pruned symlink
            // release). Cannot tell, so do nothing — and drop any candidate,
            // since its confirming read just failed.
            $this->pendingCandidate = null;
            if (! $this->loggedReadFailure) {
                $this->loggedReadFailure = true;
                error_log('[NightOwl Agent] installed.php became unreadable; update detection is paused until it can be read again.');
            }

            return null;
        }

        $this->loggedReadFailure = false;

        if ($current === $this->baseline) {
            // Reverted before we warned, or never moved.
            $this->pendingCandidate = null;
            $this->warnedAbout = null;

            return null;
        }

        if ($current === $this->warnedAbout) {
            return null; // already said so
        }

        if ($current !== $this->pendingCandidate) {
            // First sighting: arm the candidate and wait. By the next tick
            // composer has almost surely finished writing.
            $this->pendingCandidate = $current;

            return null;
        }

        $this->warnedAbout = $current;

        return "a newer nightowl/agent is installed on disk ({$this->baseline} -> {$current}) but this process is still "
            .'running the old one. Restart the agent to pick it up — until then the parent runs old code while any '
            .'respawned drain worker starts on the new code.';
    }
}
