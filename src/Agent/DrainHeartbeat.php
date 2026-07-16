<?php

namespace NightOwl\Agent;

/**
 * Liveness channel from a drain child to the agent parent.
 *
 * Separate from the drain metrics file on purpose. `updated_at` in
 * .drain-metrics.json answers "did a batch complete?" — DrainWorker::run calls
 * drainBatch() as the FIRST statement of its loop and writeDrainMetrics() only
 * afterwards, so it freezes for the whole batch and a 90s legitimate COPY is
 * indistinguishable from a wedge. This file answers a different question: "when did
 * the child last reach a point where PHP had control?" It is stamped at every step
 * boundary inside the batch, so the parent measures ONE blocking call rather than the
 * whole 25-step transaction.
 *
 * It cannot separate slow from wedged — nothing can from inside PHP, since a child
 * blocked in libpq never reaches a VM opcode boundary to stamp from. What it buys is
 * SCOPE (the threshold covers one statement, not BEGIN + 10 COPYs + 14 upserts +
 * COMMIT) and the PHASE LABEL.
 *
 * Clock: hrtime(true) — CLOCK_MONOTONIC on Linux, mach_absolute_time on macOS.
 * System-wide kernel clocks, so the value is comparable across fork, and no NTP step
 * can manufacture a false wedge from it. Deliberately NO wall clock: the parent's
 * incarnation check uses its own monotonic fork stamp, so a backward NTP step cannot
 * make a fresh stamp look stale.
 */
final class DrainHeartbeat
{
    /**
     * Throttle REPEATS of the same phase only — the idle buffer:claim spin runs
     * ~10x/s at drain_interval_ms=100 and needs none of them.
     */
    private const REPEAT_THROTTLE_NS = 1_000_000_000;

    private int $lastStampNs = 0;

    private ?string $lastPhase = null;

    private string $path;

    private string $tmpPath;

    public function __construct(string $sqlitePath, int $workerId = 0, int $totalWorkers = 1)
    {
        // Mirrors writeDrainMetrics' path convention.
        $this->path = $totalWorkers > 1
            ? $sqlitePath.".drain-alive-{$workerId}.json"
            : $sqlitePath.'.drain-alive.json';
        $this->tmpPath = $this->path.'.tmp';
    }

    /**
     * Stamp before entering a potentially-blocking call.
     *
     * ALWAYS stamps on a phase CHANGE; only repeats of the SAME phase are throttled.
     * That distinction is load-bearing, not an optimisation: a time-throttled-only
     * stamp records whichever phase happened to reset the throttle, so a fast batch
     * (26 steps in ~200ms) stamps ONCE — and if the COMMIT then wedges the parent
     * would report "blocked in buffer:claim", a SQLite operation, while the drain is
     * actually stuck in a Postgres COMMIT. A wrong subsystem is worse than no label.
     *
     * $phase is operator-facing ("pg:copy:nightowl_requests", "pg:begin"). It must
     * name the STATEMENT and never carry row values — the same privacy rule the drain
     * metrics follow for SQLSTATE + table.
     */
    public function enter(string $phase): void
    {
        $now = hrtime(true);

        if ($phase === $this->lastPhase && ($now - $this->lastStampNs) < self::REPEAT_THROTTLE_NS) {
            return;
        }

        $this->lastStampNs = $now;
        $this->lastPhase = $phase;

        $data = json_encode([
            'at_mono_ns' => $now,
            'phase' => $phase,
            'pid' => getmypid(),
        ], JSON_INVALID_UTF8_SUBSTITUTE);

        // Atomic tmp+rename, matching writeDrainMetrics. The parent treats an
        // unparseable file as absent.
        if ($data !== false && @file_put_contents($this->tmpPath, $data) !== false) {
            @rename($this->tmpPath, $this->path);
        }
    }
}
