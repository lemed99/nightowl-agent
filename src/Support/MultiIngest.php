<?php

namespace NightOwl\Support;

use Laravel\Nightwatch\Contracts\Ingest;
use Laravel\Nightwatch\Facades\Nightwatch;
use Laravel\Nightwatch\Ingest as NightwatchIngest;
use ReflectionClass;
use Throwable;

final class MultiIngest implements Ingest
{
    /** @var Ingest[] */
    private array $ingests;

    public function __construct(Ingest ...$ingests)
    {
        // Flatten + dedupe so re-wrapping can't multiply outbound writes.
        // Without this, NightOwlAgentServiceProvider::booted() firing more than
        // once with parallel_with_nightwatch=true compounds the chain (each
        // wrap adds another path to the agent), producing N copies of every
        // event in the customer's DB.
        $flat = [];
        foreach ($ingests as $ingest) {
            if ($ingest instanceof self) {
                foreach ($ingest->ingests as $inner) {
                    $flat[] = $inner;
                }
            } else {
                $flat[] = $ingest;
            }
        }

        $seen = [];
        $deduped = [];
        foreach ($flat as $ingest) {
            $sig = self::signatureFor($ingest);
            if (isset($seen[$sig])) {
                continue;
            }
            $seen[$sig] = true;
            $deduped[] = $ingest;
        }

        $this->ingests = $deduped;
    }

    private static function signatureFor(Ingest $ingest): string
    {
        // For Nightwatch's Ingest, two instances pointing at the same socket
        // with the same token hash are functionally identical — collapse them
        // even when they're freshly constructed (different object IDs).
        if ($ingest instanceof NightwatchIngest) {
            try {
                $r = new ReflectionClass($ingest);
                $transmitTo = $r->getProperty('transmitTo')->getValue($ingest);
                $tokenHash = $r->getProperty('tokenHash')->getValue($ingest);

                return 'nw:'.$transmitTo.'|'.$tokenHash;
            } catch (Throwable) {
                // Nightwatch internals changed — fall through to identity.
            }
        }

        return 'oid:'.spl_object_id($ingest);
    }

    public function write(array $record): void
    {
        foreach ($this->ingests as $ingest) {
            try {
                $ingest->write($record);
            } catch (Throwable $e) {
                $this->swallowed('write', $ingest, $e);
            }
        }
    }

    public function writeNow(array $record): void
    {
        foreach ($this->ingests as $ingest) {
            try {
                $ingest->writeNow($record);
            } catch (Throwable $e) {
                $this->swallowed('writeNow', $ingest, $e);
            }
        }
    }

    /**
     * Unlike every other method here, ping() does NOT fail open.
     *
     * It is a diagnostic, not telemetry: `nightwatch:status` decides whether
     * the agent is reachable purely by whether this throws
     * (Nightwatch\Console\StatusCommand). Swallowing here makes the one
     * command a customer runs to check connectivity report a healthy agent
     * while the socket is dead — the opposite of fail-open's intent, since
     * nothing downstream is at risk from an explicit health probe.
     */
    public function ping(): void
    {
        $failure = null;

        foreach ($this->ingests as $ingest) {
            try {
                $ingest->ping();
            } catch (Throwable $e) {
                // Probe every target before reporting: in parallel mode a
                // status check should still exercise the far side when the
                // near one is down.
                $failure ??= $e;
            }
        }

        if ($failure !== null) {
            throw $failure;
        }
    }

    // The two sampling setters below are guarded like the transport methods
    // even though Nightwatch's own Ingest cannot throw from them. They are
    // reached from Core::sample(), which sits OUTSIDE report()'s try block, and
    // in parallel mode the far side of the fan-out is whatever ingest the
    // customer already had bound. A wrapper whose job is to fail open should
    // not have two methods that don't.

    public function shouldDigest(bool $bool = true): void
    {
        foreach ($this->ingests as $ingest) {
            try {
                $ingest->shouldDigest($bool);
            } catch (Throwable $e) {
                $this->swallowed('shouldDigest', $ingest, $e);
            }
        }
    }

    public function shouldDigestWhenBufferIsFull(bool $bool = true): void
    {
        foreach ($this->ingests as $ingest) {
            try {
                $ingest->shouldDigestWhenBufferIsFull($bool);
            } catch (Throwable $e) {
                $this->swallowed('shouldDigestWhenBufferIsFull', $ingest, $e);
            }
        }
    }

    public function digest(): void
    {
        foreach ($this->ingests as $ingest) {
            try {
                $ingest->digest();
            } catch (Throwable $e) {
                $this->swallowed('digest', $ingest, $e);
            }
        }
    }

    public function flush(): void
    {
        foreach ($this->ingests as $ingest) {
            try {
                $ingest->flush();
            } catch (Throwable $e) {
                $this->swallowed('flush', $ingest, $e);
            }
        }
    }

    /**
     * Report a transport failure the fan-out just absorbed.
     *
     * Two channels, because they answer different questions. The error_log
     * line is the operator's: without it one side of the fan-out can stop
     * ingesting for weeks without anyone noticing. The Nightwatch hook is the
     * application's: `Nightwatch::handleUnrecoverableExceptionsUsing()` is the
     * documented place to observe telemetry failures, and before the ingest
     * was wrapped these reached it on their own. Swallowing them here without
     * forwarding would silently unregister a callback the customer set.
     *
     * The whole body is guarded because this method exists to stop exceptions
     * reaching the host application, and it would be absurd for it to become
     * the one that does: `unrecoverableExceptionOccurred()` is `@internal` to
     * Nightwatch, so an SDK that drops or renames it would otherwise raise an
     * Error from inside a catch block. NightwatchCompatibilityTest pins the
     * method so the drift surfaces in CI rather than as a quiet loss of the
     * hook. (The facade also guards the customer's callback itself.)
     */
    private function swallowed(string $method, Ingest $ingest, Throwable $e): void
    {
        try {
            error_log('[NightOwl Support] MultiIngest '.$method.' failed ('.$ingest::class.'): '.$e->getMessage());

            Nightwatch::unrecoverableExceptionOccurred($e);
        } catch (Throwable) {
            // Nothing left to report to.
        }
    }
}
