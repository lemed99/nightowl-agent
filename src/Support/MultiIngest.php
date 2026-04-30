<?php

namespace NightOwl\Support;

use Laravel\Nightwatch\Contracts\Ingest;
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
            } catch (\Throwable $e) {
                // Log asymmetric-failure in the fan-out — without this, one side
                // can stop ingesting for weeks without anyone noticing.
                error_log('[NightOwl Support] MultiIngest write failed ('.$ingest::class.'): '.$e->getMessage());
            }
        }
    }

    public function writeNow(array $record): void
    {
        foreach ($this->ingests as $ingest) {
            try {
                $ingest->writeNow($record);
            } catch (\Throwable $e) {
                error_log('[NightOwl Support] MultiIngest writeNow failed ('.$ingest::class.'): '.$e->getMessage());
            }
        }
    }

    public function ping(): void
    {
        foreach ($this->ingests as $ingest) {
            try {
                $ingest->ping();
            } catch (\Throwable $e) {
                error_log('[NightOwl Support] MultiIngest ping failed ('.$ingest::class.'): '.$e->getMessage());
            }
        }
    }

    public function shouldDigest(bool $bool = true): void
    {
        foreach ($this->ingests as $ingest) {
            $ingest->shouldDigest($bool);
        }
    }

    public function shouldDigestWhenBufferIsFull(bool $bool = true): void
    {
        foreach ($this->ingests as $ingest) {
            $ingest->shouldDigestWhenBufferIsFull($bool);
        }
    }

    public function digest(): void
    {
        foreach ($this->ingests as $ingest) {
            try {
                $ingest->digest();
            } catch (\Throwable $e) {
                error_log('[NightOwl Support] MultiIngest digest failed ('.$ingest::class.'): '.$e->getMessage());
            }
        }
    }

    public function flush(): void
    {
        foreach ($this->ingests as $ingest) {
            try {
                $ingest->flush();
            } catch (\Throwable $e) {
                error_log('[NightOwl Support] MultiIngest flush failed ('.$ingest::class.'): '.$e->getMessage());
            }
        }
    }
}
