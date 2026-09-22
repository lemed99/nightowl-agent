<?php

namespace NightOwl\Support;

use Laravel\Nightwatch\Contracts\Ingest;
use Throwable;

/**
 * Wraps the NightOwl-bound ingest so request records carry what PayloadCapture
 * decided — and ONLY the NightOwl-bound one: in parallel mode Nightwatch's own
 * ingest sits beside this in MultiIngest and receives its record untouched.
 *
 * Fail-open like everything on this path: if capture throws, the record goes
 * out exactly as Nightwatch built it. Losing a body is acceptable; losing the
 * request, or surfacing an error inside the customer's request, is not.
 */
final class CapturingIngest implements Ingest
{
    private bool $warned = false;

    public function __construct(
        public readonly Ingest $inner,
        private PayloadCapture $capture,
    ) {
        //
    }

    public function write(array $record): void
    {
        $this->inner->write($this->enrich($record));
    }

    public function writeNow(array $record): void
    {
        $this->inner->writeNow($this->enrich($record));
    }

    public function ping(): void
    {
        $this->inner->ping();
    }

    public function shouldDigest(bool $bool = true): void
    {
        $this->inner->shouldDigestWhenBufferIsFull($bool);
    }

    public function shouldDigestWhenBufferIsFull(bool $bool = true): void
    {
        $this->inner->shouldDigestWhenBufferIsFull($bool);
    }

    public function digest(): void
    {
        $this->inner->digest();
    }

    public function flush(): void
    {
        $this->inner->flush();
    }

    /**
     * @param  array<mixed>  $record
     * @return array<mixed>
     */
    private function enrich(array $record): array
    {
        try {
            return $this->capture->enrich($record);
        } catch (Throwable $e) {
            if (! $this->warned) {
                $this->warned = true;
                error_log('[NightOwl Agent] payload capture failed, sending the record uncaptured: '.$e->getMessage());
            }

            return $record;
        }
    }
}
