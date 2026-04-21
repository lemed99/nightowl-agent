<?php

namespace NightOwl\Support;

use Laravel\Nightwatch\Contracts\Ingest;

final class MultiIngest implements Ingest
{
    /** @var Ingest[] */
    private array $ingests;

    public function __construct(Ingest ...$ingests)
    {
        $this->ingests = $ingests;
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
