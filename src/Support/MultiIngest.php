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
            try { $ingest->write($record); } catch (\Throwable) {}
        }
    }

    public function writeNow(array $record): void
    {
        foreach ($this->ingests as $ingest) {
            try { $ingest->writeNow($record); } catch (\Throwable) {}
        }
    }

    public function ping(): void
    {
        foreach ($this->ingests as $ingest) {
            try { $ingest->ping(); } catch (\Throwable) {}
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
            try { $ingest->digest(); } catch (\Throwable) {}
        }
    }

    public function flush(): void
    {
        foreach ($this->ingests as $ingest) {
            $ingest->flush();
        }
    }
}
