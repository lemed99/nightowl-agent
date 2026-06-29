<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\RecordWriter;
use PHPUnit\Framework\TestCase;

/**
 * created_at and the rollup bucket derive from each event's own timestamp, but a
 * malformed value (millisecond-scaled or garbage) must fall back to the drain clock —
 * otherwise it stamps a row tens of thousands of years out, which Postgres rejects
 * (datetime overflow, 22008) and, with quarantine off, head-of-line-blocks the drain.
 */
class RecordWriterEventTimeTest extends TestCase
{
    private function createdAt(array $r, int $nowTs): string
    {
        // Constructor is lazy (connects on first write), so no DB is touched here.
        $w = new RecordWriter('127.0.0.1', 5432, 'x', 'x', 'x');
        $m = new \ReflectionMethod($w, 'eventCreatedAt');

        return (string) $m->invoke($w, $r, $nowTs);
    }

    public function testValidSecondsTimestampIsUsed(): void
    {
        $now = 1735689600; // 2025-01-01
        $this->assertSame(gmdate('Y-m-d H:i:s', $now - 3600), $this->createdAt(['timestamp' => $now - 3600], $now));
    }

    public function testMillisecondScaledTimestampFallsBackToDrainClock(): void
    {
        $now = 1735689600;
        // A ms-scaled value (now * 1000) lands ~50,000 years out — must NOT be used.
        // Reverting the range guard makes this a far-future date instead of `now`.
        $this->assertSame(gmdate('Y-m-d H:i:s', $now), $this->createdAt(['timestamp' => $now * 1000], $now));
    }

    public function testGarbageAndMissingTimestampsFallBackToDrainClock(): void
    {
        $now = 1735689600;
        $expected = gmdate('Y-m-d H:i:s', $now);
        $this->assertSame($expected, $this->createdAt(['timestamp' => 99999999999999], $now)); // far future
        $this->assertSame($expected, $this->createdAt(['timestamp' => 'not-a-number'], $now)); // non-numeric
        $this->assertSame($expected, $this->createdAt([], $now));                              // missing
        $this->assertSame($expected, $this->createdAt(['timestamp' => 0], $now));              // epoch 0 (1970), implausible
    }
}
