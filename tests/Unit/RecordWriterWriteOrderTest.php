<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\RecordWriter;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

/**
 * github#9 — cross-table deadlocks between agents sharing one database.
 *
 * doWrite groups a batch by record type and hands each group to its writer in
 * turn, and that order IS the order the batch takes row locks across the rollup
 * tables. It used to be the order the types happened to appear in the batch, so
 * a web host (payloads opening with a `request`) and a queue host (opening with
 * a `query`) locked nightowl_request_rollups and nightowl_query_rollups in
 * opposite orders and deadlocked on an overlapping bucket.
 *
 * The invariant these tests hold: the write order depends on WHICH types are in
 * the batch, never on the order they arrived in.
 */
class RecordWriterWriteOrderTest extends TestCase
{
    /**
     * @param  array<string, list<array<string, mixed>>>  $grouped
     * @return list<string>
     */
    private function order(array $grouped): array
    {
        $m = new ReflectionMethod(RecordWriter::class, 'orderedForWrite');

        return array_keys($m->invoke(null, $grouped));
    }

    public function testArrivalOrderDoesNotChangeWriteOrder(): void
    {
        // The two real workloads from the report: a web host whose payload opens
        // with the request, and a queue host whose payload opens with a query.
        $web = ['request' => [['t' => 'request']], 'query' => [['t' => 'query']]];
        $queue = ['query' => [['t' => 'query']], 'request' => [['t' => 'request']]];

        // Pre-fix this is the whole bug: array_keys($web) !== array_keys($queue).
        $this->assertSame(['request', 'query'], array_keys($web));
        $this->assertSame(['query', 'request'], array_keys($queue));

        $this->assertSame($this->order($web), $this->order($queue));
    }

    public function testEveryPermutationOfARealBatchAgreesOnOrder(): void
    {
        // Every relation named in the deadlock reports, in 120 arrival orders.
        $types = ['request', 'query', 'user', 'queued-job', 'job-attempt'];

        $expected = null;
        foreach ($this->permutations($types) as $arrival) {
            $grouped = [];
            foreach ($arrival as $t) {
                $grouped[$t] = [['t' => $t]];
            }
            $order = $this->order($grouped);
            $expected ??= $order;
            $this->assertSame($expected, $order, 'arrival '.implode(',', $arrival).' disagreed');
        }

        // And the agreed order is WRITE_ORDER's, not the alphabetical accident of
        // any one arrival.
        // 'queued-job' and 'job-attempt' collapse into one 'job' slot.
        $this->assertSame(['request', 'query', 'job', 'user'], $expected);
    }

    public function testBothJobTypesWriteOnceInOneSlot(): void
    {
        // A handler reached from two types locks its tables at two positions, and
        // then a web host (queued-job) and a queue host (job-attempt) invert
        // against anything written between them. writeJobs was the only such
        // handler, and nightowl_job_rollups / nightowl_user_job_rollups were among
        // the relations github#9 named.
        $ordered = (new ReflectionMethod(RecordWriter::class, 'orderedForWrite'))->invoke(null, [
            'job-attempt' => [['t' => 'job-attempt', 'n' => 1]],
            'cache-event' => [['t' => 'cache-event']],
            'queued-job' => [['t' => 'queued-job', 'n' => 2]],
        ]);

        // One slot, written once, before cache-event in both hosts' batches.
        $this->assertSame(['job', 'cache-event'], array_keys($ordered));
        $this->assertCount(2, $ordered['job'], 'neither job record may be dropped');

        // The web host and the queue host agree on where jobs sit relative to
        // cache events — which is the property that makes the cycle impossible.
        $web = $this->order(['queued-job' => [['t' => 'queued-job']], 'cache-event' => [['t' => 'cache-event']]]);
        $queue = $this->order(['cache-event' => [['t' => 'cache-event']], 'job-attempt' => [['t' => 'job-attempt']]]);
        $this->assertSame(['job', 'cache-event'], $web);
        $this->assertSame(['job', 'cache-event'], $queue);
    }

    /**
     * The invariant the whole fix rests on: whatever types a batch carries, its
     * write sequence is a SUBSEQUENCE of the full one. Subsequences of a total
     * order cannot invert against each other, so no two agents can take two
     * shared tables in opposite orders.
     */
    public function testEveryTypeSubsetIsASubsequenceOfTheFullOrder(): void
    {
        $all = ['request', 'query', 'exception', 'command', 'queued-job', 'cache-event',
            'mail', 'notification', 'outgoing-request', 'scheduled-task', 'job-attempt',
            'log', 'user'];
        $full = $this->order(array_fill_keys($all, [['t' => 'x']]));

        // 2^13 subsets is overkill; a deterministic sample of 500 is not.
        mt_srand(20260916);
        for ($i = 0; $i < 500; $i++) {
            $subset = array_values(array_filter($all, static fn () => mt_rand(0, 1) === 1));
            if ($subset === []) {
                continue;
            }
            shuffle($subset); // arrival order, which must not matter
            $got = $this->order(array_fill_keys($subset, [['t' => 'x']]));
            $this->assertSame(
                array_values(array_intersect($full, $got)),
                $got,
                'not a subsequence: '.implode(',', $subset),
            );
        }
    }

    public function testASubsetKeepsTheRelativeOrderOfTheWhole(): void
    {
        // Two agents rarely carry the same types. Consistency has to survive that:
        // a subset must be a subsequence of the full order, or two batches sharing
        // only some types could still invert them.
        $full = $this->order(array_fill_keys(
            ['request', 'query', 'exception', 'command', 'queued-job', 'cache-event',
                'mail', 'notification', 'outgoing-request', 'scheduled-task',
                'job-attempt', 'log', 'user'],
            [['t' => 'x']],
        ));

        foreach ([['user', 'request'], ['log', 'query', 'exception'], ['mail', 'cache-event']] as $subset) {
            $got = $this->order(array_fill_keys($subset, [['t' => 'x']]));
            $this->assertSame(
                array_values(array_intersect($full, $got)),
                $got,
                'subset '.implode(',', $subset).' is not a subsequence of the full order',
            );
        }
    }

    /**
     * The guard for the invariant everything else rests on.
     *
     * One handler reached from two match arms runs at two positions and locks its
     * tables at two positions, which is the cycle this fix removed. Nothing in the
     * language stops someone re-adding that, so assert it against the source: every
     * handler appears in exactly one arm, and every arm's slot is a slot
     * WRITE_ORDER positions. A new type sharing a handler belongs in WRITE_SLOTS.
     */
    public function testNoHandlerIsReachedFromMoreThanOneWriteSlot(): void
    {
        $src = file_get_contents((new \ReflectionClass(RecordWriter::class))->getFileName());

        $this->assertSame(1, preg_match(
            '/foreach \(self::orderedForWrite\(.*?\n            \}\n/s', $src, $m
        ), 'could not find the dispatch loop — this guard needs updating');

        preg_match_all("/'([a-z-]+)' => \\\$this->(\w+)\(/", $m[0], $arms, PREG_SET_ORDER);
        $this->assertNotEmpty($arms);

        $bySlot = [];
        $byHandler = [];
        foreach ($arms as [, $slot, $handler]) {
            $bySlot[$slot][] = $handler;
            $byHandler[$handler][] = $slot;
        }

        foreach ($byHandler as $handler => $slots) {
            $this->assertCount(
                1, $slots,
                "{$handler} is reached from ".count($slots).' slots ('.implode(', ', $slots).'). '
                    .'Route the extra types through WRITE_SLOTS instead, or they lock its tables at two positions.',
            );
        }

        // And every arm is actually positioned.
        $order = (new \ReflectionClass(RecordWriter::class))->getConstant('WRITE_ORDER');
        foreach (array_keys($bySlot) as $slot) {
            $this->assertContains($slot, $order, "slot '{$slot}' has no position in WRITE_ORDER");
        }

        // Every WRITE_SLOTS target must be a real slot too.
        foreach ((new \ReflectionClass(RecordWriter::class))->getConstant('WRITE_SLOTS') as $type => $slot) {
            $this->assertContains($slot, $order, "WRITE_SLOTS maps '{$type}' to unpositioned slot '{$slot}'");
            $this->assertNotContains($type, $order, "'{$type}' is merged into '{$slot}' and must not also hold a position");
        }
    }

    public function testUnknownTypesSortLastAndKeepTheirRecords(): void
    {
        // A type WRITE_ORDER does not name writes nothing (doWrite's default arm),
        // so it takes no lock and cannot deadlock — but it must not be dropped.
        $grouped = ['future-type' => [['t' => 'future-type']], 'request' => [['t' => 'request']]];

        $ordered = (new ReflectionMethod(RecordWriter::class, 'orderedForWrite'))->invoke(null, $grouped);

        $this->assertSame(['request', 'future-type'], array_keys($ordered));
        $this->assertSame($grouped['future-type'], $ordered['future-type']);
        $this->assertSame($grouped['request'], $ordered['request']);
    }

    /**
     * @param  list<string>  $items
     * @return list<list<string>>
     */
    private function permutations(array $items): array
    {
        if (count($items) <= 1) {
            return [$items];
        }
        $out = [];
        foreach ($items as $i => $item) {
            $rest = $items;
            unset($rest[$i]);
            foreach ($this->permutations(array_values($rest)) as $p) {
                $out[] = [$item, ...$p];
            }
        }

        return $out;
    }
}
