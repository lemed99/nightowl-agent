<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\RecordWriter;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * applyTransactionGuards() is the drain's only defence against three distinct
 * failure modes (pooler durability leak, unbounded lock waits, orphaned
 * idle-in-transaction sessions), so its emitted SQL is asserted directly.
 *
 * The idle_in_transaction_session_timeout guard exists because an abandoned
 * batch's server-side session can survive behind a pooler holding uncommitted
 * unique-index entries, and the retry then dies on 55P03 against its own ghost
 * (observed live on nightowl_issues through Supavisor). SET LOCAL is sufficient:
 * the timeout only fires while idle inside a transaction — exactly SET LOCAL's
 * scope — and an orphan never commits, so the value governs it for life.
 */
final class RecordWriterTransactionGuardsTest extends TestCase
{
    /** @return array{0: PDO, 1: callable(): list<string>} */
    private function capturingPdo(): array
    {
        $pdo = new class extends PDO
        {
            /** @var list<string> */
            public array $execs = [];

            public function __construct() {}

            public function exec(string $statement): int|false
            {
                $this->execs[] = $statement;

                return 0;
            }
        };

        return [$pdo, fn (): array => $pdo->execs];
    }

    private function applyGuards(RecordWriter $writer, PDO $pdo): void
    {
        $m = new \ReflectionMethod(RecordWriter::class, 'applyTransactionGuards');
        $m->invoke($writer, $pdo);
    }

    public function testDefaultGuardsCarryAllThreeInOneRoundTrip(): void
    {
        $writer = new RecordWriter('127.0.0.1', 5432, 'x', 'x', 'x');
        [$pdo, $execs] = $this->capturingPdo();

        $this->applyGuards($writer, $pdo);

        // One exec — the guards must stay a single round trip; a cross-region
        // link pays per statement.
        $this->assertCount(1, $execs());
        $sql = $execs()[0];

        $this->assertStringContainsString('SET LOCAL synchronous_commit = off', $sql);
        $this->assertStringContainsString('SET LOCAL lock_timeout = 10000', $sql);
        $this->assertStringContainsString('SET LOCAL idle_in_transaction_session_timeout = 30000', $sql);
    }

    public function testZeroDisablesTheIdleTxnGuardAlone(): void
    {
        $writer = new RecordWriter('127.0.0.1', 5432, 'x', 'x', 'x', idleTxnTimeoutMs: 0);
        [$pdo, $execs] = $this->capturingPdo();

        $this->applyGuards($writer, $pdo);

        $sql = $execs()[0];
        $this->assertStringNotContainsString('idle_in_transaction_session_timeout', $sql);
        $this->assertStringContainsString('SET LOCAL lock_timeout = 10000', $sql);
    }

    public function testMasterSwitchOffLeavesOnlyThePoolerLeakFix(): void
    {
        $writer = new RecordWriter('127.0.0.1', 5432, 'x', 'x', 'x', timeoutsEnabled: false);
        [$pdo, $execs] = $this->capturingPdo();

        $this->applyGuards($writer, $pdo);

        // synchronous_commit scoping is a bug fix, not a tunable — it must
        // survive the master switch; both timeouts must not.
        $sql = $execs()[0];
        $this->assertStringContainsString('SET LOCAL synchronous_commit = off', $sql);
        $this->assertStringNotContainsString('lock_timeout', $sql);
        $this->assertStringNotContainsString('idle_in_transaction_session_timeout', $sql);
    }
}
