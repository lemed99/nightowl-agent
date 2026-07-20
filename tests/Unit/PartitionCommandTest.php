<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Commands\PartitionCommand;
use NightOwl\Support\ConversionInProgressException;
use NightOwl\Support\PoolerAffinityException;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command as SymfonyCommand;

final class PartitionCommandTest extends TestCase
{
    /**
     * BUSY = 2 was a silent alias for Symfony's inherited Command::INVALID
     * ("the command was invoked incorrectly"). PHP warns about neither the
     * collision nor a test asserting Command::INVALID that a healthy contended
     * run would satisfy.
     */
    public function test_busy_is_distinguishable_from_symfony_invalid(): void
    {
        $this->assertNotSame(SymfonyCommand::INVALID, PartitionCommand::BUSY);
        $this->assertNotSame(SymfonyCommand::SUCCESS, PartitionCommand::BUSY);
        $this->assertNotSame(SymfonyCommand::FAILURE, PartitionCommand::BUSY);
    }

    /**
     * A pooler abort is connection-wide, contention is per-table, and the
     * command's catch order depends on neither being reachable through the
     * other's clause.
     */
    public function test_a_pooler_abort_is_typed_separately_from_contention(): void
    {
        $this->assertTrue(is_subclass_of(PoolerAffinityException::class, \RuntimeException::class));
        $this->assertFalse(is_subclass_of(PoolerAffinityException::class, ConversionInProgressException::class));
        $this->assertFalse(is_subclass_of(ConversionInProgressException::class, PoolerAffinityException::class));
    }

    /**
     * The whole point of a code beyond SUCCESS/FAILURE is that a caller can tell
     * outcomes apart; a fourth that collides with an existing one tells it
     * nothing. 4 follows BUSY = 3 because 2 is Symfony's inherited INVALID.
     */
    public function test_incomplete_is_distinguishable_from_every_other_exit_code(): void
    {
        $this->assertNotSame(SymfonyCommand::SUCCESS, PartitionCommand::INCOMPLETE);
        $this->assertNotSame(SymfonyCommand::FAILURE, PartitionCommand::INCOMPLETE);
        $this->assertNotSame(SymfonyCommand::INVALID, PartitionCommand::INCOMPLETE);
        $this->assertNotSame(PartitionCommand::BUSY, PartitionCommand::INCOMPLETE);
    }

    /**
     * The ladder used to map only $failed and $busy, so a run whose conversions
     * landed while every child window failed printed "Done… no restart needed"
     * and exited SUCCESS — indistinguishable, to a deploy pipeline gating on the
     * status, from a run that finished the job.
     */
    public function test_a_run_that_converted_but_owes_children_does_not_exit_success(): void
    {
        $code = PartitionCommand::exitCode([], [], ['nightowl_queries_p20260101: lock timeout']);

        $this->assertSame(PartitionCommand::INCOMPLETE, $code);
        $this->assertNotSame(SymfonyCommand::SUCCESS, $code);
    }

    /**
     * Precedence. A real failure needs an operator, not a retry, so it outranks
     * both. Contention outranks gaps because ITS tables are not converted at all
     * and the re-run it asks for closes the gaps on the way past.
     */
    public function test_the_ladder_reports_the_most_severe_outcome(): void
    {
        $this->assertSame(
            SymfonyCommand::FAILURE,
            PartitionCommand::exitCode(['nightowl_logs'], ['nightowl_jobs'], ['nightowl_queries_p20260101: x']),
        );
        $this->assertSame(
            PartitionCommand::BUSY,
            PartitionCommand::exitCode([], ['nightowl_jobs'], ['nightowl_queries_p20260101: x']),
        );
        $this->assertSame(SymfonyCommand::SUCCESS, PartitionCommand::exitCode([], [], []));
    }
}
