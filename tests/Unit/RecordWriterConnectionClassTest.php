<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\RecordWriter;
use PHPUnit\Framework\TestCase;

/**
 * isConnectionError must recognize the full PG connection-failure CLASS — not just a
 * substring list — so the drain worker surfaces DRAIN_UNREACHABLE (and defers rather
 * than quarantining good telemetry) when the DB is unreachable or rejects the
 * connection: wrong creds / bad dbname / pg_hba / DNS / timeout. That connect-phase
 * failure (libpq SQLSTATE 08006) is the #1 first-run failure mode, and before this it
 * was classified as "not a connection error" → the agent reported healthy.
 */
class RecordWriterConnectionClassTest extends TestCase
{
    private function isConnError(\Throwable $e, ?string $sqlstate = null): bool
    {
        // Constructor is lazy (connects on first write), so bad creds here never dial out.
        $writer = new RecordWriter('127.0.0.1', 5432, 'x', 'x', 'x');
        $m = new \ReflectionMethod($writer, 'isConnectionError');

        return (bool) $m->invoke($writer, $e, $sqlstate);
    }

    public function testDefiniteNonConnectionSqlStateShortCircuitsBeforeMessageScan(): void
    {
        // The COPY path captures the real SQLSTATE separately and passes a RuntimeException
        // whose message echoes the offending CUSTOMER ROW VALUE — which for a monitoring
        // agent ingesting other apps' telemetry routinely IS connection-error text. A
        // definite non-08 SQLSTATE must short-circuit to FALSE before the message is
        // scanned, or a poison row head-of-line-blocks the drain forever as "unreachable".
        // Reverting the non-08 short-circuit makes these match the substrings → fail.
        $poison = new \RuntimeException(
            'COPY into nightowl_logs failed: ERROR: invalid input syntax for type bigint: '
            .'"could not connect to server" CONTEXT: COPY nightowl_logs, line 1, column duration'
        );
        $this->assertFalse($this->isConnError($poison, '22P02'));

        $this->assertFalse($this->isConnError(
            new \RuntimeException('DETAIL: Failing row contains (..., "Operation timed out", ...).'),
            '23502'
        ));
        $this->assertFalse($this->isConnError(
            new \RuntimeException('Key (msg)=(no route to host) already exists.'),
            '23505'
        ));

        // A genuine connect failure still classifies true via the explicit code.
        $this->assertTrue($this->isConnError(new \RuntimeException('whatever'), '08006'));
    }

    public function testRecognizesSqlState08ConnectionClass(): void
    {
        // libpq stamps connect-phase failures — including wrong password (28P01) and bad
        // dbname (3D000) — as SQLSTATE 08006, echoed in the message.
        $this->assertTrue($this->isConnError(new \PDOException(
            'SQLSTATE[08006] [2] connection to server at "db" (10.0.0.1), port 5432 failed: FATAL: password authentication failed for user "x"'
        )));
        $this->assertTrue($this->isConnError(new \PDOException(
            'SQLSTATE[08006] [2] connection to server failed: FATAL: database "wrong" does not exist'
        )));
    }

    public function testRecognizesOsLevelConnectFailures(): void
    {
        foreach ([
            'could not translate host name "db" to address: Name or service not known',
            'no route to host',
            'host is unreachable',
            'connection refused',
            'operation timed out',
            'could not connect to server',
        ] as $msg) {
            $this->assertTrue($this->isConnError(new \PDOException($msg)), $msg);
        }
    }

    public function testDataAndConfigWriteErrorsAreNotConnectionErrors(): void
    {
        // These are write rejections (DRAIN_WRITE_FAILING / quarantine), NOT connection
        // failures — they must stay false so they aren't mislabeled "unreachable".
        $this->assertFalse($this->isConnError(new \PDOException('SQLSTATE[22001] value too long for type character varying(255)')));
        $this->assertFalse($this->isConnError(new \PDOException('SQLSTATE[42P01] relation "nightowl_requests" does not exist')));
        $this->assertFalse($this->isConnError(new \PDOException('SQLSTATE[23505] duplicate key value violates unique constraint "x"')));
    }
}
