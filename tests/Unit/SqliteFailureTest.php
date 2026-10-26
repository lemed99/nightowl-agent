<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\SqliteFailure;
use PDO;
use PDOException;
use PHPUnit\Framework\TestCase;

/**
 * The codes come from real pdo_sqlite failures, not hand-built exceptions, so a
 * change in where PDO puts the result code fails here rather than in the field.
 */
class SqliteFailureTest extends TestCase
{
    private string $path;

    protected function setUp(): void
    {
        $this->path = sys_get_temp_dir().'/nightowl_sqlite_failure_'.uniqid().'.sqlite';
    }

    protected function tearDown(): void
    {
        foreach ([$this->path, $this->path.'-wal', $this->path.'-shm', $this->path.'.ckpt', $this->path.'.ckpt-gate'] as $f) {
            @unlink($f);
        }
    }

    private function buffer(): PDO
    {
        $pdo = new PDO("sqlite:{$this->path}");
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->exec('PRAGMA journal_mode=WAL');
        $pdo->exec('CREATE TABLE buffer (id INTEGER PRIMARY KEY, payload TEXT)');

        return $pdo;
    }

    private function failureOf(callable $op): \Throwable
    {
        try {
            $op();
        } catch (\Throwable $e) {
            return $e;
        }
        $this->fail('expected the operation to fail');
    }

    public function testReadOnlyBufferReportsReadonly(): void
    {
        $pdo = $this->buffer();
        $insert = $pdo->prepare('INSERT INTO buffer (payload) VALUES (:p)');
        $pdo->exec('PRAGMA query_only = ON');

        $e = $this->failureOf(fn () => $insert->execute(['p' => 'x']));

        $this->assertSame(SqliteFailure::READONLY, SqliteFailure::code($e));
    }

    /** max_page_count makes SQLite raise the genuine SQLITE_FULL a full disk does. */
    public function testFullBufferReportsFull(): void
    {
        $pdo = $this->buffer();
        $insert = $pdo->prepare('INSERT INTO buffer (payload) VALUES (:p)');
        $pdo->exec('PRAGMA max_page_count = 3');

        $e = $this->failureOf(function () use ($insert) {
            for ($i = 0; $i < 100; $i++) {
                $insert->execute(['p' => str_repeat('x', 2000)]);
            }
        });

        $this->assertSame(SqliteFailure::FULL, SqliteFailure::code($e));
        $this->assertSame('database or disk is full', SqliteFailure::describe(SqliteFailure::code($e)));
    }

    public function testExtendedCodeMapsToItsPrimaryCode(): void
    {
        $e = new PDOException('SQLSTATE[HY000]: General error: 778 disk I/O error');
        $e->errorInfo = ['HY000', 778, 'disk I/O error']; // SQLITE_IOERR_WRITE

        $this->assertSame(SqliteFailure::IOERR, SqliteFailure::code($e));
    }

    /** errorInfo is read on its own, not only through the message fallback. */
    public function testCodeIsReadFromErrorInfo(): void
    {
        $e = new PDOException('no code in this text');
        $e->errorInfo = ['HY000', 13, 'database or disk is full'];

        $this->assertSame(SqliteFailure::FULL, SqliteFailure::code($e));
    }

    public function testCodeIsReadFromTheMessageWhenErrorInfoIsEmpty(): void
    {
        $e = new PDOException('SQLSTATE[HY000]: General error: 11 database disk image is malformed');

        $this->assertSame(SqliteFailure::CORRUPT, SqliteFailure::code($e));
    }

    public function testAThrowableWithoutACodeIsZero(): void
    {
        $this->assertSame(0, SqliteFailure::code(new \RuntimeException('boom')));
        $this->assertSame('unknown error', SqliteFailure::describe(0));
    }

    /** MISUSE describes the statement, not the file (Yomoney: ingest 21, drain 11). */
    public function testMisuseAndUnknownSayNothingAboutTheFile(): void
    {
        $this->assertFalse(SqliteFailure::isInformative(SqliteFailure::MISUSE));
        $this->assertFalse(SqliteFailure::isInformative(0));
        $this->assertTrue(SqliteFailure::isInformative(SqliteFailure::CORRUPT));
        $this->assertTrue(SqliteFailure::isInformative(SqliteFailure::FULL));
    }
}
