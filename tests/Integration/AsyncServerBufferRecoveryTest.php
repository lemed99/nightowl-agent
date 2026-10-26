<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\AsyncServer;
use NightOwl\Agent\BufferRecovery;
use NightOwl\Agent\DrainWorker;
use NightOwl\Agent\MetricsCollector;
use NightOwl\Agent\PayloadParser;
use NightOwl\Agent\RecordWriter;
use NightOwl\Agent\SqliteBuffer;
use NightOwl\Support\SqliteFailure;
use NightOwl\Tests\Support\CorruptBufferFixture;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use ReflectionProperty;

/**
 * The agent repairs a corrupt buffer by itself, from the drain's error to a
 * working buffer: a real damaged file, a real DrainWorker failing on it, its
 * real metrics file, read by the real collector inside a real AsyncServer.
 *
 * Yomoney's agent sat on "database disk image is malformed" for most of a day
 * (2026-10-21) because nothing acted on it. The file only had a broken index,
 * and every one of the 100,043 waiting rows could have been kept.
 */
class AsyncServerBufferRecoveryTest extends TestCase
{
    private string $dir;

    private string $path;

    protected function setUp(): void
    {
        $this->dir = sys_get_temp_dir().'/nightowl-async-recovery-'.getmypid().'-'.uniqid();
        mkdir($this->dir);
        $this->path = $this->dir.'/agent-buffer.sqlite';
    }

    protected function tearDown(): void
    {
        foreach (glob($this->dir.'/*') ?: [] as $f) {
            @unlink($f);
        }
        @rmdir($this->dir);
    }

    private function server(): AsyncServer
    {
        return new AsyncServer(
            parser: new PayloadParser,
            sqlitePath: $this->path,
            drainWorker: $this->worker(),
            healthEnabled: false,
            healthReportEnabled: false,
        );
    }

    private function worker(): DrainWorker
    {
        // Port 1: nothing listens. Two workers, so the drain claims with an
        // UPDATE, which is what trips over the broken index.
        $worker = new DrainWorker(
            sqlitePath: $this->path,
            pgHost: '127.0.0.1',
            pgPort: 1,
            pgDatabase: 'none',
            pgUsername: 'none',
            pgPassword: 'none',
        );
        $worker->setWorkerConfig(0, 2);

        return $worker;
    }

    private function metrics(AsyncServer $server): MetricsCollector
    {
        return (new ReflectionProperty($server, 'metrics'))->getValue($server);
    }

    /** Run one drain batch on the buffer and publish the worker's metrics file, as a live worker would. */
    private function drainOnce(): void
    {
        $worker = $this->worker();
        $buffer = new SqliteBuffer($this->path);
        (new ReflectionMethod($worker, 'drainBatch'))->invoke(
            $worker,
            $buffer,
            new RecordWriter('127.0.0.1', 1, 'none', 'none', 'none', storageV2Config: false),
        );
        (new ReflectionMethod($worker, 'writeDrainMetrics'))->invoke($worker);
    }

    /** @return array<string, array<string, mixed>> */
    private function diagnoses(MetricsCollector $metrics, int $pending): array
    {
        $metrics->runDiagnosis(false, $pending, 0, 0);
        $metrics->runDiagnosis(false, $pending, 0, 0);

        return array_column($metrics->getFullStatus(microtime(true) - 60, false, $pending, 0, 0)['diagnoses'], null, 'code');
    }

    public function test_a_drain_failing_on_a_broken_index_gets_its_buffer_reindexed(): void
    {
        CorruptBufferFixture::indexOnly($this->path);
        $server = $this->server();
        $metrics = $this->metrics($server);
        (new ReflectionMethod($server, 'openBuffer'))->invoke($server);

        $this->drainOnce();
        $metrics->readDrainMetrics($this->path, 2);
        $this->assertGreaterThan(0.0, $metrics->bufferCorruptionSignalAt(), 'the drain reported the corruption');

        (new ReflectionMethod($server, 'recoverBufferIfCorrupt'))->invoke($server);

        $this->assertSame([], BufferRecovery::integrityProblems($this->path));
        $this->assertSame([], glob($this->path.'.corrupt-*'));
        $this->assertSame(180, (new SqliteBuffer($this->path))->pendingCount(), 'every row still waiting survived');
        $this->assertNotNull((new ReflectionProperty($server, 'buffer'))->getValue($server), 'the parent reopened its buffer');

        // The stopped worker's metrics file still carries its error until a new
        // worker overwrites it, and the parent keeps reading it meanwhile.
        $metrics->readDrainMetrics($this->path, 2);
        $this->assertSame(0.0, $metrics->bufferCorruptionSignalAt(), 'an error from before the repair starts no second one');
        $diagnoses = $this->diagnoses($metrics, 180);
        $this->assertArrayHasKey('BUFFER_REPAIRED', $diagnoses);
        $this->assertStringContainsString('no buffered telemetry was lost', $diagnoses['BUFFER_REPAIRED']['message']);
        $this->assertArrayNotHasKey('BUFFER_UNWRITABLE', $diagnoses, 'the error that started the recovery is history');

        // And the drain gets past the buffer again: it claims rows instead of failing.
        $this->drainOnce();
        $metrics->readDrainMetrics($this->path, 2);
        $this->assertSame(0.0, $metrics->bufferCorruptionSignalAt());
    }

    public function test_a_buffer_damaged_beyond_repair_is_replaced_and_reported(): void
    {
        CorruptBufferFixture::tableDamaged($this->path);
        $server = $this->server();
        $metrics = $this->metrics($server);

        // The parent's own read hits the damage, as Yomoney's ingest did on Oct 15.
        $e = new \PDOException('SQLSTATE[HY000]: General error: 11 database disk image is malformed');
        $e->errorInfo = ['HY000', SqliteFailure::CORRUPT, 'database disk image is malformed'];
        (new ReflectionMethod($server, 'recordBufferWriteFailure'))->invoke($server, $e);

        (new ReflectionMethod($server, 'recoverBufferIfCorrupt'))->invoke($server);

        $copies = array_values(array_filter(glob($this->path.'.corrupt-*') ?: [], fn ($f) => ! preg_match('/-(wal|shm)$/', $f)));
        $this->assertCount(1, $copies);

        // The parent is on a fresh, working buffer.
        (new ReflectionMethod($server, 'appendToBuffer'))->invoke($server, '[{"t":"request"}]');
        $this->assertSame(1, (new SqliteBuffer($this->path))->pendingCount());

        $diagnoses = $this->diagnoses($metrics, 1);
        $this->assertArrayHasKey('BUFFER_REPAIRED', $diagnoses);
        $this->assertStringContainsString(basename($copies[0]), $diagnoses['BUFFER_REPAIRED']['message']);
        $this->assertStringNotContainsString($this->dir, $diagnoses['BUFFER_REPAIRED']['message'], 'the path stays on the host');
    }

    /**
     * The mark that fails on a broken index comes after the rows are already
     * in PostgreSQL. Recovery stops the worker that holds their ids, so the
     * ids go through a file, and the parent marks the rows once the index is
     * rebuilt. Otherwise every recovery drains up to a batch per worker twice.
     */
    public function test_rows_a_worker_already_wrote_are_not_drained_again(): void
    {
        CorruptBufferFixture::indexOnly($this->path);
        $server = $this->server();
        (new ReflectionMethod($server, 'openBuffer'))->invoke($server);

        // Worker 0 committed rows 1-60 to PostgreSQL, then could not mark them.
        $worker = $this->worker();
        try {
            (new ReflectionMethod($worker, 'markCommitted'))->invoke($worker, new SqliteBuffer($this->path), range(1, 60));
            $this->fail('the mark should fail on the broken index');
        } catch (\PDOException $e) {
            $this->assertSame(SqliteFailure::CORRUPT, SqliteFailure::code($e));
        }
        $this->assertFileExists(DrainWorker::unmarkedIdsPath($this->path, 0));

        // A list from some other file (a buffer since replaced): never applied.
        file_put_contents(DrainWorker::unmarkedIdsPath($this->path, 1), json_encode(['inode' => 1, 'ids' => range(61, 70)]));

        $e = new \PDOException('SQLSTATE[HY000]: General error: 11 database disk image is malformed');
        $e->errorInfo = ['HY000', SqliteFailure::CORRUPT, 'database disk image is malformed'];
        (new ReflectionMethod($server, 'recordBufferWriteFailure'))->invoke($server, $e);
        (new ReflectionMethod($server, 'recoverBufferIfCorrupt'))->invoke($server);

        $this->assertSame([], BufferRecovery::integrityProblems($this->path));
        $pending = array_column((new SqliteBuffer($this->path))->fetchPending(1000), 'id');
        $this->assertSame([], array_intersect(range(51, 60), $pending), 'rows already in PostgreSQL are marked');
        $this->assertSame(range(61, 70), array_values(array_intersect(range(61, 70), $pending)), 'a list from another file is ignored');
        $this->assertCount(170, $pending);
        $this->assertSame([], glob(DrainWorker::unmarkedIdsPattern($this->path)), 'every list is used up');
    }

    /**
     * Lists a previous run's workers left. Rows of this very file that they
     * wrote to PostgreSQL are marked at startup instead of being drained a
     * second time, and no list outlives the start: a buffer deleted by hand and
     * recreated can get the same inode back, and a list still on disk at the
     * next recovery would then mark the new file's rows with those ids synced,
     * telemetry that never reached PostgreSQL.
     */
    public function test_startup_marks_a_previous_runs_written_rows_and_keeps_no_list(): void
    {
        $buffer = new SqliteBuffer($this->path);
        for ($i = 0; $i < 10; $i++) {
            $buffer->appendRaw('[{"t":"request","n":'.$i.'}]');
        }
        $buffer->claimBatch(0, 5);
        unset($buffer);

        file_put_contents(DrainWorker::unmarkedIdsPath($this->path, 0), json_encode(['inode' => fileinode($this->path), 'ids' => range(1, 5)]));
        file_put_contents(DrainWorker::unmarkedIdsPath($this->path, 3), json_encode(['inode' => fileinode($this->path) + 1, 'ids' => range(6, 10)]));

        (new ReflectionMethod($this->server(), 'releaseStaleClaims'))->invoke($this->server());

        $pending = array_map('intval', array_column((new SqliteBuffer($this->path))->fetchPending(100), 'id'));
        $this->assertSame(range(6, 10), $pending, 'rows 1-5 were in PostgreSQL; a list from another file marks nothing');
        $this->assertSame([], glob(DrainWorker::unmarkedIdsPattern($this->path)), 'no list survives the start');
    }

    public function test_a_recovery_does_not_repeat_within_the_cooldown(): void
    {
        $server = $this->server();
        (new ReflectionMethod($server, 'openBuffer'))->invoke($server);
        $e = new \PDOException('SQLSTATE[HY000]: General error: 11 database disk image is malformed');
        $e->errorInfo = ['HY000', SqliteFailure::CORRUPT, 'database disk image is malformed'];
        $refuse = new ReflectionMethod($server, 'recordBufferWriteFailure');
        $recover = new ReflectionMethod($server, 'recoverBufferIfCorrupt');
        $lastRecovery = new ReflectionProperty($server, 'lastBufferRecoveryAt');

        $refuse->invoke($server, $e);
        $recover->invoke($server);
        $first = $lastRecovery->getValue($server);
        $this->assertGreaterThan(0.0, $first, 'a fresh corruption error starts a recovery');

        // The file was fine (CLEAN), and the error comes again at once.
        usleep(10_000);
        $refuse->invoke($server, $e);
        $recover->invoke($server);

        $this->assertSame($first, $lastRecovery->getValue($server), 'a second check inside the cooldown');
    }
}
