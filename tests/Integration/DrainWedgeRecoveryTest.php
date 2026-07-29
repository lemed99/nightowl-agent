<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\RecordWriter;
use NightOwl\Simulator\NightwatchSimulator;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Recovery from a drain connection stranded inside a transaction.
 *
 * Found by a real load test, not by reasoning: with PostgreSQL stopped for 90s
 * the drain wedged PERMANENTLY. It kept throwing "DictionaryCache::warm must run
 * outside the batch transaction" ~15 times a second, burned ~30% of a core, and
 * was still doing it — draining nothing — 60s after PostgreSQL came back. Only a
 * process restart cleared it, so a transient outage cost every buffered row past
 * the buffer's retention.
 *
 * The mechanism is a PDO behaviour these tests pin first, because everything
 * downstream assumes it: when the backend dies under an open transaction,
 * rollBack() THROWS and PDO leaves inTransaction() answering true. The
 * swallow-and-continue maintenance paths all guarded that rollBack — which stops
 * the throw but does not release the flag — and then returned normally, so
 * write()'s reconnect+retry never learned the handle was finished with.
 */
class DrainWedgeRecoveryTest extends TestCase
{
    private static ?PDO $pdo = null;

    private static string $host;

    private static int $port;

    private static string $database;

    private static string $username;

    private static string $password;

    private NightwatchSimulator $sim;

    public static function setUpBeforeClass(): void
    {
        self::$host = getenv('NIGHTOWL_TEST_DB_HOST') ?: '127.0.0.1';
        self::$port = (int) (getenv('NIGHTOWL_TEST_DB_PORT') ?: 5432);
        self::$database = getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test';
        self::$username = getenv('NIGHTOWL_TEST_DB_USERNAME') ?: 'nightowl_test';
        self::$password = getenv('NIGHTOWL_TEST_DB_PASSWORD') ?: 'test123';

        try {
            $dsn = sprintf('pgsql:host=%s;port=%d;dbname=%s', self::$host, self::$port, self::$database);
            self::$pdo = new PDO($dsn, self::$username, self::$password);
            self::$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (\Exception) {
            self::$pdo = null;
        }

        if (self::$pdo) {
            MigrationRunner::migrate(self::$host, self::$port, self::$database, self::$username, self::$password);
        }
    }

    protected function setUp(): void
    {
        if (self::$pdo === null) {
            $this->markTestSkipped('PostgreSQL not available. Set NIGHTOWL_TEST_DB_* env vars.');
        }

        foreach (['nightowl_requests', 'nightowl_requests_v2', 'nightowl_dict_string', 'nightowl_dict_route'] as $t) {
            self::$pdo->exec("DELETE FROM {$t}");
        }

        $this->sim = new NightwatchSimulator('test-token');
    }

    public static function tearDownAfterClass(): void
    {
        self::$pdo = null;
    }

    // ----------------------------------------------------------- the premise

    /**
     * The behaviour the whole fix rests on. If a future PHP/pdo_pgsql clears the
     * transaction flag on a failed rollBack, the recovery paths below become dead
     * code — and this test is what says so instead of them silently rotting.
     */
    public function test_pdo_keeps_reporting_a_transaction_after_the_backend_dies(): void
    {
        $pdo = self::freshPdo();
        $pdo->beginTransaction();
        self::killOwnBackend($pdo);

        $threw = false;
        try {
            $pdo->rollBack();
        } catch (\Throwable) {
            $threw = true;
        }

        $this->assertTrue($threw, 'rollBack() on a dead backend is expected to throw');
        $this->assertTrue($pdo->inTransaction(), 'PDO is expected to leave the transaction flag set');

        // And this is the line the load test surfaced in the agent's own log.
        try {
            $pdo->beginTransaction();
            $this->fail('beginTransaction() on the stranded handle should have thrown');
        } catch (\Throwable $e) {
            $this->assertStringContainsString('already an active transaction', $e->getMessage());
        }
    }

    // ------------------------------------------------------- the wedge itself

    /**
     * The reproduction. A writer holding a stranded handle must still drain the
     * next batch. Before the fix this threw the DictionaryCache assertion — a
     * RuntimeException, so not a connection error, so write() never reconnected
     * and every subsequent batch threw it too, forever.
     *
     * Runs in v2 mode deliberately: v2 is the production default and the dict
     * warm is where the assertion lives.
     *
     * The healthy write first is load-bearing, not scene-setting. v2Enabled()
     * probes once and caches only a SUCCESSFUL probe, so a writer stranded before
     * it has ever connected probes the dead handle, fails open to v1, and never
     * reaches the dict warm at all — the test would pass against the unfixed code.
     * The field agent had been draining for hours before the outage. Match it.
     */
    public function test_write_recovers_from_a_stranded_transaction(): void
    {
        $writer = self::writer(storageV2: true);
        $writer->write([$this->sim->makeRequest(['trace_id' => self::uuid(), 'status_code' => 200])]);

        self::strandWriterTransaction($writer);

        $writer->write([$this->sim->makeRequest(['trace_id' => self::uuid(), 'status_code' => 200])]);

        $this->assertSame(
            2,
            (int) self::$pdo->query('SELECT COUNT(*) FROM nightowl_requests_v2')->fetchColumn(),
            'the post-outage batch should have landed on a fresh connection'
        );
    }

    /** Same recovery on the v1 path, where the dict warm is not in play at all. */
    public function test_write_recovers_from_a_stranded_transaction_on_v1(): void
    {
        $writer = self::writer(storageV2: false);
        $writer->write([$this->sim->makeRequest(['trace_id' => 'wedge-v1-warm', 'status_code' => 200])]);

        self::strandWriterTransaction($writer);

        $writer->write([$this->sim->makeRequest(['trace_id' => 'wedge-v1', 'status_code' => 200])]);

        $this->assertSame(
            1,
            (int) self::$pdo->query("SELECT COUNT(*) FROM nightowl_requests WHERE trace_id = 'wedge-v1'")->fetchColumn(),
        );
    }

    // ------------------------------------------------- the swallowing sources

    /**
     * @return array<string, array{string, list<mixed>}>
     */
    public static function swallowingMaintenancePaths(): array
    {
        return [
            'partition maintenance' => ['maintainRawPartitions', []],
            'partition leftover sweep' => ['healRawPartitionLeftovers', []],
            'concurrency rollup' => ['maintainConcurrencyRollup', [1_700_000_000]],
            'worker saturation' => ['checkWorkerSaturation', []],
        ];
    }

    /**
     * Each of these catches its own failure and returns normally, so each is the
     * end of the line: if it hands back a handle it could not release, nothing
     * downstream will ever release it. They are the paths that CREATED the wedge
     * in the field — the drain batch guard is the backstop, not the fix.
     *
     * @param  list<mixed>  $args
     */
    #[DataProvider('swallowingMaintenancePaths')]
    public function test_maintenance_paths_do_not_hand_back_a_stranded_handle(string $method, array $args): void
    {
        $writer = self::writer(storageV2: true);
        // Warm the writer's cached probes on a healthy connection first, for the
        // same reason as the write tests: a cold writer answers every probe off
        // the dead handle and returns early, well before the code under test.
        $writer->write([$this->sim->makeRequest(['trace_id' => self::uuid(), 'status_code' => 200])]);

        self::strandWriterTransaction($writer);

        // Must not throw — the cleanup tick is one shared try and these are
        // documented as never costing it a WAL checkpoint.
        $writer->{$method}(...$args);

        $prop = new \ReflectionProperty($writer, 'pdo');
        $held = $prop->getValue($writer);

        $this->assertTrue(
            $held === null || ! $held->inTransaction(),
            "{$method}() left a stranded handle in place"
        );
    }

    // ------------------------------------------------------------- machinery

    private static function writer(bool $storageV2): RecordWriter
    {
        return new RecordWriter(
            self::$host, self::$port, self::$database, self::$username, self::$password,
            storageV2Config: $storageV2,
        );
    }

    private static function freshPdo(): PDO
    {
        $dsn = sprintf('pgsql:host=%s;port=%d;dbname=%s', self::$host, self::$port, self::$database);

        return new PDO($dsn, self::$username, self::$password, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    }

    /**
     * Reproduce what a PostgreSQL restart does to an in-flight batch: kill the
     * backend under an open transaction, then take the same guarded rollBack the
     * agent takes. What is left is the stranded handle.
     */
    private static function strandWriterTransaction(RecordWriter $writer): void
    {
        /** @var PDO $pdo */
        $pdo = (new \ReflectionMethod($writer, 'pdo'))->invoke($writer);

        $pdo->beginTransaction();
        self::killOwnBackend($pdo);

        try {
            if ($pdo->inTransaction()) {
                $pdo->rollBack();
            }
        } catch (\Throwable) {
            // Exactly the swallow the maintenance paths used to do on their own.
        }

        if (! $pdo->inTransaction()) {
            self::fail('failed to strand the handle — the premise test explains what this needs');
        }
    }

    private static function killOwnBackend(PDO $pdo): void
    {
        try {
            $pdo->query('SELECT pg_terminate_backend(pg_backend_pid())');
        } catch (\Throwable) {
            // Terminating our own backend reports itself as a fatal error.
        }
    }

    private static function uuid(): string
    {
        $b = random_bytes(16);
        $b[6] = chr((ord($b[6]) & 0x0F) | 0x40);
        $b[8] = chr((ord($b[8]) & 0x3F) | 0x80);

        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($b), 4));
    }
}
