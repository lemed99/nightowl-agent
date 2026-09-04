<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\RecordWriter;
use NightOwl\Simulator\NightwatchSimulator;
use NightOwl\Support\StorageV2;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * Searchable log context: the write path and the fence that governs whether
 * the API may search it.
 *
 * The fence is the load-bearing part. Its invariant — every log row at or
 * after it carries plaintext context — is what lets the API offer a context
 * search at all, so the transitions that maintain it are what this pins:
 * opening on the first plaintext batch, CLOSING when a compressed batch is
 * written again, and staying put in the steady state.
 */
class LogContextSearchableTest extends TestCase
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

        self::$pdo->exec('DELETE FROM nightowl_logs_v2');
        self::$pdo->exec('DELETE FROM nightowl_dict_string');
        self::$pdo->exec("DELETE FROM nightowl_settings WHERE key = '".StorageV2::LOG_CONTEXT_FENCE_KEY."'");

        $this->sim = new NightwatchSimulator('test-token');
    }

    public function test_context_is_compressed_and_unfenced_by_default(): void
    {
        $this->writer(false)->write([$this->log(['order_id' => 8412])]);

        $row = $this->onlyLog();

        $this->assertNull($row['context'], 'context should be empty when the flag is off');
        $this->assertNotNull($row['context_z']);
        $this->assertNull($this->fence(), 'no fence may be open while contexts are compressed');
    }

    public function test_flag_stores_plaintext_and_opens_the_fence(): void
    {
        $this->writer(true)->write([$this->log(['order_id' => 8412])]);

        $row = $this->onlyLog();

        $this->assertSame(json_encode(['order_id' => 8412]), $row['context']);
        $this->assertNull($row['context_z']);
        $this->assertNotNull($this->fence());
    }

    /** The plaintext must be matchable by an ordinary SQL predicate — the point of all this. */
    public function test_plaintext_context_is_searchable_in_sql(): void
    {
        $this->writer(true)->write([
            $this->log(['order_id' => 8412]),
            $this->log(['order_id' => 9999]),
        ]);

        $n = (int) self::$pdo->query(
            "SELECT count(*) FROM nightowl_logs_v2 WHERE context LIKE '%8412%'"
        )->fetchColumn();

        $this->assertSame(1, $n);
    }

    /**
     * Turning the flag back off must CLOSE the fence. Without this the rows
     * drained after the flip sit inside a window the API still advertises as
     * searchable, and a search over it silently misses every one of them.
     */
    public function test_writing_compressed_again_closes_the_fence(): void
    {
        $this->writer(true)->write([$this->log(['order_id' => 1])]);
        $this->assertNotNull($this->fence());

        $this->writer(false)->write([$this->log(['order_id' => 2])]);
        $this->assertNull($this->fence(), 'a compressed batch must close the fence');
    }

    public function test_reopening_after_a_close_sets_a_new_fence(): void
    {
        $this->writer(true)->write([$this->log(['order_id' => 1])]);
        $first = $this->fence();

        $this->writer(false)->write([$this->log(['order_id' => 2])]);
        $this->assertNull($this->fence());

        $this->writer(true)->write([$this->log(['order_id' => 3])]);
        $second = $this->fence();

        $this->assertNotNull($second);
        // The second fence must not predate the compressed row it follows, or
        // it would cover it.
        $this->assertGreaterThanOrEqual($first, $second);
    }

    /**
     * A fence is opened ONCE and never pushed forward by later batches — a
     * forward move would strand already-plaintext rows outside the searchable
     * window.
     */
    public function test_steady_state_does_not_move_the_fence(): void
    {
        $writer = $this->writer(true);
        $writer->write([$this->log(['order_id' => 1])]);
        $first = $this->fence();

        sleep(1);
        $writer->write([$this->log(['order_id' => 2])]);

        $this->assertSame($first, $this->fence());
    }

    /** An empty context is no context in either mode — never a searchable '{}'. */
    public function test_placeholder_contexts_are_stored_as_null(): void
    {
        $this->writer(true)->write([$this->log([])]);

        $row = $this->onlyLog();

        $this->assertNull($row['context']);
        $this->assertNull($row['context_z']);
    }

    /**
     * A fence write that fails must cost the FENCE, never the telemetry.
     *
     * This runs inside the drain's transaction, where any failed statement
     * aborts the whole thing — so without savepoint isolation a tenant missing
     * `nightowl_settings` (a half-migrated schema) would lose every log batch
     * at commit, forever, with the real cause nowhere in the error.
     */
    public function test_a_failing_fence_write_does_not_cost_the_batch(): void
    {
        self::$pdo->exec('ALTER TABLE nightowl_settings RENAME TO nightowl_settings_hidden');

        try {
            $this->writer(true)->write([$this->log(['order_id' => 8412])]);
        } finally {
            self::$pdo->exec('ALTER TABLE nightowl_settings_hidden RENAME TO nightowl_settings');
        }

        $row = $this->onlyLog();

        $this->assertSame(json_encode(['order_id' => 8412]), $row['context'], 'the log row must survive');
        $this->assertNull($this->fence(), 'the fence stays closed — conservative, never wrong');
    }

    /**
     * Flag on, column absent — the customer set the env var before the
     * migration ran. This MUST write compressed and keep draining; the
     * alternative was a 42703 that the drain loop retried forever.
     */
    public function test_flag_without_the_column_writes_compressed_and_keeps_draining(): void
    {
        self::$pdo->exec('ALTER TABLE nightowl_logs_v2 DROP COLUMN context');

        try {
            $this->writer(true)->write([$this->log(['order_id' => 8412])]);

            $row = self::$pdo->query('SELECT context_z FROM nightowl_logs_v2')->fetch(PDO::FETCH_ASSOC);
            $this->assertNotNull($row, 'the batch must land');
            $this->assertNotNull($row['context_z'], 'stored compressed, the only shape the table can take');
            $this->assertNull($this->fence(), 'no plaintext was written, so no fence may open');
        } finally {
            self::$pdo->exec('ALTER TABLE nightowl_logs_v2 ADD COLUMN IF NOT EXISTS context text');
        }
    }

    private function writer(bool $searchable): RecordWriter
    {
        return new RecordWriter(
            self::$host, self::$port, self::$database, self::$username, self::$password,
            logContextSearchable: $searchable,
        );
    }

    private function log(array $context): array
    {
        return $this->sim->makeLog(['context' => json_encode($context)]);
    }

    private function onlyLog(): array
    {
        $rows = self::$pdo->query('SELECT context, context_z FROM nightowl_logs_v2 ORDER BY id')
            ->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);

        return $rows[0];
    }

    private function fence(): ?string
    {
        return StorageV2::logContextFence(self::$pdo);
    }
}
