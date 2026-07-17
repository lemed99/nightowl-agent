<?php

namespace NightOwl\Tests\Integration;

use PDO;
use PHPUnit\Framework\TestCase;

/**
 * Pins migration 000056: every audited-dead index is gone, and every index the
 * read paths depend on survived — especially the singles the DataManagement
 * loose index scans hard-require and the 000044 composites.
 */
final class IndexDietTest extends TestCase
{
    private static ?PDO $pdo = null;

    public static function setUpBeforeClass(): void
    {
        $host = getenv('NIGHTOWL_TEST_DB_HOST') ?: '127.0.0.1';
        $port = (int) (getenv('NIGHTOWL_TEST_DB_PORT') ?: 5432);
        $database = getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test';
        $username = getenv('NIGHTOWL_TEST_DB_USERNAME') ?: 'nightowl_test';
        $password = getenv('NIGHTOWL_TEST_DB_PASSWORD') ?: 'test123';

        try {
            self::$pdo = new PDO(
                sprintf('pgsql:host=%s;port=%d;dbname=%s', $host, $port, $database),
                $username,
                $password,
            );
            self::$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (\Exception) {
            self::$pdo = null;
        }

        if (self::$pdo) {
            MigrationRunner::migrate($host, $port, $database, $username, $password);
        }
    }

    protected function setUp(): void
    {
        if (self::$pdo === null) {
            $this->markTestSkipped('PostgreSQL not available. Set NIGHTOWL_TEST_DB_* env vars.');
        }
    }

    private function indexExists(string $name): bool
    {
        // Partitioned parents carry the replayed definitions under a _pt
        // suffix (RawPartitions::convert / migration 000058) — same columns,
        // same planner behavior, different name.
        $stmt = self::$pdo->prepare(
            "SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'public' AND indexname IN (?, ?)"
        );
        $stmt->execute([$name, $name.'_pt']);

        return (int) $stmt->fetchColumn() > 0;
    }

    public function test_dead_indexes_are_dropped(): void
    {
        $dead = [
            'nightowl_requests_timestamp_index',
            'nightowl_queries_timestamp_index',
            'nightowl_jobs_timestamp_index',
            'idx_requests_method_url',
            'nightowl_cache_events_trace_id_index',
            'nightowl_mail_trace_id_index',
            'nightowl_notifications_trace_id_index',
            'nightowl_outgoing_requests_trace_id_index',
            'nightowl_logs_channel_index',
            'nightowl_jobs_job_class_index',
            'nightowl_requests_group_hash_index',
            'nightowl_queries_group_hash_index',
            'nightowl_exceptions_fingerprint_index',
            'nightowl_requests_duration_index',
            'nightowl_queries_duration_index',
        ];

        foreach ($dead as $name) {
            $this->assertFalse($this->indexExists($name), "{$name} should have been dropped by 000056");
        }
    }

    public function test_load_bearing_indexes_survive(): void
    {
        $required = [
            // Loose index scans (DataManagementController) hard-require these singles.
            'nightowl_requests_status_code_index',
            'nightowl_jobs_status_index',
            'nightowl_logs_level_index',
            'nightowl_exceptions_class_index',
            'nightowl_cache_events_event_type_index',
            // Detail-page composites (000044).
            'nightowl_requests_group_hash_created_at_idx',
            'nightowl_queries_group_hash_created_at_idx',
            'nightowl_exceptions_fingerprint_created_at_idx',
            'nightowl_jobs_group_hash_created_at_idx',
            // Trace/attempt lookups.
            'nightowl_requests_trace_id_index',
            'nightowl_queries_execution_id_index',
            'nightowl_jobs_attempt_id_index',
            'nightowl_jobs_job_id_index',
            // Prune + pagination.
            'nightowl_requests_created_at_index',
            'nightowl_queries_created_at_index',
        ];

        foreach ($required as $name) {
            $this->assertTrue($this->indexExists($name), "{$name} must survive the index diet");
        }
    }
}
