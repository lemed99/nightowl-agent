<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Support\DDSketchHistogram as Sketch;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * Drift guard for the cross-language contract: the plpgsql
 * nightowl_ddsketch_merge() (which runs inside the drain's ON CONFLICT SET)
 * must produce byte-identical output to the PHP DDSketchHistogram::merge() —
 * same varint layout, same delta encoding, same ordering.
 */
final class DDSketchMergeFunctionTest extends TestCase
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

    private function sqlMerge(string $a, string $b): string
    {
        $stmt = self::$pdo->prepare('SELECT nightowl_ddsketch_merge(:a, :b)');
        $stmt->bindValue(':a', $a, PDO::PARAM_LOB);
        $stmt->bindValue(':b', $b, PDO::PARAM_LOB);
        $stmt->execute();
        $result = $stmt->fetchColumn();

        // pgsql returns bytea as a stream or hex string depending on driver mode.
        if (is_resource($result)) {
            return stream_get_contents($result);
        }
        if (is_string($result) && str_starts_with($result, '\x')) {
            return hex2bin(substr($result, 2));
        }

        return (string) $result;
    }

    public function test_sql_merge_matches_php_merge(): void
    {
        $cases = [
            // [sparse a, sparse b]
            [[3 => 1, 10 => 5], [10 => 2, 99 => 4]],
            [[-1 => 2, 0 => 1], [Sketch::OVERFLOW_INDEX => 7]],
            // multi-byte varints: index > 127, count > 127, delta > 127
            [[500 => 300], [900 => 1000, 1200 => 128]],
            [[0 => 1], []],
            [[], []],
        ];

        foreach ($cases as [$a, $b]) {
            $packedA = Sketch::pack($a);
            $packedB = Sketch::pack($b);

            $this->assertSame(
                bin2hex(Sketch::merge($packedA, $packedB)),
                bin2hex($this->sqlMerge($packedA, $packedB)),
                'plpgsql merge must be byte-identical to PHP merge for '.json_encode([$a, $b])
            );
        }
    }

    public function test_aggregate_folds_multiple_sketches(): void
    {
        self::$pdo->exec('DROP TABLE IF EXISTS ddsketch_agg_probe');
        self::$pdo->exec('CREATE TABLE ddsketch_agg_probe (s bytea)');
        $insert = self::$pdo->prepare('INSERT INTO ddsketch_agg_probe VALUES (:s)');

        $maps = [[3 => 1], [3 => 2, 10 => 5], [500 => 300]];
        $expected = [];
        foreach ($maps as $m) {
            $insert->bindValue(':s', Sketch::pack($m), PDO::PARAM_LOB);
            $insert->execute();
            $expected = Sketch::mergeCounts($expected, $m);
        }

        $result = self::$pdo->query('SELECT nightowl_ddsketch_agg(s) FROM ddsketch_agg_probe')->fetchColumn();
        if (is_resource($result)) {
            $result = stream_get_contents($result);
        } elseif (is_string($result) && str_starts_with($result, '\x')) {
            $result = hex2bin(substr($result, 2));
        }

        $this->assertSame($expected, Sketch::unpack((string) $result));

        self::$pdo->exec('DROP TABLE ddsketch_agg_probe');
    }
}
