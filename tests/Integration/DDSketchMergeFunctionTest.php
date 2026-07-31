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

    /**
     * 000070 replaced the bytea-state fold with a dense bigint[] state, so the
     * aggregate accumulates in place instead of re-encoding per input row. The
     * state carries the occupied span in slots 1264/1265 and finalize() walks
     * only that — which is where an off-by-one drops the first or last bucket
     * of a sketch without changing anything else about the output.
     */
    public function test_aggregate_over_many_sketches_matches_php_fold(): void
    {
        self::$pdo->exec('DROP TABLE IF EXISTS ddsketch_agg_probe');
        self::$pdo->exec('CREATE TABLE ddsketch_agg_probe (s bytea)');
        $insert = self::$pdo->prepare('INSERT INTO ddsketch_agg_probe VALUES (:s)');

        // Spans the whole closed index domain, so the min/max slots move in
        // both directions across the fold rather than only growing upward.
        $maps = [
            [Sketch::OVERFLOW_INDEX => 3],
            [630 => 12, 631 => 1],
            [Sketch::UNDERFLOW_INDEX => 9],
            [0 => 200000, 1 => 1],
            [630 => 4],
            [], // an empty sketch must not widen the span
        ];

        $expected = [];
        foreach ($maps as $m) {
            $insert->bindValue(':s', Sketch::pack($m), PDO::PARAM_LOB);
            $insert->execute();
            $expected = Sketch::mergeCounts($expected, $m);
        }
        // ...and a NULL row, which the drain leaves on dispatch-only buckets.
        self::$pdo->exec('INSERT INTO ddsketch_agg_probe VALUES (NULL)');

        $this->assertSame(
            bin2hex(Sketch::pack($expected)),
            bin2hex($this->fetchBytea('SELECT nightowl_ddsketch_agg(s) FROM ddsketch_agg_probe')),
        );

        self::$pdo->exec('DROP TABLE ddsketch_agg_probe');
    }

    /**
     * The aggregate is declared PARALLEL SAFE with a COMBINEFUNC, so a parallel
     * plan folds per-worker states through nightowl_ddsketch_combine — a path
     * no serial query ever reaches, and therefore one no other test covers.
     */
    public function test_combine_merges_two_partial_states(): void
    {
        $left = [Sketch::UNDERFLOW_INDEX => 2, 700 => 5];
        $right = [700 => 1, Sketch::OVERFLOW_INDEX => 4];

        $stmt = self::$pdo->prepare(<<<'SQL'
SELECT nightowl_ddsketch_finalize(nightowl_ddsketch_combine(
    nightowl_ddsketch_accum(NULL::bigint[], :a),
    nightowl_ddsketch_accum(NULL::bigint[], :b)
))
SQL);
        $stmt->bindValue(':a', Sketch::pack($left), PDO::PARAM_LOB);
        $stmt->bindValue(':b', Sketch::pack($right), PDO::PARAM_LOB);
        $stmt->execute();

        $expected = Sketch::mergeCounts($left, $right);
        ksort($expected);

        $this->assertSame($expected, Sketch::unpack($this->decodeBytea($stmt->fetchColumn())));

        // A worker that saw no rows contributes a NULL state.
        $this->assertSame(
            '',
            $this->fetchBytea('SELECT nightowl_ddsketch_finalize(nightowl_ddsketch_combine(NULL, NULL))'),
        );
    }

    /**
     * The aggregate must actually be the 000070 one. A test DB whose migration
     * chain stopped at 000057 still answers every assertion above correctly —
     * just at a cost that cannot serve a wide range inside a statement timeout,
     * which is the whole reason 000070 exists.
     */
    public function test_aggregate_is_the_linear_declaration(): void
    {
        $row = self::$pdo->query(<<<'SQL'
SELECT format_type(a.aggtranstype, NULL) AS state_type,
       a.aggcombinefn <> 0 AS has_combine,
       p.proparallel AS parallel
FROM pg_aggregate a
JOIN pg_proc p ON p.oid = a.aggfnoid
WHERE p.proname = 'nightowl_ddsketch_agg'
SQL)->fetch(PDO::FETCH_ASSOC);

        $this->assertSame('bigint[]', $row['state_type'], 'aggregate still folds a bytea state (migration 000070 did not run)');
        $this->assertTrue((bool) $row['has_combine'], 'aggregate has no COMBINEFUNC, so it cannot be parallelised');
        $this->assertSame('s', $row['parallel']);
    }

    private function fetchBytea(string $sql): string
    {
        return $this->decodeBytea(self::$pdo->query($sql)->fetchColumn());
    }

    private function decodeBytea(mixed $value): string
    {
        if (is_resource($value)) {
            $value = stream_get_contents($value);
        }
        if (is_string($value) && str_starts_with($value, '\x')) {
            return hex2bin(substr($value, 2));
        }

        return (string) $value;
    }
}
