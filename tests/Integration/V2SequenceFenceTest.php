<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Support\V2SequenceFence;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * The re-applied v2 id-sequence fence. Migration 000067 fences each v2 sequence
 * to MAX(v1.id)+1 inside its creation guard, so it freezes at that instant and
 * every later v1 insert overtakes it — the mixed fleet (a 1.x agent still
 * draining into v1) and the NIGHTOWL_STORAGE_V2=false excursion both then mint
 * ids that collide across the families, which the API's v1+v2 union pages as
 * duplicate (created_at, id) cursors.
 *
 * Runs against a synthetic pair through apply()'s test hook, shaped like the
 * real thing (v2 partitioned by created_at, bigserial id, composite PK) so the
 * assertion that pg_get_serial_sequence resolves for a PARTITIONED parent is
 * real. The suite's own raw tables are left untouched — their sequences are
 * live for every other integration test.
 */
final class V2SequenceFenceTest extends TestCase
{
    private const V1 = '__fence_probe';

    private const V2 = '__fence_probe_v2';

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
    }

    protected function setUp(): void
    {
        if (self::$pdo === null) {
            $this->markTestSkipped('PostgreSQL not available. Set NIGHTOWL_TEST_DB_* env vars.');
        }

        $this->dropPair();
        $this->createPair();
    }

    protected function tearDown(): void
    {
        if (self::$pdo !== null) {
            $this->dropPair();
        }
    }

    private function dropPair(): void
    {
        self::$pdo->exec('DROP TABLE IF EXISTS '.self::V2);
        self::$pdo->exec('DROP TABLE IF EXISTS '.self::V1);
    }

    /**
     * The shapes that matter: v1 plain with a bigserial id (where $table->id()
     * leaves it), v2 partitioned by created_at with a bigserial id and the
     * composite PK 000067 gives it.
     */
    private function createPair(): void
    {
        self::$pdo->exec('CREATE TABLE '.self::V1.' (id bigserial PRIMARY KEY, created_at timestamp(0) NOT NULL)');
        self::$pdo->exec('CREATE TABLE '.self::V2.' (id bigserial NOT NULL, created_at timestamp(0) NOT NULL) PARTITION BY RANGE (created_at)');
        self::$pdo->exec('ALTER TABLE '.self::V2.' ADD PRIMARY KEY (id, created_at)');
        self::$pdo->exec('CREATE TABLE '.self::V2.'_pdefault PARTITION OF '.self::V2.' DEFAULT');
    }

    /** 000067's one-shot fence, verbatim: MAX(v1.id)+1 at v2-creation time. */
    private function applyCreationTimeFence(): void
    {
        self::$pdo->exec(
            "SELECT setval(pg_get_serial_sequence('".self::V2."', 'id'),
                           COALESCE((SELECT MAX(id) FROM ".self::V1.'), 0) + 1,
                           false)'
        );
    }

    private function insertV1(int $rows): void
    {
        for ($i = 0; $i < $rows; $i++) {
            self::$pdo->exec('INSERT INTO '.self::V1.' (created_at) VALUES (now())');
        }
    }

    private function insertV2(int $rows): void
    {
        for ($i = 0; $i < $rows; $i++) {
            self::$pdo->exec('INSERT INTO '.self::V2.' (created_at) VALUES (now())');
        }
    }

    /**
     * The id a v2 insert would take, read the only way that proves it: by
     * taking it. Consuming one is free — the sequence is allowed gaps and `id`
     * is synthetic.
     */
    private function takeNextV2Id(): int
    {
        return (int) self::$pdo->query(
            "SELECT nextval(pg_get_serial_sequence('".self::V2."', 'id'))"
        )->fetchColumn();
    }

    private function maxV1Id(): int
    {
        return (int) self::$pdo->query('SELECT COALESCE(MAX(id), 0) FROM '.self::V1)->fetchColumn();
    }

    private function apply(): array
    {
        return V2SequenceFence::apply(self::$pdo, [self::V1 => self::V2]);
    }

    /**
     * The defect and its fix in one pass: v1 keeps growing after the
     * creation-time fence froze, so the id v2 hands out next is one v1 has
     * already used — and the re-fence lifts it clear.
     */
    public function test_refence_lifts_the_v2_sequence_above_the_v1_high_water_mark(): void
    {
        $this->applyCreationTimeFence(); // v1 empty at creation → v2 starts at 1
        $this->insertV1(5);              // the mixed-fleet window: v1 ids 1..5

        $this->assertLessThanOrEqual(
            $this->maxV1Id(),
            $this->takeNextV2Id(),
            "premise: 000067's frozen fence hands v2 an id v1 already used",
        );

        $result = $this->apply();

        $this->assertGreaterThan(
            $this->maxV1Id(),
            $this->takeNextV2Id(),
            'the re-fence must lift the next v2 id strictly above MAX(v1.id)',
        );
        $this->assertSame(
            [self::V1 => 5 + V2SequenceFence::GAP],
            $result['refenced'],
            'the new fence is MAX(v1.id) + GAP, reported by logical table name',
        );
        $this->assertSame([], $result['failures'], 'pg_get_serial_sequence must resolve for a partitioned parent');
        $this->assertSame([], $result['overlapping'], 'no v2 row was ever written, so nothing can overlap');
    }

    /**
     * Strictly forward-only. MAX(v1.id) is NOT monotonic — prune and
     * nightowl:clear empty v1 — so a second run must not follow it back down
     * onto ids the v2 family has already issued.
     */
    public function test_second_run_never_moves_the_sequence_backwards(): void
    {
        $this->applyCreationTimeFence();
        $this->insertV1(5);

        $this->apply();

        // Three v2 rows off the fresh fence, then v1 emptied under us.
        $this->insertV2(3);
        $issued = (int) self::$pdo->query('SELECT MAX(id) FROM '.self::V2)->fetchColumn();
        self::$pdo->exec('DELETE FROM '.self::V1);

        $result = $this->apply();

        $this->assertSame([], $result['refenced'], 'headroom is intact — the second run must be a no-op');
        $this->assertSame(
            $issued + 1,
            $this->takeNextV2Id(),
            'the sequence must stay where it was, never fall back onto issued ids',
        );
    }

    /** Ids already issued on both sides: reported by name, never renumbered. */
    public function test_already_overlapping_id_ranges_are_reported(): void
    {
        $this->applyCreationTimeFence();
        $this->insertV1(5);
        $this->insertV2(3); // minted from the frozen fence → ids 1..3, inside v1's range

        $result = $this->apply();

        $this->assertSame([self::V1], $result['overlapping']);
        $this->assertSame([self::V1 => 5 + V2SequenceFence::GAP], $result['refenced'], 'the fence still applies going forward');

        // Nothing is rewritten — the collided rows are left exactly as they are.
        $this->assertSame(
            '1,2,3',
            self::$pdo->query("SELECT string_agg(id::text, ',' ORDER BY id) FROM ".self::V2)->fetchColumn(),
        );
    }

    /**
     * Post-EOL tenant: prune's v1 retirement DROPped the parent, so there is
     * nothing left to collide with — and naming the dropped table would 42P01
     * the whole routine.
     */
    public function test_missing_v1_parent_is_skipped_not_failed(): void
    {
        $this->applyCreationTimeFence();
        self::$pdo->exec('DROP TABLE '.self::V1);

        $result = $this->apply();

        $this->assertSame(
            ['refenced' => [], 'overlapping' => [], 'failures' => []],
            $result,
            'a retired v1 family is skipped silently',
        );
        $this->assertSame(1, $this->takeNextV2Id(), 'and its v2 sequence is left exactly where it was');
    }

    /**
     * A fresh install is fenced too: v1 is empty NOW, but an unfenced v2 sits at
     * id 1 — exactly where a NIGHTOWL_STORAGE_V2=false excursion restarts v1 —
     * so the gap is reserved against future v1 inserts, not just existing rows.
     */
    public function test_empty_v1_still_gets_the_full_gap(): void
    {
        $this->applyCreationTimeFence();

        $result = $this->apply();

        $this->assertSame([self::V1 => V2SequenceFence::GAP], $result['refenced']);
        $this->assertSame(V2SequenceFence::GAP, $this->takeNextV2Id());
    }
}
