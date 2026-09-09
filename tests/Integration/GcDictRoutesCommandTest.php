<?php

namespace NightOwl\Tests\Integration;

use Illuminate\Config\Repository;
use Illuminate\Database\DatabaseServiceProvider;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use NightOwl\Commands\GcDictRoutesCommand;
use NightOwl\Tests\Integration\Concerns\ReleasesAppConnections;
use PDO;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * nightowl:gc-dict-routes — the one-off route dictionary reclamation.
 *
 * Unlike the trace GC there is no created_at clock and no per-batch touch, so
 * the safety comes from the agent being STOPPED: a live daemon's LRU can hold
 * the id of a row this deletes, which would land a request pointing at nothing.
 * The command therefore has to (a) reclaim only genuinely unreferenced routes,
 * and (b) refuse when it can see evidence of a running agent.
 *
 * Runs inside a dedicated Postgres schema so the suite's real nightowl_* tables
 * are untouched. Skips when PostgreSQL is unavailable.
 */
final class GcDictRoutesCommandTest extends TestCase
{
    use ReleasesAppConnections;

    /** Per-process, so two concurrent runs (a reviewer, CI shards) cannot drop each other's schema. */
    private static string $schema;

    private static ?PDO $pdo = null;

    private Application $app;

    private string $sqlitePath;

    private static function dbConfig(): array
    {
        return [
            'driver' => 'pgsql',
            'host' => getenv('NIGHTOWL_TEST_DB_HOST') ?: '127.0.0.1',
            'port' => (int) (getenv('NIGHTOWL_TEST_DB_PORT') ?: 5432),
            'database' => getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test',
            'username' => getenv('NIGHTOWL_TEST_DB_USERNAME') ?: 'nightowl_test',
            'password' => getenv('NIGHTOWL_TEST_DB_PASSWORD') ?: 'test123',
            'charset' => 'utf8',
            'search_path' => self::$schema,
        ];
    }

    public static function setUpBeforeClass(): void
    {
        self::$schema = 'nightowl_gc_routes_test_'.getmypid();
        $c = self::dbConfig();

        try {
            self::$pdo = new PDO(
                sprintf('pgsql:host=%s;port=%d;dbname=%s', $c['host'], $c['port'], $c['database']),
                $c['username'],
                $c['password'],
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            );
        } catch (\Throwable) {
            self::$pdo = null;
        }

        // Only the owning pid drops its schema, so a killed run leaves one behind
        // (an alarm-killed run did, twice, in review). Sweep any whose pid is gone.
        if (self::$pdo !== null) {
            $stale = self::$pdo->query("SELECT nspname FROM pg_namespace WHERE nspname LIKE 'nightowl@_gc@_routes@_test@_%' ESCAPE '@'")->fetchAll(PDO::FETCH_COLUMN);
            foreach ($stale as $schema) {
                $pid = (int) substr($schema, strlen('nightowl_gc_routes_test_'));
                if ($pid !== getmypid() && ! self::pidAlive($pid)) {
                    self::$pdo->exec("DROP SCHEMA IF EXISTS {$schema} CASCADE");
                }
            }
        }
    }

    private static function pidAlive(int $pid): bool
    {
        if ($pid <= 0) {
            return false;
        }
        if (function_exists('posix_kill')) {
            return posix_kill($pid, 0) || posix_get_last_error() === 1; // EPERM: alive, not ours
        }

        return trim((string) shell_exec('ps -p '.$pid.' -o pid= 2>/dev/null')) !== '';
    }

    protected function setUp(): void
    {
        if (self::$pdo === null) {
            $this->markTestSkipped('PostgreSQL unavailable.');
        }

        self::$pdo->exec('DROP SCHEMA IF EXISTS '.self::$schema.' CASCADE');
        self::$pdo->exec('CREATE SCHEMA '.self::$schema);
        self::$pdo->exec('SET search_path TO '.self::$schema);

        self::$pdo->exec('CREATE TABLE '.self::$schema.'.nightowl_dict_route (
            id bigserial primary key, hash bytea, method varchar(16), domain varchar(255),
            path text, name varchar(255), action text, methods varchar(255)
        )');
        self::$pdo->exec('CREATE TABLE '.self::$schema.'.nightowl_requests_v2 (
            id bigserial primary key, route_id bigint, created_at timestamp
        )');

        // A path under the temp dir that no agent is writing to — the metrics
        // probe must find nothing, so the "is it stopped?" question falls to the
        // confirmation, which --force answers.
        $this->sqlitePath = sys_get_temp_dir().'/nightowl-gc-routes-test-'.getmypid().'.sqlite';

        $this->app = new Application(sys_get_temp_dir().'/nightowl-gc-routes-test');
        $this->app->singleton('config', fn () => new Repository([
            'database' => [
                'default' => 'nightowl',
                'connections' => ['nightowl' => self::dbConfig()],
            ],
            'nightowl' => [
                'agent' => [
                    // Health probe off: a real agent on this dev box would
                    // otherwise make the test's verdict depend on the machine.
                    'health_enabled' => false,
                    'sqlite_path' => $this->sqlitePath,
                ],
            ],
        ]));
        (new DatabaseServiceProvider($this->app))->register();

        Facade::clearResolvedInstances();
        Facade::setFacadeApplication($this->app);
    }

    protected function tearDown(): void
    {
        $this->releaseAppConnections();

        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);

        foreach (glob($this->sqlitePath.'*') ?: [] as $f) {
            @unlink($f);
        }

        self::$pdo?->exec('DROP SCHEMA IF EXISTS '.self::$schema.' CASCADE');
    }

    private function runGc(array $options = [], int $expectedExit = 0): string
    {
        $command = new GcDictRoutesCommand;
        $command->setLaravel($this->app);
        $output = new BufferedOutput;
        $exit = $command->run(new ArrayInput($options + ['--force' => true]), $output);
        $text = $output->fetch();
        $this->assertSame($expectedExit, $exit, $text);

        return $text;
    }

    private function insertRoute(string $path, string $action = 'App\C@index'): int
    {
        $stmt = self::$pdo->prepare('INSERT INTO '.self::$schema.".nightowl_dict_route (hash, method, path, action)
            VALUES (decode(?, 'hex'), 'GET', ?, ?) RETURNING id");
        $stmt->execute([bin2hex(random_bytes(16)), $path, $action]);

        return (int) $stmt->fetchColumn();
    }

    private function reference(int $routeId): void
    {
        self::$pdo->prepare('INSERT INTO '.self::$schema.'.nightowl_requests_v2 (route_id, created_at) VALUES (?, now())')
            ->execute([$routeId]);
    }

    /** @return array<int, int> */
    private function routeIds(): array
    {
        return array_map('intval', self::$pdo
            ->query('SELECT id FROM '.self::$schema.'.nightowl_dict_route ORDER BY id')
            ->fetchAll(PDO::FETCH_COLUMN));
    }

    public function test_reclaims_only_the_unreferenced_routes(): void
    {
        // Referenced id sits BETWEEN two orphans: a range-based delete without the
        // membership gate takes it too (the mutation audit's surviving mutant).
        $this->insertRoute('/api/gone-1');
        $referenced = $this->insertRoute('/api/kept');
        $this->insertRoute('/api/gone-2');
        $this->reference($referenced);

        $text = $this->runGc();

        $this->assertStringContainsString('Reclaimed 2 orphaned route(s) of 3', $text);
        $this->assertSame([$referenced], $this->routeIds());
    }

    /** ON COMMIT DROP: a second run on the same session must not hit 42P07. */
    public function test_can_run_twice_on_one_session(): void
    {
        $this->insertRoute('/api/gone');
        $this->assertStringContainsString('Reclaimed 1', $this->runGc());

        $this->insertRoute('/api/gone-again');
        $this->assertStringContainsString('Reclaimed 1', $this->runGc(), 'the temp table must not survive the first commit');
    }

    /** Paging follows the temp table's ids, so sparse ids and a tiny chunk still terminate and delete everything. */
    public function test_sparse_ids_with_a_tiny_chunk(): void
    {
        for ($i = 0; $i < 5; $i++) {
            self::$pdo->exec('SELECT setval(\''.self::$schema.'.nightowl_dict_route_id_seq\', '.(($i + 1) * 1_000_000).')');
            $this->insertRoute('/api/sparse-'.$i);
        }
        $kept = $this->insertRoute('/api/kept');
        $this->reference($kept);

        $this->assertStringContainsString('Reclaimed 5 orphaned route(s) of 6', $this->runGc(['--chunk' => 1]));
        $this->assertSame([$kept], $this->routeIds());
    }

    /**
     * A writer holding nightowl_requests_v2 makes the SHARE lock wait, and a lock
     * not granted in time is the verdict "something is writing": refuse, delete
     * nothing. This is the in-band detector for an agent the probes cannot see.
     */
    public function test_refuses_when_a_writer_holds_the_request_table(): void
    {
        $orphan = $this->insertRoute('/api/gone');

        $writer = new PDO(
            sprintf('pgsql:host=%s;port=%d;dbname=%s', self::dbConfig()['host'], self::dbConfig()['port'], self::dbConfig()['database']),
            self::dbConfig()['username'], self::dbConfig()['password'], [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
        );
        $writer->exec('SET search_path TO '.self::$schema);
        $writer->beginTransaction();
        $writer->exec('INSERT INTO '.self::$schema.'.nightowl_requests_v2 (route_id, created_at) VALUES (NULL, now())'); // ROW EXCLUSIVE, uncommitted

        try {
            $command = new GcDictRoutesCommand;
            $command->setLaravel($this->app);
            (new \ReflectionProperty($command, 'lockWaitSeconds'))->setValue($command, 1);
            $output = new BufferedOutput;
            $exit = $command->run(new ArrayInput(['--force' => true]), $output);
            $text = $output->fetch();

            $this->assertSame(1, $exit, $text);
            $this->assertStringContainsString('a writer is holding nightowl_requests_v2', $text);
            $this->assertSame([$orphan], $this->routeIds(), 'nothing deleted while a writer holds the table');
        } finally {
            $writer->rollBack();
        }
    }

    /** The buffer probe on its own (no metrics file) refuses, and says how long to wait. */
    public function test_refuses_on_a_fresh_ingest_buffer_and_names_the_wait(): void
    {
        $orphan = $this->insertRoute('/api/gone');
        file_put_contents($this->sqlitePath, 'sqlite');

        $text = $this->runGc([], expectedExit: 1);

        $this->assertStringContainsString('the ingest buffer was written', $text);
        $this->assertMatchesRegularExpression('/wait \d+s after stopping the agent/', $text);
        $this->assertSame([$orphan], $this->routeIds());
    }

    /** A dry run under a live agent still answers — with a warning, not a refusal. */
    public function test_dry_run_under_a_live_agent_warns_but_answers(): void
    {
        $this->insertRoute('/api/gone');
        file_put_contents($this->sqlitePath.'.drain-metrics.json', '{}');

        $text = $this->runGc(['--dry-run' => true]);

        $this->assertStringContainsString('would be reclaimed (dry run)', $text);
        $this->assertStringContainsString('holds share locks', $text);
    }

    /** Declining is not a failure: its own exit code, and it says so. */
    public function test_declining_the_confirmation_is_distinguishable_from_failure(): void
    {
        $orphan = $this->insertRoute('/api/gone');

        $command = new GcDictRoutesCommand;
        $command->setLaravel($this->app);
        $output = new BufferedOutput;
        $input = new ArrayInput([]);
        $input->setInteractive(false); // confirm() returns its default: no
        $exit = $command->run($input, $output);

        $this->assertSame(GcDictRoutesCommand::DECLINED, $exit);
        $this->assertStringContainsString('Aborted: not confirmed', $output->fetch());
        $this->assertSame([$orphan], $this->routeIds());
    }

    /** The health-port probe, exercised: a listener on the configured port refuses the run. */
    public function test_refuses_when_something_answers_on_the_health_port(): void
    {
        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        $this->assertNotFalse($server, $errstr);
        $port = (int) substr(stream_socket_get_name($server, false), strrpos(stream_socket_get_name($server, false), ':') + 1);
        $this->app['config']->set('nightowl.agent.health_enabled', true);
        $this->app['config']->set('nightowl.agent.health_port', $port);
        $orphan = $this->insertRoute('/api/gone');

        try {
            $text = $this->runGc([], expectedExit: 1);
            $this->assertStringContainsString('answering on its health port', $text);
            $this->assertSame([$orphan], $this->routeIds());
        } finally {
            fclose($server);
            $this->app['config']->set('nightowl.agent.health_enabled', false);
        }
    }

    /**
     * The liveness window must exceed the drain's 180s wedge-warn threshold (a
     * wedged worker stops writing its metrics file). Pinned from both sides:
     * a 500s-old file still refuses, a 700s-old file does not.
     */
    public function test_liveness_window_brackets_the_wedge_threshold(): void
    {
        $orphan = $this->insertRoute('/api/gone');
        $path = $this->sqlitePath.'.drain-metrics.json';
        file_put_contents($path, '{}');

        touch($path, time() - 500);
        $this->assertStringContainsString('Refusing to run', $this->runGc([], expectedExit: 1));
        $this->assertSame([$orphan], $this->routeIds());

        touch($path, time() - 700);
        $this->assertStringContainsString('Reclaimed 1', $this->runGc());
    }

    /** Which probe fired is named, so an operator knows what to wait for. */
    public function test_refusal_names_the_probe_that_fired(): void
    {
        $this->insertRoute('/api/gone');
        file_put_contents($this->sqlitePath.'.drain-metrics.json', '{}');
        $this->assertStringContainsString('a drain worker wrote metrics', $this->runGc([], expectedExit: 1));
    }

    /** Paging is proportional to rows: 5 sparse orphans at --chunk=1 means 5 delete statements, not a walk of the id span. */
    public function test_delete_statement_count_follows_rows_not_id_span(): void
    {
        for ($i = 0; $i < 5; $i++) {
            self::$pdo->exec('SELECT setval(\''.self::$schema.'.nightowl_dict_route_id_seq\', '.(($i + 1) * 1_000_000).')');
            $this->insertRoute('/api/sparse-'.$i);
        }
        $command = new GcDictRoutesCommand;
        $command->setLaravel($this->app);
        $deletes = new \ReflectionProperty($command, 'deleteStatements');
        $output = new BufferedOutput;
        $this->assertSame(0, $command->run(new ArrayInput(['--force' => true, '--chunk' => 1]), $output), $output->fetch());

        $this->assertSame(5, $deletes->getValue($command), 'one DELETE per chunk of ROWS');
        $this->assertSame([], $this->routeIds());
    }

    /** Paging starts below every possible id: a hand-inserted id <= 0 is not skipped. */
    public function test_ids_at_or_below_zero_are_reclaimed(): void
    {
        self::$pdo->exec('INSERT INTO '.self::$schema.".nightowl_dict_route (id, hash, method, path) VALUES (-1, decode('".bin2hex(random_bytes(16))."','hex'), 'GET', '/neg'), (0, decode('".bin2hex(random_bytes(16))."','hex'), 'GET', '/zero')");
        $this->insertRoute('/api/gone');

        $this->assertStringContainsString('Reclaimed 3 orphaned route(s) of 3', $this->runGc());
        $this->assertSame([], $this->routeIds());
    }

    /** --chunk is clamped at MAX_CHUNK: an absurd value must not become an absurd statement. */
    public function test_chunk_is_clamped_above(): void
    {
        for ($i = 0; $i < 3; $i++) {
            $this->insertRoute('/api/gone-'.$i);
        }
        $command = new GcDictRoutesCommand;
        $command->setLaravel($this->app);
        $output = new BufferedOutput;
        $this->assertSame(0, $command->run(new ArrayInput(['--force' => true, '--chunk' => 999999999]), $output), $output->fetch());
        $this->assertSame(50000, (new \ReflectionProperty($command, 'effectiveChunk'))->getValue($command));
        $this->assertSame([], $this->routeIds());
    }

    /** A path with glob metacharacters must not blind the file probes. */
    public function test_glob_metacharacters_in_the_buffer_path_do_not_blind_the_probe(): void
    {
        $dir = sys_get_temp_dir().'/nightowl-gc-[prod]-'.getmypid();
        @mkdir($dir);
        $this->sqlitePath = $dir.'/agent-buffer.sqlite';
        $this->app['config']->set('nightowl.agent.sqlite_path', $this->sqlitePath);
        file_put_contents($this->sqlitePath.'.drain-metrics.json', '{}');
        $orphan = $this->insertRoute('/api/gone');

        try {
            $text = $this->runGc([], expectedExit: 1);
            $this->assertStringContainsString('Refusing to run', $text);
            $this->assertSame([$orphan], $this->routeIds());
        } finally {
            @unlink($this->sqlitePath.'.drain-metrics.json');
            @rmdir($dir);
        }
    }

    /**
     * The Livewire clean-up this exists for: the pre-normalization rows, one per
     * page state, left behind once their requests aged out at retention.
     */
    public function test_reclaims_the_pre_normalization_livewire_rows(): void
    {
        $card = 'App\Livewire\Proposals\QuestionCard';
        for ($i = 78; $i <= 90; $i++) {
            $this->insertRoute('livewire-09e76b9c/update', implode(', ', array_fill(0, $i, $card)));
        }
        // The one row the normalized drain writes now, still in use.
        $live = $this->insertRoute('livewire-09e76b9c/update', $card);
        $this->reference($live);

        $this->runGc();

        $this->assertSame([$live], $this->routeIds());
    }

    public function test_a_referenced_route_survives_even_with_many_references(): void
    {
        $id = $this->insertRoute('/api/hot');
        for ($i = 0; $i < 5; $i++) {
            $this->reference($id);
        }
        $this->insertRoute('/api/gone'); // so the delete path actually runs

        $this->runGc();

        $this->assertSame([$id], $this->routeIds());
    }

    public function test_dry_run_reports_without_deleting(): void
    {
        $this->insertRoute('/api/gone');
        $kept = $this->insertRoute('/api/kept');
        $this->reference($kept);

        $text = $this->runGc(['--dry-run' => true]);

        $this->assertStringContainsString('1 of 2 route(s) are unreferenced', $text);
        $this->assertStringContainsString('would be reclaimed (dry run)', $text);
        $this->assertStringNotContainsString('0 bytes', $text, 'the size estimate must not be NULL-collapsed');
        $this->assertCount(2, $this->routeIds(), 'dry run must delete nothing');
    }

    public function test_nothing_to_do_is_reported_not_failed(): void
    {
        $kept = $this->insertRoute('/api/kept');
        $this->reference($kept);

        $this->assertStringContainsString('No orphaned routes', $this->runGc());
    }

    /**
     * The refusal that makes the whole design safe. A drain worker writes its IPC
     * metrics file continuously, so a recent mtime is evidence of a live agent —
     * and --force must NOT override it, or the one guarantee this command relies
     * on (no LRU holding the ids being deleted) is gone.
     */
    public function test_refuses_while_a_drain_worker_is_writing_metrics(): void
    {
        $orphan = $this->insertRoute('/api/gone');
        file_put_contents($this->sqlitePath.'.drain-metrics.json', '{}');

        $text = $this->runGc([], expectedExit: 1);

        $this->assertStringContainsString('Refusing to run', $text);
        $this->assertSame([$orphan], $this->routeIds(), 'nothing may be deleted while an agent looks alive');
    }

    /** A stale metrics file from an agent stopped long ago must not block it. */
    public function test_an_old_metrics_file_does_not_block(): void
    {
        $this->insertRoute('/api/gone');
        $path = $this->sqlitePath.'.drain-metrics.json';
        file_put_contents($path, '{}');
        touch($path, time() - 3600);

        $this->assertStringContainsString('Reclaimed 1 orphaned route', $this->runGc());
        $this->assertSame([], $this->routeIds());
    }

    /** A dry run is read-only, so it answers even while the agent is up. */
    public function test_dry_run_works_against_a_live_agent(): void
    {
        $this->insertRoute('/api/gone');
        file_put_contents($this->sqlitePath.'.drain-metrics.json', '{}');

        $this->assertStringContainsString('would be reclaimed (dry run)', $this->runGc(['--dry-run' => true]));
        $this->assertCount(1, $this->routeIds());
    }

    /**
     * Postgres caps bind parameters at 65 535. The estimate used to pass one per
     * candidate in a `WHERE id IN (…)`, so a tenant with more orphans than that —
     * precisely the tenant this command exists for, one row per Livewire page
     * state accumulated over weeks — got SQLSTATE HY000 out of the DRY RUN, the
     * first thing anyone runs. Sizes now come back with the ids from one scan.
     */
    public function test_survives_more_candidates_than_the_bind_parameter_limit(): void
    {
        self::$pdo->exec('INSERT INTO '.self::$schema.".nightowl_dict_route (hash, method, path, action)
            SELECT decode(md5(g::text), 'hex'), 'POST', 'livewire-09e76b9c/update',
                   repeat('App\\Livewire\\Proposals\\QuestionCard, ', 78)
            FROM generate_series(1, 70000) g");

        $text = $this->runGc(['--dry-run' => true]);
        $this->assertStringContainsString('70000 of 70000 route(s) are unreferenced', $text);
        $this->assertStringNotContainsString('0 bytes', $text);
        $this->assertCount(70000, $this->routeIds());

        $this->assertStringContainsString('Reclaimed 70000 orphaned route(s)', $this->runGc());
        $this->assertSame([], $this->routeIds());
    }

    public function test_a_tenant_without_the_v2_tables_is_a_no_op(): void
    {
        self::$pdo->exec('DROP TABLE '.self::$schema.'.nightowl_requests_v2');

        $this->assertStringContainsString('nothing to GC', $this->runGc());
    }
}
