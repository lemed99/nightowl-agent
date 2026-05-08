<?php

namespace NightOwl\Tests\Simulator;

/**
 * Simulates a laravel/nightwatch collector sending telemetry over TCP.
 *
 * Record bodies are loaded from captured fixtures under tests/Simulator/fixtures/
 * (one JSONL file per Nightwatch record type). The simulator picks a random
 * fixture row, refreshes mutable fields (trace_id/timestamp), then merges any
 * caller overrides. This keeps the wire-shape locked to what the real
 * laravel/nightwatch SDK emits — the fixtures are the source of truth.
 *
 * Wire format: [length]:[version]:[tokenHash]:[payload]
 *
 * Usage:
 *   php tests/Simulator/run.php --token=your-token --host=127.0.0.1 --port=2407
 *   php tests/Simulator/run.php --token=your-token --requests=50 --burst
 *   php tests/Simulator/run.php --token=your-token --scenario=error-storm
 */
final class NightwatchSimulator
{
    private string $tokenHash;

    private string $host;

    private int $port;

    private float $timeout;

    /** @var array<string, int> */
    private array $stats = ['sent' => 0, 'failed' => 0, 'bytes' => 0];

    /** @var array<string, array<int, array<string, mixed>>> Fixture cache keyed by record type */
    private array $fixtures = [];

    public function __construct(
        string $token,
        string $host = '127.0.0.1',
        int $port = 2407,
        float $timeout = 5.0,
    ) {
        $this->tokenHash = substr(hash('xxh128', $token), 0, 7);
        $this->host = $host;
        $this->port = $port;
        $this->timeout = $timeout;
    }

    // ─── Sending ───────────────────────────────────────────────

    /**
     * Send a batch of records over TCP.
     *
     * @param  array  $records  Array of record arrays (each must have 't' key)
     * @return string|null Server response ("2:OK", "5:ERROR") or null on failure
     */
    public function send(array $records): ?string
    {
        $json = json_encode($records, JSON_THROW_ON_ERROR | JSON_INVALID_UTF8_SUBSTITUTE);

        return $this->sendRaw($json);
    }

    /**
     * Send a raw JSON payload string over TCP.
     */
    public function sendRaw(string $payload): ?string
    {
        $body = "v1:{$this->tokenHash}:{$payload}";
        $wire = strlen($body).':'.$body;

        $socket = @stream_socket_client(
            "tcp://{$this->host}:{$this->port}",
            $errno,
            $errstr,
            $this->timeout,
        );

        if (! $socket) {
            $this->stats['failed']++;
            fwrite(STDERR, "Connection failed: [{$errno}] {$errstr}\n");

            return null;
        }

        stream_set_timeout($socket, (int) $this->timeout);

        fwrite($socket, $wire);
        $this->stats['bytes'] += strlen($wire);

        $response = fread($socket, 128);
        fclose($socket);

        if ($response !== false && str_starts_with($response, '2:')) {
            $this->stats['sent']++;
        } else {
            $this->stats['failed']++;
        }

        return $response ?: null;
    }

    /**
     * Send a PING health check.
     */
    public function ping(): ?string
    {
        return $this->sendRaw('PING');
    }

    /**
     * @return array{sent: int, failed: int, bytes: int}
     */
    public function getStats(): array
    {
        return $this->stats;
    }

    // ─── Scenarios ─────────────────────────────────────────────

    /**
     * Send a realistic request lifecycle: request + queries + cache + user.
     */
    public function simulateRequest(array $overrides = []): ?string
    {
        $traceId = $this->uuid();
        $now = microtime(true);
        $userId = 'user_'.mt_rand(1, 50);

        $records = [];

        // The request itself
        $records[] = $this->makeRequest(array_merge([
            'trace_id' => $traceId,
            'timestamp' => $now,
            'user' => $userId,
        ], $overrides));

        // 2-8 queries
        $queryCount = mt_rand(2, 8);
        for ($i = 0; $i < $queryCount; $i++) {
            $records[] = $this->makeQuery([
                'trace_id' => $this->uuid(),
                'timestamp' => $now + ($i * 0.001),
                'execution_id' => $traceId,
                'execution_source' => 'request',
                'user' => $userId,
            ]);
        }

        // 1-3 cache events
        $cacheCount = mt_rand(1, 3);
        for ($i = 0; $i < $cacheCount; $i++) {
            $records[] = $this->makeCacheEvent([
                'trace_id' => $this->uuid(),
                'timestamp' => $now + ($i * 0.0005),
                'execution_id' => $traceId,
                'execution_source' => 'request',
                'user' => $userId,
            ]);
        }

        // User record
        $records[] = $this->makeUser($userId);

        return $this->send($records);
    }

    /**
     * Simulate a request that throws an exception.
     */
    public function simulateErrorRequest(array $overrides = []): ?string
    {
        $traceId = $this->uuid();
        $now = microtime(true);

        $records = [];

        $records[] = $this->makeRequest(array_merge([
            'trace_id' => $traceId,
            'timestamp' => $now,
            'status_code' => 500,
            'exceptions' => 1,
            'duration' => mt_rand(50_000, 500_000),
        ], $overrides));

        $records[] = $this->makeException([
            'trace_id' => $this->uuid(),
            'timestamp' => $now,
            'execution_id' => $traceId,
            'execution_source' => 'request',
        ]);

        $records[] = $this->makeLog([
            'trace_id' => $this->uuid(),
            'timestamp' => $now,
            'execution_id' => $traceId,
            'execution_source' => 'request',
            'level' => 'error',
            'message' => 'Unhandled exception in request handler',
        ]);

        return $this->send($records);
    }

    /**
     * Simulate a queued job lifecycle: a queued-job dispatch event followed by a
     * job-attempt execution event with the given status.
     */
    public function simulateJob(string $status = 'processed', array $overrides = []): ?string
    {
        $traceId = $this->uuid();
        $now = microtime(true);
        $userId = 'user_'.mt_rand(1, 50);
        $jobId = $this->uuid();

        $records = [];

        // Dispatch event (queued-job) — no execution stats
        $records[] = $this->makeJob(array_merge([
            'trace_id' => $traceId,
            'timestamp' => $now,
            'user' => $userId,
            'job_id' => $jobId,
        ], $overrides));

        // Execution event (job-attempt) — carries status, duration, exceptions
        $records[] = $this->makeJobAttempt([
            'trace_id' => $this->uuid(),
            'timestamp' => $now + 0.01,
            'user' => $userId,
            'job_id' => $jobId,
            'attempt_id' => $this->uuid(),
            'attempt' => 1,
            'status' => $status,
        ]);

        // Jobs do queries too
        for ($i = 0; $i < mt_rand(1, 5); $i++) {
            $records[] = $this->makeQuery([
                'trace_id' => $this->uuid(),
                'timestamp' => $now + ($i * 0.002),
                'execution_id' => $traceId,
                'execution_source' => 'job',
                'user' => $userId,
            ]);
        }

        if ($status === 'failed') {
            $records[] = $this->makeException([
                'trace_id' => $this->uuid(),
                'timestamp' => $now,
                'execution_id' => $traceId,
                'execution_source' => 'job',
            ]);
        }

        return $this->send($records);
    }

    /**
     * Simulate an artisan command execution.
     */
    public function simulateCommand(array $overrides = []): ?string
    {
        $traceId = $this->uuid();
        $now = microtime(true);

        $records = [];

        $records[] = $this->makeCommand(array_merge([
            'trace_id' => $traceId,
            'timestamp' => $now,
        ], $overrides));

        for ($i = 0; $i < mt_rand(0, 3); $i++) {
            $records[] = $this->makeQuery([
                'trace_id' => $this->uuid(),
                'timestamp' => $now + ($i * 0.001),
                'execution_id' => $traceId,
                'execution_source' => 'command',
            ]);
        }

        return $this->send($records);
    }

    /**
     * Simulate a scheduled task execution.
     */
    public function simulateScheduledTask(array $overrides = []): ?string
    {
        $traceId = $this->uuid();
        $now = microtime(true);

        return $this->send([
            $this->makeScheduledTask(array_merge([
                'trace_id' => $traceId,
                'timestamp' => $now,
            ], $overrides)),
        ]);
    }

    /**
     * Run a full traffic scenario.
     */
    public function runScenario(string $scenario, int $count = 100): void
    {
        $start = microtime(true);

        match ($scenario) {
            'mixed' => $this->scenarioMixed($count),
            'error-storm' => $this->scenarioErrorStorm($count),
            'high-throughput' => $this->scenarioHighThroughput($count),
            'jobs' => $this->scenarioJobs($count),
            'realistic' => $this->scenarioRealistic($count),
            default => throw new \InvalidArgumentException("Unknown scenario: {$scenario}"),
        };

        $elapsed = round((microtime(true) - $start) * 1000);
        $s = $this->stats;
        fwrite(STDOUT, "\nDone: {$s['sent']} sent, {$s['failed']} failed, "
            .number_format($s['bytes'])." bytes, {$elapsed}ms\n");
    }

    private function scenarioMixed(int $count): void
    {
        for ($i = 0; $i < $count; $i++) {
            $type = mt_rand(0, 9);
            match (true) {
                $type <= 4 => $this->simulateRequest(),         // 50% requests
                $type <= 6 => $this->simulateJob(),             // 20% jobs
                $type === 7 => $this->simulateCommand(),        // 10% commands
                $type === 8 => $this->simulateErrorRequest(),   // 10% errors
                default => $this->simulateScheduledTask(),      // 10% scheduled
            };
            $this->printProgress($i + 1, $count);
        }
    }

    private function scenarioErrorStorm(int $count): void
    {
        for ($i = 0; $i < $count; $i++) {
            $this->simulateErrorRequest([
                'url' => '/api/checkout',
                'route_path' => '/api/checkout',
            ]);
            $this->printProgress($i + 1, $count);
        }
    }

    private function scenarioHighThroughput(int $count): void
    {
        // Send large batches to stress the buffer
        for ($i = 0; $i < $count; $i++) {
            $records = [];
            for ($j = 0; $j < 20; $j++) {
                $records[] = $this->makeRequest([
                    'trace_id' => $this->uuid(),
                    'timestamp' => microtime(true),
                ]);
            }
            $this->send($records);
            $this->printProgress($i + 1, $count);
        }
    }

    private function scenarioJobs(int $count): void
    {
        $statuses = ['processed', 'processed', 'processed', 'released', 'failed'];

        for ($i = 0; $i < $count; $i++) {
            $status = $statuses[array_rand($statuses)];
            $this->simulateJob($status);
            $this->printProgress($i + 1, $count);
        }
    }

    private function scenarioRealistic(int $count): void
    {
        for ($i = 0; $i < $count; $i++) {
            // Realistic traffic: lots of requests, some jobs, rare errors
            $roll = mt_rand(1, 100);
            match (true) {
                $roll <= 60 => $this->simulateRequest(),
                $roll <= 75 => $this->simulateJob(),
                $roll <= 85 => $this->simulateCommand(),
                $roll <= 90 => $this->simulateScheduledTask(),
                $roll <= 95 => $this->simulateErrorRequest(),
                $roll <= 98 => $this->simulateJob('released'),
                default => $this->simulateJob('failed'),
            };

            // Occasional mail/notification/outgoing request standalone
            if (mt_rand(1, 10) === 1) {
                $this->send([
                    $this->makeMail(['trace_id' => $this->uuid(), 'timestamp' => microtime(true)]),
                ]);
            }
            if (mt_rand(1, 15) === 1) {
                $this->send([
                    $this->makeNotification(['trace_id' => $this->uuid(), 'timestamp' => microtime(true)]),
                ]);
            }
            if (mt_rand(1, 5) === 1) {
                $this->send([
                    $this->makeOutgoingRequest(['trace_id' => $this->uuid(), 'timestamp' => microtime(true)]),
                ]);
            }

            $this->printProgress($i + 1, $count);
            usleep(mt_rand(10_000, 100_000)); // 10-100ms between events
        }
    }

    // ─── Record Builders ───────────────────────────────────────
    //
    // Each make*() returns a captured Nightwatch payload (random row from the
    // matching fixture file) with a fresh trace_id + timestamp, then merges
    // any caller overrides. The fixtures are the source of truth for which
    // fields exist — do not add hand-rolled defaults here.

    public function makeRequest(array $overrides = []): array
    {
        return array_merge($this->fromFixture('request'), $overrides);
    }

    public function makeQuery(array $overrides = []): array
    {
        return array_merge($this->fromFixture('query'), $overrides);
    }

    public function makeException(array $overrides = []): array
    {
        return array_merge($this->fromFixture('exception'), $overrides);
    }

    /**
     * Job dispatch event — `t: queued-job`. No execution stats (status,
     * exceptions, queries, etc.) — those belong to job-attempt records.
     */
    public function makeJob(array $overrides = []): array
    {
        return array_merge($this->fromFixture('queued-job'), $overrides);
    }

    /**
     * Job execution event — `t: job-attempt`. Carries status/duration/exceptions/etc.
     * Pair with a queued-job record (same job_id) to model the full lifecycle.
     */
    public function makeJobAttempt(array $overrides = []): array
    {
        return array_merge($this->fromFixture('job-attempt'), $overrides);
    }

    public function makeCommand(array $overrides = []): array
    {
        return array_merge($this->fromFixture('command'), $overrides);
    }

    public function makeScheduledTask(array $overrides = []): array
    {
        return array_merge($this->fromFixture('scheduled-task'), $overrides);
    }

    public function makeCacheEvent(array $overrides = []): array
    {
        return array_merge($this->fromFixture('cache-event'), $overrides);
    }

    public function makeMail(array $overrides = []): array
    {
        return array_merge($this->fromFixture('mail'), $overrides);
    }

    public function makeNotification(array $overrides = []): array
    {
        return array_merge($this->fromFixture('notification'), $overrides);
    }

    public function makeOutgoingRequest(array $overrides = []): array
    {
        return array_merge($this->fromFixture('outgoing-request'), $overrides);
    }

    public function makeLog(array $overrides = []): array
    {
        return array_merge($this->fromFixture('log'), $overrides);
    }

    public function makeUser(string $userId): array
    {
        return array_merge($this->fromFixture('user'), ['id' => $userId]);
    }

    // ─── Helpers ───────────────────────────────────────────────

    /**
     * Pull a random row from a fixture file, then refresh the mutable fields
     * (trace_id, timestamp). Fixtures are loaded lazily and cached per type.
     */
    private function fromFixture(string $type): array
    {
        if (! isset($this->fixtures[$type])) {
            $path = __DIR__."/fixtures/{$type}.jsonl";
            if (! is_file($path)) {
                throw new \RuntimeException("Missing fixture file: {$path}");
            }

            $rows = [];
            foreach (file($path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) as $line) {
                $rows[] = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
            }

            if ($rows === []) {
                throw new \RuntimeException("Empty fixture file: {$path}");
            }

            $this->fixtures[$type] = $rows;
        }

        $row = $this->fixtures[$type][array_rand($this->fixtures[$type])];

        if (array_key_exists('trace_id', $row)) {
            $row['trace_id'] = $this->uuid();
        }
        if (array_key_exists('timestamp', $row)) {
            $row['timestamp'] = microtime(true);
        }

        return $row;
    }

    private function uuid(): string
    {
        return sprintf(
            '%04x%04x-%04x-%04x-%04x-%04x%04x%04x',
            mt_rand(0, 0xFFFF), mt_rand(0, 0xFFFF),
            mt_rand(0, 0xFFFF),
            mt_rand(0, 0x0FFF) | 0x4000,
            mt_rand(0, 0x3FFF) | 0x8000,
            mt_rand(0, 0xFFFF), mt_rand(0, 0xFFFF), mt_rand(0, 0xFFFF),
        );
    }

    private function printProgress(int $current, int $total): void
    {
        if ($current % 10 === 0 || $current === $total) {
            fwrite(STDOUT, "\r  [{$current}/{$total}] sent: {$this->stats['sent']}, failed: {$this->stats['failed']}");
        }
    }
}
