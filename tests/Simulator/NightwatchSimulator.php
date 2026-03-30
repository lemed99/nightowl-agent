<?php

namespace NightOwl\Tests\Simulator;

/**
 * Simulates a laravel/nightwatch collector sending telemetry over TCP.
 *
 * Generates realistic payloads for all 12 record types and sends them
 * using the exact wire format: [length]:[version]:[tokenHash]:[payload]
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
     * @return string|null  Server response ("2:OK", "5:ERROR") or null on failure
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
        $wire = strlen($body) . ':' . $body;

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
        $userId = 'user_' . mt_rand(1, 50);

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
     * Simulate a queued job lifecycle.
     */
    public function simulateJob(string $status = 'processed', array $overrides = []): ?string
    {
        $traceId = $this->uuid();
        $now = microtime(true);
        $userId = 'user_' . mt_rand(1, 50);

        $records = [];

        $records[] = $this->makeJob(array_merge([
            'trace_id' => $traceId,
            'timestamp' => $now,
            'user' => $userId,
            'status' => $status,
        ], $overrides));

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
            . number_format($s['bytes']) . " bytes, {$elapsed}ms\n");
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
        $classes = [
            'App\\Exceptions\\PaymentFailedException',
            'Illuminate\\Database\\QueryException',
            'RuntimeException',
            'Symfony\\Component\\HttpKernel\\Exception\\NotFoundHttpException',
            'App\\Exceptions\\RateLimitExceededException',
        ];

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

    public function makeRequest(array $overrides = []): array
    {
        $routes = [
            ['GET', '/api/users', '/api/users'],
            ['POST', '/api/auth/login', '/api/auth/{uuid}/login'],
            ['GET', '/api/products', '/api/products'],
            ['GET', '/api/dashboard', '/api/dashboard'],
            ['PUT', '/api/users/1', '/api/users/{id}'],
            ['DELETE', '/api/sessions/abc', '/api/sessions/{id}'],
            ['GET', '/api/orders', '/api/orders'],
            ['POST', '/api/checkout', '/api/checkout'],
            ['GET', '/api/notifications', '/api/notifications'],
            ['PATCH', '/api/settings', '/api/settings'],
        ];

        $route = $routes[array_rand($routes)];
        $statusCodes = [200, 200, 200, 200, 200, 201, 204, 301, 404, 422, 500];
        $status = $overrides['status_code'] ?? $statusCodes[array_rand($statusCodes)];

        return array_merge([
            't' => 'request',
            'trace_id' => $this->uuid(),
            'timestamp' => microtime(true),
            'deploy' => 'production',
            'server' => 'web-01',
            '_group' => md5($route[1]),
            'user' => null,
            'method' => $route[0],
            'url' => "https://app.example.com{$route[1]}",
            'route_name' => null,
            'route_methods' => json_encode([$route[0]]),
            'route_domain' => null,
            'route_path' => $route[2],
            'route_action' => 'App\\Http\\Controllers\\ApiController@handle',
            'ip' => '192.168.1.' . mt_rand(1, 254),
            'duration' => mt_rand(5_000, 800_000),
            'status_code' => $status,
            'request_size' => mt_rand(200, 5000),
            'response_size' => mt_rand(500, 50_000),
            'bootstrap' => mt_rand(1000, 5000),
            'before_middleware' => mt_rand(500, 3000),
            'action' => mt_rand(5000, 200_000),
            'render' => mt_rand(100, 2000),
            'after_middleware' => mt_rand(100, 1000),
            'sending' => mt_rand(50, 500),
            'terminating' => mt_rand(100, 3000),
            'exceptions' => 0,
            'logs' => mt_rand(0, 3),
            'queries' => mt_rand(2, 15),
            'lazy_loads' => 0,
            'jobs_queued' => 0,
            'mail' => 0,
            'notifications' => 0,
            'outgoing_requests' => 0,
            'files_read' => 0,
            'files_written' => 0,
            'cache_events' => mt_rand(0, 5),
            'hydrated_models' => mt_rand(0, 50),
            'peak_memory_usage' => mt_rand(8_000_000, 64_000_000),
            'context' => null,
            'headers' => json_encode([
                'host' => 'app.example.com',
                'user-agent' => 'Mozilla/5.0',
                'accept' => 'application/json',
                'x-request-id' => $this->uuid(),
            ]),
            'payload' => null,
        ], $overrides);
    }

    public function makeQuery(array $overrides = []): array
    {
        $queries = [
            ['SELECT * FROM "users" WHERE "id" = ?', 'read'],
            ['SELECT * FROM "products" WHERE "active" = ? ORDER BY "created_at" DESC LIMIT ?', 'read'],
            ['INSERT INTO "orders" ("user_id", "total", "created_at") VALUES (?, ?, ?)', 'write'],
            ['UPDATE "users" SET "last_login_at" = ? WHERE "id" = ?', 'write'],
            ['SELECT COUNT(*) FROM "notifications" WHERE "read_at" IS NULL AND "user_id" = ?', 'read'],
            ['DELETE FROM "sessions" WHERE "expires_at" < ?', 'write'],
            ['SELECT "u"."id", "u"."email", "o"."total" FROM "users" "u" JOIN "orders" "o" ON "o"."user_id" = "u"."id" WHERE "o"."created_at" > ?', 'read'],
        ];

        $q = $queries[array_rand($queries)];

        return array_merge([
            't' => 'query',
            'trace_id' => $this->uuid(),
            'timestamp' => microtime(true),
            'deploy' => 'production',
            'server' => 'web-01',
            '_group' => md5($q[0]),
            'execution_source' => 'request',
            'execution_id' => null,
            'execution_stage' => 'action',
            'user' => null,
            'sql' => $q[0],
            'file' => 'app/Http/Controllers/ApiController.php',
            'line' => mt_rand(20, 200),
            'duration' => mt_rand(50, 50_000),
            'connection' => 'pgsql',
            'connection_type' => $q[1],
        ], $overrides);
    }

    public function makeException(array $overrides = []): array
    {
        $exceptions = [
            ['RuntimeException', 'Something went wrong', 'app/Services/PaymentService.php', 42],
            ['Illuminate\\Database\\QueryException', 'SQLSTATE[23505]: Unique violation', 'vendor/laravel/framework/src/Database/Connection.php', 760],
            ['App\\Exceptions\\ValidationException', 'The given data was invalid.', 'app/Http/Requests/StoreOrderRequest.php', 15],
            ['TypeError', 'Argument #1 must be of type string, null given', 'app/Models/User.php', 88],
            ['Symfony\\Component\\HttpKernel\\Exception\\NotFoundHttpException', 'The route could not be found.', 'vendor/laravel/framework/src/Routing/Router.php', 432],
        ];

        $e = $exceptions[array_rand($exceptions)];

        return array_merge([
            't' => 'exception',
            'trace_id' => $this->uuid(),
            'timestamp' => microtime(true),
            'deploy' => 'production',
            'server' => 'web-01',
            'execution_source' => 'request',
            'execution_id' => null,
            'execution_stage' => 'action',
            'user' => null,
            'class' => $e[0],
            'message' => $e[1],
            'code' => '0',
            'file' => $e[2],
            'line' => $e[3],
            'trace' => $this->fakeStackTrace($e[0], $e[2], $e[3]),
            'php_version' => '8.4.15',
            'laravel_version' => '12.43.1',
            'handled' => false,
            'fingerprint' => md5($e[0] . $e[2] . $e[3]),
        ], $overrides);
    }

    public function makeJob(array $overrides = []): array
    {
        $jobs = [
            'App\\Jobs\\SendWelcomeEmail',
            'App\\Jobs\\ProcessPayment',
            'App\\Jobs\\GenerateReport',
            'App\\Jobs\\SyncInventory',
            'App\\Jobs\\SendNotification',
        ];

        return array_merge([
            't' => 'queued_job',
            'trace_id' => $this->uuid(),
            'timestamp' => microtime(true),
            'deploy' => 'production',
            'server' => 'worker-01',
            '_group' => md5($jobs[0]),
            'execution_source' => 'request',
            'execution_id' => null,
            'user' => null,
            'name' => $jobs[array_rand($jobs)],
            'queue' => ['default', 'emails', 'reports'][array_rand(['default', 'emails', 'reports'])],
            'connection' => 'database',
            'status' => 'processed',
            'duration' => mt_rand(10_000, 5_000_000),
            'attempts' => 1,
            'exceptions' => 0,
            'logs' => 0,
            'queries' => mt_rand(1, 10),
            'lazy_loads' => 0,
            'jobs_queued' => 0,
            'mail' => 0,
            'notifications' => 0,
            'outgoing_requests' => 0,
            'files_read' => 0,
            'files_written' => 0,
            'cache_events' => 0,
            'hydrated_models' => mt_rand(0, 20),
            'peak_memory_usage' => mt_rand(16_000_000, 64_000_000),
        ], $overrides);
    }

    public function makeCommand(array $overrides = []): array
    {
        $commands = ['migrate', 'db:seed', 'cache:clear', 'queue:restart', 'config:cache', 'route:cache'];

        return array_merge([
            't' => 'command',
            'trace_id' => $this->uuid(),
            'timestamp' => microtime(true),
            'deploy' => 'production',
            'server' => 'web-01',
            '_group' => md5($commands[0]),
            'user' => null,
            'command' => $commands[array_rand($commands)],
            'exit_code' => 0,
            'duration' => mt_rand(50_000, 10_000_000),
            'exceptions' => 0,
            'logs' => mt_rand(0, 5),
            'queries' => mt_rand(0, 50),
            'lazy_loads' => 0,
            'jobs_queued' => 0,
            'mail' => 0,
            'notifications' => 0,
            'outgoing_requests' => 0,
            'files_read' => mt_rand(0, 10),
            'files_written' => mt_rand(0, 5),
            'cache_events' => 0,
            'hydrated_models' => 0,
            'peak_memory_usage' => mt_rand(32_000_000, 128_000_000),
        ], $overrides);
    }

    public function makeScheduledTask(array $overrides = []): array
    {
        $tasks = [
            ['schedule:run', '* * * * *'],
            ['horizon:snapshot', '*/5 * * * *'],
            ['nightowl:prune', '0 3 * * *'],
            ['queue:prune-batches', '0 * * * *'],
        ];

        $task = $tasks[array_rand($tasks)];

        return array_merge([
            't' => 'scheduled_task',
            'trace_id' => $this->uuid(),
            'timestamp' => microtime(true),
            'deploy' => 'production',
            'server' => 'web-01',
            '_group' => md5($task[0]),
            'user' => null,
            'command' => $task[0],
            'expression' => $task[1],
            'status' => 'processed',
            'duration' => mt_rand(100_000, 30_000_000),
            'exit_code' => 0,
            'exceptions' => 0,
            'logs' => 0,
            'queries' => mt_rand(0, 20),
            'lazy_loads' => 0,
            'jobs_queued' => 0,
            'mail' => 0,
            'notifications' => 0,
            'outgoing_requests' => 0,
            'files_read' => 0,
            'files_written' => 0,
            'cache_events' => 0,
            'hydrated_models' => 0,
            'peak_memory_usage' => mt_rand(16_000_000, 64_000_000),
        ], $overrides);
    }

    public function makeCacheEvent(array $overrides = []): array
    {
        $keys = ['users:1', 'products:list', 'config:cache', 'route:cache', 'session:abc123'];
        $types = ['hit', 'hit', 'hit', 'miss', 'set', 'delete'];

        return array_merge([
            't' => 'cache_event',
            'trace_id' => $this->uuid(),
            'timestamp' => microtime(true),
            'deploy' => 'production',
            'server' => 'web-01',
            'execution_source' => 'request',
            'execution_id' => null,
            'execution_stage' => 'action',
            'user' => null,
            'type' => $types[array_rand($types)],
            'key' => $keys[array_rand($keys)],
            'store' => 'redis',
            'ttl' => mt_rand(60, 86400),
            'duration' => mt_rand(50, 5000),
        ], $overrides);
    }

    public function makeMail(array $overrides = []): array
    {
        $mailables = [
            'App\\Mail\\WelcomeMail',
            'App\\Mail\\OrderConfirmation',
            'App\\Mail\\PasswordReset',
            'App\\Mail\\InvoiceMail',
        ];

        return array_merge([
            't' => 'mail',
            'trace_id' => $this->uuid(),
            'timestamp' => microtime(true),
            'deploy' => 'production',
            'server' => 'web-01',
            'execution_source' => 'job',
            'execution_id' => null,
            'execution_stage' => null,
            'user' => null,
            'mailer' => 'smtp',
            'to' => json_encode(['user@example.com']),
            'subject' => 'Welcome to our platform!',
            'mailable' => $mailables[array_rand($mailables)],
            'duration' => mt_rand(50_000, 500_000),
            'queued' => true,
        ], $overrides);
    }

    public function makeNotification(array $overrides = []): array
    {
        $notifications = [
            'App\\Notifications\\OrderShipped',
            'App\\Notifications\\PaymentReceived',
            'App\\Notifications\\NewFollower',
        ];

        return array_merge([
            't' => 'notification',
            'trace_id' => $this->uuid(),
            'timestamp' => microtime(true),
            'deploy' => 'production',
            'server' => 'web-01',
            'execution_source' => 'job',
            'execution_id' => null,
            'execution_stage' => null,
            'user' => null,
            'notification' => $notifications[array_rand($notifications)],
            'channel' => ['mail', 'database', 'slack'][array_rand(['mail', 'database', 'slack'])],
            'notifiable_type' => 'App\\Models\\User',
            'notifiable_id' => (string) mt_rand(1, 100),
            'duration' => mt_rand(10_000, 200_000),
            'queued' => true,
        ], $overrides);
    }

    public function makeOutgoingRequest(array $overrides = []): array
    {
        $apis = [
            ['GET', 'https://api.stripe.com/v1/charges', 200],
            ['POST', 'https://api.stripe.com/v1/payments', 201],
            ['GET', 'https://api.github.com/repos/owner/repo', 200],
            ['POST', 'https://hooks.slack.com/services/T00/B00/xxx', 200],
            ['GET', 'https://maps.googleapis.com/maps/api/geocode/json', 200],
        ];

        $api = $apis[array_rand($apis)];

        return array_merge([
            't' => 'outgoing_request',
            'trace_id' => $this->uuid(),
            'timestamp' => microtime(true),
            'deploy' => 'production',
            'server' => 'web-01',
            'execution_source' => 'request',
            'execution_id' => null,
            'execution_stage' => 'action',
            'user' => null,
            'method' => $api[0],
            'url' => $api[1],
            'status_code' => $api[2],
            'duration' => mt_rand(50_000, 2_000_000),
            'request_size' => mt_rand(100, 2000),
            'response_size' => mt_rand(200, 50_000),
            'request_headers' => null,
        ], $overrides);
    }

    public function makeLog(array $overrides = []): array
    {
        $levels = ['debug', 'info', 'info', 'notice', 'warning', 'error'];
        $messages = [
            'User logged in successfully',
            'Cache key expired, refreshing',
            'Payment processed for order #1234',
            'Slow query detected (> 500ms)',
            'Rate limit approaching for API key',
            'Failed to connect to external service',
        ];

        return array_merge([
            't' => 'log',
            'trace_id' => $this->uuid(),
            'timestamp' => microtime(true),
            'deploy' => 'production',
            'server' => 'web-01',
            'execution_source' => 'request',
            'execution_id' => null,
            'execution_stage' => 'action',
            'user' => null,
            'level' => $levels[array_rand($levels)],
            'message' => $messages[array_rand($messages)],
            'context' => json_encode(['key' => 'value', 'elapsed' => mt_rand(1, 500)]),
            'channel' => 'stack',
        ], $overrides);
    }

    public function makeUser(string $userId): array
    {
        $names = ['Alice Johnson', 'Bob Smith', 'Charlie Brown', 'Diana Prince', 'Eve Williams'];

        return [
            't' => 'user',
            'id' => $userId,
            'name' => $names[array_rand($names)],
            'username' => strtolower(str_replace(' ', '.', $names[array_rand($names)])) . '@example.com',
        ];
    }

    // ─── Helpers ───────────────────────────────────────────────

    private function uuid(): string
    {
        return sprintf(
            '%04x%04x-%04x-%04x-%04x-%04x%04x%04x',
            mt_rand(0, 0xffff), mt_rand(0, 0xffff),
            mt_rand(0, 0xffff),
            mt_rand(0, 0x0fff) | 0x4000,
            mt_rand(0, 0x3fff) | 0x8000,
            mt_rand(0, 0xffff), mt_rand(0, 0xffff), mt_rand(0, 0xffff),
        );
    }

    private function fakeStackTrace(string $class, string $file, int $line): string
    {
        return <<<TRACE
        {$class}: Error occurred
        #0 {$file}({$line}): {$class}->handle()
        #1 app/Http/Controllers/ApiController.php(45): App\\Services\\PaymentService->process()
        #2 vendor/laravel/framework/src/Routing/Controller.php(54): App\\Http\\Controllers\\ApiController->store()
        #3 vendor/laravel/framework/src/Routing/ControllerDispatcher.php(45): call_user_func_array()
        #4 vendor/laravel/framework/src/Routing/Route.php(261): Illuminate\\Routing\\ControllerDispatcher->dispatch()
        #5 vendor/laravel/framework/src/Routing/Router.php(837): Illuminate\\Routing\\Route->run()
        #6 vendor/laravel/framework/src/Pipeline/Pipeline.php(144): Illuminate\\Routing\\Router->runRoute()
        #7 {main}
        TRACE;
    }

    private function printProgress(int $current, int $total): void
    {
        if ($current % 10 === 0 || $current === $total) {
            fwrite(STDOUT, "\r  [{$current}/{$total}] sent: {$this->stats['sent']}, failed: {$this->stats['failed']}");
        }
    }
}
