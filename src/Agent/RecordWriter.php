<?php

namespace NightOwl\Agent;

use PDO;

final class RecordWriter
{
    private ?PDO $pdo = null;
    private string $dsn;
    private string $username;
    private string $password;

    /** @var array<string, list<array{target?: string, duration_ms: int}>> Thresholds grouped by type */
    private array $thresholdCache = [];
    private float $thresholdCacheExpiry = 0;
    private int $thresholdCacheTtl;

    private AlertNotifier $notifier;
    private string $appName;

    public function __construct(string $host, int $port, string $database, string $username, string $password, int $thresholdCacheTtl = 86400, ?AlertNotifier $notifier = null, string $appName = 'NightOwl')
    {
        $this->dsn = "pgsql:host={$host};port={$port};dbname={$database}";
        $this->username = $username;
        $this->password = $password;
        $this->thresholdCacheTtl = $thresholdCacheTtl;
        $this->notifier = $notifier ?? new AlertNotifier;
        $this->appName = $appName;
    }

    private function connect(): void
    {
        $this->pdo = new PDO($this->dsn, $this->username, $this->password);
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        // Disable synchronous commit — don't wait for WAL flush on each transaction.
        // Trades ~10ms of data durability for 2-5x write throughput. Acceptable for
        // monitoring data: a crash loses at most the last few milliseconds of events,
        // which are still safe in the SQLite buffer and will be re-drained on restart.
        $this->pdo->exec('SET synchronous_commit = off');
    }

    private function pdo(): PDO
    {
        if ($this->pdo === null) {
            $this->connect();
        }

        return $this->pdo;
    }

    /**
     * Create a RecordWriter from Laravel config.
     */
    public static function fromConfig(): self
    {
        return new self(
            config('nightowl.database.host', '127.0.0.1'),
            (int) config('nightowl.database.port', 5432),
            config('nightowl.database.database', 'nightowl'),
            config('nightowl.database.username', 'nightowl'),
            config('nightowl.database.password', 'nightowl'),
            (int) config('nightowl.threshold_cache_ttl', 86400),
            AlertNotifier::fromConfig(),
            config('app.name', 'NightOwl'),
        );
    }

    /**
     * Write an array of records to the database.
     * Each record has a 't' field indicating its type.
     *
     * Automatically reconnects and retries once on connection failure.
     */
    public function write(array $records): void
    {
        try {
            $this->doWrite($records);
        } catch (\Throwable $e) {
            // Check if this looks like a connection error — reconnect and retry once
            if ($this->isConnectionError($e)) {
                $this->pdo = null;
                $this->doWrite($records);
            } else {
                throw $e;
            }
        }
    }

    private function doWrite(array $records): void
    {
        $grouped = [];
        foreach ($records as $record) {
            $type = $record['t'] ?? null;
            if ($type === null) {
                continue;
            }
            $grouped[$type][] = $record;
        }

        $pdo = $this->pdo();

        $pdo->beginTransaction();

        try {
            foreach ($grouped as $type => $typeRecords) {
                match ($type) {
                    'request' => $this->writeRequests($typeRecords),
                    'query' => $this->writeQueries($typeRecords),
                    'exception' => $this->writeExceptions($typeRecords),
                    'command' => $this->writeCommands($typeRecords),
                    'queued-job' => $this->writeJobs($typeRecords),
                    'cache-event' => $this->writeCacheEvents($typeRecords),
                    'mail' => $this->writeMail($typeRecords),
                    'notification' => $this->writeNotifications($typeRecords),
                    'outgoing-request' => $this->writeOutgoingRequests($typeRecords),
                    'scheduled-task' => $this->writeScheduledTasks($typeRecords),
                    'job-attempt' => $this->writeJobs($typeRecords),
                    'log' => $this->writeLogs($typeRecords),
                    'user' => $this->writeUsers($typeRecords),
                    default => null,
                };
            }

            $pdo->commit();
        } catch (\Throwable $e) {
            $this->notifier->clearPending(); // Discard — data was rolled back
            if ($pdo->inTransaction()) {
                $pdo->rollBack();
            }
            throw $e;
        }

        // Dispatch notifications AFTER commit — no blocking I/O inside the transaction
        $this->notifier->flushNotifications($pdo);
    }

    private function isConnectionError(\Throwable $e): bool
    {
        $message = strtolower($e->getMessage());
        $prev = $e->getPrevious();
        $prevMessage = $prev ? strtolower($prev->getMessage()) : '';

        $patterns = ['server closed', 'connection reset', 'broken pipe', 'gone away', 'no connection', 'connection refused', 'connection timed out'];
        foreach ($patterns as $pattern) {
            if (str_contains($message, $pattern) || str_contains($prevMessage, $pattern)) {
                return true;
            }
        }

        return false;
    }

    /**
     * COPY a batch of rows into a table using PostgreSQL's COPY protocol.
     * 5-10x faster than batched INSERTs because it bypasses the SQL parser.
     *
     * @param string   $table   Target table name
     * @param string[] $columns Column names in order
     * @param array[]  $rows    Each row is an array of values matching $columns order
     */
    private function copyBatch(string $table, array $columns, array $rows): void
    {
        if (empty($rows)) {
            return;
        }

        $colList = implode(', ', $columns);
        $tsvRows = [];

        foreach ($rows as $row) {
            $escaped = [];
            foreach ($row as $value) {
                if ($value === null) {
                    $escaped[] = '\\N';
                } else {
                    // Escape tab, newline, carriage return, and backslash for TSV format
                    $escaped[] = str_replace(
                        ["\\", "\t", "\n", "\r"],
                        ["\\\\", "\\t", "\\n", "\\r"],
                        (string) $value,
                    );
                }
            }
            $tsvRows[] = implode("\t", $escaped);
        }

        $this->pdo()->pgsqlCopyFromArray($table . ' (' . $colList . ')', $tsvRows, "\t", '\\N');
    }

    private function writeRequests(array $records): void
    {
        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'server', 'group_hash',
            'user_id', 'method', 'url', 'route_name', 'route_methods',
            'route_domain', 'route_path', 'route_action', 'ip',
            'duration', 'status_code', 'request_size', 'response_size',
            'bootstrap', 'before_middleware', 'action', 'render',
            'after_middleware', 'sending', 'terminating',
            'exceptions', 'logs', 'queries', 'lazy_loads',
            'jobs_queued', 'mail', 'notifications', 'outgoing_requests',
            'files_read', 'files_written', 'cache_events',
            'hydrated_models', 'peak_memory_usage',
            'exception_preview', 'context', 'headers', 'payload',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null,
                $r['timestamp'] ?? null,
                $r['deploy'] ?? null,
                $r['server'] ?? null,
                $r['_group'] ?? null,
                $r['user'] ?? null,
                $r['method'] ?? 'GET',
                $r['url'] ?? '/',
                $r['route_name'] ?? null,
                json_encode($r['route_methods'] ?? []),
                $r['route_domain'] ?? null,
                $r['route_path'] ?? null,
                $r['route_action'] ?? null,
                $r['ip'] ?? null,
                $r['duration'] ?? null,
                $r['status_code'] ?? 200,
                $r['request_size'] ?? null,
                $r['response_size'] ?? null,
                $r['bootstrap'] ?? null,
                $r['before_middleware'] ?? null,
                $r['action'] ?? null,
                $r['render'] ?? null,
                $r['after_middleware'] ?? null,
                $r['sending'] ?? null,
                $r['terminating'] ?? null,
                $r['exceptions'] ?? 0,
                $r['logs'] ?? 0,
                $r['queries'] ?? 0,
                $r['lazy_loads'] ?? 0,
                $r['jobs_queued'] ?? 0,
                $r['mail'] ?? 0,
                $r['notifications'] ?? 0,
                $r['outgoing_requests'] ?? 0,
                $r['files_read'] ?? 0,
                $r['files_written'] ?? 0,
                $r['cache_events'] ?? 0,
                $r['hydrated_models'] ?? 0,
                $r['peak_memory_usage'] ?? 0,
                $r['exception_preview'] ?? null,
                is_string($r['context'] ?? null) ? $r['context'] : json_encode($r['context'] ?? null),
                is_string($r['headers'] ?? null) ? $r['headers'] : json_encode($r['headers'] ?? null),
                is_string($r['payload'] ?? null) ? $r['payload'] : json_encode($r['payload'] ?? null),
            ];
        }

        $this->copyBatch('nightowl_requests', $columns, $rows);

        $this->checkRouteThresholds($records);
    }

    private function writeQueries(array $records): void
    {
        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'server', 'group_hash',
            'execution_source', 'execution_id', 'execution_stage', 'execution_preview', 'user_id',
            'sql_query', 'file', 'line', 'duration', 'connection', 'connection_type',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null,
                $r['timestamp'] ?? null,
                $r['deploy'] ?? null,
                $r['server'] ?? null,
                $r['_group'] ?? null,
                $r['execution_source'] ?? null,
                $r['execution_id'] ?? null,
                $r['execution_stage'] ?? null,
                $r['execution_preview'] ?? null,
                $r['user'] ?? null,
                $r['sql'] ?? '',
                $r['file'] ?? null,
                $r['line'] ?? null,
                $r['duration'] ?? null,
                $r['connection'] ?? null,
                $r['connection_type'] ?? null,
            ];
        }

        $this->copyBatch('nightowl_queries', $columns, $rows);
    }

    private function writeExceptions(array $records): void
    {
        $stmt = $this->pdo()->prepare('INSERT INTO nightowl_exceptions (
            v, trace_id, timestamp, deploy, server, group_hash,
            execution_source, execution_id, execution_stage, execution_preview, user_id,
            class, message, code, file, line, trace,
            php_version, laravel_version, handled, fingerprint
        ) VALUES (
            :v, :trace_id, :timestamp, :deploy, :server, :group_hash,
            :execution_source, :execution_id, :execution_stage, :execution_preview, :user_id,
            :class, :message, :code, :file, :line, :trace,
            :php_version, :laravel_version, :handled, :fingerprint
        )');

        $issueGroups = [];

        foreach ($records as $r) {
            $fingerprint = md5(($r['class'] ?? '') . ($r['file'] ?? '') . ($r['line'] ?? ''));

            $stmt->execute([
                'v' => $r['v'] ?? null,
                'trace_id' => $r['trace_id'] ?? null,
                'timestamp' => $r['timestamp'] ?? null,
                'deploy' => $r['deploy'] ?? null,
                'server' => $r['server'] ?? null,
                'group_hash' => $r['_group'] ?? null,
                'execution_source' => $r['execution_source'] ?? null,
                'execution_id' => $r['execution_id'] ?? null,
                'execution_stage' => $r['execution_stage'] ?? null,
                'execution_preview' => $r['execution_preview'] ?? null,
                'user_id' => $r['user'] ?? null,
                'class' => $r['class'] ?? 'Unknown',
                'message' => $r['message'] ?? null,
                'code' => $r['code'] ?? null,
                'file' => $r['file'] ?? null,
                'line' => $r['line'] ?? null,
                'trace' => $r['trace'] ?? null,
                'php_version' => $r['php_version'] ?? null,
                'laravel_version' => $r['laravel_version'] ?? null,
                'handled' => filter_var($r['handled'] ?? false, FILTER_VALIDATE_BOOLEAN) ? 't' : 'f',
                'fingerprint' => $fingerprint,
            ]);

            if (! isset($issueGroups[$fingerprint])) {
                $issueGroups[$fingerprint] = [
                    'class' => $r['class'] ?? 'Unknown',
                    'message' => $r['message'] ?? null,
                    'count' => 0,
                    'users' => [],
                    'timestamps' => [],
                ];
            }
            $issueGroups[$fingerprint]['count']++;
            if (! empty($r['user'])) {
                $issueGroups[$fingerprint]['users'][$r['user']] = true;
            }
            if (! empty($r['timestamp'])) {
                $issueGroups[$fingerprint]['timestamps'][] = $r['timestamp'];
            }
        }

        $existingBefore = $this->notifier->snapshotExistingIssues($this->pdo(), array_keys($issueGroups), 'exception');
        $this->syncIssuesToExceptions($issueGroups);
        $this->notifier->queueNewIssueNotifications($this->appName, $issueGroups, 'exception', $existingBefore);
    }

    private function syncIssuesToExceptions(array $issueGroups): void
    {
        // users_count uses a subquery on nightowl_exceptions to compute the
        // actual distinct user count, instead of blindly accumulating per batch
        // (which inflates the count when the same user appears across batches).
        $upsertStmt = $this->pdo()->prepare('
            INSERT INTO nightowl_issues (
                type, status, exception_class, exception_message, group_hash,
                first_seen_at, last_seen_at, occurrences_count, users_count,
                created_at, updated_at
            ) VALUES (
                :type, :status, :exception_class, :exception_message, :group_hash,
                :first_seen_at, :last_seen_at, :occurrences_count, :users_count,
                :created_at, :updated_at
            )
            ON CONFLICT (group_hash, type) DO UPDATE SET
                exception_message = EXCLUDED.exception_message,
                last_seen_at = GREATEST(nightowl_issues.last_seen_at, EXCLUDED.last_seen_at),
                occurrences_count = nightowl_issues.occurrences_count + EXCLUDED.occurrences_count,
                users_count = (
                    SELECT COUNT(DISTINCT user_id) FROM nightowl_exceptions
                    WHERE fingerprint = EXCLUDED.group_hash AND user_id IS NOT NULL
                ),
                updated_at = EXCLUDED.updated_at
        ');

        $now = date('Y-m-d H:i:s');

        foreach ($issueGroups as $fingerprint => $group) {
            $timestamps = $group['timestamps'];
            sort($timestamps);
            $firstSeen = ! empty($timestamps) ? date('Y-m-d H:i:s', (int) $timestamps[0]) : $now;
            $lastSeen = ! empty($timestamps) ? date('Y-m-d H:i:s', (int) end($timestamps)) : $now;
            $userCount = count($group['users']);

            $upsertStmt->execute([
                'type' => 'exception',
                'status' => 'open',
                'exception_class' => $group['class'],
                'exception_message' => $group['message'],
                'group_hash' => $fingerprint,
                'first_seen_at' => $firstSeen,
                'last_seen_at' => $lastSeen,
                'occurrences_count' => $group['count'],
                'users_count' => $userCount,
                'created_at' => $now,
                'updated_at' => $now,
            ]);
        }
    }

    private function writeCommands(array $records): void
    {
        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'server', 'group_hash',
            'user_id', 'class', 'name', 'command', 'exit_code', 'duration',
            'bootstrap', 'action', 'terminating',
            'exceptions', 'logs', 'queries', 'lazy_loads',
            'jobs_queued', 'mail', 'notifications', 'outgoing_requests',
            'files_read', 'files_written', 'cache_events',
            'hydrated_models', 'peak_memory_usage', 'exception_preview', 'context',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null, $r['timestamp'] ?? null, $r['deploy'] ?? null,
                $r['server'] ?? null, $r['_group'] ?? null, $r['user'] ?? null,
                $r['class'] ?? null, $r['name'] ?? null, $r['command'] ?? 'unknown', $r['exit_code'] ?? null, $r['duration'] ?? null,
                $r['bootstrap'] ?? null, $r['action'] ?? null, $r['terminating'] ?? null,
                $r['exceptions'] ?? 0, $r['logs'] ?? 0, $r['queries'] ?? 0, $r['lazy_loads'] ?? 0,
                $r['jobs_queued'] ?? 0, $r['mail'] ?? 0, $r['notifications'] ?? 0, $r['outgoing_requests'] ?? 0,
                $r['files_read'] ?? 0, $r['files_written'] ?? 0, $r['cache_events'] ?? 0,
                $r['hydrated_models'] ?? 0, $r['peak_memory_usage'] ?? 0, $r['exception_preview'] ?? null,
                is_string($r['context'] ?? null) ? $r['context'] : json_encode($r['context'] ?? null),
            ];
        }

        $this->copyBatch('nightowl_commands', $columns, $rows);

        $this->checkThresholds('command', $records, 'command');
    }

    private function writeJobs(array $records): void
    {
        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'server', 'group_hash',
            'execution_source', 'execution_id', 'execution_stage', 'execution_preview', 'user_id',
            'job_id', 'attempt_id', 'attempt',
            'job_class', 'queue', 'connection', 'status', 'duration', 'attempts',
            'exceptions', 'logs', 'queries', 'lazy_loads',
            'jobs_queued', 'mail', 'notifications', 'outgoing_requests',
            'files_read', 'files_written', 'cache_events',
            'hydrated_models', 'peak_memory_usage', 'exception_preview', 'context',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null, $r['timestamp'] ?? null, $r['deploy'] ?? null,
                $r['server'] ?? null, $r['_group'] ?? null, $r['execution_source'] ?? null,
                $r['execution_id'] ?? null, $r['execution_stage'] ?? null, $r['execution_preview'] ?? null, $r['user'] ?? null,
                $r['job_id'] ?? null, $r['attempt_id'] ?? null, $r['attempt'] ?? null,
                $r['name'] ?? $r['job_class'] ?? 'Unknown', $r['queue'] ?? null,
                $r['connection'] ?? null, $r['status'] ?? null, $r['duration'] ?? null, $r['attempts'] ?? 1,
                $r['exceptions'] ?? 0, $r['logs'] ?? 0, $r['queries'] ?? 0, $r['lazy_loads'] ?? 0,
                $r['jobs_queued'] ?? 0, $r['mail'] ?? 0, $r['notifications'] ?? 0, $r['outgoing_requests'] ?? 0,
                $r['files_read'] ?? 0, $r['files_written'] ?? 0, $r['cache_events'] ?? 0,
                $r['hydrated_models'] ?? 0, $r['peak_memory_usage'] ?? 0, $r['exception_preview'] ?? null,
                is_string($r['context'] ?? null) ? $r['context'] : json_encode($r['context'] ?? null),
            ];
        }

        $this->copyBatch('nightowl_jobs', $columns, $rows);

        $this->checkThresholds('job', $records, ['name', 'job_class']);
    }

    private function writeCacheEvents(array $records): void
    {
        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'server', 'group_hash',
            'execution_source', 'execution_id', 'execution_stage', 'execution_preview', 'user_id',
            'event_type', 'key', 'store', 'ttl', 'duration',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null, $r['timestamp'] ?? null, $r['deploy'] ?? null, $r['server'] ?? null, $r['_group'] ?? null,
                $r['execution_source'] ?? null, $r['execution_id'] ?? null, $r['execution_stage'] ?? null, $r['execution_preview'] ?? null, $r['user'] ?? null,
                $r['type'] ?? 'unknown', $r['key'] ?? '', $r['store'] ?? null, $r['ttl'] ?? null, $r['duration'] ?? null,
            ];
        }

        $this->copyBatch('nightowl_cache_events', $columns, $rows);
    }

    private function writeMail(array $records): void
    {
        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'server', 'group_hash',
            'execution_source', 'execution_id', 'execution_stage', 'execution_preview', 'user_id',
            'mailer', 'recipients', 'cc', 'bcc', 'attachments', 'subject', 'mailable', 'duration', 'failed', 'queued',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null, $r['timestamp'] ?? null, $r['deploy'] ?? null, $r['server'] ?? null, $r['_group'] ?? null,
                $r['execution_source'] ?? null, $r['execution_id'] ?? null, $r['execution_stage'] ?? null, $r['execution_preview'] ?? null, $r['user'] ?? null,
                $r['mailer'] ?? null, is_array($r['to'] ?? null) ? json_encode($r['to']) : ($r['to'] ?? null),
                $r['cc'] ?? 0, $r['bcc'] ?? 0, $r['attachments'] ?? 0,
                $r['subject'] ?? null, $r['class'] ?? $r['mailable'] ?? null, $r['duration'] ?? null,
                ($r['failed'] ?? false) ? 't' : 'f', ($r['queued'] ?? false) ? 't' : 'f',
            ];
        }

        $this->copyBatch('nightowl_mail', $columns, $rows);
    }

    private function writeNotifications(array $records): void
    {
        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'server', 'group_hash',
            'execution_source', 'execution_id', 'execution_stage', 'execution_preview', 'user_id',
            'notification', 'channel', 'notifiable_type', 'notifiable_id', 'duration', 'failed', 'queued',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null, $r['timestamp'] ?? null, $r['deploy'] ?? null, $r['server'] ?? null, $r['_group'] ?? null,
                $r['execution_source'] ?? null, $r['execution_id'] ?? null, $r['execution_stage'] ?? null, $r['execution_preview'] ?? null, $r['user'] ?? null,
                $r['class'] ?? $r['notification'] ?? null, $r['channel'] ?? null, $r['notifiable_type'] ?? null, $r['notifiable_id'] ?? null,
                $r['duration'] ?? null, ($r['failed'] ?? false) ? 't' : 'f', ($r['queued'] ?? false) ? 't' : 'f',
            ];
        }

        $this->copyBatch('nightowl_notifications', $columns, $rows);
    }

    private function writeOutgoingRequests(array $records): void
    {
        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'server', 'group_hash',
            'execution_source', 'execution_id', 'execution_stage', 'execution_preview', 'user_id',
            'host', 'method', 'url', 'status_code', 'duration',
            'request_size', 'response_size', 'request_headers',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null, $r['timestamp'] ?? null, $r['deploy'] ?? null, $r['server'] ?? null, $r['_group'] ?? null,
                $r['execution_source'] ?? null, $r['execution_id'] ?? null, $r['execution_stage'] ?? null, $r['execution_preview'] ?? null, $r['user'] ?? null,
                $r['host'] ?? null, $r['method'] ?? 'GET', $r['url'] ?? '', $r['status_code'] ?? null, $r['duration'] ?? null,
                $r['request_size'] ?? null, $r['response_size'] ?? null, $r['request_headers'] ?? null,
            ];
        }

        $this->copyBatch('nightowl_outgoing_requests', $columns, $rows);
    }

    private function writeLogs(array $records): void
    {
        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'server',
            'execution_source', 'execution_id', 'execution_stage', 'execution_preview', 'user_id',
            'level', 'message', 'context', 'extra', 'channel', 'created_at',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null, $r['timestamp'] ?? null, $r['deploy'] ?? null, $r['server'] ?? null,
                $r['execution_source'] ?? null, $r['execution_id'] ?? null, $r['execution_stage'] ?? null, $r['execution_preview'] ?? null, $r['user'] ?? null,
                $r['level'] ?? 'info', $r['message'] ?? null,
                is_string($r['context'] ?? null) ? $r['context'] : json_encode($r['context'] ?? null),
                is_string($r['extra'] ?? null) ? $r['extra'] : json_encode($r['extra'] ?? null),
                $r['channel'] ?? null,
                isset($r['timestamp']) ? date('Y-m-d H:i:s', (int) $r['timestamp']) : date('Y-m-d H:i:s'),
            ];
        }

        $this->copyBatch('nightowl_logs', $columns, $rows);
    }

    private function writeUsers(array $records): void
    {
        $stmt = $this->pdo()->prepare('
            INSERT INTO nightowl_users (v, user_id, name, email, timestamp, updated_at)
            VALUES (:v, :user_id, :name, :email, :timestamp, :updated_at)
            ON CONFLICT (user_id) DO UPDATE SET
                v = EXCLUDED.v,
                name = EXCLUDED.name,
                email = EXCLUDED.email,
                timestamp = EXCLUDED.timestamp,
                updated_at = EXCLUDED.updated_at
        ');

        foreach ($records as $r) {
            $userId = $r['id'] ?? null;
            if ($userId === null || $userId === '') {
                continue;
            }

            $stmt->execute([
                'v' => $r['v'] ?? null,
                'user_id' => (string) $userId,
                'name' => $r['name'] ?? null,
                'email' => $r['username'] ?? null,
                'timestamp' => $r['timestamp'] ?? null,
                'updated_at' => date('Y-m-d H:i:s'),
            ]);
        }
    }

    private function writeScheduledTasks(array $records): void
    {
        $columns = [
            'v', 'trace_id', 'timestamp', 'deploy', 'server', 'group_hash',
            'user_id', 'command', 'expression',
            'timezone', 'repeat_seconds', 'without_overlapping', 'on_one_server', 'run_in_background', 'even_in_maintenance_mode',
            'status', 'duration', 'exit_code',
            'exceptions', 'logs', 'queries', 'lazy_loads',
            'jobs_queued', 'mail', 'notifications', 'outgoing_requests',
            'files_read', 'files_written', 'cache_events',
            'hydrated_models', 'peak_memory_usage', 'exception_preview', 'context',
        ];

        $rows = [];
        foreach ($records as $r) {
            $rows[] = [
                $r['v'] ?? null,
                $r['trace_id'] ?? null, $r['timestamp'] ?? null, $r['deploy'] ?? null,
                $r['server'] ?? null, $r['_group'] ?? null, $r['user'] ?? null,
                $r['name'] ?? $r['command'] ?? 'unknown', $r['cron'] ?? $r['expression'] ?? null,
                $r['timezone'] ?? null, $r['repeat_seconds'] ?? 0,
                ($r['without_overlapping'] ?? false) ? 't' : 'f', ($r['on_one_server'] ?? false) ? 't' : 'f',
                ($r['run_in_background'] ?? false) ? 't' : 'f', ($r['even_in_maintenance_mode'] ?? false) ? 't' : 'f',
                $r['status'] ?? null, $r['duration'] ?? null, $r['exit_code'] ?? null,
                $r['exceptions'] ?? 0, $r['logs'] ?? 0, $r['queries'] ?? 0, $r['lazy_loads'] ?? 0,
                $r['jobs_queued'] ?? 0, $r['mail'] ?? 0, $r['notifications'] ?? 0, $r['outgoing_requests'] ?? 0,
                $r['files_read'] ?? 0, $r['files_written'] ?? 0, $r['cache_events'] ?? 0,
                $r['hydrated_models'] ?? 0, $r['peak_memory_usage'] ?? 0, $r['exception_preview'] ?? null,
                is_string($r['context'] ?? null) ? $r['context'] : json_encode($r['context'] ?? null),
            ];
        }

        $this->copyBatch('nightowl_scheduled_tasks', $columns, $rows);

        $this->checkThresholds('scheduled-task', $records, ['name', 'command']);
    }

    // ─── Performance Threshold Checking ──────────────────────────────

    /**
     * Check route thresholds using composite "GET|HEAD /path" target keys.
     */
    private function checkRouteThresholds(array $records): void
    {
        $thresholds = $this->getThresholds();
        if (empty($thresholds['route'] ?? [])) {
            return;
        }

        // Build composite name for each record so it matches the threshold target format
        foreach ($records as &$r) {
            $methods = $r['route_methods'] ?? [];
            if (is_string($methods)) {
                $methods = json_decode($methods, true) ?? [];
            }
            $prefix = ! empty($methods) ? implode('|', $methods).' ' : '';
            $r['_route_composite'] = $prefix.($r['route_path'] ?? '');
        }
        unset($r);

        $this->checkThresholds('route', $records, '_route_composite');
    }

    /**
     * Load thresholds from nightowl_settings, cached for threshold_cache_ttl seconds.
     *
     * @return array<string, list<array{target?: string, duration_ms: int}>>
     */
    private function getThresholds(): array
    {
        $now = microtime(true);

        if ($now < $this->thresholdCacheExpiry) {
            return $this->thresholdCache;
        }

        $this->thresholdCache = [];
        $this->thresholdCacheExpiry = $now + $this->thresholdCacheTtl;

        try {
            $raw = $this->pdo()->query(
                "SELECT value FROM nightowl_settings WHERE key = 'thresholds'"
            )->fetchColumn();

            if (! $raw) {
                return $this->thresholdCache;
            }

            $items = json_decode($raw, true);
            if (! is_array($items)) {
                return $this->thresholdCache;
            }

            foreach ($items as $item) {
                $type = $item['type'] ?? 'route';
                $this->thresholdCache[$type][] = [
                    'target' => $item['target'] ?? $item['route'] ?? null,
                    'duration_ms' => (int) ($item['duration_ms'] ?? 0),
                ];
            }
        } catch (\Throwable) {
            // Table may not exist yet — silently ignore
        }

        return $this->thresholdCache;
    }

    /**
     * Find the matching threshold for a record.
     * Specific target match takes priority over global (no target).
     *
     * @return int|null Duration threshold in microseconds, or null if no threshold matches
     */
    private function findThreshold(string $type, ?string $target): ?int
    {
        $thresholds = $this->getThresholds();
        $typeThresholds = $thresholds[$type] ?? [];

        if (empty($typeThresholds)) {
            return null;
        }

        $globalThreshold = null;
        $specificThreshold = null;

        foreach ($typeThresholds as $t) {
            if (empty($t['target'])) {
                $globalThreshold = $t['duration_ms'] * 1000; // ms → μs
            } elseif ($target !== null && $t['target'] === $target) {
                $specificThreshold = $t['duration_ms'] * 1000; // ms → μs
            }
        }

        return $specificThreshold ?? $globalThreshold;
    }

    /**
     * Check records against thresholds and upsert performance issues.
     *
     * @param string          $type     Threshold type: 'route', 'job', 'command', 'scheduled-task'
     * @param array           $records  Raw records from the batch
     * @param string|string[] $nameKeys Record field(s) containing the name, tried in order
     * @param string          $groupKey Record field containing the group hash
     */
    private function checkThresholds(string $type, array $records, string|array $nameKeys, string $groupKey = '_group'): void
    {
        $thresholds = $this->getThresholds();
        if (empty($thresholds[$type] ?? [])) {
            return;
        }

        $nameKeys = (array) $nameKeys;
        $issueGroups = [];

        foreach ($records as $r) {
            $duration = $r['duration'] ?? null;
            if ($duration === null) {
                continue;
            }

            $name = null;
            foreach ($nameKeys as $key) {
                if (! empty($r[$key])) {
                    $name = $r[$key];
                    break;
                }
            }
            $threshold = $this->findThreshold($type, $name);

            if ($threshold === null || $duration < $threshold) {
                continue;
            }

            $groupHash = $r[$groupKey] ?? ($name !== null ? md5($name) : null);
            if ($groupHash === null) {
                continue;
            }

            if (! isset($issueGroups[$groupHash])) {
                $issueGroups[$groupHash] = [
                    'name' => $name ?? 'Unknown',
                    'count' => 0,
                    'users' => [],
                    'timestamps' => [],
                ];
            }
            $issueGroups[$groupHash]['count']++;
            if (! empty($r['user'])) {
                $issueGroups[$groupHash]['users'][$r['user']] = true;
            }
            if (! empty($r['timestamp'])) {
                $issueGroups[$groupHash]['timestamps'][] = $r['timestamp'];
            }
        }

        if (empty($issueGroups)) {
            return;
        }

        $existingBefore = $this->notifier->snapshotExistingIssues($this->pdo(), array_keys($issueGroups), 'performance');
        $this->upsertPerformanceIssues($issueGroups);
        $this->notifier->queueNewIssueNotifications($this->appName, $issueGroups, 'performance', $existingBefore);
    }

    /**
     * Upsert performance issues — same pattern as syncIssuesToExceptions.
     */
    private function upsertPerformanceIssues(array $issueGroups): void
    {
        // Performance issues use GREATEST for users_count instead of addition.
        // Unlike exceptions (which have a dedicated table for accurate counting),
        // performance issue users come from various source tables, so we use
        // GREATEST to prevent unbounded inflation while keeping the high-water mark.
        $upsertStmt = $this->pdo()->prepare('
            INSERT INTO nightowl_issues (
                type, status, exception_class, exception_message, group_hash,
                first_seen_at, last_seen_at, occurrences_count, users_count,
                created_at, updated_at
            ) VALUES (
                :type, :status, :exception_class, :exception_message, :group_hash,
                :first_seen_at, :last_seen_at, :occurrences_count, :users_count,
                :created_at, :updated_at
            )
            ON CONFLICT (group_hash, type) DO UPDATE SET
                last_seen_at = GREATEST(nightowl_issues.last_seen_at, EXCLUDED.last_seen_at),
                occurrences_count = nightowl_issues.occurrences_count + EXCLUDED.occurrences_count,
                users_count = GREATEST(nightowl_issues.users_count, EXCLUDED.users_count),
                updated_at = EXCLUDED.updated_at
        ');

        $now = date('Y-m-d H:i:s');

        foreach ($issueGroups as $groupHash => $group) {
            $timestamps = $group['timestamps'];
            sort($timestamps);
            $firstSeen = ! empty($timestamps) ? date('Y-m-d H:i:s', (int) $timestamps[0]) : $now;
            $lastSeen = ! empty($timestamps) ? date('Y-m-d H:i:s', (int) end($timestamps)) : $now;
            $userCount = count($group['users']);

            $upsertStmt->execute([
                'type' => 'performance',
                'status' => 'open',
                'exception_class' => $group['name'],
                'exception_message' => 'Duration exceeded threshold',
                'group_hash' => $groupHash,
                'first_seen_at' => $firstSeen,
                'last_seen_at' => $lastSeen,
                'occurrences_count' => $group['count'],
                'users_count' => $userCount,
                'created_at' => $now,
                'updated_at' => $now,
            ]);
        }
    }
}
