CREATE TABLE IF NOT EXISTS nightowl_requests (
    id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), group_hash VARCHAR(255), user_id VARCHAR(255), method VARCHAR(255) NOT NULL DEFAULT 'GET', url TEXT NOT NULL DEFAULT '/', route_name VARCHAR(255), route_methods TEXT, route_domain VARCHAR(255), route_path VARCHAR(255), route_action VARCHAR(255), ip VARCHAR(255), duration INTEGER, status_code INTEGER NOT NULL DEFAULT 200, request_size INTEGER, response_size INTEGER, bootstrap INTEGER, before_middleware INTEGER, action INTEGER, render INTEGER, after_middleware INTEGER, sending INTEGER, terminating INTEGER, exceptions INTEGER DEFAULT 0, logs INTEGER DEFAULT 0, queries INTEGER DEFAULT 0, lazy_loads INTEGER DEFAULT 0, jobs_queued INTEGER DEFAULT 0, mail INTEGER DEFAULT 0, notifications INTEGER DEFAULT 0, outgoing_requests INTEGER DEFAULT 0, files_read INTEGER DEFAULT 0, files_written INTEGER DEFAULT 0, cache_events INTEGER DEFAULT 0, hydrated_models INTEGER DEFAULT 0, peak_memory_usage INTEGER, exception_preview TEXT, context TEXT, headers TEXT, payload TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS nightowl_queries (
    id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), group_hash VARCHAR(255), execution_source VARCHAR(255), execution_id VARCHAR(255), execution_stage VARCHAR(255), user_id VARCHAR(255), sql_query TEXT NOT NULL DEFAULT '', file VARCHAR(255), line INTEGER, duration INTEGER, connection VARCHAR(255), connection_type VARCHAR(255), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS nightowl_exceptions (
    id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), execution_source VARCHAR(255), execution_id VARCHAR(255), execution_stage VARCHAR(255), user_id VARCHAR(255), class VARCHAR(255) NOT NULL DEFAULT 'Unknown', message TEXT, code VARCHAR(255), file VARCHAR(255), line INTEGER, trace TEXT, php_version VARCHAR(255), laravel_version VARCHAR(255), handled BOOLEAN DEFAULT false, fingerprint VARCHAR(255) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS nightowl_commands (
    id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), group_hash VARCHAR(255), user_id VARCHAR(255), command VARCHAR(255) NOT NULL DEFAULT 'unknown', exit_code INTEGER, duration INTEGER, exceptions INTEGER DEFAULT 0, logs INTEGER DEFAULT 0, queries INTEGER DEFAULT 0, lazy_loads INTEGER DEFAULT 0, jobs_queued INTEGER DEFAULT 0, mail INTEGER DEFAULT 0, notifications INTEGER DEFAULT 0, outgoing_requests INTEGER DEFAULT 0, files_read INTEGER DEFAULT 0, files_written INTEGER DEFAULT 0, cache_events INTEGER DEFAULT 0, hydrated_models INTEGER DEFAULT 0, peak_memory_usage INTEGER, exception_preview TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS nightowl_jobs (
    id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), group_hash VARCHAR(255), execution_source VARCHAR(255), execution_id VARCHAR(255), user_id VARCHAR(255), job_class VARCHAR(255) NOT NULL DEFAULT 'Unknown', queue VARCHAR(255), connection VARCHAR(255), status VARCHAR(255), duration INTEGER, attempts INTEGER DEFAULT 1, exceptions INTEGER DEFAULT 0, logs INTEGER DEFAULT 0, queries INTEGER DEFAULT 0, lazy_loads INTEGER DEFAULT 0, jobs_queued INTEGER DEFAULT 0, mail INTEGER DEFAULT 0, notifications INTEGER DEFAULT 0, outgoing_requests INTEGER DEFAULT 0, files_read INTEGER DEFAULT 0, files_written INTEGER DEFAULT 0, cache_events INTEGER DEFAULT 0, hydrated_models INTEGER DEFAULT 0, peak_memory_usage INTEGER, exception_preview TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS nightowl_cache_events (
    id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), execution_source VARCHAR(255), execution_id VARCHAR(255), execution_stage VARCHAR(255), user_id VARCHAR(255), event_type VARCHAR(255) NOT NULL DEFAULT 'unknown', key VARCHAR(255) NOT NULL DEFAULT '', store VARCHAR(255), ttl INTEGER, duration INTEGER, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS nightowl_mail (
    id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), execution_source VARCHAR(255), execution_id VARCHAR(255), execution_stage VARCHAR(255), user_id VARCHAR(255), mailer VARCHAR(255), recipients TEXT, subject VARCHAR(255), mailable VARCHAR(255), duration INTEGER, queued BOOLEAN DEFAULT false, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS nightowl_notifications (
    id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), execution_source VARCHAR(255), execution_id VARCHAR(255), execution_stage VARCHAR(255), user_id VARCHAR(255), notification VARCHAR(255), channel VARCHAR(255), notifiable_type VARCHAR(255), notifiable_id VARCHAR(255), duration INTEGER, queued BOOLEAN DEFAULT false, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS nightowl_outgoing_requests (
    id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), execution_source VARCHAR(255), execution_id VARCHAR(255), execution_stage VARCHAR(255), user_id VARCHAR(255), method VARCHAR(255) NOT NULL DEFAULT 'GET', url TEXT NOT NULL DEFAULT '', status_code INTEGER, duration INTEGER, request_size INTEGER, response_size INTEGER, request_headers TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS nightowl_scheduled_tasks (
    id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), group_hash VARCHAR(255), user_id VARCHAR(255), command VARCHAR(255) NOT NULL DEFAULT 'unknown', expression VARCHAR(255), status VARCHAR(255), duration INTEGER, exit_code INTEGER, exceptions INTEGER DEFAULT 0, logs INTEGER DEFAULT 0, queries INTEGER DEFAULT 0, lazy_loads INTEGER DEFAULT 0, jobs_queued INTEGER DEFAULT 0, mail INTEGER DEFAULT 0, notifications INTEGER DEFAULT 0, outgoing_requests INTEGER DEFAULT 0, files_read INTEGER DEFAULT 0, files_written INTEGER DEFAULT 0, cache_events INTEGER DEFAULT 0, hydrated_models INTEGER DEFAULT 0, peak_memory_usage INTEGER, exception_preview TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS nightowl_logs (
    id BIGSERIAL PRIMARY KEY, trace_id VARCHAR(255) NOT NULL, timestamp VARCHAR(255), deploy VARCHAR(255), server VARCHAR(255), execution_source VARCHAR(255), execution_id VARCHAR(255), execution_stage VARCHAR(255), user_id VARCHAR(255), level VARCHAR(255) DEFAULT 'info', message TEXT, context TEXT, channel VARCHAR(255), created_at VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS nightowl_users (
    user_id VARCHAR(255) PRIMARY KEY, name VARCHAR(255), email VARCHAR(255), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS nightowl_issues (
    id BIGSERIAL PRIMARY KEY, type VARCHAR(255) NOT NULL, status VARCHAR(255) DEFAULT 'open', priority VARCHAR(255), exception_class VARCHAR(255), exception_message TEXT, group_hash VARCHAR(255), first_seen_at TIMESTAMP, last_seen_at TIMESTAMP, occurrences_count INTEGER DEFAULT 0, users_count INTEGER DEFAULT 0, assigned_to VARCHAR(255), description TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, UNIQUE (group_hash, type)
);
CREATE TABLE IF NOT EXISTS nightowl_issue_comments (
    id BIGSERIAL PRIMARY KEY, issue_id BIGINT NOT NULL REFERENCES nightowl_issues(id) ON DELETE CASCADE, user_id BIGINT, user_name VARCHAR(255), user_email VARCHAR(255), body TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS nightowl_issue_activity (
    id BIGSERIAL PRIMARY KEY, issue_id BIGINT NOT NULL REFERENCES nightowl_issues(id) ON DELETE CASCADE, user_id BIGINT, user_name VARCHAR(255), action VARCHAR(50) NOT NULL, old_value VARCHAR(255), new_value VARCHAR(255), created_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS nightowl_settings (
    id BIGSERIAL PRIMARY KEY, key VARCHAR(255) NOT NULL UNIQUE, value TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS nightowl_alert_channels (
    id BIGSERIAL PRIMARY KEY, type VARCHAR(255) NOT NULL, name VARCHAR(255) NOT NULL, config TEXT NOT NULL DEFAULT '{}', enabled BOOLEAN NOT NULL DEFAULT true, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
