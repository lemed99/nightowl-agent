# NightOwl Agent

Self-hosted Laravel application monitoring. Collects telemetry from [laravel/nightwatch](https://github.com/laravel/nightwatch) and writes it to your PostgreSQL database.

## Requirements

- PHP 8.2+
- Laravel 11 or 12
- PostgreSQL 15+
- PHP extensions: `pdo_pgsql`, `pdo_sqlite`
- Recommended: `pcntl`, `posix` (for the async driver)

## Installation

### 1. Create an account

Sign up at your NightOwl dashboard, create a new app, and provide your PostgreSQL database credentials. You'll receive an agent token — copy it, you'll only see it once.

### 2. Install the package

```bash
composer require nightowl/agent
```

### 3. Configure environment

Add these to your `.env`:

```env
NIGHTWATCH_TOKEN=your-token-from-the-dashboard

NIGHTOWL_DB_HOST=127.0.0.1
NIGHTOWL_DB_PORT=5432
NIGHTOWL_DB_DATABASE=nightowl
NIGHTOWL_DB_USERNAME=nightowl
NIGHTOWL_DB_PASSWORD=your-db-password
```

> The `NIGHTWATCH_TOKEN` is used by both Nightwatch (to send telemetry) and the NightOwl agent (to validate it). Use the same database credentials you entered when creating the app in the dashboard.

### 4. Run the installer

```bash
php artisan nightowl:install
```

This publishes the config file and runs migrations to create the monitoring tables in your database.

### 5. Install Nightwatch

NightOwl receives telemetry from Laravel's Nightwatch package:

```bash
composer require laravel/nightwatch
```

### 6. Ensure the buffer directory exists

The agent uses a local SQLite file to buffer data before draining to PostgreSQL. Make sure the storage directory exists and is writable:

```bash
mkdir -p storage/nightowl && chmod 775 storage/nightowl
```

> **Docker users**: This step is required inside your container before starting the agent.

### 7. Start the agent

```bash
php artisan nightowl:agent
```

The agent listens on port 2407 by default. Nightwatch sends telemetry to the agent, which buffers it locally and drains to your PostgreSQL database.

Open your NightOwl dashboard to see monitoring data.

## Running in Production

Use a process manager to keep the agent running:

**Supervisor** (recommended):

```ini
[program:nightowl-agent]
command=php /path/to/your/app/artisan nightowl:agent
autostart=true
autorestart=true
user=www-data
redirect_stderr=true
stdout_logfile=/var/log/nightowl-agent.log
```

**systemd:**

```ini
[Unit]
Description=NightOwl Agent
After=network.target postgresql.service

[Service]
User=www-data
WorkingDirectory=/path/to/your/app
ExecStart=/usr/bin/php artisan nightowl:agent
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## Configuration

All configuration is in `config/nightowl.php` after running the installer. Key environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `NIGHTWATCH_TOKEN` | — | Token from the dashboard (used by both Nightwatch and the agent) |
| `NIGHTOWL_DB_HOST` | `127.0.0.1` | PostgreSQL host |
| `NIGHTOWL_DB_PORT` | `5432` | PostgreSQL port |
| `NIGHTOWL_DB_DATABASE` | `nightowl` | PostgreSQL database name |
| `NIGHTOWL_DB_USERNAME` | `nightowl` | PostgreSQL username |
| `NIGHTOWL_DB_PASSWORD` | `nightowl` | PostgreSQL password |
| `NIGHTOWL_AGENT_PORT` | `2407` | TCP port the agent listens on |
| `NIGHTOWL_AGENT_DRIVER` | `async` | Server driver (`async` or `sync`) |
| `NIGHTOWL_DRAIN_WORKERS` | `1` | Number of parallel drain workers |
| `NIGHTOWL_SAMPLE_RATE` | `1.0` | Global sampling rate (1.0 = keep all, exceptions always kept) |
| `NIGHTOWL_REQUEST_SAMPLE_RATE` | — | Override sample rate for HTTP requests |
| `NIGHTOWL_COMMAND_SAMPLE_RATE` | — | Override sample rate for artisan commands |
| `NIGHTOWL_SCHEDULED_TASK_SAMPLE_RATE` | — | Override sample rate for scheduled tasks |
| `NIGHTOWL_THRESHOLD_CACHE_TTL` | `86400` | Seconds to cache performance thresholds (restart agent to pick up changes immediately) |
| `NIGHTOWL_RETENTION_DAYS` | `14` | Days to keep monitoring data |

## Commands

| Command | Description |
|---------|-------------|
| `nightowl:agent` | Start the monitoring agent |
| `nightowl:install` | Publish config and run migrations |
| `nightowl:prune` | Delete monitoring data older than retention period |
| `nightowl:clear` | Truncate all monitoring tables |

## Data Retention

NightOwl never deletes your data automatically — you have full control. Use the `nightowl:prune` command to clean up old monitoring data when you're ready:

```bash
# Delete data older than 14 days (default)
php artisan nightowl:prune

# Delete data older than 30 days
php artisan nightowl:prune --days=30
```

To automate pruning, add it to your app's scheduler in `routes/console.php`:

```php
use Illuminate\Support\Facades\Schedule;

Schedule::command('nightowl:prune --days=14')->daily();
```

The default retention is controlled by `NIGHTOWL_RETENTION_DAYS` in your `.env` (defaults to 14 days). This only affects `nightowl:prune` — no data is deleted unless you run the command.

## Filtering & Context

NightOwl works with Nightwatch's built-in filtering. Since the agent receives whatever Nightwatch sends, these features work out of the box:

**Sampling** — Control how much data is collected per entry point type:

```env
NIGHTOWL_REQUEST_SAMPLE_RATE=0.1        # Keep ~10% of requests
NIGHTOWL_COMMAND_SAMPLE_RATE=1.0         # Keep all commands
NIGHTOWL_SCHEDULED_TASK_SAMPLE_RATE=1.0  # Keep all scheduled tasks
```

Exceptions and 5xx requests are always kept regardless of sample rate. When an entry point is sampled in, the entire trace (queries, cache, logs, etc.) is captured.

**In-code filtering** — Use Nightwatch's filtering API to exclude specific events:

```php
// Exclude events within this callback from capture
Nightwatch::ignore(function () {
    // queries, jobs, etc. here won't be recorded
});

// Pause/resume for finer control
Nightwatch::pause();
// ... unmonitored code ...
Nightwatch::resume();
```

**Context metadata** — Attach custom key-value data to traces using Laravel Context (11+):

```php
Context::add('user_role', $user->role);
Context::add('feature_flags', ['new-checkout' => true]);
Context::add('tenant', ['id' => $tenant->id, 'plan' => 'pro']);
```

Context data is captured by Nightwatch, stored by the agent, and displayed in the request detail page in the NightOwl dashboard as a collapsible JSON tree. Maximum 65KB per request.

**Redaction** — The agent can redact sensitive data before writing to the database:

```env
NIGHTOWL_REDACT_ENABLED=true
NIGHTOWL_REDACT_KEYS=password,token,authorization,cookie,secret
```

## How It Works

```
Your Laravel App
  └─ laravel/nightwatch (collects telemetry)
       └─ TCP → NightOwl Agent (port 2407)
            └─ SQLite buffer (local, crash-safe)
                 └─ PostgreSQL (your database)

NightOwl Dashboard (hosted)
  └─ Reads from your PostgreSQL database
  └─ Receives health reports from the agent
```

The agent never sends your application data to the dashboard. It writes directly to your database. The dashboard connects to your database (using the credentials you provided) to display monitoring data. The only thing sent to the dashboard is agent health status.

## License

MIT
