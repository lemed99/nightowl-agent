<?php

namespace NightOwl\Commands;

use Illuminate\Console\Command;
use NightOwl\Agent\AsyncServer;
use NightOwl\Agent\Server;

class AgentCommand extends Command
{
    protected $signature = 'nightowl:agent
        {--host= : The host to listen on}
        {--port= : The port to listen on}
        {--driver= : Server driver (async or sync)}
        {--sqlite-path= : SQLite buffer file path (required for multi-instance, overrides config)}';

    protected $description = 'Start the NightOwl monitoring agent';

    public function handle(): int
    {
        // The agent is a long-running daemon with its own memory back-pressure
        // system (totalBufferBytes inline check + RSS periodic check). PHP's
        // memory_limit would cause an ungraceful fatal crash that bypasses all
        // of that. Default memory_limit is 128M on some distros, which is below
        // our default max_buffer_memory (256MB).
        ini_set('memory_limit', '-1');

        $host = $this->option('host') ?? config('nightowl.agent.host', '127.0.0.1');
        $port = (int) ($this->option('port') ?? config('nightowl.agent.port', 2407));
        $driver = $this->option('driver') ?? config('nightowl.agent.driver', 'async');

        // --sqlite-path overrides config. This is critical for multi-instance
        // deployment: env vars are ignored when Laravel config is cached
        // (php artisan config:cache), but CLI options always work.
        if ($this->option('sqlite-path')) {
            config()->set('nightowl.agent.sqlite_path', $this->option('sqlite-path'));
        }

        if ($driver === 'async') {
            return $this->runAsync($host, $port);
        }

        return $this->runSync($host, $port);
    }

    private function runAsync(string $host, int $port): int
    {
        if (! function_exists('pcntl_fork') || ! function_exists('posix_kill')) {
            $this->error('The async driver requires the pcntl and posix PHP extensions.');
            $this->line('Run with --driver=sync to use the synchronous fallback, or install the missing extensions.');

            return self::FAILURE;
        }

        $server = app(AsyncServer::class);

        $this->info("NightOwl agent (async) listening on {$host}:{$port}");
        $this->line('SQLite buffer: ' . config('nightowl.agent.sqlite_path'));

        if (config('nightowl.agent.enable_udp', false)) {
            $this->line('UDP listener: ' . $host . ':' . config('nightowl.agent.udp_port', 2408));
        }

        if (config('nightowl.agent.health_enabled', true)) {
            $healthPort = config('nightowl.agent.health_port', 2409);
            $this->line("Health API: http://{$host}:{$healthPort}/status");
        }

        $this->line('Press Ctrl+C to stop.');

        $server->listen($host, $port);

        $this->info('NightOwl agent stopped.');

        return self::SUCCESS;
    }

    private function runSync(string $host, int $port): int
    {
        $server = app(Server::class);

        $this->info("NightOwl agent (sync) listening on {$host}:{$port}");
        $this->line('Press Ctrl+C to stop.');

        if (function_exists('pcntl_async_signals')) {
            pcntl_async_signals(true);
            pcntl_signal(SIGINT, fn () => $server->stop());
            pcntl_signal(SIGTERM, fn () => $server->stop());
        }

        $server->listen($host, $port);

        $this->info('NightOwl agent stopped.');

        return self::SUCCESS;
    }
}
