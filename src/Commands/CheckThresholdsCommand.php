<?php

namespace NightOwl\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Notification;
use NightOwl\Notifications\ThresholdExceeded;

class CheckThresholdsCommand extends Command
{
    protected $signature = 'nightowl:check-thresholds';

    protected $description = 'Check alert thresholds and send notifications';

    public function handle(): int
    {
        if (! config('nightowl.alerts.enabled', true)) {
            return self::SUCCESS;
        }

        $conn = DB::connection('nightowl');
        $windowMinutes = config('nightowl.alerts.threshold_window_minutes', 5);
        $since = now()->subMinutes($windowMinutes)->toDateTimeString();

        $this->checkErrorRate($conn, $since);
        $this->checkAvgDuration($conn, $since);

        return self::SUCCESS;
    }

    private function checkErrorRate($conn, string $since): void
    {
        $totalCount = $conn->table('nightowl_requests')
            ->where('created_at', '>', $since)
            ->count();

        if ($totalCount === 0) {
            return;
        }

        $errorCount = $conn->table('nightowl_requests')
            ->where('created_at', '>', $since)
            ->where('status_code', '>=', 500)
            ->count();

        $rate = ($errorCount / $totalCount) * 100;
        $threshold = config('nightowl.alerts.error_rate_threshold', 5);

        if ($rate <= $threshold) {
            return;
        }

        // Check cooldown
        $cooldown = config('nightowl.alerts.cooldown_minutes', 60);
        $cooldownSince = now()->subMinutes($cooldown)->toDateTimeString();

        $existing = $conn->table('nightowl_alerts')
            ->where('type', 'high_error_rate')
            ->where('created_at', '>', $cooldownSince)
            ->count();

        if ($existing > 0) {
            return;
        }

        $conn->table('nightowl_alerts')->insert([
            'type' => 'high_error_rate',
            'title' => sprintf('High error rate: %.1f%%', $rate),
            'message' => sprintf('%d errors out of %d requests in the last %d minutes', $errorCount, $totalCount, config('nightowl.alerts.threshold_window_minutes', 5)),
            'metadata' => json_encode(['rate' => $rate, 'errors' => $errorCount, 'total' => $totalCount]),
            'created_at' => now(),
            'updated_at' => now(),
        ]);

        $this->sendNotification('high_error_rate', sprintf('High error rate: %.1f%%', $rate), sprintf('%d errors out of %d requests', $errorCount, $totalCount));
    }

    private function checkAvgDuration($conn, string $since): void
    {
        $avg = $conn->table('nightowl_requests')
            ->where('created_at', '>', $since)
            ->avg('duration');

        if (! $avg) {
            return;
        }

        $thresholdMs = config('nightowl.alerts.avg_duration_threshold_ms', 2000);
        $thresholdUs = $thresholdMs * 1000;

        if ($avg <= $thresholdUs) {
            return;
        }

        // Check cooldown
        $cooldown = config('nightowl.alerts.cooldown_minutes', 60);
        $cooldownSince = now()->subMinutes($cooldown)->toDateTimeString();

        $existing = $conn->table('nightowl_alerts')
            ->where('type', 'slow_response')
            ->where('created_at', '>', $cooldownSince)
            ->count();

        if ($existing > 0) {
            return;
        }

        $avgMs = round($avg / 1000);

        $conn->table('nightowl_alerts')->insert([
            'type' => 'slow_response',
            'title' => "Slow average response: {$avgMs}ms",
            'message' => "Average response time ({$avgMs}ms) exceeds threshold ({$thresholdMs}ms)",
            'metadata' => json_encode(['avg_ms' => $avgMs, 'threshold_ms' => $thresholdMs]),
            'created_at' => now(),
            'updated_at' => now(),
        ]);

        $this->sendNotification('slow_response', "Slow average response: {$avgMs}ms", "Average response time ({$avgMs}ms) exceeds threshold ({$thresholdMs}ms)");
    }

    private function sendNotification(string $type, string $title, string $message): void
    {
        $channels = config('nightowl.alerts.channels', ['mail']);
        $mailTo = config('nightowl.alerts.mail_to');

        if (in_array('mail', $channels) && $mailTo) {
            try {
                Notification::route('mail', $mailTo)
                    ->notify(new ThresholdExceeded($type, $title, $message));
            } catch (\Throwable $e) {
                error_log("[NightOwl] Failed to send alert notification: {$e->getMessage()}");
            }
        }
    }
}
