<?php

namespace NightOwl\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use NightOwl\Agent\AlertNotifier;

/**
 * Send a test alert through the AGENT's own dispatchers.
 *
 * The gap this closes: the dashboard's "send test" button, and every triage
 * alert (issue.resolved, issue.ignored), are dispatched by nightowl-api using
 * Symfony Mailer. `issue.new` and `issue.reopened` are dispatched by the agent
 * on this machine, over its own raw SMTP and HTTP. Two independent transports
 * reading one config row — so a customer can test green in the dashboard,
 * receive triage mail all day, and never once get an alert about a new
 * exception, with nothing but an error_log line to say why.
 *
 * This command runs the second transport. It is the only thing that does.
 */
class TestAlertCommand extends Command
{
    protected $signature = 'nightowl:test-alert
        {--channel= : Only test the channel with this name}';

    protected $description = "Send a test alert through the agent's own dispatchers (not the dashboard's)";

    public function handle(): int
    {
        $only = $this->option('channel');
        $appName = (string) config('app.name', 'NightOwl');

        try {
            $pdo = DB::connection('nightowl')->getPdo();
        } catch (\Throwable $e) {
            $this->error('Cannot reach the NightOwl database: '.$e->getMessage());
            $this->line('Alert channels live in your monitoring database — check NIGHTOWL_DB_* in .env.');

            return self::FAILURE;
        }

        $this->line('Sending a test alert as the agent, through every enabled channel.');
        $this->newLine();

        // Same resolution order the drain stamps on every telemetry row, so the
        // test alert lands in the environment the real alerts would.
        $environment = (string) (config('nightowl.environment') ?: config('app.env', 'production'));

        $results = AlertNotifier::fromConfig()->sendTestNotifications(
            $pdo,
            $appName,
            is_string($only) && $only !== '' ? $only : null,
            $environment,
        );

        if ($results === []) {
            // Two very different situations, and the distinction is the whole
            // answer when someone is asking why they get no alerts.
            if (is_string($only) && $only !== '') {
                $this->error("No enabled channel named '{$only}'.");
            } else {
                $this->warn('No enabled alert channels are configured for this app.');
                $this->line('Add one in the dashboard under Settings → Alerts, then run this again.');
            }

            return self::FAILURE;
        }

        $failed = 0;

        foreach ($results as $result) {
            $label = "{$result['name']} ({$result['type']})";

            if ($result['ok']) {
                $this->info("  PASS  {$label}");
            } else {
                $failed++;
                $this->error("  FAIL  {$label}");
                $this->line("        {$result['error']}");
            }

            // A channel can accept the test and still stay silent in
            // production, because the test deliberately ignores notify_events
            // to isolate transport from filtering. Say so, or the pass is
            // misleading.
            if ($result['muted_events'] !== []) {
                $this->warn('        this channel has '.implode(' and ', $result['muted_events']).' switched off — it will not alert on those in production');
            }
        }

        $this->newLine();

        if ($failed > 0) {
            $this->error("{$failed} of ".count($results).' channel(s) failed.');

            return self::FAILURE;
        }

        $this->info('All '.count($results).' channel(s) accepted the test alert.');
        $this->line('If email was accepted but nothing arrives, check the recipient\'s spam folder and your relay\'s outbound logs — delivery after the 250 is out of the agent\'s hands.');

        return self::SUCCESS;
    }
}
