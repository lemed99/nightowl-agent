<?php

namespace NightOwl\Notifications;

use Illuminate\Bus\Queueable;
use Illuminate\Notifications\Messages\MailMessage;
use Illuminate\Notifications\Messages\SlackMessage;
use Illuminate\Notifications\Notification;

class ThresholdExceeded extends Notification
{
    use Queueable;

    public function __construct(
        private string $type,
        private string $title,
        private string $message,
    ) {}

    public function via(object $notifiable): array
    {
        $channels = [];

        if (config('nightowl.alerts.mail_to')) {
            $channels[] = 'mail';
        }

        if (config('nightowl.alerts.slack_webhook')) {
            $channels[] = 'slack';
        }

        return $channels;
    }

    public function toMail(object $notifiable): MailMessage
    {
        $appName = config('app.name', 'Laravel');

        return (new MailMessage)
            ->subject("[NightOwl] Alert: {$this->title}")
            ->greeting("Performance alert for {$appName}")
            ->line("**{$this->title}**")
            ->line($this->message)
            ->action('View Dashboard', 'https://api.usenightowl.com');
    }

    public function toSlack(object $notifiable): SlackMessage
    {
        $appName = config('app.name', 'Laravel');

        return (new SlackMessage)
            ->warning()
            ->content("Performance alert for {$appName}: {$this->title}")
            ->attachment(function ($attachment) {
                $attachment
                    ->title($this->title)
                    ->content($this->message);
            });
    }
}
