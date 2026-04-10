<?php

namespace NightOwl\Notifications;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Mail\Mailable;
use Illuminate\Notifications\Messages\SlackMessage;
use Illuminate\Notifications\Notification;
use NightOwl\Agent\EmailTemplate;

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

    public function toMail(object $notifiable): Mailable
    {
        $appName = config('app.name', 'Laravel');

        $html = EmailTemplate::renderThreshold($appName, $this->type, $this->title, $this->message);

        return new BrandedMail($html, "[NightOwl] Alert: {$this->title}");
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
