<?php

namespace NightOwl\Notifications;

use Illuminate\Bus\Queueable;
use Illuminate\Notifications\Messages\MailMessage;
use Illuminate\Notifications\Messages\SlackMessage;
use Illuminate\Notifications\Notification;

class ExceptionOccurred extends Notification
{
    use Queueable;

    public function __construct(
        private string $exceptionClass,
        private string $message,
        private string $file,
        private int|string|null $line,
        private ?string $traceId = null,
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
            ->subject("[NightOwl] New Exception: {$this->exceptionClass}")
            ->greeting("New exception in {$appName}")
            ->line("**{$this->exceptionClass}**")
            ->line($this->message)
            ->line("File: {$this->file}:{$this->line}")
            ->action('View in NightOwl', 'https://api.usenightowl.com/exceptions');
    }

    public function toSlack(object $notifiable): SlackMessage
    {
        $appName = config('app.name', 'Laravel');

        return (new SlackMessage)
            ->error()
            ->content("New exception in {$appName}")
            ->attachment(function ($attachment) {
                $attachment
                    ->title($this->exceptionClass)
                    ->content($this->message)
                    ->fields([
                        'File' => "{$this->file}:{$this->line}",
                    ]);
            });
    }
}
