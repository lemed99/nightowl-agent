<?php

namespace NightOwl\Notifications;

use Illuminate\Mail\Mailable;

/**
 * Simple Mailable that sends pre-rendered HTML.
 *
 * Used by ThresholdExceeded to bypass MailMessage's markdown
 * and send branded NightOwl HTML emails instead.
 */
final class BrandedMail extends Mailable
{
    public function __construct(
        private string $htmlContent,
        private string $mailSubject,
    ) {}

    public function build(): static
    {
        return $this->subject($this->mailSubject)->html($this->htmlContent);
    }
}
