<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\EmailTemplate;
use PHPUnit\Framework\TestCase;

class EmailTemplateTest extends TestCase
{
    public function test_subtype_label_maps_known_types(): void
    {
        $this->assertSame('Route', EmailTemplate::subtypeLabel('route'));
        $this->assertSame('Job', EmailTemplate::subtypeLabel('job'));
        $this->assertSame('Command', EmailTemplate::subtypeLabel('command'));
        $this->assertSame('Scheduled Task', EmailTemplate::subtypeLabel('scheduled_task'));
        $this->assertSame('Query', EmailTemplate::subtypeLabel('query'));
        $this->assertSame('Outgoing Request', EmailTemplate::subtypeLabel('outgoing_request'));
        $this->assertSame('Mail', EmailTemplate::subtypeLabel('mail'));
        $this->assertSame('Notification', EmailTemplate::subtypeLabel('notification'));
        $this->assertSame('Cache', EmailTemplate::subtypeLabel('cache'));
    }

    public function test_subtype_label_falls_back_to_title_cased_input(): void
    {
        $this->assertSame('Some Custom Type', EmailTemplate::subtypeLabel('some_custom_type'));
    }

    public function test_subtype_label_defaults_to_route_for_null(): void
    {
        $this->assertSame('Route', EmailTemplate::subtypeLabel(null));
    }

    public function test_render_issue_escapes_html_in_class_name(): void
    {
        $html = EmailTemplate::renderIssue('MyApp', [
            'class' => '<script>alert(1)</script>',
            'message' => 'normal message',
            'count' => 1,
            'users_count' => 0,
        ], 'exception', 'https://usenightowl.com');

        $this->assertStringNotContainsString('<script>alert(1)</script>', $html);
        $this->assertStringContainsString('&lt;script&gt;alert(1)&lt;/script&gt;', $html);
    }

    public function test_render_issue_includes_app_name(): void
    {
        $html = EmailTemplate::renderIssue('ShopApp', [
            'class' => 'DivisionByZeroError',
            'message' => 'division by zero',
            'count' => 7,
            'users_count' => 2,
        ], 'exception');

        $this->assertStringContainsString('ShopApp', $html);
        $this->assertStringContainsString('DivisionByZeroError', $html);
    }

    public function test_render_issue_truncates_long_messages(): void
    {
        $longMessage = str_repeat('x', 500);

        $html = EmailTemplate::renderIssue('App', [
            'class' => 'E',
            'message' => $longMessage,
            'count' => 1,
            'users_count' => 0,
        ], 'exception');

        // Truncated to 200 chars + ellipsis
        $this->assertStringContainsString(str_repeat('x', 200).'...', $html);
        $this->assertStringNotContainsString(str_repeat('x', 201), $html);
    }

    public function test_render_issue_for_performance_includes_subtype_label(): void
    {
        $html = EmailTemplate::renderIssue('App', [
            'name' => 'GET /users',
            'count' => 5,
            'users_count' => 1,
            'subtype' => 'route',
            'threshold_ms' => 500,
            'duration_ms' => 1200,
        ], 'performance');

        $this->assertStringContainsString('Route', $html);
        $this->assertStringContainsString('Performance', $html);
        $this->assertStringContainsString('500', $html);
    }

    public function test_render_issue_uses_frontend_url_for_logo(): void
    {
        $html = EmailTemplate::renderIssue('App', [
            'class' => 'E',
            'message' => 'm',
            'count' => 1,
            'users_count' => 0,
        ], 'exception', 'https://my.custom.example');

        $this->assertStringContainsString('https://my.custom.example/full-logo.png', $html);
    }

    public function test_render_issue_falls_back_to_default_logo_when_frontend_url_empty(): void
    {
        $html = EmailTemplate::renderIssue('App', [
            'class' => 'E',
            'message' => 'm',
            'count' => 1,
            'users_count' => 0,
        ], 'exception', '');

        $this->assertStringContainsString('https://usenightowl.com/full-logo.png', $html);
    }

    public function test_render_issue_includes_view_url_when_provided(): void
    {
        $html = EmailTemplate::renderIssue('App', [
            'class' => 'E',
            'message' => 'm',
            'count' => 1,
            'users_count' => 0,
            'view_url' => 'https://usenightowl.com/dashboard',
        ], 'exception');

        $this->assertStringContainsString('https://usenightowl.com/dashboard', $html);
        $this->assertStringContainsString('View issue', $html);
    }

    public function test_render_issue_omits_cta_button_when_no_view_url(): void
    {
        $html = EmailTemplate::renderIssue('App', [
            'class' => 'E',
            'message' => 'm',
            'count' => 1,
            'users_count' => 0,
        ], 'exception');

        $this->assertStringNotContainsString('View issue', $html);
    }
}
