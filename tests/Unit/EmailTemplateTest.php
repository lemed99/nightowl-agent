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

    // ─── Event badge ─────────────────────────────────────────────────

    /**
     * The badge knew only exception vs performance, so a reopened exception
     * said "New Issue" under a "Reopened Issue" subject. The API's template
     * (issue.blade.php) and its approved preview both show "Reopened".
     */
    public function test_a_reopened_exception_gets_the_reopened_badge(): void
    {
        $html = EmailTemplate::renderIssue('App', [
            'class' => 'RuntimeException',
            'message' => 'boom',
            'count' => 4,
            'users_count' => 1,
        ], 'exception', '', 'issue.reopened');

        $this->assertStringContainsString('border-left:3px solid #ea580c;">Reopened</td>', $html);
        $this->assertStringContainsString('background-color:#382216;color:#fb923c', $html);
        $this->assertStringNotContainsString('>New Issue</td>', $html);
    }

    public function test_a_reopened_performance_issue_gets_the_reopened_badge(): void
    {
        $html = EmailTemplate::renderIssue('App', [
            'name' => 'GET /users',
            'count' => 2,
            'users_count' => 0,
            'subtype' => 'route',
            'threshold_ms' => 500,
            'duration_ms' => 900,
        ], 'performance', '', 'issue.reopened');

        $this->assertStringContainsString('>Reopened</td>', $html);
        $this->assertStringNotContainsString('>Performance Alert</td>', $html);
    }

    public function test_new_issues_keep_their_badges(): void
    {
        $exception = EmailTemplate::renderIssue('App', ['class' => 'E', 'count' => 1], 'exception');
        $performance = EmailTemplate::renderIssue('App', ['name' => 'GET /', 'subtype' => 'route', 'count' => 1], 'performance', '', 'issue.new');

        $this->assertStringContainsString('border-left:3px solid #dc2626;">New Issue</td>', $exception);
        $this->assertStringContainsString('border-left:3px solid #f59e0b;">Performance Alert</td>', $performance);
        $this->assertStringNotContainsString('>Reopened</td>', $exception.$performance);
    }

    // ─── Performance message ─────────────────────────────────────────

    /**
     * Worker saturation has no threshold or duration; its numbers are in the
     * message, which the template printed for exceptions only. The email said
     * "Exceeded configured threshold." and nothing else.
     */
    public function test_a_worker_saturation_email_prints_the_numbers_from_the_message(): void
    {
        $message = 'Avg concurrency held at ≥80% of 8 workers for 5 consecutive minutes (worst minute: 7.4 busy)';

        $html = EmailTemplate::renderIssue('App', [
            'name' => 'Worker saturation',
            'subtype' => 'worker_saturation',
            'message' => $message,
            'count' => 1,
            'users_count' => 0,
        ], 'performance');

        $this->assertStringContainsString('>Message</span>', $html);
        $this->assertStringContainsString($message, $html);
        $this->assertStringNotContainsString('Exceeded configured threshold.', $html);

        // The name only restates the subtype, so its own section is dropped
        // (the title already says "Worker saturation").
        $this->assertStringNotContainsString('>Worker Saturation</span>', $html);
    }

    public function test_a_threshold_issue_keeps_its_duration_and_threshold_rows(): void
    {
        $html = EmailTemplate::renderIssue('App', [
            'name' => 'GET /users',
            'subtype' => 'route',
            'count' => 3,
            'users_count' => 1,
            'threshold_ms' => 500,
            'duration_ms' => 1200,
        ], 'performance');

        $this->assertStringContainsString('>Route</span>', $html);
        $this->assertStringContainsString('GET /users', $html);
        $this->assertStringContainsString('1,200ms', $html);
        $this->assertStringContainsString('500ms', $html);
        $this->assertStringContainsString('700ms (+140%)', $html);
        $this->assertStringNotContainsString('>Message</span>', $html);
    }

    public function test_a_performance_issue_with_no_numbers_and_no_message_keeps_the_fallback_line(): void
    {
        $html = EmailTemplate::renderIssue('App', [
            'name' => 'GET /users',
            'subtype' => 'route',
            'count' => 1,
        ], 'performance');

        $this->assertStringContainsString('Exceeded configured threshold.', $html);
    }

    // ─── Preheader ───────────────────────────────────────────────────

    public function test_the_preheader_says_one_occurrence_in_the_singular(): void
    {
        $one = EmailTemplate::renderIssue('App', ['class' => 'E', 'count' => 1], 'exception');
        $many = EmailTemplate::renderIssue('App', ['class' => 'E', 'count' => 3], 'exception');

        $this->assertStringContainsString('Exception: E — 1 occurrence</div>', $one);
        $this->assertStringNotContainsString('1 occurrences', $one);
        $this->assertStringContainsString('Exception: E — 3 occurrences</div>', $many);
    }

    /** The preheader was built from the escaped title and escaped again. */
    public function test_the_preheader_is_escaped_once(): void
    {
        $html = EmailTemplate::renderIssue('App', [
            'name' => 'GET /search?a=1&b=2',
            'subtype' => 'route',
            'count' => 2,
            'threshold_ms' => 100,
        ], 'performance');

        $this->assertStringContainsString('GET /search?a=1&amp;b=2 exceeded 100ms threshold — 2 occurrences</div>', $html);
        $this->assertStringNotContainsString('&amp;amp;', $html);
    }

    // ─── Health alerts ───────────────────────────────────────────────

    private function diagnosis(string $level = 'critical'): array
    {
        return [
            'code' => 'DRAIN_STOPPED',
            'level' => $level,
            'message' => 'Drain has not written for 120s',
            'recommendation' => 'Check PostgreSQL connectivity',
            'value' => 120,
        ];
    }

    public function test_a_health_alert_links_to_the_agent_health_page(): void
    {
        $html = EmailTemplate::renderHealth(
            'ShopApp',
            $this->diagnosis(),
            'degraded',
            'web-1:2407',
            'https://dash.example/dashboard/app_1/health',
            'https://dash.example',
        );

        $this->assertStringContainsString('<a href="https://dash.example/dashboard/app_1/health"', $html);
        $this->assertStringContainsString('>View agent health</a>', $html);
        $this->assertStringContainsString('>Agent Health Alert</td>', $html);
        $this->assertStringContainsString('>Critical</td>', $html);
        $this->assertStringContainsString('DRAIN_STOPPED', $html);
        $this->assertStringContainsString('Drain has not written for 120s', $html);
        $this->assertStringContainsString('Check PostgreSQL connectivity', $html);
        $this->assertStringContainsString('web-1:2407', $html);
        $this->assertStringContainsString('ShopApp', $html);
        $this->assertStringContainsString('https://dash.example/full-logo.png', $html);
    }

    public function test_a_health_alert_without_a_dashboard_url_has_no_button(): void
    {
        $html = EmailTemplate::renderHealth('App', $this->diagnosis(), 'degraded', '', null, '');

        $this->assertStringNotContainsString('View agent health', $html);
        $this->assertStringNotContainsString('/health', $html);
        $this->assertStringNotContainsString('>Instance<', $html);
    }

    public function test_a_recovery_uses_the_resolved_badge_and_drops_the_recommendation(): void
    {
        $html = EmailTemplate::renderHealth('App', $this->diagnosis(), 'recovered', '', 'https://dash.example/dashboard', '');

        $this->assertStringContainsString('border-left:3px solid #16a34a;">Recovered</td>', $html);
        $this->assertStringContainsString('>Resolved</td>', $html);
        $this->assertStringNotContainsString('Check PostgreSQL connectivity', $html);
        $this->assertStringContainsString('<title>Recovered: DRAIN_STOPPED</title>', $html);
    }

    public function test_a_warning_uses_the_amber_badge(): void
    {
        $html = EmailTemplate::renderHealth('App', $this->diagnosis('warning'), 'degraded');

        $this->assertStringContainsString('border-left:3px solid #f59e0b;">Agent Health Alert</td>', $html);
        $this->assertStringContainsString('>Warning</td>', $html);
    }

    public function test_a_health_alert_escapes_diagnosis_text(): void
    {
        $d = $this->diagnosis();
        $d['message'] = '<img src=x onerror=alert(1)>';

        $html = EmailTemplate::renderHealth('App', $d, 'degraded');

        $this->assertStringNotContainsString('<img src=x', $html);
        $this->assertStringContainsString('&lt;img src=x onerror=alert(1)&gt;', $html);
    }
}
