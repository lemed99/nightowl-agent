<?php

namespace NightOwl\Agent;

/**
 * Fork-safe HTML email template builder.
 *
 * Produces branded NightOwl emails via string concatenation (no Blade).
 * Used by AlertNotifier (forked drain worker) for issue alerts and by
 * HealthAlertNotifier (forked health-alert child) for agent health alerts.
 *
 * Renders are tested against Gmail (strips SVG + position:absolute), Outlook desktop
 * (drops rgba + span padding), Apple Mail, and mobile clients. The output matches
 * the approved preview HTML in nightowl-api/emails/.
 */
final class EmailTemplate
{
    /**
     * Render an issue alert email.
     *
     * @param  string                $appName      Application name
     * @param  array<string, mixed>  $group        Enriched group data from AlertNotifier
     * @param  string                $issueType    'exception' or 'performance'
     * @param  string                $frontendUrl  Frontend base URL (for logo asset)
     * @param  string                $event        'issue.new' or 'issue.reopened' — picks the badge
     */
    public static function renderIssue(string $appName, array $group, string $issueType = 'exception', string $frontendUrl = '', string $event = 'issue.new'): string
    {
        $e = fn (string $s): string => htmlspecialchars($s, ENT_QUOTES | ENT_HTML5, 'UTF-8');

        $name = $group['class'] ?? $group['name'] ?? 'Unknown';
        $message = $group['message'] ?? '';
        if ($message !== '' && mb_strlen($message) > 200) {
            $message = mb_substr($message, 0, 200) . '...';
        }
        $occurrences = (int) ($group['count'] ?? 0);
        $users = (int) ($group['users_count'] ?? count($group['users'] ?? []));
        $subtype = $group['subtype'] ?? null;
        $issueId = $group['issue_id'] ?? null;
        $handled = $group['handled'] ?? null;
        $environment = $group['environment'] ?? null;
        $location = $group['location'] ?? null;
        $phpVersion = $group['php_version'] ?? null;
        $laravelVersion = $group['laravel_version'] ?? null;
        $firstSeenAt = $group['first_seen_at'] ?? null;
        $lastSeenAt = $group['last_seen_at'] ?? null;
        $viewUrl = $group['view_url'] ?? null;
        $thresholdMs = isset($group['threshold_ms']) && $group['threshold_ms'] !== null ? (int) $group['threshold_ms'] : null;
        $durationMs = isset($group['duration_ms']) && $group['duration_ms'] !== null ? (int) $group['duration_ms'] : null;

        $isException = $issueType === 'exception';
        $subtypeLabel = self::subtypeLabel($subtype);

        // Built unescaped, then escaped once: the preheader goes through
        // layout()'s own escaping, so feeding it the escaped title showed
        // "&amp;" in the inbox preview for any name carrying an ampersand.
        if ($isException) {
            $plainTitle = 'Exception: ' . $name;
        } elseif ($thresholdMs !== null) {
            $plainTitle = $name . ' exceeded ' . number_format($thresholdMs) . 'ms threshold';
        } elseif ($subtype === 'worker_saturation') {
            // The name IS the event ("Worker saturation") — a "Slow Worker
            // Saturation:" prefix would be nonsense. Details ride the message.
            $plainTitle = $name;
        } else {
            $plainTitle = 'Slow ' . $subtypeLabel . ': ' . $name;
        }
        $titleText = $e($plainTitle);

        // Solid hex equivalents of rgba(...,0.15) composited over the #18181b card
        // so Outlook desktop (which drops rgba) still renders the tinted pill.
        // Matches nightowl-api's issue.blade.php: a reopen gets the orange
        // "Reopened" badge whatever the issue type; only a NEW performance
        // issue gets the amber "Performance Alert" one.
        if ($event === 'issue.reopened') {
            $badgeBg = '#382216';
            $badgeColor = '#fb923c';
            $badgeBorder = '#ea580c';
            $badgeLabel = 'Reopened';
        } elseif ($isException) {
            $badgeBg = '#351a1a';
            $badgeColor = '#f87171';
            $badgeBorder = '#dc2626';
            $badgeLabel = 'New Issue';
        } else {
            $badgeBg = '#392c16';
            $badgeColor = '#fbbf24';
            $badgeBorder = '#f59e0b';
            $badgeLabel = 'Performance Alert';
        }

        // Event badge (top), title, and sub-pills — all via nested tables so
        // Outlook (which ignores span padding) still renders them as pills.
        $badgeCell = '<td style="background-color:' . $badgeBg . ';color:' . $badgeColor
            . ';font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;'
            . 'padding:4px 12px;border-radius:2px;border-left:3px solid ' . $badgeBorder . ';">'
            . $badgeLabel . '</td>';

        $pillCells = '';
        if ($issueId !== null) {
            $pillCells .= '<td style="background-color:#00af55;color:#ffffff;font-size:12px;font-weight:600;padding:4px 10px;border-radius:2px;">#' . $issueId . '</td>'
                . '<td width="6" style="width:6px;font-size:0;line-height:0;">&nbsp;</td>';
        }
        if ($isException) {
            $statusText = ($handled === true) ? 'Handled' : 'Unhandled';
            $pillCells .= '<td style="background-color:#351a1a;color:#f87171;font-size:12px;font-weight:600;padding:4px 10px;border-radius:2px;">' . $statusText . '</td>';
        } else {
            $pillCells .= '<td style="background-color:#392c16;color:#fbbf24;font-size:12px;font-weight:600;padding:4px 10px;border-radius:2px;">' . $e($subtypeLabel) . '</td>';
        }

        $header = '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:24px;">'
            . '<tr><td align="center" style="padding-bottom:14px;">' . self::pillTable($badgeCell) . '</td></tr>'
            . '<tr><td align="center" style="font-size:18px;font-weight:700;color:#fafafa;line-height:1.4;padding-bottom:12px;word-break:break-word;">' . $titleText . '</td></tr>'
            . '<tr><td align="center">' . self::pillTable($pillCells) . '</td></tr>'
            . '</table>';

        // Details section
        $detailRows = ['Application' => $e($appName)];
        if ($environment !== null) {
            $detailRows['Environment'] = $e(ucfirst($environment));
        }
        $detailRows['Occurrences'] = number_format($occurrences);
        $detailRows['Users Affected'] = number_format($users);
        if ($firstSeenAt !== null) {
            $detailRows['First Seen'] = $e($firstSeenAt);
        }
        if ($lastSeenAt !== null) {
            $detailRows['Last Seen'] = $e($lastSeenAt);
        }

        $details = self::section('Details', self::kvTable($detailRows), '18px');

        // Content sections
        $sections = '';

        if ($isException) {
            if ($message !== '') {
                $sections .= self::textSection('Message', $e($message), '18px');
            }
            if ($location !== null) {
                $sections .= self::textSection('Location', $e($location), '18px');
            }
            if ($phpVersion !== null || $laravelVersion !== null) {
                $envRows = [];
                if ($laravelVersion !== null) {
                    $envRows['Laravel'] = $e($laravelVersion);
                }
                if ($phpVersion !== null) {
                    $envRows['PHP'] = $e($phpVersion);
                }
                $sections .= self::section('Environment', self::kvTable($envRows), '20px');
            }
        } else {
            // Skipped when the name only restates the subtype ("Worker
            // saturation" under a "Worker Saturation" label) — the title
            // already says it.
            if (strcasecmp($name, $subtypeLabel) !== 0) {
                $sections .= self::textSection($subtypeLabel, $e($name), '18px');
            }

            // Threshold issues carry no message; checks like worker saturation
            // put their numbers here and nowhere else.
            if ($message !== '') {
                $sections .= self::textSection('Message', $e($message), '18px');
            }

            $perfRows = [];
            if ($durationMs !== null) {
                $perfRows['Duration'] = number_format($durationMs) . 'ms';
            }
            if ($thresholdMs !== null) {
                $perfRows['Threshold'] = number_format($thresholdMs) . 'ms';
            }
            if ($durationMs !== null && $thresholdMs !== null && $durationMs > $thresholdMs) {
                $over = $durationMs - $thresholdMs;
                $pct = $thresholdMs > 0 ? (int) round(($over / $thresholdMs) * 100) : null;
                $perfRows['Over by'] = number_format($over) . 'ms' . ($pct !== null ? ' (+' . $pct . '%)' : '');
            }

            if (! empty($perfRows)) {
                $sections .= self::section('Performance', self::kvTable($perfRows), '20px');
            } elseif ($message === '') {
                $sections .= self::textSection('Performance', 'Exceeded configured threshold.', '20px');
            }
        }

        // View issue button
        $cta = $viewUrl !== null ? self::button($viewUrl, 'View issue') : '';

        $preheader = $plainTitle . ($occurrences > 0
            ? ' — ' . number_format($occurrences) . ($occurrences === 1 ? ' occurrence' : ' occurrences')
            : '');

        return self::layout(
            $titleText,
            $preheader,
            $header . $details . $sections . $cta,
            $e($appName),
            $frontendUrl,
        );
    }

    /**
     * Render an agent health alert email (a diagnosis raised or cleared).
     *
     * Same card, badge and button as renderIssue so a health alert reads as
     * part of the same family. The link goes to the app's Agent Health page;
     * with no dashboard URL configured there is nothing to link to and the
     * button is left out.
     *
     * @param  array<string, mixed>  $diagnosis  {code, level, message, recommendation}
     * @param  string                $variant    'degraded' or 'recovered'
     * @param  string                $instance   Agent instance id, '' when unknown
     * @param  string|null           $healthUrl  Agent Health page URL, null to omit the button
     * @param  string                $frontendUrl  Frontend base URL (for logo asset)
     */
    public static function renderHealth(
        string $appName,
        array $diagnosis,
        string $variant,
        string $instance = '',
        ?string $healthUrl = null,
        string $frontendUrl = '',
    ): string {
        $e = fn (string $s): string => htmlspecialchars($s, ENT_QUOTES | ENT_HTML5, 'UTF-8');

        $code = (string) ($diagnosis['code'] ?? 'UNKNOWN');
        $level = (string) ($diagnosis['level'] ?? '');
        $message = (string) ($diagnosis['message'] ?? '');
        $recommendation = (string) ($diagnosis['recommendation'] ?? '');
        $recovered = $variant === 'recovered';

        // Same solid-hex palette as the issue badges: resolved green, new-issue
        // red for critical, performance amber for anything milder.
        if ($recovered) {
            [$bg, $color, $border] = ['#142f21', '#4ade80', '#16a34a'];
            $badgeLabel = 'Recovered';
            $pillLabel = 'Resolved';
        } elseif ($level === 'critical') {
            [$bg, $color, $border] = ['#351a1a', '#f87171', '#dc2626'];
            $badgeLabel = 'Agent Health Alert';
            $pillLabel = 'Critical';
        } else {
            [$bg, $color, $border] = ['#392c16', '#fbbf24', '#f59e0b'];
            $badgeLabel = 'Agent Health Alert';
            $pillLabel = $level !== '' ? ucfirst($level) : 'Warning';
        }

        $badgeCell = '<td style="background-color:' . $bg . ';color:' . $color
            . ';font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;'
            . 'padding:4px 12px;border-radius:2px;border-left:3px solid ' . $border . ';">'
            . $badgeLabel . '</td>';
        $pillCell = '<td style="background-color:' . $bg . ';color:' . $color
            . ';font-size:12px;font-weight:600;padding:4px 10px;border-radius:2px;">' . $e($pillLabel) . '</td>';

        $header = '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:24px;">'
            . '<tr><td align="center" style="padding-bottom:14px;">' . self::pillTable($badgeCell) . '</td></tr>'
            . '<tr><td align="center" style="font-size:18px;font-weight:700;color:#fafafa;line-height:1.4;padding-bottom:12px;word-break:break-word;">' . $e($code) . '</td></tr>'
            . '<tr><td align="center">' . self::pillTable($pillCell) . '</td></tr>'
            . '</table>';

        $detailRows = ['Application' => $e($appName)];
        if ($instance !== '') {
            $detailRows['Instance'] = $e($instance);
        }
        $body = self::section('Details', self::kvTable($detailRows), '18px');

        if ($message !== '') {
            $body .= self::textSection('Message', $e($message), $recovered || $recommendation === '' ? '20px' : '18px');
        }
        if (! $recovered && $recommendation !== '') {
            $body .= self::textSection('Recommendation', $e($recommendation), '20px');
        }

        $cta = $healthUrl !== null ? self::button($healthUrl, 'View agent health') : '';

        $plainTitle = ($recovered ? 'Recovered: ' : 'Agent Health Alert: ') . $code;
        $preheader = $recovered ? $plainTitle : $code . ($message !== '' ? ': ' . $message : '');
        if (mb_strlen($preheader) > 200) {
            $preheader = mb_substr($preheader, 0, 200) . '...';
        }

        return self::layout(
            $e($plainTitle),
            $preheader,
            $header . $body . $cta,
            $e($appName),
            $frontendUrl,
        );
    }

    /**
     * Human-readable label for a performance issue subtype.
     */
    public static function subtypeLabel(?string $subtype): string
    {
        return match ($subtype) {
            'route' => 'Route',
            'job' => 'Job',
            'command' => 'Command',
            'scheduled_task' => 'Scheduled Task',
            'query' => 'Query',
            'outgoing_request' => 'Outgoing Request',
            'mail' => 'Mail',
            'notification' => 'Notification',
            'cache' => 'Cache',
            default => ucwords(str_replace('_', ' ', $subtype ?? 'route')),
        };
    }

    // ─── Building blocks ─────────────────────────────────────────────

    /**
     * Wraps one-or-more <td> cells in a centered, inline-table pill row.
     */
    private static function pillTable(string $cells): string
    {
        return '<table role="presentation" align="center" border="0" cellpadding="0" cellspacing="0" style="margin:0 auto;"><tr>'
            . $cells
            . '</tr></table>';
    }

    /**
     * Full-width call-to-action link, the "View issue" button's style.
     */
    private static function button(string $url, string $label): string
    {
        $href = htmlspecialchars($url, ENT_QUOTES | ENT_HTML5, 'UTF-8');

        return '<table role="presentation" width="100%" cellpadding="0" cellspacing="0">'
            . '<tr><td align="center">'
            . '<a href="' . $href . '" style="display:block;background-color:#00af55;color:#ffffff;font-size:14px;font-weight:700;text-decoration:none;padding:12px 32px;border-radius:2px;letter-spacing:0.3px;text-align:center;">' . $label . '</a>'
            . '</td></tr></table>';
    }

    /**
     * Section with a KV table or custom content inside a dark card.
     */
    private static function section(string $label, string $content, string $marginBottom): string
    {
        return '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:' . $marginBottom . ';">'
            . '<tr><td style="padding-bottom:8px;">'
            . '<span style="font-size:13px;font-weight:600;color:#a1a1aa;letter-spacing:0.5px;text-transform:uppercase;border-left:2px solid #00af55;padding-left:8px;">' . $label . '</span>'
            . '</td></tr>'
            . '<tr><td style="background-color:#09090b;border:1px solid #27272a;border-radius:2px;padding:14px 16px;">'
            . $content
            . '</td></tr></table>';
    }

    /**
     * Section with plain text content inside a dark card.
     */
    private static function textSection(string $label, string $text, string $marginBottom): string
    {
        return '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:' . $marginBottom . ';">'
            . '<tr><td style="padding-bottom:8px;">'
            . '<span style="font-size:13px;font-weight:600;color:#a1a1aa;letter-spacing:0.5px;text-transform:uppercase;border-left:2px solid #00af55;padding-left:8px;">' . $label . '</span>'
            . '</td></tr>'
            . '<tr><td style="background-color:#09090b;border:1px solid #27272a;border-radius:2px;padding:14px 16px;font-size:13px;color:#e4e4e7;word-break:break-word;">'
            . $text
            . '</td></tr></table>';
    }

    /**
     * @param  array<string, string>  $rows
     */
    private static function kvTable(array $rows): string
    {
        $html = '';
        $first = true;
        foreach ($rows as $key => $value) {
            $widthStyle = $first ? 'font-size:13px;color:#71717a;font-weight:600;padding:3px 0;width:130px;' : 'font-size:13px;color:#71717a;font-weight:600;padding:3px 0;';
            $html .= '<tr>'
                . '<td style="' . $widthStyle . '">' . $key . '</td>'
                . '<td style="font-size:13px;color:#e4e4e7;padding:3px 0;">' . $value . '</td>'
                . '</tr>';
            $first = false;
        }

        return '<table role="presentation" width="100%" cellpadding="0" cellspacing="0">' . $html . '</table>';
    }

    /**
     * One row of L-shaped corner brackets, built from td borders so Gmail
     * (which strips position:absolute) still renders the HUD frame.
     */
    private static function hudCornerRow(string $position): string
    {
        $vertical = $position === 'top'
            ? 'border-top:2px solid #00af55;'
            : 'border-bottom:2px solid #00af55;';

        return '<tr>'
            . '<td width="22" height="22" style="width:22px;height:22px;' . $vertical . 'border-left:2px solid #00af55;font-size:0;line-height:0;mso-line-height-rule:exactly;">&nbsp;</td>'
            . '<td height="22" style="height:22px;font-size:0;line-height:0;mso-line-height-rule:exactly;">&nbsp;</td>'
            . '<td width="22" height="22" style="width:22px;height:22px;' . $vertical . 'border-right:2px solid #00af55;font-size:0;line-height:0;mso-line-height-rule:exactly;">&nbsp;</td>'
            . '</tr>';
    }

    /**
     * Logo <img> — Gmail strips inline SVG, so we link to a hosted PNG
     * and include styled alt text for image-blocked clients.
     */
    private static function logo(string $frontendUrl): string
    {
        $base = rtrim($frontendUrl, '/');
        if ($base === '') {
            // Resolve via container only when one is bound (agent boot path); otherwise
            // fall back to the literal — keeps EmailTemplate testable without Laravel.
            try {
                $configured = (string) config('nightowl.agent.dashboard_url', 'https://usenightowl.com');
            } catch (\Throwable) {
                $configured = 'https://usenightowl.com';
            }
            $base = rtrim($configured, '/');
        }
        $src = htmlspecialchars($base . '/full-logo.png', ENT_QUOTES | ENT_HTML5, 'UTF-8');

        return '<img src="' . $src . '" alt="NightOwl" height="32" style="display:block;border:0;outline:none;text-decoration:none;height:32px;width:auto;color:#fafafa;font-size:18px;font-weight:700;letter-spacing:0.3px;">';
    }

    private static function layout(string $pageTitle, string $preheader, string $inner, string $appName, string $frontendUrl): string
    {
        $year = date('Y');
        $ePre = htmlspecialchars($preheader, ENT_QUOTES | ENT_HTML5, 'UTF-8');

        return '<!doctype html><html lang="en"><head>'
            . '<meta charset="UTF-8">'
            . '<meta name="viewport" content="width=device-width,initial-scale=1.0">'
            . '<meta name="color-scheme" content="dark light">'
            . '<meta name="supported-color-schemes" content="dark light">'
            . '<meta name="format-detection" content="telephone=no,address=no,email=no,date=no">'
            . '<title>' . $pageTitle . '</title>'
            . '<style>:root{color-scheme:dark light;supported-color-schemes:dark light;}</style>'
            . '</head>'
            . '<body style="margin:0;padding:0;background-color:#09090b;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,\'Liberation Mono\',\'Courier New\',monospace;-webkit-font-smoothing:antialiased;color:#fafafa;">'
            . '<div style="display:none;max-height:0;overflow:hidden;mso-hide:all;font-size:1px;line-height:1px;color:#09090b;opacity:0;">' . $ePre . '</div>'
            . '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#09090b;padding:32px 16px;"><tr><td align="center">'
            . '<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;">'
            // Logo
            . '<tr><td align="center" style="padding-bottom:24px;">' . self::logo($frontendUrl) . '</td></tr>'
            // Outer card with HUD corners (3-row table replaces position:absolute divs)
            . '<tr><td style="padding:0;">'
            . '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;background-color:#18181b;border-radius:2px;">'
            . self::hudCornerRow('top')
            . '<tr><td colspan="3" style="padding:14px 28px 12px;">' . $inner . '</td></tr>'
            . self::hudCornerRow('bottom')
            . '</table>'
            . '</td></tr>'
            // Footer
            . '<tr><td align="center" style="padding:24px 16px 0;font-size:12px;color:#52525b;line-height:1.6;">'
            . 'You received this email because alerts are enabled for <strong style="color:#71717a;">' . $appName . '</strong> on NightOwl.'
            . '</td></tr>'
            . '<tr><td align="center" style="padding:10px 16px 0;font-size:11px;color:#3f3f46;">&copy; ' . $year . ' NightOwl. All rights reserved.</td></tr>'
            . '</table></td></tr></table></body></html>';
    }
}
