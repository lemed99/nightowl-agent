<?php

namespace NightOwl\Agent;

/**
 * Fork-safe HTML email template builder.
 *
 * Produces branded NightOwl emails via string concatenation (no Blade).
 * Used by AlertNotifier (forked drain worker) and ThresholdExceeded (Laravel notification).
 *
 * The output matches the approved preview HTML in nightowl-api/emails/ exactly.
 */
final class EmailTemplate
{
    private const LOGO_SVG = '<svg viewBox="0 0 783 160" fill="none" xmlns="http://www.w3.org/2000/svg" style="height:32px;width:auto;" aria-hidden="true">'
        . '<path d="M0.751953 155.388C0.270244 154.359 1.76605e-08 153.211 0 152V8C1.03084e-06 3.58172 3.58172 6.44257e-08 8 0H8.00098C3.58278 6.44245e-08 0.00110953 3.58184 0.000976562 8V36C5.52382 36 10.001 31.5228 10.001 26V10H26.001C31.5238 10 36.001 5.52285 36.001 0H52.001C52.001 5.52285 56.4781 10 62.001 10H98.001C103.524 10 108.001 5.52285 108.001 0H124.001C124.001 5.52285 128.478 10 134.001 10H150.001V26C150.001 31.5225 154.478 35.9985 160 35.999V52C154.478 52.0005 150.001 56.4776 150.001 62V98C150.001 103.523 154.478 107.998 160 107.999V124C154.478 124.001 150.001 128.478 150.001 134V150H134.001C128.478 150 124.001 154.477 124.001 160H108.001C108.001 154.477 103.524 150 98.001 150H62.001C56.4782 150 52.0011 154.477 52.001 160H36.001C36.0008 154.477 31.5237 150 26.001 150H10.001V134C10.0008 128.477 5.52374 124 0.000976562 124V152L0.0117188 152.412C0.0654719 153.471 0.325435 154.476 0.751953 155.388ZM0.000976562 108C5.52382 108 10.001 103.523 10.001 98V62C10.0008 56.4773 5.52374 52 0.000976562 52V108ZM152.443 0.0126953C152.789 0.0315553 153.128 0.073488 153.46 0.134766C153.128 0.0735389 152.789 0.031512 152.443 0.0126953ZM0.889648 155.665C0.842332 155.573 0.795791 155.481 0.751953 155.388C0.795775 155.481 0.842349 155.573 0.889648 155.665Z" fill="#ffffff"/>'
        . '<path d="M92.4409 87.4856C94.8642 96.2009 102.857 102.597 112.344 102.597C123.753 102.597 133.001 93.3485 133.001 81.9397C133.001 75.1712 129.747 69.1625 124.716 65.3948L92.4409 87.4856ZM35.1636 65.3108C30.0683 69.0728 26.7632 75.12 26.7632 81.9397C26.7632 93.3484 36.0117 102.597 47.4204 102.597C56.9579 102.597 64.9848 96.1335 67.3618 87.3479L35.1636 65.3108Z" fill="#00AF55"/>'
        . '<path d="M102.018 80.9309C102.008 81.1194 102.001 81.3092 102.001 81.5002C102.001 87.2992 106.702 92.0002 112.501 92.0002C118.3 92.0002 123.001 87.2992 123.001 81.5002C123.001 76.8419 119.967 72.8934 115.768 71.5198L102.018 80.9309ZM44.2358 71.5198C40.036 72.8931 37.0015 76.8416 37.0015 81.5002C37.0015 87.2992 41.7025 92.0002 47.5015 92.0002C53.3005 92.0002 58.0015 87.2992 58.0015 81.5002C58.0015 81.3092 57.9959 81.1194 57.9858 80.9309L44.2358 71.5198Z" fill="#09090b"/>'
        . '<path d="M84.9009 99.0002C85.6533 103.464 82.0009 109.67 80.0015 113C78.0021 109.67 74.3496 103.464 75.102 99.0002C76.3515 99.7491 77.6342 100.452 78.9599 101.095L80.0015 101.599L81.043 101.095C82.3688 100.452 83.6514 99.7491 84.9009 99.0002Z" fill="#00AF55"/>'
        . '<path d="M184 129.517V30.7971H199.381L205.953 50.5132V129.517H184ZM252.097 129.517L193.928 54.9878L199.381 30.7971L257.551 105.327L252.097 129.517ZM252.097 129.517L246.224 109.801V30.7971H268.178V129.517H252.097Z" fill="#ffffff"/>'
        . '<path d="M285.276 129.517V61.5593H306.671V129.517H285.276ZM296.043 52.1907C292.687 52.1907 289.891 51.0721 287.654 48.8348C285.51 46.5043 284.438 43.7077 284.438 40.445C284.438 37.089 285.51 34.2924 287.654 32.0551C289.891 29.8179 292.687 28.6992 296.043 28.6992C299.399 28.6992 302.149 29.8179 304.293 32.0551C306.437 34.2924 307.51 37.089 307.51 40.445C307.51 43.7077 306.437 46.5043 304.293 48.8348C302.149 51.0721 299.399 52.1907 296.043 52.1907Z" fill="#ffffff"/>'
        . '<path d="M350.541 160C343.084 160 336.512 158.695 330.825 156.085C325.232 153.568 320.804 149.979 317.541 145.318L330.685 132.174C333.109 135.064 335.813 137.254 338.796 138.746C341.872 140.238 345.554 140.983 349.842 140.983C355.156 140.983 359.304 139.678 362.287 137.068C365.363 134.458 366.901 130.776 366.901 126.021V108.683L370.537 93.8605L367.321 79.0385V61.5597H388.296V125.462C388.296 132.454 386.664 138.513 383.401 143.64C380.139 148.767 375.664 152.776 369.978 155.665C364.291 158.555 357.813 160 350.541 160ZM349.563 127.42C343.317 127.42 337.77 125.928 332.923 122.945C328.075 119.962 324.253 115.907 321.457 110.78C318.66 105.653 317.262 99.9664 317.262 93.7207C317.262 87.3817 318.66 81.6953 321.457 76.6614C324.253 71.5342 328.075 67.5258 332.923 64.636C337.77 61.6529 343.317 60.1614 349.563 60.1614C354.224 60.1614 358.372 61.047 362.007 62.8182C365.736 64.4961 368.719 66.9198 370.957 70.0893C373.287 73.1656 374.592 76.7546 374.872 80.8563V106.725C374.592 110.733 373.287 114.322 370.957 117.492C368.719 120.568 365.736 122.992 362.007 124.763C358.279 126.534 354.13 127.42 349.563 127.42ZM353.618 108.403C356.601 108.403 359.164 107.75 361.308 106.445C363.546 105.14 365.224 103.416 366.342 101.271C367.554 99.0342 368.16 96.5173 368.16 93.7207C368.16 90.924 367.554 88.4071 366.342 86.1698C365.224 83.9325 363.546 82.1613 361.308 80.8563C359.164 79.5512 356.601 78.8986 353.618 78.8986C350.728 78.8986 348.164 79.5512 345.927 80.8563C343.69 82.1613 341.965 83.9325 340.753 86.1698C339.541 88.4071 338.935 90.924 338.935 93.7207C338.935 96.3308 339.541 98.8012 340.753 101.132C341.965 103.369 343.643 105.14 345.787 106.445C348.024 107.75 350.635 108.403 353.618 108.403Z" fill="#ffffff"/>'
        . '<path d="M448.281 129.517V90.7841C448.281 87.2417 447.162 84.3985 444.925 82.2544C442.781 80.0171 440.031 78.8985 436.675 78.8985C434.344 78.8985 432.293 79.4112 430.522 80.4366C428.751 81.3688 427.353 82.7671 426.327 84.6315C425.302 86.4027 424.789 88.4536 424.789 90.7841L416.539 86.729C416.539 81.4154 417.658 76.7544 419.895 72.7459C422.132 68.7375 425.255 65.6612 429.264 63.5171C433.272 61.2799 437.887 60.1612 443.107 60.1612C448.42 60.1612 453.081 61.2799 457.09 63.5171C461.098 65.6612 464.175 68.6909 466.319 72.6061C468.556 76.4281 469.675 80.9027 469.675 86.0298V129.517H448.281ZM403.395 129.517V28.0002H424.789V129.517H403.395Z" fill="#ffffff"/>'
        . '<path d="M492.244 129.517V33.4536H513.638V129.517H492.244ZM476.863 79.7375V61.5595H529.019V79.7375H476.863Z" fill="#ffffff"/>'
        . '<path d="M587.526 131.195C580.069 131.195 573.17 129.89 566.831 127.28C560.586 124.67 555.086 121.034 550.331 116.373C545.577 111.712 541.895 106.305 539.285 100.153C536.675 93.9069 535.37 87.1951 535.37 80.0171C535.37 72.746 536.675 66.0341 539.285 59.8816C541.895 53.729 545.531 48.3689 550.192 43.8011C554.853 39.1401 560.306 35.5511 566.552 33.0341C572.891 30.424 579.789 29.1189 587.247 29.1189C594.611 29.1189 601.416 30.424 607.662 33.0341C614.001 35.5511 619.501 39.1401 624.162 43.8011C628.916 48.3689 632.598 53.7757 635.208 60.0214C637.819 66.1739 639.124 72.8858 639.124 80.157C639.124 87.3349 637.819 94.0468 635.208 100.293C632.598 106.445 628.963 111.852 624.302 116.513C619.641 121.081 614.141 124.67 607.802 127.28C601.556 129.89 594.798 131.195 587.526 131.195ZM587.247 111.199C593.12 111.199 598.247 109.894 602.628 107.284C607.103 104.674 610.552 101.038 612.975 96.3773C615.399 91.6231 616.611 86.1697 616.611 80.0171C616.611 75.3561 615.912 71.1612 614.514 67.4324C613.115 63.6104 611.111 60.3477 608.501 57.6443C605.891 54.8477 602.768 52.7502 599.132 51.3519C595.59 49.8604 591.628 49.1146 587.247 49.1146C581.374 49.1146 576.2 50.4197 571.726 53.0299C567.344 55.5468 563.942 59.1358 561.518 63.7968C559.094 68.3646 557.882 73.7714 557.882 80.0171C557.882 84.6782 558.581 88.9197 559.98 92.7417C561.378 96.5637 563.336 99.873 565.853 102.67C568.463 105.373 571.586 107.47 575.221 108.962C578.857 110.454 582.865 111.199 587.247 111.199Z" fill="#00AF55"/>'
        . '<path d="M667.742 129.517L644.25 61.5596H665.225L679.767 110.92L673.615 111.06L689.695 61.5596H707.034L723.255 111.06L716.962 110.92L731.644 61.5596H752.619L729.127 129.517H711.649L695.848 82.5341H701.161L685.081 129.517H667.742Z" fill="#00AF55"/>'
        . '<path d="M760.891 129.517V28.0002H782.285V129.517H760.891Z" fill="#00AF55"/>'
        . '</svg>';

    /**
     * Render an issue alert email.
     *
     * @param  string                $appName    Application name
     * @param  array<string, mixed>  $group      Enriched group data from AlertNotifier
     * @param  string                $issueType  'exception' or 'performance'
     */
    public static function renderIssue(string $appName, array $group, string $issueType = 'exception'): string
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

        $isException = $issueType === 'exception';
        $subtypeLabel = self::subtypeLabel($subtype);

        $titleText = $isException ? 'Exception: ' . $e($name) : $e($name);

        // Event badge + title + type badges
        $header = '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:24px;">'
            . '<tr><td align="center" style="padding-bottom:14px;">'
            . '<span style="display:inline-block;background-color:rgba(220,38,38,0.15);color:#f87171;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;padding:4px 12px;border-radius:2px;border-left:3px solid #dc2626;">New Issue</span>'
            . '</td></tr>'
            . '<tr><td align="center" style="font-size:18px;font-weight:700;color:#fafafa;line-height:1.4;padding-bottom:12px;word-break:break-word;">'
            . $titleText
            . '</td></tr>'
            . '<tr><td align="center">';

        if ($issueId !== null) {
            $header .= '<span style="display:inline-block;background-color:#00af55;color:#ffffff;font-size:12px;font-weight:600;padding:4px 10px;border-radius:2px;margin-right:6px;">#' . $issueId . '</span>';
        }

        if ($isException) {
            $statusText = ($handled === true) ? 'Handled' : 'Unhandled';
            $header .= '<span style="display:inline-block;background-color:rgba(220,38,38,0.15);color:#f87171;font-size:12px;font-weight:600;padding:4px 10px;border-radius:2px;">' . $statusText . '</span>';
        } else {
            $header .= '<span style="display:inline-block;background-color:#00af55;color:#ffffff;font-size:12px;font-weight:600;padding:4px 10px;border-radius:2px;">' . $e($subtypeLabel) . '</span>';
        }

        $header .= '</td></tr></table>';

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
            $sections .= self::textSection($subtypeLabel, $e($name), '20px');
        }

        // View issue button
        $cta = '';
        if ($viewUrl !== null) {
            $cta = '<table role="presentation" width="100%" cellpadding="0" cellspacing="0">'
                . '<tr><td align="center">'
                . '<a href="' . $e($viewUrl) . '" style="display:block;background-color:#00af55;color:#ffffff;font-size:14px;font-weight:700;text-decoration:none;padding:12px 32px;border-radius:2px;letter-spacing:0.3px;text-align:center;">View issue</a>'
                . '</td></tr></table>';
        }

        return self::layout(
            $titleText,
            $header . $details . $sections . $cta,
            $e($appName),
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

    /**
     * Render a threshold alert email (high error rate or slow response).
     */
    public static function renderThreshold(string $appName, string $type, string $title, string $message): string
    {
        $e = fn (string $s): string => htmlspecialchars($s, ENT_QUOTES | ENT_HTML5, 'UTF-8');

        $typeLabel = match ($type) {
            'high_error_rate' => 'High Error Rate',
            'slow_response' => 'Slow Response',
            default => ucwords(str_replace('_', ' ', $type)),
        };

        // Event badge + title
        $header = '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:24px;">'
            . '<tr><td align="center" style="padding-bottom:14px;">'
            . '<span style="display:inline-block;background-color:rgba(245,158,11,0.15);color:#fbbf24;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;padding:4px 12px;border-radius:2px;border-left:3px solid #f59e0b;">Alert</span>'
            . '</td></tr>'
            . '<tr><td align="center" style="font-size:18px;font-weight:700;color:#fafafa;line-height:1.4;padding-bottom:12px;word-break:break-word;">'
            . $e($title)
            . '</td></tr>'
            . '</table>';

        // Details section
        $details = self::section('Details', self::kvTable([
            'Application' => $e($appName),
            'Type' => $typeLabel,
        ]), '18px');

        // Message section
        $messageSection = self::textSection('Message', $e($message), '20px');

        return self::layout($e($title), $header . $details . $messageSection, $e($appName));
    }

    // ─── Building blocks (match preview HTML exactly) ────────────────

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

    private static function hudCorners(): string
    {
        return '<div style="position:absolute;top:-1px;left:-1px;width:22px;height:2px;background-color:#00af55;"></div>'
            . '<div style="position:absolute;top:-1px;left:-1px;width:2px;height:22px;background-color:#00af55;"></div>'
            . '<div style="position:absolute;top:-1px;right:-1px;width:22px;height:2px;background-color:#00af55;"></div>'
            . '<div style="position:absolute;top:-1px;right:-1px;width:2px;height:22px;background-color:#00af55;"></div>'
            . '<div style="position:absolute;bottom:-1px;left:-1px;width:22px;height:2px;background-color:#00af55;"></div>'
            . '<div style="position:absolute;bottom:-1px;left:-1px;width:2px;height:22px;background-color:#00af55;"></div>'
            . '<div style="position:absolute;bottom:-1px;right:-1px;width:22px;height:2px;background-color:#00af55;"></div>'
            . '<div style="position:absolute;bottom:-1px;right:-1px;width:2px;height:22px;background-color:#00af55;"></div>';
    }

    private static function layout(string $pageTitle, string $inner, string $appName): string
    {
        $year = date('Y');

        return '<!doctype html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"><title>' . $pageTitle . '</title></head>'
            . '<body style="margin:0;padding:0;background-color:#09090b;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,\'Liberation Mono\',\'Courier New\',monospace;-webkit-font-smoothing:antialiased;color:#fafafa;">'
            . '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#09090b;padding:32px 16px;"><tr><td align="center">'
            . '<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;">'
            // Logo
            . '<tr><td align="center" style="padding-bottom:24px;">' . self::LOGO_SVG . '</td></tr>'
            // Outer card with HUD corners
            . '<tr><td style="padding:0;"><div style="position:relative;background-color:#18181b;border:1px solid #27272a;border-radius:2px;padding:32px 28px 28px;">'
            . self::hudCorners()
            . $inner
            . '</div></td></tr>'
            // Footer
            . '<tr><td align="center" style="padding:24px 16px 0;font-size:12px;color:#52525b;line-height:1.6;">'
            . 'You received this email because alerts are enabled for <strong style="color:#71717a;">' . $appName . '</strong> on NightOwl.'
            . '</td></tr>'
            . '<tr><td align="center" style="padding:10px 16px 0;font-size:11px;color:#3f3f46;">&copy; ' . $year . ' NightOwl. All rights reserved.</td></tr>'
            . '</table></td></tr></table></body></html>';
    }
}
