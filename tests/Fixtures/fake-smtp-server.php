<?php

/**
 * A scripted SMTP server for SmtpClientConversationTest.
 *
 * Serves exactly one connection, records every line the client sends, then
 * exits. Standalone (no Laravel, no PHPUnit) and launched as a subprocess
 * rather than a fork so the test never has PHPUnit's shutdown handlers running
 * in two processes at once.
 *
 *   php fake-smtp-server.php <transcript-path> <base64 json script>
 *
 * It binds port 0 and prints the port it landed on as its first stdout line,
 * so the parent learns the port only after the listener exists — there is no
 * window in which the client can connect to nothing.
 *
 * Script keys (all optional):
 *   greeting        full 220 line, default a normal ESMTP banner
 *   greeting_delay  seconds to stall before the banner (timeout tests)
 *   ehlo            full reply to EHLO, default advertises STARTTLS+AUTH
 *   auth            full reply to AUTH, default 235 accepted
 *   data_accepted   full reply after the message body, default 250 queued
 */

$transcriptPath = $argv[1] ?? null;
$script = json_decode(base64_decode($argv[2] ?? '') ?: '{}', true) ?: [];

if ($transcriptPath === null) {
    fwrite(STDERR, "usage: fake-smtp-server.php <transcript-path> <base64 json>\n");
    exit(1);
}

$greeting = $script['greeting'] ?? "220 fake.test ESMTP ready\r\n";
$ehlo = $script['ehlo'] ?? "250-fake.test\r\n250-SIZE 35882577\r\n250-STARTTLS\r\n250-AUTH PLAIN LOGIN\r\n250 8BITMIME\r\n";
$auth = $script['auth'] ?? "235 2.7.0 Authentication successful\r\n";
$dataAccepted = $script['data_accepted'] ?? "250 2.0.0 Ok: queued as ABC123\r\n";

$server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
if (! $server) {
    fwrite(STDERR, "bind failed: {$errstr}\n");
    exit(1);
}

$name = stream_socket_get_name($server, false);
echo 'PORT='.substr($name, strrpos($name, ':') + 1)."\n";
flush();

$transcript = [];
$writeTranscript = static function () use (&$transcript, $transcriptPath): void {
    file_put_contents($transcriptPath, implode("\n", $transcript));
};

$connection = @stream_socket_accept($server, 10);
if (! $connection) {
    $writeTranscript();
    exit(0);
}

stream_set_timeout($connection, 10);

if (($script['greeting_delay'] ?? 0) > 0) {
    // Stall past the client's read timeout without ever closing the socket —
    // the difference between "timed out" and "connection closed" is the whole
    // point of the diagnostic those tests assert on.
    sleep((int) $script['greeting_delay']);
}

fwrite($connection, $greeting);

$inData = false;

while (($line = fgets($connection, 8192)) !== false) {
    $trimmed = rtrim($line, "\r\n");
    $transcript[] = $trimmed;

    if ($inData) {
        if ($trimmed === '.') {
            $inData = false;
            fwrite($connection, $dataAccepted);
        }

        continue;
    }

    $command = strtoupper((string) strtok($trimmed, ' '));

    switch ($command) {
        case 'EHLO':
        case 'HELO':
            fwrite($connection, $ehlo);
            break;
        case 'AUTH':
            fwrite($connection, $auth);
            // A 334 continuation means AUTH LOGIN: the next two lines are the
            // base64 username and password, not commands.
            if (str_starts_with($auth, '334')) {
                foreach ([1, 2] as $step) {
                    $credential = fgets($connection, 8192);
                    if ($credential === false) {
                        break;
                    }
                    $transcript[] = rtrim($credential, "\r\n");
                    fwrite($connection, $step === 1 ? "334 UGFzc3dvcmQ6\r\n" : "235 2.7.0 Authentication successful\r\n");
                }
            }
            break;
        case 'MAIL':
        case 'RCPT':
            fwrite($connection, "250 2.1.0 Ok\r\n");
            break;
        case 'DATA':
            $inData = true;
            fwrite($connection, "354 End data with <CR><LF>.<CR><LF>\r\n");
            break;
        case 'QUIT':
            fwrite($connection, "221 2.0.0 Bye\r\n");
            break 2;
        default:
            fwrite($connection, "502 5.5.2 Command not implemented\r\n");
    }
}

fclose($connection);
$writeTranscript();
exit(0);
