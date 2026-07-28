<?php

/**
 * A scripted HTTP server for the webhook half of the alert dispatchers.
 *
 * Companion to fake-smtp-server.php and built the same way: serves one request,
 * records it, exits. Binds port 0 and prints the port as its first stdout line,
 * so the parent learns it only after the listener exists.
 *
 *   php fake-http-server.php <transcript-path> <base64 json script>
 *
 * The transcript is the raw request — request line, headers, body — which is
 * what lets a test assert the Slack/Discord payload actually carried the alert.
 *
 * Script keys (all optional):
 *   status       response status code, default 200
 *   body         response body, default empty
 *   requests     how many requests to serve before exiting, default 1
 */

$transcriptPath = $argv[1] ?? null;
$script = json_decode(base64_decode($argv[2] ?? '') ?: '{}', true) ?: [];

if ($transcriptPath === null) {
    fwrite(STDERR, "usage: fake-http-server.php <transcript-path> <base64 json>\n");
    exit(1);
}

$status = (int) ($script['status'] ?? 200);
$responseBody = (string) ($script['body'] ?? '');
$requests = max(1, (int) ($script['requests'] ?? 1));

$reasons = [
    200 => 'OK',
    204 => 'No Content',
    301 => 'Moved Permanently',
    400 => 'Bad Request',
    401 => 'Unauthorized',
    404 => 'Not Found',
    429 => 'Too Many Requests',
    500 => 'Internal Server Error',
    502 => 'Bad Gateway',
];

$server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
if (! $server) {
    fwrite(STDERR, "bind failed: {$errstr}\n");
    exit(1);
}

$name = stream_socket_get_name($server, false);
echo 'PORT='.substr($name, strrpos($name, ':') + 1)."\n";
flush();

$transcript = [];

for ($served = 0; $served < $requests; $served++) {
    $connection = @stream_socket_accept($server, 10);
    if (! $connection) {
        break;
    }

    stream_set_timeout($connection, 10);

    $contentLength = 0;

    // Headers first — the blank line ends them, and Content-Length is the only
    // way to know how much body to read without hanging on a keep-alive socket.
    while (($line = fgets($connection, 8192)) !== false) {
        $trimmed = rtrim($line, "\r\n");
        $transcript[] = $trimmed;

        if ($trimmed === '') {
            break;
        }

        if (stripos($trimmed, 'content-length:') === 0) {
            $contentLength = (int) trim(substr($trimmed, strlen('content-length:')));
        }
    }

    if ($contentLength > 0) {
        $body = '';
        while (strlen($body) < $contentLength) {
            $chunk = fread($connection, $contentLength - strlen($body));
            if ($chunk === false || $chunk === '') {
                break;
            }
            $body .= $chunk;
        }
        $transcript[] = $body;
    }

    $reason = $reasons[$status] ?? 'Status';
    fwrite($connection, "HTTP/1.1 {$status} {$reason}\r\n");
    fwrite($connection, 'Content-Length: '.strlen($responseBody)."\r\n");
    fwrite($connection, "Connection: close\r\n\r\n");
    fwrite($connection, $responseBody);
    fclose($connection);
}

file_put_contents($transcriptPath, implode("\n", $transcript));
exit(0);
