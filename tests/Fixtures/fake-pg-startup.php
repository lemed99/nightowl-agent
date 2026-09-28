<?php

/**
 * Records how a Postgres client opens a connection, to tell which sslmode it
 * was given without needing a server with SSL.
 *
 * libpq opens with an SSLRequest packet unless sslmode is 'disable', and with
 * the StartupMessage directly when it is. This answers 'N' (no SSL) to an
 * SSLRequest, and to a GSSENCRequest (sent first when libpq has GSS
 * credentials), records each packet, and hangs up at the StartupMessage, so the
 * client's connect fails fast.
 *
 *   php fake-pg-startup.php <transcript-path> <base64 json script>
 *
 * Transcript lines: GSSENC, SSL, STARTUP, in the order received. With 'prefer'
 * that is SSL then STARTUP, with 'disable' STARTUP alone, with 'require' SSL
 * alone (the client gives up at the 'N').
 *
 * With 'ready' it plays a server that takes the connection and then goes
 * silent: it answers the StartupMessage with AuthenticationOk and
 * ReadyForQuery, records each query as "QUERY <sql>" and never answers one.
 * It exits when the client hangs up or 'hold' seconds have passed.
 *
 * Script keys (all optional):
 *   startup   seconds to wait for the connection, default 10
 *   ready     complete the handshake, then stall on the first query
 *   hold      seconds to stay connected in 'ready' mode, default 20
 */

$transcriptPath = $argv[1] ?? null;
$script = json_decode(base64_decode($argv[2] ?? '') ?: '{}', true) ?: [];

if ($transcriptPath === null) {
    fwrite(STDERR, "usage: fake-pg-startup.php <transcript-path> <base64 json>\n");
    exit(1);
}

$server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
if (! $server) {
    fwrite(STDERR, "bind failed: {$errstr}\n");
    exit(1);
}

$name = stream_socket_get_name($server, false);
echo 'PORT='.substr($name, strrpos($name, ':') + 1)."\n";
flush();

$connection = @stream_socket_accept($server, (float) ($script['startup'] ?? 10));
if ($connection === false) {
    exit(0);
}
stream_set_timeout($connection, 5);

$lines = [];
for ($packet = 0; $packet < 3; $packet++) {
    $head = '';
    while (strlen($head) < 8 && ! feof($connection)) {
        $chunk = fread($connection, 8 - strlen($head));
        if ($chunk === false || $chunk === '') {
            if (stream_get_meta_data($connection)['timed_out']) {
                break 2;
            }

            continue;
        }
        $head .= $chunk;
    }

    if (strlen($head) < 8) {
        break;
    }

    ['length' => $length, 'code' => $code] = unpack('Nlength/Ncode', $head);

    if ($length === 8 && $code === 80877104) {
        $lines[] = 'GSSENC';
        fwrite($connection, 'N');

        continue;
    }

    if ($length === 8 && $code === 80877103) {
        $lines[] = 'SSL';
        fwrite($connection, 'N');

        continue;
    }

    $lines[] = 'STARTUP';

    break;
}

if (($script['ready'] ?? false) && end($lines) === 'STARTUP') {
    $deadline = microtime(true) + (float) ($script['hold'] ?? 20);
    stream_set_timeout($connection, 1);

    // Exactly $length bytes, or null when the client hung up or time ran out.
    $read = static function (int $length) use ($connection, $deadline): ?string {
        $data = '';
        while (strlen($data) < $length) {
            if (microtime(true) >= $deadline) {
                return null;
            }
            $chunk = fread($connection, $length - strlen($data));
            if ($chunk === false || $chunk === '') {
                if (feof($connection)) {
                    return null;
                }

                continue;
            }
            $data .= $chunk;
        }

        return $data;
    };

    // The rest of the StartupMessage (user, database), past the 8 bytes read.
    $read(max(0, $length - 8));

    $parameter = "server_version\x0016.0\x00";
    fwrite($connection,
        'R'.pack('NN', 8, 0)                                  // AuthenticationOk
        .'S'.pack('N', 4 + strlen($parameter)).$parameter      // ParameterStatus
        .'K'.pack('NNN', 12, 4242, 0)                         // BackendKeyData
        .'Z'.pack('N', 5).'I'                                  // ReadyForQuery, idle
    );

    while (($type = $read(1)) !== null) {
        $head = $read(4);
        $body = $head === null ? null : $read(max(0, unpack('N', $head)[1] - 4));
        if ($body === null) {
            break;
        }

        $lines[] = $type === 'Q' ? 'QUERY '.rtrim($body, "\0") : 'MESSAGE '.$type;

        if ($type === 'X') {
            break;
        }
    }
}

fclose($connection);
file_put_contents($transcriptPath, implode("\n", $lines));
