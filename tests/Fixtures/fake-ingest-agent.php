<?php

/**
 * A stand-in for an agent's ingest listener, for tests that follow records out
 * of a real Laravel application.
 *
 * Built like fake-http-server.php: binds port 0 and prints the port as its first
 * stdout line, so the parent learns it only after the listener exists. Speaks
 * the wire protocol Nightwatch's Ingest sends (`<length>:<frame>`), answers
 * every payload with `2:OK`, and records it.
 *
 *   php fake-ingest-agent.php <transcript-path> <base64 json script>
 *
 * The transcript is one line per payload: the frame after its length prefix,
 * `v1:<tokenHash>:<json records>`. json_encode escapes newlines, so a frame is
 * always one line.
 *
 * Script keys (all optional):
 *   startup   seconds to wait for the first connection, default 10
 *   idle      seconds without a connection before exiting, default 1
 */

$transcriptPath = $argv[1] ?? null;
$script = json_decode(base64_decode($argv[2] ?? '') ?: '{}', true) ?: [];

if ($transcriptPath === null) {
    fwrite(STDERR, "usage: fake-ingest-agent.php <transcript-path> <base64 json>\n");
    exit(1);
}

$startup = (float) ($script['startup'] ?? 10);
$idle = (float) ($script['idle'] ?? 1);

$server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
if (! $server) {
    fwrite(STDERR, "bind failed: {$errstr}\n");
    exit(1);
}

$name = stream_socket_get_name($server, false);
echo 'PORT='.substr($name, strrpos($name, ':') + 1)."\n";
flush();

$timeout = $startup;

while (($connection = @stream_socket_accept($server, $timeout)) !== false) {
    $timeout = $idle;
    stream_set_timeout($connection, 5);

    $data = '';
    while (! feof($connection)) {
        $chunk = fread($connection, 65536);
        if ($chunk === false || $chunk === '') {
            if (stream_get_meta_data($connection)['timed_out']) {
                break;
            }

            continue;
        }

        $data .= $chunk;
        $colon = strpos($data, ':');
        if ($colon !== false && strlen($data) >= $colon + 1 + (int) substr($data, 0, $colon)) {
            break;
        }
    }

    @fwrite($connection, '2:OK');
    fclose($connection);

    $colon = strpos($data, ':');
    if ($colon !== false) {
        file_put_contents($transcriptPath, substr($data, $colon + 1)."\n", FILE_APPEND);
    }
}
