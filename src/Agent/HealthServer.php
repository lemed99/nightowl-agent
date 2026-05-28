<?php

namespace NightOwl\Agent;

use React\EventLoop\LoopInterface;
use React\Socket\ConnectionInterface;
use React\Socket\SocketServer;

final class HealthServer
{
    private ?SocketServer $socket = null;

    public function __construct(
        private LoopInterface $loop,
    ) {}

    public function listen(string $host, int $port, AsyncServer $agent): void
    {
        $this->socket = new SocketServer("{$host}:{$port}", [], $this->loop);
        $this->socket->on('connection', function (ConnectionInterface $conn) use ($agent) {
            $buffer = '';
            $conn->on('data', function (string $chunk) use ($conn, $agent, &$buffer) {
                $buffer .= $chunk;
                if (!str_contains($buffer, "\r\n\r\n")) {
                    return;
                }
                $firstLine = strtok($buffer, "\r\n");
                $parts = explode(' ', $firstLine, 3);
                $method = $parts[0] ?? '';
                $path = $parts[1] ?? '';

                if ($method === 'GET' && $path === '/status') {
                    $body = json_encode($agent->getStatus());
                    $statusLine = '200 OK';
                } else {
                    $body = json_encode(['error' => 'Not found']);
                    $statusLine = '404 Not Found';
                }

                $conn->write(
                    "HTTP/1.1 {$statusLine}\r\n"
                    . "Content-Type: application/json\r\n"
                    . "Content-Length: " . strlen($body) . "\r\n"
                    . "Connection: close\r\n"
                    . "\r\n"
                    . $body
                );
                $conn->end();
            });
        });
    }

    public function close(): void
    {
        if ($this->socket !== null) {
            $this->socket->close();
            $this->socket = null;
        }
    }
}
