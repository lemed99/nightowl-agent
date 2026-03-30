<?php

namespace NightOwl\Agent;

use React\EventLoop\LoopInterface;
use React\Http\HttpServer;
use React\Http\Message\Response;
use React\Socket\SocketServer;
use Psr\Http\Message\ServerRequestInterface;

final class HealthServer
{
    private ?HttpServer $server = null;
    private ?SocketServer $socket = null;

    public function __construct(
        private LoopInterface $loop,
    ) {}

    public function listen(string $host, int $port, AsyncServer $agent): void
    {
        $this->server = new HttpServer(function (ServerRequestInterface $request) use ($agent) {
            if ($request->getMethod() !== 'GET' || $request->getUri()->getPath() !== '/status') {
                return new Response(404, ['Content-Type' => 'application/json'], json_encode(['error' => 'Not found']));
            }

            $status = $agent->getStatus();

            return new Response(200, ['Content-Type' => 'application/json'], json_encode($status));
        });

        $this->socket = new SocketServer("{$host}:{$port}", [], $this->loop);
        $this->server->listen($this->socket);
    }

    public function close(): void
    {
        if ($this->socket !== null) {
            $this->socket->close();
            $this->socket = null;
        }

        $this->server = null;
    }
}
