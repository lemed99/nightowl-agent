<?php

namespace NightOwl\Agent;

use RuntimeException;

final class Server
{
    private bool $running = true;

    /** @var resource|null */
    private $socket;

    /** @var array<int, array{stream: resource, data: string, connected_at: float}> */
    private array $clients = [];

    public function __construct(
        private ConnectionHandler $handler,
    ) {}

    public function listen(string $host, int $port): void
    {
        $address = "tcp://{$host}:{$port}";
        $errno = 0;
        $errstr = '';

        $this->socket = stream_socket_server($address, $errno, $errstr);

        if ($this->socket === false) {
            throw new RuntimeException("Could not bind to {$address}: {$errstr} ({$errno})");
        }

        stream_set_blocking($this->socket, false);

        while ($this->running) {
            $this->tick();
        }

        // Clean up remaining clients
        foreach ($this->clients as $id => $client) {
            if (is_resource($client['stream'])) {
                fclose($client['stream']);
            }
        }
        $this->clients = [];

        fclose($this->socket);
    }

    private function tick(): void
    {
        // Build read array: server socket + all connected clients
        $read = [$this->socket];
        foreach ($this->clients as $client) {
            $read[] = $client['stream'];
        }
        $write = $except = null;

        $changed = @stream_select($read, $write, $except, 0, 200000); // 200ms timeout

        if ($changed === false) {
            return;
        }

        // Check for new connections
        if (in_array($this->socket, $read, true)) {
            $client = @stream_socket_accept($this->socket, timeout: 0);

            if ($client !== false) {
                stream_set_blocking($client, false);
                $id = (int) $client;
                $this->clients[$id] = [
                    'stream' => $client,
                    'data' => '',
                    'connected_at' => microtime(true),
                ];
            }
        }

        // Process data from existing clients
        foreach ($this->clients as $id => $client) {
            if (! in_array($client['stream'], $read, true)) {
                // Check for timeout (10 seconds)
                if (microtime(true) - $client['connected_at'] > 10) {
                    $this->removeClient($id);
                }

                continue;
            }

            $chunk = @fread($client['stream'], 65536);

            if ($chunk === false || ($chunk === '' && feof($client['stream']))) {
                // Connection closed — process whatever we have
                if ($client['data'] !== '') {
                    $this->dispatch($id);
                } else {
                    $this->removeClient($id);
                }

                continue;
            }

            $this->clients[$id]['data'] .= $chunk;

            // Check if we have a complete payload
            if ($this->hasCompletePayload($this->clients[$id]['data'])) {
                $this->dispatch($id);
            }
        }
    }

    private function dispatch(int $id): void
    {
        $client = $this->clients[$id] ?? null;
        if ($client === null) {
            return;
        }

        try {
            $this->handler->handle($client['stream'], $client['data']);
        } catch (\Throwable $e) {
            error_log("[NightOwl Agent] Error: {$e->getMessage()}");
        }

        $this->removeClient($id);
    }

    private function removeClient(int $id): void
    {
        if (isset($this->clients[$id]) && is_resource($this->clients[$id]['stream'])) {
            fclose($this->clients[$id]['stream']);
        }
        unset($this->clients[$id]);
    }

    private function hasCompletePayload(string $data): bool
    {
        $colonPos = strpos($data, ':');
        if ($colonPos === false) {
            return false;
        }

        $declaredLength = (int) substr($data, 0, $colonPos);
        $expectedTotal = $colonPos + 1 + $declaredLength;

        return strlen($data) >= $expectedTotal;
    }

    public function stop(): void
    {
        $this->running = false;
    }
}
