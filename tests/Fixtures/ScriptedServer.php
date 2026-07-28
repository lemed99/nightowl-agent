<?php

namespace NightOwl\Tests\Fixtures;

/**
 * Launches one of the scripted servers in this directory as a subprocess and
 * hands back the port it bound plus, afterwards, everything it heard.
 *
 * Subprocess rather than fork so PHPUnit's shutdown handlers never run in two
 * processes at once, and port 0 rather than a fixed port so parallel runs and
 * a still-closing previous server can't collide. The child prints its port only
 * once the listener is bound, so reading that line IS the readiness handshake —
 * there is no sleep-and-hope anywhere in here.
 */
final class ScriptedServer
{
    /** Generous — it only ever elapses when the child is genuinely wedged. */
    private const EXIT_TIMEOUT_SECONDS = 10.0;

    private function __construct(
        public readonly int $port,
        private mixed $process,
        private array $pipes,
        private string $transcriptPath,
    ) {}

    /**
     * @param  string  $fixture  Script filename in tests/Fixtures, e.g. 'fake-smtp-server.php'
     * @param  array<string, mixed>  $script  Behaviour — see that fixture's docblock
     */
    public static function start(string $fixture, array $script = []): self
    {
        $transcriptPath = tempnam(sys_get_temp_dir(), 'nightowl-scripted-');

        $process = proc_open(
            [
                PHP_BINARY,
                __DIR__.'/'.$fixture,
                $transcriptPath,
                base64_encode((string) json_encode($script)),
            ],
            [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
        );

        if (! is_resource($process)) {
            throw new \RuntimeException("could not start {$fixture}");
        }

        // Non-blocking, because a PHPUnit failure message is built eagerly: a
        // blocking read of the child's stderr inside one waits for EOF — i.e.
        // for the server we are about to connect to to exit first — and every
        // connect then lands on a closed port.
        stream_set_blocking($pipes[2], false);

        $portLine = fgets($pipes[1]);

        if (! is_string($portLine) || preg_match('/^PORT=(\d+)$/', trim($portLine), $matches) !== 1) {
            throw new \RuntimeException(
                "{$fixture} never reported a port (got ".var_export($portLine, true).'); stderr: '
                .stream_get_contents($pipes[2])
            );
        }

        return new self((int) $matches[1], $process, $pipes, $transcriptPath);
    }

    /** Wait for the child to finish, then return the lines it recorded. */
    public function transcript(): array
    {
        $this->stop();

        $contents = (string) @file_get_contents($this->transcriptPath);

        return $contents === '' ? [] : explode("\n", $contents);
    }

    /** The single recorded line starting with $prefix, or null. */
    public function line(string $prefix): ?string
    {
        foreach ($this->transcript() as $line) {
            if (str_starts_with($line, $prefix)) {
                return $line;
            }
        }

        return null;
    }

    public function stop(): void
    {
        if (! is_resource($this->process)) {
            return;
        }

        $deadline = microtime(true) + self::EXIT_TIMEOUT_SECONDS;

        while (microtime(true) < $deadline && (proc_get_status($this->process)['running'] ?? false)) {
            usleep(20_000);
        }

        if (proc_get_status($this->process)['running'] ?? false) {
            // SIGKILL is only defined with ext-pcntl, which nothing here needs.
            proc_terminate($this->process, defined('SIGKILL') ? SIGKILL : 9);
        }

        foreach ($this->pipes as $pipe) {
            @fclose($pipe);
        }

        proc_close($this->process);
        $this->process = null;
    }

    public function cleanup(): void
    {
        $this->stop();
        @unlink($this->transcriptPath);
    }
}
