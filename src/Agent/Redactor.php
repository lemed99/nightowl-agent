<?php

namespace NightOwl\Agent;

final class Redactor
{
    /** @var array<string, int> Lowercase keys to redact (flipped for O(1) isset lookup) */
    private array $keys;

    /** Keys whose string values may contain URL query parameters we should scrub. */
    private const URL_LIKE_KEYS = [
        'url' => 1,
        'uri' => 1,
        'endpoint' => 1,
        'href' => 1,
        'request_url' => 1,
    ];

    /**
     * @param  string[]  $keys  Keys to redact (case-insensitive)
     */
    public function __construct(
        array $keys = [],
        private bool $enabled = true,
    ) {
        $this->keys = array_flip(array_map('strtolower', $keys));
    }

    /**
     * Redact sensitive keys from records.
     *
     * @return array{0: array, 1: bool} [redacted_records, was_modified]
     */
    public function redact(array $records): array
    {
        if (! $this->enabled || empty($this->keys)) {
            return [$records, false];
        }

        $modified = false;
        $result = $this->walk($records, $modified);

        return [$result, $modified];
    }

    private function walk(array $data, bool &$modified): array
    {
        foreach ($data as $key => $value) {
            $keyLc = is_string($key) ? strtolower($key) : null;

            if ($keyLc !== null && isset($this->keys[$keyLc])) {
                $data[$key] = '[REDACTED]';
                $modified = true;
            } elseif ($keyLc !== null && is_string($value) && isset(self::URL_LIKE_KEYS[$keyLc])) {
                $scrubbed = $this->scrubUrl($value);
                if ($scrubbed !== $value) {
                    $data[$key] = $scrubbed;
                    $modified = true;
                }
            } elseif (is_array($value)) {
                $data[$key] = $this->walk($value, $modified);
            }
        }

        return $data;
    }

    /**
     * Redact query-string parameters whose names match the configured keys.
     * E.g. `?api_key=abc&foo=bar` with api_key in the list → `?api_key=[REDACTED]&foo=bar`.
     */
    private function scrubUrl(string $url): string
    {
        if (! str_contains($url, '?')) {
            return $url;
        }

        return (string) preg_replace_callback(
            '/([?&])([^=&#]+)=([^&#]*)/',
            function (array $m): string {
                $name = strtolower(urldecode($m[2]));
                if (isset($this->keys[$name])) {
                    return $m[1].$m[2].'=[REDACTED]';
                }

                return $m[0];
            },
            $url,
        );
    }
}
