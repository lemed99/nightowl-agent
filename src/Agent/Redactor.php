<?php

namespace NightOwl\Agent;

final class Redactor
{
    /** @var array<string, int> Lowercase keys to redact (flipped for O(1) isset lookup) */
    private array $keys;

    /**
     * @param string[] $keys Keys to redact (case-insensitive)
     */
    public function __construct(
        array $keys = [],
        private bool $enabled = false,
    ) {
        $this->keys = array_flip(array_map('strtolower', $keys));
    }

    /**
     * Redact sensitive keys from records.
     *
     * @param array $records
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
            if (is_string($key) && isset($this->keys[strtolower($key)])) {
                $data[$key] = '[REDACTED]';
                $modified = true;
            } elseif (is_array($value)) {
                $data[$key] = $this->walk($value, $modified);
            }
        }

        return $data;
    }
}
