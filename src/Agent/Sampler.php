<?php

namespace NightOwl\Agent;

final class Sampler
{
    /**
     * @param float      $defaultRate   Global sample rate (fallback)
     * @param float|null $requestRate   Sample rate for request entry points
     * @param float|null $commandRate   Sample rate for command entry points
     * @param float|null $scheduledRate Sample rate for scheduled task entry points
     */
    public function __construct(
        private float $sampleRate = 1.0,
        private ?float $requestRate = null,
        private ?float $commandRate = null,
        private ?float $scheduledRate = null,
    ) {}

    /**
     * Determine whether a payload should be kept.
     *
     * Sampling is per-payload, not per-record. If any record in the payload
     * is an exception or a 5xx request, the entire payload is kept regardless
     * of sample rate.
     *
     * The entry point type (request, command, scheduled_task) determines which
     * sample rate is used. If no type-specific rate is set, the default applies.
     *
     * @param array $records Decoded records from the payload
     */
    public function shouldKeep(array $records): bool
    {
        $rate = $this->resolveRate($records);

        if ($rate >= 1.0) {
            return true;
        }

        // Exceptions and 5xx requests are always kept
        foreach ($records as $record) {
            if (! is_array($record)) {
                continue;
            }

            $type = $record['t'] ?? null;

            if ($type === 'exception') {
                return true;
            }

            if ($type === 'request' && isset($record['status_code']) && $record['status_code'] >= 500) {
                return true;
            }
        }

        return (mt_rand() / mt_getrandmax()) <= $rate;
    }

    /**
     * Determine the sample rate based on the entry point type in the payload.
     */
    private function resolveRate(array $records): float
    {
        foreach ($records as $record) {
            if (! is_array($record)) {
                continue;
            }

            $type = $record['t'] ?? null;

            if ($type === 'request' && $this->requestRate !== null) {
                return $this->requestRate;
            }

            if ($type === 'command' && $this->commandRate !== null) {
                return $this->commandRate;
            }

            if ($type === 'scheduled_task' && $this->scheduledRate !== null) {
                return $this->scheduledRate;
            }
        }

        return $this->sampleRate;
    }
}
