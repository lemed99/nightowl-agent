<?php

namespace NightOwl\Support;

/**
 * Builds the agent instance identifier (`hostname:pid`) used as the per-instance
 * key in health reports.
 *
 * The API stores this in a varchar(191) column and validates `max:191`, so the
 * value is capped here too — a long FQDN or Kubernetes pod hostname would
 * otherwise overflow and 422 the entire health report. The `:pid` suffix is the
 * per-process disambiguator and is always preserved; only the hostname is
 * truncated when the combined length exceeds the limit.
 */
final class AgentInstanceId
{
    public const MAX_LENGTH = 191;

    public static function current(): string
    {
        return self::build(gethostname() ?: 'unknown', (int) getmypid());
    }

    public static function build(string $host, int $pid): string
    {
        $suffix = ':' . $pid;
        $maxHost = self::MAX_LENGTH - strlen($suffix);

        if ($maxHost > 0 && strlen($host) > $maxHost) {
            $host = substr($host, 0, $maxHost);
        }

        return $host . $suffix;
    }
}
