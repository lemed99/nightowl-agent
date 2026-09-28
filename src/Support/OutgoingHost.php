<?php

namespace NightOwl\Support;

/**
 * One spelling of an outgoing-request host, so a host threshold matches the
 * calls it names whichever way it was written.
 *
 * The two sides disagree on the spelling. The SDK records `host` as the bare
 * host, lowercased and without a port (`$uri->getHost()` in laravel/nightwatch's
 * OutgoingRequestSensor), and groups calls by that alone. The dashboard lists
 * hosts as scheme + authority, `https://api.stripe.com` or
 * `http://localhost:8080` (the rollup's `host`, RollupSpecs::outgoingRequests()),
 * and the Thresholds dialog saves what it lists. Both reduce here to the bare
 * host: lowercase, with no scheme, userinfo, port, path or trailing slash. The
 * port goes too because the record never carries one.
 *
 * nightowl-api keeps a copy in App\Support\PerformanceThresholds::normalizeHost()
 * (the API cannot load this package in production); keep the two in step.
 */
final class OutgoingHost
{
    public static function normalize(string $host): string
    {
        $host = strtolower(trim($host));

        // "https://" or a scheme-relative "//".
        if (preg_match('~^(?:[a-z][a-z0-9+.-]*:)?//~', $host, $scheme) === 1) {
            $host = substr($host, strlen($scheme[0]));
        }

        // Path, query and fragment, a trailing slash included.
        $host = preg_split('~[/?#]~', $host, 2)[0];

        if (($at = strrpos($host, '@')) !== false) {
            $host = substr($host, $at + 1);
        }

        // An IPv6 literal keeps its brackets, the way getHost() returns it.
        if (str_starts_with($host, '[')) {
            $end = strpos($host, ']');

            return $end === false ? $host : substr($host, 0, $end + 1);
        }

        return match (substr_count($host, ':')) {
            0 => $host,
            1 => strstr($host, ':', true),
            default => '['.$host.']',
        };
    }
}
