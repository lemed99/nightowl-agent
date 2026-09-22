<?php

namespace NightOwl\Support;

use Illuminate\Support\Str;

/**
 * Glob matching for `nightowl.ignore_routes`.
 *
 * The case this exists for is framework chatter that costs storage and tells the
 * customer nothing — Livewire's update endpoint fires on nearly every keystroke,
 * and its route action lists every component in the request (see RouteAction).
 *
 * A route is matched on THREE fields — path, name and action — because which one
 * identifies "the Livewire update route" differs by version. Livewire 4 prefixes
 * the path with a hash of APP_KEY (`/livewire-09e76b9c/update`), so a path
 * pattern written for one install fails on the next; the route name is what
 * most people should reach for (see below for both versions' names). Matching all three means an operator
 * does not have to know which one their framework happens to expose.
 *
 * Things an operator needs to know about writing patterns. `*` is the only
 * wildcard and it spans `/`; `?` is a literal, not a single-character wildcard
 * — this is Str::is, not shell glob. The env var is comma-separated, so a
 * pattern cannot itself contain a comma — and nightwatch joins Livewire
 * components with ', ', so a multi-component action can never be pasted in
 * whole; match a fragment (`*QuestionCard*`) or, better, the route NAME. And
 * matching runs AFTER RouteAction::normalize, so the value tested is the same
 * collapsed one the dashboard shows (subject to the v1 column's own width).
 *
 * Which Livewire is which: Livewire 3 registers `POST /livewire/update` named
 * `default.livewire.update`. Livewire 4 prefixes the path with a hash of
 * APP_KEY (`/livewire-09e76b9c/update` — deterministic per install, not
 * random) and names it `default-livewire.update`. `*livewire.update` matches
 * both names; `livewire/*` matches only the v3 path.
 *
 * Leading slashes are normalized away. Route paths arrive from Nightwatch
 * without one (`livewire-09e76b9c/update`), but an operator reading the value
 * off a URL will write `/livewire*`, and a pattern that silently matches nothing
 * is the worst possible outcome for a feature whose entire job is to not record
 * something.
 */
final class IgnoredRoutes
{
    /**
     * Turn whatever config holds into a clean pattern list.
     *
     * Accepts the env string ("a, b"), an already-parsed list, or null — because
     * a customer who published config/nightowl.php and edited it to a raw
     * `env('NIGHTOWL_IGNORE_ROUTES')` hands the reader a string, and a `(array)`
     * cast of that was one comma-bearing pattern that silently matched nothing.
     *
     * Every entry is VALIDATED here rather than trusted at match time. Str::is
     * compiles the pattern into a regex under the `u` modifier, so an invalid
     * UTF-8 byte in a pattern (a latin-1 é pasted into .env) makes preg_match
     * warn on every record — and under the daemon Laravel promotes that warning
     * to an ErrorException, which escapes write() and wedges the drain. An
     * array entry raises "Array to string conversion" with the same result (other
     * non-strings are silently cast by Str::is). Both are dropped with a log line
     * instead, once per process.
     *
     * Lives here rather than inline in config/nightowl.php so the standalone
     * harness (which has no Laravel `env()`) parses the value the same way.
     *
     * `$setting` only names the env var in those log lines: the same parser
     * reads NIGHTOWL_CAPTURE_ROUTES (see PayloadCapture), which shares this
     * pattern language on purpose.
     *
     * @return array<int, string>
     */
    public static function parse(mixed $value, string $setting = 'NIGHTOWL_IGNORE_ROUTES'): array
    {
        if ($value === null || $value === false || $value === '') {
            return [];
        }

        $entries = is_array($value) ? $value : explode(',', (string) $value);

        $patterns = [];
        foreach ($entries as $entry) {
            if (! is_string($entry)) {
                self::warnOnce($setting, 'non-string entry ('.get_debug_type($entry).') dropped');

                continue;
            }
            $entry = trim($entry);
            if ($entry === '') {
                continue;
            }
            if (! mb_check_encoding($entry, 'UTF-8')) {
                self::warnOnce($setting, 'pattern with invalid UTF-8 dropped — it would make the route matcher throw on every record');

                continue;
            }
            $patterns[] = $entry;
        }

        return array_values(array_unique($patterns));
    }

    /** @var array<string, true> */
    private static array $warned = [];

    private static function warnOnce(string $setting, string $what): void
    {
        if (! isset(self::$warned[$setting.$what])) {
            self::$warned[$setting.$what] = true;
            error_log('[NightOwl Agent] '.$setting.': '.$what);
        }
    }

    /**
     * Does this request record match any pattern?
     *
     * @param  array<int, string>  $patterns
     * @param  array<string, mixed>  $record
     */
    public static function matches(array $patterns, array $record): bool
    {
        if ($patterns === []) {
            return false;
        }

        // Strip the leading slash from BOTH sides, not just the value: the
        // operator's `/livewire/*` and the payload's `livewire/update` have to
        // meet somewhere, and normalizing one side alone leaves that pair — the
        // most likely pair anyone will actually write — matching nothing.
        $patterns = array_map(static fn (string $p): string => ltrim($p, '/'), $patterns);

        foreach (['route_path', 'route_name', 'route_action'] as $field) {
            $value = $record[$field] ?? null;
            if (! is_string($value) || $value === '') {
                continue;
            }

            // No UTF-8 scrub on the VALUE: every record here came through
            // json_decode(..., JSON_THROW_ON_ERROR) in DrainWorker, which rejects
            // invalid UTF-8 outright, so a stray byte cannot reach this point. An
            // earlier scrub replaced such bytes with `?`, which then MATCHED a
            // literal `?` in a pattern — a false positive guarding an impossible
            // case. The pattern side is where the hazard lives; see parse().
            if (Str::is($patterns, ltrim($value, '/'))) {
                return true;
            }
        }

        return false;
    }
}
