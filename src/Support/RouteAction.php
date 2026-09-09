<?php

namespace NightOwl\Support;

/**
 * Collapses the repeated component lists laravel/nightwatch builds for Livewire
 * requests.
 *
 * Nightwatch's LivewireListener calls captureRequestRouteAction() once per
 * component hydrated in a request, and CapturesState appends each one to the
 * route action with ', ' — no dedupe, no bound (laravel/nightwatch 1.26.1,
 * Concerns/CapturesState.php:541). Livewire hydrates every component carried
 * in the request, and a page whose components all poll — or all listen to one
 * dispatched event — sends them all in one request, so 78 of the same
 * component report a 2,884-byte action that is one class name repeated 78
 * times.
 *
 * Two separate harms, and the second is the one that matters:
 *
 *  1. Size. ~2.9 KB of route action per request, carrying 35 bytes of information.
 *  2. Cardinality. nightowl_dict_route is keyed by a hash of the tuple and is
 *     append-only (migration 000066 states the assumption outright: "hundreds of
 *     rows per tenant, not millions"). The component list changes with the page's
 *     state, so 78 cards and 79 cards hash differently and each mints a NEW
 *     permanent row.
 *
 * SORTED and deduped. Both halves are deliberate and were both got wrong first
 * time round:
 *
 *  - Dedupe, never run-length encoding. A count ("QuestionCard ×78") fixes the
 *    size and leaves the cardinality exactly as it was, because the count is the
 *    part that keeps changing.
 *  - Sort, rather than first-occurrence order. First-occurrence order rests on
 *    Livewire hydrating a given page in a stable sequence, which is an assumption
 *    nothing here can check; where it does not hold, a 25-component page whose
 *    components rotate mints 25 permanent rows. Sorting makes the value
 *    order-invariant, so rotation costs nothing. The price is that the action no
 *    longer reads in hydration order — the components are all still there.
 *
 * NOT truncated. An earlier version capped the list at 20 distinct components
 * with a "…" marker. On the shared livewire.update route the action is the ONLY
 * field distinguishing one page from another (method, domain, path, name and
 * methods are identical for every Livewire update in an app), so two pages that
 * shared a 20-component layout prefix collapsed onto one dictionary row and the
 * components that identified them were exactly the ones discarded. The cap also
 * did not bound cardinality — it bounded length — because the surviving window
 * shifts with which components hydrate. Width is the width clamp's job
 * (RecordWriter::clampDictRow); it is not this method's.
 *
 * What this does NOT do, and the earlier docblock wrongly claimed: collapse every
 * state of a page onto one row. Component MEMBERSHIP is part of the value, so a
 * page whose components appear and disappear (conditional rendering, a modal) still
 * mints one row per distinct SET. That is bounded by the app's real page shapes
 * rather than by its traffic, which is the difference that matters, but it is not
 * one row. A tenant whose sets genuinely explode wants NIGHTOWL_IGNORE_ROUTES.
 *
 * Non-Livewire actions ("App\Http\Controllers\FooController@index") contain no
 * ', ' and come back untouched.
 */
final class RouteAction
{
    /**
     * Collapse a comma-joined component list to its sorted distinct members.
     *
     * Cheap on the overwhelming majority of records: an action with no ', '
     * returns before any allocation.
     */
    public static function normalize(string $action): string
    {
        if (! str_contains($action, ', ')) {
            return $action;
        }

        $seen = [];
        foreach (explode(', ', $action) as $part) {
            // Nightwatch joins with a literal ', ', so parts are already clean;
            // trim defensively rather than trusting an upstream format detail.
            $part = trim($part);
            if ($part === '') {
                continue;
            }
            // Keys, not in_array: a 78-entry list would otherwise be quadratic,
            // and this runs on every request record of every batch.
            $seen[$part] = true;
        }

        if ($seen === []) {
            return $action;
        }

        $components = array_keys($seen);
        // ksort would order by PHP's array-key rules, which silently coerce a
        // numeric-looking class name to an int. Sort the extracted list instead.
        sort($components, SORT_STRING);

        return implode(', ', $components);
    }
}
