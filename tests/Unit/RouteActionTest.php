<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\RouteAction;
use PHPUnit\Framework\TestCase;

class RouteActionTest extends TestCase
{
    public function test_an_ordinary_controller_action_is_untouched(): void
    {
        foreach ([
            'App\Http\Controllers\SearchController@index',
            'Closure',
            'App\Http\Controllers\InvokableController',
            '',
        ] as $action) {
            $this->assertSame($action, RouteAction::normalize($action));
        }
    }

    /** The reported case: one class repeated 78 times, 2,884 bytes. */
    public function test_a_repeated_livewire_component_collapses_to_one(): void
    {
        $component = 'App\Livewire\Proposals\QuestionCard';
        $action = implode(', ', array_fill(0, 78, $component));

        $this->assertSame($component, RouteAction::normalize($action));
    }

    /**
     * The cardinality claim, stated exactly: COUNT variation collapses (78 cards
     * and 100 cards are one row). MEMBERSHIP variation does not — an extra
     * component is a different value and a different row, which is the honest
     * bound this gives and is why the docblock no longer says "one row".
     */
    public function test_count_variation_collapses_but_membership_variation_does_not(): void
    {
        $card = 'App\Livewire\Proposals\QuestionCard';
        $bar = 'App\Livewire\Proposals\QuestionProgressBar';

        $seventyEight = implode(', ', array_fill(0, 78, $card));
        $sevenNine = $bar.', '.implode(', ', array_fill(0, 78, $card));
        $oneHundred = implode(', ', array_fill(0, 100, $card));

        $this->assertSame($card, RouteAction::normalize($seventyEight));
        $this->assertSame($card, RouteAction::normalize($oneHundred));
        // Sorted, so membership — not hydration order — decides the value.
        $this->assertSame($card.', '.$bar, RouteAction::normalize($sevenNine));
    }

    public function test_distinct_components_are_kept_and_sorted(): void
    {
        $this->assertSame(
            'App\Livewire\Bar, App\Livewire\Card, App\Livewire\Footer',
            RouteAction::normalize('App\Livewire\Footer, App\Livewire\Card, App\Livewire\Card, App\Livewire\Bar, App\Livewire\Card')
        );
    }

    /**
     * The property the cap could not give us and sorting does. A page whose
     * components hydrate in a different order each request is the same page, and
     * dict_route is append-only — order-sensitivity mints a permanent row per
     * rotation. Rotating a 25-component page through every starting position must
     * produce exactly ONE value.
     */
    public function test_rotating_the_hydration_order_produces_one_value(): void
    {
        $components = [];
        for ($i = 0; $i < 25; $i++) {
            $components[] = 'App\Livewire\C'.$i;
        }

        $values = [];
        for ($shift = 0; $shift < 25; $shift++) {
            $rotated = array_merge(array_slice($components, $shift), array_slice($components, 0, $shift));
            $values[RouteAction::normalize(implode(', ', $rotated))] = true;
        }

        $this->assertCount(1, $values, 'every rotation of one page must share a single dictionary row');
    }

    /**
     * Nothing is discarded. A wide page keeps every distinct component, because on
     * the shared livewire.update route the action is the only field that tells one
     * page from another — truncating it merged genuinely different pages.
     */
    public function test_a_wide_page_keeps_every_distinct_component(): void
    {
        // 150 components: any silent cap — 20, 40, 78, 100 — changes the SET,
        // and the assertion is on the exact set, not a count or a marker.
        $components = [];
        for ($i = 0; $i < 150; $i++) {
            $components[] = 'App\Livewire\Widget'.str_pad((string) $i, 3, '0', STR_PAD_LEFT);
        }
        $expected = $components;
        sort($expected, SORT_STRING);
        shuffle($components);

        $this->assertSame(implode(', ', $expected), RouteAction::normalize(implode(', ', $components)));
    }

    /**
     * SORT_STRING is load-bearing. Under SORT_REGULAR (or ksort over the seen
     * map) "100" and "1e2" compare numerically equal, the tie falls back to
     * insertion order, and the value becomes order-dependent again — the exact
     * property sorting exists to remove. PHP class names cannot be numeric, so
     * this is a refactor hazard rather than a live bug; pin it anyway.
     */
    public function test_sort_is_lexical_not_numeric(): void
    {
        $a = RouteAction::normalize('100, 1e2, 20');
        $b = RouteAction::normalize('1e2, 100, 20');

        $this->assertSame($a, $b, 'numeric-looking components must sort identically from any order');
        $this->assertSame('100, 1e2, 20', $a);
    }

    /**
     * Two pages sharing a long prefix but differing in their own components must
     * NOT collapse — the defect the 20-component cap introduced.
     */
    public function test_pages_sharing_a_layout_prefix_stay_distinct(): void
    {
        $layout = [];
        for ($i = 0; $i < 20; $i++) {
            $layout[] = 'App\Livewire\Layout\Part'.str_pad((string) $i, 2, '0', STR_PAD_LEFT);
        }

        $invoices = RouteAction::normalize(implode(', ', [...$layout, 'App\Livewire\Invoices\Table', 'App\Livewire\Invoices\Filters']));
        $reports = RouteAction::normalize(implode(', ', [...$layout, 'App\Livewire\Reports\Chart', 'App\Livewire\Reports\Export']));

        $this->assertNotSame($invoices, $reports);
        $this->assertStringContainsString('Invoices\Table', $invoices);
        $this->assertStringContainsString('Reports\Chart', $reports);
    }

    public function test_blank_and_malformed_lists_degrade_safely(): void
    {
        $this->assertSame(', ', RouteAction::normalize(', '));
        $this->assertSame('A', RouteAction::normalize('A, , A'));
        $this->assertSame('A, B', RouteAction::normalize('A,  B'));
    }
}
