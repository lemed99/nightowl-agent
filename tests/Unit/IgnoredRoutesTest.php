<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\IgnoredRoutes;
use PHPUnit\Framework\TestCase;

class IgnoredRoutesTest extends TestCase
{
    public function test_parse_handles_the_shapes_an_env_var_actually_carries(): void
    {
        $this->assertSame([], IgnoredRoutes::parse(null));
        $this->assertSame([], IgnoredRoutes::parse(''));
        $this->assertSame([], IgnoredRoutes::parse('  ,, '));
        $this->assertSame(['livewire/*'], IgnoredRoutes::parse('livewire/*'));
        $this->assertSame(
            ['*livewire.update', '/horizon/*', '_debugbar/*'],
            IgnoredRoutes::parse('*livewire.update, /horizon/* ,,  _debugbar/*  '),
        );
    }

    /**
     * Bad entries are dropped at parse time, not discovered at match time — an
     * invalid-UTF-8 pattern would make preg_match warn on every record, which the
     * daemon promotes to an exception that wedges the drain.
     */
    public function test_parse_drops_entries_that_would_break_the_matcher(): void
    {
        $this->assertSame(['ok/*'], IgnoredRoutes::parse(['ok/*', "bad\xC3(", null, 42, '']));
        $this->assertSame(['a', 'b'], IgnoredRoutes::parse('a, b, a'), 'deduped');
        $this->assertSame(['a,b'], IgnoredRoutes::parse(['a,b']), 'an already-parsed list is not re-split');
        $this->assertSame(['a', 'b'], IgnoredRoutes::parse('a,b'), 'a raw env string from a published config is split');
        $this->assertSame([], IgnoredRoutes::parse(false));
        $this->assertSame([], IgnoredRoutes::parse(null));
    }

    public function test_no_patterns_matches_nothing(): void
    {
        $this->assertFalse(IgnoredRoutes::matches([], ['route_path' => 'livewire/update']));
    }

    public function test_matches_on_path_name_or_action(): void
    {
        $record = [
            'route_path' => 'livewire-09e76b9c/update',
            'route_name' => 'default-livewire.update',
            'route_action' => 'App\Livewire\Proposals\QuestionCard',
        ];

        $this->assertTrue(IgnoredRoutes::matches(['livewire-*/update'], $record), 'path');
        $this->assertTrue(IgnoredRoutes::matches(['*livewire.update'], $record), 'name');
        $this->assertTrue(IgnoredRoutes::matches(['App\Livewire\*'], $record), 'action');
        $this->assertFalse(IgnoredRoutes::matches(['horizon/*'], $record));
    }

    /**
     * Livewire 3 randomizes the update path per installation, so the route NAME
     * is the stable handle — the one an operator can copy off `route:list` and
     * have keep working after a redeploy.
     */
    public function test_the_stable_livewire_handle_is_the_route_name(): void
    {
        $patterns = ['*livewire.update'];

        foreach (['livewire-09e76b9c/update', 'livewire-ff003a21/update'] as $path) {
            $this->assertTrue(IgnoredRoutes::matches($patterns, [
                'route_path' => $path,
                'route_name' => 'default-livewire.update',
            ]));
        }
    }

    /** An operator writes the pattern off a URL; the payload carries no leading slash. */
    public function test_a_leading_slash_is_optional_on_both_sides(): void
    {
        $this->assertTrue(IgnoredRoutes::matches(['/livewire/*'], ['route_path' => 'livewire/update']));
        $this->assertTrue(IgnoredRoutes::matches(['livewire/*'], ['route_path' => '/livewire/update']));
        $this->assertTrue(IgnoredRoutes::matches(['/livewire/*'], ['route_path' => '/livewire/update']));
        $this->assertTrue(IgnoredRoutes::matches(['livewire/*'], ['route_path' => 'livewire/update']));
    }

    public function test_a_pattern_must_match_the_whole_value(): void
    {
        // Str::is anchors both ends — a bare prefix is not a match without a wildcard.
        $this->assertFalse(IgnoredRoutes::matches(['livewire'], ['route_path' => 'livewire/update']));
        $this->assertTrue(IgnoredRoutes::matches(['livewire*'], ['route_path' => 'livewire/update']));
    }

    public function test_missing_and_blank_fields_are_skipped(): void
    {
        $this->assertFalse(IgnoredRoutes::matches(['*'], []));
        $this->assertFalse(IgnoredRoutes::matches(['*'], ['route_path' => '', 'route_name' => null]));
        $this->assertFalse(IgnoredRoutes::matches(['*'], ['route_path' => ['not', 'a', 'string']]));
    }
}
