<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\CacheKeyTemplate;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CacheKeyTemplateTest extends TestCase
{
    /** @return array<string, array{0: string, 1: string}> input => expected */
    public static function corpus(): array
    {
        return [
            // ── Framework-internal keys that actually emit cache events ──
            'queue restart' => ['illuminate:queue:restart', 'illuminate:queue:restart'],
            'foundation down' => ['illuminate:foundation:down', 'illuminate:foundation:down'],
            'schedule interrupt' => ['illuminate:schedule:interrupt', 'illuminate:schedule:interrupt'],
            'flexible static' => ['illuminate:cache:flexible:created:dashboard-stats', 'illuminate:cache:flexible:created:dashboard-stats'],
            'flexible with id' => ['illuminate:cache:flexible:created:user:8891:feed', 'illuminate:cache:flexible:created:user:{int}:feed'],
            'exception class key' => ['illuminate:foundation:exceptions:App\\Exceptions\\PaymentFailed', 'illuminate:foundation:exceptions:App\\Exceptions\\PaymentFailed'],
            'schedule mutex' => ['framework/schedule-9c1185a5c5e9fc54612808977ee8f548b2258d31', 'framework/schedule-{hex}'],
            'throttle sha1' => ['d033e22ae348aeb5660fc2140aec35850c4da997', '{hex}'],
            'throttle timer' => ['d033e22ae348aeb5660fc2140aec35850c4da997:timer', '{hex}:timer'],
            'throttle md5' => ['098f6bcd4621d373cade4e832627b4f6', '{hex}'],
            'throttle email' => ['attempts:user@example.com', 'attempts:{email}'],
            'login pipe ip' => ['login|203.0.113.42', 'login|{int}.{int}.{int}.{int}'],

            // ── The REAL tenant's key families (measured 2026-07-22) ──
            'spatie static' => ['spatie.permission.cache', 'spatie.permission.cache'],
            'mode uuid7' => ['mode:019f6624-b8e7-7362-afde-f149c46ce640', 'mode:{uuid}'],
            'filament export' => ['filament-excel:exports:019f6613-bcf0-72f6-8073-c7fde0ebb2d0', 'filament-excel:exports:{uuid}'],
            'filament empty id' => ['filament-excel:exports:', 'filament-excel:exports:'],
            'livewire limiter' => ['livewire-rate-limiter:c772620bca44ae9fb6a02d3d0573c2393af133ac', 'livewire-rate-limiter:{hex}'],
            'idempotency response' => ['idempotency:16f38194-b403-4016-a8a1-0a43b1fc1034:response', 'idempotency:{uuid}:response'],
            'idempotency processing' => ['idempotency:16f38194-b403-4016-a8a1-0a43b1fc1034:processing', 'idempotency:{uuid}:processing'],
            // Bare base62 session tokens, the shape Laravel's own isValidId
            // defines (ctype_alnum, exactly 40). All three off a real customer
            // database, where they were 96.8% of the cache rollup.
            'session token' => ['Y0xrz025ILHGp8VX2yJLOYI8AOVt6OhmG6OS9L3j', '{session}'],
            'session token 2' => ['KywllO9sT7khredx3bfte0iFL35hakXGTcnK4OiE', '{session}'],
            'session token 3' => ['CSYt3JQOZJ5lK76YGgTCtWT5d3wd1kU6G8xpShg4', '{session}'],
            'prefixed session token' => ['laravel_session:KywllO9sT7khredx3bfte0iFL35hakXGTcnK4OiE', 'laravel_session:{session}'],

            // ── The session rule's guards ──
            // A sha1 is ALSO 40 alnum bytes. The hex rule runs first and must win.
            'sha1 is not a session' => ['9c1185a5c5e9fc54612808977ee8f548b2258d31', '{hex}'],
            'sha1 upper is not a session' => ['9C1185A5C5E9FC54612808977EE8F548B2258D31', '{hex}'],
            // 40 bytes but only two of the three character classes: declined,
            // exactly as it was before the rule existed.
            'forty no digit' => ['abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN', 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN'],
            'forty no upper' => ['abcdefghij0123456789abcdefghij0123456789', 'abcdefghij0123456789abcdefghij0123456789'],
            'forty no lower' => ['ABCDEFGHIJ0123456789ABCDEFGHIJ0123456789', 'ABCDEFGHIJ0123456789ABCDEFGHIJ0123456789'],
            // Off by one either way.
            'thirty nine' => ['KywllO9sT7khredx3bfte0iFL35hakXGTcnK4Oi', 'KywllO9sT7khredx3bfte0iFL35hakXGTcnK4Oi'],
            'forty one' => ['KywllO9sT7khredx3bfte0iFL35hakXGTcnK4OiEz', 'KywllO9sT7khredx3bfte0iFL35hakXGTcnK4OiEz'],

            // ── Common application shapes ──
            'int id' => ['user:8213:profile', 'user:{int}:profile'],
            'two ints' => ['cart:5521:items:34', 'cart:{int}:items:{int}'],
            'dash id' => ['order-1284401', 'order-{int}'],
            'underscore id' => ['product_id_88213', 'product_id_{int}'],
            'bare uuid' => ['550e8400-e29b-41d4-a716-446655440000', '{uuid}'],
            'prefixed uuid' => ['user:550e8400-e29b-41d4-a716-446655440000', 'user:{uuid}'],
            'ulid' => ['session:01HV9AJ0T4Q2S8B3ZDGKM5NPXW', 'session:{ulid}'],
            'date' => ['stats:2026-07-21', 'stats:{date}'],
            'datetime' => ['report:2026-07-21T14:30:00Z', 'report:{datetime}'],
            'datetime offset' => ['report:2026-07-21 14:30:00+02:00', 'report:{datetime}'],
            'email' => ['verify:leonce@example.com', 'verify:{email}'],
            'locale passthrough' => ['trans:en:messages', 'trans:en:messages'],
            'slug passthrough' => ['page:getting-started', 'page:getting-started'],

            // ── Edges the rule must NOT mangle ──
            'invalid date is ints' => ['bad-date:2026-13-45', 'bad-date:{int}-{int}-{int}'],
            'short hex kept' => ['color:28554f', 'color:28554f'],
            'mixed-case hex kept' => ['id:B0dc82CF12aa', 'id:B0dc82CF12aa'],
            'hex words eaten (known cost)' => ['deadbeef', '{hex}'],
            'v1 kept' => ['config:v1:flags', 'config:v1:flags'],
            'empty' => ['', ''],
            'single char' => ['a', 'a'],
            'metacharacters' => ['a[0-9]{1,99}b', 'a[{int}-{int}]{{int},{int}}b'],
            'unicode word kept' => ['café:menu', 'café:menu'],
            'emoji kept' => ['🔥:hot', '🔥:hot'],
        ];
    }

    #[DataProvider('corpus')]
    public function test_corpus(string $input, string $expected): void
    {
        $this->assertSame($expected, CacheKeyTemplate::template($input));
    }

    public function test_family_collapse_is_the_point(): void
    {
        // 1000 distinct keys per family → exactly one pattern per family.
        $patterns = [];
        for ($i = 0; $i < 1000; $i++) {
            $uuid = sprintf('%08x-%04x-%04x-%04x-%012x', $i, $i % 0xFFFF, $i % 0xFFFF, $i % 0xFFFF, $i);
            $patterns['mode'][CacheKeyTemplate::template("mode:{$uuid}")] = true;
            $patterns['user'][CacheKeyTemplate::template('user:'.(1000 + $i).':profile')] = true;
            $patterns['limiter'][CacheKeyTemplate::template('livewire-rate-limiter:'.str_pad(dechex($i), 40, 'a'))] = true;
        }

        foreach ($patterns as $family => $set) {
            $this->assertCount(1, $set, "family {$family} must collapse to ONE pattern");
        }
    }

    /**
     * Totality + output invariants over adversarial bytes. Deterministic seed:
     * a failure reproduces.
     */
    public function test_fuzz_invariants(): void
    {
        mt_srand(20260722);

        $check = function (string $input): void {
            $out = CacheKeyTemplate::template($input);
            $this->assertLessThanOrEqual(255, strlen($out), 'output must fit varchar(255) bytes');
            $this->assertTrue(
                mb_check_encoding($out, 'UTF-8'),
                'output must be valid UTF-8 for input '.bin2hex(substr($input, 0, 40)),
            );
        };

        for ($t = 0; $t < 3000; $t++) {
            $len = mt_rand(0, 64);
            $s = '';
            for ($k = 0; $k < $len; $k++) {
                $s .= chr(mt_rand(0, 255));
            }
            $check($s);
        }

        // Pathological shapes.
        $check(str_repeat('a', 100_000));
        $check(str_repeat('2026-07-21T14:30:00Z', 5_000));
        $check(str_repeat('550e8400-e29b-41d4-a716-446655440000', 3_000));
        $check(str_repeat('x@y.', 25_000));
        $check(str_repeat("\xC3", 50_000)); // truncated lead bytes
        $check(str_repeat(':', 100_000));
        $check(str_repeat('🔥', 20_000));
        $check("user:caf\xC3"); // Str::restrict chopping a multibyte tail
    }

    /**
     * The whole point of the rule, and the size of what it gives up. A session
     * id is Str::random(40) over 62 characters; requiring all three character
     * classes is the false-positive guard, and it costs the runs that happen to
     * draw no digit at all — under a tenth of a percent, and those merely stay
     * literal. Deterministic seed: a failure reproduces.
     */
    public function test_real_session_ids_collapse_to_one_group(): void
    {
        mt_srand(20260816);

        $alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
        $patterns = [];
        $collapsed = 0;
        $total = 20_000;

        for ($i = 0; $i < $total; $i++) {
            $id = '';
            for ($k = 0; $k < 40; $k++) {
                $id .= $alphabet[mt_rand(0, 61)];
            }

            $pattern = CacheKeyTemplate::template($id);
            if ($pattern === '{session}') {
                $collapsed++;
            }
            $patterns[$pattern] = true;
        }

        $this->assertGreaterThan(
            0.99 * $total,
            $collapsed,
            'the session rule must collapse essentially every real session id',
        );

        // Everything that did NOT collapse stayed literal — the rule never
        // rewrites a session id into some OTHER pattern.
        foreach (array_keys($patterns) as $pattern) {
            if ($pattern === '{session}') {
                continue;
            }
            $this->assertSame(40, strlen($pattern), "a declined id was rewritten: {$pattern}");
        }
    }

    public function test_idempotent(): void
    {
        foreach (self::corpus() as [$input]) {
            $once = CacheKeyTemplate::template($input);
            $this->assertSame($once, CacheKeyTemplate::template($once), "not idempotent for: {$input}");
        }
    }

    public function test_variable_free_keys_round_trip_byte_identical(): void
    {
        foreach ([
            'illuminate:queue:restart',
            'spatie.permission.cache',
            'config:features',
            'trans:en:messages',
            'a|b|c~d^e',
        ] as $key) {
            $this->assertSame($key, CacheKeyTemplate::template($key));
        }
    }
}
