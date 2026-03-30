<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\Redactor;
use PHPUnit\Framework\TestCase;

class RedactorTest extends TestCase
{
    public function testDisabledRedactorReturnsUnmodified(): void
    {
        $redactor = new Redactor(keys: ['password'], enabled: false);

        $records = [['password' => 'secret', 'name' => 'test']];
        [$result, $modified] = $redactor->redact($records);

        $this->assertFalse($modified);
        $this->assertSame('secret', $result[0]['password']);
    }

    public function testEmptyKeysReturnsUnmodified(): void
    {
        $redactor = new Redactor(keys: [], enabled: true);

        $records = [['password' => 'secret']];
        [$result, $modified] = $redactor->redact($records);

        $this->assertFalse($modified);
        $this->assertSame('secret', $result[0]['password']);
    }

    public function testRedactsMatchingKey(): void
    {
        $redactor = new Redactor(keys: ['password'], enabled: true);

        $records = [['password' => 'secret123', 'name' => 'test']];
        [$result, $modified] = $redactor->redact($records);

        $this->assertTrue($modified);
        $this->assertSame('[REDACTED]', $result[0]['password']);
        $this->assertSame('test', $result[0]['name']);
    }

    public function testCaseInsensitiveMatching(): void
    {
        $redactor = new Redactor(keys: ['password'], enabled: true);

        $records = [['PASSWORD' => 'secret', 'Password' => 'secret2']];
        [$result, $modified] = $redactor->redact($records);

        $this->assertTrue($modified);
        $this->assertSame('[REDACTED]', $result[0]['PASSWORD']);
        $this->assertSame('[REDACTED]', $result[0]['Password']);
    }

    public function testRedactsNestedKeys(): void
    {
        $redactor = new Redactor(keys: ['token', 'secret'], enabled: true);

        $records = [
            [
                'data' => [
                    'user' => [
                        'name' => 'John',
                        'token' => 'abc123',
                        'prefs' => [
                            'secret' => 'hidden',
                            'theme' => 'dark',
                        ],
                    ],
                ],
            ],
        ];

        [$result, $modified] = $redactor->redact($records);

        $this->assertTrue($modified);
        $this->assertSame('John', $result[0]['data']['user']['name']);
        $this->assertSame('[REDACTED]', $result[0]['data']['user']['token']);
        $this->assertSame('[REDACTED]', $result[0]['data']['user']['prefs']['secret']);
        $this->assertSame('dark', $result[0]['data']['user']['prefs']['theme']);
    }

    public function testMultipleRedactionKeys(): void
    {
        $redactor = new Redactor(
            keys: ['password', 'ssn', 'credit_card', 'api_key'],
            enabled: true,
        );

        $records = [[
            'password' => 'pass',
            'ssn' => '123-45-6789',
            'credit_card' => '4111-1111-1111-1111',
            'api_key' => 'sk_live_xxx',
            'name' => 'John Doe',
            'email' => 'john@example.com',
        ]];

        [$result, $modified] = $redactor->redact($records);

        $this->assertTrue($modified);
        $this->assertSame('[REDACTED]', $result[0]['password']);
        $this->assertSame('[REDACTED]', $result[0]['ssn']);
        $this->assertSame('[REDACTED]', $result[0]['credit_card']);
        $this->assertSame('[REDACTED]', $result[0]['api_key']);
        $this->assertSame('John Doe', $result[0]['name']);
        $this->assertSame('john@example.com', $result[0]['email']);
    }

    public function testRedactsMultipleRecords(): void
    {
        $redactor = new Redactor(keys: ['password'], enabled: true);

        $records = [
            ['password' => 'pass1', 'name' => 'a'],
            ['password' => 'pass2', 'name' => 'b'],
            ['name' => 'c'], // no password key
        ];

        [$result, $modified] = $redactor->redact($records);

        $this->assertTrue($modified);
        $this->assertSame('[REDACTED]', $result[0]['password']);
        $this->assertSame('[REDACTED]', $result[1]['password']);
        $this->assertSame('c', $result[2]['name']);
    }

    public function testDoesNotModifyIntegerKeys(): void
    {
        $redactor = new Redactor(keys: ['password'], enabled: true);

        // Integer keys should not be matched
        $records = [['value1', 'value2', 'password' => 'secret']];
        [$result, $modified] = $redactor->redact($records);

        $this->assertTrue($modified);
        $this->assertSame('value1', $result[0][0]);
        $this->assertSame('value2', $result[0][1]);
        $this->assertSame('[REDACTED]', $result[0]['password']);
    }

    public function testEmptyRecordsArray(): void
    {
        $redactor = new Redactor(keys: ['password'], enabled: true);

        [$result, $modified] = $redactor->redact([]);

        $this->assertFalse($modified);
        $this->assertSame([], $result);
    }

    public function testNonStringValueIsRedacted(): void
    {
        $redactor = new Redactor(keys: ['secret'], enabled: true);

        $records = [['secret' => 42]];
        [$result, $modified] = $redactor->redact($records);

        $this->assertTrue($modified);
        $this->assertSame('[REDACTED]', $result[0]['secret']);
    }

    public function testArrayValueWithMatchingKeyIsRecursed(): void
    {
        $redactor = new Redactor(keys: ['secret'], enabled: true);

        // When key matches but value is an array, the walk recurses into it
        // rather than replacing the whole array with [REDACTED]
        $records = [['data' => ['secret' => 'hidden', 'public' => 'visible']]];
        [$result, $modified] = $redactor->redact($records);

        $this->assertTrue($modified);
        $this->assertSame('[REDACTED]', $result[0]['data']['secret']);
        $this->assertSame('visible', $result[0]['data']['public']);
    }
}
