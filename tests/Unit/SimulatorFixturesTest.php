<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Simulator\NightwatchSimulator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Drift detector for the simulator fixtures (now in the separate
 * nightowl/agent-simulator package). For each Nightwatch sensor it extracts every
 * wire-format key from the sensor's return-array literal and asserts the matching
 * fixture row carries it; a new laravel/nightwatch field fails this until the
 * fixture is re-captured. Kept in the agent suite (tests aren't shipped) because
 * the agent's own Integration tests drive the pipeline through these fixtures.
 */
class SimulatorFixturesTest extends TestCase
{
    private const SENSOR_DIR = __DIR__.'/../../vendor/laravel/nightwatch/src/Sensors';

    /** Fixtures live in the simulator package — resolve from the engine's own dir. */
    private static function fixtureDir(): string
    {
        return dirname((new \ReflectionClass(NightwatchSimulator::class))->getFileName()).'/fixtures';
    }

    /** @return array<string, array{string, string}> */
    public static function sensorMap(): array
    {
        return [
            'request' => ['RequestSensor.php', 'request'],
            'query' => ['QuerySensor.php', 'query'],
            'exception' => ['ExceptionSensor.php', 'exception'],
            'queued-job' => ['QueuedJobSensor.php', 'queued-job'],
            'job-attempt' => ['JobAttemptSensor.php', 'job-attempt'],
            'command' => ['CommandSensor.php', 'command'],
            'scheduled-task' => ['ScheduledTaskSensor.php', 'scheduled-task'],
            'cache-event' => ['CacheEventSensor.php', 'cache-event'],
            'mail' => ['MailSensor.php', 'mail'],
            'notification' => ['NotificationSensor.php', 'notification'],
            'outgoing-request' => ['OutgoingRequestSensor.php', 'outgoing-request'],
            'log' => ['LogSensor.php', 'log'],
            'user' => ['UserSensor.php', 'user'],
        ];
    }

    #[DataProvider('sensorMap')]
    public function test_fixture_has_every_field_the_sensor_emits(string $sensorFile, string $type): void
    {
        $sensorPath = self::SENSOR_DIR.'/'.$sensorFile;
        $fixturePath = self::fixtureDir().'/'.$type.'.jsonl';

        if (! is_file($sensorPath)) {
            $this->markTestSkipped("Sensor not found: {$sensorPath} (laravel/nightwatch may have moved it)");
        }
        $this->assertFileExists($fixturePath, "Missing fixture: {$type}.jsonl");

        $expected = $this->extractWireKeys($sensorPath, $type);
        $this->assertNotEmpty($expected, "Could not parse wire keys from {$sensorFile}");

        // Read fixture rows
        $rows = [];
        foreach (file($fixturePath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) as $line) {
            $rows[] = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
        }
        $this->assertNotEmpty($rows, "Fixture is empty: {$type}.jsonl");

        // Every fixture row must carry every key the sensor emits
        foreach ($rows as $i => $row) {
            $missing = array_diff($expected, array_keys($row));
            $this->assertSame(
                [],
                $missing,
                "Fixture {$type}.jsonl row {$i} missing keys emitted by {$sensorFile}: ".implode(', ', $missing)
                ."\nRe-capture the fixture from a recent laravel/nightwatch run."
            );
        }
    }

    /**
     * Pull the keys out of the sensor's wire-format array literal — the one
     * that contains `'v' => N` and `'t' => '<type>'`.
     */
    private function extractWireKeys(string $sensorPath, string $type): array
    {
        $src = file_get_contents($sensorPath);

        // Find `'t' => '<type>'` (e.g. `'t' => 'queued-job'`) — anchors us in the
        // wire-payload literal even when a sensor builds multiple intermediate arrays.
        $needle = "'t' => '{$type}'";
        $tPos = strpos($src, $needle);
        if ($tPos === false) {
            return [];
        }

        // Walk backward to the `[` opening this array literal (skip nested brackets).
        $depth = 0;
        $start = -1;
        for ($i = $tPos; $i >= 0; $i--) {
            $ch = $src[$i];
            if ($ch === ']') {
                $depth++;
            } elseif ($ch === '[') {
                if ($depth === 0) {
                    $start = $i;
                    break;
                }
                $depth--;
            }
        }
        if ($start < 0) {
            return [];
        }

        // Walk forward to the matching `]`.
        $depth = 0;
        $end = -1;
        $len = strlen($src);
        for ($i = $start; $i < $len; $i++) {
            $ch = $src[$i];
            if ($ch === '[') {
                $depth++;
            } elseif ($ch === ']') {
                $depth--;
                if ($depth === 0) {
                    $end = $i;
                    break;
                }
            }
        }
        if ($end < 0) {
            return [];
        }

        $body = substr($src, $start, $end - $start + 1);

        // Top-level keys only — skip keys inside nested arrays.
        $keys = [];
        $depth = 0;
        $bodyLen = strlen($body);
        for ($i = 0; $i < $bodyLen; $i++) {
            $ch = $body[$i];
            if ($ch === '[') {
                $depth++;
            } elseif ($ch === ']') {
                $depth--;
            } elseif ($depth === 1 && $ch === "'") {
                // Match `'<key>' =>` — keys are simple identifiers (letters/digits/_/-).
                if (preg_match("/\\G'([A-Za-z0-9_\\-]+)'\\s*=>/", $body, $m, 0, $i)) {
                    $keys[] = $m[1];
                    $i += strlen($m[0]) - 1;
                }
            }
        }

        return array_values(array_unique($keys));
    }
}
