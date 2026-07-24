<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\VersionDriftWatcher;
use NightOwl\Support\InstalledVersionReader;
use NightOwl\Tests\Support\InstalledPhpFixture;
use PHPUnit\Framework\TestCase;

class VersionDriftWatcherTest extends TestCase
{
    private string $fixturePath;

    protected function setUp(): void
    {
        $this->fixturePath = sys_get_temp_dir().'/nightowl-drift-'.getmypid().'-'.uniqid().'-installed.php';
    }

    protected function tearDown(): void
    {
        @unlink($this->fixturePath);
    }

    private function writeVersion(string $prettyVersion, string $reference): void
    {
        InstalledPhpFixture::write($this->fixturePath, $prettyVersion, $reference);
    }

    private function makeWatcher(): VersionDriftWatcher
    {
        return new VersionDriftWatcher(new InstalledVersionReader($this->fixturePath), pollSeconds: 300.0);
    }

    public function test_unchanged_version_never_warns(): void
    {
        $this->writeVersion('v1.0.0', 'aaa');
        $watcher = $this->makeWatcher();

        for ($i = 0; $i < 5; $i++) {
            $this->assertNull($watcher->check());
        }

        $this->assertFalse($watcher->hasDrifted());
    }

    public function test_a_new_version_warns_after_the_confirming_read(): void
    {
        $this->writeVersion('v1.0.0', 'aaa');
        $watcher = $this->makeWatcher();

        $this->writeVersion('v1.1.0', 'bbb');

        $this->assertNull($watcher->check(), 'first sighting must only arm the candidate');

        $warning = $watcher->check();
        $this->assertNotNull($warning);
        $this->assertStringContainsString('v1.0.0#aaa', $warning);
        $this->assertStringContainsString('v1.1.0#bbb', $warning);
        $this->assertStringContainsString('Restart the agent', $warning);
        $this->assertTrue($watcher->hasDrifted());
        $this->assertSame('v1.1.0#bbb', $watcher->driftedTo());
    }

    public function test_it_warns_once_per_version_not_once_per_tick(): void
    {
        // The daemon may run for weeks after an update it will not act on;
        // repeating the warning every poll would bury its own log.
        $this->writeVersion('v1.0.0', 'aaa');
        $watcher = $this->makeWatcher();

        $this->writeVersion('v1.1.0', 'bbb');
        $watcher->check();
        $this->assertNotNull($watcher->check());

        for ($i = 0; $i < 5; $i++) {
            $this->assertNull($watcher->check(), 'the same drift must not warn again');
        }
    }

    public function test_a_further_update_warns_again(): void
    {
        $this->writeVersion('v1.0.0', 'aaa');
        $watcher = $this->makeWatcher();

        $this->writeVersion('v1.1.0', 'bbb');
        $watcher->check();
        $this->assertNotNull($watcher->check());

        $this->writeVersion('v1.2.0', 'ccc');
        $this->assertNull($watcher->check());
        $warning = $watcher->check();
        $this->assertNotNull($warning);
        $this->assertStringContainsString('v1.2.0#ccc', $warning);
    }

    public function test_a_half_written_vendor_tree_never_warns(): void
    {
        // composer rewrites installed.php non-atomically; a warning telling
        // someone to restart mid-update would be actively harmful.
        $this->writeVersion('v1.0.0', 'aaa');
        $watcher = $this->makeWatcher();

        $this->writeVersion('v1.1.0', 'bbb');
        $this->assertNull($watcher->check()); // candidate armed

        file_put_contents($this->fixturePath, 'garbage {{{');
        $this->assertNull($watcher->check(), 'unreadable → skip and clear the candidate');

        $this->writeVersion('v1.1.0', 'bbb');
        $this->assertNull($watcher->check(), 'debounce restarts from scratch');
        $this->assertNotNull($watcher->check());
    }

    public function test_a_reverted_update_clears_the_candidate(): void
    {
        $this->writeVersion('v1.0.0', 'aaa');
        $watcher = $this->makeWatcher();

        $this->writeVersion('v1.1.0', 'bbb');
        $this->assertNull($watcher->check()); // armed

        $this->writeVersion('v1.0.0', 'aaa'); // rolled back
        $this->assertNull($watcher->check());
        $this->assertNull($watcher->check());
        $this->assertFalse($watcher->hasDrifted());
    }

    public function test_an_unreadable_baseline_disables_the_watcher(): void
    {
        // No file at construction: with nothing to compare against, every
        // later read would look like a change.
        $watcher = $this->makeWatcher();

        $this->writeVersion('v9.9.9', 'zzz');

        for ($i = 0; $i < 3; $i++) {
            $this->assertNull($watcher->check());
        }

        $this->assertNull($watcher->runningVersion());
    }

    public function test_a_reference_only_change_is_drift(): void
    {
        // Same tag re-pushed, or dev-main moved: pretty_version identical,
        // reference different — still new code on disk.
        $this->writeVersion('dev-main', 'aaa');
        $watcher = $this->makeWatcher();

        $this->writeVersion('dev-main', 'bbb');

        $this->assertNull($watcher->check());
        $this->assertNotNull($watcher->check());
    }
}
