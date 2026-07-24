<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\InstalledVersionReader;
use NightOwl\Tests\Support\InstalledPhpFixture;
use PHPUnit\Framework\TestCase;

class InstalledVersionReaderTest extends TestCase
{
    private string $fixturePath;

    protected function setUp(): void
    {
        $this->fixturePath = sys_get_temp_dir().'/nightowl-installed-'.getmypid().'-'.uniqid().'.php';
    }

    protected function tearDown(): void
    {
        @unlink($this->fixturePath);
    }

    private function writeFixture(string $contents): InstalledVersionReader
    {
        file_put_contents($this->fixturePath, $contents);

        return new InstalledVersionReader($this->fixturePath);
    }

    /** The canonical Composer shape — shared with the watcher + System suites. */
    private function realisticFixture(string $prettyVersion, ?string $reference): string
    {
        return InstalledPhpFixture::contents($prettyVersion, $reference);
    }

    public function test_reads_tagged_version_with_reference(): void
    {
        $reader = $this->writeFixture($this->realisticFixture('v1.4.1', 'abc123def456abc123def456abc123def456abc1'));

        $this->assertSame('v1.4.1#abc123def456abc123def456abc123def456abc1', $reader->read());
    }

    public function test_reads_dev_branch_with_reference(): void
    {
        $reader = $this->writeFixture($this->realisticFixture('dev-main', 'ce1fb235bef958458c271c8e3e67655bfdc30ea3'));

        $this->assertSame('dev-main#ce1fb235bef958458c271c8e3e67655bfdc30ea3', $reader->read());
    }

    public function test_missing_reference_yields_version_with_empty_ref(): void
    {
        $reader = $this->writeFixture($this->realisticFixture('v1.4.1', null));

        $this->assertSame('v1.4.1#', $reader->read());
    }

    public function test_root_block_decoy_alone_does_not_match(): void
    {
        // The ONLY occurrence of nightowl/agent is the root block's
        // 'name' => 'nightowl/agent' line — key followed by a comma, not =>.
        $reader = $this->writeFixture(<<<'PHP'
<?php return array(
    'root' => array(
        'name' => 'nightowl/agent',
        'pretty_version' => 'dev-root',
        'reference' => 'rootref0000000000000000000000000000000000',
    ),
    'versions' => array(
        'laravel/framework' => array(
            'pretty_version' => 'v12.0.0',
            'reference' => 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'aliases' => array(),
        ),
    ),
);
PHP);

        $this->assertNull($reader->read());
    }

    public function test_simulator_package_does_not_prefix_collide(): void
    {
        // nightowl/agent absent, but nightowl/agent-simulator present — the
        // reader must not return the simulator's version.
        $reader = $this->writeFixture(<<<'PHP'
<?php return array(
    'versions' => array(
        'nightowl/agent-simulator' => array(
            'pretty_version' => 'v0.9.9',
            'reference' => 'simref000000000000000000000000000000000000',
            'aliases' => array(),
        ),
    ),
);
PHP);

        $this->assertNull($reader->read());
    }

    public function test_garbage_file_returns_null(): void
    {
        $reader = $this->writeFixture('this is not php at all {{{');

        $this->assertNull($reader->read());
    }

    public function test_truncated_block_without_pretty_version_returns_null(): void
    {
        // Simulates a partial write cut off before pretty_version.
        $reader = $this->writeFixture("<?php return array(\n    'versions' => array(\n        'nightowl/agent' => array(\n");

        $this->assertNull($reader->read());
    }

    public function test_missing_file_returns_null(): void
    {
        $reader = new InstalledVersionReader($this->fixturePath.'.does-not-exist');

        $this->assertNull($reader->read());
    }

    public function test_empty_path_returns_null(): void
    {
        $reader = new InstalledVersionReader('');

        $this->assertNull($reader->read());
    }

    public function test_default_path_finds_the_real_installed_php(): void
    {
        // In this test process Composer is loaded, so the reflection-based
        // path must resolve to a real file that this reader can parse.
        $path = InstalledVersionReader::defaultPath();

        $this->assertNotNull($path);
        $this->assertFileExists($path);
        $this->assertStringEndsWith('installed.php', $path);
        $this->assertNotNull((new InstalledVersionReader($path))->read());
    }
}
