<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Support\BufferDirectory;
use PHPUnit\Framework\TestCase;

/**
 * The rules for writing a .gitignore next to the agent's buffer. The command
 * that applies them is covered in AgentCommandBufferLockTest.
 */
final class BufferDirectoryTest extends TestCase
{
    private string $root;

    protected function setUp(): void
    {
        $this->root = sys_get_temp_dir().'/nightowl-buffer-dir-'.getmypid().'-'.uniqid();
        mkdir($this->root.'/app/storage/nightowl', 0755, true);
    }

    protected function tearDown(): void
    {
        exec('rm -rf '.escapeshellarg($this->root));
    }

    public function test_storage_nightowl_is_always_protected(): void
    {
        $this->assertTrue(BufferDirectory::shouldProtect(
            $this->root.'/app/storage/nightowl', false, $this->root.'/app/storage/nightowl', $this->root.'/app',
        ));
    }

    public function test_a_directory_beside_the_app_that_shares_its_name_prefix_is_outside_it(): void
    {
        mkdir($this->root.'/app-buffers', 0755, true);

        $this->assertFalse(BufferDirectory::shouldProtect(
            $this->root.'/app-buffers', true, $this->root.'/app/storage/nightowl', $this->root.'/app',
        ));
    }

    public function test_the_app_root_itself_is_never_protected(): void
    {
        $this->assertFalse(BufferDirectory::shouldProtect(
            $this->root.'/app', true, $this->root.'/app/storage/nightowl', $this->root.'/app',
        ));
    }

    public function test_the_file_ignores_everything_but_itself(): void
    {
        $dir = $this->root.'/app/storage/nightowl';

        $this->assertTrue(BufferDirectory::ensureGitignore($dir));
        $this->assertSame("*\n!.gitignore\n", file_get_contents($dir.'/.gitignore'));
    }

    public function test_a_symlinked_gitignore_is_left_alone(): void
    {
        $dir = $this->root.'/app/storage/nightowl';
        file_put_contents($this->root.'/shared-ignore', "custom\n");
        symlink($this->root.'/shared-ignore', $dir.'/.gitignore');

        $this->assertTrue(BufferDirectory::ensureGitignore($dir));
        $this->assertSame("custom\n", file_get_contents($this->root.'/shared-ignore'));
        $this->assertTrue(is_link($dir.'/.gitignore'));
    }

    public function test_a_missing_directory_is_not_created(): void
    {
        $this->assertFalse(BufferDirectory::ensureGitignore($this->root.'/nope'));
        $this->assertDirectoryDoesNotExist($this->root.'/nope');
    }
}
