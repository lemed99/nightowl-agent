<?php

namespace NightOwl\Tests\Support;

/**
 * The ONE canonical Composer installed.php fixture, shared by every suite that
 * exercises InstalledVersionReader (unit reader tests, unit watcher tests, the
 * auto-restart System test). Mirrors the real Composer-generated shape: root
 * block first — whose 'name' key is a decoy occurrence of nightowl/agent — then
 * the versions map with neighbors, including nightowl/agent-simulator, a
 * prefix-collision trap. Keep all suites on this builder: three hand-rolled
 * shapes would let a reader/regex change pass one suite against a fixture the
 * others don't test.
 */
final class InstalledPhpFixture
{
    /**
     * @param  ?string  $reference  null omits the 'reference' line entirely
     *                              (path-repo installs have none)
     */
    public static function contents(string $prettyVersion, ?string $reference): string
    {
        $refLine = $reference === null ? '' : "            'reference' => '{$reference}',\n";

        return <<<PHP
<?php return array(
    'root' => array(
        'name' => 'nightowl/agent',
        'pretty_version' => 'dev-root',
        'reference' => 'rootref0000000000000000000000000000000000',
        'dev' => true,
    ),
    'versions' => array(
        'laravel/framework' => array(
            'pretty_version' => 'v12.0.0',
            'version' => '12.0.0.0',
            'reference' => 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'type' => 'library',
            'install_path' => __DIR__ . '/../laravel/framework',
            'aliases' => array(),
            'dev_requirement' => false,
        ),
        'nightowl/agent' => array(
            'pretty_version' => '{$prettyVersion}',
            'version' => '{$prettyVersion}',
{$refLine}            'type' => 'library',
            'install_path' => __DIR__ . '/../../',
            'aliases' => array(),
            'dev_requirement' => false,
        ),
        'nightowl/agent-simulator' => array(
            'pretty_version' => 'v0.9.9',
            'version' => '0.9.9.0',
            'reference' => 'simref000000000000000000000000000000000000',
            'type' => 'library',
            'install_path' => __DIR__ . '/../nightowl/agent-simulator',
            'aliases' => array(),
            'dev_requirement' => true,
        ),
    ),
);
PHP;
    }

    public static function write(string $path, string $prettyVersion, ?string $reference): void
    {
        file_put_contents($path, self::contents($prettyVersion, $reference));
    }
}
