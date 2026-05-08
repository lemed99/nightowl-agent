<?php

namespace NightOwl\Commands;

use Illuminate\Console\Command;

class InstallCommand extends Command
{
    protected $signature = 'nightowl:install';

    protected $description = 'Install NightOwl: publish config, run migrations';

    public function handle(): int
    {
        $this->info('Installing NightOwl...');

        // 1. Publish config
        $this->callSilent('vendor:publish', [
            '--tag' => 'nightowl-config',
        ]);
        $this->line('  Published config/nightowl.php');

        // 2. Run migrations on the nightowl connection
        $this->call('migrate', [
            '--database' => 'nightowl',
        ]);
        $this->line('  Ran migrations');

        $this->newLine();
        $this->info('NightOwl installed successfully!');
        $this->newLine();
        $this->line('Next step:');
        $this->line('  - Start the agent: <comment>php artisan nightowl:agent</comment>');

        return self::SUCCESS;
    }
}
