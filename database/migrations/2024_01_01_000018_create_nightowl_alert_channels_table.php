<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'nightowl';

    public function up(): void
    {
        Schema::connection($this->connection)->create('nightowl_alert_channels', function (Blueprint $table) {
            $table->id();
            $table->string('name', 255);
            $table->string('type', 50);
            $table->text('config');
            $table->boolean('enabled')->default(true);
            $table->timestamps();

            $table->index('type');
            $table->index('enabled');
        });

        // Migrate existing Slack webhook setting to alert channels
        $conn = DB::connection($this->connection);

        $slackUrl = $conn->table('nightowl_settings')
            ->where('key', 'slack_webhook_url')
            ->value('value');

        if ($slackUrl) {
            $conn->table('nightowl_alert_channels')->insert([
                'name' => 'Slack',
                'type' => 'slack',
                'config' => json_encode(['webhook_url' => $slackUrl]),
                'enabled' => true,
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        }

        $conn->table('nightowl_settings')
            ->where('key', 'slack_webhook_url')
            ->delete();

        // Rename last_slack_notify_at to last_notify_at
        $conn->table('nightowl_settings')
            ->where('key', 'last_slack_notify_at')
            ->update(['key' => 'last_notify_at']);
    }

    public function down(): void
    {
        // Migrate Slack channel back to settings
        $conn = DB::connection($this->connection);

        $slackChannel = $conn->table('nightowl_alert_channels')
            ->where('type', 'slack')
            ->first();

        if ($slackChannel) {
            $config = json_decode($slackChannel->config, true);
            if (! empty($config['webhook_url'])) {
                $conn->table('nightowl_settings')->updateOrInsert(
                    ['key' => 'slack_webhook_url'],
                    ['value' => $config['webhook_url'], 'updated_at' => now()->toDateTimeString()]
                );
            }
        }

        $conn->table('nightowl_settings')
            ->where('key', 'last_notify_at')
            ->update(['key' => 'last_slack_notify_at']);

        Schema::connection($this->connection)->dropIfExists('nightowl_alert_channels');
    }
};
