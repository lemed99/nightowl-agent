<?php

namespace NightOwl\Tests\Integration;

use NightOwl\Agent\AlertNotifier;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * The auto-reopen cooldown must be a pure function of elapsed real time — the
 * customer app's timezone may not change its verdict.
 *
 * Both instants the cooldown compares are UTC: the resolve activity's
 * created_at (and the issues.updated_at fallback) are written with gmdate()
 * (RecordWriter::logReopenActivity), and `now` is time(), a true UTC epoch. But
 * Laravel calls date_default_timezone_set(app.timezone) on boot, and the drain
 * worker inherits it, so reading those naive strings with the process default
 * skews elapsed by the app's UTC offset. West of UTC elapsed goes NEGATIVE and
 * a recurrence stays silent for the whole offset even at the shipped default
 * reopen_cooldown_hours=0; east of UTC the alert fires that many hours early.
 *
 * Real Postgres because the fixture is the point: real `timestamp` columns,
 * gmdate-written values, and the IS NOT DISTINCT FROM environment match the
 * snapshot query actually runs. Same env vars as RecordWriterTest.
 */
class AlertNotifierReopenCooldownTest extends TestCase
{
    /** The composite key snapshotExistingIssues buckets by. */
    private const KEY = 'reopen_cooldown_probe|production';

    private const FINGERPRINT = 'reopen_cooldown_probe';

    private static ?PDO $pdo = null;

    private string $originalTimezone = 'UTC';

    public static function setUpBeforeClass(): void
    {
        $host = getenv('NIGHTOWL_TEST_DB_HOST') ?: '127.0.0.1';
        $port = (int) (getenv('NIGHTOWL_TEST_DB_PORT') ?: 5432);
        $database = getenv('NIGHTOWL_TEST_DB_DATABASE') ?: 'nightowl_test';
        $username = getenv('NIGHTOWL_TEST_DB_USERNAME') ?: 'nightowl_test';
        $password = getenv('NIGHTOWL_TEST_DB_PASSWORD') ?: 'test123';

        try {
            self::$pdo = new PDO(
                sprintf('pgsql:host=%s;port=%d;dbname=%s', $host, $port, $database),
                $username,
                $password,
            );
            self::$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (\Exception) {
            self::$pdo = null;
        }

        if (self::$pdo) {
            MigrationRunner::migrate($host, $port, $database, $username, $password);
        }
    }

    protected function setUp(): void
    {
        // Captured BEFORE the skip: these tests move process-global state, and
        // tearDown must be able to put it back whichever way setUp exits.
        $this->originalTimezone = date_default_timezone_get();

        if (self::$pdo === null) {
            $this->markTestSkipped('PostgreSQL not available. Set NIGHTOWL_TEST_DB_* env vars.');
        }
    }

    protected function tearDown(): void
    {
        // Process-global state — every other test class inherits whatever we
        // leave behind.
        date_default_timezone_set($this->originalTimezone);
    }

    public static function tearDownAfterClass(): void
    {
        self::$pdo = null;
    }

    // ─── Fixture ─────────────────────────────────────────────────────

    /**
     * A resolved issue whose resolve landed $secondsAgo ago, stamped exactly the
     * way the drain stamps it: gmdate(), so the DB value is UTC wall time with
     * no offset regardless of the timezone this test is running under.
     */
    private function seedResolvedIssue(int $secondsAgo): void
    {
        $resolvedAt = gmdate('Y-m-d H:i:s', time() - $secondsAgo);

        self::$pdo->exec('TRUNCATE nightowl_issues CASCADE');

        $issue = self::$pdo->prepare(
            "INSERT INTO nightowl_issues
                (type, status, group_hash, environment, exception_class,
                 first_seen_at, last_seen_at, occurrences_count, users_count,
                 created_at, updated_at)
             VALUES ('exception', 'resolved', ?, 'production', 'RuntimeException',
                 ?, ?, 1, 0, ?, ?)
             RETURNING id"
        );
        $issue->execute([self::FINGERPRINT, $resolvedAt, $resolvedAt, $resolvedAt, $resolvedAt]);
        $issueId = (int) $issue->fetchColumn();

        $activity = self::$pdo->prepare(
            "INSERT INTO nightowl_issue_activity
                (issue_id, actor_type, action, old_value, new_value, created_at)
             VALUES (?, 'agent', 'status_changed', 'open', 'resolved', ?)"
        );
        $activity->execute([$issueId, $resolvedAt]);
    }

    /**
     * The same resolved issue with NO status_changed→resolved activity row, and
     * updated_at where the drain leaves it after a recurrence: "moments ago".
     * This is what a resolve performed outside the dashboard looks like (direct
     * SQL, a row predating the activity table), and what every resolved-but-
     * recurring issue's updated_at reads as, because the issue upsert rewrites
     * updated_at on every batch whether or not it flips the status.
     */
    private function seedResolvedIssueWithoutActivity(): void
    {
        $justNow = gmdate('Y-m-d H:i:s');

        self::$pdo->exec('TRUNCATE nightowl_issues CASCADE');

        $issue = self::$pdo->prepare(
            "INSERT INTO nightowl_issues
                (type, status, group_hash, environment, exception_class,
                 first_seen_at, last_seen_at, occurrences_count, users_count,
                 created_at, updated_at)
             VALUES ('exception', 'resolved', ?, 'production', 'RuntimeException',
                 ?, ?, 1, 0, ?, ?)"
        );
        $issue->execute([self::FINGERPRINT, $justNow, $justNow, $justNow, $justNow]);
    }

    /** A resolve stamped in the agent's future — clock skew between the two hosts. */
    private function seedResolvedIssueInTheFuture(int $secondsAhead): void
    {
        $resolvedAt = gmdate('Y-m-d H:i:s', time() + $secondsAhead);

        self::$pdo->exec('TRUNCATE nightowl_issues CASCADE');

        $issue = self::$pdo->prepare(
            "INSERT INTO nightowl_issues
                (type, status, group_hash, environment, exception_class,
                 first_seen_at, last_seen_at, occurrences_count, users_count,
                 created_at, updated_at)
             VALUES ('exception', 'resolved', ?, 'production', 'RuntimeException',
                 ?, ?, 1, 0, ?, ?)
             RETURNING id"
        );
        $issue->execute([self::FINGERPRINT, $resolvedAt, $resolvedAt, $resolvedAt, $resolvedAt]);
        $issueId = (int) $issue->fetchColumn();

        $activity = self::$pdo->prepare(
            "INSERT INTO nightowl_issue_activity
                (issue_id, actor_type, action, old_value, new_value, created_at)
             VALUES (?, 'user', 'status_changed', 'open', 'resolved', ?)"
        );
        $activity->execute([$issueId, $resolvedAt]);
    }

    /** 'reopen' | 'suppressed' — which bucket the fingerprint lands in. */
    private function verdict(int $cooldownHours): string
    {
        $notifier = new AlertNotifier(reopenCooldownHours: $cooldownHours);

        $snapshot = $notifier->snapshotExistingIssues(self::$pdo, [
            self::KEY => ['fingerprint' => self::FINGERPRINT, 'environment' => 'production'],
        ], 'exception');

        if (isset($snapshot['reopen'][self::KEY])) {
            return 'reopen';
        }

        return in_array(self::KEY, $snapshot['existing'], true) ? 'suppressed' : 'not-found';
    }

    // ─── Tests ───────────────────────────────────────────────────────

    /**
     * The shipped default (0 hours) means "always reopen, Sentry-style". West of
     * UTC the skew makes elapsed negative, so the recurrence was silently
     * swallowed for the length of the offset — up to 8 hours on US Pacific.
     */
    public function test_west_of_utc_app_timezone_reopens_at_the_default_zero_cooldown(): void
    {
        date_default_timezone_set('America/Los_Angeles');
        $this->seedResolvedIssue(60);

        $this->assertSame(
            'reopen',
            $this->verdict(0),
            'a recurrence one minute after a resolve must reopen at cooldown 0, whatever app.timezone is',
        );
    }

    /**
     * The mirror image: east of UTC the skew ADDS the offset, so a resolve well
     * inside the cooldown reads as long past it and re-alerts early.
     */
    public function test_east_of_utc_app_timezone_stays_silent_inside_the_cooldown(): void
    {
        date_default_timezone_set('Asia/Tokyo');
        $this->seedResolvedIssue(3600);

        $this->assertSame(
            'suppressed',
            $this->verdict(4),
            'a resolve 1h old is inside a 4h cooldown — +09:00 must not turn it into 10h',
        );
    }

    /**
     * With no resolve activity to read, the cooldown has no instant to measure
     * from and must not invent one. It used to COALESCE onto
     * nightowl_issues.updated_at — which the drain's issue upsert rewrites on
     * every recurrence, so the fallback advanced by exactly the interval it was
     * being compared against. Under any non-zero cooldown the issue could then
     * never reopen and never alerted again, permanently and silently.
     */
    public function test_reopens_when_no_resolve_activity_exists_whatever_the_cooldown(): void
    {
        foreach ([0, 4, 720] as $cooldownHours) {
            $this->seedResolvedIssueWithoutActivity();

            $this->assertSame(
                'reopen',
                $this->verdict($cooldownHours),
                "no resolve instant is known — a {$cooldownHours}h cooldown has nothing to measure and must not suppress",
            );
        }
    }

    /**
     * The resolve is stamped by the dashboard's host, `now` by the agent's own —
     * NTP skew can put the resolve in the agent's future. A negative elapsed used
     * to fail even `>= 0`, swallowing the recurrence at the shipped default
     * cooldown for the length of the skew.
     */
    public function test_a_future_dated_resolve_still_reopens_at_the_default_cooldown(): void
    {
        $this->seedResolvedIssueInTheFuture(600);

        $this->assertSame(
            'reopen',
            $this->verdict(0),
            'cooldown 0 means always reopen — clock skew must not turn it into a silent window',
        );
    }

    /**
     * The invariant behind both, stated once: same fixture, same cooldown, same
     * verdict in every zone. UTC is the control — it is the only one the old
     * code got right.
     */
    public function test_cooldown_verdict_is_independent_of_the_app_timezone(): void
    {
        $cases = [
            2 * 3600 => 'suppressed', // inside a 4h cooldown
            6 * 3600 => 'reopen',     // past it
        ];

        foreach (['UTC', 'America/Los_Angeles', 'Asia/Tokyo', 'Australia/Sydney'] as $timezone) {
            date_default_timezone_set($timezone);

            foreach ($cases as $secondsAgo => $expected) {
                $this->seedResolvedIssue($secondsAgo);

                $this->assertSame(
                    $expected,
                    $this->verdict(4),
                    "resolved {$secondsAgo}s ago under {$timezone}",
                );
            }
        }
    }
}
