<?php

namespace NightOwl\Tests\Unit;

use NightOwl\Agent\MetricsCollector;
use PHPUnit\Framework\TestCase;

class MetricsCollectorTest extends TestCase
{
    private MetricsCollector $collector;

    protected function setUp(): void
    {
        $this->collector = new MetricsCollector(
            maxPendingRows: 100_000,
            maxBufferMemory: 256 * 1024 * 1024,
        );
    }

    // --- Ring buffer & tick tests ---

    public function testInitialStatusIsHealthy(): void
    {
        $this->assertSame('healthy', $this->collector->getStatus());
    }

    // --- agent_version ---

    /**
     * The version was a hardcoded '1.0.0' from the initial commit through v1.2.14,
     * so every health report ever sent misidentified the agent. It must now come
     * from Composer, which cannot drift.
     */
    public function testAgentVersionIsResolvedFromComposerNotHardcoded(): void
    {
        $version = MetricsCollector::agentVersion();

        $this->assertNotSame('1.0.0', $version, 'agent_version must not be the old hardcoded fiction');
        $this->assertNotSame('', $version);
        // Running from the package repo itself, Composer reports the root package
        // as a branch install and we pin it to the commit.
        $this->assertMatchesRegularExpression('/^(dev-|v?\d+\.|unknown)/', $version);
    }

    /**
     * Cross-repo contract with nightowl-api: POST /agent/health validates
     * `agent_version` as max:16 into a varchar(16), and an over-long value 422s the
     * WHOLE report. In this repo the value is 'dev-main@<7-char ref>' — exactly 16 —
     * so this guards a real boundary, not a theoretical one.
     */
    public function testAgentVersionFitsThePlatformWireLimit(): void
    {
        $this->assertLessThanOrEqual(16, mb_strlen(MetricsCollector::agentVersion(), 'UTF-8'));
    }

    /**
     * Cross-repo contract with nightowl-api: diagnoses.*.message and
     * .recommendation are validated max:1024, and an over-long one 422s the whole
     * health report — losing every diagnosis to say one of them too verbosely. The
     * 22xxx/23xxx recommendation is long and interpolates a table name, so it is
     * the realistic candidate to drift over.
     */
    public function testDrainWriteAdviceFitsThePlatformFieldCap(): void
    {
        $advice = new \ReflectionMethod(MetricsCollector::class, 'drainWriteAdvice');
        $longestTable = 'nightowl_outgoing_request_hourly_rollups';

        foreach (['22001', '22008', '23502', '42P01', '42501', '28P01', '28000', '3D000', '53100', '53300', '25006', 'XX000', ''] as $sqlstate) {
            [$message, $recommendation] = $advice->invoke($this->collector, $sqlstate, $longestTable);

            $this->assertLessThanOrEqual(1024, strlen($message), "message for SQLSTATE {$sqlstate}");
            $this->assertLessThanOrEqual(1024, strlen($recommendation), "recommendation for SQLSTATE {$sqlstate}");
        }
    }

    /**
     * The old text told operators to "check the agent log for the offending row".
     * Postgres names neither the column nor the row for a rejected COPY, so it was
     * never there — it sent a paying customer to an empty log mid-outage.
     */
    public function testRowRejectionAdviceDoesNotSendOperatorsToTheLog(): void
    {
        $advice = new \ReflectionMethod(MetricsCollector::class, 'drainWriteAdvice');

        [$message, $recommendation] = $advice->invoke($this->collector, '22001', 'nightowl_requests');

        $this->assertStringNotContainsString('Check the agent log for the offending row', $recommendation);
        // It must say the drain is stuck, not merely that a write failed.
        $this->assertStringContainsString('blocked', $message);
        $this->assertStringContainsString('NIGHTOWL_DRAIN_QUARANTINE', $recommendation);
    }

    public function testHealthReportCarriesTheResolvedAgentVersion(): void
    {
        $status = $this->collector->getFullStatus(
            startTime: microtime(true),
            backPressure: false,
            pendingRows: 0,
            walSize: 0,
            drainWorkerPid: 0,
        );

        $this->assertSame(MetricsCollector::agentVersion(), $status['agent_version']);
        $this->assertNotSame('1.0.0', $status['agent_version']);
    }

    public function testRecordIngestAndTick(): void
    {
        // Record some ingests then tick to push to ring buffer
        $this->collector->recordIngest();
        $this->collector->recordIngest();
        $this->collector->recordIngest();
        $this->collector->tick();

        // After tick, ingest ring should have 3 in one slot
        // Run diagnosis to update status
        $this->collector->runDiagnosis(false, 0, 0, 0);

        // With 3 ingests and no issues, should still be healthy
        $this->assertSame('healthy', $this->collector->getStatus());
    }

    public function testRecordReject(): void
    {
        $this->collector->recordReject();
        $this->collector->tick();

        // No crash, state is valid
        $this->collector->runDiagnosis(false, 0, 0, 0);
        $this->assertSame('healthy', $this->collector->getStatus());
    }

    // --- Diagnosis rule tests ---

    public function testDrainStoppedDiagnosis(): void
    {
        // Simulate: no drain activity, pending rows > 100
        // Need multiple ticks to build up ring data
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }

        // Run diagnosis twice for debounce (DEBOUNCE_TICKS = 2)
        $this->collector->runDiagnosis(false, 500, 0, 0);
        $this->collector->runDiagnosis(false, 500, 0, 0);

        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 500, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertContains('DRAIN_STOPPED', $codes);
    }

    /**
     * Feed a single-worker drain-metrics file into the collector, then delete it.
     *
     * @param  array<string, mixed>  $fields
     */
    private function loadDrainMetrics(array $fields): void
    {
        $base = tempnam(sys_get_temp_dir(), 'nightowl-drain-test');
        $file = $base.'.drain-metrics.json';
        file_put_contents($file, json_encode(array_merge([
            'rows_drained' => 0,
            'batches_failed' => 0,
            'pg_latency_ms' => 0,
            'updated_at' => microtime(true),
            'last_write_sqlstate' => null,
            'last_write_table' => null,
            'last_write_at' => 0.0,
            'last_write_ok_at' => 0.0,
            'last_conn_fail_at' => 0.0,
        ], $fields)));

        $this->collector->readDrainMetrics($base, 1);

        @unlink($file);
        @unlink($base);
    }

    private function activeDiagnosis(array $status, string $code): array|false
    {
        return current(array_filter($status['diagnoses'], fn ($d) => $d['code'] === $code));
    }

    public function testDrainWriteFailingSurfacesMigrateAdviceAndSuppressesDrainStopped(): void
    {
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }
        $now = microtime(true);

        // Drain stalled because the tenant schema isn't migrated (42P01).
        $this->loadDrainMetrics([
            'last_write_sqlstate' => '42P01',
            'last_write_table' => 'nightowl_requests',
            'last_write_at' => $now,
            'batches_failed' => 5,
        ]);
        $this->collector->runDiagnosis(false, 500, 0, 0);
        $this->collector->runDiagnosis(false, 500, 0, 0);

        $status = $this->collector->getFullStatus($now - 60, false, 500, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertContains('DRAIN_WRITE_FAILING', $codes);
        // The misleading "Postgres may be unreachable" card must NOT fire — the
        // connection is fine; only the writes are being rejected.
        $this->assertNotContains('DRAIN_STOPPED', $codes);

        $d = $this->activeDiagnosis($status, 'DRAIN_WRITE_FAILING');
        $this->assertNotFalse($d);
        $this->assertStringContainsString('nightowl:migrate', $d['recommendation']);
    }

    /**
     * A full disk (SQLSTATE 53100) previously fell through to the generic "PostgreSQL
     * is reachable but rejecting writes" line — a real incident cost hours because
     * nothing pointed at the disk. The write-failing card must now name it plainly.
     */
    public function testDrainWriteFailingNamesDiskFull(): void
    {
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }
        $now = microtime(true);

        $this->loadDrainMetrics([
            'last_write_sqlstate' => '53100',
            'last_write_table' => 'nightowl_exceptions',
            'last_write_at' => $now,
            'batches_failed' => 5,
        ]);
        $this->collector->runDiagnosis(false, 500, 0, 0);
        $this->collector->runDiagnosis(false, 500, 0, 0);

        $status = $this->collector->getFullStatus($now - 60, false, 500, 0, 0);
        $d = $this->activeDiagnosis($status, 'DRAIN_WRITE_FAILING');

        $this->assertNotFalse($d);
        $this->assertStringContainsStringIgnoringCase('disk space', $d['message']);
    }

    /**
     * Managed Postgres (Supabase/RDS) enforces a full disk by flipping the instance
     * to read-only, which surfaces as 25006. That must read as read-only/disk, and it
     * must be classified whole-target so good rows are retried, never quarantined.
     */
    public function testReadOnlyModeNamesReadOnlyAndIsWholeTarget(): void
    {
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }
        $now = microtime(true);

        $this->loadDrainMetrics([
            'last_write_sqlstate' => '25006',
            'last_write_table' => 'nightowl_requests',
            'last_write_at' => $now,
            'batches_failed' => 5,
        ]);
        $this->collector->runDiagnosis(false, 500, 0, 0);
        $this->collector->runDiagnosis(false, 500, 0, 0);

        $status = $this->collector->getFullStatus($now - 60, false, 500, 0, 0);
        $d = $this->activeDiagnosis($status, 'DRAIN_WRITE_FAILING');

        $this->assertNotFalse($d);
        $this->assertStringContainsStringIgnoringCase('read-only', $d['message']);

        // 25006 must be a whole-target failure — otherwise the drain isolates and
        // DROPS individual rows against a merely read-only DB (data loss).
        $isWholeTarget = new \ReflectionMethod(\NightOwl\Agent\DrainWorker::class, 'isWholeTargetFailure');
        $worker = (new \ReflectionClass(\NightOwl\Agent\DrainWorker::class))->newInstanceWithoutConstructor();
        $this->assertTrue($isWholeTarget->invoke($worker, ['sqlstate' => '25006']));
    }

    public function testDrainErrorsCoversFreshAppConnectivityFailureWithSmallBacklog(): void
    {
        // Regression guard: a fresh app (no successful drain yet, drainTotal==0) whose
        // drain fails for a CONNECTIVITY reason (no write-rejection signal) with a SMALL
        // backlog (pendingRows<=100, below DRAIN_STOPPED's >100 guard) must still surface
        // a diagnosis. DRAIN_ERRORS is its only cover — gating it on drainTotal>0 made the
        // agent falsely report healthy/score=100 while totally unable to drain.
        // Reverting the gate to `&& $this->drainTotal > 0` fails this test.
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }
        $now = microtime(true);

        $this->loadDrainMetrics([
            'rows_drained' => 0,   // never drained successfully (drainTotal == 0)
            'batches_failed' => 5, // batches are failing
            // no last_write_at -> writeFailing stays false (connection-class failure)
        ]);
        $this->collector->runDiagnosis(false, 50, 0, 0); // small backlog
        $this->collector->runDiagnosis(false, 50, 0, 0);

        $status = $this->collector->getFullStatus($now - 60, false, 50, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertContains('DRAIN_ERRORS', $codes);
        $this->assertNotContains('DRAIN_STOPPED', $codes);       // backlog too small
        $this->assertNotContains('DRAIN_WRITE_FAILING', $codes); // not a write-rejection
    }

    public function testDrainErrorsSuppressedWhenDrainStoppedAlreadyFiring(): void
    {
        // #6: when DRAIN_STOPPED owns the same root cause (a non-draining backlog),
        // the generic DRAIN_ERRORS warning must not double-fire for it. Removing the
        // `&& ! $drainStopped` guard (the original un-gated form) fails this test.
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }
        $now = microtime(true);

        $this->loadDrainMetrics([
            'rows_drained' => 0,
            'batches_failed' => 5,
        ]);
        $this->collector->runDiagnosis(false, 500, 0, 0); // large backlog + drainRate 0 -> DRAIN_STOPPED
        $this->collector->runDiagnosis(false, 500, 0, 0);

        $status = $this->collector->getFullStatus($now - 60, false, 500, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertContains('DRAIN_STOPPED', $codes);
        $this->assertNotContains('DRAIN_ERRORS', $codes);
    }

    public function testDrainUnreachableFiresForEstablishedAppConnectivityStall(): void
    {
        // The headline Phase-3 bug: an ESTABLISHED, high-volume worker (large lifetime
        // drainTotal) that loses PG CONNECTIVITY with a small backlog. DRAIN_STOPPED is
        // skipped (pendingRows<=100), DRAIN_WRITE_FAILING is skipped (no write-rejection),
        // and DRAIN_ERRORS' failRate dilutes below threshold against lifetime volume — so
        // before Phase 3 the agent reported HEALTHY (score 100) while unable to drain.
        // Reverting Phase 3 (no DRAIN_UNREACHABLE) yields zero diagnoses → this fails.
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }
        $now = microtime(true);

        $this->loadDrainMetrics([
            'rows_drained' => 10_000_000, // huge lifetime volume → dilutes failRate
            'batches_failed' => 50,
            'last_conn_fail_at' => $now,  // PG unreachable, fresh; no success since
        ]);
        $this->collector->runDiagnosis(false, 80, 0, 0); // small backlog
        $this->collector->runDiagnosis(false, 80, 0, 0);

        $status = $this->collector->getFullStatus($now - 60, false, 80, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertContains('DRAIN_UNREACHABLE', $codes);
        $this->assertSame('critical', $this->activeDiagnosis($status, 'DRAIN_UNREACHABLE')['level']);
        $this->assertNotContains('DRAIN_STOPPED', $codes);
        $this->assertNotContains('DRAIN_WRITE_FAILING', $codes);
        $this->assertNotContains('DRAIN_ERRORS', $codes);
    }

    public function testDrainUnreachableClearsWriteFailingLatch(): void
    {
        // #7: a prior non-connection rejection (42P01 "run migrate") must NOT latch
        // DRAIN_WRITE_FAILING through a later full DB-down outage — a newer connection
        // failure flips the diagnosis to DRAIN_UNREACHABLE. Reverting the latch-clear
        // term (lastDrainErrorAt >= lastConnFailAt) leaves DRAIN_WRITE_FAILING firing.
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }
        $now = microtime(true);

        $this->loadDrainMetrics([
            'last_write_sqlstate' => '42P01',
            'last_write_table' => 'nightowl_requests',
            'last_write_at' => $now - 1,  // earlier write-rejection
            'last_conn_fail_at' => $now,  // newer connection outage
        ]);
        $this->collector->runDiagnosis(false, 200, 0, 0);
        $this->collector->runDiagnosis(false, 200, 0, 0);

        $status = $this->collector->getFullStatus($now - 60, false, 200, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertContains('DRAIN_UNREACHABLE', $codes);
        $this->assertNotContains('DRAIN_WRITE_FAILING', $codes); // latch cleared
    }

    public function testDrainUnreachableDoesNotFireAfterRecovery(): void
    {
        // False-positive guard: a connection failure followed by a SUCCESSFUL drain
        // means the DB is reachable again — no alarm.
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }
        $now = microtime(true);

        $this->loadDrainMetrics([
            'rows_drained' => 1000,
            'last_conn_fail_at' => $now - 2,
            'last_write_ok_at' => $now - 1, // drained OK AFTER the blip
        ]);
        $this->collector->runDiagnosis(false, 80, 0, 0);
        $this->collector->runDiagnosis(false, 80, 0, 0);

        $status = $this->collector->getFullStatus($now - 60, false, 80, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertNotContains('DRAIN_UNREACHABLE', $codes);
    }

    public function testDrainUnreachableDecaysWhenConnectionFailureGoesStale(): void
    {
        // False-positive guard / no-latch: a connection failure older than the stale
        // window no longer fires — the worker re-stamps lastConnFailAt every failed
        // batch, so a stale value means it isn't actively failing to connect right now.
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }
        $now = microtime(true);

        $this->loadDrainMetrics([
            'last_conn_fail_at' => $now - 60, // older than DRAIN_CONN_FAIL_FRESH_SECONDS (45)
        ]);
        $this->collector->runDiagnosis(false, 80, 0, 0);
        $this->collector->runDiagnosis(false, 80, 0, 0);

        $status = $this->collector->getFullStatus($now - 60, false, 80, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertNotContains('DRAIN_UNREACHABLE', $codes);
    }

    public function testDrainUnreachableStaysFreshPastTheResolveWindow(): void
    {
        // The connection-failure freshness window must exceed the resolve window
        // (MIN_TICKS_FOR_RESOLVE × tick ≈ 30s) so a reported critical can dispatch an
        // all-clear. A 30s-old failure (beyond the OLD 15s window) must still fire —
        // reverting DRAIN_CONN_FAIL_FRESH_SECONDS to 15 makes this absent.
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }
        $now = microtime(true);

        $this->loadDrainMetrics([
            'last_conn_fail_at' => $now - 30, // within 45s, beyond the old 15s
        ]);
        $this->collector->runDiagnosis(false, 80, 0, 0);
        $this->collector->runDiagnosis(false, 80, 0, 0);

        $status = $this->collector->getFullStatus($now - 60, false, 80, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertContains('DRAIN_UNREACHABLE', $codes);
    }

    public function testWriteFailingWinsWhenRejectionIsNewerThanConnectionFailure(): void
    {
        // Mutual exclusion (the reverse of the latch-clear): a write-rejection that
        // POST-DATES a connection blip is the current cause — only DRAIN_WRITE_FAILING
        // fires, never a contradictory DRAIN_UNREACHABLE alongside it.
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }
        $now = microtime(true);

        $this->loadDrainMetrics([
            'last_conn_fail_at' => $now - 1, // earlier connection blip
            'last_write_sqlstate' => '42P01',
            'last_write_table' => 'nightowl_requests',
            'last_write_at' => $now,         // newer write-rejection
        ]);
        $this->collector->runDiagnosis(false, 200, 0, 0);
        $this->collector->runDiagnosis(false, 200, 0, 0);

        $status = $this->collector->getFullStatus($now - 60, false, 200, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertContains('DRAIN_WRITE_FAILING', $codes);
        $this->assertNotContains('DRAIN_UNREACHABLE', $codes);
    }

    public function testDrainWriteFailingTemplatesDataErrorWithoutRawRowValue(): void
    {
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }
        $now = microtime(true);

        // A poison row (value too long) — SQLSTATE 22001.
        $this->loadDrainMetrics([
            'last_write_sqlstate' => '22001',
            'last_write_table' => 'nightowl_requests',
            'last_write_at' => $now,
        ]);
        $this->collector->runDiagnosis(false, 500, 0, 0);
        $this->collector->runDiagnosis(false, 500, 0, 0);

        $status = $this->collector->getFullStatus($now - 60, false, 500, 0, 0);
        $d = $this->activeDiagnosis($status, 'DRAIN_WRITE_FAILING');

        $this->assertNotFalse($d);
        // Only SQLSTATE + table surface — never a raw libpq message / row value.
        $this->assertStringContainsString('22001', $d['message']);
        $this->assertStringContainsString('nightowl_requests', $d['message']);
        $this->assertStringContainsString('rejected by your database', $d['message']);
    }

    public function testDrainQuarantineDiagnosisFiresFromCumulativeTotalNotPrunableLiveCount(): void
    {
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }
        $now = microtime(true);

        // 3 payloads dropped (cumulative), but the live dead-letter count is already
        // 0 — i.e. pruneQuarantined() has cleared the retention window. The diagnosis
        // MUST still fire off the cumulative total: a dropped row is permanent data
        // loss, so the critical can't silently clear once the buffer rows age out.
        // (Before F7 the diagnosis rode quarantined_live and would vanish here.)
        $this->loadDrainMetrics([
            'quarantined_live' => 0,
            'quarantined_total' => 3,
            'last_quarantine_sqlstate' => '22001',
            'last_quarantine_table' => 'nightowl_requests',
            'last_quarantine_at' => $now,
        ]);
        $this->collector->runDiagnosis(false, 0, 0, 0);
        $this->collector->runDiagnosis(false, 0, 0, 0);

        $status = $this->collector->getFullStatus($now - 60, false, 0, 0, 0);
        $d = $this->activeDiagnosis($status, 'DRAIN_QUARANTINE');

        $this->assertNotFalse($d);
        $this->assertStringContainsString('3', $d['message']);
        $this->assertStringContainsString('22001', $d['recommendation']);
    }

    public function testDrainWriteFailingResolvesAfterSuccessfulWrite(): void
    {
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }
        $now = microtime(true);

        $this->loadDrainMetrics([
            'last_write_sqlstate' => '42501',
            'last_write_table' => 'nightowl_requests',
            'last_write_at' => $now,
        ]);
        $this->collector->runDiagnosis(false, 500, 0, 0);
        $this->collector->runDiagnosis(false, 500, 0, 0);
        $this->assertContains(
            'DRAIN_WRITE_FAILING',
            array_column($this->collector->getFullStatus($now - 60, false, 500, 0, 0)['diagnoses'], 'code')
        );

        // A later successful write (ok_at newer than the error) clears it.
        $this->loadDrainMetrics([
            'last_write_sqlstate' => '42501',
            'last_write_table' => 'nightowl_requests',
            'last_write_at' => $now,
            'last_write_ok_at' => $now + 1,
        ]);
        $this->collector->runDiagnosis(false, 0, 0, 0);
        $this->collector->runDiagnosis(false, 0, 0, 0);

        $this->assertNotContains(
            'DRAIN_WRITE_FAILING',
            array_column($this->collector->getFullStatus($now - 60, false, 0, 0, 0)['diagnoses'], 'code')
        );
    }

    public function testBackPressureActiveDiagnosis(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->collector->tick();
        }

        // Back pressure active triggers critical diagnosis
        $this->collector->runDiagnosis(true, 50000, 0, 0);
        $this->collector->runDiagnosis(true, 50000, 0, 0);

        $status = $this->collector->getFullStatus(microtime(true) - 60, true, 50000, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertContains('BACK_PRESSURE_ACTIVE', $codes);
    }

    public function testBacklogCriticalDiagnosis(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->collector->tick();
        }

        // 80%+ of max pending = critical
        $this->collector->runDiagnosis(false, 85000, 0, 0);
        $this->collector->runDiagnosis(false, 85000, 0, 0);

        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 85000, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertContains('BACKLOG_CRITICAL', $codes);
    }

    public function testBacklogHighDiagnosis(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->collector->tick();
        }

        // 50-80% of max = warning
        $this->collector->runDiagnosis(false, 60000, 0, 0);
        $this->collector->runDiagnosis(false, 60000, 0, 0);

        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 60000, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertContains('BACKLOG_HIGH', $codes);
    }

    public function testWalLargeDiagnosis(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->collector->tick();
        }

        // WAL > 100MB triggers warning
        $walSize = 150 * 1024 * 1024;
        $this->collector->runDiagnosis(false, 0, $walSize, 0);
        $this->collector->runDiagnosis(false, 0, $walSize, 0);

        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, $walSize, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertContains('WAL_LARGE', $codes);
    }

    public function testMemoryHighDiagnosis(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->collector->tick();
        }

        // RSS > 70% of max buffer memory
        $rss = (int) (256 * 1024 * 1024 * 0.8); // 80%
        $this->collector->runDiagnosis(false, 0, 0, $rss);
        $this->collector->runDiagnosis(false, 0, 0, $rss);

        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        $this->assertContains('MEMORY_HIGH', $codes);
    }

    // --- Health score tests ---

    public function testHealthyScoreWithNoDiagnoses(): void
    {
        $this->collector->runDiagnosis(false, 0, 0, 0);

        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);

        $this->assertSame(100, $status['health_score']);
        $this->assertSame('healthy', $status['status']);
    }

    public function testCriticalScoreWithMultipleDiagnoses(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->collector->tick();
        }

        // Trigger multiple critical diagnoses
        $rss = (int) (256 * 1024 * 1024 * 0.8);
        $this->collector->runDiagnosis(true, 90000, 150 * 1024 * 1024, $rss);
        $this->collector->runDiagnosis(true, 90000, 150 * 1024 * 1024, $rss);

        $status = $this->collector->getFullStatus(microtime(true) - 60, true, 90000, 150 * 1024 * 1024, 0);

        // Multiple criticals should bring score well below 40
        $this->assertLessThan(40, $status['health_score']);
        $this->assertSame('critical', $status['status']);
    }

    public function testDegradedStatusRange(): void
    {
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }

        // Trigger one warning (WAL_LARGE) and one critical (DRAIN_STOPPED with pending)
        // Score = 100 - 25 (critical) - 10 (warning) = 65 → degraded
        $this->collector->runDiagnosis(false, 500, 150 * 1024 * 1024, 0);
        $this->collector->runDiagnosis(false, 500, 150 * 1024 * 1024, 0);

        $this->assertSame('degraded', $this->collector->getStatus());
    }

    // --- Debounce tests ---

    public function testDiagnosisRequiresDebounce(): void
    {
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }

        // First tick — diagnosis exists but not yet debounced
        $this->collector->runDiagnosis(false, 500, 0, 0);

        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 500, 0, 0);
        $codes = array_column($status['diagnoses'], 'code');

        // After 1 tick, DRAIN_STOPPED should NOT appear (needs 2 ticks)
        $this->assertNotContains('DRAIN_STOPPED', $codes);
    }

    public function testTransientDiagnosisRemovedSilently(): void
    {
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }

        // Trigger DRAIN_STOPPED for 1 tick only
        $this->collector->runDiagnosis(false, 500, 0, 0);

        // Then condition clears
        $this->collector->runDiagnosis(false, 0, 0, 0);

        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);

        // No diagnoses and no resolved diagnoses (transient < MIN_TICKS_FOR_RESOLVE)
        $this->assertEmpty($status['diagnoses']);
        $this->assertEmpty($status['resolved_diagnoses']);
    }

    public function testGenuineResolution(): void
    {
        for ($i = 0; $i < 60; $i++) {
            $this->collector->tick();
        }

        // Trigger for 4 ticks (> MIN_TICKS_FOR_RESOLVE = 3)
        for ($i = 0; $i < 4; $i++) {
            $this->collector->runDiagnosis(false, 500, 0, 0);
        }

        // Now condition clears
        $this->collector->runDiagnosis(false, 0, 0, 0);

        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);

        // Should appear in resolved_diagnoses
        $resolvedCodes = array_column($status['resolved_diagnoses'], 'code');
        $this->assertContains('DRAIN_STOPPED', $resolvedCodes);
    }

    // --- Full status payload structure ---

    public function testGetFullStatusStructure(): void
    {
        $status = $this->collector->getFullStatus(
            startTime: microtime(true) - 120,
            backPressure: false,
            pendingRows: 42,
            walSize: 1024,
            drainWorkerPid: 12345,
        );

        // Top-level keys
        $this->assertArrayHasKey('version', $status);
        $this->assertArrayHasKey('status', $status);
        $this->assertArrayHasKey('health_score', $status);
        $this->assertArrayHasKey('uptime_seconds', $status);
        $this->assertArrayHasKey('ingest', $status);
        $this->assertArrayHasKey('drain', $status);
        $this->assertArrayHasKey('buffer', $status);
        $this->assertArrayHasKey('process', $status);
        $this->assertArrayHasKey('system', $status);
        $this->assertArrayHasKey('diagnoses', $status);
        $this->assertArrayHasKey('resolved_diagnoses', $status);
        $this->assertArrayHasKey('reported_at', $status);

        // Nested structure
        $this->assertArrayHasKey('total', $status['ingest']);
        $this->assertArrayHasKey('rate_1m', $status['ingest']);
        $this->assertArrayHasKey('pending_rows', $status['buffer']);
        $this->assertSame(42, $status['buffer']['pending_rows']);
        $this->assertSame(12345, $status['process']['drain_worker_pid']);
        $this->assertGreaterThan(100, $status['uptime_seconds']);
    }

    // testAgentVersion() lived here. It asserted $status['agent_version'] === '1.0.0'
    // — the hardcoded constant compared against itself, which is true by
    // construction and stayed green through twelve releases of the version being
    // wrong. Superseded by the agent_version tests above, which assert the contract
    // (resolved from Composer, within the platform's 16-char limit) rather than the
    // implementation detail.

    // --- Multi-worker metrics aggregation ---

    public function testReadDrainMetricsAggregatesMultipleWorkers(): void
    {
        $tmpDir = sys_get_temp_dir();
        $basePath = $tmpDir . '/nightowl_metrics_test_' . uniqid();

        // Write metrics for 3 workers
        $workers = [
            ['batches_drained' => 10, 'batches_failed' => 1, 'rows_drained' => 5000, 'pg_latency_ms' => 100.0, 'updated_at' => microtime(true)],
            ['batches_drained' => 8, 'batches_failed' => 0, 'rows_drained' => 4000, 'pg_latency_ms' => 200.0, 'updated_at' => microtime(true)],
            ['batches_drained' => 12, 'batches_failed' => 2, 'rows_drained' => 6000, 'pg_latency_ms' => 150.0, 'updated_at' => microtime(true)],
        ];

        foreach ($workers as $i => $data) {
            file_put_contents("{$basePath}.drain-metrics-{$i}.json", json_encode($data));
        }

        try {
            // First read establishes baseline
            $this->collector->readDrainMetrics($basePath, 3);
            // Tick to advance ring buffer
            $this->collector->tick();

            // Second read with increased totals
            $workers[0]['rows_drained'] = 6000;
            $workers[1]['rows_drained'] = 5000;
            $workers[2]['rows_drained'] = 7000;
            foreach ($workers as $i => $data) {
                file_put_contents("{$basePath}.drain-metrics-{$i}.json", json_encode($data));
            }
            $this->collector->readDrainMetrics($basePath, 3);

            $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);

            // batches_failed should be summed: 1 + 0 + 2 = 3
            $this->assertSame(3, $status['drain']['batches_failed']);

            // pg_latency_ms should be averaged: (100 + 200 + 150) / 3 = 150
            $this->assertEqualsWithDelta(150.0, $status['drain']['pg_latency_ms'], 1.0);

            // drain total should reflect sum across all workers
            $this->assertSame(18000, $status['drain']['total']); // 6000 + 5000 + 7000
        } finally {
            for ($i = 0; $i < 3; $i++) {
                @unlink("{$basePath}.drain-metrics-{$i}.json");
            }
        }
    }

    public function testReadDrainMetricsSumsAppVitalsAcrossWorkers(): void
    {
        $tmpDir = sys_get_temp_dir();
        $basePath = $tmpDir . '/nightowl_metrics_vitals_' . uniqid();

        $workers = [
            ['rows_drained' => 5000, 'app_requests_total' => 1000, 'app_requests_5xx' => 12, 'app_exceptions_total' => 4, 'app_open_issues' => 7, 'updated_at' => microtime(true)],
            ['rows_drained' => 4000, 'app_requests_total' => 800, 'app_requests_5xx' => 3, 'app_exceptions_total' => 1, 'app_open_issues' => 0, 'updated_at' => microtime(true)],
            ['rows_drained' => 6000, 'app_requests_total' => 1200, 'app_requests_5xx' => 5, 'app_exceptions_total' => 0, 'app_open_issues' => 7, 'updated_at' => microtime(true)],
        ];

        foreach ($workers as $i => $data) {
            file_put_contents("{$basePath}.drain-metrics-{$i}.json", json_encode($data));
        }

        try {
            $this->collector->readDrainMetrics($basePath, 3);

            $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);

            // Cumulative vitals are summed across workers.
            $this->assertSame(3000, $status['app_vitals']['requests_total']); // 1000 + 800 + 1200
            $this->assertSame(20, $status['app_vitals']['requests_5xx']);      // 12 + 3 + 5
            $this->assertSame(5, $status['app_vitals']['exceptions_total']);   // 4 + 1 + 0
            // Open issues is a per-tenant gauge — MAX across workers, not summed
            // (a worker that hasn't counted yet reports 0).
            $this->assertSame(7, $status['app_vitals']['open_issues']);
        } finally {
            for ($i = 0; $i < 3; $i++) {
                @unlink("{$basePath}.drain-metrics-{$i}.json");
            }
        }
    }

    public function testAppVitalsDefaultToZeroWhenAbsent(): void
    {
        // Older drain workers omit the app_vitals keys — the block must still
        // render as zeros (back-compat), never crash.
        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);

        $this->assertSame(0, $status['app_vitals']['requests_total']);
        $this->assertSame(0, $status['app_vitals']['requests_5xx']);
        $this->assertSame(0, $status['app_vitals']['exceptions_total']);
        $this->assertSame(0, $status['app_vitals']['open_issues']);
    }

    public function testReadDrainMetricsSingleWorkerFallback(): void
    {
        $tmpDir = sys_get_temp_dir();
        $basePath = $tmpDir . '/nightowl_metrics_single_' . uniqid();

        file_put_contents("{$basePath}.drain-metrics.json", json_encode([
            'batches_drained' => 5,
            'batches_failed' => 0,
            'rows_drained' => 2500,
            'pg_latency_ms' => 50.0,
            'updated_at' => microtime(true),
        ]));

        try {
            $this->collector->readDrainMetrics($basePath, 1);

            $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);

            $this->assertSame(2500, $status['drain']['total']);
            $this->assertSame(0, $status['drain']['batches_failed']);
            $this->assertEqualsWithDelta(50.0, $status['drain']['pg_latency_ms'], 1.0);
        } finally {
            @unlink("{$basePath}.drain-metrics.json");
        }
    }

    public function testReadDrainMetricsHandlesMissingWorkerFiles(): void
    {
        $tmpDir = sys_get_temp_dir();
        $basePath = $tmpDir . '/nightowl_metrics_partial_' . uniqid();

        // Only worker 0 has metrics, workers 1 and 2 have no files
        file_put_contents("{$basePath}.drain-metrics-0.json", json_encode([
            'batches_drained' => 5,
            'batches_failed' => 0,
            'rows_drained' => 2500,
            'pg_latency_ms' => 80.0,
            'updated_at' => microtime(true),
        ]));

        try {
            $this->collector->readDrainMetrics($basePath, 3);

            $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);

            // Should still report worker 0's metrics
            $this->assertSame(2500, $status['drain']['total']);
        } finally {
            @unlink("{$basePath}.drain-metrics-0.json");
        }
    }

    // --- Ring buffer remainder distribution ---

    public function testDrainRingDistributesRemainderAccurately(): void
    {
        $tmpDir = sys_get_temp_dir();
        $basePath = $tmpDir . '/nightowl_ring_test_' . uniqid();

        // Write initial metrics
        file_put_contents("{$basePath}.drain-metrics.json", json_encode([
            'batches_drained' => 0, 'batches_failed' => 0,
            'rows_drained' => 0, 'pg_latency_ms' => 0,
            'updated_at' => microtime(true),
        ]));

        try {
            // Establish baseline
            $this->collector->readDrainMetrics($basePath, 1);

            // Advance ring buffer to have clean slots
            for ($i = 0; $i < 10; $i++) {
                $this->collector->tick();
            }

            // Now report 13 rows drained (13 / 5 = 2 remainder 3)
            file_put_contents("{$basePath}.drain-metrics.json", json_encode([
                'batches_drained' => 1, 'batches_failed' => 0,
                'rows_drained' => 13, 'pg_latency_ms' => 10,
                'updated_at' => microtime(true),
            ]));

            $this->collector->readDrainMetrics($basePath, 1);

            // The drain rate should reflect all 13 rows distributed across 5 slots
            // (2 per slot + 1 extra for first 3 slots = 2+3+2+3+3 = 13 total)
            $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);
            $this->assertSame(13, $status['drain']['total']);
        } finally {
            @unlink("{$basePath}.drain-metrics.json");
        }
    }

    // --- Drain metrics staleness with multi-worker ---

    public function testDrainMetricsStalenessUsesOldestWorker(): void
    {
        $tmpDir = sys_get_temp_dir();
        $basePath = $tmpDir . '/nightowl_stale_test_' . uniqid();

        $now = microtime(true);

        // Worker 0 reported recently, worker 1 is stale
        file_put_contents("{$basePath}.drain-metrics-0.json", json_encode([
            'batches_drained' => 5, 'batches_failed' => 0,
            'rows_drained' => 1000, 'pg_latency_ms' => 50,
            'updated_at' => $now, // fresh
        ]));
        file_put_contents("{$basePath}.drain-metrics-1.json", json_encode([
            'batches_drained' => 3, 'batches_failed' => 0,
            'rows_drained' => 500, 'pg_latency_ms' => 60,
            'updated_at' => $now - 30, // 30s ago = stale
        ]));

        try {
            $this->collector->readDrainMetrics($basePath, 2);

            for ($i = 0; $i < 5; $i++) {
                $this->collector->tick();
            }

            // Run diagnosis — staleness should be based on oldest worker
            $this->collector->runDiagnosis(false, 0, 0, 0);
            $this->collector->runDiagnosis(false, 0, 0, 0);

            $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);

            // metrics_stale should be true because worker 1 is >15s old
            $this->assertTrue($status['drain']['metrics_stale']);
        } finally {
            @unlink("{$basePath}.drain-metrics-0.json");
            @unlink("{$basePath}.drain-metrics-1.json");
        }
    }

    // --- Defensive metric clamps (keep gauges within the API's decimal columns) ---

    public function testBufferUtilizationIsClampedWhenMaxPendingRowsIsTiny(): void
    {
        // A misconfigured max_pending_rows of 1 makes raw utilization explode
        // (50000 / 1 * 100 = 5,000,000%). The emitted value must be clamped so it
        // can't overflow the API's decimal(8,2) buffer_utilization_pct column.
        $collector = new MetricsCollector(
            maxPendingRows: 1,
            maxBufferMemory: 256 * 1024 * 1024,
        );

        $status = $collector->getFullStatus(microtime(true) - 60, false, 50000, 0, 0);

        $this->assertSame(100_000.0, $status['buffer']['utilization_pct']);
    }

    public function testPgLatencyIsClampedToCeiling(): void
    {
        // A stalled PostgreSQL could push EWMA latency arbitrarily high. The
        // emitted value must be clamped so it can't overflow the API's
        // decimal(12,2) pg_latency_ms column.
        $ref = new \ReflectionProperty(MetricsCollector::class, 'drainPgLatencyMs');
        $ref->setValue($this->collector, 9_999_999_999.0);

        $status = $this->collector->getFullStatus(microtime(true) - 60, false, 0, 0, 0);

        $this->assertSame(86_400_000.0, $status['drain']['pg_latency_ms']);
    }
}
