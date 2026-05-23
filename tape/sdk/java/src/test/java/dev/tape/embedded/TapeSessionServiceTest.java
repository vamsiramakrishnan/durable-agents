package dev.tape.embedded;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static dev.tape.embedded.Schema.EffectRecord;
import static dev.tape.embedded.Schema.EffectResolution;
import static dev.tape.embedded.Schema.ObligationRecord;
import static dev.tape.embedded.Schema.TimerRecord;
import static org.junit.jupiter.api.Assertions.*;

/** Mirrors {@code tape/sdk/python-adk/tests/test_service.py} — same
 *  invariants, JDBC transport. */
class TapeSessionServiceTest {

    private SqliteDataSource ds;
    private TapeSessionService svc;

    @BeforeEach void setup() {
        ds = new SqliteDataSource();
        svc = new TapeSessionService(ds);
    }

    @AfterEach void teardown() { ds.shutdown(); }

    // ── effect ledger basics ───────────────────────────────────────────────

    @Test
    void beginEffectIsIdempotentOnKey() throws Exception {
        EffectRecord e1 = svc.beginEffect(
            "t", "u", "s", "inv-1", 0, "bank.wire", 0,
            "{\"amount\":2000000}", null, null, null, null, null);
        EffectRecord e2 = svc.beginEffect(
            "t", "u", "s", "inv-1", 0, "bank.wire", 0,
            "{\"amount\":2000000}", null, null, null, null, null);
        assertEquals(e1.idempotencyKey(), e2.idempotencyKey());
        assertEquals(EffectRecord.PENDING, e2.status());
        List<EffectRecord> pending = svc.listPendingEffects(0L, true, true, 100);
        assertEquals(1, pending.size());
    }

    @Test
    void completeEffectIsTerminalIdempotent() throws Exception {
        EffectRecord e = svc.beginEffect(
            "t", "u", "s", "inv-1", 0, "bank.wire", 0,
            null, null, null, null, null, null);
        Optional<EffectRecord> r1 = svc.completeEffect(
            "t", "u", "s", e.idempotencyKey(),
            EffectRecord.CONFIRMED, "{\"wire_id\":\"w-1\"}", null);
        assertEquals(EffectRecord.CONFIRMED, r1.orElseThrow().status());

        Optional<EffectRecord> r2 = svc.completeEffect(
            "t", "u", "s", e.idempotencyKey(),
            EffectRecord.FAILED, null, "{\"err\":\"x\"}");
        assertEquals(EffectRecord.CONFIRMED, r2.orElseThrow().status()); // unchanged
        assertEquals("{\"wire_id\":\"w-1\"}", r2.get().responseJson());
    }

    // ── safety invariants ──────────────────────────────────────────────────

    @Test
    void nonIdempotentInlineIsRefused() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> svc.beginEffect(
                "t", "u", "s", "inv-1", 0, "bank.wire", 0,
                null, null,
                EffectRecord.NON_IDEMPOTENT, EffectRecord.INLINE,
                null, null));
        assertTrue(ex.getMessage().contains("NON_IDEMPOTENT"));
        assertTrue(ex.getMessage().contains("OUTBOX"));
    }

    @Test
    void outboxWithoutConnectorIsRefused() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> svc.beginEffect(
                "t", "u", "s", "inv-1", 0, "bank.wire", 0,
                null, null,
                EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
                null, null));
        assertTrue(ex.getMessage().contains("OUTBOX"));
        assertTrue(ex.getMessage().contains("connector"));
    }

    @Test
    void businessKeyDedupAcrossRuns() throws Exception {
        String bk = "acct1:2m:2026-05-18";
        svc.beginEffect("t", "u", "s1", "inv-1", 0, "bank.wire", 0,
            null, null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            bk, "bank.wire");
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> svc.beginEffect("t", "u", "s2", "inv-2", 0, "bank.wire", 0,
                null, null,
                EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
                bk, "bank.wire"));
        assertTrue(ex.getMessage().contains("business_key"));
    }

    // ── CAS lease ──────────────────────────────────────────────────────────

    @Test
    void claimEffectDispatchSingleWinnerUnderConcurrency() throws Exception {
        EffectRecord e = svc.beginEffect(
            "t", "u", "s", "inv-1", 0, "bank.wire", 0,
            null, null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            "acct:2m:2026", "bank.wire");

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            CompletableFuture<TapeSessionService.ClaimEffectResult> a = CompletableFuture.supplyAsync(
                () -> { try {
                    return svc.claimEffectDispatch("t", "u", "s", e.idempotencyKey(),
                        "dispatcher-A", 60_000L, 0L);
                } catch (Exception ex) { throw new RuntimeException(ex); } }, pool);
            CompletableFuture<TapeSessionService.ClaimEffectResult> b = CompletableFuture.supplyAsync(
                () -> { try {
                    return svc.claimEffectDispatch("t", "u", "s", e.idempotencyKey(),
                        "dispatcher-B", 60_000L, 0L);
                } catch (Exception ex) { throw new RuntimeException(ex); } }, pool);
            TapeSessionService.ClaimEffectResult r1 = a.get(5, TimeUnit.SECONDS);
            TapeSessionService.ClaimEffectResult r2 = b.get(5, TimeUnit.SECONDS);
            int wins = (r1.acquired() ? 1 : 0) + (r2.acquired() ? 1 : 0);
            assertEquals(1, wins, "exactly one dispatcher must win the CAS");
            EffectRecord eff = (r1.acquired() ? r1.effect() : r2.effect()).orElseThrow();
            assertTrue("dispatcher-A".equals(eff.dispatchClaimedBy())
                || "dispatcher-B".equals(eff.dispatchClaimedBy()));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void expiredDispatchLeaseIsReclaimable() throws Exception {
        EffectRecord e = svc.beginEffect(
            "t", "u", "s", "inv-1", 0, "bank.wire", 0,
            null, null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            "acct:2m:2026", "bank.wire");
        TapeSessionService.ClaimEffectResult first = svc.claimEffectDispatch(
            "t", "u", "s", e.idempotencyKey(), "A", 1L, 0L);
        assertTrue(first.acquired());

        long future = System.currentTimeMillis() + 1000L;
        TapeSessionService.ClaimEffectResult second = svc.claimEffectDispatch(
            "t", "u", "s", e.idempotencyKey(), "B", 60_000L, future);
        assertTrue(second.acquired());
        assertEquals("B", second.effect().orElseThrow().dispatchClaimedBy());
    }

    // ── UNKNOWN transition ─────────────────────────────────────────────────

    @Test
    void recordDispatchAttemptZeroTransitionsToUnknown() throws Exception {
        EffectRecord e = svc.beginEffect(
            "t", "u", "s", "inv-1", 0, "bank.wire", 0,
            null, null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            "acct:2m:2026", "bank.wire");
        Optional<EffectRecord> r = svc.recordDispatchAttempt(
            "t", "u", "s", e.idempotencyKey(),
            "simulated lost ack", 0L);
        EffectRecord rec = r.orElseThrow();
        assertEquals(EffectRecord.UNKNOWN, rec.status());
        assertEquals(1, rec.dispatchAttempts());
        assertTrue(rec.dispatchClaimedBy() == null || rec.dispatchClaimedBy().isEmpty());

        List<EffectRecord> unknowns = svc.listPendingEffects(0L, false, true, 100);
        assertEquals(1, unknowns.size());
        assertEquals(EffectRecord.UNKNOWN, unknowns.get(0).status());
    }

    @Test
    void externalObservationConfirmedResolvesUnknown() throws Exception {
        EffectRecord e = svc.beginEffect(
            "t", "u", "s", "inv-1", 0, "bank.wire", 0,
            null, null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            "acct:2m:2026", "bank.wire");
        svc.recordDispatchAttempt("t", "u", "s", e.idempotencyKey(), "ack lost", 0L);
        Optional<EffectRecord> r = svc.recordExternalObservation(
            "t", "u", "s", e.idempotencyKey(),
            EffectResolution.CONFIRMED, "wire-0001",
            "{\"wire_id\":\"wire-0001\"}", null, "");
        EffectRecord rec = r.orElseThrow();
        assertEquals(EffectRecord.CONFIRMED, rec.status());
        assertEquals("wire-0001", rec.externalRef());
    }

    @Test
    void duplicateObservationAtomicallyRegistersCompensation() throws Exception {
        EffectRecord e = svc.beginEffect(
            "t", "u", "s", "inv-1", 0, "bank.wire", 0,
            null, null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            "acct:2m:2026", "bank.wire");
        svc.recordDispatchAttempt("t", "u", "s", e.idempotencyKey(), "ack lost", 0L);
        Optional<EffectRecord> r = svc.recordExternalObservation(
            "t", "u", "s", e.idempotencyKey(),
            EffectResolution.DUPLICATE, "wire-A",
            null, null, "reverse_wire");
        assertEquals(EffectRecord.CONFIRMED, r.orElseThrow().status());

        List<ObligationRecord> obs = svc.listObligations("t", "u", "s", true, null);
        assertEquals(1, obs.size());
        assertEquals("reverse_wire", obs.get(0).kind());
        assertEquals(ObligationRecord.PENDING, obs.get(0).status());
        assertEquals(e.idempotencyKey(), obs.get(0).effectKey());
    }

    @Test
    void absentForNonIdempotentStaysUnknown() throws Exception {
        EffectRecord e = svc.beginEffect(
            "t", "u", "s", "inv-1", 0, "bank.wire", 0,
            null, null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            "acct:2m:2026", "bank.wire");
        svc.recordDispatchAttempt("t", "u", "s", e.idempotencyKey(), "ack lost", 0L);
        Optional<EffectRecord> r = svc.recordExternalObservation(
            "t", "u", "s", e.idempotencyKey(),
            EffectResolution.ABSENT, "", null, null, "");
        assertEquals(EffectRecord.UNKNOWN, r.orElseThrow().status());
    }

    // ── obligation ledger ──────────────────────────────────────────────────

    @Test
    void registerCompensationIsIdempotent() throws Exception {
        ObligationRecord o1 = svc.registerCompensation(
            "t", "u", "s", "inv-1", "ek-1", "reverse_wire",
            "{\"amount\":1}", null, 5);
        ObligationRecord o2 = svc.registerCompensation(
            "t", "u", "s", "inv-1", "ek-1", "reverse_wire",
            "{\"amount\":2}", null, 5);
        assertEquals(o1.seq(), o2.seq());
        List<ObligationRecord> obs = svc.listObligations("t", "u", "s", true, null);
        assertEquals(1, obs.size());
    }

    @Test
    void claimObligationSingleWinner() throws Exception {
        ObligationRecord o = svc.registerCompensation(
            "t", "u", "s", "inv-1", "ek-1", "reverse_wire",
            null, null, 5);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            CompletableFuture<TapeSessionService.ClaimObligationResult> a = CompletableFuture.supplyAsync(
                () -> { try { return svc.claimObligation(o.seq(), "A", 60_000L, 0L); }
                        catch (Exception ex) { throw new RuntimeException(ex); } }, pool);
            CompletableFuture<TapeSessionService.ClaimObligationResult> b = CompletableFuture.supplyAsync(
                () -> { try { return svc.claimObligation(o.seq(), "B", 60_000L, 0L); }
                        catch (Exception ex) { throw new RuntimeException(ex); } }, pool);
            TapeSessionService.ClaimObligationResult r1 = a.get(5, TimeUnit.SECONDS);
            TapeSessionService.ClaimObligationResult r2 = b.get(5, TimeUnit.SECONDS);
            int wins = (r1.acquired() ? 1 : 0) + (r2.acquired() ? 1 : 0);
            assertEquals(1, wins);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void recordObligationAttemptRetriesThenStucks() throws Exception {
        ObligationRecord o = svc.registerCompensation(
            "t", "u", "s", "inv-1", "ek-1", "reverse_wire",
            null, null, 3);
        long future = System.currentTimeMillis() + 10_000L;

        ObligationRecord r1 = svc.recordObligationAttempt(o.seq(), "boom", future).orElseThrow();
        assertEquals(ObligationRecord.PENDING, r1.status());
        assertEquals(1, r1.attempts());

        ObligationRecord r2 = svc.recordObligationAttempt(o.seq(), "boom 2", future).orElseThrow();
        assertEquals(ObligationRecord.PENDING, r2.status());
        assertEquals(2, r2.attempts());

        ObligationRecord r3 = svc.recordObligationAttempt(o.seq(), "boom 3", future).orElseThrow();
        assertEquals(ObligationRecord.STUCK, r3.status());
        assertEquals(3, r3.attempts());
    }

    @Test
    void terminalNowAttemptForcesStuck() throws Exception {
        ObligationRecord o = svc.registerCompensation(
            "t", "u", "s", "inv-1", "ek-1", "reverse_wire",
            null, null, 10);
        ObligationRecord r = svc.recordObligationAttempt(
            o.seq(), "business rule says no", 0L).orElseThrow();
        assertEquals(ObligationRecord.STUCK, r.status());
        assertEquals(1, r.attempts());
    }

    @Test
    void listUnresolvedObligationsIncludesPendingAndCommittedExpired() throws Exception {
        ObligationRecord o1 = svc.registerCompensation(
            "t", "u", "s1", "inv-1", "ek-1", "reverse_wire",
            null, null, 5);
        // Claim with a short TTL so it expires immediately.
        svc.claimObligation(o1.seq(), "A", 1L, 0L);
        long future = System.currentTimeMillis() + 1000L;
        List<ObligationRecord> rows = svc.listUnresolvedObligations(
            future, 500, true, false, true);
        assertTrue(rows.stream().anyMatch(o -> o.seq() == o1.seq()),
            "COMMITTED-expired obligation should appear in unresolved list");
    }

    // ── timers ─────────────────────────────────────────────────────────────

    @Test
    void setTimerIdempotentOnTimerId() throws Exception {
        TimerRecord t1 = svc.setTimer("t", "u", "s", "redrive-1", 12345L, "redrive", null);
        TimerRecord t2 = svc.setTimer("t", "u", "s", "redrive-1", 99999L, "redrive", null);
        assertEquals(t1.fireAtMs(), t2.fireAtMs(), "second set_timer must not overwrite");
    }

    @Test
    void listDueTimersClaimMarksFired() throws Exception {
        long now = System.currentTimeMillis();
        svc.setTimer("t", "u", "s", "due-1", now - 1000L, "redrive", null);
        svc.setTimer("t", "u", "s", "future-1", now + 60_000L, "redrive", null);

        List<TimerRecord> due = svc.listDueTimers(now, 100, true);
        assertEquals(1, due.size());
        assertEquals("due-1", due.get(0).timerId());

        List<TimerRecord> again = svc.listDueTimers(now, 100, false);
        assertTrue(again.isEmpty(), "fired timers must not re-surface");
    }

    // ── reactive KV ────────────────────────────────────────────────────────

    @Test
    void writeValueCasAdvancesAndRejectsStale() throws Exception {
        Schema.ValueRecord v1 = svc.writeValue("treasury", "fx_rate",
            "{\"USD\":1.0}", 0, "");
        assertEquals(1, v1.version());

        Schema.ValueRecord v2 = svc.writeValue("treasury", "fx_rate",
            "{\"USD\":1.01}", 1, "");
        assertEquals(2, v2.version());

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> svc.writeValue("treasury", "fx_rate",
                "{\"USD\":1.02}", 1, ""));
        assertTrue(ex.getMessage().contains("stale CAS"));
    }
}
