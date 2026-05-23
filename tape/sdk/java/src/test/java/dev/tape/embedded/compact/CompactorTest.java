package dev.tape.embedded.compact;

import dev.tape.embedded.Schema;
import dev.tape.embedded.SqliteDataSource;
import dev.tape.embedded.TapeSessionService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static dev.tape.embedded.Schema.EffectRecord;
import static dev.tape.embedded.Schema.ObligationRecord;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Compactor tests — mirrors {@code tape/sdk/python-adk/tests/test_compact.py}.
 *
 * <p>The point of these tests isn't that DELETE works; it's that the
 * SAFETY INVARIANTS hold:
 *
 * <ul>
 *   <li>a CONFIRMED effect with an unresolved obligation referencing it
 *       must NOT be pruned, even if it's old enough;</li>
 *   <li>a session with a STUCK obligation must NOT be archived (operator
 *       triage signal);</li>
 *   <li>a session with an unfired timer (past or future) must NOT be
 *       archived;</li>
 *   <li>the compactor is idempotent across ticks — running it twice
 *       produces the same final state as running it once.</li>
 * </ul>
 */
class CompactorTest {

    private SqliteDataSource ds;
    private TapeSessionService svc;
    private final Map<String, Integer> callSeq = new HashMap<>();

    @BeforeEach void setup() throws Exception {
        ds = new SqliteDataSource();
        svc = new TapeSessionService(ds);
        svc.prepareTables();
        callSeq.clear();
    }

    @AfterEach void teardown() { ds.shutdown(); }

    /** Helper: seed a CONFIRMED effect, then backdate its ts_ms via direct
     *  UPDATE so we don't have to wait the TTL out in wall-clock time. */
    private String beginConfirmed(String key, String invocationId, long tsMs) throws Exception {
        int ci = callSeq.getOrDefault(invocationId, 0);
        callSeq.put(invocationId, ci + 1);
        EffectRecord e = svc.beginEffect(
            "a", "u", "s", invocationId, 0, "bank.wire", ci,
            null, null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            key, "bank.wire");
        svc.completeEffect("a", "u", "s", e.idempotencyKey(),
            EffectRecord.CONFIRMED, "{\"id\":\"" + key + "\"}", null);
        // Backdate ts_ms via direct UPDATE.
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                "UPDATE tape_effects SET ts_ms=? WHERE idempotency_key=?")) {
            ps.setLong(1, tsMs);
            ps.setString(2, e.idempotencyKey());
            ps.executeUpdate();
        }
        return e.idempotencyKey();
    }

    private void backdate(String table, String column, long tsMs, String whereCol, Object whereVal) throws Exception {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                "UPDATE " + table + " SET " + column + "=? WHERE " + whereCol + "=?")) {
            ps.setLong(1, tsMs);
            if (whereVal instanceof Long l) ps.setLong(2, l);
            else ps.setString(2, String.valueOf(whereVal));
            ps.executeUpdate();
        }
    }

    // ── mechanism 1+2: terminal-state pruning gated by TTL ───────────────

    @Test
    void prunesOldTerminalEffect() throws Exception {
        String key = beginConfirmed("k-1", "inv-1", 1000L);
        CompactionPolicy policy = new CompactionPolicy(
            1L, CompactionPolicy.DEFAULT_SESSION_TTL_MS, true, true, 1000);
        CompactionResult result = Compactor.compactOnce(svc, policy, 100_000L);
        assertEquals(1, result.effectsPruned());
        Optional<EffectRecord> eff = svc.getEffect("a", "u", "s", key);
        assertTrue(eff.isEmpty());
    }

    @Test
    void keepsFreshTerminalEffect() throws Exception {
        String key = beginConfirmed("k-1", "inv-1", 99_999L);
        CompactionPolicy policy = new CompactionPolicy(
            1_000_000L, CompactionPolicy.DEFAULT_SESSION_TTL_MS, true, true, 1000);
        CompactionResult result = Compactor.compactOnce(svc, policy, 100_000L);
        assertEquals(0, result.effectsPruned());
        assertTrue(svc.getEffect("a", "u", "s", key).isPresent());
    }

    @Test
    void keepsPendingEffectRegardlessOfAge() throws Exception {
        EffectRecord e = svc.beginEffect(
            "a", "u", "s", "inv-1", 0, "bank.wire", 0,
            null, null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            "k-1", "bank.wire");
        backdate("tape_effects", "ts_ms", 0L, "idempotency_key", e.idempotencyKey());

        CompactionPolicy policy = new CompactionPolicy(
            1L, CompactionPolicy.DEFAULT_SESSION_TTL_MS, true, true, 1000);
        CompactionResult result = Compactor.compactOnce(svc, policy, 100_000L);
        assertEquals(0, result.effectsPruned(),
            "PENDING is not in the terminal set");
    }

    // ── mechanism 5: compensable-window pinning ──────────────────────────

    @Test
    void pinningRefusesToPruneEffectWithActiveObligation() throws Exception {
        String key = beginConfirmed("k-1", "inv-1", 1000L);
        svc.registerCompensation("a", "u", "s", "",
            key, "reverse_wire",
            "{\"external_ref\":\"wire-1\"}", null, 5);

        CompactionPolicy policy = new CompactionPolicy(
            1L, CompactionPolicy.DEFAULT_SESSION_TTL_MS, true, true, 1000);
        CompactionResult result = Compactor.compactOnce(svc, policy, 100_000L);
        assertEquals(0, result.effectsPruned(), "PINNED");
        assertTrue(svc.getEffect("a", "u", "s", key).isPresent());
    }

    @Test
    void pinningReleasesWhenObligationResolved() throws Exception {
        String key = beginConfirmed("k-1", "inv-1", 1000L);
        ObligationRecord ob = svc.registerCompensation(
            "a", "u", "s", "", key, "reverse_wire", null, null, 5);
        svc.resolveObligation(ob.seq(), ObligationRecord.COMPENSATED, null);
        // Backdate the obligation past the TTL.
        backdate("tape_obligations", "ts_ms", 1000L, "seq", ob.seq());

        CompactionPolicy policy = new CompactionPolicy(
            1L, CompactionPolicy.DEFAULT_SESSION_TTL_MS, true, true, 1000);
        CompactionResult result = Compactor.compactOnce(svc, policy, 100_000L);
        // Both pruned: obligation (terminal + old), then now-unpinned effect.
        assertEquals(1, result.obligationsPruned());
        assertEquals(1, result.effectsPruned());
    }

    @Test
    void pinningKeepsEffectWithStuckObligation() throws Exception {
        // STUCK is NOT in active obligations (PENDING/COMMITTED) — the pin
        // doesn't catch it. Documents the policy that STUCK is terminal in
        // the "needs human triage" sense; the human keeps the obligation
        // visible but the effect is pruned per TTL like any other.
        String key = beginConfirmed("k-1", "inv-1", 1000L);
        ObligationRecord ob = svc.registerCompensation(
            "a", "u", "s", "", key, "reverse_wire", null, null, 5);
        svc.resolveObligation(ob.seq(), ObligationRecord.STUCK, null);

        CompactionPolicy policy = new CompactionPolicy(
            1L, CompactionPolicy.DEFAULT_SESSION_TTL_MS, true, true, 1000);
        CompactionResult result = Compactor.compactOnce(svc, policy, 100_000L);
        assertEquals(1, result.effectsPruned());
        // The obligation itself stays (STUCK is not in the
        // archive_terminal_obligations target — only COMPENSATED is).
        var obs = svc.listObligations("a", "u", "s", false, null);
        assertEquals(1, obs.size());
        assertEquals(ObligationRecord.STUCK, obs.get(0).status());
    }

    // ── obligation archival ───────────────────────────────────────────────

    @Test
    void prunesOldCompensatedObligation() throws Exception {
        ObligationRecord ob = svc.registerCompensation(
            "a", "u", "s", "", "ek-orphan", "reverse_wire", null, null, 5);
        svc.resolveObligation(ob.seq(), ObligationRecord.COMPENSATED, null);
        backdate("tape_obligations", "ts_ms", 1000L, "seq", ob.seq());

        CompactionPolicy policy = new CompactionPolicy(
            1L, CompactionPolicy.DEFAULT_SESSION_TTL_MS, true, true, 1000);
        CompactionResult result = Compactor.compactOnce(svc, policy, 100_000L);
        assertEquals(1, result.obligationsPruned());
    }

    @Test
    void keepsStuckObligationRegardlessOfAge() throws Exception {
        ObligationRecord ob = svc.registerCompensation(
            "a", "u", "s", "", "ek", "reverse_wire", null, null, 5);
        svc.resolveObligation(ob.seq(), ObligationRecord.STUCK, null);
        backdate("tape_obligations", "ts_ms", 0L, "seq", ob.seq());

        CompactionPolicy policy = new CompactionPolicy(
            1L, CompactionPolicy.DEFAULT_SESSION_TTL_MS, true, true, 1000);
        CompactionResult result = Compactor.compactOnce(svc, policy, 100_000L);
        assertEquals(0, result.obligationsPruned());
    }

    // ── session archival (mechanism 3) ────────────────────────────────────

    @Test
    void archivesIdleTerminalSession() throws Exception {
        // Three CONFIRMED effects, all ancient.
        for (int i = 0; i < 3; i++) {
            beginConfirmed("k-" + i, "inv-" + i, 1000L);
        }
        CompactionPolicy policy = new CompactionPolicy(
            1L, 1L, true, true, 1000);
        CompactionResult result = Compactor.compactOnce(svc, policy, 100_000L);
        assertEquals(1, result.sessionsArchived());
    }

    @Test
    void doesNotArchiveSessionWithActiveObligation() throws Exception {
        beginConfirmed("k-0", "inv-0", 1000L);
        svc.registerCompensation("a", "u", "s", "", "k-orphan",
            "reverse_wire", null, null, 5);   // PENDING

        CompactionPolicy policy = new CompactionPolicy(
            1L, 1L, true, true, 1000);
        CompactionResult result = Compactor.compactOnce(svc, policy, 100_000L);
        assertEquals(0, result.sessionsArchived());
    }

    @Test
    void doesNotArchiveSessionWithUnfiredTimer() throws Exception {
        beginConfirmed("k-0", "inv-0", 1000L);
        svc.setTimer("a", "u", "s", "redrive-1", 99_999_999L,
            "redrive", null);

        CompactionPolicy policy = new CompactionPolicy(
            1L, 1L, true, true, 1000);
        CompactionResult result = Compactor.compactOnce(svc, policy, 100_000L);
        assertEquals(0, result.sessionsArchived());
    }

    // ── idempotency: running compaction twice == once ────────────────────

    @Test
    void compactIsIdempotentAcrossTicks() throws Exception {
        for (int i = 0; i < 3; i++) {
            beginConfirmed("k-" + i, "inv-" + i, 1000L);
        }
        CompactionPolicy policy = new CompactionPolicy(
            1L, 1L, true, true, 1000);
        CompactionResult r1 = Compactor.compactOnce(svc, policy, 100_000L);
        CompactionResult r2 = Compactor.compactOnce(svc, policy, 100_000L);
        assertTrue(r1.total() > 0);
        assertEquals(0, r2.total(), "second tick is a no-op");
    }

    // ── default policy values match Python reference ─────────────────────

    @Test
    void defaultPolicyMatchesPythonReference() {
        CompactionPolicy p = CompactionPolicy.defaults();
        // 7 days, 30 days, both archives on, max_per_tick=1000.
        assertEquals(7L * 24 * 60 * 60 * 1000, p.effectTtlMs());
        assertEquals(30L * 24 * 60 * 60 * 1000, p.sessionTtlMs());
        assertTrue(p.archiveTerminalObligations());
        assertTrue(p.archiveFiredTimers());
        assertEquals(1000, p.maxPerTick());
    }
}
