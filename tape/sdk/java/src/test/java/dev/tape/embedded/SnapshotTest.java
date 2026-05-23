package dev.tape.embedded;

import dev.tape.embedded.compact.CompactionPolicy;
import dev.tape.embedded.compact.CompactionResult;
import dev.tape.embedded.compact.Compactor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.Optional;

import static dev.tape.embedded.Schema.CapturedEffect;
import static dev.tape.embedded.Schema.EffectRecord;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Mirrors {@code tape/sdk/python-adk/tests/test_snapshot.py} — same nine
 * invariants, JDBC transport.
 *
 * <p>The contract: {@link TapeSessionService#takeSnapshot} captures terminal
 * effects into a per-session JSON blob; {@link TapeSessionService#beginEffect}
 * falls back to that blob when the live row is gone, so the compactor is
 * free to prune underlying rows without breaking the idempotency-key
 * short-circuit.
 */
class SnapshotTest {

    private SqliteDataSource ds;
    private TapeSessionService svc;

    @BeforeEach void setup() throws Exception {
        ds = new SqliteDataSource();
        svc = new TapeSessionService(ds);
        svc.prepareTables();
    }

    @AfterEach void teardown() { ds.shutdown(); }

    /** Make a CONFIRMED outbox effect. {@code callIndex} is pinned per call
     *  so the derived idempotency_key is distinct across helper invocations. */
    private String confirmedEffect(String key, int callIndex, String responseJson)
            throws Exception {
        return confirmedEffect(key, "inv-1", callIndex, responseJson, key, "bank.wire");
    }

    private String confirmedEffect(
            String key, String invocation, int callIndex,
            String responseJson, String businessKey, String connector) throws Exception {
        EffectRecord e = svc.beginEffect(
            "a", "u", "s", invocation, 0, "bank.wire", callIndex,
            null, null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            businessKey, connector);
        svc.completeEffect("a", "u", "s", e.idempotencyKey(),
            EffectRecord.CONFIRMED, responseJson, null);
        return e.idempotencyKey();
    }

    // ── basic ─────────────────────────────────────────────────────────────

    @Test
    void takeSnapshot_capturesTerminalEffects() throws Exception {
        String k1 = confirmedEffect("k1", 0, "{\"id\":\"wire-1\"}");
        String k2 = confirmedEffect("k2", 1, "{\"id\":\"wire-2\"}");

        TapeSessionService.TakeSnapshotResult r =
            svc.takeSnapshot("a", "u", "s", 0L);
        assertEquals(2, r.captured());
        assertEquals(2, r.mergedTotal());

        Optional<TapeSessionService.EffectSnapshot> snap =
            svc.getSnapshot("a", "u", "s");
        assertTrue(snap.isPresent());
        Map<String, CapturedEffect> map = snap.get().effectsJson();
        assertEquals(java.util.Set.of(k1, k2), map.keySet());
        // The JSON value column is round-tripped as a JSON-encoded string;
        // the inline JSON shape is preserved (no double-encoding).
        assertEquals("{\"id\":\"wire-1\"}", map.get(k1).responseJson());
        assertEquals(EffectRecord.CONFIRMED, map.get(k2).status());
    }

    @Test
    void takeSnapshot_excludesNonTerminalEffects() throws Exception {
        // PENDING — never completed.
        svc.beginEffect(
            "a", "u", "s", "inv-1", 0, "bank.wire", 0,
            null, null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            "k-pending", "bank.wire");

        TapeSessionService.TakeSnapshotResult r =
            svc.takeSnapshot("a", "u", "s", 0L);
        assertEquals(0, r.captured());
    }

    @Test
    void repeatedSnapshot_isCumulative() throws Exception {
        String k1 = confirmedEffect("k1", 0, "{\"v\":1}");
        TapeSessionService.TakeSnapshotResult r1 =
            svc.takeSnapshot("a", "u", "s", 0L);
        assertEquals(1, r1.mergedTotal());

        String k2 = confirmedEffect("k2", 1, "{\"v\":2}");
        TapeSessionService.TakeSnapshotResult r2 =
            svc.takeSnapshot("a", "u", "s", 0L);
        // Both rows still terminal — both captured by the unbounded read.
        assertEquals(2, r2.captured());
        assertEquals(2, r2.mergedTotal());

        Optional<TapeSessionService.EffectSnapshot> snap =
            svc.getSnapshot("a", "u", "s");
        assertTrue(snap.isPresent());
        assertEquals(java.util.Set.of(k1, k2), snap.get().effectsJson().keySet());
    }

    // ── the load-bearing invariant: short-circuit survives row deletion ──

    @Test
    void beginEffect_shortCircuitsViaSnapshotAfterRowPruned() throws Exception {
        // The whole point. Snapshot the effect, manually delete the live
        // row (simulating the compactor), and verify `beginEffect` with the
        // same derived key returns the snapshot data instead of creating a
        // fresh PENDING row.
        //
        // If this test fails the compactor can break the idempotency
        // contract — the bug the snapshot exists to prevent.
        String k = confirmedEffect("k1", 0, "{\"id\":\"wire-1\"}");
        svc.takeSnapshot("a", "u", "s", 0L);

        // Brute-force delete the live row (no compactor TTL nonsense — we
        // want to test the fallback path, not the policy).
        try (Connection c = svc.acquireMaintenanceConnection();
             PreparedStatement ps = c.prepareStatement(
                 "DELETE FROM tape_effects WHERE idempotency_key=?")) {
            ps.setString(1, k);
            ps.executeUpdate();
        }
        assertTrue(svc.getEffect("a", "u", "s", k).isEmpty());

        // `beginEffect` with the same (invocation, decision, tool,
        // callIndex) — which derives to the same idempotency_key — should
        // NOT create a new PENDING row. It should return the snapshot's
        // captured CONFIRMED record.
        EffectRecord e = svc.beginEffect(
            "a", "u", "s", "inv-1", 0, "bank.wire", 0,
            null, null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            "k1", "bank.wire");
        assertEquals(k, e.idempotencyKey());
        assertEquals(EffectRecord.CONFIRMED, e.status());
        assertEquals("{\"id\":\"wire-1\"}", e.responseJson());

        // And the live row is STILL gone — no resurrection.
        assertTrue(svc.getEffect("a", "u", "s", k).isEmpty());
    }

    @Test
    void beginEffect_prefersLiveRowOverSnapshot() throws Exception {
        // When BOTH the live row and a snapshot entry exist for the same
        // key, the live row wins — it's authoritative. Snapshot is purely
        // a fallback for the row-pruned case.
        String k = confirmedEffect("k1", 0, "{\"id\":\"live\"}");
        svc.takeSnapshot("a", "u", "s", 0L);

        // Mutate the snapshot's stored effects_json to disagree with the
        // live row. `beginEffect` should still return the live row.
        String tamperedJson = "{" + jsonQuoted(k)
            + ":{\"status\":\"confirmed\",\"semantics\":\"non_idempotent\","
            + "\"dispatch_mode\":\"outbox\",\"business_key\":\"k1\","
            + "\"connector\":\"bank.wire\",\"external_ref\":null,"
            + "\"request_json\":null,"
            + "\"response_json\":{\"id\":\"stale-snapshot\"},"
            + "\"error_json\":null,\"invocation_id\":\"inv-1\","
            + "\"decision_index\":0,\"tool_name\":\"bank.wire\","
            + "\"call_index\":0,\"ts_ms\":1}}";
        try (Connection c = svc.acquireMaintenanceConnection();
             PreparedStatement ps = c.prepareStatement(
                 "UPDATE tape_effect_snapshots SET effects_json=?"
                 + " WHERE app_name=? AND user_id=? AND session_id=?")) {
            ps.setString(1, tamperedJson);
            ps.setString(2, "a");
            ps.setString(3, "u");
            ps.setString(4, "s");
            ps.executeUpdate();
        }

        EffectRecord e = svc.beginEffect(
            "a", "u", "s", "inv-1", 0, "bank.wire", 0,
            null, null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            "k1", "bank.wire");
        assertEquals("{\"id\":\"live\"}", e.responseJson());
    }

    // ── snapshot + compactor: the integration that makes pruning safe ────

    @Test
    void snapshotThenCompactThenBeginEffect_shortCircuits() throws Exception {
        // End-to-end: snapshot, compact (which prunes the underlying row),
        // then `beginEffect` still short-circuits. This is the real
        // operator path.
        String k = confirmedEffect("k1", 0, "{\"id\":\"wire-1\"}");
        svc.takeSnapshot("a", "u", "s", 0L);

        // Compact with effectTtlMs=0 so the (just-confirmed) effect is
        // immediately eligible for pruning. The snapshot row is NOT in the
        // compactor's purview — it isn't touched.
        CompactionPolicy policy = new CompactionPolicy(
            0L,             // effectTtlMs
            1_000_000_000L, // sessionTtlMs — far future, no archival
            false,          // archiveTerminalObligations
            false,          // archiveFiredTimers
            1000);          // maxPerTick
        CompactionResult result = Compactor.compactOnce(svc, policy, 0L);
        assertEquals(1, result.effectsPruned());

        // Live row gone; snapshot row remains.
        assertTrue(svc.getEffect("a", "u", "s", k).isEmpty());
        assertTrue(svc.getSnapshot("a", "u", "s").isPresent());

        // Short-circuit through the snapshot.
        EffectRecord e = svc.beginEffect(
            "a", "u", "s", "inv-1", 0, "bank.wire", 0,
            null, null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            "k1", "bank.wire");
        assertEquals(EffectRecord.CONFIRMED, e.status());
        assertEquals("{\"id\":\"wire-1\"}", e.responseJson());
    }

    // ── watermark ─────────────────────────────────────────────────────────

    @Test
    void takeSnapshot_respectsUpToTsMs() throws Exception {
        // `upToTsMs` bounds the read window — effects with `ts_ms` beyond
        // the watermark are NOT captured. The watermark on the snapshot row
        // reflects what's been captured so a later snapshot knows where to
        // resume from.
        confirmedEffect("k-early", 0, "{\"v\":1}");
        // Snapshot at ts=1 — far in the past, so the effect isn't included.
        TapeSessionService.TakeSnapshotResult r =
            svc.takeSnapshot("a", "u", "s", 1L);
        assertEquals(0, r.captured());
        assertEquals(1L, r.upToTsMs());
    }

    @Test
    void snapshot_handlesNoEffectsGracefully() throws Exception {
        // `takeSnapshot` on a session with zero terminal effects creates a
        // snapshot row with an empty map — safe and idempotent.
        TapeSessionService.TakeSnapshotResult r =
            svc.takeSnapshot("a", "u", "s", 0L);
        assertEquals(0, r.captured());
        assertEquals(0, r.mergedTotal());
        Optional<TapeSessionService.EffectSnapshot> snap =
            svc.getSnapshot("a", "u", "s");
        assertTrue(snap.isPresent());
        assertTrue(snap.get().effectsJson().isEmpty());
    }

    @Test
    void getSnapshot_returnsEmptyForNoSnapshot() throws Exception {
        // No snapshot taken — `getSnapshot` is empty, not an empty row.
        assertTrue(svc.getSnapshot("a", "u", "s").isEmpty());
    }

    // ── helpers ───────────────────────────────────────────────────────────

    /** Minimal JSON string quoting for keys we build inline in tests. */
    private static String jsonQuoted(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
