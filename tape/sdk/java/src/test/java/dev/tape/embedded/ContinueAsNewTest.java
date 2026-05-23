package dev.tape.embedded;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static dev.tape.embedded.Schema.EffectRecord;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Mirrors {@code tape/sdk/python-adk/tests/test_continue_as_new.py} — same
 * invariants, JDBC transport. Seven tests total: one per behaviour the
 * Python reference proves.
 *
 * <p>Same safety invariant as the compactor (compensable-window pinning):
 * an old invocation's effect that has an active obligation pointing at it
 * is NOT pruned, even when {@code continueAsNew} asks to wipe the slate.
 */
class ContinueAsNewTest {

    private SqliteDataSource ds;
    private TapeSessionService svc;
    private final Map<String, Integer> callSeq = new HashMap<>();

    @BeforeEach void setup() {
        ds = new SqliteDataSource();
        svc = new TapeSessionService(ds);
        callSeq.clear();
    }

    @AfterEach void teardown() { ds.shutdown(); }

    private String confirmedEffect(String invocation, String key) throws Exception {
        // Each call gets a fresh call_index so the derived idempotency_key
        // is unique (begin_effect derives the key from
        // invocation/decision/tool/call_index).
        int ci = callSeq.getOrDefault(invocation, 0);
        callSeq.put(invocation, ci + 1);
        EffectRecord e = svc.beginEffect(
            "a", "u", "s", invocation, 0, "bank.wire", ci,
            null, null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            key, "bank.wire");
        svc.completeEffect("a", "u", "s", e.idempotencyKey(),
            EffectRecord.CONFIRMED, "{\"id\":\"" + key + "\"}", null);
        return e.idempotencyKey();
    }

    // ── happy path ────────────────────────────────────────────────────────

    @Test
    void prunesOldInvocationsTerminalEffects() throws Exception {
        String[] keys = new String[3];
        for (int i = 0; i < 3; i++) {
            keys[i] = confirmedEffect("inv-old", "k-" + i);
        }
        // Sanity: 3 rows.
        for (String k : keys) {
            assertTrue(svc.getEffect("a", "u", "s", k).isPresent());
        }

        TapeSessionService.ContinueAsNewResult r = svc.continueAsNew(
            "a", "u", "s", "inv-old", "inv-new", null, true);
        assertEquals(3, r.effectsPruned());
        for (String k : keys) {
            assertTrue(svc.getEffect("a", "u", "s", k).isEmpty());
        }
    }

    @Test
    void keepsOtherInvocationsEffects() throws Exception {
        String kOld = confirmedEffect("inv-old", "k-old");
        String kOther = confirmedEffect("inv-other", "k-other");

        TapeSessionService.ContinueAsNewResult r = svc.continueAsNew(
            "a", "u", "s", "inv-old", "inv-new", null, true);
        assertEquals(1, r.effectsPruned());
        assertTrue(svc.getEffect("a", "u", "s", kOld).isEmpty());
        // The other invocation's effect is still there.
        assertTrue(svc.getEffect("a", "u", "s", kOther).isPresent());
    }

    // ── safety: pinning by an active obligation ──────────────────────────

    @Test
    void doesNotPrunePinnedEffectEvenUnderOldInvocation() throws Exception {
        String key = confirmedEffect("inv-old", "k-1");
        svc.registerCompensation("a", "u", "s", "", key,
            "reverse_wire", null, null, 5);  // PENDING (active)

        TapeSessionService.ContinueAsNewResult r = svc.continueAsNew(
            "a", "u", "s", "inv-old", "inv-new", null, true);
        assertEquals(0, r.effectsPruned());
        assertEquals(1, r.obligationsKept());
        // Effect still there — the compensator may still need its external_ref.
        assertTrue(svc.getEffect("a", "u", "s", key).isPresent());
    }

    // ── state carry ──────────────────────────────────────────────────────

    @Test
    void carriedStateIsReadableUnderNewInvocationId() throws Exception {
        svc.continueAsNew(
            "a", "u", "s", "inv-old", "inv-new",
            "{\"checkpoint\":\"after sweep\",\"balance\":42}", true);

        Optional<Schema.ValueRecord> val = svc.getValue(
            "tape:continue-as-new:s", "inv-new");
        assertTrue(val.isPresent());
        assertEquals("{\"checkpoint\":\"after sweep\",\"balance\":42}",
            val.get().valueJson());
        assertEquals("continue_as_new", val.get().writer());
    }

    @Test
    void continueAsNewIsAtomic() throws Exception {
        String key = confirmedEffect("inv-old", "k-1");
        TapeSessionService.ContinueAsNewResult r = svc.continueAsNew(
            "a", "u", "s", "inv-old", "inv-new",
            "{\"x\":1}", true);
        assertEquals(1, r.effectsPruned());
        assertTrue(r.stateWritten());
        // Both observable after one call.
        assertTrue(svc.getEffect("a", "u", "s", key).isEmpty());
        Optional<Schema.ValueRecord> val = svc.getValue(
            "tape:continue-as-new:s", "inv-new");
        assertTrue(val.isPresent());
    }

    @Test
    void noPruneWhenPruneOldFalse() throws Exception {
        String key = confirmedEffect("inv-old", "k-1");
        TapeSessionService.ContinueAsNewResult r = svc.continueAsNew(
            "a", "u", "s", "inv-old", "inv-new",
            "{\"x\":1}", false);
        assertEquals(0, r.effectsPruned());
        assertTrue(r.stateWritten());
        assertTrue(svc.getEffect("a", "u", "s", key).isPresent());
    }

    // ── idempotency: carrying state twice updates, doesn't duplicate ─────

    @Test
    void repeatedContinueAsNewUpdatesState() throws Exception {
        svc.continueAsNew("a", "u", "s", "inv-1", "inv-2",
            "{\"step\":1}", true);
        svc.continueAsNew("a", "u", "s", "inv-2", "inv-2",
            "{\"step\":2}", true);
        Optional<Schema.ValueRecord> val = svc.getValue(
            "tape:continue-as-new:s", "inv-2");
        assertTrue(val.isPresent());
        assertEquals("{\"step\":2}", val.get().valueJson());
        assertEquals(2, val.get().version());
    }
}
