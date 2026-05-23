package dev.tape.embedded.chaos;

import dev.tape.embedded.CompensationResult;
import dev.tape.embedded.Connector;
import dev.tape.embedded.DispatchResult;
import dev.tape.embedded.ObservationResult;
import dev.tape.embedded.Reactors;
import dev.tape.embedded.Schema;
import dev.tape.embedded.SqliteDataSource;
import dev.tape.embedded.TapeSessionService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static dev.tape.embedded.Schema.EffectRecord;
import static dev.tape.embedded.Schema.ObligationRecord;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Embedded-tier chaos tests — mirrors
 * {@code tape/sdk/python-adk/tests/test_chaos.py}. Proves the same
 * invariants the Python suite proves, plus the strict-faults
 * false-positive guard.
 */
class ChaosTest {

    private SqliteDataSource ds;
    private TapeSessionService svc;

    @BeforeEach void setup() {
        ds = new SqliteDataSource();
        svc = new TapeSessionService(ds);
    }

    @AfterEach void teardown() { ds.shutdown(); }

    /** A trivial idempotent ledger connector — fixture analogue of the
     *  Python {@code _LedgerConnector}. CONFIRMED on dispatch; mirrors the
     *  business_key dedupe a real bank's API provides. */
    static final class LedgerConnector implements Connector {
        final String name;
        final Map<String, String> ledger = new LinkedHashMap<>();
        long delayMs = 0L;

        LedgerConnector() { this("bank.wire"); }
        LedgerConnector(String name) { this.name = name; }

        @Override public String name() { return name; }

        @Override public DispatchResult dispatch(EffectRecord effect) throws Exception {
            if (delayMs > 0) Thread.sleep(delayMs);
            String bk = effect.businessKey() != null && !effect.businessKey().isEmpty()
                ? effect.businessKey() : effect.idempotencyKey();
            String wid = ledger.computeIfAbsent(bk,
                k -> String.format("w-%04d", ledger.size()));
            return DispatchResult.confirmed(wid,
                "{\"wire_id\":\"" + wid + "\"}");
        }

        @Override public ObservationResult observe(EffectRecord effect) {
            String bk = effect.businessKey() != null && !effect.businessKey().isEmpty()
                ? effect.businessKey() : effect.idempotencyKey();
            if (ledger.containsKey(bk)) {
                return ObservationResult.confirmed(ledger.get(bk), null);
            }
            return ObservationResult.absent();
        }

        @Override public CompensationResult compensate(ObligationRecord obligation) {
            return CompensationResult.compensated(null);
        }
    }

    private static EffectRecord syntheticEffect(String tool, String connector, String key) {
        return new EffectRecord(
            "a", "u", "s", "k-" + tool, "inv-1",
            0, tool, 0,
            EffectRecord.PENDING, EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            "bk-" + tool, connector, null,
            0, 0L, null, 0L, null,
            "{}", null, null, 0L);
    }

    // ── ChaosConnector: the fault mechanism, in isolation ────────────────

    @Test
    void loseAckFlipsConfirmedToUnknown() throws Exception {
        LedgerConnector inner = new LedgerConnector();
        ChaosConnector wrapped = new ChaosConnector(
            inner,
            List.of(Chaos.loseAck("bank.wire", "", 1.0)),
            new java.util.Random(0L));

        EffectRecord effect = new EffectRecord(
            "a", "u", "s", "k", "inv-1",
            0, "wire", 0,
            EffectRecord.PENDING, EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            "bk-1", "bank.wire", null,
            0, 0L, null, 0L, null,
            "{}", null, null, 0L);
        DispatchResult result = wrapped.dispatch(effect);
        assertEquals(DispatchResult.UNKNOWN, result.status());
        // The inner call did land (the wrapper's contract).
        assertEquals(1, inner.ledger.size());
        assertTrue(inner.ledger.containsKey("bk-1"));
    }

    @Test
    void delayConnectorBlocksDispatch() throws Exception {
        LedgerConnector inner = new LedgerConnector();
        ChaosConnector wrapped = new ChaosConnector(
            inner,
            List.of(Chaos.delayConnector("bank.wire", 80L)),
            new java.util.Random(0L));

        EffectRecord effect = new EffectRecord(
            "a", "u", "s", "k", "inv-1",
            0, "wire", 0,
            EffectRecord.PENDING, EffectRecord.IDEMPOTENT, EffectRecord.INLINE,
            null, "bank.wire", null,
            0, 0L, null, 0L, null,
            "{}", null, null, 0L);
        long t0 = System.nanoTime();
        wrapped.dispatch(effect);
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(elapsedMs >= 70, "honoured the delay; was " + elapsedMs + "ms");
    }

    @Test
    void toolScopedFaultOnlyFiresOnMatchingTool() throws Exception {
        LedgerConnector inner = new LedgerConnector();
        ChaosConnector wrapped = new ChaosConnector(
            inner,
            List.of(Chaos.loseAck("", "wire", 1.0)),
            new java.util.Random(0L));

        DispatchResult rWire = wrapped.dispatch(
            syntheticEffect("wire", "bank.wire", "bk-wire"));
        DispatchResult rPost = wrapped.dispatch(
            syntheticEffect("post_gl", "bank.wire", "bk-post"));
        assertEquals(DispatchResult.UNKNOWN, rWire.status(),
            "tool matches → fault fires");
        assertEquals(DispatchResult.CONFIRMED, rPost.status(),
            "tool doesn't match → passthrough");
    }

    // ── strict_faults: the silent-skip false-positive guard ─────────────

    @Test
    void strictFaultsFailsOnMissingConnector() throws Exception {
        Scenario scen = Chaos.scenario(
            "missing-target",
            List.of(Chaos.loseAck("bank.wire", "")),
            List.of(Invariants.NO_STUCK_OBLIGATIONS));
        ChaosReport report = Chaos.run(scen, svc, ds,
            new HashMap<>(),  // empty — bank.wire missing
            c -> {});
        assertFalse(report.passed());
        assertTrue(report.invariantResults().stream()
            .anyMatch(r -> r.toString().contains("strict_faults")));
    }

    @Test
    void strictFaultsOffAllowsSkip() throws Exception {
        Scenario scen = new Scenario(
            "optional-target",
            List.of(Chaos.loseAck("bank.wire", "")),
            List.of(Invariants.NO_STUCK_OBLIGATIONS),
            0L,
            false);   // strict_faults off
        ChaosReport report = Chaos.run(scen, svc, ds,
            new HashMap<>(), c -> {});
        assertTrue(report.passed());
        assertTrue(report.notes().stream()
            .anyMatch(n -> n.contains("not in `connectors` dict")));
    }

    // ── invariants: read the embedded tables ─────────────────────────────

    @Test
    void noStuckObligationsPassesOnCleanStore() throws Exception {
        Scenario scen = Chaos.scenario("smoke",
            List.of(),
            List.of(Invariants.NO_STUCK_OBLIGATIONS));
        ChaosReport report = Chaos.run(scen, svc, ds,
            new HashMap<>(), c -> {});
        assertTrue(report.passed(), report.toString());
    }

    @Test
    void noStuckObligationsFailsWhenOneIsStuck() throws Exception {
        ObligationRecord ob = svc.registerCompensation(
            "a", "u", "s", "", "ek", "reverse_wire", null, null, 1);
        svc.resolveObligation(ob.seq(), ObligationRecord.STUCK, null);

        Scenario scen = Chaos.scenario("stuck",
            List.of(),
            List.of(Invariants.NO_STUCK_OBLIGATIONS));
        ChaosReport report = Chaos.run(scen, svc, ds,
            new HashMap<>(), c -> {});
        assertFalse(report.passed());
        assertTrue(report.invariantResults().stream()
            .anyMatch(r -> r.toString().toLowerCase().contains("stuck")));
    }

    @Test
    void exactlyOneInvariant() throws Exception {
        EffectRecord e = svc.beginEffect(
            "a", "u", "s", "inv-1", 0, "wire", 0,
            null, null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            "bk-1", "bank.wire");
        svc.completeEffect("a", "u", "s", e.idempotencyKey(),
            EffectRecord.CONFIRMED, "{\"id\":\"1\"}", null);

        Scenario scen = Chaos.scenario("one",
            List.of(),
            List.of(Invariants.exactlyOneByConnector("bank.wire")));
        ChaosReport report = Chaos.run(scen, svc, ds,
            new HashMap<>(), c -> {});
        assertTrue(report.passed(), report.toString());
    }

    // ── end-to-end: lose_ack → reconcile loop drives UNKNOWN to CONFIRMED ─

    @Test
    void loseAckE2EWithReconciler() throws Exception {
        EffectRecord eff = svc.beginEffect(
            "a", "u", "s", "inv-1", 0, "wire", 0,
            null, null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            "bk-1", "bank.wire");
        final LedgerConnector bank = new LedgerConnector();

        Scenario scen = Chaos.scenario(
            "unknown-then-reconcile",
            List.of(Chaos.loseAck("bank.wire", "", 1.0)),
            List.of(
                Invariants.NO_STUCK_OBLIGATIONS,
                Invariants.exactlyOneByConnector("bank.wire")));

        Map<String, Connector> connectors = new HashMap<>();
        connectors.put("bank.wire", bank);

        ChaosReport report = Chaos.run(scen, svc, ds, connectors,
            wrapped -> {
                // Tick 1: dispatch (gets UNKNOWN due to lose_ack).
                List<Reactors.ReactorAction> r1 = Reactors.dispatchOutboxOnce(
                    svc, wrapped, "d-1");
                assertTrue(r1.stream().anyMatch(a -> "unknown".equals(a.outcome())),
                    "expected UNKNOWN outcome: " + r1);
                // The bank's ledger has exactly one wire (inner call landed).
                assertEquals(1, bank.ledger.size());
                // Tick 2: reconciler observes the unwrapped bank and resolves.
                Map<String, Connector> bare = new HashMap<>();
                bare.put("bank.wire", bank);
                List<Reactors.ReactorAction> r2 = Reactors.reconcileOnce(
                    svc, bare);
                assertTrue(r2.stream().anyMatch(a -> "confirmed".equals(a.outcome())),
                    "expected CONFIRMED resolution: " + r2);
            });
        assertTrue(report.passed(), report.toString());
        // And exactly one wire on the bank's side: the property the whole
        // contract exists to enforce.
        assertEquals(1, bank.ledger.size());
    }

    // ── invariant API uniformity ─────────────────────────────────────────

    @Test
    void invariantsRejectBothScopesSet() {
        // exactly_one rejects both connector and tool simultaneously.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> Invariants.exactlyOne("bank.wire", "wire"));
        assertTrue(ex.getMessage().contains("connector= or tool="));
    }

    @Test
    void loseAckRejectsBothScopesSet() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> Chaos.loseAck("bank.wire", "wire", 1.0));
        assertTrue(ex.getMessage().contains("connector= or tool="));
    }
}
