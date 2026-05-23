package dev.tape.embedded;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static dev.tape.embedded.Schema.EffectRecord;
import static dev.tape.embedded.Schema.ObligationRecord;
import static org.junit.jupiter.api.Assertions.*;

/** Mirrors {@code tape/sdk/python-adk/tests/test_reactors.py}. */
class ReactorsTest {

    private SqliteDataSource ds;
    private TapeSessionService svc;

    @BeforeEach void setup() {
        ds = new SqliteDataSource();
        svc = new TapeSessionService(ds);
    }

    @AfterEach void teardown() { ds.shutdown(); }

    // ── a configurable test connector (mirrors the Python BankConnector) ──

    /** In-memory bank that dedupes on business_key. */
    static final class FakeBank {
        final Map<String, Map<String, Object>> ledger = new HashMap<>();
        Map<String, Object> wire(String businessKey, int amount, String account) {
            return ledger.computeIfAbsent(businessKey, bk -> {
                int n = ledger.size() + 1;
                Map<String, Object> rec = new HashMap<>();
                rec.put("wire_id", String.format("wire-%04d", n));
                rec.put("amount", amount);
                rec.put("account", account);
                rec.put("business_key", bk);
                return rec;
            });
        }
        Map<String, Object> find(String bk) { return ledger.get(bk); }
    }

    static class BankConnector implements Connector {
        final FakeBank bank;
        final String name;
        boolean injectUnknownOnce;
        boolean raiseOnce;
        int nDispatches;

        BankConnector(FakeBank bank) { this(bank, "bank.wire"); }
        BankConnector(FakeBank bank, String name) { this.bank = bank; this.name = name; }

        @Override public String name() { return name; }

        @Override public DispatchResult dispatch(EffectRecord effect) {
            nDispatches++;
            String bk = effect.businessKey() == null ? "" : effect.businessKey();
            // Always write to the bank (the "call landed" part).
            int amount = 0;
            String account = "?";
            if (effect.requestJson() != null) {
                if (effect.requestJson().contains("\"amount\":")) {
                    try {
                        int i = effect.requestJson().indexOf("\"amount\":") + 9;
                        int j = i;
                        while (j < effect.requestJson().length()
                            && (Character.isDigit(effect.requestJson().charAt(j))
                                || effect.requestJson().charAt(j) == '-')) j++;
                        amount = Integer.parseInt(effect.requestJson().substring(i, j));
                    } catch (Exception ignore) {}
                }
            }
            Map<String, Object> wire = bank.wire(bk, amount, account);
            if (injectUnknownOnce && nDispatches == 1) {
                return DispatchResult.unknown("simulated lost ack");
            }
            if (raiseOnce && nDispatches == 1) {
                throw new RuntimeException("simulated transient network error");
            }
            return DispatchResult.confirmed((String) wire.get("wire_id"),
                "{\"wire_id\":\"" + wire.get("wire_id") + "\"}");
        }

        @Override public ObservationResult observe(EffectRecord effect) {
            String bk = effect.businessKey() == null ? "" : effect.businessKey();
            Map<String, Object> rec = bank.find(bk);
            if (rec == null) return ObservationResult.absent();
            return ObservationResult.confirmed((String) rec.get("wire_id"),
                "{\"wire_id\":\"" + rec.get("wire_id") + "\"}");
        }

        @Override public CompensationResult compensate(ObligationRecord ob) {
            if (ob.payloadJson() == null || !ob.payloadJson().contains("external_ref")) {
                return CompensationResult.failed("no wire_id", 0);
            }
            return CompensationResult.compensated(null);
        }
    }

    // ── the full UNKNOWN → reconcile loop ──────────────────────────────────

    @Test
    void fullUnknownReconcileLoop() throws Exception {
        FakeBank bank = new FakeBank();
        BankConnector conn = new BankConnector(bank);
        conn.injectUnknownOnce = true;

        EffectRecord e = svc.beginEffect(
            "t", "u", "s", "inv-1", 0, "bank.wire", 0,
            "{\"amount\":2000000,\"account\":\"acct-1\"}", null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            "acct1:2m:2026-05-18", "bank.wire");
        assertEquals(EffectRecord.PENDING, e.status());

        // Tick 1: outbox dispatches. Bank gets the wire, connector returns UNKNOWN.
        List<Reactors.ReactorAction> r1 = Reactors.dispatchOutboxOnce(
            svc, Map.of("bank.wire", conn), "d-1");
        assertTrue(r1.stream().anyMatch(a -> "unknown".equals(a.outcome())),
            "first tick should record UNKNOWN, got: " + r1);
        EffectRecord eff = svc.getEffect("t", "u", "s", e.idempotencyKey()).orElseThrow();
        assertEquals(EffectRecord.UNKNOWN, eff.status());
        assertEquals(1, bank.ledger.size(), "bank should have exactly one wire");

        // Tick 2: reconciler observes. Bank says CONFIRMED. Effect → CONFIRMED.
        List<Reactors.ReactorAction> r2 = Reactors.reconcileOnce(
            svc, Map.of("bank.wire", conn));
        assertTrue(r2.stream().anyMatch(a -> "confirmed".equals(a.outcome())),
            "reconciler must confirm, got: " + r2);
        eff = svc.getEffect("t", "u", "s", e.idempotencyKey()).orElseThrow();
        assertEquals(EffectRecord.CONFIRMED, eff.status());
        assertEquals("wire-0001", eff.externalRef());
        assertEquals(1, bank.ledger.size(), "still exactly one wire after reconcile");
    }

    @Test
    void outboxBacksOffOnGenericException() throws Exception {
        BankConnector conn = new BankConnector(new FakeBank());
        conn.raiseOnce = true;
        EffectRecord e = svc.beginEffect(
            "t", "u", "s", "inv-1", 0, "bank.wire", 0,
            "{\"amount\":100,\"account\":\"x\"}", null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            "x:100:2026", "bank.wire");

        long now = System.currentTimeMillis();
        List<Reactors.ReactorAction> r1 = Reactors.dispatchOutboxOnce(
            svc, Map.of("bank.wire", conn), "d-1", 50,
            60_000L, 10_000L, 300_000L);
        assertTrue(r1.stream().anyMatch(a -> "exception".equals(a.outcome())),
            "expected exception outcome, got: " + r1);
        EffectRecord eff = svc.getEffect("t", "u", "s", e.idempotencyKey()).orElseThrow();
        assertEquals(EffectRecord.PENDING, eff.status());
        assertTrue(eff.nextDispatchAtMs() > now, "must reschedule for the future");
        assertEquals(1, eff.dispatchAttempts());
    }

    @Test
    void twoDispatchersDispatchEachEffectAtMostOnce() throws Exception {
        FakeBank bank = new FakeBank();
        BankConnector conn = new BankConnector(bank);

        // Three effects, all eligible.
        for (int i = 0; i < 3; i++) {
            svc.beginEffect(
                "t", "u", "s", "inv-" + i, 0, "bank.wire", 0,
                "{\"amount\":" + i + ",\"account\":\"x\"}", null,
                EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
                "x:" + i + ":2026", "bank.wire");
        }

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            CompletableFuture<List<Reactors.ReactorAction>> f1 = CompletableFuture.supplyAsync(
                () -> { try {
                    return Reactors.dispatchOutboxOnce(svc, Map.of("bank.wire", conn), "d-1");
                } catch (Exception ex) { throw new RuntimeException(ex); } }, pool);
            CompletableFuture<List<Reactors.ReactorAction>> f2 = CompletableFuture.supplyAsync(
                () -> { try {
                    return Reactors.dispatchOutboxOnce(svc, Map.of("bank.wire", conn), "d-2");
                } catch (Exception ex) { throw new RuntimeException(ex); } }, pool);
            List<Reactors.ReactorAction> all = new ArrayList<>();
            all.addAll(f1.get(10, TimeUnit.SECONDS));
            all.addAll(f2.get(10, TimeUnit.SECONDS));
            long confirmed = all.stream().filter(a -> "confirmed".equals(a.outcome())).count();
            assertEquals(3L, confirmed, "each effect must be dispatched exactly once");
            assertEquals(3, bank.ledger.size(), "bank ledger must agree");
            List<EffectRecord> pending = svc.listPendingEffects(0L, true, true, 100);
            assertTrue(pending.isEmpty(), "no PENDING effects after all-confirmed");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void drainCompensatesPendingObligation() throws Exception {
        FakeBank bank = new FakeBank();
        bank.wire("x:1:2026", 1, "x");
        BankConnector conn = new BankConnector(bank);

        ObligationRecord ob = svc.registerCompensation(
            "t", "u", "s", "inv-1", "ek-1", "bank.wire",
            "{\"external_ref\":\"wire-0001\"}", null, 5);
        List<Reactors.ReactorAction> r = Reactors.drainObligationsOnce(
            svc, Map.of("bank.wire", conn), "dr-1");
        assertTrue(r.stream().anyMatch(a -> "compensated".equals(a.outcome())),
            "expected compensated outcome, got: " + r);

        List<ObligationRecord> after = svc.listObligations("t", "u", "s", false, null);
        assertEquals(1, after.size());
        assertEquals(ObligationRecord.COMPENSATED, after.get(0).status());
    }

    @Test
    void duplicateFlowRegistersAndDrainsCompensation() throws Exception {
        FakeBank bank = new FakeBank();
        // Pre-populate two records under the same business_key.
        Map<String, Object> recA = new HashMap<>(); recA.put("wire_id", "wire-A");
        recA.put("business_key", "x:1:2026"); recA.put("amount", 1); recA.put("account", "x");
        Map<String, Object> recB = new HashMap<>(); recB.put("wire_id", "wire-B");
        recB.put("business_key", "x:1:2026"); recB.put("amount", 1); recB.put("account", "x");
        bank.ledger.put("x:1:2026", recA);
        bank.ledger.put("x:1:2026:dup", recB);

        BankConnector conn = new BankConnector(bank) {
            @Override public ObservationResult observe(EffectRecord effect) {
                return ObservationResult.duplicate("wire-B", "bank.wire");
            }
        };

        EffectRecord e = svc.beginEffect(
            "t", "u", "s", "inv-1", 0, "bank.wire", 0,
            "{\"amount\":1,\"account\":\"x\"}", null,
            EffectRecord.NON_IDEMPOTENT, EffectRecord.OUTBOX,
            "x:1:2026", "bank.wire");
        svc.recordDispatchAttempt("t", "u", "s", e.idempotencyKey(),
            "ack lost", 0L);

        // Reconcile — sees DUPLICATE, registers compensation atomically.
        List<Reactors.ReactorAction> r1 = Reactors.reconcileOnce(
            svc, Map.of("bank.wire", conn));
        assertTrue(r1.stream().anyMatch(a -> "duplicate".equals(a.outcome())),
            "expected duplicate outcome, got: " + r1);
        List<ObligationRecord> obs = svc.listObligations("t", "u", "s", true, null);
        assertEquals(1, obs.size());
        assertEquals("bank.wire", obs.get(0).kind());
        assertEquals(ObligationRecord.PENDING, obs.get(0).status());

        // Drain — compensates the duplicate.
        List<Reactors.ReactorAction> r2 = Reactors.drainObligationsOnce(
            svc, Map.of("bank.wire", conn), "dr-1");
        assertTrue(r2.stream().anyMatch(a -> "compensated".equals(a.outcome())),
            "expected compensated outcome, got: " + r2);
        EffectRecord eff = svc.getEffect("t", "u", "s", e.idempotencyKey()).orElseThrow();
        assertEquals(EffectRecord.CONFIRMED, eff.status());
    }

    @Test
    void fireDueTimersInvokesDispatcher() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        List<String> firedIds = new ArrayList<>();

        long now = System.currentTimeMillis();
        svc.setTimer("t", "u", "s", "t-1", now - 100L, "redrive", null);
        svc.setTimer("t", "u", "s", "t-future", now + 60_000L, "redrive", null);

        List<Reactors.ReactorAction> r = Reactors.fireDueTimersOnce(svc, t -> {
            calls.incrementAndGet();
            firedIds.add(t.timerId());
        });
        assertEquals(List.of("t-1"), firedIds);
        assertTrue(r.stream().anyMatch(a -> "fired".equals(a.outcome())));
    }

    // ── decorator-equivalents (construction-time refusal) ──────────────────

    @Test
    void outboxToolsRejectsMissingConnector() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> OutboxTools.OutboxToolOpts.builder()
                .businessKey("k:1")
                .build());
        assertTrue(ex.getMessage().contains("connector"));
    }

    @Test
    void outboxToolsRejectsMissingBusinessKey() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> OutboxTools.OutboxToolOpts.builder()
                .connector("bank.wire")
                .build());
        assertTrue(ex.getMessage().contains("businessKey"));
    }

    @Test
    void outboxToolsAcceptsValid() {
        OutboxTools.OutboxToolOpts opts = OutboxTools.OutboxToolOpts.builder()
            .connector("bank.wire")
            .businessKey("x:1:2026")
            .compensate("reverse_wire")
            .build();
        OutboxTools.OutboxToolHandle handle = OutboxTools.declare(opts);
        assertEquals("bank.wire", handle.opts().connector());
        assertEquals("x:1:2026", handle.opts().resolveBusinessKey(Map.of()));
        assertEquals(EffectRecord.NON_IDEMPOTENT, handle.opts().semantics());
        assertEquals(EffectRecord.OUTBOX, handle.opts().dispatchMode());
    }

    @Test
    void effectsTrackedRunsBody() throws Exception {
        Effects.TrackedEffect<Integer> e = Effects.tracked(() -> 42);
        assertEquals(42, e.call());
        assertEquals(EffectRecord.IDEMPOTENT, e.semantics());
        assertEquals(EffectRecord.INLINE, e.dispatchMode());
    }
}
