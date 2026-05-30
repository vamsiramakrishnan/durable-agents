package dev.tape;

import dev.tape.proto.*;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Spawns the Rust tape-server (in-memory store) and round-trips the lifecycle. */
public class TapeClientTest {

    static Process server;
    static String url;
    static int port;

    @BeforeAll
    static void start() throws Exception {
        Path bin = Paths.get("../../server/target/debug/tape-server").toAbsolutePath().normalize();
        assumeTrue(Files.exists(bin), "tape-server not built (run `cargo build` in tape/server)");
        try (java.net.ServerSocket s = new java.net.ServerSocket(0, 1, java.net.InetAddress.getByName("127.0.0.1"))) {
            port = s.getLocalPort();
        }
        ProcessBuilder pb = new ProcessBuilder(bin.toString(),
                "--listen", "127.0.0.1:" + port, "--store", "memory")
                .redirectOutput(new File("/dev/null"))
                .redirectError(new File("/dev/null"));
        pb.environment().put("RUST_LOG", "tape_server=warn");
        server = pb.start();
        url = "tape://127.0.0.1:" + port;
        waitFor("127.0.0.1", port, 15_000);
    }

    @AfterAll
    static void stop() throws InterruptedException {
        if (server != null) {
            server.destroy();
            server.waitFor(5, TimeUnit.SECONDS);
        }
    }

    static void waitFor(String host, int port, int timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket()) { s.connect(new InetSocketAddress(host, port), 1000); return; }
            catch (IOException ignored) {}
            try { Thread.sleep(100); } catch (InterruptedException e) { return; }
        }
        fail("server never came up at " + host + ":" + port);
    }

    @Test
    void roundTripsLifecycle() throws Exception {
        try (TapeClient c = new TapeClient(url)) {
            BeginRunResponse r = c.beginRun("a", "u", "java-smoke", "inv-java", "test", 60_000);
            assertFalse(r.getResumed());
            String rid = r.getRunId();

            c.recordDecision(rid, 0, "m", "{}", "{\"plan\":1}", "", "p1");
            assertTrue(c.getDecision(rid, 0).getFound());

            BeginEffectResponse be = c.beginEffect(rid, 0, "execute_sweep", 0, "{}", "");
            assertEquals(EffectStatus.EFFECT_STATUS_PENDING, be.getStatus());
            assertEquals(rid + "/decision-0/execute_sweep/0", be.getIdempotencyKey());

            BeginEffectResponse be2 = c.beginEffect(rid, 0, "execute_sweep", 0, "{}", "");
            assertEquals(be.getIdempotencyKey(), be2.getIdempotencyKey());

            c.completeEffect(rid, be.getIdempotencyKey(), EffectStatus.EFFECT_STATUS_CONFIRMED,
                    "{\"wire_id\":\"w1\"}", "");
            GetEffectResponse ge = c.getEffect(rid, be.getIdempotencyKey());
            assertTrue(ge.getFound());
            assertEquals(EffectStatus.EFFECT_STATUS_CONFIRMED, ge.getEffect().getStatus());

            c.registerCompensation(rid, be.getIdempotencyKey(), "reverse_wire", "{}");
            ListObligationsResponse obs = c.listObligations(rid, true);
            assertEquals(1, obs.getObligationsCount());

            c.setBudget(rid, 1.0, 0);
            assertTrue(c.admitBudget(rid, 0.5, 0).getAdmitted());
            c.chargeBudget(rid, 0.9, 0);
            assertFalse(c.admitBudget(rid, 0.5, 0).getAdmitted());

            TimerRecord tr = c.setTimer(rid, "", System.currentTimeMillis() - 1000,
                    "gate_timeout", "{\"gate\":\"g1\"}");
            ListDueTimersResponse due = c.listDueTimers(0, 50, true);
            boolean found = false;
            for (TimerRecord t : due.getTimersList()) {
                if (t.getTimerId().equals(tr.getTimerId())) { found = true; break; }
            }
            assertTrue(found, "timer not in due list");

            c.endRun(rid, RunStatus.RUN_STATUS_TERMINAL, "");
            assertEquals(RunStatus.RUN_STATUS_TERMINAL, c.getRun(rid).getStatus());

            BeginRunResponse again = c.beginRun("a", "u", "java-smoke", "inv-java", "test", 60_000);
            assertTrue(again.getResumed());
            assertEquals(rid, again.getRunId());
            assertEquals(RunStatus.RUN_STATUS_TERMINAL, again.getStatus());
        }
    }

    @Test
    void roundTripsOutboxContract() throws Exception {
        try (TapeClient c = new TapeClient(url)) {
            // PR 12: the server's wire-level scope enforcement refuses
            // non-idempotent effects without a declared scope. Grant
            // the scope on BeginRun and declare it on BeginEffect.
            RunIdentity ident = new RunIdentity("", "", "", "", "", "",
                    java.util.List.of("tape:tools:wire_money"),
                    java.util.Collections.emptyMap());
            BeginRunResponse r = c.beginRun("a", "u", "java-outbox", "inv-java-outbox",
                    "test", 60_000, ident);
            String rid = r.getRunId();

            // Server refuses NON_IDEMPOTENT + INLINE.
            assertThrows(io.grpc.StatusRuntimeException.class, () ->
                c.beginEffect(rid, -1, "wire_money", 0, "{}", "",
                        EffectSemantics.EFFECT_SEMANTICS_NON_IDEMPOTENT,
                        EffectDispatchMode.EFFECT_DISPATCH_MODE_INLINE, "", "",
                        "tape:tools:wire_money"));

            // NON_IDEMPOTENT + OUTBOX + business_key is accepted.
            BeginEffectResponse oe = c.beginEffect(rid, -1, "wire_money", 0,
                    "{\"amount\":100}", "",
                    EffectSemantics.EFFECT_SEMANTICS_NON_IDEMPOTENT,
                    EffectDispatchMode.EFFECT_DISPATCH_MODE_OUTBOX,
                    "java:bk-1", "bank.wire",
                    "tape:tools:wire_money");
            assertEquals(EffectStatus.EFFECT_STATUS_PENDING, oe.getStatus());

            // Visible to the outbox dispatcher.
            ListEffectsToDispatchResponse list = c.listEffectsToDispatch("bank.wire", 50, 0);
            boolean seen = false;
            for (EffectRecord e : list.getEffectsList()) {
                if (e.getIdempotencyKey().equals(oe.getIdempotencyKey())) { seen = true; break; }
            }
            assertTrue(seen, "effect not in dispatch list");

            // CAS lease: only one winner.
            ClaimEffectDispatchResponse cl1 = c.claimEffectDispatch(rid, oe.getIdempotencyKey(), "java-A", 60_000);
            ClaimEffectDispatchResponse cl2 = c.claimEffectDispatch(rid, oe.getIdempotencyKey(), "java-B", 60_000);
            assertTrue(cl1.getAcquired());
            assertFalse(cl2.getAcquired());

            // Lost ack → UNKNOWN (no retry).
            c.recordDispatchAttempt(rid, oe.getIdempotencyKey(), "simulated lost ack", 0);
            assertEquals(EffectStatus.EFFECT_STATUS_UNKNOWN,
                    c.getEffect(rid, oe.getIdempotencyKey()).getEffect().getStatus());

            // Reconciler observes ABSENT — NON_IDEMPOTENT → FAILED.
            c.recordExternalObservation(rid, oe.getIdempotencyKey(),
                    EffectResolution.EFFECT_RESOLUTION_ABSENT, "", "", "", "");
            assertEquals(EffectStatus.EFFECT_STATUS_FAILED,
                    c.getEffect(rid, oe.getIdempotencyKey()).getEffect().getStatus());
        }
    }
}
