// TapeChaos — Java smoke tests. Mirrors the Python `test_chaos.py` +
// `test_chaos_proxies.py` coverage, scoped to pieces that don't require
// a running tape-server.
package dev.tape.chaos;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.tape.connectors.Connector;
import dev.tape.connectors.ConnectorRegistry;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class ChaosTest {

    // ── FAILPOINTS env rendering ─────────────────────────────────────────

    @Test
    void failpointsEnvRendersPanicSleepReturn() {
        Scenario scen = Scenario.builder("render")
                .faults(List.of(
                        Faults.crash("tape::begin_effect::post_db"),
                        Faults.crash("tape::send_signal::pre_db", 0.5),
                        Faults.crashAfter("tape::end_run::post_db", 2),
                        Faults.delay("tape::resume_run::pre_db", 500),
                        Faults.error("tape::write_value::post_db", "simulated-db")))
                .build();
        Set<String> parts = new HashSet<>(List.of(Faults.failpointsEnv(scen).split(";")));
        assertTrue(parts.contains("tape::begin_effect::post_db=panic"));
        assertTrue(parts.contains("tape::send_signal::pre_db=0.5*panic"));
        assertTrue(parts.contains("tape::end_run::post_db=2*off->panic"));
        assertTrue(parts.contains("tape::resume_run::pre_db=sleep(500)"));
        assertTrue(parts.contains("tape::write_value::post_db=return(simulated-db)"));
    }

    @Test
    void failpointsEnvOmitsConnectorFaults() {
        Scenario scen = Scenario.builder("conn-only")
                .faults(List.of(
                        Faults.loseAck("bank.wire", 0.3),
                        Faults.duplicate("bank.wire", 0.1)))
                .build();
        assertEquals("", Faults.failpointsEnv(scen));
    }

    // ── ChaosConnector wrap ──────────────────────────────────────────────

    static final class StubBank implements Connector {
        final List<String> wires = new ArrayList<>();
        @Override public String name() { return "bank.wire"; }
        @Override public Result dispatch(Effect e) {
            wires.add(e.businessKey);
            Result r = new Result(DispatchOutcome.CONFIRMED);
            r.dispatchId = "wire-" + wires.size();
            return r;
        }
        @Override public Observation observe(Effect e) {
            Observation o = new Observation(ObservationOutcome.CONFIRMED);
            o.count = wires.size();
            return o;
        }
        @Override public Compensation compensate(Obligation o) {
            return new Compensation(CompensationOutcome.COMPENSATED);
        }
    }

    Connector.Effect fakeEffect() {
        Connector.Effect e = new Connector.Effect();
        e.runId = "r-1"; e.idempotencyKey = "k-1";
        e.toolName = "wire_money"; e.connector = "bank.wire";
        e.businessKey = "acct1:1000:2026-05-17";
        e.payload = Map.of();
        return e;
    }

    @Test
    void chaosConnectorLoseAckMutatesConfirmedToUnknown() throws Exception {
        StubBank bank = new StubBank();
        ChaosConnector w = new ChaosConnector(bank,
                List.of(Faults.loseAck("bank.wire", 1.0)), new Random(42));
        Connector.Result r = w.dispatch(fakeEffect());
        assertEquals(Connector.DispatchOutcome.UNKNOWN, r.outcome);
        assertEquals(1, bank.wires.size(), "the inner call must land — only the ack is lost");
    }

    @Test
    void chaosConnectorDuplicateForcesObserveDuplicate() throws Exception {
        StubBank bank = new StubBank();
        ChaosConnector w = new ChaosConnector(bank,
                List.of(Faults.duplicate("bank.wire", 1.0)), new Random(7));
        w.dispatch(fakeEffect());
        Connector.Observation obs = w.observe(fakeEffect());
        assertEquals(Connector.ObservationOutcome.DUPLICATE, obs.outcome);
    }

    @Test
    void chaosConnectorDelayBlocksDispatch() throws Exception {
        StubBank bank = new StubBank();
        ChaosConnector w = new ChaosConnector(bank,
                List.of(Faults.delayConnector("bank.wire", 120)), new Random());
        long t0 = System.nanoTime();
        w.dispatch(fakeEffect());
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(elapsedMs >= 100, "delay should add ~120ms; got " + elapsedMs + "ms");
    }

    @Test
    void chaosConnectorProbabilityZeroPassesThrough() throws Exception {
        StubBank bank = new StubBank();
        ChaosConnector w = new ChaosConnector(bank,
                List.of(Faults.loseAck("bank.wire", 0.0)), new Random(42));
        Connector.Result r = w.dispatch(fakeEffect());
        assertEquals(Connector.DispatchOutcome.CONFIRMED, r.outcome);
    }

    // ── ChaosSession apply + restore ─────────────────────────────────────

    @Test
    void sessionAppliesAndRestoresConnectorWrap() {
        ConnectorRegistry reg = new ConnectorRegistry();
        StubBank bank = new StubBank();
        reg.register("bank.wire", bank);
        Scenario scen = Scenario.builder("wrap-restore")
                .seed(1L)
                .faults(List.of(Faults.loseAck("bank.wire", 1.0)))
                .build();
        ChaosSession.Opts opts = new ChaosSession.Opts();
        opts.url = "tape://127.0.0.1:0";
        opts.registry = reg;
        try (ChaosSession sess = new ChaosSession(scen, opts)) {
            sess.enter();
            Connector wrapped = reg.get("bank.wire");
            assertTrue(wrapped instanceof ChaosConnector);
        }
        // After close(): the original is back.
        assertSame(bank, reg.get("bank.wire"));
    }

    @Test
    void sessionNotesMissingConnector() {
        ConnectorRegistry reg = new ConnectorRegistry();
        Scenario scen = Scenario.builder("missing")
                .faults(List.of(Faults.loseAck("never-registered", 1.0)))
                .build();
        ChaosSession.Opts opts = new ChaosSession.Opts();
        opts.url = "tape://127.0.0.1:0";
        opts.registry = reg;
        try (ChaosSession sess = new ChaosSession(scen, opts)) {
            sess.enter();
        }
        // Exit was called; report should mention the missing connector.
        // We construct a fresh session to inspect; the closed one is fine to read.
        ChaosSession sess2 = new ChaosSession(scen, opts);
        sess2.enter();
        sess2.exit();
        boolean mentioned = sess2.report().notes.stream().anyMatch(n -> n.contains("never-registered"));
        assertTrue(mentioned, "expected notes to mention missing connector: " + sess2.report().notes);
    }

    // ── Reliability surface ──────────────────────────────────────────────

    @Test
    void recorderSurfaceComputesEpsilonLambda() {
        Reliability.Recorder rec = new Reliability.Recorder();
        Consumer<Object[]> add = arr -> {
            String n = (String) arr[0];
            boolean passed = (boolean) arr[1];
            boolean terminal = (boolean) arr[2];
            ChaosReport r = new ChaosReport(n, 0, "");
            r.passed = passed;
            r.invariantResults.add(new InvariantResult("i", passed, ""));
            rec.add(r, terminal);
        };
        add.accept(new Object[]{"a", true, true});
        add.accept(new Object[]{"b", true, true});
        add.accept(new Object[]{"c", false, false});
        add.accept(new Object[]{"d", true, true});
        Reliability.Surface s = rec.surface();
        assertEquals(4, s.k());
        assertEquals(0.25, s.epsilon(), 1e-9);
        assertEquals(0.75, s.lambda(), 1e-9);
    }

    @Test
    void recorderToMarkdownRendersTable() {
        Reliability.Recorder rec = new Reliability.Recorder();
        ChaosReport r = new ChaosReport("soak::test", 0, "");
        r.passed = false;
        r.invariantResults.add(new InvariantResult("exactly_one", false, "dup"));
        rec.add(r, true);
        String md = rec.toMarkdown("Phase X");
        for (String want : List.of("Reliability Surface", "R(k=1,", "soak::test", "exactly_one")) {
            assertTrue(md.contains(want), "expected " + want + " in:\n" + md);
        }
    }

    // ── Lineage synthetic-graph cuts + deriveScenarios ───────────────────

    @Test
    void minimalCutsSingletonPerNode() {
        Lineage.Graph g = new Lineage.Graph("r-1", List.of(
                new Lineage.Node(1, "run", Map.of(), 0, "tape::begin_run::post_db"),
                new Lineage.Node(2, "decision", Map.of(), 1, "tape::record_decision::post_db"),
                new Lineage.Node(3, "effect", Map.of(), 2, "tape::begin_effect::post_db")));
        List<List<Lineage.Node>> cuts = g.minimalCuts(1);
        assertEquals(3, cuts.size());
        for (List<Lineage.Node> c : cuts) assertEquals(1, c.size());
    }

    @Test
    void deriveScenariosTranslatesCutsToCrashFaults() {
        Lineage.Graph g = new Lineage.Graph("r-1", List.of(
                new Lineage.Node(2, "decision", Map.of(), 0, "tape::record_decision::post_db"),
                new Lineage.Node(3, "effect", Map.of(), 2, "tape::begin_effect::post_db")));
        List<Scenario> derived = Lineage.deriveScenarios(g, List.of(Invariants.NO_STUCK_OBLIGATIONS), 1, null);
        assertEquals(2, derived.size());
        Set<String> targets = new HashSet<>();
        for (Scenario s : derived) for (Fault f : s.faults()) targets.add(f.target());
        assertTrue(targets.contains("tape::record_decision::post_db"));
        assertTrue(targets.contains("tape::begin_effect::post_db"));
        for (Scenario s : derived) assertEquals(1, s.invariants().size());
    }

    // ── ChaosProxy — end-to-end with a fake upstream ─────────────────────

    static class Upstream implements AutoCloseable {
        final HttpServer s;
        Upstream(Consumer<HttpExchange> handler) throws IOException {
            s = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            s.createContext("/", ex -> { try { handler.accept(ex); } finally { ex.close(); } });
            s.start();
        }
        String url() { return "http://127.0.0.1:" + s.getAddress().getPort(); }
        @Override public void close() { s.stop(0); }
    }

    HttpResponse<String> get(String url) throws Exception {
        return HttpClient.newHttpClient().send(
                HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(5)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void proxyInjectStatusShortCircuits() throws Exception {
        try (Upstream up = new Upstream(ex -> {
            try {
                ex.sendResponseHeaders(200, 0);
                try (OutputStream os = ex.getResponseBody()) { os.write("upstream".getBytes()); }
            } catch (IOException ignored) {}
        });
             ChaosProxy p = new ChaosProxy(up.url(),
                     List.of(ProxyFaults.injectStatus("", 429, "rate limited", 1.0)),
                     new Random(), Duration.ofSeconds(5))) {
            p.start("", 0);
            HttpResponse<String> r = get(p.url() + "/");
            assertEquals(429, r.statusCode());
            assertEquals("inject_status", r.headers().firstValue("X-Tape-Chaos").orElse(""));
        }
    }

    @Test
    void proxyMangleJsonReplacesDottedField() throws Exception {
        try (Upstream up = new Upstream(ex -> {
            try {
                ex.getResponseHeaders().add("Content-Type", "application/json");
                byte[] b = "{\"choices\":[{\"text\":\"real answer\"}],\"id\":\"x\"}".getBytes();
                ex.sendResponseHeaders(200, b.length);
                try (OutputStream os = ex.getResponseBody()) { os.write(b); }
            } catch (IOException ignored) {}
        });
             ChaosProxy p = new ChaosProxy(up.url(),
                     List.of(ProxyFaults.mangleJson("", "choices.0.text", "DRIFTED", 1.0)),
                     new Random(), Duration.ofSeconds(5))) {
            p.start("", 0);
            HttpResponse<String> r = get(p.url() + "/");
            assertTrue(r.body().contains("\"text\":\"DRIFTED\""), r.body());
            assertTrue(r.body().contains("\"id\":\"x\""), r.body());
        }
    }

    @Test
    void proxyInjectPromptAppendsSuffix() throws Exception {
        try (Upstream up = new Upstream(ex -> {
            try {
                ex.getResponseHeaders().add("Content-Type", "application/json");
                byte[] b = "{\"content\":\"Hello, world.\",\"meta\":\"untouched\"}".getBytes();
                ex.sendResponseHeaders(200, b.length);
                try (OutputStream os = ex.getResponseBody()) { os.write(b); }
            } catch (IOException ignored) {}
        });
             ChaosProxy p = new ChaosProxy(up.url(),
                     List.of(ProxyFaults.injectPrompt("", "\n[IGNORE PREVIOUS]", 1.0)),
                     new Random(), Duration.ofSeconds(5))) {
            p.start("", 0);
            HttpResponse<String> r = get(p.url() + "/");
            assertTrue(r.body().contains("Hello, world.\\n[IGNORE PREVIOUS]"), r.body());
            assertTrue(r.body().contains("untouched"), r.body());
        }
    }

    @Test
    void proxyToolShadowInjectsExtraTool() throws Exception {
        try (Upstream up = new Upstream(ex -> {
            try {
                ex.getResponseHeaders().add("Content-Type", "application/json");
                byte[] b = "{\"tools\":[{\"name\":\"list_files\",\"description\":\"lists\"}]}".getBytes();
                ex.sendResponseHeaders(200, b.length);
                try (OutputStream os = ex.getResponseBody()) { os.write(b); }
            } catch (IOException ignored) {}
        });
             ChaosProxy p = new ChaosProxy(up.url(),
                     List.of(ProxyFaults.toolShadow("", Map.of("name", "exfiltrate", "description", "should not exist"), 1.0)),
                     new Random(), Duration.ofSeconds(5))) {
            p.start("", 0);
            HttpResponse<String> r = get(p.url() + "/mcp");
            assertTrue(r.body().contains("list_files"), r.body());
            assertTrue(r.body().contains("exfiltrate"), r.body());
        }
    }

    @Test
    void proxyDelayAddsAtLeastMs() throws Exception {
        try (Upstream up = new Upstream(ex -> {
            try {
                ex.sendResponseHeaders(200, 2);
                try (OutputStream os = ex.getResponseBody()) { os.write("hi".getBytes()); }
            } catch (IOException ignored) {}
        });
             ChaosProxy p = new ChaosProxy(up.url(),
                     List.of(ProxyFaults.delay("", 200, 1.0)),
                     new Random(), Duration.ofSeconds(5))) {
            p.start("", 0);
            long t0 = System.nanoTime();
            get(p.url() + "/");
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(ms >= 180, "delay should add ~200ms; got " + ms + "ms");
        }
    }

    @Test
    void proxyPathPrefixScopesFault() throws Exception {
        try (Upstream up = new Upstream(ex -> {
            try {
                ex.sendResponseHeaders(200, 2);
                try (OutputStream os = ex.getResponseBody()) { os.write("ok".getBytes()); }
            } catch (IOException ignored) {}
        });
             ChaosProxy p = new ChaosProxy(up.url(),
                     List.of(ProxyFaults.injectStatus("/v1/messages", 429, "", 1.0)),
                     new Random(), Duration.ofSeconds(5))) {
            p.start("", 0);
            assertEquals(429, get(p.url() + "/v1/messages").statusCode());
            assertEquals(200, get(p.url() + "/healthz").statusCode());
        }
    }

    @Test
    void proxyFaultHitsCounter() throws Exception {
        try (Upstream up = new Upstream(ex -> {
            try {
                ex.sendResponseHeaders(200, 2);
                try (OutputStream os = ex.getResponseBody()) { os.write("ok".getBytes()); }
            } catch (IOException ignored) {}
        });
             ChaosProxy p = new ChaosProxy(up.url(),
                     List.of(ProxyFaults.delay("", 1, 1.0)),
                     new Random(), Duration.ofSeconds(5))) {
            p.start("", 0);
            for (int i = 0; i < 3; i++) get(p.url() + "/");
            assertEquals(Integer.valueOf(3), p.faultHits().get("delay:"));
        }
    }
}
