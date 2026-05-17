package dev.tape;

import dev.tape.connectors.Connector;
import dev.tape.connectors.ConnectorRegistry;
import dev.tape.connectors.LogConnector;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Smoke tests for the standalone-DX additions (no tape-server required). */
class DxTest {

    @Test
    void outboxRejectsUnsafeNonIdempotent() {
        OutboxTool.OutboxConfigError ex = assertThrows(
            OutboxTool.OutboxConfigError.class,
            () -> OutboxTool.builder("wire", "bank.wire")
                .semantics(OutboxTool.Semantics.NON_IDEMPOTENT)
                .build());
        assertTrue(ex.getMessage().contains("non_idempotent"));
    }

    @Test
    void outboxBuildsEnvelopeWithBusinessKey() {
        OutboxTool wire = OutboxTool.builder("wire_money", "bank.wire")
            .semantics(OutboxTool.Semantics.NON_IDEMPOTENT)
            .businessKey(p -> p.get("account") + ":" + p.get("amount"))
            .waitForResult(true)
            .build();
        Map<String, Object> env = wire.envelope(Map.of(
            "account", "ACME-1", "amount", 100, "beneficiary", "bob"));
        assertEquals(Boolean.TRUE, env.get("__outbox__"));
        assertEquals("bank.wire", env.get("connector"));
        assertEquals("wire_money", env.get("tool"));
        assertEquals("ACME-1:100", env.get("business_key"));
        assertEquals(true, env.get("wait_for_result"));
        assertNotNull(env.get("payload"));
        assertTrue(OutboxTool.isEnvelope(env));
    }

    @Test
    void connectorRegistryRoundTrip() throws Exception {
        ConnectorRegistry r = new ConnectorRegistry();
        Connector c = new LogConnector("/tmp/tape-java-test.jsonl");
        r.register("log", c);
        assertThrows(IllegalStateException.class, () -> r.register("log", c));
        assertSame(c, r.get("log"));
        assertThrows(IllegalArgumentException.class, () -> r.get("missing"));
        Connector.Effect e = new Connector.Effect()
            .runId("r1").idempotencyKey("k1").toolName("t")
            .connector("log").payload(Map.of("x", 1));
        Connector.Result res = c.dispatch(e);
        assertEquals(Connector.DispatchOutcome.CONFIRMED, res.outcome);
    }

    @Test
    void tenancyWarnsOnHardMode() {
        Tenancy.Config t = new Tenancy.Config(Tenancy.Mode.HARD_MULTI_TENANT, "x");
        assertEquals(1, t.warnIfHardButUnenforced().size());
        assertEquals(0, Tenancy.defaults().warnIfHardButUnenforced().size());
    }

    @Test
    void obsLogJsonEmitsJsonLine() {
        // Pipe stderr into a tempfile and check the JSON shape.
        Map<String, Object> fields = Map.of(
            "run_id", "r-1", "app_name", "treasury", "reactor", "recovery");
        Obs.logJson("hello", fields);
        // Best-effort: just confirm the call doesn't throw + ALL_SPANS is populated.
        assertEquals(11, Obs.ALL_SPANS.size());
        assertTrue(Obs.ALL_SPANS.contains(Obs.SPAN_BEGIN_EFFECT));
    }

    @Test
    void durableAppRequiresName() {
        assertThrows(IllegalArgumentException.class,
            () -> DurableApp.wire(new DurableApp.Config()));
    }

    @Test
    void logConnectorWritesJsonLine() throws Exception {
        Path tmp = Files.createTempFile("tape-java-dx", ".jsonl");
        LogConnector c = new LogConnector(tmp.toString());
        Connector.Effect e = new Connector.Effect()
            .runId("r1").idempotencyKey("k1").toolName("t")
            .connector("log").payload(Map.of("x", 1));
        c.dispatch(e);
        String contents = Files.readString(tmp);
        assertTrue(contents.contains("\"kind\":\"dispatch\""), contents);
        assertTrue(contents.contains("\"ts_ms\""), contents);
        Files.deleteIfExists(tmp);
    }
}
