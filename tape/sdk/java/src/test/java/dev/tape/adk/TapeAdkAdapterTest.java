package dev.tape.adk;

import com.google.adk.events.Event;
import com.google.adk.sessions.ListSessionsResponse;
import com.google.adk.sessions.Session;
import com.google.adk.sessions.State;
import dev.tape.TapeClient;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.*;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * G4 smoke: verifies the ADK adapter contract by exercising
 * {@link TapeSessionService} against a real Rust tape-server.
 *
 * <p>We don't spin up a full ADK Runner here — that would pull in a model
 * dependency. The session service is the seam that gives single-transaction
 * atomicity (event + state delta + journal projection in one commit), so
 * proving that round-trip holds is the load-bearing test for G4.
 */
public class TapeAdkAdapterTest {

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
        if (server != null) { server.destroy(); server.waitFor(5, TimeUnit.SECONDS); }
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
    void sessionRoundTrip() throws Exception {
        try (TapeClient client = new TapeClient(url)) {
            TapeSessionService svc = new TapeSessionService(client);

            ConcurrentHashMap<String, Object> state = new ConcurrentHashMap<>();
            state.put("ticker", "ACME");

            Session created = svc.createSession("treasury", "cfo", state, "")
                    .blockingGet();
            assertNotNull(created);
            assertEquals("treasury", created.appName());
            assertEquals("cfo",      created.userId());
            assertNotNull(created.id());
            assertFalse(created.id().isEmpty());
            assertEquals("ACME", created.state().get("ticker"));

            // get round-trips
            Session fetched = svc.getSession("treasury", "cfo", created.id(), Optional.empty())
                    .blockingGet();
            assertNotNull(fetched);
            assertEquals(created.id(), fetched.id());
            assertEquals("ACME",       fetched.state().get("ticker"));

            // list includes it
            ListSessionsResponse list = svc.listSessions("treasury", "cfo").blockingGet();
            assertTrue(list.sessions().stream().anyMatch(s -> s.id().equals(created.id())));

            // appendEvent persists (committed events only — partials stay in memory)
            String json = String.format(
                "{\"id\":\"%s\",\"invocationId\":\"%s\",\"author\":\"%s\",\"timestamp\":%d}",
                "ev-1", "inv-1", "user", System.currentTimeMillis() / 1000);
            Event ev = Event.fromJsonString(json, Event.class);
            Event appended = svc.appendEvent(fetched, ev).blockingGet();
            assertNotNull(appended);

            Session afterEvent = svc.getSession("treasury", "cfo", created.id(), Optional.empty())
                    .blockingGet();
            assertNotNull(afterEvent);
            // The Tape server stores events with a stable id; the round-trip
            // should include our event.
            assertTrue(afterEvent.events().stream().anyMatch(e -> "ev-1".equals(e.id())),
                       "appended event should be persisted and returned by getSession");

            // delete is a no-throw completable
            svc.deleteSession("treasury", "cfo", created.id()).blockingAwait();

            Session gone = svc.getSession("treasury", "cfo", created.id(), Optional.empty())
                    .blockingGet();
            assertNull(gone, "deleted session should be absent");
        }
    }

    @Test
    void pluginConstructionRoundtrips() {
        try (TapeClient client = new TapeClient(url)) {
            TapePlugin plugin = new TapePlugin(client);
            assertEquals("tape", plugin.getName());
            assertSame(client, plugin.client());
        }
    }

    @Test
    void tapeAdkAppWiresBothEndpoints() {
        try (TapeAdkApp app = TapeAdkApp.wire(
                new dev.tape.DurableApp.Config().name("treasury").tapeUrl(url))) {
            assertNotNull(app.plugin());
            assertNotNull(app.sessionService());
            assertEquals("tape", app.plugin().getName());
            assertSame(app.client(), app.plugin().client());
            assertSame(app.client(), app.sessionService().client());
        }
    }
}
