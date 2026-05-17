package dev.tape;

import dev.tape.proto.*;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.*;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Reactions / tasks / SubscribeBySubject round-trip against the live Rust server.
 *
 * <p>The Python equivalents use a function-scoped fixture (one fresh server per
 * test). We mirror that here — accumulating reactions across tests confuses the
 * matcher's batched-cursor traversal under load. */
public class ReactionsTest {

    Process server;
    String url;
    int port;

    static Path serverBin;

    @BeforeAll
    static void checkBinary() {
        serverBin = Paths.get("../../server/target/debug/tape-server").toAbsolutePath().normalize();
        assumeTrue(Files.exists(serverBin), "tape-server not built (run `cargo build` in tape/server)");
    }

    @BeforeEach
    void start() throws Exception {
        Reactions.clearRegistry();
        try (java.net.ServerSocket s = new java.net.ServerSocket(0, 1,
                java.net.InetAddress.getByName("127.0.0.1"))) {
            port = s.getLocalPort();
        }
        ProcessBuilder pb = new ProcessBuilder(serverBin.toString(),
                "--listen", "127.0.0.1:" + port, "--store", "memory")
                .redirectOutput(new File("/dev/null"))
                .redirectError(new File("/dev/null"));
        pb.environment().put("RUST_LOG", "tape_server=warn");
        server = pb.start();
        url = "tape://127.0.0.1:" + port;
        waitFor("127.0.0.1", port, 15_000);
    }

    @AfterEach
    void stop() throws InterruptedException {
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

    /** Block until {@code probe} returns a non-null/non-empty value, or fail. */
    static <T> T waitFor(Supplier<T> probe, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                T t = probe.get();
                if (t == null) { /* keep polling */ }
                else if (t instanceof java.util.Collection && ((java.util.Collection<?>) t).isEmpty()) { /* keep */ }
                else return t;
            } catch (Throwable ignored) {}
            try { Thread.sleep(50); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return null; }
        }
        return null;
    }

    // ── 1. New client methods round-trip via the live server ────────────────

    @Test
    void clientReactionRpcsRoundTrip() {
        try (TapeClient c = new TapeClient(url)) {
            String stableId = "java-rt-" + UUID.randomUUID().toString().substring(0, 6);
            Reaction r = c.registerReaction(new RegisterReactionOpts()
                    .reactionId(stableId)
                    .name("client-roundtrip")
                    .subjectPattern("/tape/value/changed/java-rt/**")
                    .handlerKind(HandlerKind.HANDLER_KIND_TASK)
                    .maxConcurrency(2));
            assertEquals(stableId, r.getReactionId());
            assertEquals("/tape/value/changed/java-rt/**", r.getSubjectPattern());
            assertEquals(HandlerKind.HANDLER_KIND_TASK, r.getHandlerKind());

            // ListReactions: filter by pattern prefix.
            List<Reaction> all = c.listReactions("");
            assertTrue(all.stream().anyMatch(x -> x.getReactionId().equals(stableId)),
                    "list_reactions('') should include our reaction");
            List<Reaction> filtered = c.listReactions("/tape/value/changed/java-rt/**");
            assertTrue(filtered.stream().anyMatch(x -> x.getReactionId().equals(stableId)),
                    "list_reactions(pattern) should still include our reaction");

            // Trigger a match, then claim + complete the resulting task.
            c.pb().writeValue(WriteValueRequest.newBuilder()
                    .setNamespace("java-rt").setKey("k1").setValueJson("1").setIfVersion(-1).setWriter("t").build());

            List<Task> tasks = waitFor(
                    () -> c.listTasks(stableId, TaskStatus.TASK_STATUS_UNSPECIFIED, 100),
                    5_000);
            assertNotNull(tasks, "matcher should have created a task");
            assertFalse(tasks.isEmpty(), "expected >=1 task");

            String owner = "java-owner";
            List<Task> claimed = c.claimTasks(new ClaimTasksOpts()
                    .reactionId(stableId).owner(owner).max(10));
            assertFalse(claimed.isEmpty(), "claim should return >=1 task");
            String taskId = claimed.get(0).getTaskId();

            Task done = c.completeTask(taskId, owner);
            assertEquals(TaskStatus.TASK_STATUS_DONE, done.getStatus());

            // Deregister.
            assertTrue(c.deregisterReaction(stableId), "deregister should return true");
            assertFalse(c.listReactions("").stream().anyMatch(x ->
                    x.getReactionId().equals(stableId) && !x.getDeleted()),
                    "post-deregister, the reaction must not appear as live");
        }
    }

    // ── 2. A task reaction's handler runs via runDispatcher ────────────────

    @Test
    void dispatcherRunsTaskHandlerAndCompletesTask() throws Exception {
        CopyOnWriteArrayList<String> seenSubjects = new CopyOnWriteArrayList<>();

        Reactions.ReactionDef def = new Reactions.ReactionDef();
        def.subjectPattern = "/tape/value/changed/java-disp/**";
        def.name = "java-disp-handler";
        def.maxConcurrency = 2;
        def.handler = env -> seenSubjects.add(env.subject());
        Reactions.on(def);

        try (TapeClient c = new TapeClient(url)) {
            List<Reaction> rs = Reactions.registerAll(c, "t" + UUID.randomUUID().toString().substring(0, 4) + "-");
            assertEquals(1, rs.size());
            String rid = rs.get(0).getReactionId();

            // Trigger.
            c.pb().writeValue(WriteValueRequest.newBuilder()
                    .setNamespace("java-disp").setKey("k1").setValueJson("{\"v\":1}").setIfVersion(-1).setWriter("t").build());

            // Wait for the matcher to create a task.
            List<Task> pending = waitFor(
                    () -> c.listTasks(rid, TaskStatus.TASK_STATUS_UNSPECIFIED, 10), 5_000);
            assertNotNull(pending, "task should exist");

            // Run one dispatcher pass.
            Reactions.runDispatcher(c, new Reactions.RunDispatcherOpts()
                    .once(true).register(false).pollInterval(Duration.ofMillis(50)));

            // Wait for the handler to have run.
            for (int i = 0; i < 80 && seenSubjects.isEmpty(); i++) Thread.sleep(50);
            assertFalse(seenSubjects.isEmpty(), "handler must have been invoked");
            assertTrue(seenSubjects.get(0).startsWith("/tape/value/changed/java-disp/"),
                    "subject must match pattern; got " + seenSubjects.get(0));

            // Task should be DONE.
            List<Task> done = waitFor(
                    () -> c.listTasks(rid, TaskStatus.TASK_STATUS_DONE, 10), 5_000);
            assertNotNull(done, "task should reach DONE");
            assertFalse(done.isEmpty());
        }
    }

    // ── 3. bootstrap_from_head=true skips the backlog ──────────────────────

    @Test
    void bootstrapFromHeadSkipsBacklog() throws Exception {
        // What this verifies: when a reaction is registered with
        // `bootstrap_from_head=true` AFTER N journal entries already exist,
        // those N entries must NOT produce tasks — the cursor is seeded at
        // head, so the reaction only sees what happens going forward. We
        // assert the backlog-skip contract (the post-registration delivery
        // path is exercised by `dispatcherRunsTaskHandlerAndCompletesTask`
        // and `clientReactionRpcsRoundTrip`, which both register reactions
        // with `bootstrap_from_head=false`).
        String ns = "java-boot-" + UUID.randomUUID().toString().substring(0, 6);
        try (TapeClient c = new TapeClient(url)) {
            // Pre-write some entries — these form the backlog the reaction will skip.
            for (int i = 0; i < 5; i++) {
                c.pb().writeValue(WriteValueRequest.newBuilder()
                        .setNamespace(ns).setKey("k").setValueJson(String.valueOf(i)).setIfVersion(-1).setWriter("t").build());
            }

            String rid = "java-boot-r-" + UUID.randomUUID().toString().substring(0, 6);
            Reaction r = c.registerReaction(new RegisterReactionOpts()
                    .reactionId(rid)
                    .name("bootstrap-head")
                    .subjectPattern("/tape/value/changed/" + ns + "/**")
                    .handlerKind(HandlerKind.HANDLER_KIND_TASK)
                    .bootstrapFromHead(true));
            assertEquals(rid, r.getReactionId());
            // bootstrap_from_head is a registration-time intent (seeds the
            // per-shard cursor at head); the server intentionally does NOT
            // echo the flag back on the persisted Reaction row.

            // Give the matcher a moment; with bootstrap_from_head=true it must NOT
            // pick up the 5 pre-existing entries.
            Thread.sleep(1_500);
            List<Task> backlog = c.listTasks(rid, TaskStatus.TASK_STATUS_UNSPECIFIED, 100);
            assertEquals(0, backlog.size(),
                    "bootstrap_from_head must skip pre-existing entries; got: "
                            + backlog.stream().map(Task::getSubject).toList());
        }
    }

    // ── 4. subscribeBySubject filters by pattern ───────────────────────────

    @Test
    void subscribeBySubjectFiltersByPattern() throws Exception {
        // We drive matching journal entries via `RecordDecision` (each
        // decision becomes a /tape/decision/recorded/<run_id>/<idx> row).
        // Avoiding `WriteValue` here: every value journal entry currently
        // pins (run_id="", seq=0), so a second value write silently fails
        // the journal insert — making it unusable for "stream multiple
        // matching events" tests. Decisions are per-run-sequenced, so they
        // don't collide.
        String app = "java-sub-" + UUID.randomUUID().toString().substring(0, 6);
        try (TapeClient writer = new TapeClient(url);
             TapeClient reader = new TapeClient(url)) {

            // Run 1 + two decisions → two matching journal entries.
            BeginRunResponse r1 = writer.beginRun(app, "u", "s", "inv-1", "owner", 60_000);
            writer.recordDecision(r1.getRunId(), 0, "m", "{}", "{}", "", "p1");
            writer.recordDecision(r1.getRunId(), 1, "m", "{}", "{}", "", "p1");
            // Run 2 + one decision under a DIFFERENT app → non-matching
            // subject (pattern is scoped to the first run's run_id).
            BeginRunResponse r2 = writer.beginRun(app + "-other", "u", "s", "inv-2", "owner", 60_000);
            writer.recordDecision(r2.getRunId(), 0, "m", "{}", "{}", "", "p1");

            // Pattern matches decisions for run_id=r1 only.
            String pattern = "/tape/decision/recorded/" + r1.getRunId() + "/**";

            CopyOnWriteArrayList<EventEntry> hits = new CopyOnWriteArrayList<>();
            Thread t = new Thread(() -> {
                try {
                    java.util.Iterator<EventEntry> it = reader.subscribeBySubject(pattern, "", 0);
                    while (it.hasNext() && hits.size() < 5) {
                        hits.add(it.next());
                    }
                } catch (Throwable ignored) {
                    // CANCELLED on cleanup is expected.
                }
            }, "sub-reader");
            t.setDaemon(true);
            t.start();

            long deadline = System.currentTimeMillis() + 8_000;
            while (System.currentTimeMillis() < deadline && hits.size() < 2) {
                Thread.sleep(50);
            }
            // Close the reader's channel — that ends the streaming RPC.
            reader.close();
            assertTrue(hits.size() >= 2, "expected >=2 matching events, got " + hits.size()
                    + " (subjects=" + hits.stream().map(EventEntry::getSubject).toList() + ")");
            for (EventEntry e : hits) {
                assertTrue(e.getSubject().startsWith("/tape/decision/recorded/" + r1.getRunId() + "/"),
                        "subject must match pattern; got " + e.getSubject());
            }
        }
    }

    // ── 5. Debouncer / TokenBucket primitives (unit) ───────────────────────

    @Test
    void debouncerCoalescesSameSubject() {
        Reactions.Debouncer d = new Reactions.Debouncer(200);
        assertTrue(d.allow("s/x"));
        assertFalse(d.allow("s/x"), "second hit inside window must be coalesced");
        assertTrue(d.allow("s/y"), "different subject must pass");
    }

    @Test
    void tokenBucketAllowsBurst() throws Exception {
        Reactions.TokenBucket tb = new Reactions.TokenBucket(10);
        // Rate=10 means starting bucket is full (10 tokens). Burst acquire 10
        // should complete near-instantly.
        long t0 = System.nanoTime();
        for (int i = 0; i < 10; i++) tb.acquire();
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(elapsedMs < 500, "burst of 10 should complete fast (got " + elapsedMs + "ms)");
    }

    @Test
    void parseJsonObjectHandlesBasicCases() {
        assertNull(Reactions.parseJsonObject(null));
        assertNull(Reactions.parseJsonObject(""));
        assertNull(Reactions.parseJsonObject("\"just-a-string\""));
        java.util.Map<String, Object> m = Reactions.parseJsonObject(
                "{\"a\":1,\"b\":\"x\",\"c\":true,\"d\":null}");
        assertNotNull(m);
        assertEquals(1L, m.get("a"));
        assertEquals("x", m.get("b"));
        assertEquals(Boolean.TRUE, m.get("c"));
        assertNull(m.get("d"));
    }
}
