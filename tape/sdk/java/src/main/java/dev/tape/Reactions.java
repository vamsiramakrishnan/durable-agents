package dev.tape;

import dev.tape.proto.HandlerKind;
import dev.tape.proto.Reaction;
import dev.tape.proto.Task;
import dev.tape.proto.TaskStatus;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Java event-bus user surface — companion to the Python {@code tape.reactions}
 * module. See {@code design-principles/tape-event-bus.md}.
 *
 * <p>Use {@link #on(ReactionDef)} (or one of the {@code on*} subject helpers) to
 * register reactions at startup, then call {@link #runDispatcher(TapeClient, RunDispatcherOpts)}
 * to loop {@code ClaimTasks → handler → CompleteTask / NackTask}.
 *
 * <p>The registry is process-global; tests can call {@link #clearRegistry()} between
 * cases.
 */
public final class Reactions {

    private Reactions() {}

    private static final Logger LOG = Logger.getLogger("tape.reactions");

    // ── handler types ────────────────────────────────────────────────────────

    /** A reaction handler: takes an {@link Envelope} and runs to completion. */
    @FunctionalInterface
    public interface Handler {
        void handle(Envelope envelope) throws Exception;
    }

    /** What gets passed to a {@link Handler}. Mirrors the Python envelope dict. */
    public static final class Envelope {
        public final Task task;
        /** Parsed {@code task.payload_json}. {@code null} if absent or unparseable. */
        public final Map<String, Object> payload;
        /** Raw payload JSON (may be empty). */
        public final String payloadJson;

        public Envelope(Task task, Map<String, Object> payload, String payloadJson) {
            this.task = task;
            this.payload = payload;
            this.payloadJson = payloadJson;
        }

        public String subject() { return task.getSubject(); }
        public String reactionId() { return task.getReactionId(); }
        public String taskId() { return task.getTaskId(); }
        public String sourceRunId() { return task.getSourceRunId(); }
        public long sourceGlobalSeq() { return task.getSourceGlobalSeq(); }
        public int attempts() { return task.getAttempts(); }
        public String traceId() { return task.getTraceId(); }
        public String parentSpanId() { return task.getParentSpanId(); }
    }

    /** One {@code @tape.on(...)} declaration, before it's registered on the server. */
    public static final class ReactionDef {
        public String subjectPattern;
        public String predicate = "";
        public String agent = "";        // kind=AGENT
        public String publish = "";      // kind=PUBLISH
        public String name = "";
        public String reactionId = "";
        public int maxConcurrency = 1;
        public int rateLimitPerS = 0;
        public int debounceMs = 0;
        public int retryMax = 5;
        public int retryBackoffMs = 1000;
        public int dlqAfterN = 5;
        public int numShards = 1;
        public boolean bootstrapFromHead = false;
        public Handler handler;

        /** Populated by {@link #registerAll(TapeClient, String)}. */
        public String serverReactionId = "";

        HandlerKind handlerKind() {
            if (agent != null && !agent.isEmpty() && publish != null && !publish.isEmpty())
                throw new IllegalArgumentException("ReactionDef: set either agent= OR publish=, not both");
            if (agent != null && !agent.isEmpty()) return HandlerKind.HANDLER_KIND_AGENT;
            if (publish != null && !publish.isEmpty()) return HandlerKind.HANDLER_KIND_PUBLISH;
            return HandlerKind.HANDLER_KIND_TASK;
        }
    }

    // ── process-global registry ─────────────────────────────────────────────

    private static final List<ReactionDef> REGISTRY = Collections.synchronizedList(new ArrayList<>());

    /** Append a fully-built {@link ReactionDef} to the process registry. */
    public static void on(ReactionDef def) {
        if (def == null) throw new IllegalArgumentException("def is required");
        if (def.subjectPattern == null || def.subjectPattern.isEmpty())
            throw new IllegalArgumentException("subjectPattern is required");
        if (def.maxConcurrency < 1) def.maxConcurrency = 1;
        if (def.numShards < 1) def.numShards = 1;
        if (def.name == null || def.name.isEmpty()) def.name = "reaction-" + REGISTRY.size();
        REGISTRY.add(def);
    }

    /** Snapshot of the registry. */
    public static List<ReactionDef> getRegistry() {
        synchronized (REGISTRY) { return new ArrayList<>(REGISTRY); }
    }

    /** Test-only helper: drop every registered reaction. */
    public static void clearRegistry() { REGISTRY.clear(); }

    // ── subject helpers ──────────────────────────────────────────────────────

    /**
     * URL-encode a subject segment. Wildcards {@code *} and {@code **} pass through;
     * everything else is percent-encoded so user-chosen keys can't break the grammar.
     */
    static String seg(String s) {
        if ("*".equals(s) || "**".equals(s)) return s;
        // URLEncoder uses '+' for spaces; subjects want %20. Patch that.
        return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static ReactionDef defWith(String subjectPattern, Handler h, Consumer<ReactionDef> configure) {
        ReactionDef rd = new ReactionDef();
        rd.subjectPattern = subjectPattern;
        rd.handler = h;
        if (configure != null) configure.accept(rd);
        return rd;
    }

    public static void onValueChange(String namespace, String key, Handler h, Consumer<ReactionDef> configure) {
        String k = (key == null || key.isEmpty()) ? "*" : key;
        String pattern = "/tape/value/changed/" + seg(namespace) + "/" + seg(k);
        on(defWith(pattern, h, configure));
    }

    public static void onValueDeleted(String namespace, String key, Handler h, Consumer<ReactionDef> configure) {
        String k = (key == null || key.isEmpty()) ? "*" : key;
        String pattern = "/tape/value/deleted/" + seg(namespace) + "/" + seg(k);
        on(defWith(pattern, h, configure));
    }

    public static void onEffectConfirmed(String tool, Handler h, Consumer<ReactionDef> configure) {
        String t = (tool == null || tool.isEmpty()) ? "*" : tool;
        on(defWith("/tape/effect/confirmed/" + seg(t) + "/**", h, configure));
    }

    public static void onEffectFailed(String tool, Handler h, Consumer<ReactionDef> configure) {
        String t = (tool == null || tool.isEmpty()) ? "*" : tool;
        on(defWith("/tape/effect/failed/" + seg(t) + "/**", h, configure));
    }

    public static void onEffectUnknown(String tool, Handler h, Consumer<ReactionDef> configure) {
        String t = (tool == null || tool.isEmpty()) ? "*" : tool;
        on(defWith("/tape/effect/unknown/" + seg(t) + "/**", h, configure));
    }

    public static void onDecisionRecorded(Handler h, Consumer<ReactionDef> configure) {
        on(defWith("/tape/decision/recorded/**", h, configure));
    }

    public static void onGate(String gate, String verb, Handler h, Consumer<ReactionDef> configure) {
        String v = (verb == null || verb.isEmpty()) ? "released" : verb;
        on(defWith("/tape/gate/" + seg(v) + "/" + seg(gate) + "/**", h, configure));
    }

    public static void onRun(String status, Handler h, Consumer<ReactionDef> configure) {
        String s = (status == null || status.isEmpty()) ? "terminal" : status;
        on(defWith("/tape/run/" + seg(s) + "/**", h, configure));
    }

    // ── registration ────────────────────────────────────────────────────────

    /**
     * Call {@code RegisterReaction} for every reaction in the process registry.
     * Returns the list of persisted {@link Reaction} protos (with the server
     * {@code reactionId} filled in). {@code prefix} is prepended to each
     * reaction's name so concurrent test runs don't collide on the human label.
     */
    public static List<Reaction> registerAll(TapeClient c, String prefix) {
        List<Reaction> out = new ArrayList<>();
        String pref = prefix == null ? "" : prefix;
        for (ReactionDef rd : getRegistry()) {
            Reaction r = c.registerReaction(new RegisterReactionOpts()
                    .reactionId(rd.reactionId)
                    .name(pref.isEmpty() ? rd.name : (pref + rd.name))
                    .subjectPattern(rd.subjectPattern)
                    .predicateCel(rd.predicate)
                    .handlerKind(rd.handlerKind())
                    .agentApp(rd.agent == null ? "" : rd.agent)
                    .publishTarget(rd.publish == null ? "" : rd.publish)
                    .maxConcurrency(rd.maxConcurrency)
                    .rateLimitPerS(rd.rateLimitPerS)
                    .debounceMs(rd.debounceMs)
                    .retryMax(rd.retryMax)
                    .retryBackoffMs(rd.retryBackoffMs)
                    .dlqAfterN(rd.dlqAfterN)
                    .numShards(rd.numShards)
                    .bootstrapFromHead(rd.bootstrapFromHead));
            rd.serverReactionId = r.getReactionId();
            out.add(r);
        }
        return out;
    }

    // ── backpressure primitives ─────────────────────────────────────────────

    /** Tiny thread-safe token bucket — capacity == rate, refill 1 token every
     *  1/rate seconds. {@code acquire()} blocks until a token is available.
     *  {@code rate <= 0} disables limiting. */
    static final class TokenBucket {
        private final int rate;
        private double tokens;
        private long lastNs;

        TokenBucket(int ratePerS) {
            this.rate = Math.max(0, ratePerS);
            this.tokens = this.rate;
            this.lastNs = System.nanoTime();
        }

        synchronized void acquire() throws InterruptedException {
            if (rate <= 0) return;
            while (true) {
                long now = System.nanoTime();
                double elapsed = (now - lastNs) / 1_000_000_000.0;
                lastNs = now;
                tokens = Math.min(rate, tokens + elapsed * rate);
                if (tokens >= 1.0) { tokens -= 1.0; return; }
                double needSecs = (1.0 - tokens) / rate;
                long sleepMs = (long) Math.min(250.0, needSecs * 1000.0);
                if (sleepMs <= 0) sleepMs = 1;
                wait(sleepMs);
            }
        }
    }

    /** Subject coalescer: returns {@code true} the first time a subject is seen
     *  within the window, {@code false} subsequently. {@code window <= 0} disables. */
    static final class Debouncer {
        private final long windowNs;
        private final ConcurrentHashMap<String, Long> last = new ConcurrentHashMap<>();

        Debouncer(int windowMs) { this.windowNs = Math.max(0L, (long) windowMs) * 1_000_000L; }

        boolean allow(String subject) {
            if (windowNs <= 0) return true;
            long now = System.nanoTime();
            Long prev = last.get(subject);
            if (prev == null || (now - prev) >= windowNs) {
                // Last writer wins on the rare race — matches Python's behaviour.
                last.put(subject, now);
                return true;
            }
            return false;
        }
    }

    // ── OTel (lazy via reflection) ──────────────────────────────────────────

    private static volatile Boolean otelAvailable = null;
    private static Class<?> otelTraceCls;
    private static Object otelTracer;

    private static Object beginOtelSpan(String traceIdHex, String parentSpanIdHex) {
        // Soft, reflective OTel access. Returns the started Span (Closeable-ish)
        // or null if OTel isn't on the classpath. The returned object must be
        // closed via endOtelSpan().
        if (traceIdHex == null || traceIdHex.isEmpty()) return null;
        if (parentSpanIdHex == null || parentSpanIdHex.isEmpty()) return null;
        if (otelAvailable == Boolean.FALSE) return null;
        try {
            // Resolve classes once.
            if (otelTracer == null) {
                otelTraceCls = Class.forName("io.opentelemetry.api.GlobalOpenTelemetry");
                Object otel = otelTraceCls.getMethod("get").invoke(null);
                otelTracer = otel.getClass().getMethod("getTracer", String.class).invoke(otel, "tape.reactions");
            }
            otelAvailable = Boolean.TRUE;
            // We could attach context + parent here, but to keep the dep purely
            // soft (no compile-time reference to OTel), we just start a span
            // named "tape.task" on the global tracer. This is enough for the
            // handler to nest its own spans inside.
            Object builder = otelTracer.getClass().getMethod("spanBuilder", String.class).invoke(otelTracer, "tape.task");
            Object span = builder.getClass().getMethod("startSpan").invoke(builder);
            // Make span current.
            Object scope = span.getClass().getMethod("makeCurrent").invoke(span);
            return new Object[] { span, scope };
        } catch (ClassNotFoundException notFound) {
            otelAvailable = Boolean.FALSE;
            return null;
        } catch (Throwable ignored) {
            return null;
        }
    }

    private static void endOtelSpan(Object handle) {
        if (handle == null) return;
        try {
            Object[] pair = (Object[]) handle;
            Object span = pair[0];
            Object scope = pair[1];
            try { scope.getClass().getMethod("close").invoke(scope); } catch (Throwable ignored) {}
            try { span.getClass().getMethod("end").invoke(span); } catch (Throwable ignored) {}
        } catch (Throwable ignored) {}
    }

    // ── payload parsing (tiny JSON-object reader) ──────────────────────────

    /**
     * Minimal, dependency-free JSON-object parser — enough to expose the
     * triggering payload to a handler as a {@code Map<String,Object>}. Returns
     * {@code null} if the input is empty, isn't an object, or can't be parsed.
     *
     * <p>This is deliberately conservative: we don't pull in a JSON library
     * just for this convenience field. Handlers that need real parsing can
     * use {@link Envelope#payloadJson} with their JSON library of choice.
     */
    static Map<String, Object> parseJsonObject(String json) {
        if (json == null || json.isEmpty()) return null;
        try {
            JsonReader r = new JsonReader(json);
            r.skipWs();
            Object v = r.readValue();
            r.skipWs();
            if (r.pos < r.s.length()) return null;
            if (v instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> m = (Map<String, Object>) v;
                return m;
            }
            return null;
        } catch (Exception ex) {
            return null;
        }
    }

    private static final class JsonReader {
        final String s;
        int pos = 0;
        JsonReader(String s) { this.s = s; }

        void skipWs() {
            while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) pos++;
        }

        Object readValue() {
            skipWs();
            if (pos >= s.length()) throw new RuntimeException("eof");
            char c = s.charAt(pos);
            if (c == '{') return readObject();
            if (c == '[') return readArray();
            if (c == '"') return readString();
            if (c == 't' || c == 'f') return readBool();
            if (c == 'n') return readNull();
            return readNumber();
        }

        Map<String, Object> readObject() {
            Map<String, Object> m = new HashMap<>();
            pos++; // {
            skipWs();
            if (pos < s.length() && s.charAt(pos) == '}') { pos++; return m; }
            while (true) {
                skipWs();
                String k = readString();
                skipWs();
                if (pos >= s.length() || s.charAt(pos) != ':') throw new RuntimeException("expected :");
                pos++;
                Object v = readValue();
                m.put(k, v);
                skipWs();
                if (pos < s.length() && s.charAt(pos) == ',') { pos++; continue; }
                if (pos < s.length() && s.charAt(pos) == '}') { pos++; return m; }
                throw new RuntimeException("expected , or }");
            }
        }

        List<Object> readArray() {
            List<Object> out = new ArrayList<>();
            pos++; // [
            skipWs();
            if (pos < s.length() && s.charAt(pos) == ']') { pos++; return out; }
            while (true) {
                out.add(readValue());
                skipWs();
                if (pos < s.length() && s.charAt(pos) == ',') { pos++; continue; }
                if (pos < s.length() && s.charAt(pos) == ']') { pos++; return out; }
                throw new RuntimeException("expected , or ]");
            }
        }

        String readString() {
            if (s.charAt(pos) != '"') throw new RuntimeException("expected string");
            pos++;
            StringBuilder sb = new StringBuilder();
            while (pos < s.length()) {
                char c = s.charAt(pos++);
                if (c == '"') return sb.toString();
                if (c == '\\') {
                    if (pos >= s.length()) throw new RuntimeException("bad escape");
                    char e = s.charAt(pos++);
                    switch (e) {
                        case '"': sb.append('"'); break;
                        case '\\': sb.append('\\'); break;
                        case '/': sb.append('/'); break;
                        case 'b': sb.append('\b'); break;
                        case 'f': sb.append('\f'); break;
                        case 'n': sb.append('\n'); break;
                        case 'r': sb.append('\r'); break;
                        case 't': sb.append('\t'); break;
                        case 'u':
                            if (pos + 4 > s.length()) throw new RuntimeException("bad \\u");
                            sb.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16));
                            pos += 4;
                            break;
                        default: throw new RuntimeException("bad escape \\" + e);
                    }
                } else {
                    sb.append(c);
                }
            }
            throw new RuntimeException("unterminated string");
        }

        Boolean readBool() {
            if (s.startsWith("true", pos)) { pos += 4; return Boolean.TRUE; }
            if (s.startsWith("false", pos)) { pos += 5; return Boolean.FALSE; }
            throw new RuntimeException("bad bool");
        }

        Object readNull() {
            if (s.startsWith("null", pos)) { pos += 4; return null; }
            throw new RuntimeException("bad null");
        }

        Object readNumber() {
            int start = pos;
            if (s.charAt(pos) == '-') pos++;
            while (pos < s.length() && "0123456789.eE+-".indexOf(s.charAt(pos)) >= 0) pos++;
            String tok = s.substring(start, pos);
            if (tok.contains(".") || tok.contains("e") || tok.contains("E")) return Double.parseDouble(tok);
            try { return Long.parseLong(tok); } catch (NumberFormatException nfe) { return Double.parseDouble(tok); }
        }
    }

    // ── dispatcher ──────────────────────────────────────────────────────────

    /** Options for {@link #runDispatcher(TapeClient, RunDispatcherOpts)}. */
    public static final class RunDispatcherOpts {
        public String owner = "";
        public Duration pollInterval = Duration.ofMillis(500);
        public boolean once = false;
        public int claimMax = 16;
        public long leaseMs = 60_000L;
        public boolean register = true;
        public String prefix = "";

        public RunDispatcherOpts owner(String v) { this.owner = v; return this; }
        public RunDispatcherOpts pollInterval(Duration v) { this.pollInterval = v; return this; }
        public RunDispatcherOpts once(boolean v) { this.once = v; return this; }
        public RunDispatcherOpts claimMax(int v) { this.claimMax = v; return this; }
        public RunDispatcherOpts leaseMs(long v) { this.leaseMs = v; return this; }
        public RunDispatcherOpts register(boolean v) { this.register = v; return this; }
        public RunDispatcherOpts prefix(String v) { this.prefix = v; return this; }
    }

    private static String defaultOwner() {
        String env = System.getenv("TAPE_DISPATCHER_OWNER");
        if (env != null && !env.isEmpty()) return env;
        String host = "unknown";
        try { host = InetAddress.getLocalHost().getHostName(); } catch (Throwable ignored) {}
        String pidStr = ManagementFactory.getRuntimeMXBean().getName();  // <pid>@<host>
        String pid = pidStr.contains("@") ? pidStr.substring(0, pidStr.indexOf('@')) : pidStr;
        return host + ":" + pid + ":" + UUID.randomUUID().toString().substring(0, 6);
    }

    /**
     * In-proc dispatcher: claim → run → ack for every TASK reaction.
     *
     * <ol>
     *   <li>(Optional) register every reaction in the registry.
     *   <li>For each TASK reaction, {@code ClaimTasks(reaction_id, …)}; for each
     *       returned task, submit to a per-reaction executor sized to {@code maxConcurrency}.
     *   <li>Each worker enforces {@code rateLimitPerS} (token bucket) and
     *       {@code debounceMs} (subject coalesce — a debounce-skip completes the
     *       task as a no-op), runs the handler, then {@code CompleteTask} on
     *       success or {@code NackTask(permanent=…)} on failure (permanent once
     *       {@code attempts >= dlqAfterN}).
     * </ol>
     *
     * <p>AGENT reactions run on the server; PUBLISH reactions belong to a bridge.
     */
    public static void runDispatcher(TapeClient c, RunDispatcherOpts optsIn) {
        RunDispatcherOpts opts = optsIn == null ? new RunDispatcherOpts() : optsIn;
        String owner = (opts.owner == null || opts.owner.isEmpty()) ? defaultOwner() : opts.owner;
        if (opts.register) registerAll(c, opts.prefix);

        Map<String, ReactionState> state = new HashMap<>();
        for (ReactionDef rd : getRegistry()) {
            if (rd.handlerKind() != HandlerKind.HANDLER_KIND_TASK) continue;
            if (rd.serverReactionId == null || rd.serverReactionId.isEmpty()) continue;
            state.put(rd.serverReactionId, new ReactionState(rd));
        }

        long pollMs = opts.pollInterval == null ? 500L : Math.max(0L, opts.pollInterval.toMillis());
        try {
            while (true) {
                boolean didAny = false;
                for (Map.Entry<String, ReactionState> e : state.entrySet()) {
                    String rid = e.getKey();
                    ReactionState st = e.getValue();
                    List<Task> tasks;
                    try {
                        tasks = c.claimTasks(new ClaimTasksOpts()
                                .reactionId(rid).shard(-1).owner(owner)
                                .leaseMs(opts.leaseMs).max(opts.claimMax));
                    } catch (Throwable ex) {
                        LOG.log(Level.WARNING, "claim failed for " + rid + ": " + ex.getMessage());
                        continue;
                    }
                    for (Task t : tasks) {
                        didAny = true;
                        dispatchOne(c, st, owner, t);
                    }
                }
                if (opts.once) return;
                if (!didAny) {
                    try { Thread.sleep(pollMs); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt(); return;
                    }
                }
            }
        } finally {
            // Drain executors BEFORE the caller closes the client — workers
            // still need to call complete/nack on outcomes.
            for (ReactionState st : state.values()) {
                st.executor.shutdown();
                try { st.executor.awaitTermination(30, TimeUnit.SECONDS); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }

    private static final class ReactionState {
        final ReactionDef rd;
        final ExecutorService executor;
        final TokenBucket bucket;
        final Debouncer debouncer;
        final Semaphore concurrency;
        final AtomicLong threadCounter = new AtomicLong();

        ReactionState(ReactionDef rd) {
            this.rd = rd;
            this.bucket = new TokenBucket(rd.rateLimitPerS);
            this.debouncer = new Debouncer(rd.debounceMs);
            this.concurrency = new Semaphore(Math.max(1, rd.maxConcurrency));
            String namePrefix = "tape-r-" + (rd.name == null ? "anon" : rd.name) + "-";
            ThreadFactory tf = r -> {
                Thread th = new Thread(r, namePrefix + threadCounter.incrementAndGet());
                th.setDaemon(true);
                return th;
            };
            // We size the executor to maxConcurrency too; the semaphore is
            // belt-and-braces in case future work submits from elsewhere.
            this.executor = Executors.newFixedThreadPool(Math.max(1, rd.maxConcurrency), tf);
        }
    }

    private static void dispatchOne(TapeClient c, ReactionState st, String owner, Task task) {
        // Debounce-skip: complete the task as a no-op so the server doesn't
        // re-lease it (it's not an error — the handler chose to coalesce).
        if (!st.debouncer.allow(task.getSubject())) {
            try { c.completeTask(task.getTaskId(), owner); }
            catch (Throwable ex) { LOG.log(Level.WARNING, "debounce-complete failed for " + task.getTaskId() + ": " + ex.getMessage()); }
            return;
        }

        try { st.concurrency.acquire(); }
        catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }

        st.executor.submit(() -> {
            try {
                try { st.bucket.acquire(); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt(); return;
                }
                Map<String, Object> payload = parseJsonObject(task.getPayloadJson());
                Envelope env = new Envelope(task, payload, task.getPayloadJson());
                Object span = beginOtelSpan(task.getTraceId(), task.getParentSpanId());
                try {
                    if (st.rd.handler != null) st.rd.handler.handle(env);
                    c.completeTask(task.getTaskId(), owner);
                } catch (Throwable ex) {
                    boolean permanent = task.getAttempts() >= st.rd.dlqAfterN;
                    try {
                        c.nackTask(task.getTaskId(), owner,
                                ex.getClass().getSimpleName() + ": " + ex.getMessage(),
                                permanent);
                    } catch (Throwable ex2) {
                        LOG.log(Level.WARNING, "nack failed for " + task.getTaskId() + ": " + ex2.getMessage());
                    }
                } finally {
                    endOtelSpan(span);
                }
            } finally {
                st.concurrency.release();
            }
        });
    }

    // ── pub/sub bridge ──────────────────────────────────────────────────────

    /** Options for {@link #runPubSubBridge(TapeClient, RunPubSubBridgeOpts)}. */
    public static final class RunPubSubBridgeOpts {
        public String project;
        public String topic;
        public String reactionId = "";
        public String owner = "";
        public boolean once = false;
        public Duration pollInterval = Duration.ofMillis(500);
        public int claimMax = 32;
        public long leaseMs = 60_000L;

        public RunPubSubBridgeOpts project(String v) { this.project = v; return this; }
        public RunPubSubBridgeOpts topic(String v) { this.topic = v; return this; }
        public RunPubSubBridgeOpts reactionId(String v) { this.reactionId = v; return this; }
        public RunPubSubBridgeOpts owner(String v) { this.owner = v; return this; }
        public RunPubSubBridgeOpts once(boolean v) { this.once = v; return this; }
        public RunPubSubBridgeOpts pollInterval(Duration v) { this.pollInterval = v; return this; }
        public RunPubSubBridgeOpts claimMax(int v) { this.claimMax = v; return this; }
        public RunPubSubBridgeOpts leaseMs(long v) { this.leaseMs = v; return this; }
    }

    /**
     * Pull PUBLISH-kind tasks from Tape and publish them to a Pub/Sub topic.
     *
     * <p>Lazy-loads the Google Cloud Pub/Sub publisher via reflection so the
     * dependency is fully optional. Throws {@link RuntimeException} if it
     * isn't on the classpath. The body of each Pub/Sub message is the task's
     * {@code payload_json}; attributes carry {@code tape-task-id},
     * {@code tape-reaction-id}, {@code tape-subject}, {@code tape-global-seq},
     * {@code tape-trace-id}. The {@code ordering_key} is the source {@code run_id}.
     */
    public static void runPubSubBridge(TapeClient c, RunPubSubBridgeOpts optsIn) {
        RunPubSubBridgeOpts opts = optsIn == null ? new RunPubSubBridgeOpts() : optsIn;
        if (opts.project == null || opts.topic == null)
            throw new IllegalArgumentException("project and topic are required");
        String owner = (opts.owner == null || opts.owner.isEmpty()) ? defaultOwner() : opts.owner;

        // Resolve the publisher reflectively so google-cloud-pubsub stays soft.
        final Object publisher;
        final Object topicName;
        final Class<?> pubsubMessageBuilderCls;
        final Class<?> byteStringCls;
        try {
            Class<?> topicNameCls = Class.forName("com.google.pubsub.v1.TopicName");
            topicName = topicNameCls.getMethod("of", String.class, String.class)
                    .invoke(null, opts.project, opts.topic);
            Class<?> publisherCls = Class.forName("com.google.cloud.pubsub.v1.Publisher");
            Object builder = publisherCls.getMethod("newBuilder", topicNameCls).invoke(null, topicName);
            builder = builder.getClass().getMethod("setEnableMessageOrdering", boolean.class).invoke(builder, true);
            publisher = builder.getClass().getMethod("build").invoke(builder);
            pubsubMessageBuilderCls = Class.forName("com.google.pubsub.v1.PubsubMessage");
            byteStringCls = Class.forName("com.google.protobuf.ByteString");
        } catch (Throwable ex) {
            throw new RuntimeException(
                    "runPubSubBridge requires google-cloud-pubsub on the classpath", ex);
        }

        List<String> rids = new ArrayList<>();
        if (opts.reactionId != null && !opts.reactionId.isEmpty()) {
            rids.add(opts.reactionId);
        } else {
            for (ReactionDef rd : getRegistry()) {
                if (rd.handlerKind() != HandlerKind.HANDLER_KIND_PUBLISH) continue;
                if (rd.serverReactionId == null || rd.serverReactionId.isEmpty()) registerAll(c, "");
                if (rd.serverReactionId != null && !rd.serverReactionId.isEmpty()) rids.add(rd.serverReactionId);
            }
        }

        long pollMs = opts.pollInterval == null ? 500L : Math.max(0L, opts.pollInterval.toMillis());
        try {
            while (true) {
                boolean didAny = false;
                for (String rid : rids) {
                    List<Task> tasks;
                    try {
                        tasks = c.claimTasks(new ClaimTasksOpts()
                                .reactionId(rid).shard(-1).owner(owner)
                                .leaseMs(opts.leaseMs).max(opts.claimMax));
                    } catch (Throwable ex) {
                        LOG.log(Level.WARNING, "claim failed for " + rid + ": " + ex.getMessage());
                        continue;
                    }
                    for (Task t : tasks) {
                        didAny = true;
                        try {
                            // Build the PubsubMessage via reflection.
                            Object msgBuilder = pubsubMessageBuilderCls.getMethod("newBuilder").invoke(null);
                            Object bytes = byteStringCls.getMethod("copyFromUtf8", String.class)
                                    .invoke(null, t.getPayloadJson() == null ? "" : t.getPayloadJson());
                            msgBuilder = msgBuilder.getClass().getMethod("setData", byteStringCls).invoke(msgBuilder, bytes);
                            msgBuilder = msgBuilder.getClass().getMethod("setOrderingKey", String.class)
                                    .invoke(msgBuilder, t.getSourceRunId() == null ? "" : t.getSourceRunId());
                            // Attributes (Map<String,String>) via putAllAttributes.
                            Map<String, String> attrs = new HashMap<>();
                            attrs.put("tape-task-id", t.getTaskId());
                            attrs.put("tape-reaction-id", t.getReactionId());
                            attrs.put("tape-subject", t.getSubject());
                            attrs.put("tape-global-seq", String.valueOf(t.getSourceGlobalSeq()));
                            attrs.put("tape-trace-id", t.getTraceId());
                            msgBuilder = msgBuilder.getClass().getMethod("putAllAttributes", Map.class)
                                    .invoke(msgBuilder, attrs);
                            Object msg = msgBuilder.getClass().getMethod("build").invoke(msgBuilder);

                            Object fut = publisher.getClass().getMethod("publish", pubsubMessageBuilderCls)
                                    .invoke(publisher, msg);
                            fut.getClass().getMethod("get", long.class, TimeUnit.class)
                                    .invoke(fut, 10L, TimeUnit.SECONDS);
                            c.completeTask(t.getTaskId(), owner);
                        } catch (Throwable ex) {
                            try {
                                c.nackTask(t.getTaskId(), owner,
                                        "pubsub-publish: " + ex.getMessage(), false);
                            } catch (Throwable ex2) {
                                LOG.log(Level.WARNING, "nack failed for " + t.getTaskId() + ": " + ex2.getMessage());
                            }
                        }
                    }
                }
                if (opts.once) return;
                if (!didAny) {
                    try { Thread.sleep(pollMs); }
                    catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
                }
            }
        } finally {
            try { publisher.getClass().getMethod("shutdown").invoke(publisher); } catch (Throwable ignored) {}
        }
    }

    // Re-export the handful of proto enums most-likely needed at the call site.
    public static final HandlerKind KIND_AGENT = HandlerKind.HANDLER_KIND_AGENT;
    public static final HandlerKind KIND_TASK = HandlerKind.HANDLER_KIND_TASK;
    public static final HandlerKind KIND_PUBLISH = HandlerKind.HANDLER_KIND_PUBLISH;

    public static final TaskStatus STATUS_PENDING = TaskStatus.TASK_STATUS_PENDING;
    public static final TaskStatus STATUS_CLAIMED = TaskStatus.TASK_STATUS_CLAIMED;
    public static final TaskStatus STATUS_DONE = TaskStatus.TASK_STATUS_DONE;
    public static final TaskStatus STATUS_FAILED = TaskStatus.TASK_STATUS_FAILED;
    public static final TaskStatus STATUS_DLQ = TaskStatus.TASK_STATUS_DLQ;
}
