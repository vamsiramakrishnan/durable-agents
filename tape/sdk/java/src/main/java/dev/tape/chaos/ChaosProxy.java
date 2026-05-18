package dev.tape.chaos;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HTTP forward-proxy with declarative chaos rules. Streams SSE
 * chunk-by-chunk. Pure-JDK ({@code com.sun.net.httpserver.HttpServer} +
 * {@code java.net.http.HttpClient}) — no extra deps.
 *
 * <p>Mirrors {@code tape.chaos.proxies.ChaosProxy} (Python/TS/Go).
 */
public final class ChaosProxy implements AutoCloseable {

    private static final Set<String> DROP_REQUEST_HEADERS = Set.of(
            "host", "content-length", "connection", "keep-alive",
            "proxy-authenticate", "proxy-authorization", "te",
            "trailers", "transfer-encoding", "upgrade"
    );

    private final String upstream;
    private final List<ProxyFault> faults;
    private final Random rng;
    private final Duration timeout;
    private final HttpClient client;
    private final Map<String, AtomicInteger> faultHits = new ConcurrentHashMap<>();

    private HttpServer server;
    private String url = "";

    public ChaosProxy(String upstream, List<ProxyFault> faults, Random rng, Duration timeout) {
        this.upstream = upstream.endsWith("/") ? upstream.substring(0, upstream.length() - 1) : upstream;
        this.faults = (faults == null) ? List.of() : List.copyOf(faults);
        this.rng = (rng != null) ? rng : new Random();
        this.timeout = (timeout != null) ? timeout : Duration.ofSeconds(60);
        this.client = HttpClient.newBuilder().connectTimeout(this.timeout).build();
    }

    public String url() { return url; }

    public Map<String, Integer> faultHits() {
        Map<String, Integer> out = new HashMap<>();
        for (Map.Entry<String, AtomicInteger> e : faultHits.entrySet()) out.put(e.getKey(), e.getValue().get());
        return out;
    }

    /** Bind on {@code host:port}. Port 0 picks a free port. */
    public void start(String host, int port) throws IOException {
        if (host == null || host.isEmpty()) host = "127.0.0.1";
        server = HttpServer.create(new InetSocketAddress(host, port), 0);
        server.createContext("/", this::handle);
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        this.url = "http://" + host + ":" + server.getAddress().getPort();
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
            server = null;
        }
    }

    @Override public void close() { stop(); }

    private List<ProxyFault> matching(String path, ProxyFault.Kind kind) {
        List<ProxyFault> out = new ArrayList<>();
        for (ProxyFault f : faults) {
            if (f.kind != kind) continue;
            if (!f.pathPrefix.isEmpty() && !path.startsWith(f.pathPrefix)) continue;
            if (f.probability >= 1.0 || rng.nextDouble() < f.probability) {
                out.add(f);
                String key = kind.name().toLowerCase() + ":" + f.pathPrefix;
                faultHits.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
            }
        }
        return out;
    }

    // ── request handler ────────────────────────────────────────────────

    private void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getRawPath();
        if (exchange.getRequestURI().getRawQuery() != null) {
            path += "?" + exchange.getRequestURI().getRawQuery();
        }
        String pathOnly = exchange.getRequestURI().getRawPath();

        try {
            // PRE-FORWARD: delay
            for (ProxyFault f : matching(pathOnly, ProxyFault.Kind.DELAY)) {
                double ms = f.ms;
                if (f.jitter > 0) ms = Math.max(0, ms * (1.0 + (rng.nextDouble() * 2 - 1) * f.jitter));
                try { Thread.sleep((long) ms); } catch (InterruptedException ignored) {}
            }

            // PRE-FORWARD: inject_status
            List<ProxyFault> injected = matching(pathOnly, ProxyFault.Kind.INJECT_STATUS);
            if (!injected.isEmpty()) {
                ProxyFault f = injected.get(0);
                byte[] body = f.body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
                exchange.getResponseHeaders().add("X-Tape-Chaos", "inject_status");
                exchange.sendResponseHeaders(f.status, body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
                return;
            }

            // Forward the request upstream.
            byte[] reqBody = exchange.getRequestBody().readAllBytes();
            HttpRequest.Builder rb = HttpRequest.newBuilder()
                    .uri(URI.create(upstream + path))
                    .timeout(timeout)
                    .method(exchange.getRequestMethod(),
                            reqBody.length == 0 ? HttpRequest.BodyPublishers.noBody()
                                                : HttpRequest.BodyPublishers.ofByteArray(reqBody));
            for (Map.Entry<String, List<String>> e : exchange.getRequestHeaders().entrySet()) {
                String name = e.getKey().toLowerCase(Locale.ROOT);
                if (DROP_REQUEST_HEADERS.contains(name)) continue;
                for (String v : e.getValue()) {
                    try { rb.header(e.getKey(), v); } catch (IllegalArgumentException ignore) {}
                }
            }
            HttpResponse<byte[]> upRes;
            try {
                upRes = client.send(rb.build(), HttpResponse.BodyHandlers.ofByteArray());
            } catch (Exception ex) {
                exchange.getResponseHeaders().add("X-Tape-Chaos", "upstream-unreachable");
                exchange.sendResponseHeaders(502, 0);
                try (OutputStream os = exchange.getResponseBody()) { os.write(("upstream unreachable: " + ex.getMessage()).getBytes()); }
                return;
            }

            String ctype = upRes.headers().firstValue("Content-Type").orElse("");
            if (ctype.startsWith("text/event-stream")) {
                replyStream(pathOnly, exchange, upRes);
            } else if (ctype.startsWith("application/json")) {
                replyJson(pathOnly, exchange, upRes);
            } else {
                replyPassthrough(pathOnly, exchange, upRes);
            }
        } finally {
            exchange.close();
        }
    }

    private void writeHeaders(HttpExchange ex, HttpResponse<?> upRes, int overrideLen, Map<String, String> extra) throws IOException {
        for (Map.Entry<String, List<String>> e : upRes.headers().map().entrySet()) {
            String name = e.getKey().toLowerCase(Locale.ROOT);
            if (name.equals("transfer-encoding") || name.equals("content-encoding")
                    || name.equals("connection") || name.equals("keep-alive")) continue;
            if (overrideLen >= 0 && name.equals("content-length")) continue;
            // HttpExchange doesn't allow some restricted headers; filter to be safe.
            if (name.equals("date") || name.equals("host")) continue;
            for (String v : e.getValue()) {
                try { ex.getResponseHeaders().add(e.getKey(), v); } catch (IllegalArgumentException ignored) {}
            }
        }
        if (extra != null) for (Map.Entry<String, String> e : extra.entrySet()) {
            ex.getResponseHeaders().set(e.getKey(), e.getValue());
        }
        int contentLen = overrideLen >= 0 ? overrideLen : -1;
        // sendResponseHeaders: 0 = chunked; -1 = unknown/none; we use the upstream's status
        if (contentLen == -1) {
            ex.sendResponseHeaders(upRes.statusCode(), 0);
        } else if (contentLen == 0) {
            ex.sendResponseHeaders(upRes.statusCode(), -1);
        } else {
            ex.sendResponseHeaders(upRes.statusCode(), contentLen);
        }
    }

    private void replyPassthrough(String path, HttpExchange ex, HttpResponse<byte[]> upRes) throws IOException {
        byte[] data = upRes.body();
        List<ProxyFault> drops = matching(path, ProxyFault.Kind.DROP_CONNECTION);
        writeHeaders(ex, upRes, data.length, Map.of("X-Tape-Chaos", "passthrough"));
        if (drops.isEmpty()) try (OutputStream os = ex.getResponseBody()) { os.write(data); }
    }

    private void replyJson(String path, HttpExchange ex, HttpResponse<byte[]> upRes) throws IOException {
        byte[] data = upRes.body();
        Object payload = SimpleJson.parse(new String(data, StandardCharsets.UTF_8));
        if (payload == null) {
            writeHeaders(ex, upRes, data.length, null);
            try (OutputStream os = ex.getResponseBody()) { os.write(data); }
            return;
        }
        List<String> applied = new ArrayList<>();
        for (ProxyFault f : matching(path, ProxyFault.Kind.MANGLE_JSON)) {
            payload = setJsonAt(payload, f.jsonPath, f.replacement);
            applied.add("mangle_json");
        }
        for (ProxyFault f : matching(path, ProxyFault.Kind.INJECT_PROMPT)) {
            injectPromptInto(payload, f.suffix);
            applied.add("inject_prompt");
        }
        for (ProxyFault f : matching(path, ProxyFault.Kind.TOOL_SHADOW)) {
            if (f.extraTool != null) {
                shadowTools(payload, f.extraTool);
                applied.add("tool_shadow");
            }
        }
        for (ProxyFault f : matching(path, ProxyFault.Kind.SCHEMA_DRIFT)) {
            if (f.driftFn != null) {
                try { payload = f.driftFn.apply(payload); } catch (RuntimeException ignored) {}
                applied.add("schema_drift");
            }
        }
        List<ProxyFault> drops = matching(path, ProxyFault.Kind.DROP_CONNECTION);
        byte[] newBody = SimpleJson.stringify(payload).getBytes(StandardCharsets.UTF_8);
        String tag = applied.isEmpty() ? "json" : String.join(",", applied);
        writeHeaders(ex, upRes, newBody.length, Map.of("X-Tape-Chaos", tag));
        if (drops.isEmpty()) try (OutputStream os = ex.getResponseBody()) { os.write(newBody); }
    }

    private void replyStream(String path, HttpExchange ex, HttpResponse<byte[]> upRes) throws IOException {
        // HttpClient.send with ofByteArray already buffered the whole body — we
        // walk it event-by-event and emit. (Pure-JDK; for true low-latency
        // streaming an ofByteArrayConsumer would be next-step.)
        byte[] data = upRes.body();
        List<ProxyFault> truncates = matching(path, ProxyFault.Kind.TRUNCATE_STREAM);
        int cutAt = 0;
        for (ProxyFault t : truncates) if (t.atEvent > 0 && (cutAt == 0 || t.atEvent < cutAt)) cutAt = t.atEvent;
        List<ProxyFault> drops = matching(path, ProxyFault.Kind.DROP_CONNECTION);
        String tag = !truncates.isEmpty() ? "truncate_stream"
                                          : !drops.isEmpty() ? "drop_connection" : "sse";
        writeHeaders(ex, upRes, -1, Map.of("X-Tape-Chaos", tag));

        try (OutputStream os = ex.getResponseBody()) {
            int eventCount = 0;
            int start = 0;
            for (int i = 0; i + 1 < data.length; i++) {
                if (data[i] == '\n' && data[i + 1] == '\n') {
                    int end = i + 2;
                    os.write(data, start, end - start);
                    os.flush();
                    eventCount++;
                    if (cutAt > 0 && eventCount >= cutAt) return;
                    if (!drops.isEmpty() && eventCount >= 1) return;
                    start = end;
                    i = end - 1;
                }
            }
            // tail (if any)
            if (start < data.length && cutAt == 0) {
                os.write(data, start, data.length - start);
            }
        }
    }

    // ── JSON helpers ───────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static Object setJsonAt(Object obj, String path, Object value) {
        if (path == null || path.isEmpty()) return value;
        String[] parts = path.split("\\.");
        Object cur = obj;
        for (int i = 0; i < parts.length - 1; i++) {
            String p = parts[i];
            if (cur instanceof List<?> l && p.matches("\\d+")) {
                int n = Integer.parseInt(p);
                if (n < 0 || n >= l.size()) return obj;
                cur = l.get(n);
            } else if (cur instanceof Map<?, ?> m) {
                cur = ((Map<String, Object>) m).get(p);
            } else return obj;
        }
        String last = parts[parts.length - 1];
        if (cur instanceof List<?> l && last.matches("\\d+")) {
            int n = Integer.parseInt(last);
            if (n >= 0 && n < l.size()) ((List<Object>) l).set(n, value);
        } else if (cur instanceof Map<?, ?> m) {
            ((Map<String, Object>) m).put(last, value);
        }
        return obj;
    }

    @SuppressWarnings("unchecked")
    private static void injectPromptInto(Object obj, String suffix) {
        if (obj instanceof Map<?, ?> m) {
            Map<String, Object> mm = (Map<String, Object>) m;
            for (String k : new String[]{"text", "content", "output_text"}) {
                Object v = mm.get(k);
                if (v instanceof String s) mm.put(k, s + suffix);
            }
            for (Object v : mm.values()) injectPromptInto(v, suffix);
        } else if (obj instanceof List<?> l) {
            for (Object v : l) injectPromptInto(v, suffix);
        }
    }

    @SuppressWarnings("unchecked")
    private static void shadowTools(Object obj, Map<String, Object> extra) {
        if (obj instanceof Map<?, ?> m) {
            Map<String, Object> mm = (Map<String, Object>) m;
            for (Map.Entry<String, Object> e : new ArrayList<>(mm.entrySet())) {
                if (e.getKey().equals("tools") && e.getValue() instanceof List<?> l) {
                    ((List<Object>) l).add(new HashMap<>(extra));
                } else {
                    shadowTools(e.getValue(), extra);
                }
            }
        } else if (obj instanceof List<?> l) {
            for (Object v : l) shadowTools(v, extra);
        }
    }
}
