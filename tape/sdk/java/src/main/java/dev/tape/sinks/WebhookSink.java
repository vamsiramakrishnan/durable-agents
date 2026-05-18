package dev.tape.sinks;

import dev.tape.proto.EventEntry;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * POST each entry to {@link Opts#url} as JSON. Sets
 * {@code X-Tape-Event-Id: <run_id>/<seq>} so receivers can dedup on it.
 *
 * <p>At-least-once: a successful POST can still be retried after relay
 * restart (the durable cursor advances after publish returns); the receiver
 * must dedup on the event id.
 */
public final class WebhookSink implements Sink {

    /** Configuration. */
    public static final class Opts {
        public String  url;
        public Map<String, String> headers = new LinkedHashMap<>();
        public int     maxRetries        = 3;
        public Duration initialBackoff   = Duration.ofMillis(500);
        public Duration timeout          = Duration.ofSeconds(10);

        public Opts url(String v) { this.url = v; return this; }
        public Opts header(String k, String v) { this.headers.put(k, v); return this; }
        public Opts maxRetries(int n) { this.maxRetries = Math.max(1, n); return this; }
        public Opts initialBackoff(Duration d) { this.initialBackoff = d; return this; }
        public Opts timeout(Duration d) { this.timeout = d; return this; }
    }

    private final Opts opts;
    private final HttpClient client;

    public WebhookSink(Opts opts) {
        if (opts == null || opts.url == null || opts.url.isEmpty()) {
            throw new IllegalArgumentException("WebhookSink: opts.url is required");
        }
        this.opts = opts;
        this.client = HttpClient.newBuilder().connectTimeout(opts.timeout).build();
    }

    @Override public void publish(EventEntry e) throws Exception {
        String body = LogSink.entryJson(e);
        String eventId = e.getRunId() + "/" + e.getSeq();
        Exception last = null;
        Duration delay = opts.initialBackoff;

        for (int i = 0; i < opts.maxRetries; i++) {
            HttpRequest.Builder b = HttpRequest.newBuilder()
                    .uri(URI.create(opts.url))
                    .timeout(opts.timeout)
                    .header("Content-Type", "application/json")
                    .header("X-Tape-Event-Id", eventId);
            for (Map.Entry<String, String> h : opts.headers.entrySet()) b.header(h.getKey(), h.getValue());
            b.POST(HttpRequest.BodyPublishers.ofString(body));
            try {
                HttpResponse<String> r = client.send(b.build(), HttpResponse.BodyHandlers.ofString());
                int s = r.statusCode();
                if (s >= 200 && s < 300) return;
                last = new RuntimeException("webhook " + opts.url + " returned HTTP " + s);
            } catch (Exception ex) {
                last = ex;
            }
            try { Thread.sleep(delay.toMillis()); } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw ie;
            }
            delay = delay.multipliedBy(2);
        }
        if (last == null) last = new RuntimeException("webhook " + opts.url + " exhausted retries");
        throw last;
    }
}
