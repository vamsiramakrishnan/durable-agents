package dev.tape.connectors;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * POST the intent payload to an HTTPS endpoint. Headers attached:
 * <ul>
 *   <li>{@code X-Tape-Idempotency-Key} — runner-derived dedup key</li>
 *   <li>{@code X-Tape-Business-Key} — when supplied by {@code OutboxTool}</li>
 *   <li>{@code X-Tape-Run-Id} — for traceability</li>
 *   <li>{@code X-Tape-Attempt} — dispatch attempt number</li>
 * </ul>
 *
 * <p>Outcome mapping: 2xx ⇒ CONFIRMED, 4xx ⇒ FAILED, 5xx / network ⇒ UNKNOWN.
 */
public final class HttpConnector implements Connector {

    public static final class Opts {
        public String name = "http";
        public String url;
        public String observeUrl;
        public String compensateUrl;
        public Duration timeout = Duration.ofSeconds(30);
        public Map<String, String> headers = new HashMap<>();

        public Opts name(String v)          { this.name = v; return this; }
        public Opts url(String v)           { this.url = v; return this; }
        public Opts observeUrl(String v)    { this.observeUrl = v; return this; }
        public Opts compensateUrl(String v) { this.compensateUrl = v; return this; }
        public Opts timeout(Duration v)     { this.timeout = v; return this; }
        public Opts header(String k, String v) { this.headers.put(k, v); return this; }
    }

    private final Opts opts;
    private final HttpClient client;

    public HttpConnector(Opts opts) {
        if (opts == null || opts.url == null || opts.url.isEmpty()) {
            throw new IllegalArgumentException("HttpConnector: url is required");
        }
        this.opts = opts;
        this.client = HttpClient.newBuilder().connectTimeout(opts.timeout).build();
    }

    @Override public String name() { return opts.name; }

    private HttpResponse<String> post(String url, Map<String, Object> body,
                                      String idempKey, String runId, String bizKey, int attempt) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(opts.timeout)
                .header("Content-Type", "application/json")
                .header("X-Tape-Idempotency-Key", idempKey == null ? "" : idempKey);
        if (runId != null) b.header("X-Tape-Run-Id", runId);
        if (bizKey != null && !bizKey.isEmpty()) b.header("X-Tape-Business-Key", bizKey);
        if (attempt > 0) b.header("X-Tape-Attempt", String.valueOf(attempt));
        for (Map.Entry<String, String> e : opts.headers.entrySet()) b.header(e.getKey(), e.getValue());
        b.POST(HttpRequest.BodyPublishers.ofString(LogConnector.toJson(body)));
        return client.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Override public Result dispatch(Effect e) {
        try {
            HttpResponse<String> r = post(opts.url, e.payload,
                e.idempotencyKey, e.runId, e.businessKey, e.attempt);
            int s = r.statusCode();
            if (s >= 200 && s < 300) return new Result(DispatchOutcome.CONFIRMED).response(r.body());
            if (s >= 400 && s < 500) return new Result(DispatchOutcome.FAILED).response(r.body()).error("http " + s);
            return new Result(DispatchOutcome.UNKNOWN).response(r.body()).error("http " + s);
        } catch (Exception ex) {
            return new Result(DispatchOutcome.UNKNOWN).error(ex.getMessage());
        }
    }

    @Override public Observation observe(Effect e) {
        if (opts.observeUrl == null || opts.observeUrl.isEmpty()) {
            return new Observation(ObservationOutcome.UNKNOWN).error("no observeUrl configured");
        }
        Map<String, Object> probe = Map.of(
            "idempotency_key", e.idempotencyKey,
            "business_key", e.businessKey == null ? "" : e.businessKey,
            "payload", e.payload == null ? Map.of() : e.payload);
        try {
            HttpResponse<String> r = post(opts.observeUrl, probe,
                e.idempotencyKey, e.runId, e.businessKey, e.attempt);
            if (r.statusCode() != 200) {
                return new Observation(ObservationOutcome.UNKNOWN).error("http " + r.statusCode()).response(r.body());
            }
            // Parse just enough to find `count` — keep this dep-free.
            int count = parseCount(r.body());
            if (count == 0) return new Observation(ObservationOutcome.ABSENT).count(0).response(r.body());
            if (count == 1) return new Observation(ObservationOutcome.CONFIRMED).count(1).response(r.body());
            return new Observation(ObservationOutcome.DUPLICATE).count(count).response(r.body());
        } catch (Exception ex) {
            return new Observation(ObservationOutcome.UNKNOWN).error(ex.getMessage());
        }
    }

    @Override public Compensation compensate(Obligation o) {
        if (opts.compensateUrl == null || opts.compensateUrl.isEmpty()) {
            return new Compensation(CompensationOutcome.STUCK).error("no compensateUrl configured");
        }
        try {
            HttpResponse<String> r = post(opts.compensateUrl, o.payload,
                o.effectKey, o.runId, "", o.attempt);
            int s = r.statusCode();
            if (s >= 200 && s < 300) return new Compensation(CompensationOutcome.COMPENSATED).response(r.body());
            if (s >= 400 && s < 500) return new Compensation(CompensationOutcome.FAILED).response(r.body()).error("http " + s);
            return new Compensation(CompensationOutcome.PENDING).response(r.body()).error("http " + s);
        } catch (Exception ex) {
            return new Compensation(CompensationOutcome.PENDING).error(ex.getMessage());
        }
    }

    // Minimal JSON `count` extractor — looks for `"count":<int>`.
    static int parseCount(String body) {
        if (body == null) return 0;
        int idx = body.indexOf("\"count\"");
        if (idx < 0) return 0;
        int colon = body.indexOf(':', idx);
        if (colon < 0) return 0;
        int i = colon + 1;
        while (i < body.length() && Character.isWhitespace(body.charAt(i))) i++;
        int start = i;
        while (i < body.length() && (Character.isDigit(body.charAt(i)) || body.charAt(i) == '-')) i++;
        if (i == start) return 0;
        try { return Integer.parseInt(body.substring(start, i)); }
        catch (NumberFormatException e) { return 0; }
    }
}
