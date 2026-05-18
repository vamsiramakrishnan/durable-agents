package dev.tape.reactors;

import com.google.gson.Gson;
import dev.tape.TapeClient;
import dev.tape.connectors.Connector;
import dev.tape.connectors.ConnectorRegistry;
import dev.tape.proto.*;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Outbox-reactor — Java counterpart of Python's {@code tape.reactors.outbox}.
 *
 * <p>One pass: list effects to dispatch (PENDING+OUTBOX+due), then for each:
 * claim (atomic CAS lease), look up the connector, dispatch through it, and
 * record the result:
 *
 * <ul>
 *   <li>confirmed → {@link TapeClient#completeEffect}({@code CONFIRMED})</li>
 *   <li>failed → {@code recordDispatchAttempt(next_at = backoff)} (eventually FAILED)</li>
 *   <li>unknown → {@code recordDispatchAttempt(next_at = 0)} (status UNKNOWN;
 *       the reconciler resolves — do not blindly retry; that is the entire
 *       safety claim for non-idempotent upstreams)</li>
 * </ul>
 *
 * <p>Safety: the server's CAS on {@code claimEffectDispatch} enforces
 * non-blind-retry; this reactor double-checks (refuses to act if status is not
 * {@code PENDING} after the claim).
 */
public final class OutboxReactor {

    private static final Gson GSON = new Gson();

    private OutboxReactor() {}

    /** Per-effect outcome of one dispatch attempt. */
    public static final class Outcome {
        public String runId;
        public String idempotencyKey;
        public String connector;
        public String tool;
        /** confirmed | unknown | failed | retry-scheduled | skipped | error */
        public String status;
        public String reason;
        public String externalRef;
        public String error;
        public long   nextAtMs;
        public int    attempts;

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("run_id", runId);
            m.put("idempotency_key", idempotencyKey);
            m.put("connector", connector);
            m.put("tool", tool);
            m.put("status", status);
            if (reason != null)      m.put("reason", reason);
            if (externalRef != null) m.put("external_ref", externalRef);
            if (error != null)       m.put("error", error);
            if (nextAtMs > 0)        m.put("next_at_ms", nextAtMs);
            if (attempts > 0)        m.put("attempts", attempts);
            return m;
        }
    }

    /** Dispatcher options. */
    public static final class Options {
        public String connector = "";          // restrict to one connector name
        public ConnectorRegistry registry;     // defaults to ConnectorRegistry.DEFAULT
        public String claimer = "";            // identity for dispatch_claimed_by
        public long   limit = 200;
        public int    dispatchMaxAttempts = 5;

        public Options connector(String v) { this.connector = v == null ? "" : v; return this; }
        public Options registry(ConnectorRegistry v) { this.registry = v; return this; }
        public Options claimer(String v) { this.claimer = v == null ? "" : v; return this; }
        public Options limit(long v) { this.limit = v <= 0 ? 200 : v; return this; }
        public Options dispatchMaxAttempts(int v) { this.dispatchMaxAttempts = v <= 0 ? 5 : v; return this; }
    }

    /** Long-lived dispatcher options. */
    public static final class RunOptions {
        public Options outbox = new Options();
        public long    intervalMs = 1000;
        public boolean once = false;
        public java.util.function.Consumer<List<Outcome>> onTick;
    }

    private static String claimerId() {
        String env = System.getenv("TAPE_DISPATCH_CLAIMER");
        if (env != null && !env.isEmpty()) return env;
        String host = "unknown";
        try { host = InetAddress.getLocalHost().getHostName(); } catch (Exception ignore) {}
        String pid = ManagementFactory.getRuntimeMXBean().getName(); // "<pid>@host"
        int at = pid.indexOf('@'); if (at > 0) pid = pid.substring(0, at);
        return host + ":" + pid;
    }

    private static long backoffMs(int attempt) {
        double delayS = Math.min(1.0 * Math.pow(2.0, Math.max(attempt - 1, 0)), 60.0);
        return (long) (delayS * 1000.0);
    }

    @SuppressWarnings("unchecked")
    private static Connector.Effect toConnectorEffect(EffectRecord eff) {
        Connector.Effect e = new Connector.Effect();
        e.runId = eff.getRunId();
        e.idempotencyKey = eff.getIdempotencyKey();
        e.toolName = eff.getToolName();
        e.connector = eff.getConnector();
        e.businessKey = eff.getBusinessKey();
        e.attempt = eff.getDispatchAttempts() + 1;
        e.semantics = (eff.getSemantics() == EffectSemantics.EFFECT_SEMANTICS_NON_IDEMPOTENT)
                ? "non_idempotent" : "idempotent";
        String body = eff.getRequestJson();
        if (body != null && !body.isEmpty()) {
            try { e.payload = GSON.fromJson(body, Map.class); }
            catch (Exception ignore) {
                Map<String, Object> m = new HashMap<>(); m.put("raw", body); e.payload = m;
            }
        } else {
            e.payload = new HashMap<>();
        }
        return e;
    }

    /** Run a single effect through its connector. Returns the per-effect outcome. */
    public static Outcome dispatchOne(TapeClient client, EffectRecord eff, Options opt) {
        Outcome out = new Outcome();
        out.runId = eff.getRunId();
        out.idempotencyKey = eff.getIdempotencyKey();
        out.connector = eff.getConnector();
        out.tool = eff.getToolName();
        out.status = "skipped";

        ConnectorRegistry reg = (opt.registry != null) ? opt.registry : ConnectorRegistry.DEFAULT;
        if (!reg.has(eff.getConnector())) {
            out.reason = "no connector registered: '" + eff.getConnector() + "'";
            return out;
        }
        Connector connector = reg.get(eff.getConnector());

        String claimer = (opt.claimer == null || opt.claimer.isEmpty()) ? claimerId() : opt.claimer;
        ClaimEffectDispatchResponse claim = client.claimEffectDispatch(
                eff.getRunId(), eff.getIdempotencyKey(), claimer, 60_000L);
        if (!claim.getAcquired()) { out.reason = "lease contended"; return out; }

        EffectRecord cur = claim.getEffect();
        if (cur.getStatus() != EffectStatus.EFFECT_STATUS_PENDING) {
            out.reason = "unexpected status after claim: " + cur.getStatus();
            return out;
        }

        boolean isNonIdem = (cur.getSemantics() == EffectSemantics.EFFECT_SEMANTICS_NON_IDEMPOTENT);

        Connector.Result result;
        try {
            result = connector.dispatch(toConnectorEffect(cur));
        } catch (Exception ex) {
            String msg = "connector raised: " + ex.getClass().getSimpleName() + ": " + ex.getMessage();
            if (isNonIdem) {
                client.recordDispatchAttempt(cur.getRunId(), cur.getIdempotencyKey(), msg, 0L);
                out.status = "unknown"; out.error = ex.toString();
                return out;
            }
            int attempts = cur.getDispatchAttempts() + 1;
            long nextAt = System.currentTimeMillis() + backoffMs(attempts);
            client.recordDispatchAttempt(cur.getRunId(), cur.getIdempotencyKey(), msg, nextAt);
            out.status = "retry-scheduled"; out.error = ex.toString();
            out.nextAtMs = nextAt; out.attempts = attempts;
            return out;
        }

        switch (result.outcome) {
            case CONFIRMED: {
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("external_ref", result.dispatchId == null ? "" : result.dispatchId);
                if (result.response instanceof Map) {
                    body.putAll((Map<String, Object>) result.response);
                } else if (result.response != null) {
                    body.put("value", result.response);
                }
                client.completeEffect(cur.getRunId(), cur.getIdempotencyKey(),
                        EffectStatus.EFFECT_STATUS_CONFIRMED, GSON.toJson(body), "");
                out.status = "confirmed";
                out.externalRef = result.dispatchId == null ? "" : result.dispatchId;
                return out;
            }
            case UNKNOWN: {
                Map<String, Object> err = new LinkedHashMap<>();
                err.put("reason", "ack lost");
                if (result.error != null && !result.error.isEmpty()) err.put("error", result.error);
                client.recordDispatchAttempt(cur.getRunId(), cur.getIdempotencyKey(),
                        GSON.toJson(err), 0L);
                out.status = "unknown";
                return out;
            }
            default: {
                int attempts = cur.getDispatchAttempts() + 1;
                if (attempts >= opt.dispatchMaxAttempts) {
                    Map<String, Object> err = new LinkedHashMap<>();
                    err.put("final", true);
                    err.put("attempts", attempts);
                    err.put("last", result.error);
                    client.recordExternalObservation(cur.getRunId(), cur.getIdempotencyKey(),
                            EffectResolution.EFFECT_RESOLUTION_FAILED, "", "", GSON.toJson(err), "");
                    out.status = "failed"; out.attempts = attempts;
                    return out;
                }
                long nextAt = (result.retryAfterMs > 0)
                        ? System.currentTimeMillis() + result.retryAfterMs
                        : System.currentTimeMillis() + backoffMs(attempts);
                Map<String, Object> err = new LinkedHashMap<>();
                if (result.error != null && !result.error.isEmpty()) err.put("error", result.error);
                client.recordDispatchAttempt(cur.getRunId(), cur.getIdempotencyKey(),
                        GSON.toJson(err), nextAt);
                out.status = "retry-scheduled";
                out.nextAtMs = nextAt; out.attempts = attempts;
                return out;
            }
        }
    }

    /** One pass of the outbox dispatcher. */
    public static List<Outcome> dispatchOnce(TapeClient client, Options opt) {
        ListEffectsToDispatchResponse resp = client.listEffectsToDispatch(opt.connector, opt.limit, 0L);
        List<Outcome> outs = new ArrayList<>(resp.getEffectsCount());
        for (EffectRecord e : resp.getEffectsList()) {
            try { outs.add(dispatchOne(client, e, opt)); }
            catch (Exception ex) {
                Outcome o = new Outcome();
                o.runId = e.getRunId(); o.idempotencyKey = e.getIdempotencyKey();
                o.connector = e.getConnector(); o.tool = e.getToolName();
                o.status = "error"; o.error = ex.toString();
                outs.add(o);
            }
        }
        return outs;
    }

    /** Long-lived loop. Returns when {@code once} is true or the AtomicBoolean is cleared. */
    public static void run(TapeClient client, RunOptions opt, AtomicBoolean keepGoing) throws InterruptedException {
        if (opt.intervalMs <= 0) opt.intervalMs = 1000;
        while (true) {
            List<Outcome> outs;
            try { outs = dispatchOnce(client, opt.outbox); }
            catch (Exception ex) {
                System.err.println("[tape outbox] tick error: " + ex);
                outs = new ArrayList<>();
            }
            if (opt.onTick != null) opt.onTick.accept(outs);
            if (opt.once || (keepGoing != null && !keepGoing.get())) return;
            Thread.sleep(opt.intervalMs);
        }
    }
}
