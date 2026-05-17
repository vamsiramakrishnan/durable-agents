package dev.tape;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Observability — structured-log helper + OpenTelemetry span-name constants.
 * Mirrors {@code tape.obs} in Python and {@code tape/sdk/go/obs.go}.
 *
 * <p>Use {@link #logJson(String, Map)} to emit a JSON line to stderr with
 * canonical field ordering. Install a {@link SpanHook} to forward Tape
 * spans into your tracing system; {@link #span(String, Map)} is a no-op
 * by default.
 */
public final class Obs {
    private Obs() {}

    public static final String SPAN_BEGIN_RUN         = "tape.begin_run";
    public static final String SPAN_RESUME_RUN        = "tape.resume_run";
    public static final String SPAN_RECORD_DECISION   = "tape.record_decision";
    public static final String SPAN_BEGIN_EFFECT      = "tape.begin_effect";
    public static final String SPAN_COMPLETE_EFFECT   = "tape.complete_effect";
    public static final String SPAN_RECONCILE_EFFECT  = "tape.reconcile_effect";
    public static final String SPAN_DISPATCH_EFFECT   = "tape.dispatch_effect";
    public static final String SPAN_COMPENSATE        = "tape.compensate";
    public static final String SPAN_REDRIVE           = "tape.redrive";
    public static final String SPAN_AWAIT_SIGNAL      = "tape.await_signal";
    public static final String SPAN_SEND_SIGNAL       = "tape.send_signal";

    public static final List<String> ALL_SPANS = List.of(
        SPAN_BEGIN_RUN, SPAN_RESUME_RUN, SPAN_RECORD_DECISION,
        SPAN_BEGIN_EFFECT, SPAN_COMPLETE_EFFECT,
        SPAN_RECONCILE_EFFECT, SPAN_DISPATCH_EFFECT,
        SPAN_COMPENSATE, SPAN_REDRIVE,
        SPAN_AWAIT_SIGNAL, SPAN_SEND_SIGNAL);

    public static final List<String> STRUCTURED_FIELDS = List.of(
        "ts", "level", "msg",
        "tenant_id", "app_name", "run_id", "invocation_id", "session_id",
        "seq", "effect_key", "decision_index", "reactor", "lease_owner");

    /** Emit one structured JSON line to stderr in canonical field order. */
    public static void logJson(String msg, Map<String, ?> fields) {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("ts", System.currentTimeMillis() / 1000.0);
        rec.put("level", "INFO");
        rec.put("msg", msg);
        if (fields != null) {
            for (Map.Entry<String, ?> e : fields.entrySet()) {
                Object v = e.getValue();
                if (v == null) continue;
                if (v instanceof CharSequence cs && cs.length() == 0) continue;
                rec.put(e.getKey(), v);
            }
        }
        Map<String, Object> ordered = new LinkedHashMap<>();
        for (String k : STRUCTURED_FIELDS) {
            if (rec.containsKey(k)) {
                ordered.put(k, rec.remove(k));
            }
        }
        ordered.putAll(rec);
        System.err.println(dev.tape.connectors.LogConnector.toJson(ordered));
    }

    /** Convenience: log at a custom level. */
    public static void log(String level, String msg, Map<String, ?> fields) {
        Map<String, Object> m = new LinkedHashMap<>(fields == null ? Map.of() : fields);
        m.put("level", level);
        logJson(msg, m);
    }

    /** A span hook callable installed by tracing adapters (no-op default). */
    @FunctionalInterface public interface SpanHook {
        SpanEnd open(String name, Map<String, Object> attrs);
    }
    @FunctionalInterface public interface SpanEnd {
        void close(Throwable error);
    }

    private static final AtomicReference<SpanHook> HOOK = new AtomicReference<>(null);

    public static void setSpanHook(SpanHook h) { HOOK.set(h); }

    /** Open a span via the installed hook (or a no-op). */
    public static SpanEnd span(String name, Map<String, Object> attrs) {
        SpanHook h = HOOK.get();
        if (h == null) return err -> {};
        return h.open(name, attrs == null ? Map.of() : attrs);
    }
}
