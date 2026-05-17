package dev.tape;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * High-level surface for *non-idempotent* upstreams — Java port of
 * Python's {@code @tape.outbox_tool(...)}.
 *
 * <p>Build with {@link #builder(String, String)}; call {@link #envelope(Map)}
 * inside a tool body to produce the JSON-serialisable intent the outbox
 * reactor will dispatch via the named connector.
 *
 * <p>Rules — enforced at {@link Builder#build()} time:
 * <ul>
 *   <li>{@code semantics=NON_IDEMPOTENT} MUST declare at least one of
 *       {@code businessKey}, {@code statusCheck}, {@code compensate}, or
 *       {@code humanGate=true}.</li>
 *   <li>The body builds an intent only — it MUST NOT perform IO.</li>
 * </ul>
 */
public final class OutboxTool {

    public enum Semantics {
        IDEMPOTENT("idempotent"),
        AT_LEAST_ONCE("at_least_once"),
        NON_IDEMPOTENT("non_idempotent");
        public final String wire;
        Semantics(String w) { this.wire = w; }
    }

    public static final class OutboxConfigError extends IllegalArgumentException {
        public OutboxConfigError(String msg) { super("tape.outbox: " + msg); }
    }

    /** Status-check signature for `Reactions`-style reconciliation. */
    @FunctionalInterface public interface StatusCheck {
        Map<String, Object> check(String idempotencyKey) throws Exception;
    }
    /** Compensation signature. */
    @FunctionalInterface public interface Compensate {
        void run(Map<String, Object> payload) throws Exception;
    }
    /** Business-key derivation. */
    @FunctionalInterface public interface BusinessKey {
        String derive(Map<String, Object> payload);
    }

    private final String name;
    private final String connector;
    private final Semantics semantics;
    private final BusinessKey businessKey;
    private final boolean waitForResult;
    private final boolean humanGate;
    private final long    dispatchTimeoutMs;
    private final int     maxAttempts;

    private OutboxTool(Builder b) {
        this.name = b.name; this.connector = b.connector;
        this.semantics = b.semantics; this.businessKey = b.businessKey;
        this.waitForResult = b.waitForResult; this.humanGate = b.humanGate;
        this.dispatchTimeoutMs = b.dispatchTimeoutMs; this.maxAttempts = b.maxAttempts;
    }

    public String  name()              { return name; }
    public String  connector()         { return connector; }
    public Semantics semantics()       { return semantics; }
    public boolean waitForResult()     { return waitForResult; }
    public boolean humanGate()         { return humanGate; }
    public long    dispatchTimeoutMs() { return dispatchTimeoutMs; }
    public int     maxAttempts()       { return maxAttempts; }

    /**
     * Build the JSON envelope the outbox reactor will dispatch. The payload
     * is wrapped, not consumed.
     */
    public Map<String, Object> envelope(Map<String, Object> payload) {
        Objects.requireNonNull(payload, "payload");
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("__outbox__", Boolean.TRUE);
        env.put("connector", connector);
        env.put("tool", name);
        env.put("semantics", semantics.wire);
        env.put("wait_for_result", waitForResult);
        env.put("human_gate", humanGate);
        if (dispatchTimeoutMs > 0) env.put("dispatch_timeout_ms", dispatchTimeoutMs);
        if (businessKey != null) env.put("business_key", businessKey.derive(payload));
        env.put("payload", payload);
        return env;
    }

    /** True iff `value` is an outbox envelope (the `__outbox__: true` sentinel). */
    @SuppressWarnings("unchecked")
    public static boolean isEnvelope(Object value) {
        if (value instanceof Map<?, ?> m) {
            Object flag = ((Map<String, Object>) m).get("__outbox__");
            return Boolean.TRUE.equals(flag);
        }
        return false;
    }

    public static Builder builder(String name, String connector) {
        return new Builder(name, connector);
    }

    public static final class Builder {
        private final String name;
        private final String connector;
        private Semantics   semantics = Semantics.IDEMPOTENT;
        private BusinessKey businessKey;
        private StatusCheck statusCheck;
        private Compensate  compensate;
        private boolean     waitForResult = true;
        private boolean     humanGate;
        private long        dispatchTimeoutMs;
        private int         maxAttempts;

        private Builder(String name, String connector) {
            if (name == null || name.isEmpty())
                throw new OutboxConfigError("name is required");
            if (connector == null || connector.isEmpty())
                throw new OutboxConfigError("connector is required");
            this.name = name; this.connector = connector;
        }

        public Builder semantics(Semantics v)        { this.semantics = v; return this; }
        public Builder businessKey(BusinessKey v)    { this.businessKey = v; return this; }
        public Builder statusCheck(StatusCheck v)    { this.statusCheck = v; return this; }
        public Builder compensate(Compensate v)      { this.compensate = v; return this; }
        public Builder waitForResult(boolean v)      { this.waitForResult = v; return this; }
        public Builder humanGate(boolean v)          { this.humanGate = v; return this; }
        public Builder dispatchTimeoutMs(long v)     { this.dispatchTimeoutMs = v; return this; }
        public Builder maxAttempts(int v)            { this.maxAttempts = v; return this; }

        public OutboxTool build() {
            if (semantics == null) semantics = Semantics.IDEMPOTENT;
            if (semantics == Semantics.NON_IDEMPOTENT
                    && businessKey == null && statusCheck == null && compensate == null && !humanGate) {
                throw new OutboxConfigError(
                    "non_idempotent tools must declare at least one of businessKey, statusCheck, " +
                    "compensate, or humanGate=true — otherwise an UNKNOWN dispatch could be blindly retried");
            }
            return new OutboxTool(this);
        }
    }

    /**
     * Convenience: chain a payload-builder function with the envelope step so
     * callers can write {@code OutboxTool.bind(tool, args -> ...)}.
     */
    public static <A> Function<A, Map<String, Object>> bind(
            OutboxTool tool, Function<A, Map<String, Object>> payloadFn) {
        Objects.requireNonNull(tool); Objects.requireNonNull(payloadFn);
        return args -> tool.envelope(payloadFn.apply(args));
    }
}
