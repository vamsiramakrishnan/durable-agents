package dev.tape.embedded;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Java-flavoured equivalent of Python's {@code @outbox_tool} decorator.
 * Since Java doesn't have function decorators, expose a builder:
 * {@link #declare} returns an {@link OutboxToolHandle} that carries the
 * metadata a future ADK-Java plugin would consume.
 *
 * <p>Construction-time refusal — the load-bearing safety check:
 * {@link OutboxToolOpts.Builder#build()} throws
 * {@link IllegalArgumentException} immediately when {@code businessKey}
 * or {@code connector} are missing. The bug never makes it past static
 * initialization.
 *
 * <p>Distinct from {@link dev.tape.OutboxTool} which is the gRPC-path
 * outbox-envelope builder. This one is the embedded-path declaration.
 */
public final class OutboxTools {

    private OutboxTools() {}

    /** The declared options. Semantics fixed to NON_IDEMPOTENT + OUTBOX. */
    public record OutboxToolOpts(
            String connector,
            String businessKeyStatic,
            Function<Map<String, Object>, String> businessKeyFn,
            String compensate,
            Function<Map<String, Object>, String> customKeyFn) {

        public String semantics()    { return Schema.EffectRecord.NON_IDEMPOTENT; }
        public String dispatchMode() { return Schema.EffectRecord.OUTBOX; }

        /** Derive a business_key from the tool's payload map. */
        public String resolveBusinessKey(Map<String, Object> args) {
            if (businessKeyStatic != null) return businessKeyStatic;
            if (businessKeyFn != null) return businessKeyFn.apply(args);
            return null;
        }

        public static Builder builder() { return new Builder(); }

        public static final class Builder {
            private String connector;
            private String businessKeyStatic;
            private Function<Map<String, Object>, String> businessKeyFn;
            private String compensate;
            private Function<Map<String, Object>, String> customKeyFn;

            private Builder() {}

            public Builder connector(String v) { this.connector = v; return this; }
            public Builder businessKey(String v) {
                this.businessKeyStatic = v;
                this.businessKeyFn = null;
                return this;
            }
            public Builder businessKey(Function<Map<String, Object>, String> fn) {
                this.businessKeyFn = fn;
                this.businessKeyStatic = null;
                return this;
            }
            public Builder compensate(String kind) { this.compensate = kind; return this; }
            public Builder customKey(Function<Map<String, Object>, String> fn) {
                this.customKeyFn = fn;
                return this;
            }

            /** Build and validate — same refusal rules as Python's
             *  {@code @outbox_tool}: missing {@code connector} or
             *  {@code businessKey} throws immediately. */
            public OutboxToolOpts build() {
                if (connector == null || connector.isEmpty()) {
                    throw new IllegalArgumentException(
                        "OutboxTools.declare: `connector` is required — the outbox dispatcher "
                        + "needs to know which connector to dispatch through.");
                }
                if (businessKeyStatic == null && businessKeyFn == null) {
                    throw new IllegalArgumentException(
                        "OutboxTools.declare: `businessKey` is required — non-idempotent "
                        + "operations must declare the key the upstream uses to dedupe.");
                }
                return new OutboxToolOpts(connector, businessKeyStatic, businessKeyFn,
                    compensate, customKeyFn);
            }
        }
    }

    /** A future-plugin handle: the validated options + a body the plugin
     *  would NEVER execute inline (the outbox dispatcher calls the
     *  connector instead). */
    public record OutboxToolHandle(OutboxToolOpts opts) {
        public OutboxToolHandle {
            Objects.requireNonNull(opts, "opts");
        }
    }

    /** Validate the options and return a handle. Throws at this call site
     *  if the contract is violated. */
    public static OutboxToolHandle declare(OutboxToolOpts opts) {
        return new OutboxToolHandle(opts);
    }
}
