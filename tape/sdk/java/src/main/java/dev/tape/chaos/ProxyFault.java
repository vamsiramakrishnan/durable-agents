package dev.tape.chaos;

import java.util.Map;
import java.util.function.Function;

/**
 * One declarative chaos rule the {@link ChaosProxy} applies. Mirrors
 * {@code tape.chaos.proxies.ProxyFault} (Python/TS/Go).
 *
 * <p>Use {@link ProxyFaults} for static constructors.
 */
public final class ProxyFault {
    public enum Kind {
        DELAY, INJECT_STATUS, TRUNCATE_STREAM,
        MANGLE_JSON, INJECT_PROMPT, TOOL_SHADOW,
        SCHEMA_DRIFT, DROP_CONNECTION
    }

    public final Kind kind;
    public final String pathPrefix;
    public final double probability;
    public final int ms;
    public final double jitter;
    public final int status;
    public final String body;
    public final int atEvent;
    public final String jsonPath;
    public final Object replacement;
    public final String suffix;
    public final Map<String, Object> extraTool;
    public final Function<Object, Object> driftFn;

    ProxyFault(Kind kind, String pathPrefix, double probability,
               int ms, double jitter, int status, String body, int atEvent,
               String jsonPath, Object replacement, String suffix,
               Map<String, Object> extraTool, Function<Object, Object> driftFn) {
        this.kind = kind;
        this.pathPrefix = pathPrefix == null ? "" : pathPrefix;
        this.probability = probability;
        this.ms = ms;
        this.jitter = jitter;
        this.status = status;
        this.body = body == null ? "" : body;
        this.atEvent = atEvent;
        this.jsonPath = jsonPath == null ? "" : jsonPath;
        this.replacement = replacement;
        this.suffix = suffix == null ? "" : suffix;
        this.extraTool = extraTool;
        this.driftFn = driftFn;
    }
}
