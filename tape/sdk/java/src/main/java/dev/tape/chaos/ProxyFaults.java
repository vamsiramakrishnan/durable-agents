package dev.tape.chaos;

import java.util.Map;
import java.util.function.Function;

/** Static factories for {@link ProxyFault}. */
public final class ProxyFaults {
    private ProxyFaults() {}

    public static ProxyFault delay(String pathPrefix, int ms, double probability) {
        return new ProxyFault(ProxyFault.Kind.DELAY, pathPrefix, probability,
                ms, 0.0, 0, "", 0, "", null, "", null, null);
    }

    public static ProxyFault injectStatus(String pathPrefix, int status, String body, double probability) {
        return new ProxyFault(ProxyFault.Kind.INJECT_STATUS, pathPrefix, probability,
                0, 0.0, status, body, 0, "", null, "", null, null);
    }

    public static ProxyFault truncateStream(String pathPrefix, int atEvent, double probability) {
        return new ProxyFault(ProxyFault.Kind.TRUNCATE_STREAM, pathPrefix, probability,
                0, 0.0, 0, "", atEvent, "", null, "", null, null);
    }

    public static ProxyFault mangleJson(String pathPrefix, String jsonPath, Object replacement, double probability) {
        return new ProxyFault(ProxyFault.Kind.MANGLE_JSON, pathPrefix, probability,
                0, 0.0, 0, "", 0, jsonPath, replacement, "", null, null);
    }

    public static ProxyFault injectPrompt(String pathPrefix, String suffix, double probability) {
        return new ProxyFault(ProxyFault.Kind.INJECT_PROMPT, pathPrefix, probability,
                0, 0.0, 0, "", 0, "", null, suffix, null, null);
    }

    public static ProxyFault toolShadow(String pathPrefix, Map<String, Object> extraTool, double probability) {
        return new ProxyFault(ProxyFault.Kind.TOOL_SHADOW, pathPrefix, probability,
                0, 0.0, 0, "", 0, "", null, "", extraTool, null);
    }

    public static ProxyFault schemaDrift(String pathPrefix, Function<Object, Object> driftFn, double probability) {
        return new ProxyFault(ProxyFault.Kind.SCHEMA_DRIFT, pathPrefix, probability,
                0, 0.0, 0, "", 0, "", null, "", null, driftFn);
    }

    public static ProxyFault dropConnection(String pathPrefix, double probability) {
        return new ProxyFault(ProxyFault.Kind.DROP_CONNECTION, pathPrefix, probability,
                0, 0.0, 0, "", 0, "", null, "", null, null);
    }
}
