package dev.tape.connectors;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Append each dispatch / observe / compensate as a JSON line. Useful for
 * tests, demos, and the non-idempotent-bank example. No external deps —
 * uses a tiny inline JSON encoder so this can ship in the core jar.
 */
public final class LogConnector implements Connector {

    private final String name;
    private final Path path;

    public LogConnector(String path) {
        this.name = "log";
        this.path = Paths.get(path == null || path.isEmpty() ? "/tmp/tape-outbox.jsonl" : path);
    }

    @Override public String name() { return name; }

    private synchronized void append(String kind, Object body) throws IOException {
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("kind", kind);
        rec.put("ts_ms", System.currentTimeMillis());
        rec.put("body", body);
        try (BufferedWriter w = Files.newBufferedWriter(path,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            w.write(toJson(rec));
            w.write('\n');
        }
    }

    @Override public Result dispatch(Effect e) throws Exception {
        append("dispatch", e);
        return new Result(DispatchOutcome.CONFIRMED).response(Map.of("logged", true));
    }
    @Override public Observation observe(Effect e) throws Exception {
        append("observe", e);
        return new Observation(ObservationOutcome.CONFIRMED).count(1);
    }
    @Override public Compensation compensate(Obligation o) throws Exception {
        append("compensate", o);
        return new Compensation(CompensationOutcome.COMPENSATED);
    }

    // ── tiny JSON encoder — enough for diagnostics, not a JSON library ──

    @SuppressWarnings("unchecked")
    public static String toJson(Object v) {
        if (v == null) return "null";
        if (v instanceof Boolean || v instanceof Number) return String.valueOf(v);
        if (v instanceof CharSequence) return "\"" + escape(v.toString()) + "\"";
        if (v instanceof Map<?, ?> m) {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, Object> e : ((Map<String, Object>) m).entrySet()) {
                if (!first) sb.append(',');
                sb.append('"').append(escape(e.getKey())).append("\":").append(toJson(e.getValue()));
                first = false;
            }
            return sb.append('}').toString();
        }
        if (v instanceof Iterable<?> it) {
            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (Object e : it) {
                if (!first) sb.append(',');
                sb.append(toJson(e));
                first = false;
            }
            return sb.append(']').toString();
        }
        // Effect / Obligation are reflective; fall back to their toString-ish dump.
        return "\"" + escape(String.valueOf(v)) + "\"";
    }

    public static String escape(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
            }
        }
        return sb.toString();
    }
}
