package dev.tape.chaos;

import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Dependency-free JSON read/write — enough for the chaos surface
 * (payload inspection, response mangling). Mirrors the dependency-free
 * approach in {@code dev.tape.Reactions} so the SDK stays light on deps.
 *
 * <p>Use a real JSON library for production code paths; this is internal
 * to chaos.
 */
final class SimpleJson {
    private SimpleJson() {}

    /** Parse an object; return null if input isn't an object or fails to parse. */
    static Map<String, Object> parseObject(String json) {
        Object v = parse(json);
        if (v instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> m = (Map<String, Object>) v;
            return m;
        }
        return null;
    }

    /** Parse any JSON value. Returns null on parse error. */
    static Object parse(String json) {
        if (json == null || json.isEmpty()) return null;
        try {
            Reader r = new Reader(json);
            r.skipWs();
            Object v = r.readValue();
            r.skipWs();
            if (r.pos < r.s.length()) return null;
            return v;
        } catch (Exception ex) {
            return null;
        }
    }

    /** Serialize a Map/List/primitive tree to JSON. Sorted keys are NOT required. */
    static String stringify(Object value) {
        StringBuilder b = new StringBuilder();
        write(b, value);
        return b.toString();
    }

    @SuppressWarnings("unchecked")
    private static void write(StringBuilder b, Object v) {
        if (v == null)                  { b.append("null"); return; }
        if (v instanceof String s)      { writeString(b, s); return; }
        if (v instanceof Boolean bv)    { b.append(bv ? "true" : "false"); return; }
        if (v instanceof Number n)      { writeNumber(b, n); return; }
        if (v instanceof Map<?, ?> m)   {
            b.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (!first) b.append(',');
                writeString(b, String.valueOf(e.getKey()));
                b.append(':');
                write(b, e.getValue());
                first = false;
            }
            b.append('}');
            return;
        }
        if (v instanceof List<?> l) {
            b.append('[');
            for (int i = 0; i < l.size(); i++) {
                if (i > 0) b.append(',');
                write(b, l.get(i));
            }
            b.append(']');
            return;
        }
        // Fallback: stringify-as-string.
        writeString(b, String.valueOf(v));
    }

    private static void writeString(StringBuilder b, String s) {
        b.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':  b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\b': b.append("\\b"); break;
                case '\f': b.append("\\f"); break;
                case '\n': b.append("\\n"); break;
                case '\r': b.append("\\r"); break;
                case '\t': b.append("\\t"); break;
                default:
                    if (c < 0x20) b.append(String.format("\\u%04x", (int) c));
                    else b.append(c);
            }
        }
        b.append('"');
    }

    private static void writeNumber(StringBuilder b, Number n) {
        if (n instanceof Double || n instanceof Float) {
            double d = n.doubleValue();
            if (d == (long) d) b.append((long) d);
            else b.append(d);
        } else {
            b.append(n.toString());
        }
    }

    private static final class Reader {
        final String s; int pos = 0;
        Reader(String s) { this.s = s; }

        void skipWs() { while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) pos++; }

        Object readValue() {
            skipWs();
            if (pos >= s.length()) throw new RuntimeException("eof");
            char c = s.charAt(pos);
            if (c == '{') return readObject();
            if (c == '[') return readArray();
            if (c == '"') return readString();
            if (c == 't' || c == 'f') return readBool();
            if (c == 'n') { if (s.startsWith("null", pos)) { pos += 4; return null; } throw new RuntimeException("expected null"); }
            return readNumber();
        }

        Map<String, Object> readObject() {
            Map<String, Object> m = new LinkedHashMap<>();
            pos++; skipWs();
            if (pos < s.length() && s.charAt(pos) == '}') { pos++; return m; }
            while (true) {
                skipWs();
                String k = readString();
                skipWs();
                if (pos >= s.length() || s.charAt(pos) != ':') throw new RuntimeException("expected :");
                pos++;
                m.put(k, readValue());
                skipWs();
                if (pos < s.length() && s.charAt(pos) == ',') { pos++; continue; }
                if (pos < s.length() && s.charAt(pos) == '}') { pos++; return m; }
                throw new RuntimeException("expected , or }");
            }
        }

        List<Object> readArray() {
            List<Object> out = new ArrayList<>();
            pos++; skipWs();
            if (pos < s.length() && s.charAt(pos) == ']') { pos++; return out; }
            while (true) {
                out.add(readValue());
                skipWs();
                if (pos < s.length() && s.charAt(pos) == ',') { pos++; continue; }
                if (pos < s.length() && s.charAt(pos) == ']') { pos++; return out; }
                throw new RuntimeException("expected , or ]");
            }
        }

        String readString() {
            if (s.charAt(pos) != '"') throw new RuntimeException("expected string");
            pos++;
            StringBuilder b = new StringBuilder();
            while (pos < s.length()) {
                char c = s.charAt(pos++);
                if (c == '"') return b.toString();
                if (c == '\\') {
                    if (pos >= s.length()) throw new RuntimeException("bad escape");
                    char e = s.charAt(pos++);
                    switch (e) {
                        case '"': b.append('"'); break;
                        case '\\': b.append('\\'); break;
                        case '/': b.append('/'); break;
                        case 'b': b.append('\b'); break;
                        case 'f': b.append('\f'); break;
                        case 'n': b.append('\n'); break;
                        case 'r': b.append('\r'); break;
                        case 't': b.append('\t'); break;
                        case 'u':
                            if (pos + 4 > s.length()) throw new RuntimeException("bad \\u");
                            b.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16));
                            pos += 4;
                            break;
                        default: throw new RuntimeException("bad escape \\" + e);
                    }
                } else {
                    b.append(c);
                }
            }
            throw new RuntimeException("unterminated string");
        }

        Object readBool() {
            if (s.startsWith("true", pos))  { pos += 4; return Boolean.TRUE; }
            if (s.startsWith("false", pos)) { pos += 5; return Boolean.FALSE; }
            throw new RuntimeException("expected boolean");
        }

        Object readNumber() {
            int start = pos;
            while (pos < s.length()) {
                char c = s.charAt(pos);
                if (c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E' || (c >= '0' && c <= '9')) pos++;
                else break;
            }
            String num = s.substring(start, pos);
            if (num.contains(".") || num.contains("e") || num.contains("E")) return Double.parseDouble(num);
            try { return Long.parseLong(num); } catch (NumberFormatException ex) { return Double.parseDouble(num); }
        }
    }
}
