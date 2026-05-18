package dev.tape.chaos;

import dev.tape.TapeClient;
import dev.tape.proto.JournalEntry;
import dev.tape.proto.SubscribeRunRequest;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * A run's journal, canonicalised for equality. Two snapshots are equal
 * when the underlying runs recorded the same logical history.
 *
 * <p>See {@code tape.chaos.snapshot} for the canonicalisation rules,
 * kept in lockstep with Python/TS/Go.
 */
public final class Snapshot {

    public record Line(String kind, String payload) {}

    public final String runId;
    public final List<Line> lines;

    public Snapshot(String runId, List<Line> lines) {
        this.runId = runId;
        this.lines = List.copyOf(lines);
    }

    /** Bit-for-bit equality, after canonicalisation. */
    public boolean equalsSnapshot(Snapshot other) {
        if (other == null || this.lines.size() != other.lines.size()) return false;
        for (int i = 0; i < this.lines.size(); i++) {
            if (!this.lines.get(i).equals(other.lines.get(i))) return false;
        }
        return true;
    }

    public record DiffEntry(int index, String op, Line a, Line b) {}

    public List<DiffEntry> diff(Snapshot other) {
        List<DiffEntry> out = new ArrayList<>();
        int n = Math.max(this.lines.size(), other.lines.size());
        for (int i = 0; i < n; i++) {
            Line a = i < this.lines.size() ? this.lines.get(i) : null;
            Line b = i < other.lines.size() ? other.lines.get(i) : null;
            if (a == null)      out.add(new DiffEntry(i, "<", null, b));
            else if (b == null) out.add(new DiffEntry(i, ">", a, null));
            else if (!a.equals(b)) out.add(new DiffEntry(i, "!=", a, b));
        }
        return out;
    }

    // ── canonicalisation ────────────────────────────────────────────────

    static final Set<String> STRIP_KEYS = Set.of(
            "ts_ms", "started_at_ms", "ended_at_ms", "last_update_time_ms",
            "lease_expires_at_ms", "claim_expires_at_ms", "dispatch_claim_expires_at_ms",
            "next_dispatch_at_ms", "next_attempt_at_ms", "fire_at_ms",
            "lease_owner", "claimed_by", "dispatch_claimed_by",
            "trace_id", "span_id", "parent_span_id",
            "seq", "global_seq",
            "invocation_id"
    );

    static final Set<String> TERMINAL_RUN_STATUSES = Set.of(
            "terminal", "failed", "cancelled", "stuck"
    );

    @SuppressWarnings("unchecked")
    static Object canonical(Object value, Map<String, String> runIdMap) {
        if (value instanceof Map<?, ?> m) {
            // sorted-key map for deterministic serialisation
            TreeMap<String, Object> out = new TreeMap<>();
            for (Map.Entry<?, ?> e : m.entrySet()) {
                String k = String.valueOf(e.getKey());
                if (STRIP_KEYS.contains(k)) continue;
                out.put(k, canonical(e.getValue(), runIdMap));
            }
            return out;
        }
        if (value instanceof List<?> l) {
            List<Object> out = new ArrayList<>(l.size());
            for (Object v : l) out.add(canonical(v, runIdMap));
            return out;
        }
        if (value instanceof String s) {
            for (Map.Entry<String, String> e : runIdMap.entrySet()) {
                String raw = e.getKey();
                if (!raw.isEmpty() && s.contains(raw)) s = s.replace(raw, e.getValue());
            }
            return s;
        }
        return value;
    }

    /**
     * Stream the journal via SubscribeRun, canonicalise, and stop at the
     * first terminal {@code run} entry.
     */
    public static Snapshot capture(TapeClient client, String runId, String canonicalRunId) {
        if (canonicalRunId == null || canonicalRunId.isEmpty()) canonicalRunId = "run-1";
        Map<String, String> runIdMap = Map.of(runId, canonicalRunId);
        List<Line> lines = new ArrayList<>();
        Iterator<JournalEntry> stream = client.pb().subscribeRun(
                SubscribeRunRequest.newBuilder().setRunId(runId).setFromSeq(0).build());
        while (stream.hasNext()) {
            JournalEntry entry = stream.next();
            Object payload = SimpleJson.parse(entry.getPayloadJson());
            if (payload == null) payload = Map.of("_raw", entry.getPayloadJson());
            Object canon = canonical(payload, runIdMap);
            lines.add(new Line(entry.getKind(), SimpleJson.stringify(canon)));
            if (entry.getKind().equals("run")) {
                if (payload instanceof Map<?, ?> m) {
                    Object status = m.get("status");
                    if (status instanceof String s
                            && TERMINAL_RUN_STATUSES.contains(s.toLowerCase())) break;
                }
            }
        }
        return new Snapshot(runId, lines);
    }
}
