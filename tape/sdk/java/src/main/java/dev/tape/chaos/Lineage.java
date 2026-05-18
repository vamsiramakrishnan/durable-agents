package dev.tape.chaos;

import dev.tape.TapeClient;
import dev.tape.proto.JournalEntry;
import dev.tape.proto.SubscribeRunRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Lineage-driven fault injection. Walks a successful run's journal,
 * builds a DAG, and derives chaos scenarios from minimal cuts.
 *
 * <p>Mirrors {@code tape.chaos.lineage} (Python/TS/Go). The
 * breaking-failpoint map is kept in lockstep across SDKs so LDFI
 * scenarios are comparable across languages.
 */
public final class Lineage {
    private Lineage() {}

    public record Node(long seq, String kind, Map<String, Object> payload,
                       long parentSeq, String breakingFailpoint) {}

    public static final class Graph {
        public final String runId;
        public final List<Node> nodes;

        public Graph(String runId, List<Node> nodes) {
            this.runId = runId;
            this.nodes = List.copyOf(nodes);
        }

        public List<Node> ofKind(String kind) {
            List<Node> out = new ArrayList<>();
            for (Node n : nodes) if (n.kind.equals(kind)) out.add(n);
            return out;
        }

        public List<long[]> edges() {
            List<long[]> out = new ArrayList<>();
            for (Node n : nodes) if (n.parentSeq > 0) out.add(new long[]{n.parentSeq, n.seq});
            return out;
        }

        /** v1: max_size=1 = singletons (one cut per node). max_size>=2 enumerates pairs. */
        public List<List<Node>> minimalCuts(int maxSize) {
            if (maxSize <= 0) maxSize = 1;
            List<Node> candidates = new ArrayList<>();
            for (Node n : nodes) if (!n.breakingFailpoint.isEmpty()) candidates.add(n);
            List<List<Node>> cuts = new ArrayList<>(candidates.size());
            for (Node n : candidates) cuts.add(List.of(n));
            if (maxSize >= 2) {
                for (int i = 0; i < candidates.size(); i++) {
                    for (int j = i + 1; j < candidates.size(); j++) {
                        if (candidates.get(i).breakingFailpoint.equals(candidates.get(j).breakingFailpoint)) continue;
                        cuts.add(List.of(candidates.get(i), candidates.get(j)));
                    }
                }
            }
            return cuts;
        }
    }

    public static Graph fromRun(TapeClient client, String runId) {
        Iterator<JournalEntry> stream = client.pb().subscribeRun(
                SubscribeRunRequest.newBuilder().setRunId(runId).setFromSeq(0).build());
        Map<Long, Long> decisionSeqs = new HashMap<>();
        Map<String, Long> effectSeqs = new HashMap<>();
        Map<String, Long> gateSeqs = new HashMap<>();
        List<Node> nodes = new ArrayList<>();

        while (stream.hasNext()) {
            JournalEntry entry = stream.next();
            Map<String, Object> payload = SimpleJson.parseObject(entry.getPayloadJson());
            if (payload == null) payload = new HashMap<>();
            long parent = 0;
            String bp = "";
            String kind = entry.getKind();

            switch (kind) {
                case "run":
                    String status = String.valueOf(payload.getOrDefault("status", ""));
                    bp = "running".equals(status)
                            ? "tape::begin_run::post_db" : "tape::end_run::post_db";
                    break;
                case "decision":
                    long idx = ((Number) payload.getOrDefault("decision_index", -1L)).longValue();
                    decisionSeqs.put(idx, entry.getSeq());
                    parent = decisionSeqs.getOrDefault(idx - 1, 0L);
                    bp = "tape::record_decision::post_db";
                    break;
                case "effect":
                    long dIdx = ((Number) payload.getOrDefault("decision_index", -1L)).longValue();
                    parent = decisionSeqs.getOrDefault(dIdx, 0L);
                    String key = (String) payload.getOrDefault("idempotency_key", "");
                    String estatus = String.valueOf(payload.getOrDefault("status", ""));
                    if (!key.isEmpty()) {
                        switch (estatus.toLowerCase()) {
                            case "pending":
                                effectSeqs.putIfAbsent(key, entry.getSeq());
                                bp = "tape::begin_effect::post_db";
                                break;
                            case "confirmed":
                                bp = "tape::complete_effect::post_db";
                                break;
                            case "failed":
                            case "unknown":
                            case "reconciled":
                                bp = "tape::reconcile_effect::post_db";
                                break;
                            default:
                                bp = "tape::begin_effect::post_db";
                        }
                    }
                    break;
                case "obligation":
                    String effectKey = (String) payload.getOrDefault("effect_key", "");
                    parent = effectSeqs.getOrDefault(effectKey, 0L);
                    String ostatus = String.valueOf(payload.getOrDefault("status", "")).toLowerCase();
                    bp = (ostatus.equals("compensated") || ostatus.equals("stuck"))
                            ? "tape::resolve_obligation::post_db"
                            : "tape::register_compensation::post_db";
                    break;
                case "gate":
                    String gate = (String) payload.getOrDefault("gate", "");
                    if (!gate.isEmpty()) gateSeqs.putIfAbsent(gate, entry.getSeq());
                    String gstatus = String.valueOf(payload.getOrDefault("status", "")).toLowerCase();
                    bp = (gstatus.equals("delivered") || gstatus.equals("resolved"))
                            ? "tape::send_signal::post_db"
                            : "tape::await_signal::post_db";
                    break;
                case "value":
                    Boolean deleted = (Boolean) payload.getOrDefault("deleted", Boolean.FALSE);
                    bp = deleted ? "tape::delete_value::post_db" : "tape::write_value::post_db";
                    break;
            }
            nodes.add(new Node(entry.getSeq(), kind, payload, parent, bp));
            if (kind.equals("run")) {
                String s = String.valueOf(payload.getOrDefault("status", "")).toLowerCase();
                if (Snapshot.TERMINAL_RUN_STATUSES.contains(s)) break;
            }
        }
        return new Graph(runId, nodes);
    }

    /** Translate every minimal cut of {@code g} into a {@link Scenario}. */
    public static List<Scenario> deriveScenarios(Graph g, List<Invariant> invariants, int maxCutSize, String baseName) {
        if (baseName == null || baseName.isEmpty()) baseName = "ldfi";
        List<Scenario> out = new ArrayList<>();
        for (List<Node> cut : g.minimalCuts(maxCutSize)) {
            List<Fault> faults = new ArrayList<>(cut.size());
            StringBuilder name = new StringBuilder(baseName).append("::cut::");
            for (int i = 0; i < cut.size(); i++) {
                if (i > 0) name.append("+");
                Node n = cut.get(i);
                faults.add(Faults.crashAfter(n.breakingFailpoint, 1));
                name.append(n.kind).append("@").append(n.seq);
            }
            out.add(new Scenario(name.toString(), faults,
                    invariants == null ? List.of() : invariants, 0L));
        }
        return out;
    }
}
