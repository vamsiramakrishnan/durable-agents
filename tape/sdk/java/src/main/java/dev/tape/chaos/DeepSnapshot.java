package dev.tape.chaos;

import dev.tape.TapeClient;
import dev.tape.proto.DecisionRecord;
import dev.tape.proto.EffectRecord;
import dev.tape.proto.GetDecisionResponse;
import dev.tape.proto.GetEffectResponse;
import dev.tape.proto.JournalEntry;
import dev.tape.proto.ObligationRecord;
import dev.tape.proto.SubscribeRunRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Walks the full projection tables — catches body-level drift the
 * journal-summary {@link Snapshot} misses.
 */
public final class DeepSnapshot {

    public final String runId;
    public final List<String> decisions;
    public final List<String> effects;
    public final List<String> obligations;

    public DeepSnapshot(String runId, List<String> decisions, List<String> effects, List<String> obligations) {
        this.runId = runId;
        this.decisions = List.copyOf(decisions);
        this.effects = List.copyOf(effects);
        this.obligations = List.copyOf(obligations);
    }

    public boolean equalsDeep(DeepSnapshot other) {
        return other != null
                && this.decisions.equals(other.decisions)
                && this.effects.equals(other.effects)
                && this.obligations.equals(other.obligations);
    }

    public static DeepSnapshot capture(TapeClient client, String runId, String canonicalRunId, int maxDecisions) {
        if (canonicalRunId == null || canonicalRunId.isEmpty()) canonicalRunId = "run-1";
        if (maxDecisions <= 0) maxDecisions = 1000;
        Map<String, String> runIdMap = Map.of(runId, canonicalRunId);

        // Decisions
        List<String> decisions = new ArrayList<>();
        for (long i = 0; i < maxDecisions; i++) {
            GetDecisionResponse got;
            try { got = client.getDecision(runId, i); }
            catch (Exception ex) { break; }
            if (!got.getFound()) break;
            DecisionRecord d = got.getDecision();
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("decision_index", d.getDecisionIndex());
            m.put("model", d.getModel());
            m.put("request_json", d.getRequestJson());
            m.put("response_json", d.getResponseJson());
            m.put("policy_version", d.getPolicyVersion());
            m.put("rationale", d.getRationale());
            decisions.add(SimpleJson.stringify(Snapshot.canonical(m, runIdMap)));
        }

        // Effects: walk journal to collect keys, then GetEffect each.
        Set<String> seen = new HashSet<>();
        Iterator<JournalEntry> stream = client.pb().subscribeRun(
                SubscribeRunRequest.newBuilder().setRunId(runId).setFromSeq(0).build());
        while (stream.hasNext()) {
            JournalEntry e = stream.next();
            Object payload = SimpleJson.parse(e.getPayloadJson());
            if (e.getKind().equals("effect") && payload instanceof Map<?, ?> m) {
                Object k = m.get("idempotency_key");
                if (k instanceof String s && !s.isEmpty()) seen.add(s);
            }
            if (e.getKind().equals("run") && payload instanceof Map<?, ?> m) {
                Object status = m.get("status");
                if (status instanceof String s
                        && Snapshot.TERMINAL_RUN_STATUSES.contains(s.toLowerCase())) break;
            }
        }
        List<String> keys = new ArrayList<>(seen);
        Collections.sort(keys);
        List<String> effects = new ArrayList<>();
        for (String key : keys) {
            GetEffectResponse got;
            try { got = client.getEffect(runId, key); }
            catch (Exception ex) { continue; }
            if (!got.getFound()) continue;
            EffectRecord rec = got.getEffect();
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("tool_name", rec.getToolName());
            m.put("idempotency_key", rec.getIdempotencyKey());
            m.put("status", rec.getStatus().getNumber());
            m.put("request_json", rec.getRequestJson());
            m.put("response_json", rec.getResponseJson());
            m.put("error_json", rec.getErrorJson());
            m.put("semantics", rec.getSemantics().getNumber());
            m.put("dispatch_mode", rec.getDispatchMode().getNumber());
            m.put("business_key", rec.getBusinessKey());
            m.put("connector", rec.getConnector());
            m.put("external_ref", rec.getExternalRef());
            m.put("decision_index", rec.getDecisionIndex());
            effects.add(SimpleJson.stringify(Snapshot.canonical(m, runIdMap)));
        }

        // Obligations
        List<String> obligations = new ArrayList<>();
        try {
            var resp = client.listObligations(runId, false);
            for (ObligationRecord o : resp.getObligationsList()) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("kind", o.getKind());
                m.put("effect_key", o.getEffectKey());
                m.put("status", o.getStatus().getNumber());
                m.put("payload_json", o.getPayloadJson());
                m.put("attempts", o.getAttempts());
                m.put("max_attempts", o.getMaxAttempts());
                m.put("last_error", o.getLastError());
                m.put("result_json", o.getResultJson());
                m.put("compensator_ref", o.getCompensatorRef());
                obligations.add(SimpleJson.stringify(Snapshot.canonical(m, runIdMap)));
            }
        } catch (Exception ignored) { /* skip */ }

        return new DeepSnapshot(runId, decisions, effects, obligations);
    }
}
