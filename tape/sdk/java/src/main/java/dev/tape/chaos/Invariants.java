package dev.tape.chaos;

import dev.tape.TapeClient;
import dev.tape.proto.EffectStatus;
import dev.tape.proto.EffectSemantics;
import dev.tape.proto.EventEntry;
import dev.tape.proto.ListPendingEffectsResponse;
import dev.tape.proto.ListUnresolvedObligationsResponse;
import dev.tape.proto.ObligationRecord;
import dev.tape.proto.ObligationStatus;
import dev.tape.proto.EffectRecord;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Built-in invariants — predicates over Tape's journal projections.
 * Mirrors {@code tape.chaos.invariants} (Python/TS/Go).
 */
public final class Invariants {
    private Invariants() {}

    // ── exactly_one ──────────────────────────────────────────────────────

    /** "One wire, one record" — every CONFIRMED effect with a non-empty
     *  business_key has count == 1 under {@code connector} (or {@code tool}). */
    public static Invariant exactlyOne(String connector, String tool, String by) {
        if ((connector == null || connector.isEmpty()) && (tool == null || tool.isEmpty())) {
            throw new IllegalArgumentException("exactlyOne: needs connector or tool");
        }
        String byField = (by == null || by.isEmpty()) ? "business_key" : by;
        String target = (connector != null && !connector.isEmpty()) ? connector : tool;
        String name = "exactly_one(\"" + target + "\", by=\"" + byField + "\")";

        return new Invariant() {
            @Override public String name() { return name; }
            @Override public InvariantResult check(TapeClient client, String runId) {
                String pattern = (tool != null && !tool.isEmpty())
                        ? "/tape/effect/confirmed/" + tool + "/**"
                        : "/tape/effect/confirmed/**";
                Map<String, Integer> counts = new HashMap<>();
                try {
                    Iterator<EventEntry> it = client.subscribeBySubject(pattern, "", 1);
                    while (it.hasNext()) {
                        EventEntry e = it.next();
                        Map<String, Object> payload = SimpleJson.parseObject(e.getPayloadJson());
                        if (payload == null) continue;
                        if (connector != null && !connector.isEmpty()
                            && !connector.equals(String.valueOf(payload.get("connector")))) continue;
                        Object k = payload.get(byField);
                        if (!(k instanceof String) || ((String) k).isEmpty()) continue;
                        counts.merge((String) k, 1, Integer::sum);
                    }
                } catch (Exception ex) {
                    return InvariantResult.fail(name, "subscribeBySubject failed: " + ex.getMessage());
                }
                StringBuilder dupes = new StringBuilder();
                int dupeCount = 0;
                for (Map.Entry<String, Integer> e : counts.entrySet()) {
                    if (e.getValue() > 1) {
                        if (dupeCount > 0) dupes.append(",");
                        dupes.append(e.getKey()).append(":").append(e.getValue());
                        dupeCount++;
                    }
                }
                if (dupeCount > 0) {
                    return InvariantResult.fail(name, "duplicate business keys: " + dupes);
                }
                return InvariantResult.ok(name, "unique business keys: " + counts.size());
            }
        };
    }

    // ── no_stuck_obligations ─────────────────────────────────────────────

    public static final Invariant NO_STUCK_OBLIGATIONS = new Invariant() {
        @Override public String name() { return "no_stuck_obligations"; }
        @Override public InvariantResult check(TapeClient client, String runId) {
            try {
                ListUnresolvedObligationsResponse resp = client.listUnresolvedObligations(
                        500, 0L, /*includePending*/ false, /*includeStuck*/ true,
                        /*includeCommittedExpired*/ false);
                int stuck = 0;
                for (ObligationRecord o : resp.getObligationsList()) {
                    if (o.getStatus() != ObligationStatus.OBLIGATION_STATUS_STUCK) continue;
                    if (runId != null && !runId.isEmpty() && !runId.equals(o.getRunId())) continue;
                    stuck++;
                }
                if (stuck > 0) return InvariantResult.fail(name(), stuck + " stuck obligation(s)");
                return InvariantResult.ok(name(), "0 stuck");
            } catch (Exception ex) {
                return InvariantResult.fail(name(), "listUnresolvedObligations failed: " + ex.getMessage());
            }
        }
    };

    // ── no_blind_non_idempotent_retry ────────────────────────────────────

    public static final Invariant NO_BLIND_NON_IDEMPOTENT_RETRY = new Invariant() {
        @Override public String name() { return "no_blind_non_idempotent_retry"; }
        @Override public InvariantResult check(TapeClient client, String runId) {
            try {
                ListPendingEffectsResponse resp = client.listPendingEffects(
                        0L, /*includePending*/ true, /*includeUnknown*/ true, 500L);
                int bad = 0;
                StringBuilder head = new StringBuilder();
                int headN = 0;
                for (EffectRecord e : resp.getEffectsList()) {
                    if (runId != null && !runId.isEmpty() && !runId.equals(e.getRunId())) continue;
                    if (e.getSemantics() != EffectSemantics.EFFECT_SEMANTICS_NON_IDEMPOTENT) continue;
                    if (e.getDispatchAttempts() > 1
                            && e.getStatus() == EffectStatus.EFFECT_STATUS_PENDING
                            && e.getExternalRef().isEmpty()) {
                        bad++;
                        if (headN < 3) {
                            if (headN > 0) head.append(", ");
                            head.append(e.getRunId()).append("/").append(e.getIdempotencyKey())
                                .append("@").append(e.getDispatchAttempts());
                            headN++;
                        }
                    }
                }
                if (bad > 0) {
                    return InvariantResult.fail(name(),
                            bad + " non-idempotent effect(s) re-dispatched without observation: " + head);
                }
                return InvariantResult.ok(name(), "no blind retries on non-idempotent effects");
            } catch (Exception ex) {
                return InvariantResult.fail(name(), "listPendingEffects failed: " + ex.getMessage());
            }
        }
    };

    // ── no_orphan_compensation ───────────────────────────────────────────

    public static final Invariant NO_ORPHAN_COMPENSATION = new Invariant() {
        @Override public String name() { return "no_orphan_compensation"; }
        @Override public InvariantResult check(TapeClient client, String runId) {
            if (runId == null || runId.isEmpty()) return InvariantResult.ok(name(), "no runId; skipped");
            try {
                var resp = client.listObligations(runId, false);
                int orphans = 0;
                StringBuilder head = new StringBuilder();
                int headN = 0;
                for (ObligationRecord o : resp.getObligationsList()) {
                    var got = client.getEffect(runId, o.getEffectKey());
                    if (!got.getFound()) {
                        orphans++;
                        if (headN < 3) {
                            if (headN > 0) head.append(", ");
                            head.append(o.getEffectKey());
                            headN++;
                        }
                    }
                }
                if (orphans > 0) {
                    return InvariantResult.fail(name(),
                            orphans + " obligation(s) with no effect: " + head);
                }
                return InvariantResult.ok(name(),
                        "all " + resp.getObligationsCount() + " obligation(s) have an effect");
            } catch (Exception ex) {
                return InvariantResult.fail(name(), "listObligations failed: " + ex.getMessage());
            }
        }
    };

    // ── no_budget_overrun (v1 stub) ──────────────────────────────────────

    public static final Invariant NO_BUDGET_OVERRUN = new Invariant() {
        @Override public String name() { return "no_budget_overrun"; }
        @Override public InvariantResult check(TapeClient client, String runId) {
            if (runId == null || runId.isEmpty()) return InvariantResult.ok(name(), "no runId; skipped");
            return InvariantResult.ok(name(), "budget projection check is a Phase-3 invariant; v1 stub");
        }
    };
}
