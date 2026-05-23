package dev.tape.embedded.chaos;

import dev.tape.embedded.Schema.EffectRecord;
import dev.tape.embedded.Schema.ObligationRecord;
import dev.tape.embedded.TapeSessionService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Built-in invariants — Java port of {@code tape_adk.chaos.no_stuck_obligations}
 * et al.
 *
 * <p>Each invariant reads the embedded tables directly. For predicates that
 * are expressible through the service's existing list APIs
 * ({@code no_stuck_obligations}, {@code no_blind_non_idempotent_retry})
 * we go through those. For {@code exactly_one}, where the service has no
 * "list all CONFIRMED" surface, we go through the raw {@link DataSource} —
 * the chaos session carries it for this purpose.
 */
public final class Invariants {

    private Invariants() {}

    /** Slot for the DataSource so {@code exactlyOne} can query directly.
     *  Set by {@link Chaos#session} when it constructs the orchestration.
     *  ThreadLocal so concurrent scenarios don't trip each other. */
    static final ThreadLocal<DataSource> ACTIVE_DS = new ThreadLocal<>();

    /** No obligation has reached STUCK. Pass on a clean store. */
    public static final Invariant NO_STUCK_OBLIGATIONS = new Invariant() {
        @Override public String name() { return "no_stuck_obligations"; }
        @Override public InvariantResult check(TapeSessionService svc) throws Exception {
            var stuck = svc.listUnresolvedObligations(0L, 1000,
                false, true, false);
            if (stuck.isEmpty()) return new InvariantResult(name(), true, "0 stuck");
            StringBuilder sb = new StringBuilder(stuck.size() + " stuck: ");
            int n = Math.min(5, stuck.size());
            for (int i = 0; i < n; i++) {
                if (i > 0) sb.append(", ");
                ObligationRecord o = stuck.get(i);
                sb.append("seq=").append(o.seq()).append(" kind=").append(o.kind());
            }
            return new InvariantResult(name(), false, sb.toString());
        }
    };

    /**
     * A NON_IDEMPOTENT + OUTBOX effect should never reach
     * {@code dispatch_attempts > 1} while still PENDING — the contract
     * says {@code next_dispatch_at_ms = 0} flips it to UNKNOWN for the
     * reconciler instead of a blind retry.
     */
    public static final Invariant NO_BLIND_NON_IDEMPOTENT_RETRY = new Invariant() {
        @Override public String name() { return "no_blind_non_idempotent_retry"; }
        @Override public InvariantResult check(TapeSessionService svc) throws Exception {
            var pending = svc.listPendingEffects(0L, true, false, 1000);
            int violators = 0;
            for (EffectRecord e : pending) {
                if (EffectRecord.NON_IDEMPOTENT.equals(e.semantics())
                        && e.dispatchAttempts() > 1) {
                    violators++;
                }
            }
            if (violators == 0) return new InvariantResult(name(), true, "0 violators");
            return new InvariantResult(name(), false,
                violators + " NON_IDEMPOTENT effects retried while PENDING");
        }
    };

    /**
     * Exactly one CONFIRMED effect matches the filter. Parameterised —
     * use as {@code exactlyOne(connector="bank.wire", tool="")} or
     * {@code exactlyOneByTool("wire")}.
     */
    public static Invariant exactlyOne(String connector, String tool) {
        if (connector != null && !connector.isEmpty()
                && tool != null && !tool.isEmpty()) {
            throw new IllegalArgumentException(
                "exactlyOne: pass connector= or tool=, not both");
        }
        if ((connector == null || connector.isEmpty())
                && (tool == null || tool.isEmpty())) {
            throw new IllegalArgumentException(
                "exactlyOne requires connector= or tool=");
        }
        final String c = connector == null ? "" : connector;
        final String t = tool == null ? "" : tool;
        final String label;
        if (!c.isEmpty()) label = "exactly_one(connector='" + c + "')";
        else label = "exactly_one(tool='" + t + "')";

        return new Invariant() {
            @Override public String name() { return label; }
            @Override public InvariantResult check(TapeSessionService svc) throws Exception {
                DataSource ds = ACTIVE_DS.get();
                if (ds == null) {
                    return new InvariantResult(name(), false,
                        "no DataSource bound to chaos session (pass ds= to Chaos.session)");
                }
                int matches = countConfirmedMatching(ds, c, t);
                if (matches == 1) return new InvariantResult(name(), true, "1 confirmed");
                return new InvariantResult(name(), false,
                    matches + " confirmed (expected 1)");
            }
        };
    }

    /** Convenience overload for the common "by connector" case. */
    public static Invariant exactlyOneByConnector(String connector) {
        return exactlyOne(connector, "");
    }

    /** Convenience overload for the "by tool" case. */
    public static Invariant exactlyOneByTool(String tool) {
        return exactlyOne("", tool);
    }

    private static int countConfirmedMatching(
            DataSource ds, String connector, String tool) throws Exception {
        StringBuilder sb = new StringBuilder(
            "SELECT COUNT(*) FROM tape_effects WHERE status=?");
        if (!connector.isEmpty()) sb.append(" AND connector=?");
        if (!tool.isEmpty()) sb.append(" AND tool_name=?");
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(sb.toString())) {
            int idx = 1;
            ps.setString(idx++, EffectRecord.CONFIRMED);
            if (!connector.isEmpty()) ps.setString(idx++, connector);
            if (!tool.isEmpty()) ps.setString(idx++, tool);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
                return 0;
            }
        }
    }
}
