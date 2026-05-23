package dev.tape.embedded.compact;

import dev.tape.embedded.TapeSessionService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * The fifth reactor — Java port of {@code tape_adk.compact}.
 *
 * <p>Where the existing four reactors move rows FORWARD through the
 * state machine (PENDING → CONFIRMED, etc.), this one moves them OUT —
 * the journal isn't free, and a long-running agent accumulates terminal
 * rows that have no replay value.
 *
 * <p>The mechanism is one composite SQL DELETE per category with the
 * safety invariants encoded as WHERE-clause predicates — not Java-level
 * pre-checks:
 *
 * <pre>
 *   DELETE FROM tape_effects
 *    WHERE status IN ('confirmed','failed')
 *      AND ts_ms &lt; :cutoff
 *      AND NOT EXISTS (                            -- compensable-window pinning
 *        SELECT 1 FROM tape_obligations o
 *         WHERE o.session_id  = tape_effects.session_id
 *           AND o.effect_key  = tape_effects.idempotency_key
 *           AND o.status IN ('pending','committed'))
 * </pre>
 *
 * <p>The {@code NOT EXISTS} clause IS the pinning mechanism (primitive
 * #5 in the roadmap). It runs at SQL level under the same lock all other
 * embedded-mode writes use (via
 * {@link TapeSessionService#acquireMaintenanceConnection()}), so
 * concurrent compaction and concurrent register-compensation can't race
 * a pinned effect into the trash.
 *
 * <p>Ordering matters: session-archival runs FIRST (it's the superset
 * operation; when a whole session qualifies, one round of DELETEs wipes
 * its three tables in one shot), then the per-table pruning handles
 * surviving rows in still-active sessions.
 */
public final class Compactor {

    private Compactor() {}

    /** One pass of the compactor. Idempotent across ticks. */
    public static CompactionResult compactOnce(
            final TapeSessionService svc,
            final CompactionPolicy policy,
            final long nowMs) throws SQLException {

        svc.prepareTables();
        final long now = nowMs > 0 ? nowMs : System.currentTimeMillis();
        final long effectCutoff = now - policy.effectTtlMs();
        final long sessionCutoff = now - policy.sessionTtlMs();

        int effectsPruned = 0;
        int obligationsPruned = 0;
        int timersPruned = 0;
        int sessionsArchived = 0;

        try (Connection c = svc.acquireMaintenanceConnection()) {
            boolean priorAuto = c.getAutoCommit();
            c.setAutoCommit(false);
            try {
                // 1) Session-level archival FIRST. The superset operation:
                //    when a whole session qualifies (all rows old + no
                //    active obligations + no unfired timers), one round of
                //    DELETEs wipes its three tape_* tables.
                List<String[]> sessions = findArchivableSessions(
                    c, sessionCutoff, policy.maxPerTick());
                for (String[] keys : sessions) {
                    int[] perTable = archiveSession(c, keys[0], keys[1], keys[2]);
                    // Roll archival deletes into per-table counters so the
                    // audit log adds up.
                    effectsPruned += perTable[0];
                    obligationsPruned += perTable[1];
                    timersPruned += perTable[2];
                    sessionsArchived += 1;
                }

                // 2) Terminal obligations older than the effect TTL — keep
                //    STUCK (operator triage signal). The compactor never
                //    deletes a row a human still needs to see.
                if (policy.archiveTerminalObligations()) {
                    try (PreparedStatement ps = c.prepareStatement(
                            "DELETE FROM tape_obligations"
                            + " WHERE status='compensated' AND ts_ms<?")) {
                        ps.setLong(1, effectCutoff);
                        obligationsPruned += ps.executeUpdate();
                    }
                }

                // 3) Fired timers older than the effect TTL.
                if (policy.archiveFiredTimers()) {
                    try (PreparedStatement ps = c.prepareStatement(
                            "DELETE FROM tape_timers"
                            + " WHERE fired=1 AND created_at_ms<?")) {
                        ps.setLong(1, effectCutoff);
                        timersPruned += ps.executeUpdate();
                    }
                }

                // 4) Effects terminal + old enough + NO active obligation —
                //    the compensable-window pinning invariant encoded as a
                //    NOT EXISTS subquery rather than an application-level
                //    loop. This is the load-bearing line that keeps a row
                //    whose compensator still needs the external_ref.
                String sql =
                    "DELETE FROM tape_effects" +
                    " WHERE status IN ('confirmed','failed')" +
                    "   AND ts_ms < ?" +
                    "   AND NOT EXISTS (" +
                    "     SELECT 1 FROM tape_obligations o" +
                    "      WHERE o.session_id  = tape_effects.session_id" +
                    "        AND o.effect_key  = tape_effects.idempotency_key" +
                    "        AND o.status IN ('pending','committed'))";
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    ps.setLong(1, effectCutoff);
                    effectsPruned += ps.executeUpdate();
                }

                c.commit();
            } catch (SQLException | RuntimeException ex) {
                try { c.rollback(); } catch (SQLException ignore) {}
                throw ex;
            } finally {
                try { c.setAutoCommit(priorAuto); } catch (SQLException ignore) {}
            }
        }

        return new CompactionResult(
            effectsPruned, obligationsPruned, timersPruned, sessionsArchived);
    }

    /** Find sessions where (a) the latest effect ts is older than the
     *  cutoff, (b) there are no active or stuck obligations, (c) there
     *  are no unfired timers. Mirrors {@code _find_archivable_sessions}. */
    private static List<String[]> findArchivableSessions(
            Connection c, long sessionCutoff, int limit) throws SQLException {
        List<String[]> candidates = new ArrayList<>();
        String latest =
            "SELECT app_name, user_id, session_id, MAX(ts_ms) AS max_ts" +
            " FROM tape_effects" +
            " GROUP BY app_name, user_id, session_id" +
            " HAVING MAX(ts_ms) < ?" +
            " LIMIT ?";
        try (PreparedStatement ps = c.prepareStatement(latest)) {
            ps.setLong(1, sessionCutoff);
            ps.setInt(2, limit <= 0 ? 1000 : limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    candidates.add(new String[] {
                        rs.getString(1), rs.getString(2), rs.getString(3)
                    });
                }
            }
        }

        List<String[]> out = new ArrayList<>();
        for (String[] keys : candidates) {
            if (hasActiveObligations(c, keys)) continue;
            if (hasStuckObligations(c, keys)) continue;
            if (hasLiveTimers(c, keys)) continue;
            out.add(keys);
        }
        return out;
    }

    private static boolean hasActiveObligations(Connection c, String[] keys) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT COUNT(*) FROM tape_obligations"
                + " WHERE app_name=? AND user_id=? AND session_id=?"
                + "   AND status IN ('pending','committed')")) {
            ps.setString(1, keys[0]);
            ps.setString(2, keys[1]);
            ps.setString(3, keys[2]);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    private static boolean hasStuckObligations(Connection c, String[] keys) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT COUNT(*) FROM tape_obligations"
                + " WHERE app_name=? AND user_id=? AND session_id=? AND status='stuck'")) {
            ps.setString(1, keys[0]);
            ps.setString(2, keys[1]);
            ps.setString(3, keys[2]);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    private static boolean hasLiveTimers(Connection c, String[] keys) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT COUNT(*) FROM tape_timers"
                + " WHERE app_name=? AND user_id=? AND session_id=? AND fired=0")) {
            ps.setString(1, keys[0]);
            ps.setString(2, keys[1]);
            ps.setString(3, keys[2]);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    /** Returns [effects, obligations, timers] rowcounts for the deletes. */
    private static int[] archiveSession(
            Connection c, String app, String user, String sid) throws SQLException {
        int eff, ob, ti;
        try (PreparedStatement ps = c.prepareStatement(
                "DELETE FROM tape_effects"
                + " WHERE app_name=? AND user_id=? AND session_id=?")) {
            ps.setString(1, app); ps.setString(2, user); ps.setString(3, sid);
            eff = ps.executeUpdate();
        }
        try (PreparedStatement ps = c.prepareStatement(
                "DELETE FROM tape_obligations"
                + " WHERE app_name=? AND user_id=? AND session_id=?")) {
            ps.setString(1, app); ps.setString(2, user); ps.setString(3, sid);
            ob = ps.executeUpdate();
        }
        try (PreparedStatement ps = c.prepareStatement(
                "DELETE FROM tape_timers"
                + " WHERE app_name=? AND user_id=? AND session_id=?")) {
            ps.setString(1, app); ps.setString(2, user); ps.setString(3, sid);
            ti = ps.executeUpdate();
        }
        return new int[] { eff, ob, ti };
    }
}
