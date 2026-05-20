package dev.tape.embedded;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

import static dev.tape.embedded.Schema.EffectRecord;
import static dev.tape.embedded.Schema.EffectResolution;
import static dev.tape.embedded.Schema.ObligationRecord;
import static dev.tape.embedded.Schema.TimerRecord;
import static dev.tape.embedded.Schema.ValueRecord;

/**
 * Embedded-mode session service — Java port of
 * {@code tape_adk.service.TapeSessionService}.
 *
 * <p>Speaks plain JDBC against any {@link DataSource}. Works with SQLite
 * (via {@code org.xerial:sqlite-jdbc}) and Postgres (via
 * {@code org.postgresql:postgresql}) — no driver is a hard dep of this
 * SDK; callers bring their own.
 *
 * <p><b>The SQLite CAS lock.</b> The Python reference learned the hard way
 * that ADK's {@code StaticPool} (one shared SQLite connection across all
 * async sessions) means two concurrent {@code claimEffectDispatch} calls
 * can interleave their BEGIN/UPDATE/COMMIT operations in ways that make
 * the rowcount-based CAS unreliable. Same hazard applies here: a single
 * {@link Connection} (or a SQLite-backed connection pool with a single
 * underlying handle) won't serialise UPDATE/SELECT cleanly. The fix is
 * the same: an in-process {@link ReentrantLock} held across the CAS for
 * SQLite. For Postgres (real per-session connections, row-level locks)
 * the lock is a no-op. Detection is by JDBC URL prefix.
 *
 * <p>The class is intentionally synchronous (no {@code CompletableFuture})
 * even though Python's is async — JDBC is blocking and callers wrap
 * however they like. The shapes + semantics match.
 */
public final class TapeSessionService {

    private final DataSource ds;
    private final boolean isSqlite;
    private final ReentrantLock casLock = new ReentrantLock();
    private volatile boolean tablesPrepared = false;

    /**
     * Build a service over the given DataSource. The dialect is auto-detected
     * by probing the first connection's URL (sqlite vs not).
     */
    public TapeSessionService(DataSource ds) {
        this.ds = ds;
        this.isSqlite = detectSqlite(ds);
    }

    private static boolean detectSqlite(DataSource ds) {
        try (Connection c = ds.getConnection()) {
            String url = c.getMetaData().getURL();
            return url != null && url.toLowerCase().startsWith("jdbc:sqlite");
        } catch (SQLException e) {
            return false;
        }
    }

    /** True iff the underlying driver is SQLite. */
    public boolean isSqlite() { return isSqlite; }

    /** Idempotently create all four tables + indices. Called lazily by each
     *  method, mirroring Python's `_prepare_tables` hook. */
    public void prepareTables() throws SQLException {
        if (tablesPrepared) return;
        synchronized (this) {
            if (tablesPrepared) return;
            try (Connection c = ds.getConnection()) {
                Schema.createAllTables(c);
            }
            tablesPrepared = true;
        }
    }

    private static long nowMs() { return System.currentTimeMillis(); }

    // ── effect ledger ──────────────────────────────────────────────────────

    /**
     * Idempotent. If an effect with this idempotency_key already exists,
     * returns the existing record. Otherwise creates a fresh PENDING row.
     *
     * <p>Refuses NON_IDEMPOTENT + INLINE — that combination is the bug
     * this whole project exists to prevent. Also refuses OUTBOX without a
     * {@code connector} name.
     */
    public EffectRecord beginEffect(
            String appName, String userId, String sessionId,
            String invocationId, int decisionIndex, String toolName, int callIndex,
            String requestJson, String customKey,
            String semantics, String dispatchMode,
            String businessKey, String connector) throws SQLException {

        if (semantics == null) semantics = EffectRecord.IDEMPOTENT;
        if (dispatchMode == null) dispatchMode = EffectRecord.INLINE;

        if (EffectRecord.NON_IDEMPOTENT.equals(semantics)
                && EffectRecord.INLINE.equals(dispatchMode)) {
            throw new IllegalArgumentException(
                "beginEffect: NON_IDEMPOTENT effects must use OUTBOX dispatch");
        }
        if (EffectRecord.OUTBOX.equals(dispatchMode)
                && (connector == null || connector.isEmpty())) {
            throw new IllegalArgumentException(
                "beginEffect: OUTBOX dispatch requires a `connector` name");
        }

        String key = (customKey != null && !customKey.isEmpty())
            ? customKey
            : invocationId + "/decision-" + decisionIndex + "/" + toolName + "/" + callIndex;

        prepareTables();
        try (Connection c = ds.getConnection()) {
            EffectRecord existing = selectEffect(c, appName, userId, sessionId, key);
            if (existing != null) return existing;

            long now = nowMs();
            String sql = "INSERT INTO tape_effects ("
                + " app_name, user_id, session_id, idempotency_key,"
                + " invocation_id, decision_index, tool_name, call_index,"
                + " status, semantics, dispatch_mode,"
                + " business_key, connector, external_ref,"
                + " dispatch_attempts, next_dispatch_at_ms,"
                + " dispatch_claimed_by, dispatch_claim_expires_at_ms,"
                + " last_dispatch_error,"
                + " request_json, response_json, error_json, ts_ms)"
                + " VALUES (?,?,?,?, ?,?,?,?, ?,?,?, ?,?,?, ?,?, ?,?, ?, ?,?,?,?)";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, appName);
                ps.setString(2, userId);
                ps.setString(3, sessionId);
                ps.setString(4, key);
                ps.setString(5, invocationId == null ? "" : invocationId);
                ps.setInt(6, decisionIndex);
                ps.setString(7, toolName == null ? "" : toolName);
                ps.setInt(8, callIndex);
                ps.setString(9, EffectRecord.PENDING);
                ps.setString(10, semantics);
                ps.setString(11, dispatchMode);
                setNullableString(ps, 12, emptyToNull(businessKey));
                setNullableString(ps, 13, emptyToNull(connector));
                setNullableString(ps, 14, null);
                ps.setInt(15, 0);
                ps.setLong(16, 0L);
                setNullableString(ps, 17, null);
                ps.setLong(18, 0L);
                setNullableString(ps, 19, null);
                setNullableString(ps, 20, requestJson);
                setNullableString(ps, 21, null);
                setNullableString(ps, 22, null);
                ps.setLong(23, now);
                try {
                    ps.executeUpdate();
                } catch (SQLException ex) {
                    // Most likely: (connector, business_key) UNIQUE clash.
                    // Mirror the Python ValueError message.
                    String msg = ex.getMessage() == null ? "" : ex.getMessage().toLowerCase();
                    if (msg.contains("unique") || msg.contains("constraint")
                            || msg.contains("duplicate")) {
                        throw new IllegalArgumentException(
                            "beginEffect: business_key already exists for "
                            + "connector=" + connector + ": " + ex.getMessage(), ex);
                    }
                    throw ex;
                }
            }
            return selectEffect(c, appName, userId, sessionId, key);
        }
    }

    /**
     * Flip an effect's terminal status. Idempotent — if the effect is
     * already CONFIRMED/FAILED/UNKNOWN this is a no-op that returns the
     * current row.
     */
    public Optional<EffectRecord> completeEffect(
            String appName, String userId, String sessionId, String idempotencyKey,
            String status, String responseJson, String errorJson) throws SQLException {

        if (!EffectRecord.CONFIRMED.equals(status)
                && !EffectRecord.FAILED.equals(status)
                && !EffectRecord.UNKNOWN.equals(status)) {
            throw new IllegalArgumentException(
                "completeEffect: invalid status " + status);
        }
        prepareTables();
        try (Connection c = ds.getConnection()) {
            EffectRecord row = selectEffect(c, appName, userId, sessionId, idempotencyKey);
            if (row == null) return Optional.empty();
            if (!EffectRecord.PENDING.equals(row.status())) return Optional.of(row);

            String sql = "UPDATE tape_effects SET"
                + " status=?, response_json=?, error_json=?, ts_ms=?,"
                + " dispatch_claimed_by=NULL, dispatch_claim_expires_at_ms=0"
                + " WHERE app_name=? AND user_id=? AND session_id=? AND idempotency_key=?";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, status);
                setNullableString(ps, 2, responseJson);
                setNullableString(ps, 3, errorJson);
                ps.setLong(4, nowMs());
                ps.setString(5, appName);
                ps.setString(6, userId);
                ps.setString(7, sessionId);
                ps.setString(8, idempotencyKey);
                ps.executeUpdate();
            }
            return Optional.ofNullable(selectEffect(c, appName, userId, sessionId, idempotencyKey));
        }
    }

    public Optional<EffectRecord> getEffect(
            String appName, String userId, String sessionId, String idempotencyKey) throws SQLException {
        prepareTables();
        try (Connection c = ds.getConnection()) {
            return Optional.ofNullable(selectEffect(c, appName, userId, sessionId, idempotencyKey));
        }
    }

    // ── outbox: dispatch claim (CAS) + attempt recording ──────────────────

    /** Result of {@link #claimEffectDispatch}: whether we acquired the lease,
     *  and the current row state regardless. */
    public record ClaimEffectResult(boolean acquired, Optional<EffectRecord> effect) {}

    /**
     * Atomic CAS lease on the dispatch slot. The CAS predicate: row is
     * PENDING + OUTBOX + dispatch-eligible (next_dispatch_at_ms ≤ now) +
     * the existing lease (if any) has expired. One conditional UPDATE; a
     * rowcount of 1 means we won.
     */
    public ClaimEffectResult claimEffectDispatch(
            String appName, String userId, String sessionId, String idempotencyKey,
            String claimer, long leaseTtlMs, long nowMsArg) throws SQLException {

        prepareTables();
        long now = nowMsArg > 0 ? nowMsArg : nowMs();
        long expires = now + leaseTtlMs;

        if (isSqlite) casLock.lock();
        try (Connection c = ds.getConnection()) {
            String sql = "UPDATE tape_effects SET"
                + " dispatch_claimed_by=?, dispatch_claim_expires_at_ms=?"
                + " WHERE app_name=? AND user_id=? AND session_id=? AND idempotency_key=?"
                + "   AND status=? AND dispatch_mode=?"
                + "   AND next_dispatch_at_ms <= ?"
                + "   AND (dispatch_claimed_by IS NULL OR dispatch_claimed_by = ''"
                + "        OR dispatch_claim_expires_at_ms <= ?)";
            boolean acquired;
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, claimer);
                ps.setLong(2, expires);
                ps.setString(3, appName);
                ps.setString(4, userId);
                ps.setString(5, sessionId);
                ps.setString(6, idempotencyKey);
                ps.setString(7, EffectRecord.PENDING);
                ps.setString(8, EffectRecord.OUTBOX);
                ps.setLong(9, now);
                ps.setLong(10, now);
                acquired = ps.executeUpdate() == 1;
            }
            EffectRecord row = selectEffect(c, appName, userId, sessionId, idempotencyKey);
            return new ClaimEffectResult(acquired, Optional.ofNullable(row));
        } finally {
            if (isSqlite) casLock.unlock();
        }
    }

    /**
     * Report a failed dispatch. {@code nextDispatchAtMs = 0} is the
     * load-bearing case: transitions PENDING → UNKNOWN so the reconciler
     * takes over. Any positive value reschedules a retry — effect stays
     * PENDING.
     */
    public Optional<EffectRecord> recordDispatchAttempt(
            String appName, String userId, String sessionId, String idempotencyKey,
            String error, long nextDispatchAtMs) throws SQLException {

        prepareTables();
        try (Connection c = ds.getConnection()) {
            EffectRecord row = selectEffect(c, appName, userId, sessionId, idempotencyKey);
            if (row == null) return Optional.empty();

            int attempts = row.dispatchAttempts() + 1;
            String newStatus = nextDispatchAtMs <= 0 ? EffectRecord.UNKNOWN : EffectRecord.PENDING;
            long newNext = nextDispatchAtMs <= 0 ? 0L : nextDispatchAtMs;

            String sql = "UPDATE tape_effects SET"
                + " dispatch_attempts=?, last_dispatch_error=?,"
                + " dispatch_claimed_by=NULL, dispatch_claim_expires_at_ms=0,"
                + " status=?, next_dispatch_at_ms=?, ts_ms=?"
                + " WHERE app_name=? AND user_id=? AND session_id=? AND idempotency_key=?";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setInt(1, attempts);
                setNullableString(ps, 2, error);
                ps.setString(3, newStatus);
                ps.setLong(4, newNext);
                ps.setLong(5, nowMs());
                ps.setString(6, appName);
                ps.setString(7, userId);
                ps.setString(8, sessionId);
                ps.setString(9, idempotencyKey);
                ps.executeUpdate();
            }
            return Optional.ofNullable(selectEffect(c, appName, userId, sessionId, idempotencyKey));
        }
    }

    /**
     * The reconciler's write path. Maps an {@code EffectResolution} →
     * {@code EffectStatus}; on DUPLICATE, atomically registers a
     * compensation obligation in the same transaction when
     * {@code compensateOnDuplicateKind} is non-empty.
     */
    public Optional<EffectRecord> recordExternalObservation(
            String appName, String userId, String sessionId, String idempotencyKey,
            String resolution, String externalRef,
            String responseJson, String errorJson,
            String compensateOnDuplicateKind) throws SQLException {

        prepareTables();
        try (Connection c = ds.getConnection()) {
            boolean priorAuto = c.getAutoCommit();
            c.setAutoCommit(false);
            try {
                EffectRecord row = selectEffect(c, appName, userId, sessionId, idempotencyKey);
                if (row == null) { c.commit(); return Optional.empty(); }

                long now = nowMs();
                String newStatus = row.status();
                String newExternalRef = row.externalRef();
                String newResponse = row.responseJson();
                String newError = row.errorJson();

                switch (resolution == null ? "" : resolution) {
                    case EffectResolution.CONFIRMED -> {
                        newStatus = EffectRecord.CONFIRMED;
                        if (externalRef != null && !externalRef.isEmpty()) newExternalRef = externalRef;
                        newResponse = responseJson;
                    }
                    case EffectResolution.FAILED -> {
                        newStatus = EffectRecord.FAILED;
                        newError = errorJson;
                    }
                    case EffectResolution.ABSENT -> {
                        if (EffectRecord.NON_IDEMPOTENT.equals(row.semantics())) {
                            newStatus = EffectRecord.UNKNOWN;
                        }
                        if (errorJson != null) newError = errorJson;
                    }
                    case EffectResolution.DUPLICATE -> {
                        newStatus = EffectRecord.CONFIRMED;
                        if (externalRef != null && !externalRef.isEmpty()) newExternalRef = externalRef;
                        newResponse = responseJson;
                        if (compensateOnDuplicateKind != null && !compensateOnDuplicateKind.isEmpty()) {
                            insertObligationInTxn(c,
                                appName, userId, sessionId, row.invocationId(),
                                row.idempotencyKey(), compensateOnDuplicateKind,
                                "{\"external_ref\":" + jsonString(externalRef != null ? externalRef : row.externalRef())
                                + ",\"reason\":\"duplicate observed by reconciler\"}",
                                null, 5, now);
                        }
                    }
                    case EffectResolution.STUCK -> {
                        newStatus = EffectRecord.FAILED;
                        newError = errorJson != null ? errorJson
                            : "{\"resolution\":\"stuck\",\"detail\":\"reconciler couldn't resolve\"}";
                    }
                    default -> throw new IllegalArgumentException(
                        "unknown resolution: " + resolution);
                }

                String sql = "UPDATE tape_effects SET"
                    + " status=?, external_ref=?, response_json=?, error_json=?, ts_ms=?"
                    + " WHERE app_name=? AND user_id=? AND session_id=? AND idempotency_key=?";
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    ps.setString(1, newStatus);
                    setNullableString(ps, 2, newExternalRef);
                    setNullableString(ps, 3, newResponse);
                    setNullableString(ps, 4, newError);
                    ps.setLong(5, now);
                    ps.setString(6, appName);
                    ps.setString(7, userId);
                    ps.setString(8, sessionId);
                    ps.setString(9, idempotencyKey);
                    ps.executeUpdate();
                }
                c.commit();
                return Optional.ofNullable(selectEffect(c, appName, userId, sessionId, idempotencyKey));
            } catch (SQLException | RuntimeException ex) {
                try { c.rollback(); } catch (SQLException ignore) {}
                throw ex;
            } finally {
                try { c.setAutoCommit(priorAuto); } catch (SQLException ignore) {}
            }
        }
    }

    // ── reconciler / outbox queues ────────────────────────────────────────

    /**
     * The reconciler's hot set: PENDING (optionally filtered by age) plus
     * UNKNOWN. Cross-session.
     */
    public List<EffectRecord> listPendingEffects(
            long olderThanMs, boolean includePending, boolean includeUnknown,
            int limit) throws SQLException {
        prepareTables();
        if (!includePending && !includeUnknown) return List.of();
        StringBuilder sql = new StringBuilder("SELECT * FROM tape_effects WHERE ");
        List<Object> args = new ArrayList<>();
        if (includePending && includeUnknown) {
            if (olderThanMs > 0) {
                sql.append("(status=? OR (status=? AND ts_ms<?))");
                args.add(EffectRecord.UNKNOWN);
                args.add(EffectRecord.PENDING);
                args.add(olderThanMs);
            } else {
                sql.append("status IN (?, ?)");
                args.add(EffectRecord.PENDING);
                args.add(EffectRecord.UNKNOWN);
            }
        } else if (includePending) {
            sql.append("status=?");
            args.add(EffectRecord.PENDING);
            if (olderThanMs > 0) { sql.append(" AND ts_ms<?"); args.add(olderThanMs); }
        } else {
            sql.append("status=?");
            args.add(EffectRecord.UNKNOWN);
        }
        sql.append(" ORDER BY ts_ms LIMIT ?");
        args.add(limit <= 0 ? 200 : limit);

        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(sql.toString())) {
            bindArgs(ps, args);
            return readEffects(ps);
        }
    }

    /**
     * The outbox dispatcher's hot set: PENDING + OUTBOX +
     * {@code next_dispatch_at_ms ≤ now} + lease is free or expired.
     */
    public List<EffectRecord> listEffectsToDispatch(
            long nowMsArg, String connector, int limit) throws SQLException {
        prepareTables();
        long now = nowMsArg > 0 ? nowMsArg : nowMs();
        StringBuilder sql = new StringBuilder(
            "SELECT * FROM tape_effects WHERE status=? AND dispatch_mode=?"
            + " AND next_dispatch_at_ms<=?"
            + " AND (dispatch_claimed_by IS NULL OR dispatch_claimed_by=''"
            + "      OR dispatch_claim_expires_at_ms<=?)");
        List<Object> args = new ArrayList<>();
        args.add(EffectRecord.PENDING);
        args.add(EffectRecord.OUTBOX);
        args.add(now);
        args.add(now);
        if (connector != null && !connector.isEmpty()) {
            sql.append(" AND connector=?");
            args.add(connector);
        }
        sql.append(" ORDER BY ts_ms LIMIT ?");
        args.add(limit <= 0 ? 200 : limit);

        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(sql.toString())) {
            bindArgs(ps, args);
            return readEffects(ps);
        }
    }

    // ── obligation ledger ─────────────────────────────────────────────────

    /** Idempotent on (session, effect_key, kind). */
    public ObligationRecord registerCompensation(
            String appName, String userId, String sessionId, String invocationId,
            String effectKey, String kind, String payloadJson,
            String compensatorRef, int maxAttempts) throws SQLException {
        prepareTables();
        try (Connection c = ds.getConnection()) {
            ObligationRecord existing = selectObligationByKindKey(
                c, appName, userId, sessionId, effectKey, kind);
            if (existing != null) return existing;

            long now = nowMs();
            long seq = insertObligationInTxn(c, appName, userId, sessionId,
                invocationId == null ? "" : invocationId,
                effectKey, kind, payloadJson, compensatorRef,
                maxAttempts <= 0 ? 5 : maxAttempts, now);
            return selectObligation(c, seq);
        }
    }

    public List<ObligationRecord> listObligations(
            String appName, String userId, String sessionId,
            boolean onlyUnresolved, String statusFilter) throws SQLException {
        prepareTables();
        StringBuilder sql = new StringBuilder(
            "SELECT * FROM tape_obligations WHERE app_name=? AND user_id=? AND session_id=?");
        List<Object> args = new ArrayList<>();
        args.add(appName); args.add(userId); args.add(sessionId);
        if (statusFilter != null && !statusFilter.isEmpty()) {
            sql.append(" AND status=?");
            args.add(statusFilter);
        } else if (onlyUnresolved) {
            sql.append(" AND status IN (?, ?)");
            args.add(ObligationRecord.PENDING);
            args.add(ObligationRecord.COMMITTED);
        }
        sql.append(" ORDER BY seq DESC");
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(sql.toString())) {
            bindArgs(ps, args);
            return readObligations(ps);
        }
    }

    /**
     * Cross-session drainer feed: PENDING-ready by default, plus
     * COMMITTED-expired (the lease-takeover path); optional include_stuck
     * for triage.
     */
    public List<ObligationRecord> listUnresolvedObligations(
            long nowMsArg, int limit,
            boolean includePending, boolean includeStuck,
            boolean includeCommittedExpired) throws SQLException {
        prepareTables();
        long now = nowMsArg > 0 ? nowMsArg : nowMs();
        List<String> ors = new ArrayList<>();
        List<Object> args = new ArrayList<>();
        if (includePending) {
            ors.add("(status=? AND next_attempt_at_ms<=?)");
            args.add(ObligationRecord.PENDING);
            args.add(now);
        }
        if (includeCommittedExpired) {
            ors.add("(status=? AND claim_expires_at_ms<=?)");
            args.add(ObligationRecord.COMMITTED);
            args.add(now);
        }
        if (includeStuck) {
            ors.add("status=?");
            args.add(ObligationRecord.STUCK);
        }
        if (ors.isEmpty()) return List.of();
        String sql = "SELECT * FROM tape_obligations WHERE ("
            + String.join(" OR ", ors)
            + ") ORDER BY seq DESC LIMIT ?";
        args.add(limit <= 0 ? 500 : limit);
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            bindArgs(ps, args);
            return readObligations(ps);
        }
    }

    /** Result of {@link #claimObligation}: whether we won, and the current row. */
    public record ClaimObligationResult(boolean acquired, Optional<ObligationRecord> obligation) {}

    /** Atomic CAS — single winner. Also reclaims COMMITTED rows whose
     *  {@code claim_expires_at_ms ≤ now}. */
    public ClaimObligationResult claimObligation(
            long seq, String claimer, long leaseTtlMs, long nowMsArg) throws SQLException {
        prepareTables();
        long now = nowMsArg > 0 ? nowMsArg : nowMs();
        long expires = now + leaseTtlMs;

        if (isSqlite) casLock.lock();
        try (Connection c = ds.getConnection()) {
            String sql = "UPDATE tape_obligations SET"
                + " status=?, claimed_by=?, claim_expires_at_ms=?"
                + " WHERE seq=? AND ("
                + "   (status=? AND next_attempt_at_ms<=?)"
                + "   OR (status=? AND claim_expires_at_ms<=?)"
                + " )";
            boolean acquired;
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, ObligationRecord.COMMITTED);
                ps.setString(2, claimer);
                ps.setLong(3, expires);
                ps.setLong(4, seq);
                ps.setString(5, ObligationRecord.PENDING);
                ps.setLong(6, now);
                ps.setString(7, ObligationRecord.COMMITTED);
                ps.setLong(8, now);
                acquired = ps.executeUpdate() == 1;
            }
            ObligationRecord row = selectObligation(c, seq);
            return new ClaimObligationResult(acquired, Optional.ofNullable(row));
        } finally {
            if (isSqlite) casLock.unlock();
        }
    }

    /** Report a failed compensation attempt. {@code nextAttemptAtMs=0}
     *  forces STUCK (terminal-now). Otherwise bump attempts; if at/over
     *  max, mark STUCK; else reschedule PENDING. */
    public Optional<ObligationRecord> recordObligationAttempt(
            long seq, String error, long nextAttemptAtMs) throws SQLException {
        prepareTables();
        try (Connection c = ds.getConnection()) {
            ObligationRecord row = selectObligation(c, seq);
            if (row == null) return Optional.empty();

            int attempts = row.attempts() + 1;
            String newStatus;
            long newNext;
            if (nextAttemptAtMs <= 0 || attempts >= row.maxAttempts()) {
                newStatus = ObligationRecord.STUCK;
                newNext = 0L;
            } else {
                newStatus = ObligationRecord.PENDING;
                newNext = nextAttemptAtMs;
            }
            String sql = "UPDATE tape_obligations SET"
                + " attempts=?, last_error=?, claimed_by=NULL, claim_expires_at_ms=0,"
                + " status=?, next_attempt_at_ms=?, ts_ms=?"
                + " WHERE seq=?";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setInt(1, attempts);
                setNullableString(ps, 2, error);
                ps.setString(3, newStatus);
                ps.setLong(4, newNext);
                ps.setLong(5, nowMs());
                ps.setLong(6, seq);
                ps.executeUpdate();
            }
            return Optional.ofNullable(selectObligation(c, seq));
        }
    }

    /** Terminal transition: COMPENSATED (success) or STUCK (failure). */
    public Optional<ObligationRecord> resolveObligation(
            long seq, String status, String resultJson) throws SQLException {
        if (!ObligationRecord.COMPENSATED.equals(status)
                && !ObligationRecord.STUCK.equals(status)) {
            throw new IllegalArgumentException(
                "resolveObligation: status must be COMPENSATED or STUCK, got " + status);
        }
        prepareTables();
        try (Connection c = ds.getConnection()) {
            ObligationRecord row = selectObligation(c, seq);
            if (row == null) return Optional.empty();
            String sql = "UPDATE tape_obligations SET"
                + " status=?, result_json=?, claimed_by=NULL, claim_expires_at_ms=0,"
                + " ts_ms=? WHERE seq=?";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, status);
                setNullableString(ps, 2, resultJson);
                ps.setLong(3, nowMs());
                ps.setLong(4, seq);
                ps.executeUpdate();
            }
            return Optional.ofNullable(selectObligation(c, seq));
        }
    }

    // ── timers ────────────────────────────────────────────────────────────

    /** Idempotent on (session, timer_id) — a second call returns the
     *  existing record (and does NOT overwrite fire_at_ms). */
    public TimerRecord setTimer(
            String appName, String userId, String sessionId,
            String timerId, long fireAtMs, String kind, String payloadJson) throws SQLException {
        prepareTables();
        try (Connection c = ds.getConnection()) {
            TimerRecord existing = selectTimer(c, appName, userId, sessionId, timerId);
            if (existing != null) return existing;

            long now = nowMs();
            String sql = "INSERT INTO tape_timers ("
                + " app_name, user_id, session_id, timer_id,"
                + " fire_at_ms, kind, payload_json, fired, created_at_ms)"
                + " VALUES (?,?,?,?, ?,?,?,?,?)";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, appName);
                ps.setString(2, userId);
                ps.setString(3, sessionId);
                ps.setString(4, timerId);
                ps.setLong(5, fireAtMs);
                ps.setString(6, kind == null ? "" : kind);
                setNullableString(ps, 7, payloadJson);
                ps.setInt(8, 0);
                ps.setLong(9, now);
                ps.executeUpdate();
            }
            return selectTimer(c, appName, userId, sessionId, timerId);
        }
    }

    /**
     * Returns timers with {@code fire_at_ms ≤ now} and {@code fired=false}.
     * If {@code claim=true}, atomically marks them fired in the same txn so
     * peer reactors don't re-fire.
     */
    public List<TimerRecord> listDueTimers(
            long nowMsArg, int limit, boolean claim) throws SQLException {
        prepareTables();
        long now = nowMsArg > 0 ? nowMsArg : nowMs();
        if (isSqlite && claim) casLock.lock();
        try (Connection c = ds.getConnection()) {
            boolean priorAuto = c.getAutoCommit();
            if (claim) c.setAutoCommit(false);
            try {
                String sql = "SELECT * FROM tape_timers WHERE fired=0 AND fire_at_ms<=?"
                    + " ORDER BY fire_at_ms LIMIT ?";
                List<TimerRecord> out;
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    ps.setLong(1, now);
                    ps.setInt(2, limit <= 0 ? 200 : limit);
                    out = readTimers(ps);
                }
                if (claim && !out.isEmpty()) {
                    String upd = "UPDATE tape_timers SET fired=1"
                        + " WHERE app_name=? AND user_id=? AND session_id=? AND timer_id=?";
                    try (PreparedStatement up = c.prepareStatement(upd)) {
                        for (TimerRecord t : out) {
                            up.setString(1, t.appName());
                            up.setString(2, t.userId());
                            up.setString(3, t.sessionId());
                            up.setString(4, t.timerId());
                            up.addBatch();
                        }
                        up.executeBatch();
                    }
                    c.commit();
                }
                return out;
            } catch (SQLException | RuntimeException ex) {
                if (claim) try { c.rollback(); } catch (SQLException ignore) {}
                throw ex;
            } finally {
                if (claim) try { c.setAutoCommit(priorAuto); } catch (SQLException ignore) {}
            }
        } finally {
            if (isSqlite && claim) casLock.unlock();
        }
    }

    public boolean cancelTimer(
            String appName, String userId, String sessionId, String timerId) throws SQLException {
        prepareTables();
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                "DELETE FROM tape_timers WHERE app_name=? AND user_id=? AND session_id=? AND timer_id=?")) {
            ps.setString(1, appName);
            ps.setString(2, userId);
            ps.setString(3, sessionId);
            ps.setString(4, timerId);
            return ps.executeUpdate() > 0;
        }
    }

    // ── reactive KV ───────────────────────────────────────────────────────

    /**
     * Optimistic-CAS write. {@code ifVersion < 0} disables CAS (last writer
     * wins). {@code ifVersion == current_version} advances; mismatch throws
     * {@link IllegalArgumentException}.
     */
    public ValueRecord writeValue(
            String namespace, String key, String valueJson,
            int ifVersion, String writer) throws SQLException {
        prepareTables();
        if (isSqlite) casLock.lock();
        try (Connection c = ds.getConnection()) {
            ValueRecord existing = selectValue(c, namespace, key);
            long now = nowMs();
            if (existing == null) {
                if (ifVersion >= 0 && ifVersion != 0) {
                    throw new IllegalArgumentException(
                        "writeValue: if_version=" + ifVersion
                        + " but no prior row exists (version 0)");
                }
                String sql = "INSERT INTO tape_values"
                    + " (namespace, key, value_json, version, ts_ms, writer, deleted)"
                    + " VALUES (?,?,?,?,?,?,?)";
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    ps.setString(1, namespace);
                    ps.setString(2, key);
                    setNullableString(ps, 3, valueJson);
                    ps.setInt(4, 1);
                    ps.setLong(5, now);
                    setNullableString(ps, 6, emptyToNull(writer));
                    ps.setInt(7, 0);
                    ps.executeUpdate();
                }
            } else {
                if (ifVersion >= 0 && ifVersion != existing.version()) {
                    throw new IllegalArgumentException(
                        "writeValue: stale CAS — if_version=" + ifVersion
                        + ", current=" + existing.version());
                }
                String sql = "UPDATE tape_values SET"
                    + " value_json=?, version=?, ts_ms=?, writer=?, deleted=0"
                    + " WHERE namespace=? AND key=?";
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    setNullableString(ps, 1, valueJson);
                    ps.setInt(2, existing.version() + 1);
                    ps.setLong(3, now);
                    String w = (writer != null && !writer.isEmpty()) ? writer : existing.writer();
                    setNullableString(ps, 4, emptyToNull(w));
                    ps.setString(5, namespace);
                    ps.setString(6, key);
                    ps.executeUpdate();
                }
            }
            return selectValue(c, namespace, key);
        } finally {
            if (isSqlite) casLock.unlock();
        }
    }

    public Optional<ValueRecord> getValue(String namespace, String key) throws SQLException {
        prepareTables();
        try (Connection c = ds.getConnection()) {
            return Optional.ofNullable(selectValue(c, namespace, key));
        }
    }

    // ── private helpers ───────────────────────────────────────────────────

    private static String emptyToNull(String s) {
        return (s == null || s.isEmpty()) ? null : s;
    }

    private static void setNullableString(PreparedStatement ps, int idx, String val) throws SQLException {
        if (val == null) ps.setNull(idx, java.sql.Types.VARCHAR);
        else ps.setString(idx, val);
    }

    private static void bindArgs(PreparedStatement ps, List<Object> args) throws SQLException {
        for (int i = 0; i < args.size(); i++) {
            Object v = args.get(i);
            int p = i + 1;
            if (v == null) ps.setObject(p, null);
            else if (v instanceof Long l) ps.setLong(p, l);
            else if (v instanceof Integer in) ps.setInt(p, in);
            else if (v instanceof String s) ps.setString(p, s);
            else ps.setObject(p, v);
        }
    }

    /** Minimal JSON-string escape for the inline payload we build on DUPLICATE.
     *  Just enough for {@code external_ref} values, which are short identifiers. */
    private static String jsonString(String s) {
        if (s == null) return "null";
        StringBuilder b = new StringBuilder(s.length() + 2);
        b.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\n': b.append("\\n"); break;
                case '\r': b.append("\\r"); break;
                case '\t': b.append("\\t"); break;
                default:
                    if (c < 0x20) b.append(String.format("\\u%04x", (int) c));
                    else b.append(c);
            }
        }
        b.append('"');
        return b.toString();
    }

    private EffectRecord selectEffect(
            Connection c, String appName, String userId, String sessionId, String key) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT * FROM tape_effects WHERE app_name=? AND user_id=? AND session_id=? AND idempotency_key=?")) {
            ps.setString(1, appName);
            ps.setString(2, userId);
            ps.setString(3, sessionId);
            ps.setString(4, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return mapEffect(rs);
            }
        }
    }

    private List<EffectRecord> readEffects(PreparedStatement ps) throws SQLException {
        List<EffectRecord> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(mapEffect(rs));
        }
        return out;
    }

    private static EffectRecord mapEffect(ResultSet rs) throws SQLException {
        return new EffectRecord(
            rs.getString("app_name"),
            rs.getString("user_id"),
            rs.getString("session_id"),
            rs.getString("idempotency_key"),
            rs.getString("invocation_id"),
            rs.getInt("decision_index"),
            rs.getString("tool_name"),
            rs.getInt("call_index"),
            rs.getString("status"),
            rs.getString("semantics"),
            rs.getString("dispatch_mode"),
            rs.getString("business_key"),
            rs.getString("connector"),
            rs.getString("external_ref"),
            rs.getInt("dispatch_attempts"),
            rs.getLong("next_dispatch_at_ms"),
            rs.getString("dispatch_claimed_by"),
            rs.getLong("dispatch_claim_expires_at_ms"),
            rs.getString("last_dispatch_error"),
            rs.getString("request_json"),
            rs.getString("response_json"),
            rs.getString("error_json"),
            rs.getLong("ts_ms")
        );
    }

    private ObligationRecord selectObligation(Connection c, long seq) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT * FROM tape_obligations WHERE seq=?")) {
            ps.setLong(1, seq);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return mapObligation(rs);
            }
        }
    }

    private ObligationRecord selectObligationByKindKey(
            Connection c, String appName, String userId, String sessionId,
            String effectKey, String kind) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT * FROM tape_obligations WHERE app_name=? AND user_id=? AND session_id=?"
                + " AND effect_key=? AND kind=?")) {
            ps.setString(1, appName);
            ps.setString(2, userId);
            ps.setString(3, sessionId);
            ps.setString(4, effectKey);
            ps.setString(5, kind);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return mapObligation(rs);
            }
        }
    }

    private List<ObligationRecord> readObligations(PreparedStatement ps) throws SQLException {
        List<ObligationRecord> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(mapObligation(rs));
        }
        return out;
    }

    private static ObligationRecord mapObligation(ResultSet rs) throws SQLException {
        return new ObligationRecord(
            rs.getLong("seq"),
            rs.getString("app_name"),
            rs.getString("user_id"),
            rs.getString("session_id"),
            rs.getString("invocation_id"),
            rs.getString("effect_key"),
            rs.getString("kind"),
            rs.getString("payload_json"),
            rs.getString("status"),
            rs.getInt("attempts"),
            rs.getInt("max_attempts"),
            rs.getLong("next_attempt_at_ms"),
            rs.getString("last_error"),
            rs.getString("claimed_by"),
            rs.getLong("claim_expires_at_ms"),
            rs.getString("compensator_ref"),
            rs.getString("result_json"),
            rs.getLong("ts_ms")
        );
    }

    private long insertObligationInTxn(
            Connection c, String appName, String userId, String sessionId, String invocationId,
            String effectKey, String kind, String payloadJson,
            String compensatorRef, int maxAttempts, long now) throws SQLException {
        String sql = "INSERT INTO tape_obligations ("
            + " app_name, user_id, session_id, invocation_id,"
            + " effect_key, kind, payload_json,"
            + " status, attempts, max_attempts, next_attempt_at_ms,"
            + " last_error, claimed_by, claim_expires_at_ms,"
            + " compensator_ref, result_json, ts_ms)"
            + " VALUES (?,?,?,?, ?,?,?, ?,?,?,?, ?,?,?, ?,?,?)";
        try (PreparedStatement ps = c.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, appName);
            ps.setString(2, userId);
            ps.setString(3, sessionId);
            ps.setString(4, invocationId == null ? "" : invocationId);
            ps.setString(5, effectKey);
            ps.setString(6, kind);
            setNullableString(ps, 7, payloadJson);
            ps.setString(8, ObligationRecord.PENDING);
            ps.setInt(9, 0);
            ps.setInt(10, maxAttempts);
            ps.setLong(11, now);
            setNullableString(ps, 12, null);
            setNullableString(ps, 13, null);
            ps.setLong(14, 0L);
            setNullableString(ps, 15, compensatorRef);
            setNullableString(ps, 16, null);
            ps.setLong(17, now);
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                if (keys.next()) return keys.getLong(1);
            }
        }
        // Fallback: most recent row for this composite key (Postgres/SQLite
        // both return generated keys, so this rarely fires).
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT seq FROM tape_obligations WHERE app_name=? AND user_id=? AND session_id=?"
                + " AND effect_key=? AND kind=? ORDER BY seq DESC LIMIT 1")) {
            ps.setString(1, appName);
            ps.setString(2, userId);
            ps.setString(3, sessionId);
            ps.setString(4, effectKey);
            ps.setString(5, kind);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getLong(1);
            }
        }
        throw new SQLException("insertObligationInTxn: generated seq not retrievable");
    }

    private TimerRecord selectTimer(
            Connection c, String appName, String userId, String sessionId, String timerId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT * FROM tape_timers WHERE app_name=? AND user_id=? AND session_id=? AND timer_id=?")) {
            ps.setString(1, appName);
            ps.setString(2, userId);
            ps.setString(3, sessionId);
            ps.setString(4, timerId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return mapTimer(rs);
            }
        }
    }

    private List<TimerRecord> readTimers(PreparedStatement ps) throws SQLException {
        List<TimerRecord> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(mapTimer(rs));
        }
        return out;
    }

    private static TimerRecord mapTimer(ResultSet rs) throws SQLException {
        return new TimerRecord(
            rs.getString("app_name"),
            rs.getString("user_id"),
            rs.getString("session_id"),
            rs.getString("timer_id"),
            rs.getLong("fire_at_ms"),
            rs.getString("kind"),
            rs.getString("payload_json"),
            rs.getInt("fired") != 0,
            rs.getLong("created_at_ms")
        );
    }

    private ValueRecord selectValue(Connection c, String namespace, String key) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT * FROM tape_values WHERE namespace=? AND key=?")) {
            ps.setString(1, namespace);
            ps.setString(2, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return new ValueRecord(
                    rs.getString("namespace"),
                    rs.getString("key"),
                    rs.getString("value_json"),
                    rs.getInt("version"),
                    rs.getLong("ts_ms"),
                    rs.getString("writer"),
                    rs.getInt("deleted") != 0
                );
            }
        }
    }
}
