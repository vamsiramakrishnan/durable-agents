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
 *
 * <p>For SQLite specifically: every public method's body acquires the
 * shared {@link #casLock} before touching JDBC. This mirrors how
 * SQLAlchemy's StaticPool funnels all queries through a single connection
 * in the Python reference — the natural-serialization fallback for an
 * embedded engine that doesn't have row-level locks. For Postgres the
 * lock is a no-op (real per-session connections + row-level locks do the
 * coordination).
 */
public final class TapeSessionService {

    private final DataSource ds;
    private final boolean isSqlite;
    private final ReentrantLock casLock = new ReentrantLock();
    private volatile boolean tablesPrepared = false;

    private void acquireIfSqlite() { if (isSqlite) casLock.lock(); }
    private void releaseIfSqlite() { if (isSqlite && casLock.isHeldByCurrentThread()) casLock.unlock(); }

    /**
     * Acquire a connection AND, for SQLite, the in-process serialization
     * lock — the lock is released when the returned Connection is closed.
     *
     * <p>This is the JDBC equivalent of "every query through one
     * connection" that SQLAlchemy's StaticPool gives the Python reference.
     * Without it, two threads each opening their own SQLite connection
     * collide on SQLITE_BUSY / SQLITE_LOCKED_SHAREDCACHE — the embedded
     * engine has no row-level locks to coordinate via. For Postgres the
     * lock is a no-op (real per-session connections + row locks do it).
     *
     * <p>Always use with try-with-resources.
     */
    private Connection acquireConn() throws SQLException {
        acquireIfSqlite();
        try {
            Connection real = ds.getConnection();
            if (!isSqlite) return real;
            // Proxy that delegates everything except close(); on close,
            // releases the real connection AND the cas lock.
            return (Connection) java.lang.reflect.Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] { Connection.class },
                (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        try { real.close(); } finally { releaseIfSqlite(); }
                        return null;
                    }
                    try {
                        return method.invoke(real, args);
                    } catch (java.lang.reflect.InvocationTargetException ite) {
                        throw ite.getTargetException();
                    }
                });
        } catch (SQLException e) {
            releaseIfSqlite();
            throw e;
        }
    }

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
            try (Connection c = acquireConn()) {
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
        try (Connection c = acquireConn()) {
            EffectRecord existing = selectEffect(c, appName, userId, sessionId, key);
            if (existing != null) return existing;

            // Snapshot fallback: the live row may have been pruned by the
            // compactor. If we have a terminal-state snapshot for this key,
            // synthesise the short-circuit EffectRecord from it so the
            // caller sees the same idempotent behaviour they'd see with the
            // row still present. No row is created here — the snapshot IS
            // the durable record. The live row is checked FIRST (above) so
            // a still-live row always wins over a (possibly stale) snapshot
            // entry.
            EffectRecord synthesised = synthesiseFromSnapshot(
                c, appName, userId, sessionId, key,
                toolName, callIndex, semantics, dispatchMode);
            if (synthesised != null) return synthesised;

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
        try (Connection c = acquireConn()) {
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
        try (Connection c = acquireConn()) {
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

        try (Connection c = acquireConn()) {
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
        try (Connection c = acquireConn()) {
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
        try (Connection c = acquireConn()) {
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

        try (Connection c = acquireConn();
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

        try (Connection c = acquireConn();
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
        try (Connection c = acquireConn()) {
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
        try (Connection c = acquireConn();
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
        try (Connection c = acquireConn();
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

        try (Connection c = acquireConn()) {
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
        }
    }

    /** Report a failed compensation attempt. {@code nextAttemptAtMs=0}
     *  forces STUCK (terminal-now). Otherwise bump attempts; if at/over
     *  max, mark STUCK; else reschedule PENDING. */
    public Optional<ObligationRecord> recordObligationAttempt(
            long seq, String error, long nextAttemptAtMs) throws SQLException {
        prepareTables();
        try (Connection c = acquireConn()) {
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
        try (Connection c = acquireConn()) {
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
        try (Connection c = acquireConn()) {
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
        try (Connection c = acquireConn()) {
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
        }
    }

    public boolean cancelTimer(
            String appName, String userId, String sessionId, String timerId) throws SQLException {
        prepareTables();
        try (Connection c = acquireConn();
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
        try (Connection c = acquireConn()) {
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
        }
    }

    public Optional<ValueRecord> getValue(String namespace, String key) throws SQLException {
        prepareTables();
        try (Connection c = acquireConn()) {
            return Optional.ofNullable(selectValue(c, namespace, key));
        }
    }

    // ── continue-as-new (mechanism #4 in the compaction roadmap) ──────────

    /** Result of {@link #continueAsNew}: per-counter audit. */
    public record ContinueAsNewResult(
            int effectsPruned, boolean stateWritten, int obligationsKept) {}

    /**
     * End one invocation chapter, start a new one in the same session,
     * with optional state carried forward — Java port of
     * {@code TapeSessionService.continue_as_new}.
     *
     * <p>Atomic — one transaction commits the prune + the carried-state
     * write together. {@code pruneOld=true} (default) deletes the old
     * invocation's terminal effects that aren't pinned by an active
     * obligation; the pinning predicate is the same {@code NOT EXISTS}
     * subquery the compactor uses (this is the load-bearing safety
     * invariant — copy it verbatim).
     *
     * <p>{@code obligationsKept} reports the number of still-active
     * obligations that pin old-invocation effects (the reason
     * continue_as_new didn't fully reset the slate). The pinning
     * relationship is via {@code effect_key} → {@code idempotency_key},
     * NOT via the obligation's own {@code invocation_id} — an obligation
     * registered under a later invocation can still pin an earlier
     * invocation's row.
     *
     * <p>{@code carriedState}, when non-null, is written to a
     * {@code tape_values} row at
     * {@code namespace='tape:continue-as-new:<sessionId>'},
     * {@code key=<newInvocationId>}. The new invocation can read it on
     * startup to pick up where the old one left off.
     */
    public ContinueAsNewResult continueAsNew(
            String appName, String userId, String sessionId,
            String oldInvocationId, String newInvocationId,
            String carriedStateJson, boolean pruneOld) throws SQLException {

        prepareTables();
        int effectsPruned = 0;
        int obligationsKept = 0;
        boolean stateWritten = false;
        long now = nowMs();

        try (Connection c = acquireConn()) {
            boolean priorAuto = c.getAutoCommit();
            c.setAutoCommit(false);
            try {
                if (pruneOld) {
                    // The NOT EXISTS pinning predicate — same shape as the
                    // compactor. The "pin" here is "an active obligation
                    // references this effect via effect_key=idempotency_key".
                    String del =
                        "DELETE FROM tape_effects" +
                        " WHERE app_name=? AND user_id=? AND session_id=?" +
                        "   AND invocation_id=?" +
                        "   AND status IN ('confirmed','failed')" +
                        "   AND NOT EXISTS (" +
                        "     SELECT 1 FROM tape_obligations o" +
                        "      WHERE o.session_id  = tape_effects.session_id" +
                        "        AND o.effect_key  = tape_effects.idempotency_key" +
                        "        AND o.status IN ('pending','committed'))";
                    try (PreparedStatement ps = c.prepareStatement(del)) {
                        ps.setString(1, appName);
                        ps.setString(2, userId);
                        ps.setString(3, sessionId);
                        ps.setString(4, oldInvocationId);
                        effectsPruned = ps.executeUpdate();
                    }

                    // Obligations that still pin OLD-invocation effects.
                    // Via effect_key membership — NOT via obligation
                    // invocation_id (matching Python exactly).
                    String pinnedSql =
                        "SELECT COUNT(*) FROM tape_obligations" +
                        " WHERE app_name=? AND user_id=? AND session_id=?" +
                        "   AND status IN ('pending','committed')" +
                        "   AND effect_key IN (" +
                        "     SELECT idempotency_key FROM tape_effects" +
                        "      WHERE app_name=? AND user_id=? AND session_id=?" +
                        "        AND invocation_id=?)";
                    try (PreparedStatement ps = c.prepareStatement(pinnedSql)) {
                        ps.setString(1, appName);
                        ps.setString(2, userId);
                        ps.setString(3, sessionId);
                        ps.setString(4, appName);
                        ps.setString(5, userId);
                        ps.setString(6, sessionId);
                        ps.setString(7, oldInvocationId);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) obligationsKept = rs.getInt(1);
                        }
                    }
                }

                if (carriedStateJson != null) {
                    String ns = "tape:continue-as-new:" + sessionId;
                    // Try insert; on UNIQUE clash do update — same "update on
                    // repeat" semantics as Python.
                    ValueRecord existing = selectValue(c, ns, newInvocationId);
                    if (existing == null) {
                        try (PreparedStatement ps = c.prepareStatement(
                                "INSERT INTO tape_values"
                                + " (namespace, key, value_json, version, ts_ms, writer, deleted)"
                                + " VALUES (?,?,?,?,?,?,?)")) {
                            ps.setString(1, ns);
                            ps.setString(2, newInvocationId);
                            setNullableString(ps, 3, carriedStateJson);
                            ps.setInt(4, 1);
                            ps.setLong(5, now);
                            ps.setString(6, "continue_as_new");
                            ps.setInt(7, 0);
                            ps.executeUpdate();
                        }
                    } else {
                        try (PreparedStatement ps = c.prepareStatement(
                                "UPDATE tape_values SET"
                                + " value_json=?, version=?, ts_ms=?, writer=?, deleted=0"
                                + " WHERE namespace=? AND key=?")) {
                            setNullableString(ps, 1, carriedStateJson);
                            ps.setInt(2, existing.version() + 1);
                            ps.setLong(3, now);
                            ps.setString(4, "continue_as_new");
                            ps.setString(5, ns);
                            ps.setString(6, newInvocationId);
                            ps.executeUpdate();
                        }
                    }
                    stateWritten = true;
                }

                c.commit();
            } catch (SQLException | RuntimeException ex) {
                try { c.rollback(); } catch (SQLException ignore) {}
                throw ex;
            } finally {
                try { c.setAutoCommit(priorAuto); } catch (SQLException ignore) {}
            }
        }
        return new ContinueAsNewResult(effectsPruned, stateWritten, obligationsKept);
    }

    // ── effect-ledger snapshot (mechanism #3 in the compaction roadmap) ───

    /** Result of {@link #takeSnapshot}: per-call audit counters. */
    public record TakeSnapshotResult(
            int captured, int mergedTotal, long upToTsMs) {}

    /** Decoded snapshot row returned by {@link #getSnapshot}.
     *
     *  <p>{@code effectsJson} is parsed into an in-memory map of
     *  {@code idempotency_key → CapturedEffect} — the caller never sees the
     *  raw JSON string. */
    public record EffectSnapshot(
            String appName,
            String userId,
            String sessionId,
            java.util.Map<String, Schema.CapturedEffect> effectsJson,
            long upToTsMs,
            int effectsCount,
            long createdAtMs,
            long updatedAtMs) {}

    /**
     * Capture terminal effects under this session into the per-session
     * snapshot row. Merges with the existing snapshot — re-calling this is
     * the cumulative way to keep the snapshot current as new effects
     * confirm.
     *
     * <p>After a snapshot, the compactor can safely prune the underlying
     * terminal effect rows: {@link #beginEffect} falls back to the
     * snapshot's JSON map for the idempotency-key short-circuit, so
     * re-dispatch is prevented even when the source row is gone.
     *
     * <p>{@code upToTsMs ≤ 0} captures everything with a terminal status
     * up to "now". Pass an explicit watermark to bound the snapshot for
     * large sessions; the watermark goes into the row so operators can see
     * how fresh the snapshot is.
     *
     * <p>Runs holding the existing CAS lock (the SQLite serialisation
     * mechanism), in one transaction with the merge → re-encode → write
     * cycle, mirroring the Python reference exactly.
     */
    public TakeSnapshotResult takeSnapshot(
            String appName, String userId, String sessionId,
            long upToTsMs) throws SQLException {
        prepareTables();
        long watermark = upToTsMs > 0 ? upToTsMs : nowMs();

        try (Connection c = acquireConn()) {
            boolean priorAuto = c.getAutoCommit();
            c.setAutoCommit(false);
            try {
                // 1) Read all terminal effects up to the watermark. The set
                //    is bounded by the session's effect count — operators
                //    with very large sessions should pass a watermark to
                //    limit the read window.
                java.util.Map<String, Schema.CapturedEffect> captured =
                    new java.util.LinkedHashMap<>();
                String readSql =
                    "SELECT * FROM tape_effects" +
                    " WHERE app_name=? AND user_id=? AND session_id=?" +
                    "   AND status IN ('confirmed','failed')" +
                    "   AND ts_ms <= ?";
                try (PreparedStatement ps = c.prepareStatement(readSql)) {
                    ps.setString(1, appName);
                    ps.setString(2, userId);
                    ps.setString(3, sessionId);
                    ps.setLong(4, watermark);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            EffectRecord r = mapEffect(rs);
                            captured.put(r.idempotencyKey(),
                                new Schema.CapturedEffect(
                                    r.status(),
                                    r.semantics(),
                                    r.dispatchMode(),
                                    r.businessKey(),
                                    r.connector(),
                                    r.externalRef(),
                                    r.requestJson(),
                                    r.responseJson(),
                                    r.errorJson(),
                                    r.invocationId(),
                                    r.decisionIndex(),
                                    r.toolName(),
                                    r.callIndex(),
                                    r.tsMs()));
                        }
                    }
                }

                // 2) Merge with the existing snapshot (last-write-wins per
                //    key). Decode → merge → re-encode → write back, same
                //    semantics as Python's `dict(snap).update(captured)`.
                Schema.EffectSnapshotRecord existing = selectSnapshotRow(
                    c, appName, userId, sessionId);
                java.util.Map<String, Schema.CapturedEffect> merged =
                    existing == null
                        ? new java.util.LinkedHashMap<>()
                        : decodeSnapshotMap(existing.effectsJson());
                merged.putAll(captured);
                String mergedJson = encodeSnapshotMap(merged);

                long now = nowMs();
                if (existing == null) {
                    String ins = "INSERT INTO tape_effect_snapshots ("
                        + " app_name, user_id, session_id,"
                        + " effects_json, up_to_ts_ms, effects_count,"
                        + " created_at_ms, updated_at_ms)"
                        + " VALUES (?,?,?, ?,?,?, ?,?)";
                    try (PreparedStatement ps = c.prepareStatement(ins)) {
                        ps.setString(1, appName);
                        ps.setString(2, userId);
                        ps.setString(3, sessionId);
                        ps.setString(4, mergedJson);
                        ps.setLong(5, watermark);
                        ps.setInt(6, merged.size());
                        ps.setLong(7, now);
                        ps.setLong(8, now);
                        ps.executeUpdate();
                    }
                } else {
                    long newWatermark = Math.max(existing.upToTsMs(), watermark);
                    String upd = "UPDATE tape_effect_snapshots SET"
                        + " effects_json=?, up_to_ts_ms=?, effects_count=?,"
                        + " updated_at_ms=?"
                        + " WHERE app_name=? AND user_id=? AND session_id=?";
                    try (PreparedStatement ps = c.prepareStatement(upd)) {
                        ps.setString(1, mergedJson);
                        ps.setLong(2, newWatermark);
                        ps.setInt(3, merged.size());
                        ps.setLong(4, now);
                        ps.setString(5, appName);
                        ps.setString(6, userId);
                        ps.setString(7, sessionId);
                        ps.executeUpdate();
                    }
                }

                c.commit();
                return new TakeSnapshotResult(
                    captured.size(), merged.size(), watermark);
            } catch (SQLException | RuntimeException ex) {
                try { c.rollback(); } catch (SQLException ignore) {}
                throw ex;
            } finally {
                try { c.setAutoCommit(priorAuto); } catch (SQLException ignore) {}
            }
        }
    }

    /**
     * Read the snapshot row for inspection / debugging. Returns the decoded
     * record (with {@code effectsJson} already a map). {@code Optional.empty()}
     * if no snapshot has been taken for this session.
     */
    public Optional<EffectSnapshot> getSnapshot(
            String appName, String userId, String sessionId) throws SQLException {
        prepareTables();
        try (Connection c = acquireConn()) {
            Schema.EffectSnapshotRecord row = selectSnapshotRow(
                c, appName, userId, sessionId);
            if (row == null) return Optional.empty();
            return Optional.of(new EffectSnapshot(
                row.appName(), row.userId(), row.sessionId(),
                decodeSnapshotMap(row.effectsJson()),
                row.upToTsMs(), row.effectsCount(),
                row.createdAtMs(), row.updatedAtMs()));
        }
    }

    /**
     * Acquire a lock-aware {@link Connection} for maintenance operations
     * (compaction, doctor) that need raw SQL access while honouring the
     * SQLite serialisation invariant. Public because the in-repo
     * {@code dev.tape.embedded.compact.Compactor} (a sibling package)
     * needs it, but intended for maintenance code only — application
     * code should go through the typed public methods on this class.
     * Mirrors how Python's {@code _write_lock()} +
     * {@code _rollback_on_exception_session()} compose for the compactor.
     */
    public Connection acquireMaintenanceConnection() throws SQLException {
        return acquireConn();
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

    // ── snapshot helpers ───────────────────────────────────────────────────

    /**
     * Look up a snapshot entry for {@code key} and synthesise a fresh
     * {@link EffectRecord} from it. Returns {@code null} when no snapshot
     * row exists, the blob is empty, or the key isn't present.
     *
     * <p>Mirrors the Python {@code begin_effect} "Snapshot fallback" block
     * verbatim: same field-by-field reconstruction, same default fallbacks
     * for the post-dispatch counters (zero attempts, no claim).
     */
    private EffectRecord synthesiseFromSnapshot(
            Connection c, String appName, String userId, String sessionId,
            String key, String toolName, int callIndex,
            String semantics, String dispatchMode) throws SQLException {
        Schema.EffectSnapshotRecord snapRow = selectSnapshotRow(
            c, appName, userId, sessionId);
        if (snapRow == null) return null;
        java.util.Map<String, Schema.CapturedEffect> map =
            decodeSnapshotMap(snapRow.effectsJson());
        Schema.CapturedEffect captured = map.get(key);
        if (captured == null) return null;
        return new EffectRecord(
            appName, userId, sessionId, key,
            captured.invocationId() == null ? "" : captured.invocationId(),
            captured.decisionIndex(),
            captured.toolName() == null ? toolName : captured.toolName(),
            captured.callIndex(),
            captured.status() == null ? EffectRecord.CONFIRMED : captured.status(),
            captured.semantics() == null ? semantics : captured.semantics(),
            captured.dispatchMode() == null ? dispatchMode : captured.dispatchMode(),
            captured.businessKey(),
            captured.connector(),
            captured.externalRef(),
            0, 0L, null, 0L, null,
            captured.requestJson(),
            captured.responseJson(),
            captured.errorJson(),
            captured.tsMs());
    }

    private Schema.EffectSnapshotRecord selectSnapshotRow(
            Connection c, String appName, String userId, String sessionId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT * FROM tape_effect_snapshots"
                + " WHERE app_name=? AND user_id=? AND session_id=?")) {
            ps.setString(1, appName);
            ps.setString(2, userId);
            ps.setString(3, sessionId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return new Schema.EffectSnapshotRecord(
                    rs.getString("app_name"),
                    rs.getString("user_id"),
                    rs.getString("session_id"),
                    rs.getString("effects_json"),
                    rs.getLong("up_to_ts_ms"),
                    rs.getInt("effects_count"),
                    rs.getLong("created_at_ms"),
                    rs.getLong("updated_at_ms"));
            }
        }
    }

    /** Decode the JSON blob into a key→CapturedEffect map. Returns an empty
     *  map for null/empty input, mirroring Python's {@code dict(snap or {})}.
     *
     *  <p>Field names in the JSON are snake_case (matches the Python writer
     *  exactly) so a snapshot blob is portable across SDKs. */
    private static java.util.Map<String, Schema.CapturedEffect> decodeSnapshotMap(
            String json) {
        java.util.LinkedHashMap<String, Schema.CapturedEffect> out =
            new java.util.LinkedHashMap<>();
        if (json == null || json.isEmpty()) return out;
        try {
            com.google.gson.JsonElement root =
                com.google.gson.JsonParser.parseString(json);
            if (!root.isJsonObject()) return out;
            for (java.util.Map.Entry<String, com.google.gson.JsonElement> e
                    : root.getAsJsonObject().entrySet()) {
                if (!e.getValue().isJsonObject()) continue;
                com.google.gson.JsonObject v = e.getValue().getAsJsonObject();
                out.put(e.getKey(), new Schema.CapturedEffect(
                    jsonStringOrNull(v, "status"),
                    jsonStringOrNull(v, "semantics"),
                    jsonStringOrNull(v, "dispatch_mode"),
                    jsonStringOrNull(v, "business_key"),
                    jsonStringOrNull(v, "connector"),
                    jsonStringOrNull(v, "external_ref"),
                    jsonNestedOrNull(v, "request_json"),
                    jsonNestedOrNull(v, "response_json"),
                    jsonNestedOrNull(v, "error_json"),
                    jsonStringOrEmpty(v, "invocation_id"),
                    jsonIntOrDefault(v, "decision_index", -1),
                    jsonStringOrNull(v, "tool_name"),
                    jsonIntOrDefault(v, "call_index", 0),
                    jsonLongOrDefault(v, "ts_ms", 0L)));
            }
        } catch (com.google.gson.JsonSyntaxException ignore) {
            // Malformed blob — treat as empty so we don't crash the
            // fallback path. The Python reference's `dict(snap or {})`
            // also coerces to empty on a non-dict.
        }
        return out;
    }

    /** Encode a key→CapturedEffect map to JSON. Keys are sorted by
     *  insertion order (LinkedHashMap); values use snake_case field names
     *  so the blob is portable across SDKs. */
    private static String encodeSnapshotMap(
            java.util.Map<String, Schema.CapturedEffect> map) {
        com.google.gson.JsonObject root = new com.google.gson.JsonObject();
        for (java.util.Map.Entry<String, Schema.CapturedEffect> e : map.entrySet()) {
            com.google.gson.JsonObject v = new com.google.gson.JsonObject();
            Schema.CapturedEffect c = e.getValue();
            v.add("status", strOrNull(c.status()));
            v.add("semantics", strOrNull(c.semantics()));
            v.add("dispatch_mode", strOrNull(c.dispatchMode()));
            v.add("business_key", strOrNull(c.businessKey()));
            v.add("connector", strOrNull(c.connector()));
            v.add("external_ref", strOrNull(c.externalRef()));
            v.add("request_json", parsedOrNull(c.requestJson()));
            v.add("response_json", parsedOrNull(c.responseJson()));
            v.add("error_json", parsedOrNull(c.errorJson()));
            v.add("invocation_id", strOrNull(c.invocationId()));
            v.addProperty("decision_index", c.decisionIndex());
            v.add("tool_name", strOrNull(c.toolName()));
            v.addProperty("call_index", c.callIndex());
            v.addProperty("ts_ms", c.tsMs());
            root.add(e.getKey(), v);
        }
        return root.toString();
    }

    private static com.google.gson.JsonElement strOrNull(String s) {
        return s == null
            ? com.google.gson.JsonNull.INSTANCE
            : new com.google.gson.JsonPrimitive(s);
    }

    /** A JSON-shaped column (e.g. {@code response_json}) is stored as a
     *  TEXT-encoded JSON string in the row. When we put it into the
     *  snapshot blob we want the JSON value INLINE (not a doubly-quoted
     *  string) so the blob matches the Python reference's shape exactly
     *  (where DynamicJSON deserialises to a dict before storage). */
    private static com.google.gson.JsonElement parsedOrNull(String jsonText) {
        if (jsonText == null) return com.google.gson.JsonNull.INSTANCE;
        try {
            return com.google.gson.JsonParser.parseString(jsonText);
        } catch (com.google.gson.JsonSyntaxException ex) {
            // Fall back to storing the raw string — defensive: we'd rather
            // round-trip imperfectly than crash the snapshot path.
            return new com.google.gson.JsonPrimitive(jsonText);
        }
    }

    private static String jsonStringOrNull(com.google.gson.JsonObject o, String k) {
        com.google.gson.JsonElement v = o.get(k);
        if (v == null || v.isJsonNull()) return null;
        if (v.isJsonPrimitive() && v.getAsJsonPrimitive().isString()) return v.getAsString();
        return v.toString();
    }

    private static String jsonStringOrEmpty(com.google.gson.JsonObject o, String k) {
        String v = jsonStringOrNull(o, k);
        return v == null ? "" : v;
    }

    /** For JSON-shaped columns: read the inline JSON value back out as a
     *  JSON-encoded string (the Java EffectRecord.responseJson() field is a
     *  String containing the column's TEXT contents). */
    private static String jsonNestedOrNull(com.google.gson.JsonObject o, String k) {
        com.google.gson.JsonElement v = o.get(k);
        if (v == null || v.isJsonNull()) return null;
        if (v.isJsonPrimitive() && v.getAsJsonPrimitive().isString()) {
            // Legacy / cross-SDK path: a Python writer may have stored the
            // raw JSON string here. Return it as-is.
            return v.getAsString();
        }
        return v.toString();
    }

    private static int jsonIntOrDefault(com.google.gson.JsonObject o, String k, int dflt) {
        com.google.gson.JsonElement v = o.get(k);
        if (v == null || v.isJsonNull()) return dflt;
        try { return v.getAsInt(); } catch (Exception ex) { return dflt; }
    }

    private static long jsonLongOrDefault(com.google.gson.JsonObject o, String k, long dflt) {
        com.google.gson.JsonElement v = o.get(k);
        if (v == null || v.isJsonNull()) return dflt;
        try { return v.getAsLong(); } catch (Exception ex) { return dflt; }
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
