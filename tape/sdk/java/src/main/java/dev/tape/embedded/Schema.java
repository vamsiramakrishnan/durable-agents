package dev.tape.embedded;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * DDL + record types for the four tables that back the embedded SQL path —
 * mirrors {@code tape_adk.schemas} in the Python package exactly: same
 * column names, same composite primary keys, same {@code UNIQUE(connector,
 * business_key)}, same indices. A Python writer + a Java reader (or vice
 * versa) against the same SQLite file are compatible.
 *
 * <p>The Python schema declares foreign keys into ADK's {@code sessions}
 * table because {@code TapeSessionService} extends ADK's
 * {@code DatabaseSessionService}. The Java standalone path has no ADK
 * sessions table, so we omit those FKs — the tables are self-contained.
 *
 * <p>JSON columns are stored as TEXT in SQLite (matching what SQLAlchemy's
 * {@code DynamicJSON} decoder uses) and JSONB in Postgres if you swap
 * datasources — both are TEXT-compatible on read.
 */
public final class Schema {

    private Schema() {}

    /** {@code tape_effects} — one row per effect (run/invocation × tool call). */
    public static final String DDL_EFFECTS =
        "CREATE TABLE IF NOT EXISTS tape_effects (\n" +
        "  app_name                       TEXT NOT NULL,\n" +
        "  user_id                        TEXT NOT NULL,\n" +
        "  session_id                     TEXT NOT NULL,\n" +
        "  idempotency_key                TEXT NOT NULL,\n" +
        "  invocation_id                  TEXT NOT NULL DEFAULT '',\n" +
        "  decision_index                 INTEGER NOT NULL DEFAULT -1,\n" +
        "  tool_name                      TEXT NOT NULL DEFAULT '',\n" +
        "  call_index                     INTEGER NOT NULL DEFAULT 0,\n" +
        "  status                         TEXT NOT NULL,\n" +
        "  semantics                      TEXT NOT NULL DEFAULT 'idempotent',\n" +
        "  dispatch_mode                  TEXT NOT NULL DEFAULT 'inline',\n" +
        "  business_key                   TEXT,\n" +
        "  connector                      TEXT,\n" +
        "  external_ref                   TEXT,\n" +
        "  dispatch_attempts              INTEGER NOT NULL DEFAULT 0,\n" +
        "  next_dispatch_at_ms            BIGINT NOT NULL DEFAULT 0,\n" +
        "  dispatch_claimed_by            TEXT,\n" +
        "  dispatch_claim_expires_at_ms   BIGINT NOT NULL DEFAULT 0,\n" +
        "  last_dispatch_error            TEXT,\n" +
        "  request_json                   TEXT,\n" +
        "  response_json                  TEXT,\n" +
        "  error_json                     TEXT,\n" +
        "  ts_ms                          BIGINT NOT NULL,\n" +
        "  PRIMARY KEY (app_name, user_id, session_id, idempotency_key),\n" +
        "  CONSTRAINT uq_tape_effects_connector_business_key UNIQUE (connector, business_key)\n" +
        ");";

    public static final String[] IDX_EFFECTS = new String[] {
        "CREATE INDEX IF NOT EXISTS ix_tape_effects_status_ts ON tape_effects (status, ts_ms);",
        "CREATE INDEX IF NOT EXISTS ix_tape_effects_dispatch_ready ON tape_effects (dispatch_mode, status, next_dispatch_at_ms);",
        "CREATE INDEX IF NOT EXISTS ix_tape_effects_invocation ON tape_effects (invocation_id);",
    };

    /** {@code tape_obligations} — one row per registered compensation. */
    public static final String DDL_OBLIGATIONS =
        "CREATE TABLE IF NOT EXISTS tape_obligations (\n" +
        "  seq                  INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
        "  app_name             TEXT NOT NULL,\n" +
        "  user_id              TEXT NOT NULL,\n" +
        "  session_id           TEXT NOT NULL,\n" +
        "  invocation_id        TEXT NOT NULL DEFAULT '',\n" +
        "  effect_key           TEXT NOT NULL,\n" +
        "  kind                 TEXT NOT NULL,\n" +
        "  payload_json         TEXT,\n" +
        "  status               TEXT NOT NULL DEFAULT 'pending',\n" +
        "  attempts             INTEGER NOT NULL DEFAULT 0,\n" +
        "  max_attempts         INTEGER NOT NULL DEFAULT 5,\n" +
        "  next_attempt_at_ms   BIGINT NOT NULL DEFAULT 0,\n" +
        "  last_error           TEXT,\n" +
        "  claimed_by           TEXT,\n" +
        "  claim_expires_at_ms  BIGINT NOT NULL DEFAULT 0,\n" +
        "  compensator_ref      TEXT,\n" +
        "  result_json          TEXT,\n" +
        "  ts_ms                BIGINT NOT NULL,\n" +
        "  CONSTRAINT uq_tape_obligations_effect_kind_per_session UNIQUE\n" +
        "    (app_name, user_id, session_id, effect_key, kind)\n" +
        ");";

    public static final String[] IDX_OBLIGATIONS = new String[] {
        "CREATE INDEX IF NOT EXISTS ix_tape_obligations_status_next ON tape_obligations (status, next_attempt_at_ms);",
        "CREATE INDEX IF NOT EXISTS ix_tape_obligations_status ON tape_obligations (status);",
    };

    /** {@code tape_timers} — server-side timer registry. */
    public static final String DDL_TIMERS =
        "CREATE TABLE IF NOT EXISTS tape_timers (\n" +
        "  app_name        TEXT NOT NULL,\n" +
        "  user_id         TEXT NOT NULL,\n" +
        "  session_id      TEXT NOT NULL,\n" +
        "  timer_id        TEXT NOT NULL,\n" +
        "  fire_at_ms      BIGINT NOT NULL,\n" +
        "  kind            TEXT NOT NULL,\n" +
        "  payload_json    TEXT,\n" +
        "  fired           INTEGER NOT NULL DEFAULT 0,\n" +
        "  created_at_ms   BIGINT NOT NULL,\n" +
        "  PRIMARY KEY (app_name, user_id, session_id, timer_id)\n" +
        ");";

    public static final String[] IDX_TIMERS = new String[] {
        "CREATE INDEX IF NOT EXISTS ix_tape_timers_due ON tape_timers (fired, fire_at_ms);",
        "CREATE INDEX IF NOT EXISTS ix_tape_timers_fire_at ON tape_timers (fire_at_ms);",
    };

    /** {@code tape_values} — reactive KV with per-key monotonic versioning. */
    public static final String DDL_VALUES =
        "CREATE TABLE IF NOT EXISTS tape_values (\n" +
        "  namespace   TEXT NOT NULL,\n" +
        "  key         TEXT NOT NULL,\n" +
        "  value_json  TEXT,\n" +
        "  version     INTEGER NOT NULL DEFAULT 0,\n" +
        "  ts_ms       BIGINT NOT NULL,\n" +
        "  writer      TEXT,\n" +
        "  deleted     INTEGER NOT NULL DEFAULT 0,\n" +
        "  PRIMARY KEY (namespace, key)\n" +
        ");";

    /**
     * {@code tape_effect_snapshots} — one row per session: a cumulative JSON
     * map of terminal-effect short-circuit data keyed by idempotency_key.
     *
     * <p>Mirrors {@code StorageEffectSnapshot} in {@code tape_adk/schemas.py}
     * exactly — same composite primary key (app_name, user_id, session_id),
     * same JSON-typed {@code effects_json} blob, same watermark / counter /
     * timestamp columns. A SQLite file written by one SDK is readable by
     * another.
     *
     * <p>Why this exists: the compactor prunes terminal effect rows past the
     * TTL so the journal doesn't grow without bound. But the
     * idempotency-key short-circuit in {@code beginEffect} reads
     * {@code tape_effects} — once pruned, {@code beginEffect} would create a
     * fresh PENDING row for the same key and re-dispatch the work.
     * Double-spend. The snapshot solves that by holding the <em>minimum data
     * the short-circuit needs</em> (status + response + the cross-run dedup
     * fields) in one row. {@code beginEffect} reads the live row first and
     * falls back to the snapshot when the live row is gone, so the compactor
     * is free to delete the underlying rows that were captured.
     *
     * <p>No FK to {@code tape_effects} — snapshot data outlives the source
     * rows by design.
     */
    public static final String DDL_EFFECT_SNAPSHOTS =
        "CREATE TABLE IF NOT EXISTS tape_effect_snapshots (\n" +
        "  app_name        TEXT NOT NULL,\n" +
        "  user_id         TEXT NOT NULL,\n" +
        "  session_id      TEXT NOT NULL,\n" +
        "  effects_json    TEXT,\n" +
        "  up_to_ts_ms     BIGINT NOT NULL DEFAULT 0,\n" +
        "  effects_count   INTEGER NOT NULL DEFAULT 0,\n" +
        "  created_at_ms   BIGINT NOT NULL,\n" +
        "  updated_at_ms   BIGINT NOT NULL,\n" +
        "  PRIMARY KEY (app_name, user_id, session_id)\n" +
        ");";

    /**
     * Create all five tables + indices on the given connection. Idempotent —
     * safe to call repeatedly.
     */
    public static void createAllTables(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(DDL_EFFECTS);
            for (String ix : IDX_EFFECTS) st.execute(ix);
            st.execute(DDL_OBLIGATIONS);
            for (String ix : IDX_OBLIGATIONS) st.execute(ix);
            st.execute(DDL_TIMERS);
            for (String ix : IDX_TIMERS) st.execute(ix);
            st.execute(DDL_VALUES);
            st.execute(DDL_EFFECT_SNAPSHOTS);
        }
    }

    // ── row records ────────────────────────────────────────────────────────

    /** Effect-ledger row. Mirrors {@code tape_adk.service.EffectRecord}. */
    public record EffectRecord(
            String appName,
            String userId,
            String sessionId,
            String idempotencyKey,
            String invocationId,
            int decisionIndex,
            String toolName,
            int callIndex,
            String status,
            String semantics,
            String dispatchMode,
            String businessKey,
            String connector,
            String externalRef,
            int dispatchAttempts,
            long nextDispatchAtMs,
            String dispatchClaimedBy,
            long dispatchClaimExpiresAtMs,
            String lastDispatchError,
            String requestJson,
            String responseJson,
            String errorJson,
            long tsMs) {

        public static final String PENDING   = "pending";
        public static final String CONFIRMED = "confirmed";
        public static final String FAILED    = "failed";
        public static final String UNKNOWN   = "unknown";

        public static final String IDEMPOTENT     = "idempotent";
        public static final String NON_IDEMPOTENT = "non_idempotent";
        public static final String OBSERVE_ONLY   = "observe_only";

        public static final String INLINE = "inline";
        public static final String OUTBOX = "outbox";
    }

    /** Obligation-ledger row. Mirrors {@code tape_adk.service.ObligationRecord}. */
    public record ObligationRecord(
            long seq,
            String appName,
            String userId,
            String sessionId,
            String invocationId,
            String effectKey,
            String kind,
            String payloadJson,
            String status,
            int attempts,
            int maxAttempts,
            long nextAttemptAtMs,
            String lastError,
            String claimedBy,
            long claimExpiresAtMs,
            String compensatorRef,
            String resultJson,
            long tsMs) {

        public static final String PENDING      = "pending";
        public static final String COMMITTED    = "committed";
        public static final String COMPENSATED  = "compensated";
        public static final String STUCK        = "stuck";
    }

    /** Timer-registry row. Mirrors {@code tape_adk.service.TimerRecord}. */
    public record TimerRecord(
            String appName,
            String userId,
            String sessionId,
            String timerId,
            long fireAtMs,
            String kind,
            String payloadJson,
            boolean fired,
            long createdAtMs) {}

    /** Reactive KV row. Mirrors {@code tape_adk.schemas.StorageValue}. */
    public record ValueRecord(
            String namespace,
            String key,
            String valueJson,
            int version,
            long tsMs,
            String writer,
            boolean deleted) {}

    /**
     * Effect-ledger snapshot row. Mirrors
     * {@code tape_adk.schemas.StorageEffectSnapshot}.
     *
     * <p>{@code effectsJson} is the raw JSON string stored in the DB column;
     * callers needing the decoded map should use
     * {@link TapeSessionService#getSnapshot} (which decodes on the way out)
     * or parse the string themselves.
     */
    public record EffectSnapshotRecord(
            String appName,
            String userId,
            String sessionId,
            String effectsJson,
            long upToTsMs,
            int effectsCount,
            long createdAtMs,
            long updatedAtMs) {}

    /**
     * The per-key value shape inside the snapshot JSON map — the minimum
     * data the {@code beginEffect} short-circuit needs to reconstruct a
     * synthetic {@link EffectRecord}.
     *
     * <p>Field names match Python's snake_case keys verbatim (the JSON
     * payload IS the cross-SDK contract): a snapshot blob written by the
     * Python SDK is read by the Java SDK and vice versa.
     */
    public record CapturedEffect(
            String status,
            String semantics,
            String dispatchMode,
            String businessKey,
            String connector,
            String externalRef,
            String requestJson,
            String responseJson,
            String errorJson,
            String invocationId,
            int decisionIndex,
            String toolName,
            int callIndex,
            long tsMs) {}

    // ── effect resolution (what reconciler observes upstream) ──────────────

    public static final class EffectResolution {
        public static final String CONFIRMED = "confirmed";
        public static final String FAILED    = "failed";
        public static final String ABSENT    = "absent";
        public static final String DUPLICATE = "duplicate";
        public static final String STUCK     = "stuck";

        private EffectResolution() {}
    }
}
