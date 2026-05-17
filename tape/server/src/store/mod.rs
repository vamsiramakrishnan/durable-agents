//! The storage layer.
//!
//! Tape's logical operations — begin a run, record a decision, begin/complete an
//! effect, register a compensation, charge a budget, append an event, … — are a
//! trait, [`RunStore`]. Backends implement it however they like:
//!
//!   * [`sql`] — a SQL implementation (`SqlRunStore` over a tiny `SqlBackend`),
//!     with `SqliteBackend` and `PostgresBackend` (and AlloyDB, which is
//!     PostgreSQL-wire-compatible — see `open`);
//!   * [`bigtable`] — a Cloud Bigtable implementation, for very-high-scale /
//!     GCP-native deployments. Bigtable doesn't speak SQL; it speaks single-row
//!     atomic mutations, `CheckAndMutate` (the effect-key dedup), and
//!     `ReadModifyWrite` counters (the per-run `seq`) — which is exactly the
//!     shape `RunStore` needs.
//!
//! The store is chosen by **URL at deploy time** (`TAPE_STORE` / `--store`):
//!
//!   sqlite:./tape.db                          file-backed SQLite (default)
//!   sqlite::memory:  |  memory                ephemeral, for tests/demos
//!   postgres://user:pass@host:5432/db         pooled PostgreSQL
//!   alloydb://user:pass@host:5432/db          AlloyDB via the Auth Proxy (Postgres wire)
//!   bigtable://project/instance/table         Cloud Bigtable
//!     (BIGTABLE_EMULATOR_HOST is honoured, so `bigtable://demo/demo/tape` works
//!      against the local emulator)
//!
//! Run N replicas of the server against a shared network store (Postgres/AlloyDB/
//! Bigtable) behind a load balancer and you have a horizontally scalable Tape:
//! the server holds no state between requests; "one driver per run at a time" is
//! the per-run lease; every mutating RPC is idempotent, so a double-drive is
//! harmless.

pub mod bigtable;
pub mod sql;

use std::sync::Arc;

use async_trait::async_trait;

use crate::pb::*;

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("tape store: {0}")]
    Msg(String),
}
impl StoreError {
    pub fn msg(s: impl Into<String>) -> Self {
        StoreError::Msg(s.into())
    }
}

/// Tape's logical operations. A backend that implements this is a complete
/// storage layer. All ordering, sequencing and journaling lives inside the
/// implementation — the gRPC layer above is pure plumbing.
#[async_trait]
pub trait RunStore: Send + Sync {
    // ── run lifecycle ───────────────────────────────────────────────────────
    async fn begin_run(&self, app: &str, user: &str, session: &str, invocation: &str,
                       lease_owner: &str, lease_ttl_ms: i64) -> StoreResult<BeginRunResponse>;
    async fn resume_run(&self, run_id: &str, lease_owner: &str, lease_ttl_ms: i64)
        -> StoreResult<Option<RunState>>;
    async fn end_run(&self, run_id: &str, status: i32, detail_json: &str) -> StoreResult<Option<RunState>>;
    async fn get_run(&self, run_id: &str) -> StoreResult<Option<RunState>>;
    async fn list_runs_to_recover(&self, now_ms: i64, limit: i64) -> StoreResult<Vec<RunState>>;
    async fn journal_range(&self, run_id: &str, from_seq: i64) -> StoreResult<Vec<JournalEntry>>;

    // ── decision ledger ─────────────────────────────────────────────────────
    async fn record_decision(&self, run_id: &str, decision_index: i64, model: &str,
                             request_json: &str, response_json: &str, rationale: &str,
                             policy_version: &str) -> StoreResult<DecisionRecord>;
    async fn get_decision(&self, run_id: &str, decision_index: i64) -> StoreResult<Option<DecisionRecord>>;

    // ── effect ledger ───────────────────────────────────────────────────────
    /// Returns the effect row. If it didn't exist, it was created with status
    /// PENDING (and committed) before this returns — that's the outbox move.
    async fn begin_effect(&self, run_id: &str, decision_index: i64, tool_name: &str, call_index: i32,
                          request_json: &str, custom_key: &str) -> StoreResult<EffectRecord>;
    async fn complete_effect(&self, run_id: &str, key: &str, status: i32, response_json: &str,
                             error_json: &str) -> StoreResult<Option<EffectRecord>>;
    async fn get_effect(&self, run_id: &str, key: &str) -> StoreResult<Option<EffectRecord>>;
    async fn reconcile_effect(&self, run_id: &str, key: &str, resolved_status: i32,
                              response_json: &str, error_json: &str) -> StoreResult<Option<EffectRecord>>;

    // ── obligations / compensation ──────────────────────────────────────────
    /// Register an obligation in PENDING with `next_attempt_at_ms = now` (immediately
    /// eligible for the drainer). Idempotent on (run_id, effect_key, kind): a
    /// repeat returns the existing row untouched. `max_attempts == 0` uses the
    /// server default (5). `compensator_ref` is optional — empty when the inverse
    /// is resolved from the in-process registry by `kind`.
    async fn register_compensation(&self, run_id: &str, effect_key: &str, kind: &str,
                                   payload_json: &str, compensator_ref: &str,
                                   max_attempts: i32) -> StoreResult<ObligationRecord>;
    /// LIFO list, per-run. `status_filter` is the obligation status to match
    /// exactly (or `ObligationStatus::Unspecified` / 0 for any). `only_unresolved`
    /// is a shorthand that excludes COMPENSATED and STUCK.
    async fn list_obligations(&self, run_id: &str, only_unresolved: bool,
                              status_filter: i32) -> StoreResult<Vec<ObligationRecord>>;
    /// Cross-run drainer feed. Returns rows in `(next_attempt_at_ms, ts_ms)` order
    /// (oldest-first) so the drainer makes forward progress on the most-stale work.
    async fn list_unresolved_obligations(&self, now_ms: i64, include_pending: bool,
                                         include_stuck: bool, include_committed_expired: bool,
                                         limit: i64) -> StoreResult<Vec<ObligationRecord>>;
    /// Atomic CAS lease: PENDING (with `next_attempt_at_ms <= now`) — or COMMITTED
    /// with `claim_expires_at_ms <= now` (a stale lease) — becomes COMMITTED with
    /// `claimed_by = claimer` and `claim_expires_at_ms = now + lease_ttl_ms`.
    /// Returns `(true, row)` on success, `(false, current_row)` on contention.
    async fn claim_obligation(&self, run_id: &str, obligation_seq: i64, claimer: &str,
                              lease_ttl_ms: i64, now_ms: i64)
        -> StoreResult<(bool, Option<ObligationRecord>)>;
    /// Record a failed attempt. Bumps `attempts`, sets `last_error`, clears the
    /// lease, and either schedules a retry (status → PENDING with `next_attempt_at_ms`)
    /// or marks terminally STUCK (when `next_attempt_at_ms == 0`, or when the
    /// new attempt count >= `max_attempts`).
    async fn record_obligation_attempt(&self, run_id: &str, obligation_seq: i64,
                                       error: &str, next_attempt_at_ms: i64)
        -> StoreResult<Option<ObligationRecord>>;
    /// Terminal transition: COMPENSATED | STUCK. Stores `result_json` and clears
    /// the lease fields.
    async fn resolve_obligation(&self, run_id: &str, obligation_seq: i64, status: i32,
                                result_json: &str) -> StoreResult<Option<ObligationRecord>>;

    // ── budget ──────────────────────────────────────────────────────────────
    async fn set_budget(&self, run_id: &str, usd_cap: f64, token_cap: i64) -> StoreResult<BudgetState>;
    async fn get_budget(&self, run_id: &str) -> StoreResult<BudgetState>;
    async fn charge_budget(&self, run_id: &str, usd: f64, tokens: i64) -> StoreResult<BudgetState>;

    // ── gates / signals ─────────────────────────────────────────────────────
    /// Returns (delivered, resolution_json). If not delivered, the run is parked
    /// (status WAITING) on `gate_name`.
    async fn await_signal(&self, run_id: &str, gate_name: &str, payload_json: &str)
        -> StoreResult<(bool, String)>;
    /// Returns (run_id, run_status). `run_id` may be "" to route by (app, user, session).
    async fn send_signal(&self, run_id: &str, app: &str, user: &str, session: &str, gate_name: &str,
                         resolution_json: &str) -> StoreResult<(String, i32)>;

    // ── ADK SessionService shim ─────────────────────────────────────────────
    async fn create_session(&self, app: &str, user: &str, session: &str, state_json: &str)
        -> StoreResult<Session>;
    async fn get_session(&self, app: &str, user: &str, session: &str, max_events: i64)
        -> StoreResult<Option<Session>>;
    async fn list_sessions(&self, app: &str, user: &str) -> StoreResult<Vec<Session>>;
    async fn delete_session(&self, app: &str, user: &str, session: &str) -> StoreResult<bool>;
    /// Appends the ADK event and applies `state_delta_json` to the session state
    /// in one transaction. Returns (event, last_update_time_ms).
    async fn append_event(&self, app: &str, user: &str, session: &str, event: EventRecord,
                          state_delta_json: &str) -> StoreResult<(EventRecord, i64)>;

    // ── reconciliation ──────────────────────────────────────────────────────
    /// PENDING (older than `older_than_ms`, or all if 0) and/or UNKNOWN effects,
    /// for the reconciler reactor to resolve via the registered status checks.
    async fn list_pending_effects(&self, older_than_ms: i64, include_pending: bool,
                                  include_unknown: bool, limit: i64) -> StoreResult<Vec<EffectRecord>>;

    // ── timers ──────────────────────────────────────────────────────────────
    async fn set_timer(&self, run_id: &str, timer_id: &str, fire_at_ms: i64, kind: &str,
                       payload_json: &str) -> StoreResult<TimerRecord>;
    async fn cancel_timer(&self, run_id: &str, timer_id: &str) -> StoreResult<bool>;
    /// Due (fire_at_ms <= now), not-yet-fired timers. If `claim`, each returned
    /// timer is atomically marked fired so a peer reactor won't re-fire it.
    async fn list_due_timers(&self, now_ms: i64, limit: i64, claim: bool) -> StoreResult<Vec<TimerRecord>>;

    // ── the WAL tail (cross-run journal feed) ───────────────────────────────
    /// Journal entries with `ts_ms >= from_ts_ms`, optionally filtered to one
    /// run and/or one kind, ordered by (ts_ms, run_id, seq). The reactor / fanout
    /// re-polls with `from_ts_ms` = the last seen ts to follow the WAL.
    async fn events_since(&self, from_ts_ms: i64, run_id: &str, kind: &str, limit: i64)
        -> StoreResult<Vec<EventEntry>>;

    // ── event bus (see design-principles/tape-event-bus.md) ─────────────────
    /// Subject-filtered, global_seq-cursored WAL tail. Returns events with
    /// `global_seq > from_global_seq` whose `subject` matches `subject_pattern`
    /// (`*` = one segment, `**` = trailing segments; empty pattern = all).
    /// Ordered by `global_seq ASC`, capped at `limit`.
    async fn events_by_subject(&self, from_global_seq: i64, subject_pattern: &str, limit: i64)
        -> StoreResult<Vec<EventEntry>> {
        let _ = (from_global_seq, subject_pattern, limit);
        Err(StoreError::msg("events_by_subject: not supported on this backend"))
    }
    /// Raw journal tail used by the matcher: every entry with
    /// `global_seq > from_global_seq`, ordered by `global_seq ASC`, capped.
    async fn read_journal_after(&self, from_global_seq: i64, limit: i64)
        -> StoreResult<Vec<EventEntry>> {
        // Default to events_by_subject with the match-all pattern.
        self.events_by_subject(from_global_seq, "", limit).await
    }

    // ── reactions ───────────────────────────────────────────────────────────
    async fn register_reaction(&self, r: &Reaction) -> StoreResult<Reaction> {
        let _ = r;
        Err(StoreError::msg("register_reaction: not supported on this backend"))
    }
    async fn deregister_reaction(&self, reaction_id: &str) -> StoreResult<bool> {
        let _ = reaction_id;
        Err(StoreError::msg("deregister_reaction: not supported on this backend"))
    }
    async fn list_reactions(&self, subject_pattern: &str) -> StoreResult<Vec<Reaction>> {
        let _ = subject_pattern;
        Err(StoreError::msg("list_reactions: not supported on this backend"))
    }
    async fn get_reaction_cursor(&self, reaction_id: &str, shard: i32) -> StoreResult<i64> {
        let _ = (reaction_id, shard);
        Ok(0)
    }
    async fn set_reaction_cursor(&self, reaction_id: &str, shard: i32, global_seq: i64,
                                 now_ms: i64) -> StoreResult<()> {
        let _ = (reaction_id, shard, global_seq, now_ms);
        Err(StoreError::msg("set_reaction_cursor: not supported on this backend"))
    }

    // ── tasks ───────────────────────────────────────────────────────────────
    async fn create_task(&self, t: &Task) -> StoreResult<Task> {
        let _ = t;
        Err(StoreError::msg("create_task: not supported on this backend"))
    }
    async fn claim_tasks(&self, reaction_id: &str, shard: i32, owner: &str, lease_ms: i64,
                         max: i32, now_ms: i64) -> StoreResult<Vec<Task>> {
        let _ = (reaction_id, shard, owner, lease_ms, max, now_ms);
        Err(StoreError::msg("claim_tasks: not supported on this backend"))
    }
    async fn complete_task(&self, task_id: &str, owner: &str) -> StoreResult<Option<Task>> {
        let _ = (task_id, owner);
        Err(StoreError::msg("complete_task: not supported on this backend"))
    }
    async fn nack_task(&self, task_id: &str, owner: &str, error: &str, permanent: bool,
                       now_ms: i64) -> StoreResult<Option<Task>> {
        let _ = (task_id, owner, error, permanent, now_ms);
        Err(StoreError::msg("nack_task: not supported on this backend"))
    }
    async fn list_tasks(&self, reaction_id: &str, status: i32, limit: i64) -> StoreResult<Vec<Task>> {
        let _ = (reaction_id, status, limit);
        Err(StoreError::msg("list_tasks: not supported on this backend"))
    }
    /// The newest PENDING task for `(reaction_id, subject)` (or None). Used by
    /// the matcher to drive server-side debounce: if a PENDING task is still
    /// within its debounce window, the next match coalesces into it rather
    /// than inserting a new row.
    async fn find_pending_task_for_subject(&self, reaction_id: &str, subject: &str)
        -> StoreResult<Option<Task>> {
        let _ = (reaction_id, subject);
        Err(StoreError::msg("find_pending_task_for_subject: not supported on this backend"))
    }
    /// Conditional UPDATE for server-side debounce coalescing. Only succeeds
    /// if the task is still PENDING (so a race with `claim_tasks` falls back
    /// to inserting a fresh task). `attempts`, `status`, `next_attempt_at_ms`,
    /// and `created_at_ms` are NOT touched — the task keeps its existing
    /// schedule; only the payload, source pointer, and trace pair advance to
    /// the latest entry. Returns the updated row, or None if the row was no
    /// longer PENDING (lost the race).
    async fn coalesce_task(&self, task_id: &str, source_global_seq: i64, payload_json: &str,
                           trace_id: &str, parent_span_id: &str) -> StoreResult<Option<Task>> {
        let _ = (task_id, source_global_seq, payload_json, trace_id, parent_span_id);
        Err(StoreError::msg("coalesce_task: not supported on this backend"))
    }

    /// Returns a hook that is notified whenever the journal grows. Used by the
    /// matcher and the SubscribeBySubject stream to avoid busy-polling. Default:
    /// a freshly created Notify that never fires (back-compat for backends that
    /// can't push wake-ups).
    fn journal_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::new(tokio::sync::Notify::new())
    }

    // ── reactive key-value store (treatise §IX ⑥: coordination through state) ─
    /// Atomic versioned write. Returns the new ValueRecord. If `if_version >= 0`,
    /// the write is conditional on `current_version == if_version` (CAS); a
    /// mismatch returns `Err(StoreError::Msg("version conflict ..."))`. Also
    /// emits a journal entry of kind "value" so the WAL tail catches it.
    async fn write_value(&self, namespace: &str, key: &str, value_json: &str,
                         if_version: i64, writer: &str) -> StoreResult<ValueRecord>;
    /// Read the current value (or None if absent / tombstoned).
    async fn get_value(&self, namespace: &str, key: &str) -> StoreResult<Option<ValueRecord>>;
    /// Return the current record if its version > `from_version`. The streaming
    /// `WatchValue` handler polls this with the last-seen version each tick.
    async fn get_value_if_newer(&self, namespace: &str, key: &str, from_version: i64)
        -> StoreResult<Option<ValueRecord>>;
    /// Delete (writes a tombstone with `deleted = true`, incrementing version,
    /// so subscribers see the delete as a ValueEvent).
    async fn delete_value(&self, namespace: &str, key: &str) -> StoreResult<(bool, i64)>;
}

/// Parse a store URL and build the matching `RunStore`, migrated and ready.
pub async fn open(url: &str) -> StoreResult<Arc<dyn RunStore>> {
    if url == "memory" || url == ":memory:" || url == "sqlite::memory:" {
        return Ok(Arc::new(sql::SqlRunStore::sqlite_memory().await?));
    }
    if let Some(path) = url.strip_prefix("sqlite:") {
        let path = path.strip_prefix("//").unwrap_or(path);
        return if path == ":memory:" {
            Ok(Arc::new(sql::SqlRunStore::sqlite_memory().await?))
        } else {
            Ok(Arc::new(sql::SqlRunStore::sqlite_file(path).await?))
        };
    }
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        return Ok(Arc::new(sql::SqlRunStore::postgres(url).await?));
    }
    if let Some(rest) = url.strip_prefix("alloydb://") {
        // AlloyDB speaks the PostgreSQL wire protocol. Run the AlloyDB Auth Proxy
        // (`alloydb-auth-proxy "projects/…/instances/…" --port 5432`) and point
        // this at 127.0.0.1:5432 — or use a private-IP host directly.
        return Ok(Arc::new(sql::SqlRunStore::postgres(&format!("postgres://{rest}")).await?));
    }
    if let Some(rest) = url.strip_prefix("bigtable://") {
        // bigtable://<project>/<instance>/<table>
        let parts: Vec<&str> = rest.splitn(3, '/').collect();
        if parts.len() != 3 || parts.iter().any(|p| p.is_empty()) {
            return Err(StoreError::msg("bigtable URL must be bigtable://<project>/<instance>/<table>"));
        }
        let s = Arc::new(bigtable::BigtableRunStore::connect(parts[0], parts[1], parts[2]).await?);
        // Start the optional change-stream wake-up watcher. Best-effort: if the
        // table doesn't have change-streams enabled (or we're on the emulator,
        // which doesn't implement the RPC), the watcher logs a warning and
        // returns; the matcher continues polling — see
        // `crate::bigtable_change_stream` for the contract.
        crate::bigtable_change_stream::spawn(s.clone(), s.notify_handle());
        return Ok(s);
    }
    if url.starts_with("spanner://") {
        return Err(StoreError::msg(
            "the spanner store is not yet implemented — add an impl of RunStore in src/store/ \
             (see design-principles/tape.md §12)"));
    }
    // Bare path -> a SQLite file.
    Ok(Arc::new(sql::SqlRunStore::sqlite_file(url).await?))
}

pub fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// `idempotency_key` derivation: name the decision, not its inputs (treatise §IX ①).
pub fn derive_key(run_id: &str, decision_index: i64, tool: &str, call_index: i32) -> String {
    if decision_index < 0 {
        format!("{run_id}/no-decision/{tool}/{call_index}")
    } else {
        format!("{run_id}/decision-{decision_index}/{tool}/{call_index}")
    }
}

/// Shallow-merge `delta` (a JSON object) into `base`; a `null` value deletes.
pub fn merge_json(base: &str, delta: &str) -> String {
    let mut b: serde_json::Value = serde_json::from_str(base).unwrap_or(serde_json::json!({}));
    let d: serde_json::Value = serde_json::from_str(delta).unwrap_or(serde_json::json!({}));
    if let (Some(bo), Some(dobj)) = (b.as_object_mut(), d.as_object()) {
        for (k, v) in dobj {
            if v.is_null() {
                bo.remove(k);
            } else {
                bo.insert(k.clone(), v.clone());
            }
        }
    }
    b.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_names_the_decision_not_the_inputs() {
        assert_eq!(derive_key("r", 0, "execute_sweep", 0), "r/decision-0/execute_sweep/0");
        assert_eq!(derive_key("r", 0, "execute_sweep", 0), derive_key("r", 0, "execute_sweep", 0));
        assert_ne!(derive_key("r", 0, "execute_sweep", 0), derive_key("r", 0, "execute_sweep", 1));
        assert_eq!(derive_key("r", -1, "post_gl", 0), "r/no-decision/post_gl/0");
    }

    #[test]
    fn merge_json_shallow_with_null_delete() {
        let m = merge_json("{\"a\":1,\"b\":2}", "{\"b\":3,\"c\":4}");
        let v: serde_json::Value = serde_json::from_str(&m).unwrap();
        assert_eq!(v["a"], 1); assert_eq!(v["b"], 3); assert_eq!(v["c"], 4);
        let m = merge_json("{\"a\":1,\"b\":2}", "{\"b\":null}");
        let v: serde_json::Value = serde_json::from_str(&m).unwrap();
        assert!(v.get("b").is_none()); assert_eq!(v["a"], 1);
    }
}
