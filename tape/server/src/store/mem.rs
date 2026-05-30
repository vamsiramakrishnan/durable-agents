//! `MemRunStore` — a pure-async, in-memory [`RunStore`] for sim tests.
//!
//! Phase 2.6 of TapeChaos. The production [`SqlRunStore`](super::sql) routes
//! every operation through [`tokio::task::spawn_blocking`] so the synchronous
//! `rusqlite` work doesn't stall the async runtime. Madsim under
//! `--cfg madsim` refuses real OS threads, so that path can't run there.
//!
//! `MemRunStore` is the bridge: a [`HashMap`]-backed implementation of the
//! [`RunStore`] trait with no `spawn_blocking`, no thread pool, and no IO.
//! Use it from `sim.rs` to drive the real [`TapeService`](crate::service)
//! under [`madsim`]'s deterministic runtime.
//!
//! Honest scope: this store implements the methods the sim test suite
//! exercises (run lifecycle, decisions, effects, obligations, journal range,
//! lease-claim CAS). The rest of the trait — outbox dispatch, sessions,
//! gates, timers, budgets, the KV store, reactions, tasks — returns
//! [`StoreError::msg("MemRunStore: not implemented in sim")`]. This is
//! deliberately not a production store; the production paths stay on
//! [`SqlRunStore`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::{CompactReport, RunIdentity, RunStore, StoreError, StoreResult};
use crate::pb::*;
use crate::store::now_ms;

/// Per-store in-memory state. One `Arc<Mutex<>>` shared across the trait
/// methods; all ops are short, so contention isn't a concern in tests.
#[derive(Default)]
struct Inner {
    runs: HashMap<String, RunState>,
    /// (app, user, session, invocation_id) → run_id, for the BeginRun
    /// idempotency check — a second BeginRun with the same invocation_id
    /// returns the existing run.
    invocations: HashMap<(String, String, String, String), String>,
    /// run_id → next seq.
    seq: HashMap<String, i64>,
    /// run_id → ordered journal entries.
    journal: HashMap<String, Vec<JournalEntry>>,
    /// (run_id, decision_index) → DecisionRecord.
    decisions: HashMap<(String, i64), DecisionRecord>,
    /// (run_id, idempotency_key) → EffectRecord.
    effects: HashMap<(String, String), EffectRecord>,
    /// (run_id, seq) → ObligationRecord.
    obligations: HashMap<(String, i64), ObligationRecord>,
    /// Next obligation seq per run.
    next_obligation_seq: HashMap<String, i64>,
}

/// The sim-only in-memory [`RunStore`].
pub struct MemRunStore {
    inner: Arc<Mutex<Inner>>,
    notify: Arc<tokio::sync::Notify>,
}

impl Default for MemRunStore {
    fn default() -> Self { Self::new() }
}

impl MemRunStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn next_seq(&self, run_id: &str) -> i64 {
        let mut g = self.inner.lock().unwrap();
        let n = g.seq.entry(run_id.into()).or_insert(0);
        *n += 1;
        *n
    }

    fn append_journal(&self, run_id: &str, kind: &str, payload_json: String) {
        let seq = self.next_seq(run_id);
        let ts = now_ms();
        let entry = JournalEntry {
            seq, kind: kind.into(), payload_json, ts_ms: ts,
            global_seq: 0, subject: String::new(), schema_version: 1,
            trace_id: String::new(), span_id: String::new(), parent_span_id: String::new(),
        };
        self.inner.lock().unwrap()
            .journal.entry(run_id.into()).or_default().push(entry);
        self.notify.notify_waiters();
    }
}

/// Helper macro for "this method isn't part of the sim test surface".
macro_rules! not_in_sim {
    ($name:literal) => {
        Err(StoreError::msg(concat!("MemRunStore: not implemented in sim — ", $name)))
    };
}

#[async_trait]
impl RunStore for MemRunStore {
    // ── run lifecycle ───────────────────────────────────────────────────────

    async fn begin_run(&self, app: &str, user: &str, session: &str, invocation: &str,
                       identity: &RunIdentity<'_>,
                       lease_owner: &str, lease_ttl_ms: i64) -> StoreResult<BeginRunResponse> {
        let inv_key = (app.into(), user.into(), session.into(), invocation.into());
        let now = now_ms();
        let lease_expires = now + lease_ttl_ms;

        let mut g = self.inner.lock().unwrap();
        if let Some(existing_id) = g.invocations.get(&inv_key).cloned() {
            // Resumed: same invocation_id ⇒ same run. Refresh the lease.
            let next_seq = *g.seq.get(&existing_id).unwrap_or(&0);
            if let Some(r) = g.runs.get_mut(&existing_id) {
                r.lease_owner = lease_owner.into();
                r.lease_expires_at_ms = lease_expires;
                r.status = RunStatus::Running as i32;
                let status = r.status;
                return Ok(BeginRunResponse {
                    run_id: existing_id,
                    resumed: true,
                    next_seq: next_seq + 1,
                    status,
                });
            }
        }
        let run_id = format!("r-{}", uuid::Uuid::new_v4());
        let scopes: Vec<String> = if identity.scopes_json.is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(identity.scopes_json).unwrap_or_default()
        };
        let labels: std::collections::HashMap<String, String> = if identity.labels_json.is_empty() {
            std::collections::HashMap::new()
        } else {
            serde_json::from_str(identity.labels_json).unwrap_or_default()
        };
        let run = RunState {
            run_id: run_id.clone(),
            app_name: app.into(),
            user_id: user.into(),
            session_id: session.into(),
            invocation_id: invocation.into(),
            status: RunStatus::Running as i32,
            seq_cursor: 0,
            lease_owner: lease_owner.into(),
            lease_expires_at_ms: lease_expires,
            started_at_ms: now,
            ended_at_ms: 0,
            waiting_on_gate: String::new(),
            tenant_id: identity.tenant_id.into(),
            actor: identity.actor.into(),
            subject: identity.subject.into(),
            agent_id: identity.agent_id.into(),
            aiplex_instance_id: identity.aiplex_instance_id.into(),
            gateway_route: identity.gateway_route.into(),
            scopes,
            labels,
        };
        g.runs.insert(run_id.clone(), run);
        g.invocations.insert(inv_key, run_id.clone());
        drop(g);

        // First journal entry — the "running" run row.
        let payload = format!(
            r#"{{"app":"{}","user":"{}","session":"{}","status":"running","run_id":"{}"}}"#,
            esc(app), esc(user), esc(session), esc(&run_id));
        self.append_journal(&run_id, "run", payload);

        Ok(BeginRunResponse {
            run_id, resumed: false, next_seq: 1,
            status: RunStatus::Running as i32,
        })
    }

    async fn resume_run(&self, run_id: &str, lease_owner: &str, lease_ttl_ms: i64)
        -> StoreResult<Option<RunState>> {
        let mut g = self.inner.lock().unwrap();
        let Some(r) = g.runs.get_mut(run_id) else { return Ok(None) };
        r.lease_owner = lease_owner.into();
        r.lease_expires_at_ms = now_ms() + lease_ttl_ms;
        r.status = RunStatus::Running as i32;
        Ok(Some(r.clone()))
    }

    async fn end_run(&self, run_id: &str, status: i32, detail_json: &str) -> StoreResult<Option<RunState>> {
        let (cloned, payload);
        {
            let mut g = self.inner.lock().unwrap();
            let Some(r) = g.runs.get_mut(run_id) else { return Ok(None) };
            r.status = status;
            r.ended_at_ms = now_ms();
            cloned = r.clone();
            let label = match RunStatus::try_from(status).unwrap_or(RunStatus::Terminal) {
                RunStatus::Terminal => "terminal",
                RunStatus::Failed => "failed",
                RunStatus::Cancelled => "cancelled",
                RunStatus::Stuck => "stuck",
                _ => "terminal",
            };
            payload = format!(
                r#"{{"status":"{}","run_id":"{}","detail":{}}}"#,
                label, esc(run_id),
                if detail_json.is_empty() { "{}".into() } else { detail_json.to_string() });
        }
        self.append_journal(run_id, "run", payload);
        Ok(Some(cloned))
    }

    async fn get_run(&self, run_id: &str) -> StoreResult<Option<RunState>> {
        Ok(self.inner.lock().unwrap().runs.get(run_id).cloned())
    }

    async fn list_runs_to_recover(&self, now_ms: i64, limit: i64) -> StoreResult<Vec<RunState>> {
        let g = self.inner.lock().unwrap();
        let mut out: Vec<RunState> = g.runs.values()
            .filter(|r| {
                let status = RunStatus::try_from(r.status).unwrap_or(RunStatus::Unspecified);
                // RUNNABLE or RUNNING-with-stale-lease.
                matches!(status, RunStatus::Runnable)
                    || (matches!(status, RunStatus::Running) && r.lease_expires_at_ms <= now_ms)
            })
            .cloned()
            .collect();
        if limit > 0 { out.truncate(limit as usize); }
        Ok(out)
    }

    async fn journal_range(&self, run_id: &str, from_seq: i64) -> StoreResult<Vec<JournalEntry>> {
        let g = self.inner.lock().unwrap();
        let entries: Vec<JournalEntry> = g.journal.get(run_id)
            .map(|v| v.iter().filter(|e| e.seq >= from_seq).cloned().collect())
            .unwrap_or_default();
        Ok(entries)
    }

    // ── compaction (PR 13) — sim store impl ────────────────────────────────
    //
    // MemRunStore is a sim/test backend; compaction here zeroes payloads
    // on decisions + effects in-place. Settlement check is identical to
    // the SQL impl (no open obligations, no UNKNOWN effects).
    async fn list_compactable_runs(&self, before_ms: i64, limit: i64) -> StoreResult<Vec<RunState>> {
        let g = self.inner.lock().unwrap();
        let mut out = Vec::new();
        for r in g.runs.values() {
            let settled = matches!(
                RunStatus::try_from(r.status).unwrap_or(RunStatus::Unspecified),
                RunStatus::Terminal | RunStatus::Failed
                    | RunStatus::Cancelled | RunStatus::Stuck);
            if !settled || r.ended_at_ms == 0 || r.ended_at_ms >= before_ms {
                continue;
            }
            out.push(r.clone());
            if limit > 0 && (out.len() as i64) >= limit {
                break;
            }
        }
        Ok(out)
    }

    async fn compact_run(&self, run_id: &str, _ts_ms: i64) -> StoreResult<CompactReport> {
        let mut g = self.inner.lock().unwrap();
        // Settlement check
        let unknown = g.effects.values()
            .filter(|e| e.run_id == run_id && e.status == EffectStatus::Unknown as i32)
            .count();
        if unknown > 0 {
            return Err(StoreError::msg(format!(
                "compact_run: run {run_id} has {unknown} UNKNOWN effect(s); not settled")));
        }
        let open_oblig = g.obligations.values()
            .filter(|o| o.run_id == run_id && matches!(
                ObligationStatus::try_from(o.status).unwrap_or(ObligationStatus::Unspecified),
                ObligationStatus::Pending | ObligationStatus::Committed))
            .count();
        if open_oblig > 0 {
            return Err(StoreError::msg(format!(
                "compact_run: run {run_id} has {open_oblig} open obligation(s); not settled")));
        }

        let mut bytes_saved = 0i64;
        let mut decisions_zeroed = 0i64;
        for d in g.decisions.values_mut() {
            if d.run_id == run_id && (!d.request_json.is_empty() || !d.response_json.is_empty()) {
                bytes_saved += (d.request_json.len() + d.response_json.len() + d.rationale.len()) as i64;
                d.request_json = String::new();
                d.response_json = String::new();
                d.rationale = String::new();
                decisions_zeroed += 1;
            }
        }
        let mut effects_zeroed = 0i64;
        for e in g.effects.values_mut() {
            if e.run_id == run_id && (!e.request_json.is_empty()
                || !e.response_json.is_empty() || !e.error_json.is_empty()) {
                bytes_saved += (e.request_json.len() + e.response_json.len() + e.error_json.len()) as i64;
                e.request_json = String::new();
                e.response_json = String::new();
                e.error_json = String::new();
                effects_zeroed += 1;
            }
        }
        Ok(CompactReport {
            decisions_zeroed, effects_zeroed, bytes_saved, already_compacted: false,
        })
    }

    // ── decision ledger ─────────────────────────────────────────────────────

    async fn record_decision(&self, run_id: &str, decision_index: i64, model: &str,
                             request_json: &str, response_json: &str, rationale: &str,
                             policy_version: &str) -> StoreResult<DecisionRecord> {
        let seq = self.next_seq(run_id);
        let rec = DecisionRecord {
            run_id: run_id.into(), seq, decision_index,
            model: model.into(), request_json: request_json.into(),
            response_json: response_json.into(), rationale: rationale.into(),
            policy_version: policy_version.into(), ts_ms: now_ms(),
        };
        self.inner.lock().unwrap()
            .decisions.insert((run_id.into(), decision_index), rec.clone());
        let payload = format!(
            r#"{{"run_id":"{}","decision_index":{},"model":"{}","policy_version":"{}","rationale":""}}"#,
            esc(run_id), decision_index, esc(model), esc(policy_version));
        // Re-use append_journal but with the seq we already minted: rewrite
        // the entry's seq to match (avoid double-allocating a sequence).
        let ts = now_ms();
        let entry = JournalEntry {
            seq, kind: "decision".into(), payload_json: payload, ts_ms: ts,
            global_seq: 0, subject: String::new(), schema_version: 1,
            trace_id: String::new(), span_id: String::new(), parent_span_id: String::new(),
        };
        {
            let mut g = self.inner.lock().unwrap();
            g.journal.entry(run_id.into()).or_default().push(entry);
        }
        self.notify.notify_waiters();
        Ok(rec)
    }

    async fn get_decision(&self, run_id: &str, decision_index: i64) -> StoreResult<Option<DecisionRecord>> {
        Ok(self.inner.lock().unwrap()
            .decisions.get(&(run_id.into(), decision_index)).cloned())
    }

    // ── effect ledger ───────────────────────────────────────────────────────

    async fn begin_effect(&self, run_id: &str, decision_index: i64, tool_name: &str, call_index: i32,
                          request_json: &str, custom_key: &str,
                          semantics: i32, dispatch_mode: i32,
                          business_key: &str, connector: &str,
                          scope: &str) -> StoreResult<EffectRecord> {
        // Server-side safety: NON_IDEMPOTENT + INLINE is refused (matches SQL store).
        if semantics == EffectSemantics::NonIdempotent as i32
            && dispatch_mode == EffectDispatchMode::Inline as i32 {
            return Err(StoreError::msg(
                "begin_effect: NON_IDEMPOTENT effects must use OUTBOX dispatch"));
        }
        // PR 12 item B: non_idempotent + empty scope is refused on the
        // wire, not just at SDK decoration time (matches SQL store).
        if semantics == EffectSemantics::NonIdempotent as i32 && scope.is_empty() {
            return Err(StoreError::denied(
                "<required>",
                "non_idempotent effects must declare an authorization scope on the wire"));
        }
        // Authorization check (defence-in-depth). Mirrors the SQL store: empty
        // scope skips, non-empty scope must appear in the run's grants.
        if !scope.is_empty() {
            let granted: Vec<String> = {
                let g = self.inner.lock().unwrap();
                g.runs.get(run_id).map(|r| r.scopes.clone()).unwrap_or_default()
            };
            if !granted.iter().any(|s| s == scope) {
                return Err(StoreError::denied(
                    scope,
                    format!("effect {tool_name} requires scope {scope:?} not present on run {run_id}"),
                ));
            }
        }
        let key = if !custom_key.is_empty() {
            custom_key.to_string()
        } else {
            format!("{}/decision-{}/{}/{}", run_id, decision_index, tool_name, call_index)
        };

        let mut g = self.inner.lock().unwrap();
        if let Some(existing) = g.effects.get(&(run_id.into(), key.clone())) {
            return Ok(existing.clone());
        }
        let seq = {
            let n = g.seq.entry(run_id.into()).or_insert(0);
            *n += 1;
            *n
        };
        let rec = EffectRecord {
            run_id: run_id.into(), seq, decision_index,
            tool_name: tool_name.into(), idempotency_key: key.clone(),
            status: EffectStatus::Pending as i32,
            request_json: request_json.into(),
            response_json: String::new(), error_json: String::new(),
            ts_ms: now_ms(),
            semantics, dispatch_mode,
            business_key: business_key.into(), connector: connector.into(),
            dispatch_attempts: 0, next_dispatch_at_ms: 0,
            external_ref: String::new(),
            dispatch_claimed_by: String::new(),
            dispatch_claim_expires_at_ms: 0,
            last_dispatch_error: String::new(),
            scope: scope.into(),
        };
        g.effects.insert((run_id.into(), key.clone()), rec.clone());
        let entry = JournalEntry {
            seq, kind: "effect".into(),
            payload_json: format!(
                r#"{{"run_id":"{}","status":"pending","tool":"{}","idempotency_key":"{}","decision_index":{},"semantics":{},"dispatch_mode":{},"business_key":"{}","connector":"{}","scope":"{}"}}"#,
                esc(run_id), esc(tool_name), esc(&key), decision_index,
                semantics, dispatch_mode, esc(business_key), esc(connector), esc(scope)),
            ts_ms: now_ms(),
            global_seq: 0, subject: String::new(), schema_version: 1,
            trace_id: String::new(), span_id: String::new(), parent_span_id: String::new(),
        };
        g.journal.entry(run_id.into()).or_default().push(entry);
        drop(g);
        self.notify.notify_waiters();
        Ok(rec)
    }

    async fn complete_effect(&self, run_id: &str, key: &str, status: i32, response_json: &str,
                             error_json: &str) -> StoreResult<Option<EffectRecord>> {
        let mut g = self.inner.lock().unwrap();
        let Some(eff) = g.effects.get_mut(&(run_id.into(), key.into())) else { return Ok(None) };
        eff.status = status;
        eff.response_json = response_json.into();
        eff.error_json = error_json.into();
        eff.ts_ms = now_ms();
        let cloned = eff.clone();
        let seq = {
            let n = g.seq.entry(run_id.into()).or_insert(0);
            *n += 1;
            *n
        };
        let label = match EffectStatus::try_from(status).unwrap_or(EffectStatus::Confirmed) {
            EffectStatus::Confirmed => "confirmed",
            EffectStatus::Failed => "failed",
            EffectStatus::Unknown => "unknown",
            _ => "pending",
        };
        let entry = JournalEntry {
            seq, kind: "effect".into(),
            payload_json: format!(
                r#"{{"run_id":"{}","status":"{}","tool":"{}","idempotency_key":"{}"}}"#,
                esc(run_id), label, esc(&cloned.tool_name), esc(key)),
            ts_ms: now_ms(),
            global_seq: 0, subject: String::new(), schema_version: 1,
            trace_id: String::new(), span_id: String::new(), parent_span_id: String::new(),
        };
        g.journal.entry(run_id.into()).or_default().push(entry);
        drop(g);
        self.notify.notify_waiters();
        Ok(Some(cloned))
    }

    async fn get_effect(&self, run_id: &str, key: &str) -> StoreResult<Option<EffectRecord>> {
        Ok(self.inner.lock().unwrap()
            .effects.get(&(run_id.into(), key.into())).cloned())
    }

    async fn reconcile_effect(&self, _run_id: &str, _key: &str, _resolved_status: i32,
                              _response_json: &str, _error_json: &str) -> StoreResult<Option<EffectRecord>> {
        not_in_sim!("reconcile_effect")
    }

    // ── outbox dispatch — stubbed ──────────────────────────────────────────

    async fn list_effects_to_dispatch(&self, _now_ms: i64, _connector: &str, _limit: i64)
        -> StoreResult<Vec<EffectRecord>> { not_in_sim!("list_effects_to_dispatch") }
    async fn claim_effect_dispatch(&self, _run_id: &str, _key: &str, _claimer: &str,
                                   _lease_ttl_ms: i64, _now_ms: i64)
        -> StoreResult<(bool, Option<EffectRecord>)> { not_in_sim!("claim_effect_dispatch") }
    async fn record_dispatch_attempt(&self, _run_id: &str, _key: &str,
                                     _error: &str, _next_dispatch_at_ms: i64)
        -> StoreResult<Option<EffectRecord>> { not_in_sim!("record_dispatch_attempt") }
    async fn record_external_observation(&self, _run_id: &str, _key: &str,
                                         _resolution: i32, _external_ref: &str,
                                         _response_json: &str, _error_json: &str,
                                         _compensate_on_duplicate_kind: &str)
        -> StoreResult<Option<EffectRecord>> { not_in_sim!("record_external_observation") }

    // ── obligations ─────────────────────────────────────────────────────────

    async fn register_compensation(&self, run_id: &str, effect_key: &str, kind: &str,
                                   payload_json: &str, compensator_ref: &str,
                                   max_attempts: i32) -> StoreResult<ObligationRecord> {
        let now = now_ms();
        let max = if max_attempts > 0 { max_attempts } else { 5 };
        let mut g = self.inner.lock().unwrap();
        // Idempotent on (run_id, effect_key, kind): return existing if present.
        for ob in g.obligations.values()
            .filter(|o| o.run_id == run_id && o.effect_key == effect_key && o.kind == kind) {
            return Ok(ob.clone());
        }
        let seq = {
            let n = g.next_obligation_seq.entry(run_id.into()).or_insert(0);
            *n += 1;
            *n
        };
        let journal_seq = {
            let n = g.seq.entry(run_id.into()).or_insert(0);
            *n += 1;
            *n
        };
        let rec = ObligationRecord {
            run_id: run_id.into(), seq, effect_key: effect_key.into(),
            kind: kind.into(), payload_json: payload_json.into(),
            status: ObligationStatus::Pending as i32, ts_ms: now,
            compensator_ref: compensator_ref.into(),
            attempts: 0, max_attempts: max,
            next_attempt_at_ms: now, last_error: String::new(),
            claimed_by: String::new(), claim_expires_at_ms: 0,
            result_json: String::new(),
        };
        g.obligations.insert((run_id.into(), seq), rec.clone());
        let entry = JournalEntry {
            seq: journal_seq, kind: "obligation".into(),
            payload_json: format!(
                r#"{{"run_id":"{}","kind":"{}","effect_key":"{}","status":"pending"}}"#,
                esc(run_id), esc(kind), esc(effect_key)),
            ts_ms: now,
            global_seq: 0, subject: String::new(), schema_version: 1,
            trace_id: String::new(), span_id: String::new(), parent_span_id: String::new(),
        };
        g.journal.entry(run_id.into()).or_default().push(entry);
        drop(g);
        self.notify.notify_waiters();
        Ok(rec)
    }

    async fn list_obligations(&self, run_id: &str, only_unresolved: bool,
                              status_filter: i32) -> StoreResult<Vec<ObligationRecord>> {
        let g = self.inner.lock().unwrap();
        let mut out: Vec<ObligationRecord> = g.obligations.values()
            .filter(|o| o.run_id == run_id)
            .filter(|o| {
                if status_filter != ObligationStatus::Unspecified as i32 {
                    return o.status == status_filter;
                }
                if only_unresolved {
                    let s = ObligationStatus::try_from(o.status).unwrap_or(ObligationStatus::Pending);
                    return !matches!(s, ObligationStatus::Compensated | ObligationStatus::Stuck);
                }
                true
            })
            .cloned().collect();
        // LIFO: newest seq first (matches SqlRunStore).
        out.sort_by(|a, b| b.seq.cmp(&a.seq));
        Ok(out)
    }

    async fn list_unresolved_obligations(&self, now_ms: i64, include_pending: bool,
                                         include_stuck: bool, include_committed_expired: bool,
                                         limit: i64) -> StoreResult<Vec<ObligationRecord>> {
        let g = self.inner.lock().unwrap();
        let mut out: Vec<ObligationRecord> = g.obligations.values()
            .filter(|o| {
                let s = ObligationStatus::try_from(o.status).unwrap_or(ObligationStatus::Pending);
                match s {
                    ObligationStatus::Pending => include_pending && o.next_attempt_at_ms <= now_ms,
                    ObligationStatus::Stuck => include_stuck,
                    ObligationStatus::Committed => include_committed_expired && o.claim_expires_at_ms <= now_ms,
                    _ => false,
                }
            })
            .cloned().collect();
        out.sort_by(|a, b| (a.next_attempt_at_ms, a.ts_ms).cmp(&(b.next_attempt_at_ms, b.ts_ms)));
        if limit > 0 { out.truncate(limit as usize); }
        Ok(out)
    }

    async fn claim_obligation(&self, run_id: &str, obligation_seq: i64, claimer: &str,
                              lease_ttl_ms: i64, now_ms: i64)
        -> StoreResult<(bool, Option<ObligationRecord>)> {
        let mut g = self.inner.lock().unwrap();
        let Some(o) = g.obligations.get_mut(&(run_id.into(), obligation_seq)) else {
            return Ok((false, None));
        };
        let s = ObligationStatus::try_from(o.status).unwrap_or(ObligationStatus::Pending);
        let eligible = match s {
            ObligationStatus::Pending => o.next_attempt_at_ms <= now_ms,
            ObligationStatus::Committed => o.claim_expires_at_ms <= now_ms,  // stale lease
            _ => false,
        };
        if !eligible { return Ok((false, Some(o.clone()))); }
        o.status = ObligationStatus::Committed as i32;
        o.claimed_by = claimer.into();
        o.claim_expires_at_ms = now_ms + lease_ttl_ms;
        Ok((true, Some(o.clone())))
    }

    async fn record_obligation_attempt(&self, _run_id: &str, _obligation_seq: i64,
                                       _error: &str, _next_attempt_at_ms: i64)
        -> StoreResult<Option<ObligationRecord>> { not_in_sim!("record_obligation_attempt") }

    async fn resolve_obligation(&self, run_id: &str, obligation_seq: i64, status: i32,
                                result_json: &str) -> StoreResult<Option<ObligationRecord>> {
        let mut g = self.inner.lock().unwrap();
        let Some(o) = g.obligations.get_mut(&(run_id.into(), obligation_seq)) else { return Ok(None) };
        o.status = status;
        o.result_json = result_json.into();
        o.claimed_by = String::new();
        o.claim_expires_at_ms = 0;
        Ok(Some(o.clone()))
    }

    // ── budget — stubbed ───────────────────────────────────────────────────

    async fn set_budget(&self, _run_id: &str, _usd_cap: f64, _token_cap: i64) -> StoreResult<BudgetState> {
        not_in_sim!("set_budget")
    }
    async fn get_budget(&self, _run_id: &str) -> StoreResult<BudgetState> { not_in_sim!("get_budget") }
    async fn charge_budget(&self, _run_id: &str, _usd: f64, _tokens: i64) -> StoreResult<BudgetState> {
        not_in_sim!("charge_budget")
    }

    // ── gates / signals — stubbed ──────────────────────────────────────────

    async fn await_signal(&self, _run_id: &str, _gate_name: &str, _payload_json: &str)
        -> StoreResult<(bool, String)> { not_in_sim!("await_signal") }
    async fn send_signal(&self, _run_id: &str, _app: &str, _user: &str, _session: &str, _gate_name: &str,
                         _resolution_json: &str) -> StoreResult<(String, i32)> { not_in_sim!("send_signal") }

    // ── sessions — stubbed ─────────────────────────────────────────────────

    async fn create_session(&self, _app: &str, _user: &str, _session: &str, _state_json: &str)
        -> StoreResult<Session> { not_in_sim!("create_session") }
    async fn get_session(&self, _app: &str, _user: &str, _session: &str, _max_events: i64)
        -> StoreResult<Option<Session>> { not_in_sim!("get_session") }
    async fn list_sessions(&self, _app: &str, _user: &str) -> StoreResult<Vec<Session>> {
        not_in_sim!("list_sessions")
    }
    async fn delete_session(&self, _app: &str, _user: &str, _session: &str) -> StoreResult<bool> {
        not_in_sim!("delete_session")
    }
    async fn append_event(&self, _app: &str, _user: &str, _session: &str, _event: EventRecord,
                          _state_delta_json: &str) -> StoreResult<(EventRecord, i64)> {
        not_in_sim!("append_event")
    }

    // ── reconciliation ─────────────────────────────────────────────────────

    async fn list_pending_effects(&self, _older_than_ms: i64, include_pending: bool,
                                  include_unknown: bool, limit: i64) -> StoreResult<Vec<EffectRecord>> {
        let g = self.inner.lock().unwrap();
        let mut out: Vec<EffectRecord> = g.effects.values()
            .filter(|e| {
                let s = EffectStatus::try_from(e.status).unwrap_or(EffectStatus::Confirmed);
                match s {
                    EffectStatus::Pending => include_pending,
                    EffectStatus::Unknown => include_unknown,
                    _ => false,
                }
            })
            .cloned().collect();
        if limit > 0 { out.truncate(limit as usize); }
        Ok(out)
    }

    // ── timers — stubbed ───────────────────────────────────────────────────

    async fn set_timer(&self, _run_id: &str, _timer_id: &str, _fire_at_ms: i64, _kind: &str,
                       _payload_json: &str) -> StoreResult<TimerRecord> { not_in_sim!("set_timer") }
    async fn cancel_timer(&self, _run_id: &str, _timer_id: &str) -> StoreResult<bool> {
        not_in_sim!("cancel_timer")
    }
    async fn list_due_timers(&self, _now_ms: i64, _limit: i64, _claim: bool) -> StoreResult<Vec<TimerRecord>> {
        Ok(vec![]) // empty list is "no due timers" — useful default for sim
    }

    // ── WAL tail — minimal impl ────────────────────────────────────────────

    async fn events_since(&self, _from_ts_ms: i64, _run_id: &str, _kind: &str, _limit: i64)
        -> StoreResult<Vec<EventEntry>> { not_in_sim!("events_since") }

    // ── reactive KV — stubbed ──────────────────────────────────────────────

    async fn write_value(&self, _namespace: &str, _key: &str, _value_json: &str,
                         _if_version: i64, _writer: &str) -> StoreResult<ValueRecord> {
        not_in_sim!("write_value")
    }
    async fn get_value(&self, _namespace: &str, _key: &str) -> StoreResult<Option<ValueRecord>> {
        not_in_sim!("get_value")
    }
    async fn get_value_if_newer(&self, _namespace: &str, _key: &str, _from_version: i64)
        -> StoreResult<Option<ValueRecord>> { not_in_sim!("get_value_if_newer") }
    async fn delete_value(&self, _namespace: &str, _key: &str) -> StoreResult<(bool, i64)> {
        not_in_sim!("delete_value")
    }

    fn journal_notify(&self) -> Arc<tokio::sync::Notify> {
        self.notify.clone()
    }
}

/// Minimal JSON-string escaping for the payload literals we emit.
fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n").replace('\r', "\\r")
}
