//! The Cloud Bigtable implementation of [`RunStore`].
//!
//! Bigtable is a wide-column store: no SQL, no cross-row transactions — but
//! single-row atomic mutations, `CheckAndMutateRow` (a conditional write), and
//! versioned cells. That is enough for Tape's journal, which is append-only and
//! per-run-ordered. The row design:
//!
//!   r#<run_id>                            — the run: family `m`, qualifiers
//!                                            status, app, user, session, invocation,
//!                                            lease_owner, lease_exp, started, ended,
//!                                            waiting_gate, detail
//!   idx#<app>#<user>#<session>#<inv>       — reverse index → run_id  (for begin_run)
//!   route#<app>#<user>#<session>           — newest run_id for the session (for send_signal)
//!   d#<run_id>#<decision_index:020>        — a decision: m:model, m:request, m:response,
//!                                            m:rationale, m:policy, m:ts
//!   e#<run_id>#<idempotency_key>           — an effect: m:status, m:decision_index,
//!                                            m:tool, m:request, m:response, m:error, m:ts
//!   o#<run_id>#<seq:020>                   — an obligation: m:effect_key, m:kind, m:payload, m:status, m:ts
//!   b#<run_id>                             — budget: m:usd_cap, m:tok_cap, m:usd_spent, m:tok_spent
//!   sig#<run_id>#<gate>                    — a signal: m:delivered, m:awaited, m:consumed, m:context, m:resolution, m:ts
//!   sess#<app>#<user>#<session>            — session state: m:state, m:last_update
//!   ev#<app>#<user>#<session>#<ord:020>    — an ADK event: m:event_id, m:invocation, m:author, m:branch, m:content, m:actions, m:ts
//!   j#<run_id>#<seq:020>                   — a journal entry: m:kind, m:payload, m:ts
//!
//! `seq` is a per-run counter held on `r#<run_id>` qualifier `m:seq` and bumped
//! with a `CheckAndMutateRow` read-increment-retry loop (Bigtable's
//! `ReadModifyWriteRow` would be cleaner but isn't exposed by the data-plane
//! client crate). `list_runs_to_recover` scans `r#` rows and filters in memory —
//! fine at moderate scale; at very high scale you'd drive recovery from the
//! Pub/Sub reactor (see design-principles/tape.md §12) rather than polling.
//!
//! The table and its single column family `m` must exist before the server
//! starts (Bigtable requires explicit table creation — like creating a Postgres
//! database):
//!
//!   cbt -project P -instance I createtable tape && cbt -project P -instance I createfamily tape m
//!
//! With `BIGTABLE_EMULATOR_HOST` set, `bigtable://demo/demo/tape` talks to the
//! local emulator.
//!
//! Status: this module is the foundation — the row design and the connection are
//! in place; the per-operation Bigtable mutations are being filled in. Until then
//! `connect` returns an error so misconfiguration is loud, not silent.

use async_trait::async_trait;

use super::{RunStore, StoreError, StoreResult};
use crate::pb::*;

pub struct BigtableRunStore {
    #[allow(dead_code)]
    project: String,
    #[allow(dead_code)]
    instance: String,
    #[allow(dead_code)]
    table: String,
}

impl BigtableRunStore {
    pub async fn connect(project: &str, instance: &str, table: &str) -> StoreResult<Self> {
        // TODO(tape-bigtable): wire up bigtable_rs::BigTableConnection here. The
        // RunStore impl below maps every op onto the row design documented above;
        // it needs the table + column family `m` to exist. Until the data-plane
        // wiring lands, fail loudly rather than pretend.
        let _ = (project, instance, table);
        Err(StoreError::msg(
            "the bigtable store is not yet wired in this build — see src/store/bigtable.rs \
             for the (documented) row design; use sqlite:/postgres:/alloydb: for now",
        ))
    }
}

#[async_trait]
#[allow(unused_variables)]
impl RunStore for BigtableRunStore {
    async fn begin_run(&self, app: &str, user: &str, session: &str, invocation: &str, lease_owner: &str, lease_ttl_ms: i64) -> StoreResult<BeginRunResponse> { unimplemented!() }
    async fn resume_run(&self, run_id: &str, lease_owner: &str, lease_ttl_ms: i64) -> StoreResult<Option<RunState>> { unimplemented!() }
    async fn end_run(&self, run_id: &str, status: i32, detail_json: &str) -> StoreResult<Option<RunState>> { unimplemented!() }
    async fn get_run(&self, run_id: &str) -> StoreResult<Option<RunState>> { unimplemented!() }
    async fn list_runs_to_recover(&self, now_ms: i64, limit: i64) -> StoreResult<Vec<RunState>> { unimplemented!() }
    async fn journal_range(&self, run_id: &str, from_seq: i64) -> StoreResult<Vec<JournalEntry>> { unimplemented!() }
    async fn record_decision(&self, run_id: &str, decision_index: i64, model: &str, request_json: &str, response_json: &str, rationale: &str, policy_version: &str) -> StoreResult<DecisionRecord> { unimplemented!() }
    async fn get_decision(&self, run_id: &str, decision_index: i64) -> StoreResult<Option<DecisionRecord>> { unimplemented!() }
    async fn begin_effect(&self, run_id: &str, decision_index: i64, tool_name: &str, call_index: i32, request_json: &str, custom_key: &str) -> StoreResult<EffectRecord> { unimplemented!() }
    async fn complete_effect(&self, run_id: &str, key: &str, status: i32, response_json: &str, error_json: &str) -> StoreResult<Option<EffectRecord>> { unimplemented!() }
    async fn get_effect(&self, run_id: &str, key: &str) -> StoreResult<Option<EffectRecord>> { unimplemented!() }
    async fn reconcile_effect(&self, run_id: &str, key: &str, resolved_status: i32, response_json: &str, error_json: &str) -> StoreResult<Option<EffectRecord>> { unimplemented!() }
    async fn register_compensation(&self, run_id: &str, effect_key: &str, kind: &str, payload_json: &str) -> StoreResult<ObligationRecord> { unimplemented!() }
    async fn list_obligations(&self, run_id: &str, only_unresolved: bool) -> StoreResult<Vec<ObligationRecord>> { unimplemented!() }
    async fn resolve_obligation(&self, run_id: &str, obligation_seq: i64, status: i32, result_json: &str) -> StoreResult<Option<ObligationRecord>> { unimplemented!() }
    async fn set_budget(&self, run_id: &str, usd_cap: f64, token_cap: i64) -> StoreResult<BudgetState> { unimplemented!() }
    async fn get_budget(&self, run_id: &str) -> StoreResult<BudgetState> { unimplemented!() }
    async fn charge_budget(&self, run_id: &str, usd: f64, tokens: i64) -> StoreResult<BudgetState> { unimplemented!() }
    async fn await_signal(&self, run_id: &str, gate_name: &str, payload_json: &str) -> StoreResult<(bool, String)> { unimplemented!() }
    async fn send_signal(&self, run_id: &str, app: &str, user: &str, session: &str, gate_name: &str, resolution_json: &str) -> StoreResult<(String, i32)> { unimplemented!() }
    async fn create_session(&self, app: &str, user: &str, session: &str, state_json: &str) -> StoreResult<Session> { unimplemented!() }
    async fn get_session(&self, app: &str, user: &str, session: &str, max_events: i64) -> StoreResult<Option<Session>> { unimplemented!() }
    async fn list_sessions(&self, app: &str, user: &str) -> StoreResult<Vec<Session>> { unimplemented!() }
    async fn delete_session(&self, app: &str, user: &str, session: &str) -> StoreResult<bool> { unimplemented!() }
    async fn append_event(&self, app: &str, user: &str, session: &str, event: EventRecord, state_delta_json: &str) -> StoreResult<(EventRecord, i64)> { unimplemented!() }
}
