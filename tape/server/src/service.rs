//! The Tape gRPC service — every RPC in `tape.proto`, over the pluggable `Store`.
//!
//! The shape to keep in mind:
//!   * a *run* is one row in `tape_runs`, keyed by (app, user, session, invocation_id);
//!   * its *journal* is `tape_journal` ordered by `seq`, with typed detail in
//!     `tape_decisions` / `tape_effects` / `tape_obligations`;
//!   * `seq` is a per-run monotonic counter (`tape_runs.seq_cursor`) — the anchor
//!     the re-drive aligns against;
//!   * mutating RPCs are idempotent: a replay returns the recorded row. (That is
//!     also what makes N server replicas safe — a double-drive short-circuits.)
//!
//! All SQL is written once here, in the portable subset both stores speak (`?N`
//! placeholders; the Postgres store rewrites to `$N`).

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Request, Response, Status};

use crate::pb::tape_server::Tape;
use crate::pb::*;
use crate::store::{now_ms, RowExt, Store, StoreError, Val};

pub struct TapeService {
    store: Arc<dyn Store>,
}

impl TapeService {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }
    fn s(&self) -> &dyn Store {
        self.store.as_ref()
    }
}

// ── helpers ─────────────────────────────────────────────────────────────────

fn db(e: StoreError) -> Status {
    Status::internal(e.to_string())
}

fn effect_status(i: i32) -> EffectStatus {
    EffectStatus::try_from(i).unwrap_or(EffectStatus::Unspecified)
}

fn derive_key(run_id: &str, decision_index: i64, tool: &str, call_index: i32) -> String {
    if decision_index < 0 {
        format!("{run_id}/no-decision/{tool}/{call_index}")
    } else {
        format!("{run_id}/decision-{decision_index}/{tool}/{call_index}")
    }
}

async fn next_seq(store: &dyn Store, run_id: &str) -> Result<i64, StoreError> {
    store
        .exec(
            "UPDATE tape_runs SET seq_cursor = seq_cursor + 1 WHERE run_id = ?1",
            vec![run_id.into()],
        )
        .await?;
    let row = store
        .query_opt(
            "SELECT seq_cursor FROM tape_runs WHERE run_id = ?1",
            vec![run_id.into()],
        )
        .await?;
    Ok(row.map(|r| r.i64(0)).unwrap_or(0))
}

async fn journal(
    store: &dyn Store,
    run_id: &str,
    seq: i64,
    kind: &str,
    payload_json: &str,
    ts: i64,
) -> Result<(), StoreError> {
    store
        .exec(
            "INSERT INTO tape_journal (run_id, seq, kind, payload_json, ts_ms) VALUES (?1,?2,?3,?4,?5)",
            vec![run_id.into(), seq.into(), kind.into(), payload_json.into(), ts.into()],
        )
        .await
        .map(|_| ())
}

fn run_state_of(r: &Vec<Val>) -> RunState {
    RunState {
        run_id: r.str(0),
        app_name: r.str(1),
        user_id: r.str(2),
        session_id: r.str(3),
        invocation_id: r.str(4),
        status: r.i32(5),
        seq_cursor: r.i64(6),
        lease_owner: r.str(7),
        lease_expires_at_ms: r.i64(8),
        started_at_ms: r.i64(9),
        ended_at_ms: r.i64(10),
        waiting_on_gate: r.str(11),
    }
}
const RUN_COLS: &str = "run_id, app_name, user_id, session_id, invocation_id, status, seq_cursor, \
    lease_owner, lease_expires_at_ms, started_at_ms, ended_at_ms, waiting_on_gate";

async fn read_run(store: &dyn Store, run_id: &str) -> Result<Option<RunState>, StoreError> {
    let sql = format!("SELECT {RUN_COLS} FROM tape_runs WHERE run_id = ?1");
    Ok(store
        .query_opt(&sql, vec![run_id.into()])
        .await?
        .map(|r| run_state_of(&r)))
}

fn effect_of(r: &Vec<Val>) -> EffectRecord {
    EffectRecord {
        run_id: r.str(0),
        seq: r.i64(1),
        decision_index: r.i64(2),
        tool_name: r.str(3),
        idempotency_key: r.str(4),
        status: r.i32(5),
        request_json: r.str(6),
        response_json: r.str(7),
        error_json: r.str(8),
        ts_ms: r.i64(9),
    }
}
const EFFECT_COLS: &str = "run_id, seq, decision_index, tool_name, idempotency_key, status, \
    request_json, response_json, error_json, ts_ms";

async fn read_effect(store: &dyn Store, run_id: &str, key: &str) -> Result<Option<EffectRecord>, StoreError> {
    let sql = format!("SELECT {EFFECT_COLS} FROM tape_effects WHERE run_id = ?1 AND idempotency_key = ?2");
    Ok(store
        .query_opt(&sql, vec![run_id.into(), key.into()])
        .await?
        .map(|r| effect_of(&r)))
}

fn decision_of(r: &Vec<Val>) -> DecisionRecord {
    DecisionRecord {
        run_id: r.str(0),
        seq: r.i64(1),
        decision_index: r.i64(2),
        model: r.str(3),
        request_json: r.str(4),
        response_json: r.str(5),
        rationale: r.str(6),
        policy_version: r.str(7),
        ts_ms: r.i64(8),
    }
}
const DECISION_COLS: &str =
    "run_id, seq, decision_index, model, request_json, response_json, rationale, policy_version, ts_ms";

fn obligation_of(r: &Vec<Val>) -> ObligationRecord {
    ObligationRecord {
        run_id: r.str(0),
        seq: r.i64(1),
        effect_key: r.str(2),
        kind: r.str(3),
        payload_json: r.str(4),
        status: r.i32(5),
        ts_ms: r.i64(6),
    }
}
const OBLIGATION_COLS: &str = "run_id, seq, effect_key, kind, payload_json, status, ts_ms";

async fn budget_state(store: &dyn Store, run_id: &str) -> Result<BudgetState, StoreError> {
    let row = store
        .query_opt(
            "SELECT usd_cap, token_cap, usd_spent, tokens_spent FROM tape_budget WHERE run_id = ?1",
            vec![run_id.into()],
        )
        .await?;
    Ok(match row {
        Some(r) => BudgetState {
            run_id: run_id.to_string(),
            usd_cap: r.f64(0),
            token_cap: r.i64(1),
            usd_spent: r.f64(2),
            tokens_spent: r.i64(3),
        },
        None => BudgetState {
            run_id: run_id.to_string(),
            usd_cap: 0.0,
            token_cap: 0,
            usd_spent: 0.0,
            tokens_spent: 0,
        },
    })
}

/// Shallow-merge `delta` (a JSON object) into `base`; a `null` value deletes.
fn merge_json(base: &str, delta: &str) -> String {
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

// ── the service ─────────────────────────────────────────────────────────────

#[tonic::async_trait]
impl Tape for TapeService {
    // ─── run lifecycle ──────────────────────────────────────────────────────

    async fn begin_run(
        &self,
        request: Request<BeginRunRequest>,
    ) -> Result<Response<BeginRunResponse>, Status> {
        let r = request.into_inner();
        let ts = now_ms();
        let lease_exp = ts + r.lease_ttl_ms.max(0);

        let existing = self
            .s()
            .query_opt(
                "SELECT run_id, seq_cursor FROM tape_runs \
                 WHERE app_name=?1 AND user_id=?2 AND session_id=?3 AND invocation_id=?4",
                vec![
                    r.app_name.clone().into(),
                    r.user_id.clone().into(),
                    r.session_id.clone().into(),
                    r.invocation_id.clone().into(),
                ],
            )
            .await
            .map_err(db)?;

        if let Some(row) = existing {
            let run_id = row.str(0);
            let seq_cursor = row.i64(1);
            // Take the lease, flip to RUNNING — unless the run already finished
            // or is stuck (then leave it; the caller short-circuits).
            self.s()
                .exec(
                    "UPDATE tape_runs SET status=?2, lease_owner=?3, lease_expires_at_ms=?4 \
                     WHERE run_id=?1 AND status NOT IN (?5, ?6)",
                    vec![
                        run_id.clone().into(),
                        (RunStatus::Running as i32).into(),
                        r.lease_owner.clone().into(),
                        lease_exp.into(),
                        (RunStatus::Terminal as i32).into(),
                        (RunStatus::Stuck as i32).into(),
                    ],
                )
                .await
                .map_err(db)?;
            let cur = read_run(self.s(), &run_id).await.map_err(db)?.unwrap();
            return Ok(Response::new(BeginRunResponse {
                run_id,
                resumed: true,
                next_seq: seq_cursor,
                status: cur.status,
            }));
        }

        let run_id = uuid::Uuid::new_v4().to_string();
        self.s()
            .exec(
                "INSERT INTO tape_runs (run_id, app_name, user_id, session_id, invocation_id, \
                 status, seq_cursor, lease_owner, lease_expires_at_ms, started_at_ms) \
                 VALUES (?1,?2,?3,?4,?5,?6,0,?7,?8,?9)",
                vec![
                    run_id.clone().into(),
                    r.app_name.into(),
                    r.user_id.into(),
                    r.session_id.into(),
                    r.invocation_id.into(),
                    (RunStatus::Running as i32).into(),
                    r.lease_owner.into(),
                    lease_exp.into(),
                    ts.into(),
                ],
            )
            .await
            .map_err(db)?;
        Ok(Response::new(BeginRunResponse {
            run_id,
            resumed: false,
            next_seq: 0,
            status: RunStatus::Running as i32,
        }))
    }

    async fn resume_run(
        &self,
        request: Request<ResumeRunRequest>,
    ) -> Result<Response<ResumeRunResponse>, Status> {
        let r = request.into_inner();
        let ts = now_ms();
        self.s()
            .exec(
                "UPDATE tape_runs SET status=?2, lease_owner=?3, lease_expires_at_ms=?4 WHERE run_id=?1",
                vec![
                    r.run_id.clone().into(),
                    (RunStatus::Running as i32).into(),
                    r.lease_owner.into(),
                    (ts + r.lease_ttl_ms.max(0)).into(),
                ],
            )
            .await
            .map_err(db)?;
        let run = read_run(self.s(), &r.run_id)
            .await
            .map_err(db)?
            .ok_or_else(|| Status::not_found("no such run"))?;
        Ok(Response::new(ResumeRunResponse { run: Some(run) }))
    }

    async fn end_run(
        &self,
        request: Request<EndRunRequest>,
    ) -> Result<Response<EndRunResponse>, Status> {
        let r = request.into_inner();
        self.s()
            .exec(
                "UPDATE tape_runs SET status=?2, ended_at_ms=?3, detail_json=?4, lease_owner='' WHERE run_id=?1",
                vec![r.run_id.clone().into(), r.status.into(), now_ms().into(), r.detail_json.into()],
            )
            .await
            .map_err(db)?;
        let run = read_run(self.s(), &r.run_id)
            .await
            .map_err(db)?
            .ok_or_else(|| Status::not_found("no such run"))?;
        Ok(Response::new(EndRunResponse { run: Some(run) }))
    }

    async fn get_run(&self, request: Request<GetRunRequest>) -> Result<Response<RunState>, Status> {
        let r = request.into_inner();
        let run = read_run(self.s(), &r.run_id)
            .await
            .map_err(db)?
            .ok_or_else(|| Status::not_found("no such run"))?;
        Ok(Response::new(run))
    }

    async fn list_runs_to_recover(
        &self,
        request: Request<ListRunsToRecoverRequest>,
    ) -> Result<Response<ListRunsToRecoverResponse>, Status> {
        let r = request.into_inner();
        let now = if r.now_ms > 0 { r.now_ms } else { now_ms() };
        let limit = if r.limit > 0 { r.limit } else { 100 };
        let sql = format!(
            "SELECT {RUN_COLS} FROM tape_runs r \
             WHERE status = ?1 \
                OR (status = ?2 AND lease_expires_at_ms < ?3) \
                OR (status = ?4 AND EXISTS ( \
                      SELECT 1 FROM tape_signals s \
                      WHERE s.run_id = r.run_id AND s.delivered = 1 AND s.consumed = 0)) \
             LIMIT ?5"
        );
        let rows = self
            .s()
            .query(
                &sql,
                vec![
                    (RunStatus::Runnable as i32).into(),
                    (RunStatus::Running as i32).into(),
                    now.into(),
                    (RunStatus::Waiting as i32).into(),
                    limit.into(),
                ],
            )
            .await
            .map_err(db)?;
        Ok(Response::new(ListRunsToRecoverResponse {
            runs: rows.iter().map(run_state_of).collect(),
        }))
    }

    type SubscribeRunStream =
        Pin<Box<dyn Stream<Item = Result<JournalEntry, Status>> + Send + 'static>>;

    async fn subscribe_run(
        &self,
        request: Request<SubscribeRunRequest>,
    ) -> Result<Response<Self::SubscribeRunStream>, Status> {
        let r = request.into_inner();
        let store = self.store.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            let mut from = r.from_seq;
            loop {
                let batch = match store
                    .query(
                        "SELECT seq, kind, payload_json, ts_ms FROM tape_journal \
                         WHERE run_id=?1 AND seq>=?2 ORDER BY seq",
                        vec![r.run_id.clone().into(), from.into()],
                    )
                    .await
                {
                    Ok(rows) => rows,
                    Err(_) => break,
                };
                for row in &batch {
                    let entry = JournalEntry {
                        seq: row.i64(0),
                        kind: row.str(1),
                        payload_json: row.str(2),
                        ts_ms: row.i64(3),
                    };
                    from = entry.seq + 1;
                    if tx.send(Ok(entry)).await.is_err() {
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    // ─── decision ledger ────────────────────────────────────────────────────

    async fn record_decision(
        &self,
        request: Request<RecordDecisionRequest>,
    ) -> Result<Response<DecisionRecord>, Status> {
        let r = request.into_inner();
        let ts = now_ms();
        let sql = format!(
            "SELECT {DECISION_COLS} FROM tape_decisions WHERE run_id=?1 AND decision_index=?2"
        );
        if let Some(row) = self
            .s()
            .query_opt(&sql, vec![r.run_id.clone().into(), r.decision_index.into()])
            .await
            .map_err(db)?
        {
            return Ok(Response::new(decision_of(&row)));
        }
        let seq = next_seq(self.s(), &r.run_id).await.map_err(db)?;
        self.s()
            .exec(
                "INSERT INTO tape_decisions (run_id, seq, decision_index, model, request_json, response_json, rationale, policy_version, ts_ms) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                vec![
                    r.run_id.clone().into(), seq.into(), r.decision_index.into(),
                    r.model.clone().into(), r.request_json.clone().into(), r.response_json.clone().into(),
                    r.rationale.clone().into(), r.policy_version.clone().into(), ts.into(),
                ],
            )
            .await
            .map_err(db)?;
        let payload = serde_json::json!({
            "decision_index": r.decision_index, "model": r.model,
            "policy_version": r.policy_version, "rationale": r.rationale
        })
        .to_string();
        journal(self.s(), &r.run_id, seq, "decision", &payload, ts)
            .await
            .map_err(db)?;
        Ok(Response::new(DecisionRecord {
            run_id: r.run_id,
            seq,
            decision_index: r.decision_index,
            model: r.model,
            request_json: r.request_json,
            response_json: r.response_json,
            rationale: r.rationale,
            policy_version: r.policy_version,
            ts_ms: ts,
        }))
    }

    async fn get_decision(
        &self,
        request: Request<GetDecisionRequest>,
    ) -> Result<Response<GetDecisionResponse>, Status> {
        let r = request.into_inner();
        let sql = format!(
            "SELECT {DECISION_COLS} FROM tape_decisions WHERE run_id=?1 AND decision_index=?2"
        );
        let rec = self
            .s()
            .query_opt(&sql, vec![r.run_id.into(), r.decision_index.into()])
            .await
            .map_err(db)?
            .map(|row| decision_of(&row));
        Ok(Response::new(GetDecisionResponse {
            found: rec.is_some(),
            decision: rec,
        }))
    }

    // ─── effect ledger ──────────────────────────────────────────────────────

    async fn begin_effect(
        &self,
        request: Request<BeginEffectRequest>,
    ) -> Result<Response<BeginEffectResponse>, Status> {
        let r = request.into_inner();
        let key = if r.custom_key.is_empty() {
            derive_key(&r.run_id, r.decision_index, &r.tool_name, r.call_index)
        } else {
            r.custom_key.clone()
        };

        if let Some(e) = read_effect(self.s(), &r.run_id, &key).await.map_err(db)? {
            return Ok(Response::new(BeginEffectResponse {
                seq: e.seq,
                idempotency_key: key,
                status: e.status,
                response_json: e.response_json,
                error_json: e.error_json,
            }));
        }

        let ts = now_ms();
        let seq = next_seq(self.s(), &r.run_id).await.map_err(db)?;
        self.s()
            .exec(
                "INSERT INTO tape_effects (run_id, seq, decision_index, tool_name, idempotency_key, status, request_json, response_json, error_json, ts_ms) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,'','',?8)",
                vec![
                    r.run_id.clone().into(), seq.into(), r.decision_index.into(),
                    r.tool_name.clone().into(), key.clone().into(),
                    (EffectStatus::Pending as i32).into(), r.request_json.into(), ts.into(),
                ],
            )
            .await
            .map_err(db)?;
        let payload = serde_json::json!({
            "tool": r.tool_name, "decision_index": r.decision_index,
            "idempotency_key": key, "status": "pending"
        })
        .to_string();
        journal(self.s(), &r.run_id, seq, "effect", &payload, ts)
            .await
            .map_err(db)?;
        Ok(Response::new(BeginEffectResponse {
            seq,
            idempotency_key: key,
            status: EffectStatus::Pending as i32,
            response_json: String::new(),
            error_json: String::new(),
        }))
    }

    async fn complete_effect(
        &self,
        request: Request<CompleteEffectRequest>,
    ) -> Result<Response<EffectRecord>, Status> {
        let r = request.into_inner();
        let existing = read_effect(self.s(), &r.run_id, &r.idempotency_key)
            .await
            .map_err(db)?
            .ok_or_else(|| Status::failed_precondition("complete_effect before begin_effect"))?;
        if existing.status != EffectStatus::Pending as i32 {
            return Ok(Response::new(existing));
        }
        let ts = now_ms();
        self.s()
            .exec(
                "UPDATE tape_effects SET status=?3, response_json=?4, error_json=?5, ts_ms=?6 \
                 WHERE run_id=?1 AND idempotency_key=?2",
                vec![
                    r.run_id.clone().into(), r.idempotency_key.clone().into(),
                    r.status.into(), r.response_json.clone().into(), r.error_json.clone().into(), ts.into(),
                ],
            )
            .await
            .map_err(db)?;
        let seq = next_seq(self.s(), &r.run_id).await.map_err(db)?;
        let label = match effect_status(r.status) {
            EffectStatus::Confirmed => "confirmed",
            EffectStatus::Failed => "failed",
            EffectStatus::Unknown => "unknown",
            _ => "completed",
        };
        let payload = serde_json::json!({
            "tool": existing.tool_name, "idempotency_key": r.idempotency_key, "status": label
        })
        .to_string();
        journal(self.s(), &r.run_id, seq, "effect", &payload, ts)
            .await
            .map_err(db)?;
        Ok(Response::new(EffectRecord {
            status: r.status,
            response_json: r.response_json,
            error_json: r.error_json,
            ts_ms: ts,
            ..existing
        }))
    }

    async fn get_effect(
        &self,
        request: Request<GetEffectRequest>,
    ) -> Result<Response<GetEffectResponse>, Status> {
        let r = request.into_inner();
        let e = read_effect(self.s(), &r.run_id, &r.idempotency_key)
            .await
            .map_err(db)?;
        Ok(Response::new(GetEffectResponse {
            found: e.is_some(),
            effect: e,
        }))
    }

    async fn reconcile_effect(
        &self,
        request: Request<ReconcileEffectRequest>,
    ) -> Result<Response<EffectRecord>, Status> {
        let r = request.into_inner();
        let existing = read_effect(self.s(), &r.run_id, &r.idempotency_key)
            .await
            .map_err(db)?
            .ok_or_else(|| Status::not_found("no such effect"))?;
        if existing.status == EffectStatus::Confirmed as i32 || existing.status == EffectStatus::Failed as i32 {
            return Ok(Response::new(existing));
        }
        let ts = now_ms();
        self.s()
            .exec(
                "UPDATE tape_effects SET status=?3, response_json=?4, error_json=?5, ts_ms=?6 \
                 WHERE run_id=?1 AND idempotency_key=?2",
                vec![
                    r.run_id.clone().into(), r.idempotency_key.clone().into(),
                    r.resolved_status.into(), r.response_json.clone().into(), r.error_json.clone().into(), ts.into(),
                ],
            )
            .await
            .map_err(db)?;
        let seq = next_seq(self.s(), &r.run_id).await.map_err(db)?;
        let payload = serde_json::json!({
            "tool": existing.tool_name, "idempotency_key": r.idempotency_key,
            "status": "reconciled", "resolved_to": r.resolved_status
        })
        .to_string();
        journal(self.s(), &r.run_id, seq, "effect", &payload, ts)
            .await
            .map_err(db)?;
        Ok(Response::new(EffectRecord {
            status: r.resolved_status,
            response_json: r.response_json,
            error_json: r.error_json,
            ts_ms: ts,
            ..existing
        }))
    }

    // ─── obligations / compensation ─────────────────────────────────────────

    async fn register_compensation(
        &self,
        request: Request<RegisterCompensationRequest>,
    ) -> Result<Response<ObligationRecord>, Status> {
        let r = request.into_inner();
        let sql = format!(
            "SELECT {OBLIGATION_COLS} FROM tape_obligations WHERE run_id=?1 AND effect_key=?2 AND kind=?3"
        );
        if let Some(row) = self
            .s()
            .query_opt(
                &sql,
                vec![r.run_id.clone().into(), r.effect_key.clone().into(), r.kind.clone().into()],
            )
            .await
            .map_err(db)?
        {
            return Ok(Response::new(obligation_of(&row)));
        }
        let ts = now_ms();
        let seq = next_seq(self.s(), &r.run_id).await.map_err(db)?;
        let status = ObligationStatus::Committed as i32;
        self.s()
            .exec(
                "INSERT INTO tape_obligations (run_id, seq, effect_key, kind, payload_json, status, ts_ms) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7)",
                vec![
                    r.run_id.clone().into(), seq.into(), r.effect_key.clone().into(),
                    r.kind.clone().into(), r.payload_json.clone().into(), status.into(), ts.into(),
                ],
            )
            .await
            .map_err(db)?;
        let payload = serde_json::json!({"effect_key": r.effect_key, "kind": r.kind}).to_string();
        journal(self.s(), &r.run_id, seq, "obligation", &payload, ts)
            .await
            .map_err(db)?;
        Ok(Response::new(ObligationRecord {
            run_id: r.run_id,
            seq,
            effect_key: r.effect_key,
            kind: r.kind,
            payload_json: r.payload_json,
            status,
            ts_ms: ts,
        }))
    }

    async fn list_obligations(
        &self,
        request: Request<ListObligationsRequest>,
    ) -> Result<Response<ListObligationsResponse>, Status> {
        let r = request.into_inner();
        let sql = if r.only_unresolved {
            format!(
                "SELECT {OBLIGATION_COLS} FROM tape_obligations WHERE run_id=?1 AND status NOT IN (?2, ?3) ORDER BY seq DESC"
            )
        } else {
            format!("SELECT {OBLIGATION_COLS} FROM tape_obligations WHERE run_id=?1 ORDER BY seq DESC")
        };
        let params: Vec<Val> = if r.only_unresolved {
            vec![
                r.run_id.into(),
                (ObligationStatus::Compensated as i32).into(),
                (ObligationStatus::Stuck as i32).into(),
            ]
        } else {
            vec![r.run_id.into()]
        };
        let rows = self.s().query(&sql, params).await.map_err(db)?;
        Ok(Response::new(ListObligationsResponse {
            obligations: rows.iter().map(obligation_of).collect(),
        }))
    }

    async fn resolve_obligation(
        &self,
        request: Request<ResolveObligationRequest>,
    ) -> Result<Response<ObligationRecord>, Status> {
        let r = request.into_inner();
        self.s()
            .exec(
                "UPDATE tape_obligations SET status=?3, ts_ms=?4 WHERE run_id=?1 AND seq=?2",
                vec![r.run_id.clone().into(), r.obligation_seq.into(), r.status.into(), now_ms().into()],
            )
            .await
            .map_err(db)?;
        let sql = format!("SELECT {OBLIGATION_COLS} FROM tape_obligations WHERE run_id=?1 AND seq=?2");
        let rec = self
            .s()
            .query_opt(&sql, vec![r.run_id.into(), r.obligation_seq.into()])
            .await
            .map_err(db)?
            .map(|row| obligation_of(&row))
            .ok_or_else(|| Status::not_found("no such obligation"))?;
        Ok(Response::new(rec))
    }

    // ─── budget ─────────────────────────────────────────────────────────────

    async fn set_budget(
        &self,
        request: Request<SetBudgetRequest>,
    ) -> Result<Response<BudgetState>, Status> {
        let r = request.into_inner();
        self.s()
            .exec(
                "INSERT INTO tape_budget (run_id, usd_cap, token_cap, usd_spent, tokens_spent) \
                 VALUES (?1,?2,?3,0,0) \
                 ON CONFLICT(run_id) DO UPDATE SET usd_cap=excluded.usd_cap, token_cap=excluded.token_cap",
                vec![r.run_id.clone().into(), r.usd_cap.into(), r.token_cap.into()],
            )
            .await
            .map_err(db)?;
        Ok(Response::new(budget_state(self.s(), &r.run_id).await.map_err(db)?))
    }

    async fn admit_budget(
        &self,
        request: Request<AdmitBudgetRequest>,
    ) -> Result<Response<AdmitBudgetResponse>, Status> {
        let r = request.into_inner();
        let b = budget_state(self.s(), &r.run_id).await.map_err(db)?;
        let mut admitted = true;
        let mut reason = String::new();
        if b.usd_cap > 0.0 && b.usd_spent + r.usd_estimate > b.usd_cap {
            admitted = false;
            reason = format!(
                "usd cap {:.2} would be exceeded (spent {:.2} + estimate {:.2})",
                b.usd_cap, b.usd_spent, r.usd_estimate
            );
        }
        if admitted && b.token_cap > 0 && b.tokens_spent + r.token_estimate > b.token_cap {
            admitted = false;
            reason = format!(
                "token cap {} would be exceeded (spent {} + estimate {})",
                b.token_cap, b.tokens_spent, r.token_estimate
            );
        }
        Ok(Response::new(AdmitBudgetResponse {
            admitted,
            reason,
            budget: Some(b),
        }))
    }

    async fn charge_budget(
        &self,
        request: Request<ChargeBudgetRequest>,
    ) -> Result<Response<BudgetState>, Status> {
        let r = request.into_inner();
        self.s()
            .exec(
                "INSERT INTO tape_budget (run_id, usd_spent, tokens_spent) VALUES (?1,?2,?3) \
                 ON CONFLICT(run_id) DO UPDATE SET usd_spent = tape_budget.usd_spent + ?2, tokens_spent = tape_budget.tokens_spent + ?3",
                vec![r.run_id.clone().into(), r.usd.into(), r.tokens.into()],
            )
            .await
            .map_err(db)?;
        Ok(Response::new(budget_state(self.s(), &r.run_id).await.map_err(db)?))
    }

    // ─── gates / signals ────────────────────────────────────────────────────

    async fn await_signal(
        &self,
        request: Request<AwaitSignalRequest>,
    ) -> Result<Response<AwaitSignalResponse>, Status> {
        let r = request.into_inner();
        let ts = now_ms();
        let delivered = self
            .s()
            .query_opt(
                "SELECT delivered, resolution_json FROM tape_signals WHERE run_id=?1 AND gate_name=?2",
                vec![r.run_id.clone().into(), r.gate_name.clone().into()],
            )
            .await
            .map_err(db)?;
        if let Some(row) = delivered {
            if row.i64(0) == 1 {
                self.s()
                    .exec(
                        "UPDATE tape_signals SET consumed=1 WHERE run_id=?1 AND gate_name=?2",
                        vec![r.run_id.clone().into(), r.gate_name.clone().into()],
                    )
                    .await
                    .map_err(db)?;
                return Ok(Response::new(AwaitSignalResponse {
                    delivered: true,
                    resolution_json: row.str(1),
                }));
            }
        }
        self.s()
            .exec(
                "INSERT INTO tape_signals (run_id, gate_name, context_json, awaited, created_at_ms) \
                 VALUES (?1,?2,?3,1,?4) \
                 ON CONFLICT(run_id, gate_name) DO UPDATE SET awaited=1, context_json=excluded.context_json",
                vec![r.run_id.clone().into(), r.gate_name.clone().into(), r.payload_json.into(), ts.into()],
            )
            .await
            .map_err(db)?;
        let seq = next_seq(self.s(), &r.run_id).await.map_err(db)?;
        self.s()
            .exec(
                "UPDATE tape_runs SET status=?2, waiting_on_gate=?3, lease_owner='' WHERE run_id=?1",
                vec![r.run_id.clone().into(), (RunStatus::Waiting as i32).into(), r.gate_name.clone().into()],
            )
            .await
            .map_err(db)?;
        journal(
            self.s(),
            &r.run_id,
            seq,
            "gate",
            &serde_json::json!({"gate": r.gate_name, "status": "waiting"}).to_string(),
            ts,
        )
        .await
        .map_err(db)?;
        Ok(Response::new(AwaitSignalResponse {
            delivered: false,
            resolution_json: String::new(),
        }))
    }

    async fn send_signal(
        &self,
        request: Request<SendSignalRequest>,
    ) -> Result<Response<SendSignalResponse>, Status> {
        let r = request.into_inner();
        let ts = now_ms();
        let run_id = if !r.run_id.is_empty() {
            r.run_id.clone()
        } else {
            self.s()
                .query_opt(
                    "SELECT run_id FROM tape_runs WHERE app_name=?1 AND user_id=?2 AND session_id=?3 \
                     ORDER BY started_at_ms DESC LIMIT 1",
                    vec![r.app_name.into(), r.user_id.into(), r.session_id.into()],
                )
                .await
                .map_err(db)?
                .map(|row| row.str(0))
                .ok_or_else(|| Status::not_found("no run for that session"))?
        };
        self.s()
            .exec(
                "INSERT INTO tape_signals (run_id, gate_name, resolution_json, delivered, created_at_ms) \
                 VALUES (?1,?2,?3,1,?4) \
                 ON CONFLICT(run_id, gate_name) DO UPDATE SET resolution_json=excluded.resolution_json, delivered=1",
                vec![run_id.clone().into(), r.gate_name.clone().into(), r.resolution_json.into(), ts.into()],
            )
            .await
            .map_err(db)?;
        let mut run_status = RunStatus::Unspecified;
        let waiting_on = self
            .s()
            .query_opt(
                "SELECT waiting_on_gate FROM tape_runs WHERE run_id=?1 AND status=?2",
                vec![run_id.clone().into(), (RunStatus::Waiting as i32).into()],
            )
            .await
            .map_err(db)?
            .map(|row| row.str(0));
        if let Some(gate) = waiting_on {
            if gate == r.gate_name {
                self.s()
                    .exec(
                        "UPDATE tape_runs SET status=?2, waiting_on_gate='' WHERE run_id=?1",
                        vec![run_id.clone().into(), (RunStatus::Runnable as i32).into()],
                    )
                    .await
                    .map_err(db)?;
                run_status = RunStatus::Runnable;
                if let Ok(seq) = next_seq(self.s(), &run_id).await {
                    let _ = journal(
                        self.s(),
                        &run_id,
                        seq,
                        "gate",
                        &serde_json::json!({"gate": r.gate_name, "status": "released"}).to_string(),
                        ts,
                    )
                    .await;
                }
            }
        }
        Ok(Response::new(SendSignalResponse {
            accepted: true,
            run_id,
            run_status: run_status as i32,
        }))
    }

    // ─── ADK SessionService shim ────────────────────────────────────────────

    async fn create_session(
        &self,
        request: Request<CreateSessionRequest>,
    ) -> Result<Response<Session>, Status> {
        let r = request.into_inner();
        let ts = now_ms();
        let session_id = if r.session_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            r.session_id.clone()
        };
        let state = if r.state_json.is_empty() { "{}".to_string() } else { r.state_json.clone() };
        self.s()
            .exec(
                "INSERT INTO tape_sessions (app_name, user_id, session_id, state_json, last_update_time_ms) \
                 VALUES (?1,?2,?3,?4,?5) \
                 ON CONFLICT(app_name, user_id, session_id) DO UPDATE SET state_json=excluded.state_json, last_update_time_ms=excluded.last_update_time_ms",
                vec![r.app_name.clone().into(), r.user_id.clone().into(), session_id.clone().into(), state.clone().into(), ts.into()],
            )
            .await
            .map_err(db)?;
        Ok(Response::new(Session {
            app_name: r.app_name,
            user_id: r.user_id,
            session_id,
            state_json: state,
            events: vec![],
            last_update_time_ms: ts,
        }))
    }

    async fn get_session(
        &self,
        request: Request<GetSessionRequest>,
    ) -> Result<Response<GetSessionResponse>, Status> {
        let r = request.into_inner();
        let meta = self
            .s()
            .query_opt(
                "SELECT state_json, last_update_time_ms FROM tape_sessions WHERE app_name=?1 AND user_id=?2 AND session_id=?3",
                vec![r.app_name.clone().into(), r.user_id.clone().into(), r.session_id.clone().into()],
            )
            .await
            .map_err(db)?;
        let Some(meta) = meta else {
            return Ok(Response::new(GetSessionResponse { found: false, session: None }));
        };
        let state_json = meta.str(0);
        let last_update = meta.i64(1);
        let limit = if r.max_events > 0 { r.max_events } else { i64::MAX };
        let rows = self
            .s()
            .query(
                "SELECT event_id, invocation_id, author, branch, content_json, actions_json, timestamp_ms \
                 FROM tape_events WHERE app_name=?1 AND user_id=?2 AND session_id=?3 ORDER BY ord LIMIT ?4",
                vec![r.app_name.clone().into(), r.user_id.clone().into(), r.session_id.clone().into(), limit.into()],
            )
            .await
            .map_err(db)?;
        let events = rows
            .iter()
            .map(|row| EventRecord {
                id: row.str(0),
                invocation_id: row.str(1),
                author: row.str(2),
                branch: row.str(3),
                content_json: row.str(4),
                actions_json: row.str(5),
                timestamp_ms: row.i64(6),
            })
            .collect();
        Ok(Response::new(GetSessionResponse {
            found: true,
            session: Some(Session {
                app_name: r.app_name,
                user_id: r.user_id,
                session_id: r.session_id,
                state_json,
                events,
                last_update_time_ms: last_update,
            }),
        }))
    }

    async fn list_sessions(
        &self,
        request: Request<ListSessionsRequest>,
    ) -> Result<Response<ListSessionsResponse>, Status> {
        let r = request.into_inner();
        let rows = self
            .s()
            .query(
                "SELECT session_id, state_json, last_update_time_ms FROM tape_sessions WHERE app_name=?1 AND user_id=?2 ORDER BY last_update_time_ms DESC",
                vec![r.app_name.clone().into(), r.user_id.clone().into()],
            )
            .await
            .map_err(db)?;
        let sessions = rows
            .iter()
            .map(|row| Session {
                app_name: r.app_name.clone(),
                user_id: r.user_id.clone(),
                session_id: row.str(0),
                state_json: row.str(1),
                events: vec![],
                last_update_time_ms: row.i64(2),
            })
            .collect();
        Ok(Response::new(ListSessionsResponse { sessions }))
    }

    async fn delete_session(
        &self,
        request: Request<DeleteSessionRequest>,
    ) -> Result<Response<DeleteSessionResponse>, Status> {
        let r = request.into_inner();
        let n = self
            .s()
            .exec(
                "DELETE FROM tape_sessions WHERE app_name=?1 AND user_id=?2 AND session_id=?3",
                vec![r.app_name.clone().into(), r.user_id.clone().into(), r.session_id.clone().into()],
            )
            .await
            .map_err(db)?;
        self.s()
            .exec(
                "DELETE FROM tape_events WHERE app_name=?1 AND user_id=?2 AND session_id=?3",
                vec![r.app_name.into(), r.user_id.into(), r.session_id.into()],
            )
            .await
            .map_err(db)?;
        Ok(Response::new(DeleteSessionResponse { deleted: n > 0 }))
    }

    async fn append_event(
        &self,
        request: Request<AppendEventRequest>,
    ) -> Result<Response<AppendEventResponse>, Status> {
        let r = request.into_inner();
        let ev = r
            .event
            .ok_or_else(|| Status::invalid_argument("append_event: missing event"))?;
        let ts = if ev.timestamp_ms > 0 { ev.timestamp_ms } else { now_ms() };

        // ord = current max + 1 (a minor race across concurrent invocations on
        // the same session is acceptable — events within one invocation are
        // serialized; the upsert below carries the merged state in the same txn).
        let ord = self
            .s()
            .query_opt(
                "SELECT COALESCE(MAX(ord), -1) + 1 FROM tape_events WHERE app_name=?1 AND user_id=?2 AND session_id=?3",
                vec![r.app_name.clone().into(), r.user_id.clone().into(), r.session_id.clone().into()],
            )
            .await
            .map_err(db)?
            .map(|row| row.i64(0))
            .unwrap_or(0);
        let cur_state = self
            .s()
            .query_opt(
                "SELECT state_json FROM tape_sessions WHERE app_name=?1 AND user_id=?2 AND session_id=?3",
                vec![r.app_name.clone().into(), r.user_id.clone().into(), r.session_id.clone().into()],
            )
            .await
            .map_err(db)?
            .map(|row| row.str(0))
            .unwrap_or_else(|| "{}".to_string());
        let delta = if r.state_delta_json.is_empty() { "{}".to_string() } else { r.state_delta_json.clone() };
        let merged = merge_json(&cur_state, &delta);

        self.s()
            .tx(vec![
                (
                    "INSERT INTO tape_events (app_name, user_id, session_id, ord, event_id, invocation_id, author, branch, content_json, actions_json, timestamp_ms) \
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)".to_string(),
                    vec![
                        r.app_name.clone().into(), r.user_id.clone().into(), r.session_id.clone().into(),
                        ord.into(), ev.id.clone().into(), ev.invocation_id.clone().into(), ev.author.clone().into(),
                        ev.branch.clone().into(), ev.content_json.clone().into(), ev.actions_json.clone().into(), ts.into(),
                    ],
                ),
                (
                    "INSERT INTO tape_sessions (app_name, user_id, session_id, state_json, last_update_time_ms) \
                     VALUES (?1,?2,?3,?4,?5) \
                     ON CONFLICT(app_name, user_id, session_id) DO UPDATE SET state_json=excluded.state_json, last_update_time_ms=excluded.last_update_time_ms".to_string(),
                    vec![r.app_name.clone().into(), r.user_id.clone().into(), r.session_id.clone().into(), merged.into(), ts.into()],
                ),
            ])
            .await
            .map_err(db)?;
        Ok(Response::new(AppendEventResponse {
            event: Some(EventRecord { timestamp_ms: ts, ..ev }),
            last_update_time_ms: ts,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::open;

    #[test]
    fn key_names_the_decision_not_the_inputs() {
        let k = derive_key("run-1", 0, "execute_sweep", 0);
        assert_eq!(k, "run-1/decision-0/execute_sweep/0");
        assert_eq!(k, derive_key("run-1", 0, "execute_sweep", 0));
        assert_ne!(k, derive_key("run-1", 0, "execute_sweep", 1));
        assert_eq!(derive_key("run-1", -1, "post_gl", 0), "run-1/no-decision/post_gl/0");
    }

    #[test]
    fn merge_json_shallow_with_null_delete() {
        let m = merge_json("{\"a\":1,\"b\":2}", "{\"b\":3,\"c\":4}");
        let v: serde_json::Value = serde_json::from_str(&m).unwrap();
        assert_eq!(v["a"], 1);
        assert_eq!(v["b"], 3);
        assert_eq!(v["c"], 4);
        let m = merge_json("{\"a\":1,\"b\":2}", "{\"b\":null}");
        let v: serde_json::Value = serde_json::from_str(&m).unwrap();
        assert!(v.get("b").is_none());
        assert_eq!(v["a"], 1);
    }

    #[tokio::test]
    async fn effect_lifecycle_over_the_store() {
        let store = open(":memory:").await.unwrap();
        let svc = TapeService::new(store);
        // begin a run
        let run = svc
            .begin_run(Request::new(BeginRunRequest {
                app_name: "a".into(), user_id: "u".into(), session_id: "s".into(),
                invocation_id: "inv".into(), lease_owner: "t".into(), lease_ttl_ms: 60_000,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!run.resumed);
        let rid = run.run_id;
        // a decision
        svc.record_decision(Request::new(RecordDecisionRequest {
            run_id: rid.clone(), decision_index: 0, model: "m".into(),
            request_json: "{}".into(), response_json: "{\"plan\":1}".into(),
            rationale: "".into(), policy_version: "p1".into(),
        }))
        .await
        .unwrap();
        assert!(svc.get_decision(Request::new(GetDecisionRequest { run_id: rid.clone(), decision_index: 0 })).await.unwrap().into_inner().found);
        // begin an effect -> PENDING
        let be = svc
            .begin_effect(Request::new(BeginEffectRequest {
                run_id: rid.clone(), decision_index: 0, tool_name: "execute_sweep".into(),
                call_index: 0, request_json: "{}".into(), custom_key: "".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(be.status, EffectStatus::Pending as i32);
        assert_eq!(be.idempotency_key, format!("{rid}/decision-0/execute_sweep/0"));
        // a second begin short-circuits to PENDING
        let be2 = svc
            .begin_effect(Request::new(BeginEffectRequest {
                run_id: rid.clone(), decision_index: 0, tool_name: "execute_sweep".into(),
                call_index: 0, request_json: "{}".into(), custom_key: "".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(be2.status, EffectStatus::Pending as i32);
        // complete -> CONFIRMED
        svc.complete_effect(Request::new(CompleteEffectRequest {
            run_id: rid.clone(), idempotency_key: be.idempotency_key.clone(),
            status: EffectStatus::Confirmed as i32, response_json: "{\"wire_id\":\"w1\"}".into(), error_json: "".into(),
        }))
        .await
        .unwrap();
        let ge = svc.get_effect(Request::new(GetEffectRequest { run_id: rid.clone(), idempotency_key: be.idempotency_key.clone() })).await.unwrap().into_inner();
        assert!(ge.found);
        assert_eq!(ge.effect.as_ref().unwrap().status, EffectStatus::Confirmed as i32);
        assert!(ge.effect.unwrap().response_json.contains("wire_id"));
        // budget admit/charge
        svc.set_budget(Request::new(SetBudgetRequest { run_id: rid.clone(), usd_cap: 1.0, token_cap: 0 })).await.unwrap();
        assert!(svc.admit_budget(Request::new(AdmitBudgetRequest { run_id: rid.clone(), usd_estimate: 0.5, token_estimate: 0 })).await.unwrap().into_inner().admitted);
        svc.charge_budget(Request::new(ChargeBudgetRequest { run_id: rid.clone(), usd: 0.9, tokens: 0 })).await.unwrap();
        assert!(!svc.admit_budget(Request::new(AdmitBudgetRequest { run_id: rid.clone(), usd_estimate: 0.5, token_estimate: 0 })).await.unwrap().into_inner().admitted);
        // end the run
        svc.end_run(Request::new(EndRunRequest { run_id: rid.clone(), status: RunStatus::Terminal as i32, detail_json: "".into() })).await.unwrap();
        assert_eq!(svc.get_run(Request::new(GetRunRequest { run_id: rid.clone() })).await.unwrap().into_inner().status, RunStatus::Terminal as i32);
        // a re-begin_run finds the existing (TERMINAL) run
        let again = svc.begin_run(Request::new(BeginRunRequest {
            app_name: "a".into(), user_id: "u".into(), session_id: "s".into(),
            invocation_id: "inv".into(), lease_owner: "t".into(), lease_ttl_ms: 60_000,
        })).await.unwrap().into_inner();
        assert!(again.resumed);
        assert_eq!(again.run_id, rid);
        assert_eq!(again.status, RunStatus::Terminal as i32);
    }
}
