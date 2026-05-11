//! The Tape gRPC service — every RPC in `tape.proto`, backed by the SQLite store.
//!
//! The shape to keep in mind while reading:
//!   * a *run* is one row in `tape_runs`, keyed by (app, user, session, invocation_id);
//!   * its *journal* is `tape_journal` ordered by `seq`, with typed detail in
//!     `tape_decisions` / `tape_effects` / `tape_obligations`;
//!   * `seq` is a per-run monotonic counter (`tape_runs.seq_cursor`) — the anchor
//!     the re-drive aligns against;
//!   * mutating RPCs are idempotent: a replay returns the recorded row.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rusqlite::{params, OptionalExtension};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Request, Response, Status};

use crate::pb::tape_server::Tape;
use crate::pb::*;
use crate::store::{now_ms, Store};

pub struct TapeService {
    store: Arc<Store>,
}

impl TapeService {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }
}

// ── small helpers ───────────────────────────────────────────────────────────

fn db(e: rusqlite::Error) -> Status {
    Status::internal(format!("tape store: {e}"))
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

/// Bump the run's seq cursor and return the new value.
fn next_seq(conn: &rusqlite::Connection, run_id: &str) -> rusqlite::Result<i64> {
    conn.execute(
        "UPDATE tape_runs SET seq_cursor = seq_cursor + 1 WHERE run_id = ?1",
        params![run_id],
    )?;
    conn.query_row(
        "SELECT seq_cursor FROM tape_runs WHERE run_id = ?1",
        params![run_id],
        |r| r.get(0),
    )
}

fn journal(
    conn: &rusqlite::Connection,
    run_id: &str,
    seq: i64,
    kind: &str,
    payload_json: &str,
    ts: i64,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO tape_journal (run_id, seq, kind, payload_json, ts_ms) VALUES (?1,?2,?3,?4,?5)",
        params![run_id, seq, kind, payload_json, ts],
    )?;
    Ok(())
}

fn read_run(conn: &rusqlite::Connection, run_id: &str) -> rusqlite::Result<Option<RunState>> {
    conn.query_row(
        "SELECT run_id, app_name, user_id, session_id, invocation_id, status, seq_cursor, \
         lease_owner, lease_expires_at_ms, started_at_ms, ended_at_ms, waiting_on_gate \
         FROM tape_runs WHERE run_id = ?1",
        params![run_id],
        |r| {
            Ok(RunState {
                run_id: r.get(0)?,
                app_name: r.get(1)?,
                user_id: r.get(2)?,
                session_id: r.get(3)?,
                invocation_id: r.get(4)?,
                status: r.get::<_, i32>(5)?,
                seq_cursor: r.get(6)?,
                lease_owner: r.get(7)?,
                lease_expires_at_ms: r.get(8)?,
                started_at_ms: r.get(9)?,
                ended_at_ms: r.get(10)?,
                waiting_on_gate: r.get(11)?,
            })
        },
    )
    .optional()
}

fn read_effect(
    conn: &rusqlite::Connection,
    run_id: &str,
    key: &str,
) -> rusqlite::Result<Option<EffectRecord>> {
    conn.query_row(
        "SELECT run_id, seq, decision_index, tool_name, idempotency_key, status, \
         request_json, response_json, error_json, ts_ms \
         FROM tape_effects WHERE run_id = ?1 AND idempotency_key = ?2",
        params![run_id, key],
        |r| {
            Ok(EffectRecord {
                run_id: r.get(0)?,
                seq: r.get(1)?,
                decision_index: r.get(2)?,
                tool_name: r.get(3)?,
                idempotency_key: r.get(4)?,
                status: r.get::<_, i32>(5)?,
                request_json: r.get(6)?,
                response_json: r.get(7)?,
                error_json: r.get(8)?,
                ts_ms: r.get(9)?,
            })
        },
    )
    .optional()
}

fn budget_state(conn: &rusqlite::Connection, run_id: &str) -> rusqlite::Result<BudgetState> {
    conn.query_row(
        "SELECT usd_cap, token_cap, usd_spent, tokens_spent FROM tape_budget WHERE run_id = ?1",
        params![run_id],
        |r| {
            Ok(BudgetState {
                run_id: run_id.to_string(),
                usd_cap: r.get(0)?,
                token_cap: r.get(1)?,
                usd_spent: r.get(2)?,
                tokens_spent: r.get(3)?,
            })
        },
    )
    .optional()
    .map(|o| {
        o.unwrap_or(BudgetState {
            run_id: run_id.to_string(),
            usd_cap: 0.0,
            token_cap: 0,
            usd_spent: 0.0,
            tokens_spent: 0,
        })
    })
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
        let conn = self.store.conn();
        let ts = now_ms();
        let lease_exp = ts + r.lease_ttl_ms.max(0);

        // Already a run for this invocation_id? -> this is a re-drive.
        let existing: Option<(String, i32, i64)> = conn
            .query_row(
                "SELECT run_id, status, seq_cursor FROM tape_runs \
                 WHERE app_name=?1 AND user_id=?2 AND session_id=?3 AND invocation_id=?4",
                params![r.app_name, r.user_id, r.session_id, r.invocation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(db)?;

        if let Some((run_id, _status, seq_cursor)) = existing {
            // Take the lease, flip to RUNNING — unless the run already finished,
            // in which case leave it TERMINAL and let the caller short-circuit.
            conn.execute(
                "UPDATE tape_runs SET status=?2, lease_owner=?3, lease_expires_at_ms=?4 \
                 WHERE run_id=?1 AND status NOT IN (?5, ?6)",
                params![
                    run_id,
                    RunStatus::Running as i32,
                    r.lease_owner,
                    lease_exp,
                    RunStatus::Terminal as i32,
                    RunStatus::Stuck as i32
                ],
            )
            .map_err(db)?;
            let cur = read_run(&conn, &run_id).map_err(db)?.unwrap();
            return Ok(Response::new(BeginRunResponse {
                run_id,
                resumed: true,
                next_seq: seq_cursor,
                status: cur.status,
            }));
        }

        let run_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO tape_runs (run_id, app_name, user_id, session_id, invocation_id, \
             status, seq_cursor, lease_owner, lease_expires_at_ms, started_at_ms) \
             VALUES (?1,?2,?3,?4,?5,?6,0,?7,?8,?9)",
            params![
                run_id,
                r.app_name,
                r.user_id,
                r.session_id,
                r.invocation_id,
                RunStatus::Running as i32,
                r.lease_owner,
                lease_exp,
                ts
            ],
        )
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
        let conn = self.store.conn();
        let ts = now_ms();
        conn.execute(
            "UPDATE tape_runs SET status=?2, lease_owner=?3, lease_expires_at_ms=?4 WHERE run_id=?1",
            params![
                r.run_id,
                RunStatus::Running as i32,
                r.lease_owner,
                ts + r.lease_ttl_ms.max(0)
            ],
        )
        .map_err(db)?;
        let run = read_run(&conn, &r.run_id)
            .map_err(db)?
            .ok_or_else(|| Status::not_found("no such run"))?;
        Ok(Response::new(ResumeRunResponse { run: Some(run) }))
    }

    async fn end_run(
        &self,
        request: Request<EndRunRequest>,
    ) -> Result<Response<EndRunResponse>, Status> {
        let r = request.into_inner();
        let conn = self.store.conn();
        conn.execute(
            "UPDATE tape_runs SET status=?2, ended_at_ms=?3, detail_json=?4, lease_owner='' \
             WHERE run_id=?1",
            params![r.run_id, r.status, now_ms(), r.detail_json],
        )
        .map_err(db)?;
        let run = read_run(&conn, &r.run_id)
            .map_err(db)?
            .ok_or_else(|| Status::not_found("no such run"))?;
        Ok(Response::new(EndRunResponse { run: Some(run) }))
    }

    async fn get_run(&self, request: Request<GetRunRequest>) -> Result<Response<RunState>, Status> {
        let r = request.into_inner();
        let conn = self.store.conn();
        let run = read_run(&conn, &r.run_id)
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
        let conn = self.store.conn();
        // Recoverable = RUNNABLE, or RUNNING with a stale lease, or WAITING with
        // a delivered-but-unconsumed signal.
        let mut stmt = conn
            .prepare(
                "SELECT r.run_id, r.app_name, r.user_id, r.session_id, r.invocation_id, r.status, \
                    r.seq_cursor, r.lease_owner, r.lease_expires_at_ms, r.started_at_ms, r.ended_at_ms, r.waiting_on_gate \
                 FROM tape_runs r \
                 WHERE r.status = ?1 \
                    OR (r.status = ?2 AND r.lease_expires_at_ms < ?3) \
                    OR (r.status = ?4 AND EXISTS ( \
                          SELECT 1 FROM tape_signals s \
                          WHERE s.run_id = r.run_id AND s.delivered = 1 AND s.consumed = 0)) \
                 LIMIT ?5",
            )
            .map_err(db)?;
        let rows = stmt
            .query_map(
                params![
                    RunStatus::Runnable as i32,
                    RunStatus::Running as i32,
                    now,
                    RunStatus::Waiting as i32,
                    limit
                ],
                |r| {
                    Ok(RunState {
                        run_id: r.get(0)?,
                        app_name: r.get(1)?,
                        user_id: r.get(2)?,
                        session_id: r.get(3)?,
                        invocation_id: r.get(4)?,
                        status: r.get::<_, i32>(5)?,
                        seq_cursor: r.get(6)?,
                        lease_owner: r.get(7)?,
                        lease_expires_at_ms: r.get(8)?,
                        started_at_ms: r.get(9)?,
                        ended_at_ms: r.get(10)?,
                        waiting_on_gate: r.get(11)?,
                    })
                },
            )
            .map_err(db)?;
        let runs: Vec<RunState> = rows.collect::<rusqlite::Result<_>>().map_err(db)?;
        Ok(Response::new(ListRunsToRecoverResponse { runs }))
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
                let batch: Vec<JournalEntry> = {
                    let conn = store.conn();
                    let mut stmt = match conn.prepare(
                        "SELECT seq, kind, payload_json, ts_ms FROM tape_journal \
                         WHERE run_id=?1 AND seq>=?2 ORDER BY seq",
                    ) {
                        Ok(s) => s,
                        Err(_) => break,
                    };
                    let mapped = stmt.query_map(params![r.run_id, from], |row| {
                        Ok(JournalEntry {
                            seq: row.get(0)?,
                            kind: row.get(1)?,
                            payload_json: row.get(2)?,
                            ts_ms: row.get(3)?,
                        })
                    });
                    match mapped {
                        Ok(it) => it.filter_map(|x| x.ok()).collect(),
                        Err(_) => break,
                    }
                };
                for e in &batch {
                    from = e.seq + 1;
                    if tx.send(Ok(e.clone())).await.is_err() {
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
        let conn = self.store.conn();
        let ts = now_ms();
        // Already recorded? -> idempotent replay.
        if let Some(rec) = conn
            .query_row(
                "SELECT run_id, seq, decision_index, model, request_json, response_json, rationale, policy_version, ts_ms \
                 FROM tape_decisions WHERE run_id=?1 AND decision_index=?2",
                params![r.run_id, r.decision_index],
                |row| Ok(DecisionRecord {
                    run_id: row.get(0)?, seq: row.get(1)?, decision_index: row.get(2)?,
                    model: row.get(3)?, request_json: row.get(4)?, response_json: row.get(5)?,
                    rationale: row.get(6)?, policy_version: row.get(7)?, ts_ms: row.get(8)?,
                }),
            )
            .optional()
            .map_err(db)?
        {
            return Ok(Response::new(rec));
        }
        let seq = next_seq(&conn, &r.run_id).map_err(db)?;
        conn.execute(
            "INSERT INTO tape_decisions (run_id, seq, decision_index, model, request_json, response_json, rationale, policy_version, ts_ms) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![r.run_id, seq, r.decision_index, r.model, r.request_json, r.response_json, r.rationale, r.policy_version, ts],
        ).map_err(db)?;
        let payload = serde_json::json!({
            "decision_index": r.decision_index, "model": r.model.clone(),
            "policy_version": r.policy_version.clone(), "rationale": r.rationale.clone()
        })
        .to_string();
        journal(&conn, &r.run_id, seq, "decision", &payload, ts).map_err(db)?;
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
        let conn = self.store.conn();
        let rec = conn
            .query_row(
                "SELECT run_id, seq, decision_index, model, request_json, response_json, rationale, policy_version, ts_ms \
                 FROM tape_decisions WHERE run_id=?1 AND decision_index=?2",
                params![r.run_id, r.decision_index],
                |row| Ok(DecisionRecord {
                    run_id: row.get(0)?, seq: row.get(1)?, decision_index: row.get(2)?,
                    model: row.get(3)?, request_json: row.get(4)?, response_json: row.get(5)?,
                    rationale: row.get(6)?, policy_version: row.get(7)?, ts_ms: row.get(8)?,
                }),
            )
            .optional()
            .map_err(db)?;
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
        let conn = self.store.conn();

        // Already on file? -> short-circuit (this is the re-drive skipping a
        // confirmed effect, or a retry of BeginEffect for a still-pending one).
        if let Some(e) = read_effect(&conn, &r.run_id, &key).map_err(db)? {
            return Ok(Response::new(BeginEffectResponse {
                seq: e.seq,
                idempotency_key: key,
                status: e.status,
                response_json: e.response_json,
                error_json: e.error_json,
            }));
        }

        // Fresh: write the intent and (autocommit) commit it before returning,
        // so the tool body runs only once the intent is durable.
        let ts = now_ms();
        let seq = next_seq(&conn, &r.run_id).map_err(db)?;
        conn.execute(
            "INSERT INTO tape_effects (run_id, seq, decision_index, tool_name, idempotency_key, status, request_json, response_json, error_json, ts_ms) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,'','',?8)",
            params![r.run_id, seq, r.decision_index, r.tool_name, key, EffectStatus::Pending as i32, r.request_json, ts],
        ).map_err(db)?;
        let payload = serde_json::json!({
            "tool": r.tool_name.clone(), "decision_index": r.decision_index,
            "idempotency_key": key.clone(), "status": "pending"
        })
        .to_string();
        journal(&conn, &r.run_id, seq, "effect", &payload, ts).map_err(db)?;
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
        let conn = self.store.conn();
        let existing = read_effect(&conn, &r.run_id, &r.idempotency_key)
            .map_err(db)?
            .ok_or_else(|| Status::failed_precondition("complete_effect before begin_effect"))?;
        // Already terminal? leave it (idempotent). Only PENDING -> {CONFIRMED|FAILED|UNKNOWN}.
        if existing.status != EffectStatus::Pending as i32 {
            return Ok(Response::new(existing));
        }
        let ts = now_ms();
        conn.execute(
            "UPDATE tape_effects SET status=?3, response_json=?4, error_json=?5, ts_ms=?6 \
             WHERE run_id=?1 AND idempotency_key=?2",
            params![r.run_id, r.idempotency_key, r.status, r.response_json, r.error_json, ts],
        )
        .map_err(db)?;
        let seq = next_seq(&conn, &r.run_id).map_err(db)?;
        let label = match effect_status(r.status) {
            EffectStatus::Confirmed => "confirmed",
            EffectStatus::Failed => "failed",
            EffectStatus::Unknown => "unknown",
            _ => "completed",
        };
        let tool_name = existing.tool_name.clone();
        let payload = serde_json::json!({
            "tool": tool_name, "idempotency_key": r.idempotency_key.clone(), "status": label
        })
        .to_string();
        journal(&conn, &r.run_id, seq, "effect", &payload, ts).map_err(db)?;
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
        let conn = self.store.conn();
        let e = read_effect(&conn, &r.run_id, &r.idempotency_key).map_err(db)?;
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
        let conn = self.store.conn();
        let existing = read_effect(&conn, &r.run_id, &r.idempotency_key)
            .map_err(db)?
            .ok_or_else(|| Status::not_found("no such effect"))?;
        // Only PENDING/UNKNOWN are reconcilable; a CONFIRMED/FAILED stays.
        if existing.status == EffectStatus::Confirmed as i32
            || existing.status == EffectStatus::Failed as i32
        {
            return Ok(Response::new(existing));
        }
        let ts = now_ms();
        conn.execute(
            "UPDATE tape_effects SET status=?3, response_json=?4, error_json=?5, ts_ms=?6 \
             WHERE run_id=?1 AND idempotency_key=?2",
            params![r.run_id, r.idempotency_key, r.resolved_status, r.response_json, r.error_json, ts],
        )
        .map_err(db)?;
        let seq = next_seq(&conn, &r.run_id).map_err(db)?;
        let tool_name = existing.tool_name.clone();
        let payload = serde_json::json!({
            "tool": tool_name, "idempotency_key": r.idempotency_key.clone(),
            "status": "reconciled", "resolved_to": r.resolved_status
        })
        .to_string();
        journal(&conn, &r.run_id, seq, "effect", &payload, ts).map_err(db)?;
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
        let conn = self.store.conn();
        // Idempotent on (run_id, effect_key, kind).
        if let Some(rec) = conn
            .query_row(
                "SELECT run_id, seq, effect_key, kind, payload_json, status, ts_ms FROM tape_obligations \
                 WHERE run_id=?1 AND effect_key=?2 AND kind=?3",
                params![r.run_id, r.effect_key, r.kind],
                |row| Ok(ObligationRecord {
                    run_id: row.get(0)?, seq: row.get(1)?, effect_key: row.get(2)?,
                    kind: row.get(3)?, payload_json: row.get(4)?, status: row.get::<_, i32>(5)?, ts_ms: row.get(6)?,
                }),
            )
            .optional()
            .map_err(db)?
        {
            return Ok(Response::new(rec));
        }
        let ts = now_ms();
        let seq = next_seq(&conn, &r.run_id).map_err(db)?;
        let status = ObligationStatus::Committed as i32;
        conn.execute(
            "INSERT INTO tape_obligations (run_id, seq, effect_key, kind, payload_json, status, ts_ms) \
             VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![r.run_id, seq, r.effect_key, r.kind, r.payload_json, status, ts],
        ).map_err(db)?;
        let payload =
            serde_json::json!({"effect_key": r.effect_key.clone(), "kind": r.kind.clone()}).to_string();
        journal(&conn, &r.run_id, seq, "obligation", &payload, ts).map_err(db)?;
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
        let conn = self.store.conn();
        let sql = if r.only_unresolved {
            "SELECT run_id, seq, effect_key, kind, payload_json, status, ts_ms FROM tape_obligations \
             WHERE run_id=?1 AND status NOT IN (3,4) ORDER BY seq DESC"
        } else {
            "SELECT run_id, seq, effect_key, kind, payload_json, status, ts_ms FROM tape_obligations \
             WHERE run_id=?1 ORDER BY seq DESC"
        };
        let mut stmt = conn.prepare(sql).map_err(db)?;
        let rows = stmt
            .query_map(params![r.run_id], |row| {
                Ok(ObligationRecord {
                    run_id: row.get(0)?,
                    seq: row.get(1)?,
                    effect_key: row.get(2)?,
                    kind: row.get(3)?,
                    payload_json: row.get(4)?,
                    status: row.get::<_, i32>(5)?,
                    ts_ms: row.get(6)?,
                })
            })
            .map_err(db)?;
        let obligations: Vec<ObligationRecord> = rows.collect::<rusqlite::Result<_>>().map_err(db)?;
        Ok(Response::new(ListObligationsResponse { obligations }))
    }

    async fn resolve_obligation(
        &self,
        request: Request<ResolveObligationRequest>,
    ) -> Result<Response<ObligationRecord>, Status> {
        let r = request.into_inner();
        let conn = self.store.conn();
        let ts = now_ms();
        conn.execute(
            "UPDATE tape_obligations SET status=?3, ts_ms=?4 WHERE run_id=?1 AND seq=?2",
            params![r.run_id, r.obligation_seq, r.status, ts],
        )
        .map_err(db)?;
        let rec = conn
            .query_row(
                "SELECT run_id, seq, effect_key, kind, payload_json, status, ts_ms FROM tape_obligations \
                 WHERE run_id=?1 AND seq=?2",
                params![r.run_id, r.obligation_seq],
                |row| Ok(ObligationRecord {
                    run_id: row.get(0)?, seq: row.get(1)?, effect_key: row.get(2)?,
                    kind: row.get(3)?, payload_json: row.get(4)?, status: row.get::<_, i32>(5)?, ts_ms: row.get(6)?,
                }),
            )
            .optional()
            .map_err(db)?
            .ok_or_else(|| Status::not_found("no such obligation"))?;
        Ok(Response::new(rec))
    }

    // ─── budget ─────────────────────────────────────────────────────────────

    async fn set_budget(
        &self,
        request: Request<SetBudgetRequest>,
    ) -> Result<Response<BudgetState>, Status> {
        let r = request.into_inner();
        let conn = self.store.conn();
        conn.execute(
            "INSERT INTO tape_budget (run_id, usd_cap, token_cap, usd_spent, tokens_spent) \
             VALUES (?1,?2,?3,0,0) \
             ON CONFLICT(run_id) DO UPDATE SET usd_cap=excluded.usd_cap, token_cap=excluded.token_cap",
            params![r.run_id, r.usd_cap, r.token_cap],
        )
        .map_err(db)?;
        Ok(Response::new(budget_state(&conn, &r.run_id).map_err(db)?))
    }

    async fn admit_budget(
        &self,
        request: Request<AdmitBudgetRequest>,
    ) -> Result<Response<AdmitBudgetResponse>, Status> {
        let r = request.into_inner();
        let conn = self.store.conn();
        let b = budget_state(&conn, &r.run_id).map_err(db)?;
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
        let conn = self.store.conn();
        conn.execute(
            "INSERT INTO tape_budget (run_id, usd_spent, tokens_spent) VALUES (?1,?2,?3) \
             ON CONFLICT(run_id) DO UPDATE SET usd_spent = usd_spent + ?2, tokens_spent = tokens_spent + ?3",
            params![r.run_id, r.usd, r.tokens],
        )
        .map_err(db)?;
        Ok(Response::new(budget_state(&conn, &r.run_id).map_err(db)?))
    }

    // ─── gates / signals ────────────────────────────────────────────────────

    async fn await_signal(
        &self,
        request: Request<AwaitSignalRequest>,
    ) -> Result<Response<AwaitSignalResponse>, Status> {
        let r = request.into_inner();
        let conn = self.store.conn();
        let ts = now_ms();
        // Was the signal already delivered (a re-drive after SendSignal)? -> hand it over.
        let delivered: Option<(i32, i32, String)> = conn
            .query_row(
                "SELECT delivered, consumed, resolution_json FROM tape_signals WHERE run_id=?1 AND gate_name=?2",
                params![r.run_id, r.gate_name],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(db)?;
        if let Some((1, _, resolution)) = delivered {
            conn.execute(
                "UPDATE tape_signals SET consumed=1 WHERE run_id=?1 AND gate_name=?2",
                params![r.run_id, r.gate_name],
            )
            .map_err(db)?;
            return Ok(Response::new(AwaitSignalResponse {
                delivered: true,
                resolution_json: resolution,
            }));
        }
        // Otherwise: park the run on this gate.
        conn.execute(
            "INSERT INTO tape_signals (run_id, gate_name, context_json, awaited, created_at_ms) \
             VALUES (?1,?2,?3,1,?4) \
             ON CONFLICT(run_id, gate_name) DO UPDATE SET awaited=1, context_json=excluded.context_json",
            params![r.run_id, r.gate_name, r.payload_json, ts],
        )
        .map_err(db)?;
        let seq = next_seq(&conn, &r.run_id).map_err(db)?;
        conn.execute(
            "UPDATE tape_runs SET status=?2, waiting_on_gate=?3, lease_owner='' WHERE run_id=?1",
            params![r.run_id, RunStatus::Waiting as i32, r.gate_name],
        )
        .map_err(db)?;
        journal(
            &conn,
            &r.run_id,
            seq,
            "gate",
            &serde_json::json!({"gate": r.gate_name, "status": "waiting"}).to_string(),
            ts,
        )
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
        let conn = self.store.conn();
        let ts = now_ms();
        // Resolve the run.
        let run_id = if !r.run_id.is_empty() {
            r.run_id.clone()
        } else {
            conn.query_row(
                "SELECT run_id FROM tape_runs WHERE app_name=?1 AND user_id=?2 AND session_id=?3 \
                 ORDER BY started_at_ms DESC LIMIT 1",
                params![r.app_name, r.user_id, r.session_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(db)?
            .ok_or_else(|| Status::not_found("no run for that session"))?
        };
        conn.execute(
            "INSERT INTO tape_signals (run_id, gate_name, resolution_json, delivered, created_at_ms) \
             VALUES (?1,?2,?3,1,?4) \
             ON CONFLICT(run_id, gate_name) DO UPDATE SET resolution_json=excluded.resolution_json, delivered=1",
            params![run_id, r.gate_name, r.resolution_json, ts],
        )
        .map_err(db)?;
        // If the run is parked on this gate, release it.
        let mut run_status = RunStatus::Unspecified;
        let waiting_on: Option<String> = conn
            .query_row(
                "SELECT waiting_on_gate FROM tape_runs WHERE run_id=?1 AND status=?2",
                params![run_id, RunStatus::Waiting as i32],
                |row| row.get(0),
            )
            .optional()
            .map_err(db)?;
        if let Some(gate) = waiting_on {
            if gate == r.gate_name {
                conn.execute(
                    "UPDATE tape_runs SET status=?2, waiting_on_gate='' WHERE run_id=?1",
                    params![run_id, RunStatus::Runnable as i32],
                )
                .map_err(db)?;
                run_status = RunStatus::Runnable;
                if let Ok(seq) = next_seq(&conn, &run_id) {
                    let _ = journal(
                        &conn,
                        &run_id,
                        seq,
                        "gate",
                        &serde_json::json!({"gate": r.gate_name, "status": "released"}).to_string(),
                        ts,
                    );
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
        let conn = self.store.conn();
        let ts = now_ms();
        let session_id = if r.session_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            r.session_id.clone()
        };
        let state = if r.state_json.is_empty() { "{}".to_string() } else { r.state_json.clone() };
        conn.execute(
            "INSERT INTO tape_sessions (app_name, user_id, session_id, state_json, last_update_time_ms) \
             VALUES (?1,?2,?3,?4,?5) \
             ON CONFLICT(app_name, user_id, session_id) DO UPDATE SET state_json=excluded.state_json, last_update_time_ms=excluded.last_update_time_ms",
            params![r.app_name, r.user_id, session_id, state, ts],
        ).map_err(db)?;
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
        let conn = self.store.conn();
        let row: Option<(String, i64)> = conn
            .query_row(
                "SELECT state_json, last_update_time_ms FROM tape_sessions WHERE app_name=?1 AND user_id=?2 AND session_id=?3",
                params![r.app_name, r.user_id, r.session_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(db)?;
        let Some((state_json, last_update)) = row else {
            return Ok(Response::new(GetSessionResponse { found: false, session: None }));
        };
        let limit = if r.max_events > 0 { r.max_events } else { i64::MAX };
        let mut stmt = conn
            .prepare(
                "SELECT event_id, invocation_id, author, branch, content_json, actions_json, timestamp_ms \
                 FROM tape_events WHERE app_name=?1 AND user_id=?2 AND session_id=?3 ORDER BY ord LIMIT ?4",
            )
            .map_err(db)?;
        let events: Vec<EventRecord> = stmt
            .query_map(params![r.app_name, r.user_id, r.session_id, limit], |row| {
                Ok(EventRecord {
                    id: row.get(0)?,
                    invocation_id: row.get(1)?,
                    author: row.get(2)?,
                    branch: row.get(3)?,
                    content_json: row.get(4)?,
                    actions_json: row.get(5)?,
                    timestamp_ms: row.get(6)?,
                })
            })
            .map_err(db)?
            .collect::<rusqlite::Result<_>>()
            .map_err(db)?;
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
        let conn = self.store.conn();
        let mut stmt = conn
            .prepare(
                "SELECT session_id, state_json, last_update_time_ms FROM tape_sessions WHERE app_name=?1 AND user_id=?2 ORDER BY last_update_time_ms DESC",
            )
            .map_err(db)?;
        let sessions: Vec<Session> = stmt
            .query_map(params![r.app_name, r.user_id], |row| {
                Ok(Session {
                    app_name: r.app_name.clone(),
                    user_id: r.user_id.clone(),
                    session_id: row.get(0)?,
                    state_json: row.get(1)?,
                    events: vec![],
                    last_update_time_ms: row.get(2)?,
                })
            })
            .map_err(db)?
            .collect::<rusqlite::Result<_>>()
            .map_err(db)?;
        Ok(Response::new(ListSessionsResponse { sessions }))
    }

    async fn delete_session(
        &self,
        request: Request<DeleteSessionRequest>,
    ) -> Result<Response<DeleteSessionResponse>, Status> {
        let r = request.into_inner();
        let conn = self.store.conn();
        let n = conn
            .execute(
                "DELETE FROM tape_sessions WHERE app_name=?1 AND user_id=?2 AND session_id=?3",
                params![r.app_name, r.user_id, r.session_id],
            )
            .map_err(db)?;
        conn.execute(
            "DELETE FROM tape_events WHERE app_name=?1 AND user_id=?2 AND session_id=?3",
            params![r.app_name, r.user_id, r.session_id],
        )
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
        let mut guard = self.store.conn();
        let tx = guard.transaction().map_err(db)?;

        // append order = current max + 1
        let ord: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(ord), -1) + 1 FROM tape_events WHERE app_name=?1 AND user_id=?2 AND session_id=?3",
                params![r.app_name, r.user_id, r.session_id],
                |row| row.get(0),
            )
            .map_err(db)?;
        tx.execute(
            "INSERT INTO tape_events (app_name, user_id, session_id, ord, event_id, invocation_id, author, branch, content_json, actions_json, timestamp_ms) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![r.app_name, r.user_id, r.session_id, ord, ev.id, ev.invocation_id, ev.author, ev.branch, ev.content_json, ev.actions_json, ts],
        ).map_err(db)?;

        // apply state_delta in the same txn (run-scoped keys only here; the
        // user:/app: prefixes go to tape_scoped_state — left simple for v1).
        if !r.state_delta_json.is_empty() && r.state_delta_json != "{}" {
            let cur: String = tx
                .query_row(
                    "SELECT state_json FROM tape_sessions WHERE app_name=?1 AND user_id=?2 AND session_id=?3",
                    params![r.app_name, r.user_id, r.session_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(db)?
                .unwrap_or_else(|| "{}".to_string());
            let merged = merge_json(&cur, &r.state_delta_json);
            tx.execute(
                "INSERT INTO tape_sessions (app_name, user_id, session_id, state_json, last_update_time_ms) \
                 VALUES (?1,?2,?3,?4,?5) \
                 ON CONFLICT(app_name, user_id, session_id) DO UPDATE SET state_json=excluded.state_json, last_update_time_ms=excluded.last_update_time_ms",
                params![r.app_name, r.user_id, r.session_id, merged, ts],
            ).map_err(db)?;
        } else {
            tx.execute(
                "INSERT INTO tape_sessions (app_name, user_id, session_id, state_json, last_update_time_ms) \
                 VALUES (?1,?2,?3,'{}',?4) \
                 ON CONFLICT(app_name, user_id, session_id) DO UPDATE SET last_update_time_ms=excluded.last_update_time_ms",
                params![r.app_name, r.user_id, r.session_id, ts],
            ).map_err(db)?;
        }
        tx.commit().map_err(db)?;
        Ok(Response::new(AppendEventResponse {
            event: Some(EventRecord { timestamp_ms: ts, ..ev }),
            last_update_time_ms: ts,
        }))
    }
}

/// Shallow-merge `delta` (a JSON object) into `base` (a JSON object). A `null`
/// value in the delta deletes the key.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_names_the_decision_not_the_inputs() {
        let k = derive_key("run-1", 0, "execute_sweep", 0);
        assert_eq!(k, "run-1/decision-0/execute_sweep/0");
        // Same decision, same call -> same key, regardless of any recomputed args.
        assert_eq!(k, derive_key("run-1", 0, "execute_sweep", 0));
        // A second call of the same tool under the same decision is distinct.
        assert_ne!(k, derive_key("run-1", 0, "execute_sweep", 1));
        // No authorizing decision -> a stable "no-decision" key.
        assert_eq!(derive_key("run-1", -1, "post_gl", 0), "run-1/no-decision/post_gl/0");
    }

    #[test]
    fn merge_json_shallow_with_null_delete() {
        assert_eq!(merge_json("{}", "{\"a\":1}"), "{\"a\":1}");
        let m = merge_json("{\"a\":1,\"b\":2}", "{\"b\":3,\"c\":4}");
        let v: serde_json::Value = serde_json::from_str(&m).unwrap();
        assert_eq!(v["a"], 1);
        assert_eq!(v["b"], 3);
        assert_eq!(v["c"], 4);
        // null deletes
        let m = merge_json("{\"a\":1,\"b\":2}", "{\"b\":null}");
        let v: serde_json::Value = serde_json::from_str(&m).unwrap();
        assert!(v.get("b").is_none());
        assert_eq!(v["a"], 1);
    }

    #[test]
    fn effect_lifecycle_in_store() {
        // A tiny end-to-end over the store: begin_run -> begin_effect (pending)
        // -> complete_effect (confirmed) -> get_effect sees confirmed; a second
        // begin_effect for the same key short-circuits.
        let store = std::sync::Arc::new(Store::open(":memory:").unwrap());
        let conn = store.conn();
        let ts = now_ms();
        conn.execute(
            "INSERT INTO tape_runs (run_id, app_name, user_id, session_id, invocation_id, status, seq_cursor, lease_owner, lease_expires_at_ms, started_at_ms) \
             VALUES ('r','a','u','s','inv',2,0,'',0,?1)",
            params![ts],
        ).unwrap();
        // begin_effect: write pending
        let seq = next_seq(&conn, "r").unwrap();
        conn.execute(
            "INSERT INTO tape_effects (run_id, seq, decision_index, tool_name, idempotency_key, status, request_json, response_json, error_json, ts_ms) \
             VALUES ('r',?1,0,'execute_sweep','r/decision-0/execute_sweep/0',?2,'{}','','',?3)",
            params![seq, EffectStatus::Pending as i32, ts],
        ).unwrap();
        let e = read_effect(&conn, "r", "r/decision-0/execute_sweep/0").unwrap().unwrap();
        assert_eq!(e.status, EffectStatus::Pending as i32);
        // complete: confirmed
        conn.execute(
            "UPDATE tape_effects SET status=?1, response_json='{\"wire_id\":\"w1\"}' WHERE run_id='r' AND idempotency_key='r/decision-0/execute_sweep/0'",
            params![EffectStatus::Confirmed as i32],
        ).unwrap();
        let e = read_effect(&conn, "r", "r/decision-0/execute_sweep/0").unwrap().unwrap();
        assert_eq!(e.status, EffectStatus::Confirmed as i32);
        assert!(e.response_json.contains("wire_id"));
    }
}
