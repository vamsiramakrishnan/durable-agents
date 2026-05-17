//! The Tape gRPC service — pure plumbing over a [`RunStore`]. Every RPC maps to
//! one store operation; the ordering, sequencing and journaling all live in the
//! store implementation (so the same logic works over SQL or Bigtable).

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Request, Response, Status};

use crate::pb::tape_server::Tape;
use crate::pb::*;
use crate::store::{now_ms, RunStore, StoreError};

pub struct TapeService {
    store: Arc<dyn RunStore>,
}
impl TapeService {
    pub fn new(store: Arc<dyn RunStore>) -> Self {
        Self { store }
    }
}

fn db(e: StoreError) -> Status {
    Status::internal(e.to_string())
}

const DEFAULT_LEASE_MS: i64 = 120_000;

#[tonic::async_trait]
impl Tape for TapeService {
    // ── run lifecycle ───────────────────────────────────────────────────────
    async fn begin_run(&self, req: Request<BeginRunRequest>) -> Result<Response<BeginRunResponse>, Status> {
        let r = req.into_inner();
        let ttl = if r.lease_ttl_ms > 0 { r.lease_ttl_ms } else { DEFAULT_LEASE_MS };
        Ok(Response::new(self.store
            .begin_run(&r.app_name, &r.user_id, &r.session_id, &r.invocation_id, &r.lease_owner, ttl)
            .await.map_err(db)?))
    }
    async fn resume_run(&self, req: Request<ResumeRunRequest>) -> Result<Response<ResumeRunResponse>, Status> {
        let r = req.into_inner();
        let ttl = if r.lease_ttl_ms > 0 { r.lease_ttl_ms } else { DEFAULT_LEASE_MS };
        let run = self.store.resume_run(&r.run_id, &r.lease_owner, ttl).await.map_err(db)?
            .ok_or_else(|| Status::not_found("no such run"))?;
        Ok(Response::new(ResumeRunResponse { run: Some(run) }))
    }
    async fn end_run(&self, req: Request<EndRunRequest>) -> Result<Response<EndRunResponse>, Status> {
        let r = req.into_inner();
        let run = self.store.end_run(&r.run_id, r.status, &r.detail_json).await.map_err(db)?
            .ok_or_else(|| Status::not_found("no such run"))?;
        Ok(Response::new(EndRunResponse { run: Some(run) }))
    }
    async fn get_run(&self, req: Request<GetRunRequest>) -> Result<Response<RunState>, Status> {
        let r = req.into_inner();
        Ok(Response::new(self.store.get_run(&r.run_id).await.map_err(db)?
            .ok_or_else(|| Status::not_found("no such run"))?))
    }
    async fn list_runs_to_recover(&self, req: Request<ListRunsToRecoverRequest>) -> Result<Response<ListRunsToRecoverResponse>, Status> {
        let r = req.into_inner();
        let now = if r.now_ms > 0 { r.now_ms } else { now_ms() };
        let limit = if r.limit > 0 { r.limit } else { 100 };
        Ok(Response::new(ListRunsToRecoverResponse { runs: self.store.list_runs_to_recover(now, limit).await.map_err(db)? }))
    }

    type SubscribeRunStream = Pin<Box<dyn Stream<Item = Result<JournalEntry, Status>> + Send + 'static>>;
    async fn subscribe_run(&self, req: Request<SubscribeRunRequest>) -> Result<Response<Self::SubscribeRunStream>, Status> {
        let r = req.into_inner();
        let store = self.store.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            let mut from = r.from_seq;
            loop {
                let batch = match store.journal_range(&r.run_id, from).await {
                    Ok(b) => b,
                    Err(_) => break,
                };
                for e in batch {
                    from = e.seq + 1;
                    if tx.send(Ok(e)).await.is_err() { return; }
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    // ── decisions ───────────────────────────────────────────────────────────
    async fn record_decision(&self, req: Request<RecordDecisionRequest>) -> Result<Response<DecisionRecord>, Status> {
        let r = req.into_inner();
        Ok(Response::new(self.store
            .record_decision(&r.run_id, r.decision_index, &r.model, &r.request_json, &r.response_json, &r.rationale, &r.policy_version)
            .await.map_err(db)?))
    }
    async fn get_decision(&self, req: Request<GetDecisionRequest>) -> Result<Response<GetDecisionResponse>, Status> {
        let r = req.into_inner();
        let d = self.store.get_decision(&r.run_id, r.decision_index).await.map_err(db)?;
        Ok(Response::new(GetDecisionResponse { found: d.is_some(), decision: d }))
    }

    // ── effects ─────────────────────────────────────────────────────────────
    async fn begin_effect(&self, req: Request<BeginEffectRequest>) -> Result<Response<BeginEffectResponse>, Status> {
        let r = req.into_inner();
        let e = self.store.begin_effect(&r.run_id, r.decision_index, &r.tool_name, r.call_index, &r.request_json, &r.custom_key).await.map_err(db)?;
        Ok(Response::new(BeginEffectResponse {
            seq: e.seq, idempotency_key: e.idempotency_key, status: e.status,
            response_json: e.response_json, error_json: e.error_json,
        }))
    }
    async fn complete_effect(&self, req: Request<CompleteEffectRequest>) -> Result<Response<EffectRecord>, Status> {
        let r = req.into_inner();
        Ok(Response::new(self.store.complete_effect(&r.run_id, &r.idempotency_key, r.status, &r.response_json, &r.error_json).await.map_err(db)?
            .ok_or_else(|| Status::failed_precondition("complete_effect before begin_effect"))?))
    }
    async fn get_effect(&self, req: Request<GetEffectRequest>) -> Result<Response<GetEffectResponse>, Status> {
        let r = req.into_inner();
        let e = self.store.get_effect(&r.run_id, &r.idempotency_key).await.map_err(db)?;
        Ok(Response::new(GetEffectResponse { found: e.is_some(), effect: e }))
    }
    async fn reconcile_effect(&self, req: Request<ReconcileEffectRequest>) -> Result<Response<EffectRecord>, Status> {
        let r = req.into_inner();
        Ok(Response::new(self.store.reconcile_effect(&r.run_id, &r.idempotency_key, r.resolved_status, &r.response_json, &r.error_json).await.map_err(db)?
            .ok_or_else(|| Status::not_found("no such effect"))?))
    }

    // ── obligations ─────────────────────────────────────────────────────────
    async fn register_compensation(&self, req: Request<RegisterCompensationRequest>) -> Result<Response<ObligationRecord>, Status> {
        let r = req.into_inner();
        Ok(Response::new(self.store
            .register_compensation(&r.run_id, &r.effect_key, &r.kind, &r.payload_json,
                                   &r.compensator_ref, r.max_attempts)
            .await.map_err(db)?))
    }
    async fn list_obligations(&self, req: Request<ListObligationsRequest>) -> Result<Response<ListObligationsResponse>, Status> {
        let r = req.into_inner();
        Ok(Response::new(ListObligationsResponse {
            obligations: self.store.list_obligations(&r.run_id, r.only_unresolved, r.status_filter).await.map_err(db)?,
        }))
    }
    async fn list_unresolved_obligations(&self, req: Request<ListUnresolvedObligationsRequest>)
        -> Result<Response<ListUnresolvedObligationsResponse>, Status> {
        let r = req.into_inner();
        // Default: include_pending=true, include_committed_expired=true (the drainer
        // wants ready-to-run and reclaim-stale-lease in one pass). Caller can flip.
        let include_pending = r.include_pending || (!r.include_stuck && !r.include_committed_expired);
        let include_committed_expired = r.include_committed_expired || include_pending;
        let now = if r.now_ms > 0 { r.now_ms } else { now_ms() };
        let limit = if r.limit > 0 { r.limit as i64 } else { 500 };
        Ok(Response::new(ListUnresolvedObligationsResponse {
            obligations: self.store
                .list_unresolved_obligations(now, include_pending, r.include_stuck, include_committed_expired, limit)
                .await.map_err(db)?,
        }))
    }
    async fn claim_obligation(&self, req: Request<ClaimObligationRequest>) -> Result<Response<ClaimObligationResponse>, Status> {
        let r = req.into_inner();
        let (acquired, ob) = self.store
            .claim_obligation(&r.run_id, r.obligation_seq, &r.claimer, r.lease_ttl_ms, now_ms())
            .await.map_err(db)?;
        Ok(Response::new(ClaimObligationResponse { acquired, obligation: ob }))
    }
    async fn record_obligation_attempt(&self, req: Request<RecordObligationAttemptRequest>) -> Result<Response<ObligationRecord>, Status> {
        let r = req.into_inner();
        Ok(Response::new(self.store
            .record_obligation_attempt(&r.run_id, r.obligation_seq, &r.error, r.next_attempt_at_ms)
            .await.map_err(db)?
            .ok_or_else(|| Status::not_found("no such obligation"))?))
    }
    async fn resolve_obligation(&self, req: Request<ResolveObligationRequest>) -> Result<Response<ObligationRecord>, Status> {
        let r = req.into_inner();
        Ok(Response::new(self.store.resolve_obligation(&r.run_id, r.obligation_seq, r.status, &r.result_json).await.map_err(db)?
            .ok_or_else(|| Status::not_found("no such obligation"))?))
    }

    // ── budget ──────────────────────────────────────────────────────────────
    async fn set_budget(&self, req: Request<SetBudgetRequest>) -> Result<Response<BudgetState>, Status> {
        let r = req.into_inner();
        Ok(Response::new(self.store.set_budget(&r.run_id, r.usd_cap, r.token_cap).await.map_err(db)?))
    }
    async fn admit_budget(&self, req: Request<AdmitBudgetRequest>) -> Result<Response<AdmitBudgetResponse>, Status> {
        let r = req.into_inner();
        let b = self.store.get_budget(&r.run_id).await.map_err(db)?;
        let (mut admitted, mut reason) = (true, String::new());
        if b.usd_cap > 0.0 && b.usd_spent + r.usd_estimate > b.usd_cap {
            admitted = false;
            reason = format!("usd cap {:.2} would be exceeded (spent {:.2} + estimate {:.2})", b.usd_cap, b.usd_spent, r.usd_estimate);
        }
        if admitted && b.token_cap > 0 && b.tokens_spent + r.token_estimate > b.token_cap {
            admitted = false;
            reason = format!("token cap {} would be exceeded (spent {} + estimate {})", b.token_cap, b.tokens_spent, r.token_estimate);
        }
        Ok(Response::new(AdmitBudgetResponse { admitted, reason, budget: Some(b) }))
    }
    async fn charge_budget(&self, req: Request<ChargeBudgetRequest>) -> Result<Response<BudgetState>, Status> {
        let r = req.into_inner();
        Ok(Response::new(self.store.charge_budget(&r.run_id, r.usd, r.tokens).await.map_err(db)?))
    }

    // ── gates / signals ─────────────────────────────────────────────────────
    async fn await_signal(&self, req: Request<AwaitSignalRequest>) -> Result<Response<AwaitSignalResponse>, Status> {
        let r = req.into_inner();
        let (delivered, resolution) = self.store.await_signal(&r.run_id, &r.gate_name, &r.payload_json).await.map_err(db)?;
        Ok(Response::new(AwaitSignalResponse { delivered, resolution_json: resolution }))
    }
    async fn send_signal(&self, req: Request<SendSignalRequest>) -> Result<Response<SendSignalResponse>, Status> {
        let r = req.into_inner();
        let (run_id, status) = self.store.send_signal(&r.run_id, &r.app_name, &r.user_id, &r.session_id, &r.gate_name, &r.resolution_json).await.map_err(db)?;
        Ok(Response::new(SendSignalResponse { accepted: true, run_id, run_status: status }))
    }

    // ── ADK SessionService shim ─────────────────────────────────────────────
    async fn create_session(&self, req: Request<CreateSessionRequest>) -> Result<Response<Session>, Status> {
        let r = req.into_inner();
        Ok(Response::new(self.store.create_session(&r.app_name, &r.user_id, &r.session_id, &r.state_json).await.map_err(db)?))
    }
    async fn get_session(&self, req: Request<GetSessionRequest>) -> Result<Response<GetSessionResponse>, Status> {
        let r = req.into_inner();
        let s = self.store.get_session(&r.app_name, &r.user_id, &r.session_id, r.max_events).await.map_err(db)?;
        Ok(Response::new(GetSessionResponse { found: s.is_some(), session: s }))
    }
    async fn list_sessions(&self, req: Request<ListSessionsRequest>) -> Result<Response<ListSessionsResponse>, Status> {
        let r = req.into_inner();
        Ok(Response::new(ListSessionsResponse { sessions: self.store.list_sessions(&r.app_name, &r.user_id).await.map_err(db)? }))
    }
    async fn delete_session(&self, req: Request<DeleteSessionRequest>) -> Result<Response<DeleteSessionResponse>, Status> {
        let r = req.into_inner();
        Ok(Response::new(DeleteSessionResponse { deleted: self.store.delete_session(&r.app_name, &r.user_id, &r.session_id).await.map_err(db)? }))
    }
    async fn append_event(&self, req: Request<AppendEventRequest>) -> Result<Response<AppendEventResponse>, Status> {
        let r = req.into_inner();
        let ev = r.event.ok_or_else(|| Status::invalid_argument("append_event: missing event"))?;
        let (event, last_update) = self.store.append_event(&r.app_name, &r.user_id, &r.session_id, ev, &r.state_delta_json).await.map_err(db)?;
        Ok(Response::new(AppendEventResponse { event: Some(event), last_update_time_ms: last_update }))
    }

    // ── reconciliation ──────────────────────────────────────────────────────
    async fn list_pending_effects(&self, req: Request<ListPendingEffectsRequest>) -> Result<Response<ListPendingEffectsResponse>, Status> {
        let r = req.into_inner();
        let limit = if r.limit > 0 { r.limit } else { 200 };
        Ok(Response::new(ListPendingEffectsResponse {
            effects: self.store.list_pending_effects(r.older_than_ms, r.include_pending, r.include_unknown, limit).await.map_err(db)?,
        }))
    }

    // ── timers ──────────────────────────────────────────────────────────────
    async fn set_timer(&self, req: Request<SetTimerRequest>) -> Result<Response<TimerRecord>, Status> {
        let r = req.into_inner();
        Ok(Response::new(self.store.set_timer(&r.run_id, &r.timer_id, r.fire_at_ms, &r.kind, &r.payload_json).await.map_err(db)?))
    }
    async fn cancel_timer(&self, req: Request<CancelTimerRequest>) -> Result<Response<CancelTimerResponse>, Status> {
        let r = req.into_inner();
        Ok(Response::new(CancelTimerResponse { cancelled: self.store.cancel_timer(&r.run_id, &r.timer_id).await.map_err(db)? }))
    }
    async fn list_due_timers(&self, req: Request<ListDueTimersRequest>) -> Result<Response<ListDueTimersResponse>, Status> {
        let r = req.into_inner();
        let now = if r.now_ms > 0 { r.now_ms } else { now_ms() };
        let limit = if r.limit > 0 { r.limit } else { 200 };
        Ok(Response::new(ListDueTimersResponse { timers: self.store.list_due_timers(now, limit, r.claim).await.map_err(db)? }))
    }

    // ── reactive key-value store ────────────────────────────────────────────
    async fn write_value(&self, req: Request<WriteValueRequest>) -> Result<Response<ValueRecord>, Status> {
        let r = req.into_inner();
        Ok(Response::new(self.store.write_value(&r.namespace, &r.key, &r.value_json, r.if_version, &r.writer).await.map_err(db)?))
    }
    async fn get_value(&self, req: Request<GetValueRequest>) -> Result<Response<GetValueResponse>, Status> {
        let r = req.into_inner();
        let v = self.store.get_value(&r.namespace, &r.key).await.map_err(db)?;
        Ok(Response::new(GetValueResponse { found: v.is_some() && !v.as_ref().map(|x| x.deleted).unwrap_or(false), value: v }))
    }
    async fn delete_value(&self, req: Request<DeleteValueRequest>) -> Result<Response<DeleteValueResponse>, Status> {
        let r = req.into_inner();
        let (deleted, version) = self.store.delete_value(&r.namespace, &r.key).await.map_err(db)?;
        Ok(Response::new(DeleteValueResponse { deleted, version }))
    }

    type WatchValueStream = Pin<Box<dyn Stream<Item = Result<ValueEvent, Status>> + Send + 'static>>;
    async fn watch_value(&self, req: Request<WatchValueRequest>) -> Result<Response<Self::WatchValueStream>, Status> {
        let r = req.into_inner();
        let store = self.store.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            let mut from = r.from_version;
            let mut prev_version: i64 = 0;
            let mut prev_value_json = String::new();
            loop {
                match store.get_value_if_newer(&r.namespace, &r.key, from).await {
                    Ok(Some(rec)) => {
                        let evt = ValueEvent {
                            value: Some(rec.clone()), prev_version, prev_value_json: prev_value_json.clone(),
                        };
                        from = rec.version;
                        prev_version = rec.version;
                        prev_value_json = rec.value_json.clone();
                        if tx.send(Ok(evt)).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => {}
                    Err(_) => break,
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    // ── the WAL tail (at-least-once; entries at the boundary ts may repeat — the
    //    reactors / fan-out de-dup on (run_id, seq), which is harmless since
    //    re-processing a journal entry is idempotent) ─────────────────────────
    type SubscribeEventsStream = Pin<Box<dyn Stream<Item = Result<EventEntry, Status>> + Send + 'static>>;
    async fn subscribe_events(&self, req: Request<SubscribeEventsRequest>) -> Result<Response<Self::SubscribeEventsStream>, Status> {
        let r = req.into_inner();
        let store = self.store.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            let mut from = r.from_ts_ms;
            loop {
                let batch = match store.events_since(from, &r.run_id, &r.kind, 512).await {
                    Ok(b) => b,
                    Err(_) => break,
                };
                for e in batch {
                    from = from.max(e.ts_ms);
                    if tx.send(Ok(e)).await.is_err() {
                        return;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::open;

    #[tokio::test]
    async fn effect_lifecycle_over_the_store() {
        let store = open(":memory:").await.unwrap();
        let svc = TapeService::new(store);
        let run = svc.begin_run(Request::new(BeginRunRequest {
            app_name: "a".into(), user_id: "u".into(), session_id: "s".into(),
            invocation_id: "inv".into(), lease_owner: "t".into(), lease_ttl_ms: 60_000,
        })).await.unwrap().into_inner();
        assert!(!run.resumed);
        let rid = run.run_id;
        svc.record_decision(Request::new(RecordDecisionRequest {
            run_id: rid.clone(), decision_index: 0, model: "m".into(),
            request_json: "{}".into(), response_json: "{\"plan\":1}".into(),
            rationale: "".into(), policy_version: "p1".into(),
        })).await.unwrap();
        assert!(svc.get_decision(Request::new(GetDecisionRequest { run_id: rid.clone(), decision_index: 0 })).await.unwrap().into_inner().found);
        let be = svc.begin_effect(Request::new(BeginEffectRequest {
            run_id: rid.clone(), decision_index: 0, tool_name: "execute_sweep".into(),
            call_index: 0, request_json: "{}".into(), custom_key: "".into(),
        })).await.unwrap().into_inner();
        assert_eq!(be.status, EffectStatus::Pending as i32);
        assert_eq!(be.idempotency_key, format!("{rid}/decision-0/execute_sweep/0"));
        let be2 = svc.begin_effect(Request::new(BeginEffectRequest {
            run_id: rid.clone(), decision_index: 0, tool_name: "execute_sweep".into(),
            call_index: 0, request_json: "{}".into(), custom_key: "".into(),
        })).await.unwrap().into_inner();
        assert_eq!(be2.status, EffectStatus::Pending as i32);
        svc.complete_effect(Request::new(CompleteEffectRequest {
            run_id: rid.clone(), idempotency_key: be.idempotency_key.clone(),
            status: EffectStatus::Confirmed as i32, response_json: "{\"wire_id\":\"w1\"}".into(), error_json: "".into(),
        })).await.unwrap();
        let ge = svc.get_effect(Request::new(GetEffectRequest { run_id: rid.clone(), idempotency_key: be.idempotency_key.clone() })).await.unwrap().into_inner();
        assert!(ge.found);
        assert_eq!(ge.effect.as_ref().unwrap().status, EffectStatus::Confirmed as i32);
        assert!(ge.effect.unwrap().response_json.contains("wire_id"));
        // compensation
        svc.register_compensation(Request::new(RegisterCompensationRequest {
            run_id: rid.clone(), effect_key: be.idempotency_key.clone(),
            kind: "reverse_wire".into(), payload_json: "{}".into(),
            compensator_ref: "".into(), max_attempts: 0,
        })).await.unwrap();
        let obs = svc.list_obligations(Request::new(ListObligationsRequest {
            run_id: rid.clone(), only_unresolved: true, status_filter: 0,
        })).await.unwrap().into_inner().obligations;
        assert_eq!(obs.len(), 1);
        // The newly-registered obligation is PENDING, eligible immediately.
        assert_eq!(obs[0].status, ObligationStatus::Pending as i32);
        assert_eq!(obs[0].max_attempts, 5);
        // Claim → COMMITTED with lease.
        let claim = svc.claim_obligation(Request::new(ClaimObligationRequest {
            run_id: rid.clone(), obligation_seq: obs[0].seq, claimer: "test-drainer".into(),
            lease_ttl_ms: 30_000,
        })).await.unwrap().into_inner();
        assert!(claim.acquired);
        assert_eq!(claim.obligation.as_ref().unwrap().status, ObligationStatus::Committed as i32);
        assert_eq!(claim.obligation.as_ref().unwrap().claimed_by, "test-drainer");
        // A second claim by a different drainer is rejected while the lease is live.
        let claim2 = svc.claim_obligation(Request::new(ClaimObligationRequest {
            run_id: rid.clone(), obligation_seq: obs[0].seq, claimer: "other".into(),
            lease_ttl_ms: 30_000,
        })).await.unwrap().into_inner();
        assert!(!claim2.acquired);
        // Record a failed attempt → back to PENDING with attempts=1.
        let attempt = svc.record_obligation_attempt(Request::new(RecordObligationAttemptRequest {
            run_id: rid.clone(), obligation_seq: obs[0].seq, error: "network blip".into(),
            next_attempt_at_ms: now_ms() + 1_000,
        })).await.unwrap().into_inner();
        assert_eq!(attempt.status, ObligationStatus::Pending as i32);
        assert_eq!(attempt.attempts, 1);
        assert_eq!(attempt.last_error, "network blip");
        // Resolve terminal.
        let resolved = svc.resolve_obligation(Request::new(ResolveObligationRequest {
            run_id: rid.clone(), obligation_seq: obs[0].seq,
            status: ObligationStatus::Compensated as i32, result_json: "{\"ok\":true}".into(),
        })).await.unwrap().into_inner();
        assert_eq!(resolved.status, ObligationStatus::Compensated as i32);
        assert_eq!(resolved.result_json, "{\"ok\":true}");
        // budget
        svc.set_budget(Request::new(SetBudgetRequest { run_id: rid.clone(), usd_cap: 1.0, token_cap: 0 })).await.unwrap();
        assert!(svc.admit_budget(Request::new(AdmitBudgetRequest { run_id: rid.clone(), usd_estimate: 0.5, token_estimate: 0 })).await.unwrap().into_inner().admitted);
        svc.charge_budget(Request::new(ChargeBudgetRequest { run_id: rid.clone(), usd: 0.9, tokens: 0 })).await.unwrap();
        assert!(!svc.admit_budget(Request::new(AdmitBudgetRequest { run_id: rid.clone(), usd_estimate: 0.5, token_estimate: 0 })).await.unwrap().into_inner().admitted);
        // gate: send then await -> delivered
        let _ = svc.send_signal(Request::new(SendSignalRequest { run_id: rid.clone(), app_name: "".into(), user_id: "".into(), session_id: "".into(), gate_name: "g1".into(), resolution_json: "{\"ok\":true}".into() })).await.unwrap();
        let aw = svc.await_signal(Request::new(AwaitSignalRequest { run_id: rid.clone(), gate_name: "g1".into(), payload_json: "{}".into() })).await.unwrap().into_inner();
        assert!(aw.delivered);
        assert!(aw.resolution_json.contains("ok"));
        // sessions
        let sess = svc.create_session(Request::new(CreateSessionRequest { app_name: "a".into(), user_id: "u".into(), session_id: "s1".into(), state_json: "{\"k\":1}".into() })).await.unwrap().into_inner();
        assert_eq!(sess.session_id, "s1");
        let ge = svc.append_event(Request::new(AppendEventRequest {
            app_name: "a".into(), user_id: "u".into(), session_id: "s1".into(),
            event: Some(EventRecord { id: "e1".into(), invocation_id: "inv".into(), author: "user".into(), branch: "".into(), content_json: "{}".into(), actions_json: "{}".into(), timestamp_ms: 0 }),
            state_delta_json: "{\"k\":2}".into(),
        })).await.unwrap().into_inner();
        assert!(ge.event.is_some());
        let got = svc.get_session(Request::new(GetSessionRequest { app_name: "a".into(), user_id: "u".into(), session_id: "s1".into(), max_events: 0 })).await.unwrap().into_inner();
        assert!(got.found);
        assert!(got.session.as_ref().unwrap().state_json.contains("\"k\":2"));
        assert_eq!(got.session.unwrap().events.len(), 1);
        // end + re-begin
        svc.end_run(Request::new(EndRunRequest { run_id: rid.clone(), status: RunStatus::Terminal as i32, detail_json: "".into() })).await.unwrap();
        let again = svc.begin_run(Request::new(BeginRunRequest {
            app_name: "a".into(), user_id: "u".into(), session_id: "s".into(),
            invocation_id: "inv".into(), lease_owner: "t".into(), lease_ttl_ms: 60_000,
        })).await.unwrap().into_inner();
        assert!(again.resumed);
        assert_eq!(again.run_id, rid);
        assert_eq!(again.status, RunStatus::Terminal as i32);
    }
}
