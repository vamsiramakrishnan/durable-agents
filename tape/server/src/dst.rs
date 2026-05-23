//! TapeChaos — Layer 1 (deterministic simulation testing harness).
//!
//! A small DST harness that runs the server's `TapeService` over the
//! in-memory store under tokio's `current_thread` runtime + a `LocalSet` —
//! enough to give us single-threaded execution order with no thread-scheduler
//! nondeterminism. Combined with the in-memory store and the failpoint
//! catalogue, two replays of the same workload produce bit-identical
//! journals after timestamp/identifier canonicalisation.
//!
//! This is the *pragmatic* DST scope:
//!   * No swap to madsim — that would require feature-gating tokio across
//!     the whole crate (see `design-principles/chaos.md §9`). Phase 2.5.
//!   * Pure-tokio single-thread scheduling is deterministic enough for
//!     workloads that don't depend on multi-thread parallelism (which is
//!     every server-internal test).
//!   * The journal-equality oracle lives in the Python SDK
//!     (`tape.chaos.snapshot`); this module only proves the substrate
//!     supports replay.
//!
//! Real-madsim DST (true virtualised time + network) lands in Phase 2.5.

#![cfg(test)]

use std::sync::Arc;

use serde_json::Value;

use crate::pb::tape_server::Tape;
use crate::pb::*;
use crate::service::TapeService;
use crate::store::{open, RunStore};

/// One step the DST harness can drive. Adding more variants is the
/// dominant way to cover more of the matrix in `design-principles/chaos.md
/// §2`.
#[derive(Debug, Clone)]
enum Step {
    BeginRun {
        invocation: String,
    },
    RecordDecision {
        invocation: String,
        decision_index: i64,
        response: String,
    },
    BeginEffect {
        invocation: String,
        decision_index: i64,
        tool: String,
        call_index: i32,
    },
    CompleteEffect {
        invocation: String,
        decision_index: i64,
        tool: String,
        call_index: i32,
        response: String,
    },
    EndRun {
        invocation: String,
    },
}

/// Replay-capable workload definition. The harness drives one of these end
/// to end and returns the canonicalised journal for assertion.
#[derive(Default)]
struct Workload {
    steps: Vec<Step>,
}

impl Workload {
    fn add(&mut self, step: Step) -> &mut Self {
        self.steps.push(step);
        self
    }
}

/// One canonicalised journal line — kind + payload-as-string with
/// timestamps and run-scoped identifiers stripped. Equality is on this.
#[derive(Debug, PartialEq, Eq, Clone)]
struct CanonicalLine {
    kind: String,
    payload: String,
}

/// Strip the same keys `tape::chaos::snapshot` strips on the Python side
/// (keep these in lockstep).
fn canonicalise(value: &mut Value, run_id_map: &std::collections::HashMap<String, String>) {
    const STRIP: &[&str] = &[
        "ts_ms", "started_at_ms", "ended_at_ms", "last_update_time_ms",
        "lease_expires_at_ms", "claim_expires_at_ms", "dispatch_claim_expires_at_ms",
        "next_dispatch_at_ms", "next_attempt_at_ms", "fire_at_ms",
        "lease_owner", "claimed_by", "dispatch_claimed_by",
        "trace_id", "span_id", "parent_span_id",
        "seq", "global_seq", "invocation_id",
    ];
    match value {
        Value::Object(map) => {
            map.retain(|k, _| !STRIP.contains(&k.as_str()));
            for (_, v) in map.iter_mut() {
                canonicalise(v, run_id_map);
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                canonicalise(v, run_id_map);
            }
        }
        Value::String(s) => {
            for (raw, canonical) in run_id_map {
                if !raw.is_empty() && s.contains(raw.as_str()) {
                    *s = s.replace(raw.as_str(), canonical);
                }
            }
        }
        _ => {}
    }
}

async fn drive_workload(workload: &Workload) -> Vec<CanonicalLine> {
    let store: Arc<dyn RunStore> = open(":memory:").await.unwrap();
    let svc = TapeService::new(store.clone());

    // invocation -> minted run_id
    let mut runs: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    // invocation -> idempotency keys keyed by (decision_idx, tool, call_idx)
    let mut keys: std::collections::HashMap<(String, i64, String, i32), String> =
        std::collections::HashMap::new();

    for step in workload.steps.iter().cloned() {
        match step {
            Step::BeginRun { invocation } => {
                let r = svc
                    .begin_run(tonic::Request::new(BeginRunRequest {
                        app_name: "dst-app".into(),
                        user_id: "dst-user".into(),
                        session_id: "dst-session".into(),
                        invocation_id: invocation.clone(),
                        lease_owner: "dst-driver".into(),
                        lease_ttl_ms: 60_000,
                        ..Default::default()
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                runs.insert(invocation, r.run_id);
            }
            Step::RecordDecision { invocation, decision_index, response } => {
                let rid = runs[&invocation].clone();
                svc.record_decision(tonic::Request::new(RecordDecisionRequest {
                    run_id: rid,
                    decision_index,
                    model: "dst-model".into(),
                    request_json: "{}".into(),
                    response_json: response,
                    rationale: "".into(),
                    policy_version: "p1".into(),
                }))
                .await
                .unwrap();
            }
            Step::BeginEffect { invocation, decision_index, tool, call_index } => {
                let rid = runs[&invocation].clone();
                let resp = svc
                    .begin_effect(tonic::Request::new(BeginEffectRequest {
                        run_id: rid,
                        decision_index,
                        tool_name: tool.clone(),
                        call_index,
                        request_json: "{}".into(),
                        custom_key: "".into(),
                        semantics: 0,
                        dispatch_mode: 0,
                        business_key: "".into(),
                        connector: "".into(),
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                keys.insert((invocation, decision_index, tool, call_index), resp.idempotency_key);
            }
            Step::CompleteEffect { invocation, decision_index, tool, call_index, response } => {
                let rid = runs[&invocation].clone();
                let key = keys[&(invocation, decision_index, tool, call_index)].clone();
                svc.complete_effect(tonic::Request::new(CompleteEffectRequest {
                    run_id: rid,
                    idempotency_key: key,
                    status: EffectStatus::Confirmed as i32,
                    response_json: response,
                    error_json: "".into(),
                }))
                .await
                .unwrap();
            }
            Step::EndRun { invocation } => {
                let rid = runs[&invocation].clone();
                svc.end_run(tonic::Request::new(EndRunRequest {
                    run_id: rid,
                    status: RunStatus::Terminal as i32,
                    detail_json: "".into(),
                }))
                .await
                .unwrap();
            }
        }
    }

    // Build the canonical journal across every minted run.
    let mut run_id_map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for (i, (_inv, rid)) in runs.iter().enumerate() {
        run_id_map.insert(rid.clone(), format!("run-{}", i + 1));
    }

    let mut out = Vec::new();
    for (_inv, rid) in runs.iter() {
        let entries = store.journal_range(rid, 0).await.unwrap();
        for e in entries {
            let mut payload: Value = serde_json::from_str(&e.payload_json).unwrap_or(Value::Null);
            canonicalise(&mut payload, &run_id_map);
            out.push(CanonicalLine {
                kind: e.kind,
                payload: serde_json::to_string(&payload).unwrap_or_default(),
            });
        }
    }
    out
}

// ── tests ───────────────────────────────────────────────────────────────────

fn sample_workload() -> Workload {
    let mut w = Workload::default();
    w.add(Step::BeginRun { invocation: "inv-dst".into() })
        .add(Step::RecordDecision {
            invocation: "inv-dst".into(),
            decision_index: 0,
            response: r#"{"plan":"sweep"}"#.into(),
        })
        .add(Step::BeginEffect {
            invocation: "inv-dst".into(),
            decision_index: 0,
            tool: "execute_sweep".into(),
            call_index: 0,
        })
        .add(Step::CompleteEffect {
            invocation: "inv-dst".into(),
            decision_index: 0,
            tool: "execute_sweep".into(),
            call_index: 0,
            response: r#"{"wire_id":"w1"}"#.into(),
        })
        .add(Step::EndRun { invocation: "inv-dst".into() });
    w
}

/// The headline Phase-2 claim: two drives of the same workload produce
/// the same canonical journal. Run under `current_thread + LocalSet` for
/// single-threaded execution order.
#[test]
fn replay_is_bit_identical_under_current_thread() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();

    let workload = sample_workload();
    let (j1, j2) = local.block_on(&runtime, async {
        let a = drive_workload(&workload).await;
        let b = drive_workload(&workload).await;
        (a, b)
    });

    assert_eq!(j1.len(), j2.len(),
        "two drives produced journals of different length: {} vs {}", j1.len(), j2.len());
    for (i, (a, b)) in j1.iter().zip(j2.iter()).enumerate() {
        assert_eq!(a, b,
            "drift at canonical journal line {i}:\n  A: {a:?}\n  B: {b:?}");
    }
}

/// Negative case — if the second drive flips one decision response, the
/// journals must diverge. Proves the equality check is not vacuous.
#[test]
fn replay_detects_an_inserted_drift() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();

    let mut w1 = sample_workload();
    let mut w2 = sample_workload();
    // Flip the tool name in the second drive's BeginEffect + CompleteEffect.
    // The journal records `tool` and `idempotency_key` (which contains the
    // tool name), so this drift must surface in the canonical journals. We
    // can't drift `response_json` here because the journal stores only a
    // summary of decisions / effects — the full body lives in the
    // `tape_decisions` / `tape_effects` projections (see store/sql.rs); the
    // canonical-equality check is consequently on journal *summaries*. See
    // `tape.chaos.snapshot` for the corresponding limitation note.
    for s in w2.steps.iter_mut() {
        match s {
            Step::BeginEffect { tool, .. } => *tool = "execute_hedge".into(),
            Step::CompleteEffect { tool, .. } => *tool = "execute_hedge".into(),
            _ => {}
        }
    }

    let (j1, j2) = local.block_on(&runtime, async {
        let a = drive_workload(&w1).await;
        // Cheap shut-up for the "unused mut" lint when steps don't change.
        let _ = &mut w1;
        let b = drive_workload(&w2).await;
        (a, b)
    });

    assert!(j1 != j2,
        "an injected drift should produce different canonical journals\nj1: {j1:#?}\nj2: {j2:#?}");
}
