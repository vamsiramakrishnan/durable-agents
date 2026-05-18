//! TapeChaos — Phase 2.5: Madsim DST.
//!
//! Wires Madsim — the deterministic simulator the [chaos treatise §9]
//! calls out — as a dev-dep, and exercises three orthogonal properties
//! the single-thread DST harness (`src/dst.rs`) can't deliver:
//!
//!   1. **Virtualised time.** A one-hour `madsim::time::sleep` finishes
//!      under `cfg(madsim)` in milliseconds of wall clock — the
//!      foundational FoundationDB-style DST property.
//!   2. **Deterministic scheduling.** Two concurrent tasks racing for
//!      one slot — the simulator picks the same winner on every replay
//!      with the same seed.
//!   3. **Seeded RNG.** `madsim::rand` under `cfg(madsim)` produces a
//!      reproducible sequence per seed; the catch-bug-then-replay loop
//!      depends on this.
//!
//! Run mode:
//!
//! ```bash
//! # Passthrough (no virtualised time, real tokio): every test runs.
//! cargo test --features sim
//!
//! # Simulator (cfg(madsim) — virtualised time + deterministic scheduling):
//! RUSTFLAGS='--cfg madsim' cargo test --features sim
//! ```
//!
//! Both modes pass. Under passthrough, the tests are still useful — they
//! exercise the same code paths against real tokio. The simulator mode
//! delivers the extra properties (virtualised time, reproducibility).
//!
//! ## Architectural constraint, documented honestly
//!
//! Tape's production store
//! ([`SqliteBackend::with()`](crate::store::sql) — see `src/store/sql.rs`)
//! calls `tokio::task::spawn_blocking` to ferry the synchronous `rusqlite`
//! work off the async runtime. Under `cfg(madsim)`, madsim refuses real
//! OS threads (that would defeat determinism), so anything that hits the
//! store path panics. Two ways forward, both Phase 2.6:
//!
//!   * **A sim-only `MemRunStore`** — a pure-async in-memory `RunStore`
//!     impl with no `spawn_blocking`. ~40 trait methods, but only the
//!     ones the sim tests touch need real bodies; the rest can return
//!     `StoreError::msg("sim store: not implemented")`. Tighter scope,
//!     cleaner code.
//!   * **The `madsim-tokio` shim** — swap the `tokio` package
//!     dependency for `madsim-tokio` under `cfg(madsim)`. The shim
//!     intercepts `tokio::task::spawn_blocking` and routes it through
//!     the simulator. Broader scope (touches every dep using tokio), but
//!     gives multi-node simulation over a virtualised tonic.
//!
//! This module is the foundation that proves the substrate is
//! madsim-capable; the store-bridging work follows in 2.6.
//!
//! [chaos treatise §9]: ../../design-principles/chaos.md

#![cfg(all(test, feature = "sim"))]

use std::sync::Arc;
use std::time::Duration;

// ── Test 1: virtualised time under cfg(madsim) ──────────────────────────────

/// One simulated hour completes under `cfg(madsim)` instantly in wall
/// clock — the headline DST property. Under passthrough mode (no
/// `--cfg madsim`) this test would take an hour, so it's gated.
#[cfg(madsim)]
#[madsim::test]
async fn one_hour_sleep_is_free_under_virtualised_time() {
    let t0 = madsim::time::Instant::now();
    madsim::time::sleep(Duration::from_secs(3600)).await;
    let elapsed = t0.elapsed();
    assert!(
        elapsed >= Duration::from_secs(3600),
        "simulated clock should have advanced one hour; got {:?}",
        elapsed
    );
}

// ── Test 2: deterministic scheduling of concurrent tasks ────────────────────

/// Two tasks contending for one slot, joined via `tokio::join!`. Under
/// passthrough this is non-deterministic (real tokio interleaves
/// however it likes); under `cfg(madsim)` the simulator picks the same
/// winner on every replay with the same seed. The test asserts only the
/// CAS invariant (exactly one wins), which holds in both modes — the
/// determinism is a property of *the choice*, not of the count.
#[madsim::test]
async fn concurrent_cas_exactly_one_wins() {
    // Pure-protocol CAS: a single Arc<Mutex<bool>> stands in for the
    // store's CAS lease. Demonstrates the racing pattern without
    // depending on the production store (which can't run under
    // cfg(madsim); see the module docstring).
    let lease = Arc::new(std::sync::Mutex::new(false));

    let claim = |actor: &'static str| {
        let lease = lease.clone();
        async move {
            // Yield once to let the scheduler interleave us.
            madsim::time::sleep(Duration::from_millis(1)).await;
            let mut taken = lease.lock().unwrap();
            if *taken {
                return (actor, false);
            }
            *taken = true;
            (actor, true)
        }
    };

    let (a, b) = tokio::join!(claim("reactor-A"), claim("reactor-B"));
    let wins = (a.1 as u8) + (b.1 as u8);
    assert_eq!(
        wins, 1,
        "exactly one CAS must win; got A={:?}, B={:?}",
        a, b
    );
}

// ── Test 3: seeded RNG is reproducible under cfg(madsim) ────────────────────

/// `madsim::rand` produces the same sequence on every replay with the
/// same seed — the property the LDFI + replay loop relies on. Under
/// passthrough this collapses to `tokio::*`-flavoured non-determinism;
/// under `cfg(madsim)` the simulator seeds its own RNG from
/// `MADSIM_TEST_SEED` and reproduces bit-for-bit.
#[madsim::test]
async fn seeded_random_is_deterministic_per_run() {
    use madsim::rand::{thread_rng, Rng};

    let mut rng = thread_rng();
    let sequence: Vec<u64> = (0..8).map(|_| rng.gen()).collect();

    // Under `cfg(madsim)`, re-running this test with the same
    // `MADSIM_TEST_SEED` env var produces the same `sequence`. We don't
    // assert that here (we'd need a separate process); the test exists
    // to exercise the API and document the property.
    assert_eq!(sequence.len(), 8);
    // sanity: not all the same value
    assert!(sequence.iter().any(|&x| x != sequence[0]));
}

// ── Test 4: store-based — TapeService over MemRunStore (both modes) ─────────
//
// Phase 2.6 closes the gap: instead of the SQL store (which uses
// `tokio::task::spawn_blocking` and so panics under `cfg(madsim)`), we
// use `MemRunStore` — pure-async, no thread spawns. The same test runs
// in both passthrough and simulator modes.

#[madsim::test]
async fn store_based_lease_expiry() {
    use crate::pb::tape_server::Tape;
    use crate::pb::*;
    use crate::service::TapeService;
    use crate::store::mem::MemRunStore;

    let store = std::sync::Arc::new(MemRunStore::new());
    let svc = TapeService::new(store);

    // Begin a run with a 500ms lease.
    let r = svc
        .begin_run(tonic::Request::new(BeginRunRequest {
            app_name: "sim".into(),
            user_id: "u".into(),
            session_id: "s".into(),
            invocation_id: "inv-pass".into(),
            lease_owner: "driver-A".into(),
            lease_ttl_ms: 500,
        }))
        .await
        .unwrap()
        .into_inner();
    let run_id = r.run_id;

    // Advance the store's notion of `now` past the lease TTL. Tape's
    // explicit `now_ms` parameter on time-sensitive RPCs is what gives
    // us this knob — under cfg(madsim) we could also advance
    // `madsim::time::sleep`, but the parameter is what the store
    // actually reads, so this works in both modes.
    let now_ms = crate::store::now_ms() + 600;
    let late = svc
        .list_runs_to_recover(tonic::Request::new(ListRunsToRecoverRequest {
            limit: 10,
            now_ms,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        late.runs.iter().any(|r| r.run_id == run_id),
        "after the lease expired, the run must be in the recovery list"
    );
}

// ── Test 5: real CAS race against TapeService under madsim's scheduler ──────
//
// Two simulated reactors call `claim_obligation` on the same row,
// concurrently, through the real `TapeService`. The store's CAS lease
// guarantees exactly one wins. Under cfg(madsim) the simulator's
// deterministic scheduling makes the *winner* reproducible per seed —
// the property classic flaky-test triage depends on.

#[madsim::test]
async fn real_cas_against_tape_service_exactly_one_wins() {
    use crate::pb::tape_server::Tape;
    use crate::pb::*;
    use crate::service::TapeService;
    use crate::store::mem::MemRunStore;

    let store = std::sync::Arc::new(MemRunStore::new());
    let svc = std::sync::Arc::new(TapeService::new(store));

    // Setup: run → decision → effect → obligation.
    let r = svc.begin_run(tonic::Request::new(BeginRunRequest {
        app_name: "sim".into(), user_id: "u".into(),
        session_id: "s".into(), invocation_id: "inv-cas".into(),
        lease_owner: "driver".into(), lease_ttl_ms: 60_000,
    })).await.unwrap().into_inner();
    let run_id = r.run_id;

    svc.record_decision(tonic::Request::new(RecordDecisionRequest {
        run_id: run_id.clone(), decision_index: 0, model: "m".into(),
        request_json: "{}".into(), response_json: "{}".into(),
        rationale: "".into(), policy_version: "".into(),
    })).await.unwrap();
    let be = svc.begin_effect(tonic::Request::new(BeginEffectRequest {
        run_id: run_id.clone(), decision_index: 0, tool_name: "wire".into(),
        call_index: 0, request_json: "{}".into(), custom_key: "".into(),
        semantics: 0, dispatch_mode: 0, business_key: "".into(), connector: "".into(),
    })).await.unwrap().into_inner();
    svc.complete_effect(tonic::Request::new(CompleteEffectRequest {
        run_id: run_id.clone(), idempotency_key: be.idempotency_key.clone(),
        status: EffectStatus::Confirmed as i32,
        response_json: "{}".into(), error_json: "".into(),
    })).await.unwrap();
    svc.register_compensation(tonic::Request::new(RegisterCompensationRequest {
        run_id: run_id.clone(), effect_key: be.idempotency_key.clone(),
        kind: "reverse_wire".into(), payload_json: "{}".into(),
        compensator_ref: "".into(), max_attempts: 0,
    })).await.unwrap();
    let obs = svc.list_obligations(tonic::Request::new(ListObligationsRequest {
        run_id: run_id.clone(), only_unresolved: true, status_filter: 0,
    })).await.unwrap().into_inner().obligations;
    let ob_seq = obs[0].seq;

    // Two reactors race for the lease — exactly one must win.
    let claim = |claimer: &'static str| {
        let svc = svc.clone();
        let rid = run_id.clone();
        async move {
            svc.claim_obligation(tonic::Request::new(ClaimObligationRequest {
                run_id: rid, obligation_seq: ob_seq,
                claimer: claimer.into(), lease_ttl_ms: 60_000,
            })).await.unwrap().into_inner()
        }
    };
    let (a, b) = tokio::join!(claim("reactor-A"), claim("reactor-B"));
    let wins = (a.acquired as u8) + (b.acquired as u8);
    assert_eq!(wins, 1,
        "exactly one CAS must win; got acquired=({}, {})", a.acquired, b.acquired);
}

// ── Test 6: Phase 3.5 — Porcupine-style linearizability of the lease ────────
//
// "Exactly one wins" is necessary but not sufficient — the formal
// Jepsen-style invariant is *linearizability*: there exists a total
// order of the concurrent ops consistent with real-time happens-before
// and with the sequential lease model. We record a history of
// concurrent claim_obligation calls from N reactors, then feed it to
// the Wing-Gong checker in `lin.rs`. Under cfg(madsim) the simulator's
// determinism makes the witness reproducible per seed; under
// passthrough the property still holds (linearizability is a real-time
// invariant, not a determinism one), the checker just sees different
// scheduling.

#[madsim::test]
async fn lease_history_is_linearizable() {
    use crate::lin::{check, Event, LeaseModel, LeaseOp, LeaseRet};
    use crate::pb::tape_server::Tape;
    use crate::pb::*;
    use crate::service::TapeService;
    use crate::store::mem::MemRunStore;
    use std::time::Instant;

    let store = std::sync::Arc::new(MemRunStore::new());
    let svc = std::sync::Arc::new(TapeService::new(store));

    // Setup: run → decision → effect → obligation (the row everyone races for).
    svc.begin_run(tonic::Request::new(BeginRunRequest {
        app_name: "sim".into(), user_id: "u".into(),
        session_id: "s".into(), invocation_id: "inv-lin".into(),
        lease_owner: "driver".into(), lease_ttl_ms: 60_000,
    })).await.unwrap();
    let run_id_setup = svc.list_runs_to_recover(tonic::Request::new(
        ListRunsToRecoverRequest { limit: 1, now_ms: crate::store::now_ms() + 1_000_000 }
    )).await.unwrap().into_inner().runs[0].run_id.clone();
    svc.record_decision(tonic::Request::new(RecordDecisionRequest {
        run_id: run_id_setup.clone(), decision_index: 0, model: "m".into(),
        request_json: "{}".into(), response_json: "{}".into(),
        rationale: "".into(), policy_version: "".into(),
    })).await.unwrap();
    let be = svc.begin_effect(tonic::Request::new(BeginEffectRequest {
        run_id: run_id_setup.clone(), decision_index: 0, tool_name: "wire".into(),
        call_index: 0, request_json: "{}".into(), custom_key: "".into(),
        semantics: 0, dispatch_mode: 0, business_key: "".into(), connector: "".into(),
    })).await.unwrap().into_inner();
    svc.complete_effect(tonic::Request::new(CompleteEffectRequest {
        run_id: run_id_setup.clone(), idempotency_key: be.idempotency_key.clone(),
        status: EffectStatus::Confirmed as i32,
        response_json: "{}".into(), error_json: "".into(),
    })).await.unwrap();
    svc.register_compensation(tonic::Request::new(RegisterCompensationRequest {
        run_id: run_id_setup.clone(), effect_key: be.idempotency_key.clone(),
        kind: "reverse_wire".into(), payload_json: "{}".into(),
        compensator_ref: "".into(), max_attempts: 0,
    })).await.unwrap();
    let obs = svc.list_obligations(tonic::Request::new(ListObligationsRequest {
        run_id: run_id_setup.clone(), only_unresolved: true, status_filter: 0,
    })).await.unwrap().into_inner().obligations;
    let ob_seq = obs[0].seq;

    // Three reactors race; we record (start_ns, end_ns, op, ret) per call.
    let t0 = Instant::now();
    let claim = |claimer: &'static str, ttl_ms: i64| {
        let svc = svc.clone();
        let run_id = run_id_setup.clone();
        async move {
            let start = t0.elapsed().as_nanos() as u64;
            let now_ms = crate::store::now_ms();
            let resp = svc.claim_obligation(tonic::Request::new(ClaimObligationRequest {
                run_id, obligation_seq: ob_seq,
                claimer: claimer.into(), lease_ttl_ms: ttl_ms,
            })).await.unwrap().into_inner();
            let end = t0.elapsed().as_nanos() as u64;
            Event {
                process: claimer.into(),
                op: LeaseOp::Claim {
                    claimer: claimer.into(),
                    ttl_ns: (ttl_ms as u64) * 1_000_000,
                    now_ns: (now_ms as u64) * 1_000_000,
                },
                ret: LeaseRet::Claimed {
                    acquired: resp.acquired,
                    holder: if resp.acquired {
                        claimer.into()
                    } else {
                        resp.obligation
                            .as_ref()
                            .map(|o| o.claimed_by.clone())
                            .unwrap_or_default()
                    },
                },
                start_ns: start,
                end_ns: end,
            }
        }
    };

    let (a, b, c) = tokio::join!(
        claim("reactor-A", 60_000),
        claim("reactor-B", 60_000),
        claim("reactor-C", 60_000),
    );
    let history = vec![a, b, c];

    // The Wing-Gong checker must accept this history — i.e., a total
    // order respecting real-time happens-before and the sequential
    // lease model exists. If it doesn't, the production CAS is buggy
    // in a way the "exactly one wins" check can't see.
    if let Err(witness) = check(&LeaseModel, &history) {
        panic!(
            "lease history NOT linearizable; deepest prefix the search reached: {} of {} ops:\n{:#?}",
            witness.deepest_prefix.len(),
            history.len(),
            witness.deepest_prefix
        );
    }
}
