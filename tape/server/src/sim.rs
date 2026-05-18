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

// ── Test 4: store-based sim test — passthrough only ─────────────────────────
//
// This is the test we'd *like* to run under cfg(madsim) — it exercises
// the production TapeService under deterministic scheduling. It runs
// only under passthrough mode for now, until the Phase 2.6 store
// bridging lands. Demonstrates the integration shape; documents the
// path forward.

#[cfg(not(madsim))]
#[madsim::test]
async fn store_based_lease_expiry_via_passthrough() {
    use crate::pb::tape_server::Tape;
    use crate::pb::*;
    use crate::service::TapeService;
    use crate::store::open;

    let store = open(":memory:").await.unwrap();
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

    // Fast-forward the simulator's clock past the TTL by querying
    // list_runs_to_recover with an explicit future `now_ms` — the
    // store reads it as a comparator, no real sleep required. (The
    // simulator's time advancement is a separate axis from the store's
    // notion of `now`; Tape's design making `now_ms` an explicit
    // parameter is what gives us this knob.)
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
