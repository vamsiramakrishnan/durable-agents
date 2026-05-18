//! Wing-Gong linearizability checker — Porcupine-style, in-tree.
//!
//! Phase 3.5 of TapeChaos. Given a history of concurrent operations,
//! decide whether there exists a total order (a *linearization*) such
//! that:
//!
//!  1. If operation `A` completed strictly before operation `B` began,
//!     `A` precedes `B` in the linearization (respects real-time
//!     ordering — the "happens-before" partial order).
//!  2. Applying the ops in the linearization to the sequential model
//!     produces the responses the clients actually observed.
//!
//! The algorithm is Wing & Gong's exhaustive search with memoization on
//! `(set of un-linearized ops, model state)`. It's exponential in the
//! worst case but milliseconds for the ~10-op histories sim tests
//! generate. Porcupine (the Go library) adds partition-tolerance,
//! linearization-point visualisation, and a faster bitset rep — none
//! of which we need here. The algorithm and its termination invariants
//! are the same.
//!
//! Not a production tool: this lives behind `#[cfg(test)]` because its
//! only consumer is the lease-linearizability test. Move it to a
//! `pub`-facing module if other invariants want checking.

#![cfg(test)]

use std::collections::HashSet;
use std::hash::Hash;

/// One observed operation from a client thread.
///
/// `start_ns` / `end_ns` are the wall-clock instants at which the client
/// *issued* and *received* the operation — under cfg(madsim), simulator
/// virtual time. The checker uses them only as a partial order: which
/// pairs of ops are concurrent (intervals overlap) and which are
/// strictly ordered (one's end < the other's start).
#[derive(Clone, Debug)]
pub struct Event<Op, Ret> {
    pub process: String,
    pub op: Op,
    pub ret: Ret,
    pub start_ns: u64,
    pub end_ns: u64,
}

/// Sequential model: `apply(state, op) -> (next_state, expected_ret)`.
pub trait Model {
    type State: Clone + Eq + Hash;
    type Op: Clone;
    type Ret: Eq + Clone;

    fn initial(&self) -> Self::State;
    fn apply(&self, state: &Self::State, op: &Self::Op) -> (Self::State, Self::Ret);
}

/// Check whether `history` is linearizable against `model`.
///
/// Returns `Ok(())` on success or `Err(witness)` with a partial
/// linearization showing the deepest point the search reached — useful
/// for debugging which prefix is OK and where the constraint failed.
pub fn check<M: Model>(
    model: &M,
    history: &[Event<M::Op, M::Ret>],
) -> Result<(), Witness<M::Op, M::Ret>> {
    let n = history.len();
    let mut pending: Vec<usize> = (0..n).collect();
    let mut linearized: Vec<usize> = Vec::with_capacity(n);
    let mut visited: HashSet<(Vec<usize>, M::State)> = HashSet::new();
    let init = model.initial();
    if recurse(model, history, &mut pending, &mut linearized, &init, &mut visited) {
        Ok(())
    } else {
        Err(Witness {
            deepest_prefix: linearized.iter()
                .map(|&i| history[i].clone())
                .collect(),
        })
    }
}

/// Returned on failure — the longest prefix the search managed to
/// linearize before getting stuck. Lets the test print "got this far,
/// then operation X with response Y was inconsistent".
#[derive(Debug)]
pub struct Witness<Op, Ret> {
    pub deepest_prefix: Vec<Event<Op, Ret>>,
}

fn recurse<M: Model>(
    model: &M,
    history: &[Event<M::Op, M::Ret>],
    pending: &mut Vec<usize>,
    linearized: &mut Vec<usize>,
    state: &M::State,
    visited: &mut HashSet<(Vec<usize>, M::State)>,
) -> bool {
    if pending.is_empty() {
        return true;
    }

    // Memo key: (sorted pending set, state). If we've been here before
    // and failed, no point retrying.
    let mut key_pending = pending.clone();
    key_pending.sort_unstable();
    let key = (key_pending, state.clone());
    if visited.contains(&key) {
        return false;
    }

    // "Minimal" pending op = one whose start_ns is <= the earliest
    // end_ns among pending. Equivalent: no pending op completed before
    // this one began, so this op is free to be placed next without
    // violating real-time order.
    let earliest_end = pending.iter().map(|&i| history[i].end_ns).min().unwrap();
    let candidates: Vec<usize> = pending.iter().copied()
        .filter(|&i| history[i].start_ns <= earliest_end)
        .collect();

    for cand in candidates {
        let (next_state, expected_ret) = model.apply(state, &history[cand].op);
        if expected_ret == history[cand].ret {
            let pos = pending.iter().position(|&x| x == cand).unwrap();
            pending.remove(pos);
            linearized.push(cand);
            if recurse(model, history, pending, linearized, &next_state, visited) {
                return true;
            }
            linearized.pop();
            pending.insert(pos, cand);
        }
    }

    visited.insert(key);
    false
}

// ── Lease model ─────────────────────────────────────────────────────────────

/// Sequential semantics of the obligation lease.
///
/// State: `Option<(owner, expires_at_ns)>`. Operations:
///
///   * `Claim { claimer, ttl_ns, now_ns }` — succeeds iff no live lease;
///     returns `(acquired, current_holder_if_known)`.
///   * `Release` — clears the lease (used to model `resolve_obligation`).
///
/// This is the model `claim_obligation` and friends *should* satisfy.
/// Linearizability against this model is the formal Jepsen-style
/// invariant; the test in `chaos::tests` exercises it.
#[derive(Clone, Debug)]
pub enum LeaseOp {
    Claim { claimer: String, ttl_ns: u64, now_ns: u64 },
    Release,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseRet {
    /// `(acquired, observed_holder)`. `observed_holder` is `""` when
    /// the lease was free and the call acquired it (matching the
    /// server's response shape for a fresh claim).
    Claimed { acquired: bool, holder: String },
    Released,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LeaseState {
    /// `Some((owner, expires_at_ns))` while a lease is live.
    pub lease: Option<(String, u64)>,
}

pub struct LeaseModel;

impl Model for LeaseModel {
    type State = LeaseState;
    type Op = LeaseOp;
    type Ret = LeaseRet;

    fn initial(&self) -> LeaseState { LeaseState { lease: None } }

    fn apply(&self, state: &LeaseState, op: &LeaseOp) -> (LeaseState, LeaseRet) {
        match op {
            LeaseOp::Claim { claimer, ttl_ns, now_ns } => {
                let live = state.lease.as_ref().filter(|(_, exp)| *exp > *now_ns);
                if let Some((holder, _)) = live {
                    (state.clone(), LeaseRet::Claimed {
                        acquired: false, holder: holder.clone(),
                    })
                } else {
                    let new_state = LeaseState {
                        lease: Some((claimer.clone(), now_ns + ttl_ns)),
                    };
                    (new_state, LeaseRet::Claimed {
                        acquired: true, holder: claimer.clone(),
                    })
                }
            }
            LeaseOp::Release => (LeaseState { lease: None }, LeaseRet::Released),
        }
    }
}

// ── Unit tests for the checker itself ───────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(p: &str, op: LeaseOp, ret: LeaseRet, s: u64, e: u64) -> Event<LeaseOp, LeaseRet> {
        Event { process: p.into(), op, ret, start_ns: s, end_ns: e }
    }

    /// Sequential history: A claims, A releases, B claims. Linearizable.
    #[test]
    fn sequential_claims_are_linearizable() {
        let h = vec![
            ev("A", LeaseOp::Claim { claimer: "A".into(), ttl_ns: 100, now_ns: 0 },
               LeaseRet::Claimed { acquired: true, holder: "A".into() }, 0, 10),
            ev("A", LeaseOp::Release, LeaseRet::Released, 20, 30),
            ev("B", LeaseOp::Claim { claimer: "B".into(), ttl_ns: 100, now_ns: 40 },
               LeaseRet::Claimed { acquired: true, holder: "B".into() }, 40, 50),
        ];
        assert!(check(&LeaseModel, &h).is_ok());
    }

    /// Concurrent claims, exactly one wins. Linearizable (the search
    /// finds the order [A wins, B loses]).
    #[test]
    fn concurrent_claims_exactly_one_wins_is_linearizable() {
        let h = vec![
            ev("A", LeaseOp::Claim { claimer: "A".into(), ttl_ns: 1000, now_ns: 0 },
               LeaseRet::Claimed { acquired: true, holder: "A".into() }, 0, 20),
            ev("B", LeaseOp::Claim { claimer: "B".into(), ttl_ns: 1000, now_ns: 0 },
               LeaseRet::Claimed { acquired: false, holder: "A".into() }, 5, 25),
        ];
        assert!(check(&LeaseModel, &h).is_ok());
    }

    /// Two clients see themselves acquire concurrently — violates
    /// mutual exclusion, no linearization exists.
    #[test]
    fn two_winners_is_not_linearizable() {
        let h = vec![
            ev("A", LeaseOp::Claim { claimer: "A".into(), ttl_ns: 1000, now_ns: 0 },
               LeaseRet::Claimed { acquired: true, holder: "A".into() }, 0, 20),
            ev("B", LeaseOp::Claim { claimer: "B".into(), ttl_ns: 1000, now_ns: 0 },
               LeaseRet::Claimed { acquired: true, holder: "B".into() }, 5, 25),
        ];
        assert!(check(&LeaseModel, &h).is_err());
    }

    /// Lease expiry: A claims with short TTL, B claims after expiry.
    /// Linearizable.
    #[test]
    fn lease_expiry_allows_reclaim() {
        let h = vec![
            ev("A", LeaseOp::Claim { claimer: "A".into(), ttl_ns: 10, now_ns: 0 },
               LeaseRet::Claimed { acquired: true, holder: "A".into() }, 0, 5),
            ev("B", LeaseOp::Claim { claimer: "B".into(), ttl_ns: 100, now_ns: 50 },
               LeaseRet::Claimed { acquired: true, holder: "B".into() }, 50, 60),
        ];
        assert!(check(&LeaseModel, &h).is_ok());
    }
}
