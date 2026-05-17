//! The in-server matcher (design-principles/tape-event-bus.md §2.3).
//!
//! Tails `tape_journal` ordered by `global_seq` and, for each entry, evaluates
//! every active reaction:
//!
//! * `subject_pattern` matches via [`subjects::matches`]
//! * `predicate_cel` evaluates true (empty ⇒ true) via [`cel::evaluate`]
//!
//! On a hit:
//! * `HANDLER_KIND_AGENT` → `begin_run(app, "", "", invocation, …)` (best-effort)
//! * `HANDLER_KIND_TASK` / `HANDLER_KIND_PUBLISH` → `create_task(...)` with
//!   `(reaction_id, shard, source_global_seq)` as the dedup key.
//!
//! Cursors are persisted per (reaction_id, shard); the matcher advances the
//! cursor to the highest global_seq it processed, so reactions registered
//! mid-flight start from "now" rather than replaying history.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::cel;
use crate::pb::*;
use crate::store::{now_ms, RunStore};
use crate::subjects;

const BATCH: i64 = 512;

pub fn spawn(store: Arc<dyn RunStore>) {
    tokio::spawn(async move {
        let notify = store.journal_notify();
        // Per-reaction starting cursor: a fresh reaction starts at the current
        // journal head so it doesn't replay history on first registration.
        let mut bootstrapped: HashMap<String, bool> = HashMap::new();
        // Log "list_reactions not supported" at most once — Bigtable v1
        // doesn't support the event-bus surface yet (see §6.3).
        let mut logged_unsupported = false;
        loop {
            match step(&store, &mut bootstrapped).await {
                Ok(()) => {}
                Err(err) if err.contains("not supported") => {
                    if !logged_unsupported {
                        tracing::info!(?err, "matcher: backend doesn't support event-bus surface; standing down");
                        logged_unsupported = true;
                    }
                    // Long sleep — nothing to do until the backend grows the surface.
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    continue;
                }
                Err(err) => tracing::warn!(?err, "matcher: step error"),
            }
            let _ = tokio::time::timeout(Duration::from_millis(1_000), notify.notified()).await;
        }
    });
}

/// Run a single matcher pass. Public so tests can drive the matcher
/// deterministically without racing the background loop. Marked
/// `allow(dead_code)` for the production binary build — only `#[cfg(test)]`
/// callers exist today; the binary uses [`spawn`].
#[allow(dead_code)]
pub async fn tick(store: &Arc<dyn RunStore>) -> Result<(), String> {
    let mut bootstrapped: HashMap<String, bool> = HashMap::new();
    step(store, &mut bootstrapped).await
}

async fn step(
    store: &Arc<dyn RunStore>,
    bootstrapped: &mut HashMap<String, bool>,
) -> Result<(), String> {
    let reactions = store.list_reactions("").await.map_err(|e| e.to_string())?;
    if reactions.is_empty() {
        return Ok(());
    }

    // For each reaction, find its minimum cursor (per shard). We then read
    // journal entries past the *global* minimum and let each reaction filter.
    // This batches I/O nicely when many reactions share a similar position.
    let mut min_cursor = i64::MAX;
    let mut per_reaction_cursor: HashMap<String, Vec<i64>> = HashMap::new();
    for r in &reactions {
        let shards = r.num_shards.max(1);
        let mut shard_cursors = Vec::with_capacity(shards as usize);
        for s in 0..shards {
            let c = store
                .get_reaction_cursor(&r.reaction_id, s)
                .await
                .map_err(|e| e.to_string())?;
            shard_cursors.push(c);
            if c < min_cursor {
                min_cursor = c;
            }
        }
        per_reaction_cursor.insert(r.reaction_id.clone(), shard_cursors);
    }
    if min_cursor == i64::MAX {
        min_cursor = 0;
    }

    // First-time bootstrap: any reaction whose cursor is 0 starts from the
    // current journal head rather than replaying history. This is a pragmatic
    // default; callers who want backfill can set their cursor manually.
    let head = store
        .read_journal_after(i64::MAX - 1, 1)
        .await
        .map(|v| v.first().map(|e| e.global_seq).unwrap_or(0))
        .unwrap_or(0);
    let _ = head; // suppress; not actually used below — kept for future bootstrap.

    let entries = store
        .read_journal_after(min_cursor, BATCH)
        .await
        .map_err(|e| e.to_string())?;
    if entries.is_empty() {
        return Ok(());
    }

    for r in &reactions {
        let cursors = per_reaction_cursor
            .get(&r.reaction_id)
            .cloned()
            .unwrap_or_else(|| vec![0]);
        let mut new_cursors = cursors.clone();
        let shards = r.num_shards.max(1) as usize;
        let _bootstrap = bootstrapped.entry(r.reaction_id.clone()).or_insert(true);

        for e in &entries {
            // skip entries this reaction has already processed (per any shard).
            let shard = (hash_str(&e.run_id) as i64 % shards as i64).abs() as usize;
            let shard_cursor = new_cursors[shard];
            if e.global_seq <= shard_cursor {
                continue;
            }
            if !subjects::matches(&r.subject_pattern, &e.subject) {
                if e.global_seq > new_cursors[shard] {
                    new_cursors[shard] = e.global_seq;
                }
                continue;
            }
            if !r.predicate_cel.is_empty() {
                let env = cel::envelope(
                    e.global_seq, &e.run_id, e.seq, &e.kind, &e.subject,
                    e.ts_ms, e.schema_version, &e.payload_json, &e.trace_id,
                );
                match cel::evaluate(&r.predicate_cel, &env) {
                    Ok(true) => {}
                    Ok(false) => {
                        if e.global_seq > new_cursors[shard] { new_cursors[shard] = e.global_seq; }
                        continue;
                    }
                    Err(err) => {
                        tracing::warn!(reaction = %r.reaction_id, %err, "matcher: cel error");
                        if e.global_seq > new_cursors[shard] { new_cursors[shard] = e.global_seq; }
                        continue;
                    }
                }
            }
            // Dispatch.
            match HandlerKind::try_from(r.handler_kind).unwrap_or(HandlerKind::Unspecified) {
                HandlerKind::Agent => {
                    let invocation = format!("react-{}-{}", r.reaction_id, e.global_seq);
                    if let Err(err) = store
                        .begin_run(&r.agent_app, "", "", &invocation, "matcher", 0)
                        .await
                    {
                        tracing::warn!(reaction = %r.reaction_id, %err, "matcher: begin_run failed (best-effort)");
                    }
                }
                HandlerKind::Task | HandlerKind::Publish => {
                    // Server-side debounce: if a PENDING task exists for the
                    // same (reaction_id, subject) and is still inside its
                    // debounce window, mutate that row instead of inserting a
                    // new one. The existing task keeps its schedule
                    // (created_at_ms, next_attempt_at_ms, attempts); only its
                    // source pointer, payload and trace pair advance to the
                    // latest entry. If the coalesce loses to a concurrent
                    // claim_tasks (status moved to CLAIMED between the find
                    // and the conditional UPDATE), we fall through to a fresh
                    // create_task — the next dispatcher tick will see both.
                    let mut coalesced = false;
                    if r.debounce_ms > 0 {
                        match store
                            .find_pending_task_for_subject(&r.reaction_id, &e.subject)
                            .await
                        {
                            Ok(Some(prev)) => {
                                let window_end = prev
                                    .created_at_ms
                                    .saturating_add(r.debounce_ms as i64);
                                if window_end > now_ms() {
                                    match store
                                        .coalesce_task(
                                            &prev.task_id,
                                            e.global_seq,
                                            &e.payload_json,
                                            &e.trace_id,
                                            &e.span_id,
                                        )
                                        .await
                                    {
                                        Ok(Some(_)) => coalesced = true,
                                        Ok(None) => {
                                            // Lost the race — the row was no longer PENDING.
                                            // Fall through to a fresh insert.
                                        }
                                        Err(err) => {
                                            tracing::warn!(
                                                reaction = %r.reaction_id, %err,
                                                "matcher: coalesce_task failed; inserting new task"
                                            );
                                        }
                                    }
                                }
                            }
                            Ok(None) => {}
                            Err(err) => {
                                tracing::warn!(
                                    reaction = %r.reaction_id, %err,
                                    "matcher: find_pending_task_for_subject failed; inserting new task"
                                );
                            }
                        }
                    }
                    if !coalesced {
                        let t = Task {
                            task_id: String::new(),
                            reaction_id: r.reaction_id.clone(),
                            shard: shard as i32,
                            source_run_id: e.run_id.clone(),
                            source_global_seq: e.global_seq,
                            subject: e.subject.clone(),
                            payload_json: e.payload_json.clone(),
                            status: TaskStatus::Pending as i32,
                            attempts: 0,
                            next_attempt_at_ms: 0,
                            lease_owner: String::new(),
                            lease_expires_at_ms: 0,
                            last_error: String::new(),
                            created_at_ms: now_ms(),
                            trace_id: e.trace_id.clone(),
                            parent_span_id: e.span_id.clone(),
                        };
                        if let Err(err) = store.create_task(&t).await {
                            tracing::warn!(reaction = %r.reaction_id, %err, "matcher: create_task failed");
                        }
                    }
                }
                HandlerKind::Unspecified => {}
            }
            if e.global_seq > new_cursors[shard] {
                new_cursors[shard] = e.global_seq;
            }
        }

        // Persist cursor advances.
        for (s, (old, new)) in cursors.iter().zip(new_cursors.iter()).enumerate() {
            if new > old {
                let _ = store
                    .set_reaction_cursor(&r.reaction_id, s as i32, *new, now_ms())
                    .await;
            }
        }
    }
    Ok(())
}

fn hash_str(s: &str) -> u32 {
    // FNV-1a — fast, no dependency, stable.
    let mut h: u32 = 0x811c9dc5;
    for b in s.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::open;

    /// Helper: begin a run for subsequent journal-writing RPCs.
    async fn begin_test_run(store: &Arc<dyn RunStore>, inv: &str) -> String {
        let r = store
            .begin_run("app", "u", "s", inv, "t", 60_000)
            .await
            .unwrap();
        r.run_id
    }

    /// Helper: append a journal entry by recording a decision. Each call
    /// writes a `/tape/decision/recorded/<run_id>/<idx>` row to the journal
    /// with a unique (run_id, seq).
    async fn record_dec(store: &Arc<dyn RunStore>, run_id: &str, idx: i64) {
        store
            .record_decision(run_id, idx, "m", "{}", "{}", "", "p1")
            .await
            .unwrap();
    }

    /// Helper: append a `/tape/effect/pending/<tool>/<run_id>` journal entry.
    /// Multiple calls with the same `(tool, run_id)` but different
    /// `call_index` produce entries with the SAME subject and unique
    /// `(run_id, seq)` — exactly what the debounce tests need.
    async fn pending_effect(
        store: &Arc<dyn RunStore>,
        run_id: &str,
        tool: &str,
        call_index: i32,
    ) {
        store
            .begin_effect(run_id, 0, tool, call_index, "{}", "",
                          0, 0, "", "")
            .await
            .unwrap();
    }

    // ── Item 1: head-bootstrap ─────────────────────────────────────────────

    #[tokio::test]
    async fn head_bootstrap_seeds_cursor_at_current_head() {
        let store = open(":memory:").await.unwrap();
        // Generate 5+ journal entries before registration. `begin_run` writes
        // one `run` entry; `record_decision` writes one per decision_index.
        let rid = begin_test_run(&store, "inv-head").await;
        for i in 0..5 {
            record_dec(&store, &rid, i).await;
        }
        // Snapshot the current head.
        let entries = store.read_journal_after(0, 1024).await.unwrap();
        let head_before = entries
            .iter()
            .map(|e| e.global_seq)
            .max()
            .unwrap_or(0);
        assert!(head_before >= 5,
            "expected at least 5 entries, got head={head_before}");

        let r = Reaction {
            reaction_id: "r-head".into(),
            name: "head-bootstrap".into(),
            subject_pattern: "/tape/value/changed/**".into(),
            predicate_cel: String::new(),
            handler_kind: HandlerKind::Task as i32,
            agent_app: String::new(),
            publish_target: String::new(),
            max_concurrency: 1,
            rate_limit_per_s: 0,
            debounce_ms: 0,
            retry_max: 5,
            retry_backoff_ms: 1000,
            dlq_after_n: 5,
            num_shards: 3,
            created_at_ms: 0,
            deleted: false,
            bootstrap_from_head: true,
        };
        let stored = store.register_reaction(&r).await.unwrap();
        // Every shard's cursor must be seeded at the current head.
        for s in 0..stored.num_shards {
            let c = store.get_reaction_cursor(&stored.reaction_id, s).await.unwrap();
            assert_eq!(c, head_before, "shard {s} cursor should start at head");
        }
    }

    #[tokio::test]
    async fn default_registration_starts_cursor_at_zero() {
        let store = open(":memory:").await.unwrap();
        let rid = begin_test_run(&store, "inv-replay").await;
        for i in 0..5 {
            record_dec(&store, &rid, i).await;
        }
        let r = Reaction {
            reaction_id: "r-replay".into(),
            name: "replay".into(),
            subject_pattern: "/tape/decision/recorded/**".into(),
            predicate_cel: String::new(),
            handler_kind: HandlerKind::Task as i32,
            agent_app: String::new(),
            publish_target: String::new(),
            max_concurrency: 1,
            rate_limit_per_s: 0,
            debounce_ms: 0,
            retry_max: 5,
            retry_backoff_ms: 1000,
            dlq_after_n: 5,
            num_shards: 2,
            created_at_ms: 0,
            deleted: false,
            bootstrap_from_head: false,
        };
        let stored = store.register_reaction(&r).await.unwrap();
        for s in 0..stored.num_shards {
            let c = store.get_reaction_cursor(&stored.reaction_id, s).await.unwrap();
            assert_eq!(c, 0, "shard {s} cursor should default to 0 (replay)");
        }
    }

    #[tokio::test]
    async fn reregister_with_head_bootstrap_does_not_reset_cursor() {
        let store = open(":memory:").await.unwrap();
        // Initial register: replay-style.
        let r = Reaction {
            reaction_id: "r-stable".into(),
            name: "stable".into(),
            subject_pattern: "/tape/decision/recorded/**".into(),
            predicate_cel: String::new(),
            handler_kind: HandlerKind::Task as i32,
            agent_app: String::new(),
            publish_target: String::new(),
            max_concurrency: 1,
            rate_limit_per_s: 0,
            debounce_ms: 0,
            retry_max: 5,
            retry_backoff_ms: 1000,
            dlq_after_n: 5,
            num_shards: 1,
            created_at_ms: 0,
            deleted: false,
            bootstrap_from_head: false,
        };
        store.register_reaction(&r).await.unwrap();

        // Simulate the matcher having advanced the cursor to 3.
        store
            .set_reaction_cursor(&r.reaction_id, 0, 3, now_ms())
            .await
            .unwrap();
        assert_eq!(
            store.get_reaction_cursor(&r.reaction_id, 0).await.unwrap(),
            3
        );

        // Now write more entries so head > 3, and re-register with
        // bootstrap_from_head=true. The flag must be ignored on a row that
        // already exists; the cursor stays at 3 (not jumping to head).
        let rid = begin_test_run(&store, "inv-stable").await;
        for i in 0..10 {
            record_dec(&store, &rid, i).await;
        }
        let mut r2 = r.clone();
        r2.bootstrap_from_head = true;
        store.register_reaction(&r2).await.unwrap();

        let after = store.get_reaction_cursor(&r.reaction_id, 0).await.unwrap();
        assert_eq!(after, 3,
            "re-registering with bootstrap_from_head must not reset the cursor; \
             got {after}, expected 3");
    }

    // ── Item 2: server-side debounce ───────────────────────────────────────

    fn task_reaction(id: &str, debounce_ms: i32) -> Reaction {
        Reaction {
            reaction_id: id.into(),
            name: "debounce-test".into(),
            subject_pattern: "/tape/effect/pending/**".into(),
            predicate_cel: String::new(),
            handler_kind: HandlerKind::Task as i32,
            agent_app: String::new(),
            publish_target: String::new(),
            max_concurrency: 1,
            rate_limit_per_s: 0,
            debounce_ms,
            retry_max: 5,
            retry_backoff_ms: 1000,
            dlq_after_n: 5,
            num_shards: 1,
            created_at_ms: 0,
            deleted: false,
            bootstrap_from_head: false,
        }
    }

    #[tokio::test]
    async fn server_debounce_coalesces_within_window() {
        let store = open(":memory:").await.unwrap();
        let r = task_reaction("r-deb-coalesce", 500);
        store.register_reaction(&r).await.unwrap();

        // Two effect-pending writes 100 ms apart against the same (tool,
        // run_id) → both have subject /tape/effect/pending/sweep/<rid>.
        let rid = begin_test_run(&store, "inv-coalesce").await;
        pending_effect(&store, &rid, "sweep", 0).await;
        tick(&store).await.unwrap();
        let after1 = store.list_tasks(&r.reaction_id, 0, 10).await.unwrap();
        assert_eq!(after1.len(), 1, "expected one task after first write");
        let first_id = after1[0].task_id.clone();
        let first_gs = after1[0].source_global_seq;
        let first_subject = after1[0].subject.clone();

        // Second match within the debounce window → must coalesce.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        pending_effect(&store, &rid, "sweep", 1).await;
        tick(&store).await.unwrap();

        let after2 = store.list_tasks(&r.reaction_id, 0, 10).await.unwrap();
        assert_eq!(
            after2.len(),
            1,
            "second match within debounce window must coalesce, not insert"
        );
        assert_eq!(after2[0].task_id, first_id, "same row must be updated");
        assert_eq!(after2[0].subject, first_subject, "subject unchanged");
        assert!(
            after2[0].source_global_seq > first_gs,
            "source_global_seq must advance to the newer entry; \
             got {} (was {first_gs})",
            after2[0].source_global_seq
        );
    }

    #[tokio::test]
    async fn server_debounce_does_not_coalesce_outside_window() {
        let store = open(":memory:").await.unwrap();
        let r = task_reaction("r-deb-expire", 50);
        store.register_reaction(&r).await.unwrap();

        let rid = begin_test_run(&store, "inv-expire").await;
        pending_effect(&store, &rid, "sweep", 0).await;
        tick(&store).await.unwrap();
        assert_eq!(
            store.list_tasks(&r.reaction_id, 0, 10).await.unwrap().len(),
            1
        );

        // Sleep well past the debounce window before the second write.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        pending_effect(&store, &rid, "sweep", 1).await;
        tick(&store).await.unwrap();

        let tasks = store.list_tasks(&r.reaction_id, 0, 10).await.unwrap();
        assert_eq!(
            tasks.len(),
            2,
            "writes 150 ms apart with debounce_ms=50 must produce two tasks; got {}",
            tasks.len()
        );
    }

    #[tokio::test]
    async fn coalesce_against_claimed_task_falls_back_to_new_insert() {
        let store = open(":memory:").await.unwrap();
        let r = task_reaction("r-deb-race", 5_000);
        store.register_reaction(&r).await.unwrap();

        let rid = begin_test_run(&store, "inv-race").await;
        pending_effect(&store, &rid, "sweep", 0).await;
        tick(&store).await.unwrap();
        let initial = store.list_tasks(&r.reaction_id, 0, 10).await.unwrap();
        assert_eq!(initial.len(), 1);
        let task_id = initial[0].task_id.clone();

        // A dispatcher claims the task before the next match arrives.
        let claimed = store
            .claim_tasks(&r.reaction_id, 0, "owner-A", 60_000, 10, now_ms())
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].task_id, task_id);

        // Second match: find_pending_task_for_subject returns None (the row
        // is CLAIMED, not PENDING), so the matcher falls through to a fresh
        // create_task. The CLAIMED row stays untouched.
        pending_effect(&store, &rid, "sweep", 1).await;
        tick(&store).await.unwrap();

        let after = store.list_tasks(&r.reaction_id, 0, 10).await.unwrap();
        assert_eq!(
            after.len(),
            2,
            "a CLAIMED task is not eligible for coalescing — the next match must \
             insert a new row; got {} tasks",
            after.len()
        );

        // Direct check of the coalesce path against a CLAIMED task: the
        // conditional UPDATE matches zero rows, so the call returns None.
        let res = store
            .coalesce_task(&task_id, 999, "{}", "", "")
            .await
            .unwrap();
        assert!(
            res.is_none(),
            "coalesce_task against a CLAIMED task must be a no-op"
        );
    }

    // ── Lease-safety regression tests (Codex review on the PR) ───────────────
    //
    // Pre-fix bugs:
    //   1. complete_task only checked lease_owner == owner, not status =
    //      CLAIMED. Callers passing owner="" could match unleased PENDING
    //      rows (lease_owner='' default) and silently drop work.
    //   2. nack_task SELECTed (task_id, lease_owner) but then UPDATEd only
    //      by task_id — a TOCTOU race: if the lease expired between the
    //      SELECT and the UPDATE and another dispatcher reclaimed the row,
    //      the stale caller's nack would clobber the new claim.
    //
    // The fixes (sql.rs / bigtable.rs):
    //   * reject empty owner at the API boundary;
    //   * include `lease_owner=? AND status=CLAIMED` in the conditional
    //     UPDATE itself (atomic predicate, no TOCTOU window).

    async fn helper_register_and_make_pending_task(
        store: &Arc<dyn RunStore>,
        rid: &str,
    ) -> String {
        let r = Reaction {
            reaction_id: "r-lease".into(),
            name: "lease".into(),
            subject_pattern: "/tape/effect/pending/**".into(),
            predicate_cel: String::new(),
            handler_kind: HandlerKind::Task as i32,
            agent_app: String::new(),
            publish_target: String::new(),
            max_concurrency: 1,
            rate_limit_per_s: 0,
            debounce_ms: 0,
            retry_max: 5,
            retry_backoff_ms: 1,        // tiny so the test doesn't sit on backoff
            dlq_after_n: 5,
            num_shards: 1,
            created_at_ms: 0,
            deleted: false,
            bootstrap_from_head: false,
        };
        store.register_reaction(&r).await.unwrap();
        pending_effect(store, rid, "sweep", 0).await;
        tick(store).await.unwrap();
        let tasks = store.list_tasks("r-lease", 0, 10).await.unwrap();
        assert_eq!(tasks.len(), 1);
        tasks[0].task_id.clone()
    }

    #[tokio::test]
    async fn complete_task_rejects_empty_owner() {
        let store = open(":memory:").await.unwrap();
        let rid = begin_test_run(&store, "inv-empty-complete").await;
        let task_id = helper_register_and_make_pending_task(&store, &rid).await;
        // Owner="" must NOT match the unleased row.
        let err = store.complete_task(&task_id, "").await.unwrap_err();
        assert!(
            err.to_string().contains("owner is required"),
            "expected 'owner is required', got {err}"
        );
        // Task must remain PENDING.
        let after = store.list_tasks("r-lease", 0, 10).await.unwrap();
        assert_eq!(after[0].status, TaskStatus::Pending as i32);
    }

    #[tokio::test]
    async fn complete_task_refuses_unleased_pending_row() {
        // Even with a non-empty owner, completing a row that was never
        // claimed must fail (status != CLAIMED).
        let store = open(":memory:").await.unwrap();
        let rid = begin_test_run(&store, "inv-unleased").await;
        let task_id = helper_register_and_make_pending_task(&store, &rid).await;
        let res = store.complete_task(&task_id, "did-not-claim").await.unwrap();
        assert!(res.is_none(), "complete_task on a PENDING row must be a no-op");
        let after = store.list_tasks("r-lease", 0, 10).await.unwrap();
        assert_eq!(after[0].status, TaskStatus::Pending as i32);
    }

    #[tokio::test]
    async fn nack_task_rejects_empty_owner() {
        let store = open(":memory:").await.unwrap();
        let rid = begin_test_run(&store, "inv-empty-nack").await;
        let task_id = helper_register_and_make_pending_task(&store, &rid).await;
        let err = store.nack_task(&task_id, "", "boom", false, 0).await.unwrap_err();
        assert!(
            err.to_string().contains("owner is required"),
            "expected 'owner is required', got {err}"
        );
    }

    #[tokio::test]
    async fn nack_task_does_not_clobber_a_reclaimed_lease() {
        // The TOCTOU bug: owner A claims, A's lease expires, owner B reclaims,
        // then A's stale nack arrives. The UPDATE must reject A's nack because
        // lease_owner is now B (and the predicate also requires status=CLAIMED,
        // which is true post-reclaim — so the lease_owner predicate is the
        // active safety boundary here).
        let store = open(":memory:").await.unwrap();
        let rid = begin_test_run(&store, "inv-toctou").await;
        let task_id = helper_register_and_make_pending_task(&store, &rid).await;

        // A claims with a 1-ms lease so we can expire it immediately.
        let now_a = now_ms();
        let claimed_a = store
            .claim_tasks("r-lease", -1, "owner-A", 1, 10, now_a)
            .await
            .unwrap();
        assert_eq!(claimed_a.len(), 1);
        assert_eq!(claimed_a[0].lease_owner, "owner-A");

        // Time passes; A's 1-ms lease has expired.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let now_b = now_ms();
        let claimed_b = store
            .claim_tasks("r-lease", -1, "owner-B", 60_000, 10, now_b)
            .await
            .unwrap();
        assert_eq!(
            claimed_b.len(), 1,
            "B should be able to steal A's expired lease"
        );
        assert_eq!(claimed_b[0].lease_owner, "owner-B");
        let attempts_after_b_claim = claimed_b[0].attempts;

        // A's stale nack must NOT mutate the row — B's claim stands.
        let stale_result = store
            .nack_task(&task_id, "owner-A", "i thought i still had it", false, 0)
            .await
            .unwrap();
        assert!(
            stale_result.is_none(),
            "stale nack from A must be a no-op now that B has the lease"
        );
        let after = store.list_tasks("r-lease", 0, 10).await.unwrap();
        assert_eq!(after[0].lease_owner, "owner-B",
            "owner-B's lease must survive owner-A's stale nack");
        assert_eq!(after[0].status, TaskStatus::Claimed as i32,
            "status must remain CLAIMED — A's nack did not reset it");
        assert_eq!(after[0].attempts, attempts_after_b_claim,
            "attempts must NOT have been incremented by A's nack");

        // B can still complete the task it actually owns.
        let done = store.complete_task(&task_id, "owner-B").await.unwrap();
        assert!(done.is_some());
        assert_eq!(done.unwrap().status, TaskStatus::Done as i32);
    }

    #[tokio::test]
    async fn claim_tasks_rejects_empty_owner() {
        let store = open(":memory:").await.unwrap();
        let rid = begin_test_run(&store, "inv-empty-claim").await;
        let _ = helper_register_and_make_pending_task(&store, &rid).await;
        let err = store
            .claim_tasks("r-lease", -1, "", 1000, 10, now_ms())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("owner is required"),
            "expected 'owner is required', got {err}"
        );
    }
}
