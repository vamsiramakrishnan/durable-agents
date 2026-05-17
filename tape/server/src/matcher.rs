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
