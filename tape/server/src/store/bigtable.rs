//! The Cloud Bigtable implementation of [`RunStore`].
//!
//! Bigtable is a wide-column store — no SQL, no cross-row transactions — but it
//! has single-row atomic mutations and versioned cells, which is enough for
//! Tape's journal (append-only, per-run-ordered). One column family, `m`. Row
//! keys (all qualifiers live in family `m`; every value is a UTF-8 string):
//!
//!   r#<run_id>                       status, app, user, session, inv, lease_owner,
//!                                    lease_exp, waiting_gate, detail, started, ended, seq
//!   idx#<app>#<user>#<session>#<inv> run  (the reverse index used by begin_run)
//!   route#<app>#<user>#<session>     run  (the newest run for a session — used by send_signal)
//!   d#<run_id>#<decision_index:020>  seq, model, request, response, rationale, policy, ts
//!   e#<idempotency_key>              seq, run, decision_index, tool, status, request, response, error, ts
//!   o#<run_id>#<seq:020>             effect_key, kind, payload, status, ts
//!   b#<run_id>                       usd_cap, tok_cap, usd_spent, tok_spent
//!   sig#<run_id>#<gate>              delivered, awaited, consumed, context, resolution, ts
//!   sess#<app>#<user>#<session>      state, last_update, event_count
//!   ev#<app>#<user>#<session>#<ord:020>  event_id, inv, author, branch, content, actions, ts
//!   j#<run_id>#<seq:020>             kind, payload, ts
//!
//! The per-run `seq` is held on `r#<run_id>` and bumped read-then-write; that is
//! safe because the lease guarantees a single writer per run (Bigtable's
//! `ReadModifyWriteRow` counter would be tidier but the data-plane client crate
//! doesn't expose it). `list_runs_to_recover` scans `r#` rows and filters in
//! memory — fine at moderate scale; at very high scale you'd drive recovery from
//! the Bigtable-change-streams → Pub/Sub reactor (design-principles/tape.md §12)
//! rather than polling. `AppendEvent` is two writes (the session row, then the
//! event row) — Bigtable has no cross-row transactions; a crash between leaves
//! the state applied without its event, which the re-drive re-creates idempotently.
//!
//! The table and its column family `m` must exist before the server starts
//! (Bigtable needs explicit table creation, like creating a Postgres database):
//!
//!   cbt -project P -instance I createtable tape
//!   cbt -project P -instance I createfamily tape m maxversions=1
//!
//! With `BIGTABLE_EMULATOR_HOST=localhost:PORT` set, `bigtable://demo/demo/tape`
//! talks to the local emulator (the same `cbt` commands create the table there).

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use bigtable_rs::bigtable::{BigTable, BigTableConnection, RowCell};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    mutation, row_filter, MutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowSet,
};

use super::{derive_key, merge_json, now_ms, RunStore, StoreError, StoreResult};
use crate::pb::*;

const FAM: &str = "m";

fn e<E: std::fmt::Display>(err: E) -> StoreError {
    StoreError::Msg(format!("bigtable: {err}"))
}

// ── value encoding (every cell is a UTF-8 string) ───────────────────────────
fn sv(v: impl Into<String>) -> Vec<u8> { v.into().into_bytes() }
fn iv(v: i64) -> Vec<u8> { v.to_string().into_bytes() }
fn fv(v: f64) -> Vec<u8> { v.to_string().into_bytes() }

type RowMap = HashMap<String, Vec<u8>>;
trait RowMapExt {
    fn gs(&self, q: &str) -> String;
    fn gi(&self, q: &str) -> i64;
    fn gf(&self, q: &str) -> f64;
}
impl RowMapExt for RowMap {
    fn gs(&self, q: &str) -> String {
        self.get(q).map(|b| String::from_utf8_lossy(b).into_owned()).unwrap_or_default()
    }
    fn gi(&self, q: &str) -> i64 { self.gs(q).parse().unwrap_or(0) }
    fn gf(&self, q: &str) -> f64 { self.gs(q).parse().unwrap_or(0.0) }
}

fn cells_to_map(cells: Vec<RowCell>) -> RowMap {
    // `CellsPerColumnLimitFilter(1)` keeps the newest cell per (family, qualifier),
    // returned newest-first; we only use family `m`, so key by qualifier.
    let mut m = HashMap::new();
    for c in cells {
        if c.family_name == FAM {
            let q = String::from_utf8_lossy(&c.qualifier).into_owned();
            m.entry(q).or_insert(c.value);
        }
    }
    m
}

fn set(qualifier: &str, value: Vec<u8>) -> Mutation {
    Mutation {
        mutation: Some(mutation::Mutation::SetCell(mutation::SetCell {
            family_name: FAM.to_string(),
            column_qualifier: qualifier.as_bytes().to_vec(),
            timestamp_micros: -1, // Bigtable server time
            value,
        })),
    }
}
fn delete_row_mut() -> Mutation {
    Mutation { mutation: Some(mutation::Mutation::DeleteFromRow(mutation::DeleteFromRow {})) }
}
fn latest_only() -> Option<RowFilter> {
    Some(RowFilter { filter: Some(row_filter::Filter::CellsPerColumnLimitFilter(1)) })
}

pub struct BigtableRunStore {
    conn: BigTableConnection,
    table: String, // full name: projects/<p>/instances/<i>/tables/<t>
}

impl BigtableRunStore {
    pub async fn connect(project: &str, instance: &str, table: &str) -> StoreResult<Self> {
        let conn = BigTableConnection::new(project, instance, false, 4, Some(Duration::from_secs(30)))
            .await
            .map_err(e)?;
        let full = conn.client().get_full_table_name(table);
        let s = Self { conn, table: full };
        // Probe — fails loudly (with a fix-it message) if the table/family is missing.
        s.read_row("__tape_probe__").await.map_err(|err| StoreError::msg(format!(
            "{err} — does the Bigtable table exist? create it once: \
             `cbt -project {project} -instance {instance} createtable {table}` then \
             `cbt -project {project} -instance {instance} createfamily {table} m maxversions=1`")))?;
        Ok(s)
    }

    fn bt(&self) -> BigTable { self.conn.client() }

    async fn read_row(&self, key: &str) -> StoreResult<Option<RowMap>> {
        let mut bt = self.bt();
        let req = ReadRowsRequest {
            table_name: self.table.clone(),
            rows: Some(RowSet { row_keys: vec![key.as_bytes().to_vec()], row_ranges: vec![] }),
            filter: latest_only(),
            ..Default::default()
        };
        let rows = bt.read_rows(req).await.map_err(e)?;
        Ok(rows.into_iter().next().map(|(_k, cells)| cells_to_map(cells)))
    }

    async fn read_prefix(&self, prefix: &str, limit: i64) -> StoreResult<Vec<(String, RowMap)>> {
        let mut bt = self.bt();
        let req = ReadRowsRequest {
            table_name: self.table.clone(),
            filter: latest_only(),
            rows_limit: limit.max(0),
            ..Default::default()
        };
        let rows = bt.read_rows_with_prefix(req, prefix.as_bytes().to_vec()).await.map_err(e)?;
        Ok(rows.into_iter()
            .map(|(k, cells)| (String::from_utf8_lossy(&k).into_owned(), cells_to_map(cells)))
            .collect())
    }

    async fn write_row(&self, key: &str, muts: Vec<Mutation>) -> StoreResult<()> {
        if muts.is_empty() {
            return Ok(());
        }
        let mut bt = self.bt();
        bt.mutate_row(MutateRowRequest {
            table_name: self.table.clone(),
            row_key: key.as_bytes().to_vec(),
            mutations: muts,
            ..Default::default()
        })
        .await
        .map_err(e)?;
        Ok(())
    }
    async fn delete_row(&self, key: &str) -> StoreResult<()> {
        self.write_row(key, vec![delete_row_mut()]).await
    }

    async fn next_seq(&self, run_id: &str) -> StoreResult<i64> {
        let cur = self.read_row(&rk_run(run_id)).await?.map(|m| m.gi("seq")).unwrap_or(0);
        let next = cur + 1;
        self.write_row(&rk_run(run_id), vec![set("seq", iv(next))]).await?;
        Ok(next)
    }
    async fn journal(&self, run_id: &str, seq: i64, kind: &str, payload: &str, ts: i64) -> StoreResult<()> {
        self.write_row(&rk_journal(run_id, seq), vec![set("kind", sv(kind)), set("payload", sv(payload)), set("ts", iv(ts))]).await
    }

    async fn run_state(&self, run_id: &str) -> StoreResult<Option<RunState>> {
        Ok(self.read_row(&rk_run(run_id)).await?.map(|m| run_from(run_id, &m)))
    }
}

// ── row keys ────────────────────────────────────────────────────────────────
fn rk_run(run_id: &str) -> String { format!("r#{run_id}") }
fn rk_idx(app: &str, user: &str, session: &str, inv: &str) -> String { format!("idx#{app}#{user}#{session}#{inv}") }
fn rk_route(app: &str, user: &str, session: &str) -> String { format!("route#{app}#{user}#{session}") }
fn rk_decision(run_id: &str, i: i64) -> String { format!("d#{run_id}#{i:020}") }
fn rk_effect(key: &str) -> String { format!("e#{key}") }
fn rk_obligation(run_id: &str, seq: i64) -> String { format!("o#{run_id}#{seq:020}") }
fn rk_budget(run_id: &str) -> String { format!("b#{run_id}") }
fn rk_signal(run_id: &str, gate: &str) -> String { format!("sig#{run_id}#{gate}") }
fn rk_session(app: &str, user: &str, session: &str) -> String { format!("sess#{app}#{user}#{session}") }
fn rk_event(app: &str, user: &str, session: &str, ord: i64) -> String { format!("ev#{app}#{user}#{session}#{ord:020}") }
fn rk_journal(run_id: &str, seq: i64) -> String { format!("j#{run_id}#{seq:020}") }

// ── decoders ────────────────────────────────────────────────────────────────
fn run_from(run_id: &str, m: &RowMap) -> RunState {
    RunState {
        run_id: run_id.to_string(),
        app_name: m.gs("app"), user_id: m.gs("user"), session_id: m.gs("session"),
        invocation_id: m.gs("inv"), status: m.gi("status") as i32, seq_cursor: m.gi("seq"),
        lease_owner: m.gs("lease_owner"), lease_expires_at_ms: m.gi("lease_exp"),
        started_at_ms: m.gi("started"), ended_at_ms: m.gi("ended"), waiting_on_gate: m.gs("waiting_gate"),
    }
}
fn effect_from(key: &str, m: &RowMap) -> EffectRecord {
    EffectRecord {
        run_id: m.gs("run"), seq: m.gi("seq"), decision_index: m.gi("decision_index"),
        tool_name: m.gs("tool"), idempotency_key: key.to_string(), status: m.gi("status") as i32,
        request_json: m.gs("request"), response_json: m.gs("response"), error_json: m.gs("error"), ts_ms: m.gi("ts"),
    }
}
fn decision_from(run_id: &str, idx: i64, m: &RowMap) -> DecisionRecord {
    DecisionRecord {
        run_id: run_id.to_string(), seq: m.gi("seq"), decision_index: idx, model: m.gs("model"),
        request_json: m.gs("request"), response_json: m.gs("response"), rationale: m.gs("rationale"),
        policy_version: m.gs("policy"), ts_ms: m.gi("ts"),
    }
}
fn obligation_from(run_id: &str, seq: i64, m: &RowMap) -> ObligationRecord {
    ObligationRecord {
        run_id: run_id.to_string(), seq, effect_key: m.gs("effect_key"), kind: m.gs("kind"),
        payload_json: m.gs("payload"), status: m.gi("status") as i32, ts_ms: m.gi("ts"),
    }
}

#[async_trait]
impl RunStore for BigtableRunStore {
    // ── run lifecycle ───────────────────────────────────────────────────────
    async fn begin_run(&self, app: &str, user: &str, session: &str, invocation: &str, lease_owner: &str, lease_ttl_ms: i64) -> StoreResult<BeginRunResponse> {
        let ts = now_ms();
        let lease_exp = ts + lease_ttl_ms.max(0);
        if let Some(idx) = self.read_row(&rk_idx(app, user, session, invocation)).await? {
            let run_id = idx.gs("run");
            if !run_id.is_empty() {
                let cur = self.run_state(&run_id).await?;
                let status = cur.as_ref().map(|r| r.status).unwrap_or(RunStatus::Running as i32);
                let seq = cur.as_ref().map(|r| r.seq_cursor).unwrap_or(0);
                if status != RunStatus::Terminal as i32 && status != RunStatus::Stuck as i32 {
                    self.write_row(&rk_run(&run_id), vec![set("status", iv(RunStatus::Running as i64)), set("lease_owner", sv(lease_owner)), set("lease_exp", iv(lease_exp))]).await?;
                }
                let st = if status == RunStatus::Terminal as i32 || status == RunStatus::Stuck as i32 { status } else { RunStatus::Running as i32 };
                return Ok(BeginRunResponse { run_id, resumed: true, next_seq: seq, status: st });
            }
        }
        let run_id = uuid::Uuid::new_v4().to_string();
        self.write_row(&rk_run(&run_id), vec![
            set("status", iv(RunStatus::Running as i64)), set("app", sv(app)), set("user", sv(user)),
            set("session", sv(session)), set("inv", sv(invocation)), set("seq", iv(0)),
            set("lease_owner", sv(lease_owner)), set("lease_exp", iv(lease_exp)), set("started", iv(ts)),
        ]).await?;
        self.write_row(&rk_idx(app, user, session, invocation), vec![set("run", sv(run_id.clone()))]).await?;
        self.write_row(&rk_route(app, user, session), vec![set("run", sv(run_id.clone())), set("started", iv(ts))]).await?;
        Ok(BeginRunResponse { run_id, resumed: false, next_seq: 0, status: RunStatus::Running as i32 })
    }
    async fn resume_run(&self, run_id: &str, lease_owner: &str, lease_ttl_ms: i64) -> StoreResult<Option<RunState>> {
        self.write_row(&rk_run(run_id), vec![set("status", iv(RunStatus::Running as i64)), set("lease_owner", sv(lease_owner)), set("lease_exp", iv(now_ms() + lease_ttl_ms.max(0)))]).await?;
        self.run_state(run_id).await
    }
    async fn end_run(&self, run_id: &str, status: i32, detail_json: &str) -> StoreResult<Option<RunState>> {
        self.write_row(&rk_run(run_id), vec![set("status", iv(status as i64)), set("ended", iv(now_ms())), set("detail", sv(detail_json)), set("lease_owner", sv(""))]).await?;
        self.run_state(run_id).await
    }
    async fn get_run(&self, run_id: &str) -> StoreResult<Option<RunState>> { self.run_state(run_id).await }
    async fn list_runs_to_recover(&self, now_ms: i64, limit: i64) -> StoreResult<Vec<RunState>> {
        let rows = self.read_prefix("r#", limit.max(1) * 8).await?;
        let mut out = Vec::new();
        for (key, m) in rows {
            let run_id = key.trim_start_matches("r#").to_string();
            let st = m.gi("status") as i32;
            let recoverable = if st == RunStatus::Runnable as i32 {
                true
            } else if st == RunStatus::Running as i32 {
                m.gi("lease_exp") < now_ms
            } else if st == RunStatus::Waiting as i32 {
                self.read_prefix(&format!("sig#{run_id}#"), 1000).await?
                    .iter().any(|(_, s)| s.gi("delivered") == 1 && s.gi("consumed") == 0)
            } else {
                false
            };
            if recoverable {
                out.push(run_from(&run_id, &m));
                if out.len() as i64 >= limit { break; }
            }
        }
        Ok(out)
    }
    async fn journal_range(&self, run_id: &str, from_seq: i64) -> StoreResult<Vec<JournalEntry>> {
        let rows = self.read_prefix(&format!("j#{run_id}#"), 100_000).await?;
        let mut out: Vec<JournalEntry> = rows.into_iter().filter_map(|(key, m)| {
            let seq: i64 = key.rsplit('#').next().and_then(|s| s.parse().ok()).unwrap_or(0);
            if seq >= from_seq { Some(JournalEntry {
                seq, kind: m.gs("kind"), payload_json: m.gs("payload"), ts_ms: m.gi("ts"),
                global_seq: 0, subject: String::new(), schema_version: 1,
                trace_id: String::new(), span_id: String::new(), parent_span_id: String::new(),
            }) } else { None }
        }).collect();
        out.sort_by_key(|j| j.seq);
        Ok(out)
    }

    // ── decisions ───────────────────────────────────────────────────────────
    async fn record_decision(&self, run_id: &str, decision_index: i64, model: &str, request_json: &str, response_json: &str, rationale: &str, policy_version: &str) -> StoreResult<DecisionRecord> {
        if let Some(m) = self.read_row(&rk_decision(run_id, decision_index)).await? {
            return Ok(decision_from(run_id, decision_index, &m));
        }
        let ts = now_ms();
        let seq = self.next_seq(run_id).await?;
        self.write_row(&rk_decision(run_id, decision_index), vec![
            set("seq", iv(seq)), set("model", sv(model)), set("request", sv(request_json)), set("response", sv(response_json)),
            set("rationale", sv(rationale)), set("policy", sv(policy_version)), set("ts", iv(ts)),
        ]).await?;
        self.journal(run_id, seq, "decision", &serde_json::json!({"decision_index": decision_index, "model": model, "policy_version": policy_version, "rationale": rationale}).to_string(), ts).await?;
        Ok(DecisionRecord { run_id: run_id.into(), seq, decision_index, model: model.into(), request_json: request_json.into(), response_json: response_json.into(), rationale: rationale.into(), policy_version: policy_version.into(), ts_ms: ts })
    }
    async fn get_decision(&self, run_id: &str, decision_index: i64) -> StoreResult<Option<DecisionRecord>> {
        Ok(self.read_row(&rk_decision(run_id, decision_index)).await?.map(|m| decision_from(run_id, decision_index, &m)))
    }

    // ── effects ─────────────────────────────────────────────────────────────
    async fn begin_effect(&self, run_id: &str, decision_index: i64, tool_name: &str, call_index: i32, request_json: &str, custom_key: &str) -> StoreResult<EffectRecord> {
        let key = if custom_key.is_empty() { derive_key(run_id, decision_index, tool_name, call_index) } else { custom_key.to_string() };
        if let Some(m) = self.read_row(&rk_effect(&key)).await? {
            return Ok(effect_from(&key, &m));
        }
        let ts = now_ms();
        let seq = self.next_seq(run_id).await?;
        // intent first — Bigtable single-row writes are atomic & durable; the tool body runs only after this returns.
        self.write_row(&rk_effect(&key), vec![
            set("run", sv(run_id)), set("seq", iv(seq)), set("decision_index", iv(decision_index)), set("tool", sv(tool_name)),
            set("status", iv(EffectStatus::Pending as i64)), set("request", sv(request_json)), set("ts", iv(ts)),
        ]).await?;
        self.journal(run_id, seq, "effect", &serde_json::json!({"tool": tool_name, "decision_index": decision_index, "idempotency_key": key, "status": "pending"}).to_string(), ts).await?;
        Ok(EffectRecord { run_id: run_id.into(), seq, decision_index, tool_name: tool_name.into(), idempotency_key: key, status: EffectStatus::Pending as i32, request_json: request_json.into(), response_json: String::new(), error_json: String::new(), ts_ms: ts })
    }
    async fn complete_effect(&self, run_id: &str, key: &str, status: i32, response_json: &str, error_json: &str) -> StoreResult<Option<EffectRecord>> {
        let Some(m) = self.read_row(&rk_effect(key)).await? else { return Ok(None); };
        let existing = effect_from(key, &m);
        if existing.status != EffectStatus::Pending as i32 { return Ok(Some(existing)); }
        let ts = now_ms();
        self.write_row(&rk_effect(key), vec![set("status", iv(status as i64)), set("response", sv(response_json)), set("error", sv(error_json)), set("ts", iv(ts))]).await?;
        let seq = self.next_seq(run_id).await?;
        let label = match EffectStatus::try_from(status) { Ok(EffectStatus::Confirmed) => "confirmed", Ok(EffectStatus::Failed) => "failed", Ok(EffectStatus::Unknown) => "unknown", _ => "completed" };
        self.journal(run_id, seq, "effect", &serde_json::json!({"tool": existing.tool_name, "idempotency_key": key, "status": label}).to_string(), ts).await?;
        Ok(Some(EffectRecord { status, response_json: response_json.into(), error_json: error_json.into(), ts_ms: ts, ..existing }))
    }
    async fn get_effect(&self, run_id: &str, key: &str) -> StoreResult<Option<EffectRecord>> {
        let _ = run_id;
        Ok(self.read_row(&rk_effect(key)).await?.map(|m| effect_from(key, &m)))
    }
    async fn reconcile_effect(&self, run_id: &str, key: &str, resolved_status: i32, response_json: &str, error_json: &str) -> StoreResult<Option<EffectRecord>> {
        let Some(m) = self.read_row(&rk_effect(key)).await? else { return Ok(None); };
        let existing = effect_from(key, &m);
        if existing.status == EffectStatus::Confirmed as i32 || existing.status == EffectStatus::Failed as i32 { return Ok(Some(existing)); }
        let ts = now_ms();
        self.write_row(&rk_effect(key), vec![set("status", iv(resolved_status as i64)), set("response", sv(response_json)), set("error", sv(error_json)), set("ts", iv(ts))]).await?;
        let seq = self.next_seq(run_id).await?;
        self.journal(run_id, seq, "effect", &serde_json::json!({"tool": existing.tool_name, "idempotency_key": key, "status": "reconciled", "resolved_to": resolved_status}).to_string(), ts).await?;
        Ok(Some(EffectRecord { status: resolved_status, response_json: response_json.into(), error_json: error_json.into(), ts_ms: ts, ..existing }))
    }

    // ── obligations ─────────────────────────────────────────────────────────
    async fn register_compensation(&self, run_id: &str, effect_key: &str, kind: &str, payload_json: &str) -> StoreResult<ObligationRecord> {
        // idempotent on (run_id, effect_key, kind): scan existing obligations.
        for (key, m) in self.read_prefix(&format!("o#{run_id}#"), 100_000).await? {
            if m.gs("effect_key") == effect_key && m.gs("kind") == kind {
                let seq: i64 = key.rsplit('#').next().and_then(|s| s.parse().ok()).unwrap_or(0);
                return Ok(obligation_from(run_id, seq, &m));
            }
        }
        let ts = now_ms();
        let seq = self.next_seq(run_id).await?;
        let status = ObligationStatus::Committed as i64;
        self.write_row(&rk_obligation(run_id, seq), vec![set("effect_key", sv(effect_key)), set("kind", sv(kind)), set("payload", sv(payload_json)), set("status", iv(status)), set("ts", iv(ts))]).await?;
        self.journal(run_id, seq, "obligation", &serde_json::json!({"effect_key": effect_key, "kind": kind}).to_string(), ts).await?;
        Ok(ObligationRecord { run_id: run_id.into(), seq, effect_key: effect_key.into(), kind: kind.into(), payload_json: payload_json.into(), status: status as i32, ts_ms: ts })
    }
    async fn list_obligations(&self, run_id: &str, only_unresolved: bool) -> StoreResult<Vec<ObligationRecord>> {
        let mut out: Vec<ObligationRecord> = self.read_prefix(&format!("o#{run_id}#"), 100_000).await?.into_iter().filter_map(|(key, m)| {
            let seq: i64 = key.rsplit('#').next().and_then(|s| s.parse().ok())?;
            let st = m.gi("status") as i32;
            if only_unresolved && (st == ObligationStatus::Compensated as i32 || st == ObligationStatus::Stuck as i32) { None } else { Some(obligation_from(run_id, seq, &m)) }
        }).collect();
        out.sort_by(|a, b| b.seq.cmp(&a.seq)); // newest-first (LIFO)
        Ok(out)
    }
    async fn resolve_obligation(&self, run_id: &str, obligation_seq: i64, status: i32, result_json: &str) -> StoreResult<Option<ObligationRecord>> {
        let key = rk_obligation(run_id, obligation_seq);
        if self.read_row(&key).await?.is_none() { return Ok(None); }
        self.write_row(&key, vec![set("status", iv(status as i64)), set("result", sv(result_json)), set("ts", iv(now_ms()))]).await?;
        Ok(self.read_row(&key).await?.map(|m| obligation_from(run_id, obligation_seq, &m)))
    }

    // ── budget ──────────────────────────────────────────────────────────────
    async fn set_budget(&self, run_id: &str, usd_cap: f64, token_cap: i64) -> StoreResult<BudgetState> {
        self.write_row(&rk_budget(run_id), vec![set("usd_cap", fv(usd_cap)), set("tok_cap", iv(token_cap))]).await?;
        self.get_budget(run_id).await
    }
    async fn get_budget(&self, run_id: &str) -> StoreResult<BudgetState> {
        let m = self.read_row(&rk_budget(run_id)).await?.unwrap_or_default();
        Ok(BudgetState { run_id: run_id.into(), usd_cap: m.gf("usd_cap"), token_cap: m.gi("tok_cap"), usd_spent: m.gf("usd_spent"), tokens_spent: m.gi("tok_spent") })
    }
    async fn charge_budget(&self, run_id: &str, usd: f64, tokens: i64) -> StoreResult<BudgetState> {
        // read-then-write: single-writer per run (the lease) makes this safe.
        let m = self.read_row(&rk_budget(run_id)).await?.unwrap_or_default();
        self.write_row(&rk_budget(run_id), vec![set("usd_spent", fv(m.gf("usd_spent") + usd)), set("tok_spent", iv(m.gi("tok_spent") + tokens))]).await?;
        self.get_budget(run_id).await
    }

    // ── gates ───────────────────────────────────────────────────────────────
    async fn await_signal(&self, run_id: &str, gate_name: &str, payload_json: &str) -> StoreResult<(bool, String)> {
        let ts = now_ms();
        if let Some(m) = self.read_row(&rk_signal(run_id, gate_name)).await? {
            if m.gi("delivered") == 1 {
                self.write_row(&rk_signal(run_id, gate_name), vec![set("consumed", iv(1))]).await?;
                return Ok((true, m.gs("resolution")));
            }
        }
        self.write_row(&rk_signal(run_id, gate_name), vec![set("awaited", iv(1)), set("context", sv(payload_json)), set("ts", iv(ts))]).await?;
        let seq = self.next_seq(run_id).await?;
        self.write_row(&rk_run(run_id), vec![set("status", iv(RunStatus::Waiting as i64)), set("waiting_gate", sv(gate_name)), set("lease_owner", sv(""))]).await?;
        self.journal(run_id, seq, "gate", &serde_json::json!({"gate": gate_name, "status": "waiting"}).to_string(), ts).await?;
        Ok((false, String::new()))
    }
    async fn send_signal(&self, run_id: &str, app: &str, user: &str, session: &str, gate_name: &str, resolution_json: &str) -> StoreResult<(String, i32)> {
        let ts = now_ms();
        let run_id = if !run_id.is_empty() { run_id.to_string() } else {
            self.read_row(&rk_route(app, user, session)).await?.map(|m| m.gs("run")).filter(|r| !r.is_empty())
                .ok_or_else(|| StoreError::msg("no run for that session"))?
        };
        self.write_row(&rk_signal(&run_id, gate_name), vec![set("delivered", iv(1)), set("resolution", sv(resolution_json)), set("ts", iv(ts))]).await?;
        let mut run_status = RunStatus::Unspecified as i32;
        if let Some(m) = self.run_state(&run_id).await? {
            if m.status == RunStatus::Waiting as i32 && m.waiting_on_gate == gate_name {
                self.write_row(&rk_run(&run_id), vec![set("status", iv(RunStatus::Runnable as i64)), set("waiting_gate", sv(""))]).await?;
                run_status = RunStatus::Runnable as i32;
                if let Ok(seq) = self.next_seq(&run_id).await {
                    let _ = self.journal(&run_id, seq, "gate", &serde_json::json!({"gate": gate_name, "status": "released"}).to_string(), ts).await;
                }
            }
        }
        Ok((run_id, run_status))
    }

    // ── sessions ────────────────────────────────────────────────────────────
    async fn create_session(&self, app: &str, user: &str, session: &str, state_json: &str) -> StoreResult<Session> {
        let ts = now_ms();
        let session_id = if session.is_empty() { uuid::Uuid::new_v4().to_string() } else { session.to_string() };
        let state = if state_json.is_empty() { "{}".to_string() } else { state_json.to_string() };
        // create if absent; if present, just update the state.
        let existing_count = self.read_row(&rk_session(app, user, &session_id)).await?.map(|m| m.gi("event_count")).unwrap_or(0);
        self.write_row(&rk_session(app, user, &session_id), vec![set("state", sv(state.clone())), set("last_update", iv(ts)), set("event_count", iv(existing_count))]).await?;
        Ok(Session { app_name: app.into(), user_id: user.into(), session_id, state_json: state, events: vec![], last_update_time_ms: ts })
    }
    async fn get_session(&self, app: &str, user: &str, session: &str, max_events: i64) -> StoreResult<Option<Session>> {
        let Some(m) = self.read_row(&rk_session(app, user, session)).await? else { return Ok(None); };
        let mut events: Vec<(i64, EventRecord)> = self.read_prefix(&format!("ev#{app}#{user}#{session}#"), if max_events > 0 { max_events } else { 1_000_000 }).await?
            .into_iter().filter_map(|(key, e)| {
                let ord: i64 = key.rsplit('#').next().and_then(|x| x.parse().ok())?;
                Some((ord, EventRecord { id: e.gs("event_id"), invocation_id: e.gs("inv"), author: e.gs("author"), branch: e.gs("branch"), content_json: e.gs("content"), actions_json: e.gs("actions"), timestamp_ms: e.gi("ts") }))
            }).collect();
        events.sort_by_key(|(ord, _)| *ord);
        Ok(Some(Session { app_name: app.into(), user_id: user.into(), session_id: session.into(), state_json: m.gs("state"), events: events.into_iter().map(|(_, e)| e).collect(), last_update_time_ms: m.gi("last_update") }))
    }
    async fn list_sessions(&self, app: &str, user: &str) -> StoreResult<Vec<Session>> {
        let rows = self.read_prefix(&format!("sess#{app}#{user}#"), 100_000).await?;
        Ok(rows.into_iter().map(|(key, m)| {
            let session_id = key.rsplit('#').next().unwrap_or("").to_string();
            Session { app_name: app.into(), user_id: user.into(), session_id, state_json: m.gs("state"), events: vec![], last_update_time_ms: m.gi("last_update") }
        }).collect())
    }
    async fn delete_session(&self, app: &str, user: &str, session: &str) -> StoreResult<bool> {
        let existed = self.read_row(&rk_session(app, user, session)).await?.is_some();
        for (key, _) in self.read_prefix(&format!("ev#{app}#{user}#{session}#"), 1_000_000).await? {
            self.delete_row(&key).await?;
        }
        self.delete_row(&rk_session(app, user, session)).await?;
        Ok(existed)
    }
    async fn append_event(&self, app: &str, user: &str, session: &str, event: EventRecord, state_delta_json: &str) -> StoreResult<(EventRecord, i64)> {
        let ts = if event.timestamp_ms > 0 { event.timestamp_ms } else { now_ms() };
        let sess = self.read_row(&rk_session(app, user, session)).await?.unwrap_or_default();
        let ord = sess.gi("event_count");
        let cur_state = if sess.contains_key("state") { sess.gs("state") } else { "{}".to_string() };
        let delta = if state_delta_json.is_empty() { "{}".to_string() } else { state_delta_json.to_string() };
        let merged = merge_json(&cur_state, &delta);
        // session row first (state + count), then the event row. Bigtable has no
        // cross-row txn; a crash between leaves the state applied but the event
        // missing — the re-drive re-creates it idempotently.
        self.write_row(&rk_session(app, user, session), vec![set("state", sv(merged)), set("last_update", iv(ts)), set("event_count", iv(ord + 1))]).await?;
        self.write_row(&rk_event(app, user, session, ord), vec![
            set("event_id", sv(event.id.clone())), set("inv", sv(event.invocation_id.clone())), set("author", sv(event.author.clone())),
            set("branch", sv(event.branch.clone())), set("content", sv(event.content_json.clone())), set("actions", sv(event.actions_json.clone())), set("ts", iv(ts)),
        ]).await?;
        Ok((EventRecord { timestamp_ms: ts, ..event }, ts))
    }

    // ── reconciliation ──────────────────────────────────────────────────────
    async fn list_pending_effects(&self, older_than_ms: i64, include_pending: bool, include_unknown: bool, limit: i64) -> StoreResult<Vec<EffectRecord>> {
        let (ip, iu) = if !include_pending && !include_unknown { (true, true) } else { (include_pending, include_unknown) };
        let mut out: Vec<EffectRecord> = self.read_prefix("e#", limit.max(1) * 32).await?.into_iter().filter_map(|(key, m)| {
            let st = m.gi("status") as i32;
            let want = (iu && st == EffectStatus::Unknown as i32)
                || (ip && st == EffectStatus::Pending as i32 && (older_than_ms == 0 || m.gi("ts") < older_than_ms));
            if want { Some(effect_from(key.trim_start_matches("e#"), &m)) } else { None }
        }).collect();
        out.sort_by_key(|x| x.ts_ms);
        out.truncate(limit.max(1) as usize);
        Ok(out)
    }

    // ── timers ──────────────────────────────────────────────────────────────
    async fn set_timer(&self, run_id: &str, timer_id: &str, fire_at_ms: i64, kind: &str, payload_json: &str) -> StoreResult<TimerRecord> {
        let tid = if timer_id.is_empty() { uuid::Uuid::new_v4().to_string() } else { timer_id.to_string() };
        let ts = now_ms();
        self.write_row(&rk_timer(run_id, &tid), vec![set("fire_at", iv(fire_at_ms)), set("kind", sv(kind)), set("payload", sv(payload_json)), set("fired", iv(0)), set("created", iv(ts))]).await?;
        Ok(TimerRecord { run_id: run_id.into(), timer_id: tid, fire_at_ms, kind: kind.into(), payload_json: payload_json.into(), fired: false, created_at_ms: ts })
    }
    async fn cancel_timer(&self, run_id: &str, timer_id: &str) -> StoreResult<bool> {
        let existed = self.read_row(&rk_timer(run_id, timer_id)).await?.is_some();
        self.delete_row(&rk_timer(run_id, timer_id)).await?;
        Ok(existed)
    }
    async fn list_due_timers(&self, now_ms: i64, limit: i64, claim: bool) -> StoreResult<Vec<TimerRecord>> {
        let mut due: Vec<(String, RowMap)> = self.read_prefix("tmr#", 100_000).await?.into_iter()
            .filter(|(_, m)| m.gi("fired") == 0 && m.gi("fire_at") <= now_ms).collect();
        due.sort_by_key(|(_, m)| m.gi("fire_at"));
        due.truncate(limit.max(1) as usize);
        let mut out = Vec::new();
        for (key, m) in due {
            // key = tmr#<run_id>#<timer_id>
            let rest = key.trim_start_matches("tmr#");
            let (run_id, tid) = rest.split_once('#').unwrap_or((rest, ""));
            if claim {
                // read-then-write claim; a double-claim is harmless because timer actions are idempotent.
                self.write_row(&key, vec![set("fired", iv(1))]).await?;
            }
            out.push(TimerRecord { run_id: run_id.into(), timer_id: tid.into(), fire_at_ms: m.gi("fire_at"), kind: m.gs("kind"), payload_json: m.gs("payload"), fired: claim, created_at_ms: m.gi("created") });
        }
        Ok(out)
    }

    // ── reactive key-value store ────────────────────────────────────────────
    async fn write_value(&self, namespace: &str, key: &str, value_json: &str, if_version: i64, writer: &str) -> StoreResult<ValueRecord> {
        let rk = rk_value(namespace, key);
        if if_version >= 0 {
            let cur = self.read_row(&rk).await?;
            let cur_v = cur.as_ref().map(|m| m.gi("version")).unwrap_or(0);
            if cur_v != if_version {
                return Err(StoreError::msg(format!("write_value: version conflict (have {cur_v}, expected {if_version})")));
            }
        }
        let ts = now_ms();
        let cur = self.read_row(&rk).await?;
        let next_v = cur.as_ref().map(|m| m.gi("version")).unwrap_or(0) + 1;
        self.write_row(&rk, vec![
            set("ns", sv(namespace)), set("k", sv(key)), set("value", sv(value_json)),
            set("version", iv(next_v)), set("ts", iv(ts)), set("writer", sv(writer)), set("deleted", iv(0)),
        ]).await?;
        Ok(ValueRecord {
            namespace: namespace.into(), key: key.into(), value_json: value_json.into(),
            version: next_v, ts_ms: ts, writer: writer.into(), deleted: false,
        })
    }
    async fn get_value(&self, namespace: &str, key: &str) -> StoreResult<Option<ValueRecord>> {
        Ok(self.read_row(&rk_value(namespace, key)).await?.map(|m| ValueRecord {
            namespace: namespace.into(), key: key.into(), value_json: m.gs("value"),
            version: m.gi("version"), ts_ms: m.gi("ts"), writer: m.gs("writer"),
            deleted: m.gi("deleted") != 0,
        }))
    }
    async fn get_value_if_newer(&self, namespace: &str, key: &str, from_version: i64) -> StoreResult<Option<ValueRecord>> {
        Ok(self.get_value(namespace, key).await?.filter(|r| r.version > from_version))
    }
    async fn delete_value(&self, namespace: &str, key: &str) -> StoreResult<(bool, i64)> {
        let rk = rk_value(namespace, key);
        let cur = self.read_row(&rk).await?;
        if cur.is_none() {
            return Ok((false, 0));
        }
        let next_v = cur.unwrap().gi("version") + 1;
        let ts = now_ms();
        self.write_row(&rk, vec![
            set("value", sv("")), set("version", iv(next_v)),
            set("ts", iv(ts)), set("deleted", iv(1)),
        ]).await?;
        Ok((true, next_v))
    }

    // ── the WAL tail ────────────────────────────────────────────────────────
    async fn events_since(&self, _from_ts_ms: i64, run_id: &str, kind: &str, limit: i64) -> StoreResult<Vec<EventEntry>> {
        // A cross-run, time-ordered tail isn't expressible against Bigtable's
        // row-key layout — that's what Bigtable change streams are for (consume
        // them via Dataflow or the ReadChangeStream API). The per-run feed
        // (SubscribeRun / journal_range) still works.
        if !run_id.is_empty() {
            return Ok(self.journal_range(run_id, 0).await?.into_iter()
                .filter(|j| kind.is_empty() || j.kind == kind)
                .take(limit.max(1) as usize)
                .map(|j| EventEntry {
                    run_id: run_id.into(), seq: j.seq, kind: j.kind,
                    payload_json: j.payload_json, ts_ms: j.ts_ms,
                    global_seq: 0, subject: String::new(), schema_version: 1,
                    trace_id: String::new(), span_id: String::new(), parent_span_id: String::new(),
                })
                .collect());
        }
        tracing::warn!("SubscribeEvents (cross-run) is not supported on the Bigtable backend — use Bigtable change streams (design-principles/tape.md §12); returning empty");
        Ok(vec![])
    }
}

fn rk_timer(run_id: &str, timer_id: &str) -> String { format!("tmr#{run_id}#{timer_id}") }
fn rk_value(namespace: &str, key: &str) -> String { format!("val#{namespace}#{key}") }
