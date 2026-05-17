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
    mutation, row_filter, CheckAndMutateRowRequest, MutateRowRequest, Mutation,
    ReadRowsRequest, RowFilter, RowSet,
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

    /// Conditional write. Predicate matches when any cell satisfies `predicate`;
    /// `true_mutations` apply on match, `false_mutations` on no-match.
    /// Returns whether the predicate matched.
    async fn check_and_mutate(&self, key: &str, predicate: RowFilter,
                              true_mutations: Vec<Mutation>, false_mutations: Vec<Mutation>)
        -> StoreResult<bool> {
        let mut bt = self.bt();
        let resp = bt.check_and_mutate_row(CheckAndMutateRowRequest {
            table_name: self.table.clone(),
            row_key: key.as_bytes().to_vec(),
            predicate_filter: Some(predicate),
            true_mutations, false_mutations,
            ..Default::default()
        }).await.map_err(e)?;
        Ok(resp.predicate_matched)
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
fn rk_business_key(connector: &str, business_key: &str) -> String {
    format!("bk#{connector}#{business_key}")
}

/// RowFilter that matches when the named qualifier has *any* non-empty cell
/// value. Chain of FamilyNameRegex + ColumnQualifierRegex + ValueRegex(".+"),
/// limited to the latest cell. Used by the claim CAS to detect "row already
/// has a claimer".
fn col_has_nonempty(qualifier: &str) -> RowFilter {
    use row_filter::Chain;
    RowFilter {
        filter: Some(row_filter::Filter::Chain(Chain {
            filters: vec![
                RowFilter { filter: Some(row_filter::Filter::FamilyNameRegexFilter(FAM.to_string())) },
                RowFilter { filter: Some(row_filter::Filter::ColumnQualifierRegexFilter(qualifier.as_bytes().to_vec())) },
                RowFilter { filter: Some(row_filter::Filter::CellsPerColumnLimitFilter(1)) },
                RowFilter { filter: Some(row_filter::Filter::ValueRegexFilter(b".+".to_vec())) },
            ],
        })),
    }
}

/// RowFilter that matches when the named qualifier has the *exact* given
/// value. Used by the lease-renewal CAS: predicate succeeds iff the existing
/// expiry equals the one we read — i.e., nobody else moved the lease under us.
fn col_value_eq(qualifier: &str, value: &str) -> RowFilter {
    use row_filter::Chain;
    // Regex literal special chars must be escaped — but the values we use are
    // always digit strings (epoch ms), so no escaping needed.
    RowFilter {
        filter: Some(row_filter::Filter::Chain(Chain {
            filters: vec![
                RowFilter { filter: Some(row_filter::Filter::FamilyNameRegexFilter(FAM.to_string())) },
                RowFilter { filter: Some(row_filter::Filter::ColumnQualifierRegexFilter(qualifier.as_bytes().to_vec())) },
                RowFilter { filter: Some(row_filter::Filter::CellsPerColumnLimitFilter(1)) },
                RowFilter { filter: Some(row_filter::Filter::ValueRegexFilter(value.as_bytes().to_vec())) },
            ],
        })),
    }
}

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
    // Outbox/non-idempotent fields default to UNSPECIFIED/empty on Bigtable —
    // the outbox dispatch RPCs are not implemented for this backend yet (see
    // the stubs below). Reads still surface whatever was written via SQL or
    // via begin_effect's new args (which we persist on this row).
    EffectRecord {
        run_id: m.gs("run"), seq: m.gi("seq"), decision_index: m.gi("decision_index"),
        tool_name: m.gs("tool"), idempotency_key: key.to_string(), status: m.gi("status") as i32,
        request_json: m.gs("request"), response_json: m.gs("response"), error_json: m.gs("error"), ts_ms: m.gi("ts"),
        semantics: m.gi("semantics") as i32,
        dispatch_mode: m.gi("dispatch_mode") as i32,
        business_key: m.gs("business_key"),
        connector: m.gs("connector"),
        dispatch_attempts: m.gi("dispatch_attempts") as i32,
        next_dispatch_at_ms: m.gi("next_dispatch_at_ms"),
        external_ref: m.gs("external_ref"),
        dispatch_claimed_by: m.gs("dispatch_claimed_by"),
        dispatch_claim_expires_at_ms: m.gi("dispatch_claim_expires_at_ms"),
        last_dispatch_error: m.gs("last_dispatch_error"),
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
        compensator_ref: m.gs("compensator_ref"),
        attempts: m.gi("attempts") as i32,
        max_attempts: { let v = m.gi("max_attempts") as i32; if v <= 0 { 5 } else { v } },
        next_attempt_at_ms: m.gi("next_attempt_at_ms"),
        last_error: m.gs("last_error"),
        claimed_by: m.gs("claimed_by"),
        claim_expires_at_ms: m.gi("claim_expires_at_ms"),
        result_json: m.gs("result"),
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
            if seq >= from_seq { Some(JournalEntry { seq, kind: m.gs("kind"), payload_json: m.gs("payload"), ts_ms: m.gi("ts") }) } else { None }
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
    async fn begin_effect(&self, run_id: &str, decision_index: i64, tool_name: &str, call_index: i32,
                          request_json: &str, custom_key: &str,
                          semantics: i32, dispatch_mode: i32,
                          business_key: &str, connector: &str) -> StoreResult<EffectRecord> {
        let sem = if semantics == EffectSemantics::Unspecified as i32 {
            EffectSemantics::Idempotent as i32
        } else { semantics };
        let dmode = if dispatch_mode == EffectDispatchMode::Unspecified as i32 {
            EffectDispatchMode::Inline as i32
        } else { dispatch_mode };
        if sem == EffectSemantics::NonIdempotent as i32 && dmode == EffectDispatchMode::Inline as i32 {
            return Err(StoreError::msg(
                "begin_effect: NON_IDEMPOTENT semantics requires OUTBOX dispatch"));
        }
        let key = if custom_key.is_empty() { derive_key(run_id, decision_index, tool_name, call_index) } else { custom_key.to_string() };
        if let Some(m) = self.read_row(&rk_effect(&key)).await? {
            return Ok(effect_from(&key, &m));
        }
        // Business-key dedupe across runs. On the SQL backend this is a partial
        // UNIQUE index that the database enforces atomically; on Bigtable we
        // approximate with a CheckAndMutate-style claim on the bk# row. Race:
        // two concurrent first-writers could both observe an empty bk row;
        // CheckAndMutate makes only one succeed at writing it. The loser
        // re-reads and returns the canonical effect.
        if !business_key.is_empty() && !connector.is_empty() {
            let bk_row = rk_business_key(connector, business_key);
            if let Some(m) = self.read_row(&bk_row).await? {
                let pointer = m.gs("effect_key");
                if !pointer.is_empty() {
                    if let Some(em) = self.read_row(&rk_effect(&pointer)).await? {
                        return Ok(effect_from(&pointer, &em));
                    }
                }
            }
            // Try to claim the bk row for our key.
            let won = self.check_and_mutate(
                &bk_row,
                col_has_nonempty("effect_key"),
                vec![],  // someone else already claimed; no mutations
                vec![set("effect_key", sv(&key)), set("ts", iv(now_ms()))],
            ).await?;
            if won {
                // Predicate matched => bk row was already populated by a racer.
                // Read it and return that effect instead of creating ours.
                if let Some(m) = self.read_row(&bk_row).await? {
                    let pointer = m.gs("effect_key");
                    if !pointer.is_empty() {
                        if let Some(em) = self.read_row(&rk_effect(&pointer)).await? {
                            return Ok(effect_from(&pointer, &em));
                        }
                    }
                }
            }
        }
        let ts = now_ms();
        let seq = self.next_seq(run_id).await?;
        let next_dispatch = if dmode == EffectDispatchMode::Outbox as i32 { ts } else { 0 };
        // intent first — Bigtable single-row writes are atomic & durable; the tool body runs only after this returns.
        self.write_row(&rk_effect(&key), vec![
            set("run", sv(run_id)), set("seq", iv(seq)), set("decision_index", iv(decision_index)), set("tool", sv(tool_name)),
            set("status", iv(EffectStatus::Pending as i64)), set("request", sv(request_json)), set("ts", iv(ts)),
            set("semantics", iv(sem as i64)), set("dispatch_mode", iv(dmode as i64)),
            set("business_key", sv(business_key)), set("connector", sv(connector)),
            set("next_dispatch_at_ms", iv(next_dispatch)),
        ]).await?;
        self.journal(run_id, seq, "effect",
            &serde_json::json!({"tool": tool_name, "decision_index": decision_index,
                                "idempotency_key": key, "status": "pending",
                                "semantics": sem, "dispatch_mode": dmode,
                                "business_key": business_key, "connector": connector}).to_string(), ts).await?;
        Ok(EffectRecord {
            run_id: run_id.into(), seq, decision_index, tool_name: tool_name.into(),
            idempotency_key: key, status: EffectStatus::Pending as i32,
            request_json: request_json.into(), response_json: String::new(),
            error_json: String::new(), ts_ms: ts,
            semantics: sem, dispatch_mode: dmode,
            business_key: business_key.into(), connector: connector.into(),
            dispatch_attempts: 0, next_dispatch_at_ms: next_dispatch,
            external_ref: String::new(), dispatch_claimed_by: String::new(),
            dispatch_claim_expires_at_ms: 0, last_dispatch_error: String::new(),
        })
    }

    // ── outbox dispatch on Bigtable ─────────────────────────────────────────
    //
    // No SQL → no indexed scans, no UPDATE…WHERE for CAS. We do:
    //
    //   list_effects_to_dispatch  → full scan of `e#` rows, filter in memory.
    //                               Linear in total effects; fine up to a few
    //                               thousand outstanding outbox rows. For
    //                               very large scales, add a `obx#` secondary
    //                               index row at begin_effect time and scan
    //                               `obx#` by row-key range instead (the
    //                               existing journal pattern uses this trick).
    //
    //   claim_effect_dispatch     → CheckAndMutate on the effect row, twice:
    //                                 (a) when unclaimed: predicate "claimer
    //                                     non-empty" → take false_mutations
    //                                     (claim); racer who arrives first
    //                                     causes predicate to match, we lose.
    //                                 (b) when claimed but expired: predicate
    //                                     "expires == the old value we read"
    //                                     → true_mutations (renew with our
    //                                     claimer); concurrent reclaim makes
    //                                     the predicate fail, we lose.
    //                               Both branches are atomic single-row CAS.
    //
    //   record_dispatch_attempt    → plain write of the new status + counters,
    //                                because the *lease* serialises writers;
    //                                no CAS needed inside it.
    //
    //   record_external_observation→ same write pattern, with the
    //                                EffectResolution → EffectStatus mapping
    //                                done in code, and a compensation
    //                                obligation registered via the existing
    //                                register_compensation when DUPLICATE.

    async fn list_effects_to_dispatch(&self, now_ms: i64, connector: &str, limit: i64) -> StoreResult<Vec<EffectRecord>> {
        let now = if now_ms > 0 { now_ms } else { super::now_ms() };
        let cap = if limit > 0 { limit } else { 200 };
        // Scan is bounded by cap * 32 to give the in-memory filter some
        // headroom — most non-matching rows are still cheap. At very large
        // scales prefer the secondary-index approach.
        let rows = self.read_prefix("e#", cap.saturating_mul(32)).await?;
        let pending = EffectStatus::Pending as i32;
        let outbox = EffectDispatchMode::Outbox as i32;
        let mut out: Vec<EffectRecord> = Vec::new();
        for (k, m) in rows {
            let key = k.trim_start_matches("e#");
            let er = effect_from(key, &m);
            if er.status != pending || er.dispatch_mode != outbox {
                continue;
            }
            if er.next_dispatch_at_ms > now {
                continue;
            }
            let lease_open = er.dispatch_claimed_by.is_empty()
                || (er.dispatch_claim_expires_at_ms > 0 && er.dispatch_claim_expires_at_ms <= now);
            if !lease_open {
                continue;
            }
            if !connector.is_empty() && er.connector != connector {
                continue;
            }
            out.push(er);
            if out.len() as i64 >= cap {
                break;
            }
        }
        // Oldest-due first — fairness across runs.
        out.sort_by(|a, b| a.next_dispatch_at_ms.cmp(&b.next_dispatch_at_ms)
            .then(a.ts_ms.cmp(&b.ts_ms)));
        if out.len() as i64 > cap { out.truncate(cap as usize); }
        Ok(out)
    }

    async fn claim_effect_dispatch(&self, run_id: &str, key: &str, claimer: &str,
                                   lease_ttl_ms: i64, now_ms: i64)
        -> StoreResult<(bool, Option<EffectRecord>)> {
        let _ = run_id;
        let Some(existing) = self.get_effect("", key).await? else {
            return Ok((false, None));
        };
        let now = if now_ms > 0 { now_ms } else { super::now_ms() };
        let ttl = if lease_ttl_ms > 0 { lease_ttl_ms } else { 60_000 };
        let new_exp = now + ttl;
        // Pre-checks; the server also enforces these via the CAS predicate,
        // but we filter up-front to keep the round-trip count down.
        if existing.status != EffectStatus::Pending as i32
            || existing.dispatch_mode != EffectDispatchMode::Outbox as i32
            || existing.next_dispatch_at_ms > now
        {
            return Ok((false, Some(existing)));
        }
        let unclaimed = existing.dispatch_claimed_by.is_empty();
        let expired = !unclaimed
            && existing.dispatch_claim_expires_at_ms > 0
            && existing.dispatch_claim_expires_at_ms <= now;
        if !unclaimed && !expired {
            return Ok((false, Some(existing)));
        }
        let muts = vec![
            set("dispatch_claimed_by", sv(claimer)),
            set("dispatch_claim_expires_at_ms", iv(new_exp)),
            set("ts", iv(now)),
        ];
        let predicate_matched = if unclaimed {
            // predicate matches iff someone else has already taken the claim
            // → we lose. No-match (predicate_matched=false) → we win.
            self.check_and_mutate(&rk_effect(key),
                                  col_has_nonempty("dispatch_claimed_by"),
                                  vec![],
                                  muts.clone()).await?
        } else {
            // predicate matches iff the existing expiry equals the value we
            // read (i.e., no one else has renewed since). On match → renew.
            let expected = existing.dispatch_claim_expires_at_ms.to_string();
            !self.check_and_mutate(&rk_effect(key),
                                   col_value_eq("dispatch_claim_expires_at_ms", &expected),
                                   muts.clone(),
                                   vec![]).await?
            // Logic note: check_and_mutate returns `true` on predicate match;
            // for the renew case "match" means our true_mutations ran (= we
            // won), so we want the *opposite* of `predicate_matched` here for
            // the "racer won" branch below.
        };
        let acquired = if unclaimed { !predicate_matched } else { !predicate_matched };
        let updated = self.get_effect("", key).await?;
        if acquired {
            // Journal the lease event for parity with the SQL backend.
            let seq = self.next_seq(run_id).await?;
            self.journal(run_id, seq, "effect",
                &serde_json::json!({"tool": existing.tool_name, "idempotency_key": key,
                                     "status": "pending", "transition": "dispatch-claimed",
                                     "claimer": claimer}).to_string(), now).await?;
        }
        Ok((acquired, updated))
    }

    async fn record_dispatch_attempt(&self, run_id: &str, key: &str,
                                     error: &str, next_dispatch_at_ms: i64)
        -> StoreResult<Option<EffectRecord>> {
        let Some(existing) = self.get_effect("", key).await? else { return Ok(None); };
        let now = now_ms();
        let new_attempts = existing.dispatch_attempts + 1;
        let to_unknown = next_dispatch_at_ms <= 0;
        let new_status = if to_unknown {
            EffectStatus::Unknown as i32
        } else {
            EffectStatus::Pending as i32
        };
        let next_at = if to_unknown { 0_i64 } else { next_dispatch_at_ms };
        // Clear the lease columns by writing empties (Bigtable doesn't have
        // "drop cell"-by-default in our set helper; an empty string value
        // matches the "lease open" predicate, so this is safe.)
        self.write_row(&rk_effect(key), vec![
            set("status", iv(new_status as i64)),
            set("dispatch_attempts", iv(new_attempts as i64)),
            set("last_dispatch_error", sv(error)),
            set("next_dispatch_at_ms", iv(next_at)),
            set("dispatch_claimed_by", sv("")),
            set("dispatch_claim_expires_at_ms", iv(0)),
            set("ts", iv(now)),
        ]).await?;
        let seq = self.next_seq(run_id).await?;
        let transition = if to_unknown { "dispatch-unknown" } else { "dispatch-retry-scheduled" };
        let status_label = if to_unknown { "unknown" } else { "pending" };
        self.journal(run_id, seq, "effect",
            &serde_json::json!({
                "tool": existing.tool_name, "idempotency_key": key,
                "status": status_label, "transition": transition,
                "dispatch_attempts": new_attempts, "error": error,
                "next_dispatch_at_ms": next_at,
            }).to_string(), now).await?;
        Ok(self.get_effect("", key).await?)
    }

    async fn record_external_observation(&self, run_id: &str, key: &str,
                                         resolution: i32, external_ref: &str,
                                         response_json: &str, error_json: &str,
                                         compensate_on_duplicate_kind: &str)
        -> StoreResult<Option<EffectRecord>> {
        let Some(existing) = self.get_effect("", key).await? else { return Ok(None); };
        // Same mapping as the SQL backend.
        let target_status = match resolution {
            r if r == EffectResolution::Confirmed as i32 => EffectStatus::Confirmed as i32,
            r if r == EffectResolution::Failed as i32 => EffectStatus::Failed as i32,
            r if r == EffectResolution::Duplicate as i32 => EffectStatus::Confirmed as i32,
            r if r == EffectResolution::Absent as i32 => {
                if existing.semantics == EffectSemantics::Idempotent as i32 {
                    EffectStatus::Pending as i32
                } else {
                    EffectStatus::Failed as i32
                }
            }
            r if r == EffectResolution::Stuck as i32 => EffectStatus::Unknown as i32,
            _ => existing.status,
        };
        let now = now_ms();
        let next_dispatch = if resolution == EffectResolution::Absent as i32
            && existing.semantics == EffectSemantics::Idempotent as i32
        { now } else { existing.next_dispatch_at_ms };
        self.write_row(&rk_effect(key), vec![
            set("status", iv(target_status as i64)),
            set("response", sv(response_json)),
            set("error", sv(error_json)),
            set("external_ref", sv(external_ref)),
            set("next_dispatch_at_ms", iv(next_dispatch)),
            set("dispatch_claimed_by", sv("")),
            set("dispatch_claim_expires_at_ms", iv(0)),
            set("ts", iv(now)),
        ]).await?;
        let seq = self.next_seq(run_id).await?;
        let label = match resolution {
            r if r == EffectResolution::Confirmed as i32 => "observed-confirmed",
            r if r == EffectResolution::Failed as i32 => "observed-failed",
            r if r == EffectResolution::Absent as i32 => "observed-absent",
            r if r == EffectResolution::Duplicate as i32 => "observed-duplicate",
            r if r == EffectResolution::Stuck as i32 => "observed-stuck",
            _ => "observed-unspecified",
        };
        self.journal(run_id, seq, "effect",
            &serde_json::json!({
                "tool": existing.tool_name, "idempotency_key": key,
                "status": "observed", "transition": label,
                "external_ref": external_ref,
            }).to_string(), now).await?;
        if resolution == EffectResolution::Duplicate as i32 && !compensate_on_duplicate_kind.is_empty() {
            let payload = serde_json::json!({
                "reason": "duplicate-observed",
                "external_ref": external_ref,
                "idempotency_key": key,
            }).to_string();
            let _ = self.register_compensation(run_id, key, compensate_on_duplicate_kind,
                                               &payload, "", 0).await?;
        }
        Ok(self.get_effect("", key).await?)
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
    //
    // Note on CAS: the SQL store uses `UPDATE … WHERE` for the claim CAS. Bigtable
    // does not have cross-row transactions, and the data-plane client crate used
    // here does not expose `CheckAndMutateRow`, so the claim is implemented as
    // read-then-write. Two drainers racing on the same obligation can both
    // observe a claimable row and both write; whichever write lands second owns
    // the lease, which is the usual Bigtable single-row last-writer-wins. The
    // lease boundary then re-syncs the next time a claim is attempted. For
    // strict CAS, run the SQL backend.
    async fn register_compensation(&self, run_id: &str, effect_key: &str, kind: &str,
                                   payload_json: &str, compensator_ref: &str,
                                   max_attempts: i32) -> StoreResult<ObligationRecord> {
        for (key, m) in self.read_prefix(&format!("o#{run_id}#"), 100_000).await? {
            if m.gs("effect_key") == effect_key && m.gs("kind") == kind {
                let seq: i64 = key.rsplit('#').next().and_then(|s| s.parse().ok()).unwrap_or(0);
                return Ok(obligation_from(run_id, seq, &m));
            }
        }
        let ts = now_ms();
        let seq = self.next_seq(run_id).await?;
        let status = ObligationStatus::Pending as i64;
        let max_att = if max_attempts <= 0 { 5 } else { max_attempts };
        self.write_row(&rk_obligation(run_id, seq), vec![
            set("effect_key", sv(effect_key)), set("kind", sv(kind)), set("payload", sv(payload_json)),
            set("status", iv(status)), set("ts", iv(ts)),
            set("compensator_ref", sv(compensator_ref)),
            set("attempts", iv(0)), set("max_attempts", iv(max_att as i64)),
            set("next_attempt_at_ms", iv(ts)), set("last_error", sv("")),
            set("claimed_by", sv("")), set("claim_expires_at_ms", iv(0)),
            set("result", sv("")),
        ]).await?;
        self.journal(run_id, seq, "obligation",
            &serde_json::json!({"effect_key": effect_key, "kind": kind, "status": "pending", "transition": "registered"}).to_string(),
            ts).await?;
        Ok(ObligationRecord {
            run_id: run_id.into(), seq, effect_key: effect_key.into(), kind: kind.into(),
            payload_json: payload_json.into(), status: status as i32, ts_ms: ts,
            compensator_ref: compensator_ref.into(), attempts: 0, max_attempts: max_att,
            next_attempt_at_ms: ts, last_error: String::new(),
            claimed_by: String::new(), claim_expires_at_ms: 0, result_json: String::new(),
        })
    }
    async fn list_obligations(&self, run_id: &str, only_unresolved: bool, status_filter: i32) -> StoreResult<Vec<ObligationRecord>> {
        let mut out: Vec<ObligationRecord> = self.read_prefix(&format!("o#{run_id}#"), 100_000).await?.into_iter().filter_map(|(key, m)| {
            let seq: i64 = key.rsplit('#').next().and_then(|s| s.parse().ok())?;
            let st = m.gi("status") as i32;
            if status_filter > 0 {
                if st != status_filter { return None; }
            } else if only_unresolved && (st == ObligationStatus::Compensated as i32 || st == ObligationStatus::Stuck as i32) {
                return None;
            }
            Some(obligation_from(run_id, seq, &m))
        }).collect();
        out.sort_by(|a, b| b.seq.cmp(&a.seq)); // newest-first (LIFO)
        Ok(out)
    }
    async fn list_unresolved_obligations(&self, now_ms_in: i64, include_pending: bool,
                                         include_stuck: bool, include_committed_expired: bool,
                                         limit: i64) -> StoreResult<Vec<ObligationRecord>> {
        // Bigtable has no secondary indexes; this scans all `o#` rows. Fine at
        // moderate scale; at very high scale, drive the drainer from Bigtable
        // change streams (the obligation-state journal entries) instead.
        let now = if now_ms_in > 0 { now_ms_in } else { now_ms() };
        let lim = if limit > 0 { limit as usize } else { 500 };
        let mut out: Vec<ObligationRecord> = Vec::new();
        for (key, m) in self.read_prefix("o#", 100_000).await? {
            let parts: Vec<&str> = key.splitn(3, '#').collect();
            if parts.len() < 3 { continue; }
            let run = parts[1];
            let seq: i64 = parts[2].parse().unwrap_or(0);
            let st = m.gi("status") as i32;
            let next_at = m.gi("next_attempt_at_ms");
            let claim_exp = m.gi("claim_expires_at_ms");
            let keep = (include_pending && st == ObligationStatus::Pending as i32 && next_at <= now)
                   || (include_committed_expired && st == ObligationStatus::Committed as i32 && claim_exp > 0 && claim_exp <= now)
                   || (include_stuck && st == ObligationStatus::Stuck as i32);
            if keep {
                out.push(obligation_from(run, seq, &m));
            }
        }
        // oldest-first by next_attempt_at_ms, then ts — same order as the SQL impl.
        out.sort_by(|a, b| a.next_attempt_at_ms.cmp(&b.next_attempt_at_ms).then(a.ts_ms.cmp(&b.ts_ms)));
        out.truncate(lim);
        Ok(out)
    }
    async fn claim_obligation(&self, run_id: &str, obligation_seq: i64, claimer: &str,
                              lease_ttl_ms: i64, now_ms_in: i64)
        -> StoreResult<(bool, Option<ObligationRecord>)> {
        let key = rk_obligation(run_id, obligation_seq);
        let Some(m) = self.read_row(&key).await? else { return Ok((false, None)); };
        let existing = obligation_from(run_id, obligation_seq, &m);
        let now = if now_ms_in > 0 { now_ms_in } else { now_ms() };
        let ttl = if lease_ttl_ms > 0 { lease_ttl_ms } else { 60_000 };
        let claimable = (existing.status == ObligationStatus::Pending as i32 && existing.next_attempt_at_ms <= now)
                     || (existing.status == ObligationStatus::Committed as i32
                         && existing.claim_expires_at_ms > 0 && existing.claim_expires_at_ms <= now);
        if !claimable {
            return Ok((false, Some(existing)));
        }
        let lease_exp = now + ttl;
        self.write_row(&key, vec![
            set("status", iv(ObligationStatus::Committed as i64)),
            set("claimed_by", sv(claimer)),
            set("claim_expires_at_ms", iv(lease_exp)),
            set("ts", iv(now)),
        ]).await?;
        let jseq = self.next_seq(run_id).await?;
        self.journal(run_id, jseq, "obligation",
            &serde_json::json!({"obligation_seq": existing.seq, "effect_key": existing.effect_key, "kind": existing.kind, "status": "committed", "transition": "claimed", "claimer": claimer}).to_string(),
            now).await?;
        let updated = self.read_row(&key).await?.map(|m| obligation_from(run_id, obligation_seq, &m));
        Ok((true, updated))
    }
    async fn record_obligation_attempt(&self, run_id: &str, obligation_seq: i64,
                                       error: &str, next_attempt_at_ms: i64)
        -> StoreResult<Option<ObligationRecord>> {
        let key = rk_obligation(run_id, obligation_seq);
        let Some(m) = self.read_row(&key).await? else { return Ok(None); };
        let existing = obligation_from(run_id, obligation_seq, &m);
        let now = now_ms();
        let new_attempts = existing.attempts + 1;
        let terminal = next_attempt_at_ms <= 0 || new_attempts >= existing.max_attempts;
        let (new_status, next_at) = if terminal {
            (ObligationStatus::Stuck as i64, 0_i64)
        } else {
            (ObligationStatus::Pending as i64, next_attempt_at_ms)
        };
        self.write_row(&key, vec![
            set("status", iv(new_status)),
            set("attempts", iv(new_attempts as i64)),
            set("last_error", sv(error)),
            set("next_attempt_at_ms", iv(next_at)),
            set("claimed_by", sv("")),
            set("claim_expires_at_ms", iv(0)),
            set("ts", iv(now)),
        ]).await?;
        let transition = if terminal { "stuck" } else { "retry-scheduled" };
        let label = if terminal { "stuck" } else { "pending" };
        let jseq = self.next_seq(run_id).await?;
        self.journal(run_id, jseq, "obligation",
            &serde_json::json!({"obligation_seq": existing.seq, "effect_key": existing.effect_key, "kind": existing.kind,
                                "status": label, "transition": transition,
                                "attempts": new_attempts, "error": error,
                                "next_attempt_at_ms": next_at}).to_string(),
            now).await?;
        Ok(self.read_row(&key).await?.map(|m| obligation_from(run_id, obligation_seq, &m)))
    }
    async fn resolve_obligation(&self, run_id: &str, obligation_seq: i64, status: i32, result_json: &str) -> StoreResult<Option<ObligationRecord>> {
        let key = rk_obligation(run_id, obligation_seq);
        let Some(m) = self.read_row(&key).await? else { return Ok(None); };
        let existing = obligation_from(run_id, obligation_seq, &m);
        let target = if status == ObligationStatus::Compensated as i32 || status == ObligationStatus::Stuck as i32 {
            status
        } else {
            ObligationStatus::Compensated as i32
        };
        let now = now_ms();
        self.write_row(&key, vec![
            set("status", iv(target as i64)),
            set("result", sv(result_json)),
            set("claimed_by", sv("")),
            set("claim_expires_at_ms", iv(0)),
            set("ts", iv(now)),
        ]).await?;
        let label = if target == ObligationStatus::Compensated as i32 { "compensated" } else { "stuck" };
        let jseq = self.next_seq(run_id).await?;
        self.journal(run_id, jseq, "obligation",
            &serde_json::json!({"obligation_seq": existing.seq, "effect_key": existing.effect_key, "kind": existing.kind,
                                "status": label, "transition": "resolved"}).to_string(),
            now).await?;
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
                .map(|j| EventEntry { run_id: run_id.into(), seq: j.seq, kind: j.kind, payload_json: j.payload_json, ts_ms: j.ts_ms })
                .collect());
        }
        tracing::warn!("SubscribeEvents (cross-run) is not supported on the Bigtable backend — use Bigtable change streams (design-principles/tape.md §12); returning empty");
        Ok(vec![])
    }
}

fn rk_timer(run_id: &str, timer_id: &str) -> String { format!("tmr#{run_id}#{timer_id}") }
fn rk_value(namespace: &str, key: &str) -> String { format!("val#{namespace}#{key}") }
