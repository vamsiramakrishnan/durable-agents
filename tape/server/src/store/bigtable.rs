//! The Cloud Bigtable implementation of [`RunStore`].
//!
//! Bigtable is a wide-column store — no SQL, no cross-row transactions — but it
//! has single-row atomic mutations, `CheckAndMutateRow` (the predicate-and-write
//! primitive) and `ReadModifyWriteRow` (the atomic counter primitive), which is
//! enough to back the journal *and* the event-bus surface (reactions, tasks,
//! per-shard cursors, by-global-seq journal index). One column family, `m`. Row
//! keys (all qualifiers live in family `m`; every value is a UTF-8 string unless
//! noted):
//!
//!   r#<run_id>                           run row: status, app, user, session, inv,
//!                                        lease_owner, lease_exp, waiting_gate, detail,
//!                                        started, ended, seq
//!   idx#<app>#<user>#<session>#<inv>     reverse index used by begin_run
//!   route#<app>#<user>#<session>         newest run for a session (send_signal)
//!   d#<run_id>#<decision_index:020>      decision row
//!   e#<idempotency_key>                  effect row
//!   o#<run_id>#<seq:020>                 obligation row
//!   b#<run_id>                           budget row
//!   sig#<run_id>#<gate>                  gate-signal row
//!   sess#<app>#<user>#<session>          session row
//!   ev#<app>#<user>#<session>#<ord:020>  session-event row
//!   tmr#<run_id>#<timer_id>              timer row
//!   val#<namespace>#<key>                value row
//!   j#<run_id>#<seq:020>                 per-run journal row (for SubscribeRun, journal_range)
//!
//! ── event-bus tables (design-principles/tape-event-bus.md §6.3) ─────────────
//!
//!   meta#global_seq                      counter row, column `v` (8-byte big-endian
//!                                        i64), bumped via `ReadModifyWriteRow.IncrementAmount(1)`.
//!                                        The new value is the entry's `global_seq`.
//!   jg#<global_seq:020>                  by-global-seq journal index row: run_id, seq,
//!                                        kind, subject, payload, schema_version, trace_id,
//!                                        span_id, parent_span_id, ts. Powers the matcher
//!                                        and `SubscribeBySubject` (read prefix `jg#`,
//!                                        ordered by row key = ordered by global_seq).
//!                                        Disambiguated from per-run `j#…` so a prefix
//!                                        scan of one doesn't pull in the other.
//!   react#<reaction_id>                  reaction row: name, subject_pattern, predicate_cel,
//!                                        handler_kind, agent_app, publish_target, the
//!                                        backpressure knobs, created_at_ms, deleted.
//!   cursor#<reaction_id>#<shard:010>     per-(reaction, shard) cursor row: last_global_seq,
//!                                        last_processed_at_ms.
//!   task#<task_id>                       task row (PK = task_id, opaque uuid).
//!   taskidx#<reaction_id>#<shard:010>#<source_global_seq:020>
//!                                        UNIQUE-constraint index row used by create_task:
//!                                        existence ⇒ a duplicate matcher hit, skip the
//!                                        insert. Stores `task_id` (=> dedup → return existing).
//!   pending#<reaction_id>#<shard:010>#<task_id>
//!                                        pending-task tracking index: written when the
//!                                        task is created/re-pended, removed when
//!                                        the task reaches DONE/DLQ. Speeds up
//!                                        `claim_tasks` and `find_pending_task_for_subject`
//!                                        by scoping the scan to one reaction; otherwise
//!                                        Bigtable would scan every task row in the table.
//!
//! ── constraints we hit, and how we work around them ─────────────────────────
//! * No cross-row transactions. The per-run `seq` is held on `r#<run_id>` and
//!   bumped read-then-write; the lease guarantees a single writer per run so
//!   this is safe. Cross-run global_seq uses a dedicated counter row.
//! * No SQL, no secondary indexes. Listing tasks for a reaction means scanning
//!   `task#`. We add a `pending#<reaction>#…` index that the writer maintains so
//!   the hot path (`claim_tasks`) is O(pending), not O(every task ever).
//! * No SELECT … FOR UPDATE. Lease-stealing in `claim_tasks` uses
//!   `CheckAndMutateRow` predicated on (status=PENDING with next_attempt_at_ms<=now)
//!   OR (status=CLAIMED with lease_expires_at_ms<now). Races between two
//!   dispatchers are decided by the server.
//!
//! `list_runs_to_recover` scans `r#` rows and filters in memory — fine at
//! moderate scale; at very high scale you'd drive recovery from the
//! Bigtable-change-streams → Pub/Sub reactor (design-principles/tape.md §12)
//! rather than polling. `AppendEvent` is two writes (the session row, then the
//! event row) — Bigtable has no cross-row transactions; a crash between leaves
//! the state applied without its event, which the re-drive re-creates idempotently.
//!
//! The matcher tails the `jg#` prefix every ~1 s; that's the `journal_notify()`
//! default. A push-driven matcher (Bigtable change streams) is in
//! `bigtable_change_stream`; see that file's docstring for emulator caveats.
//!
//! The table and its column family `m` must exist before the server starts
//! (Bigtable needs explicit table creation, like creating a Postgres database):
//!
//!   cbt -project P -instance I createtable tape
//!   cbt -project P -instance I createfamily tape m maxversions=1
//!
//! (Optional — for the change-stream matcher path:)
//!
//!   cbt -project P -instance I updatetable tape changeStreamRetention=1d
//!
//! With `BIGTABLE_EMULATOR_HOST=localhost:PORT` set, `bigtable://demo/demo/tape`
//! talks to the local emulator (the same `cbt` commands create the table there).
//! The emulator currently does *not* support change-streams; the matcher
//! falls back to polling there. See `bigtable_change_stream.rs`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bigtable_rs::bigtable::{BigTable, BigTableConnection, RowCell};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    mutation, read_modify_write_rule, row_filter, row_range, CheckAndMutateRowRequest,
    MutateRowRequest, Mutation, ReadModifyWriteRowRequest, ReadModifyWriteRule, ReadRowsRequest,
    RowFilter, RowRange, RowSet,
};

use super::{derive_key, merge_json, now_ms, RunIdentity, RunStore, StoreError, StoreResult};
use crate::pb::*;
use crate::subjects;

const FAM: &str = "m";

fn e<E: std::fmt::Display>(err: E) -> StoreError {
    StoreError::Msg(format!("bigtable: {err}"))
}

// ── value encoding (every cell is a UTF-8 string unless noted) ──────────────
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
    /// In-process wake-up hook for the matcher / SubscribeBySubject stream.
    /// Pulsed on every journal write; also pulsed by the optional change-stream
    /// watcher (see `bigtable_change_stream`). Cloning is cheap.
    notify: Arc<tokio::sync::Notify>,
}

impl BigtableRunStore {
    pub async fn connect(project: &str, instance: &str, table: &str) -> StoreResult<Self> {
        let conn = BigTableConnection::new(project, instance, false, 4, Some(Duration::from_secs(30)))
            .await
            .map_err(e)?;
        let full = conn.client().get_full_table_name(table);
        let s = Self { conn, table: full, notify: Arc::new(tokio::sync::Notify::new()) };
        // Probe — fails loudly (with a fix-it message) if the table/family is missing.
        s.read_row("__tape_probe__").await.map_err(|err| StoreError::msg(format!(
            "{err} — does the Bigtable table exist? create it once: \
             `cbt -project {project} -instance {instance} createtable {table}` then \
             `cbt -project {project} -instance {instance} createfamily {table} m maxversions=1`")))?;
        Ok(s)
    }

    pub fn bt(&self) -> BigTable { self.conn.client() }
    pub fn table_name(&self) -> &str { &self.table }
    pub fn notify_handle(&self) -> Arc<tokio::sync::Notify> { self.notify.clone() }

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

    /// Read every row in [`start_key`, `end_key_exclusive`) — used to scan a
    /// suffix of the `jg#` index past a given global_seq.
    async fn read_range(&self, start_key: &str, end_key_exclusive: Option<&str>, limit: i64)
        -> StoreResult<Vec<(String, RowMap)>>
    {
        let mut bt = self.bt();
        let row_range = RowRange {
            start_key: Some(row_range::StartKey::StartKeyClosed(start_key.as_bytes().to_vec())),
            end_key: end_key_exclusive
                .map(|k| row_range::EndKey::EndKeyOpen(k.as_bytes().to_vec())),
        };
        let req = ReadRowsRequest {
            table_name: self.table.clone(),
            filter: latest_only(),
            rows_limit: limit.max(0),
            rows: Some(RowSet { row_keys: vec![], row_ranges: vec![row_range] }),
            ..Default::default()
        };
        let rows = bt.read_rows(req).await.map_err(e)?;
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

    /// Allocate the next `global_seq` via `ReadModifyWriteRow` on
    /// `meta#global_seq`, column `v` (8-byte big-endian signed integer). The
    /// RPC is atomic and returns the new value — collisions across replicas
    /// are impossible. The 8-byte big-endian encoding is what
    /// `ReadModifyWriteRule.IncrementAmount` requires; the rest of the schema
    /// uses UTF-8 strings, so this column is the one big-endian exception.
    async fn next_global_seq(&self) -> StoreResult<i64> {
        let mut bt = self.bt();
        let req = ReadModifyWriteRowRequest {
            table_name: self.table.clone(),
            row_key: rk_meta_global_seq().into_bytes(),
            rules: vec![ReadModifyWriteRule {
                family_name: FAM.to_string(),
                column_qualifier: b"v".to_vec(),
                rule: Some(read_modify_write_rule::Rule::IncrementAmount(1)),
            }],
            ..Default::default()
        };
        let resp = bt
            .get_client()
            .read_modify_write_row(req)
            .await
            .map_err(e)?
            .into_inner();
        // Find the cell we just bumped and decode its 8-byte big-endian value.
        if let Some(row) = resp.row {
            for fam in &row.families {
                if fam.name == FAM {
                    for col in &fam.columns {
                        if col.qualifier.as_slice() == b"v" {
                            if let Some(cell) = col.cells.first() {
                                if cell.value.len() >= 8 {
                                    let mut buf = [0u8; 8];
                                    buf.copy_from_slice(&cell.value[..8]);
                                    return Ok(i64::from_be_bytes(buf));
                                }
                            }
                        }
                    }
                }
            }
        }
        Err(StoreError::msg("bigtable: read_modify_write_row(global_seq) returned no value"))
    }

    /// Append both journal rows:
    ///   * per-run `j#<run_id>#<seq:020>` (for SubscribeRun / journal_range)
    ///   * by-global-seq `jg#<global_seq:020>` (for the matcher and
    ///     SubscribeBySubject — the event-bus surface)
    /// Computes the subject via `subjects::derive` from `kind` + parsed payload.
    /// OTel fields default to empty for now (the RPC layer doesn't propagate
    /// them on the Bigtable path yet — same as the SQL path).
    async fn journal(&self, run_id: &str, seq: i64, kind: &str, payload: &str, ts: i64) -> StoreResult<()> {
        // Inject run_id into the parsed payload so subjects::derive can find it.
        let payload_v: serde_json::Value = serde_json::from_str(payload).unwrap_or(serde_json::Value::Null);
        let mut p = payload_v;
        if let Some(o) = p.as_object_mut() {
            if !o.contains_key("run_id") && !run_id.is_empty() {
                o.insert("run_id".to_string(), serde_json::Value::String(run_id.to_string()));
            }
        }
        let subject = subjects::derive(kind, &p);
        self.journal_full(run_id, seq, kind, &subject, payload, ts, 1, "", "", "").await
    }

    async fn journal_full(
        &self,
        run_id: &str,
        seq: i64,
        kind: &str,
        subject: &str,
        payload: &str,
        ts: i64,
        schema_version: i32,
        trace_id: &str,
        span_id: &str,
        parent_span_id: &str,
    ) -> StoreResult<()> {
        let global_seq = self.next_global_seq().await?;
        // per-run row (back-compat shape)
        if !run_id.is_empty() {
            self.write_row(
                &rk_journal(run_id, seq),
                vec![
                    set("kind", sv(kind)),
                    set("payload", sv(payload)),
                    set("ts", iv(ts)),
                    set("global_seq", iv(global_seq)),
                    set("subject", sv(subject)),
                    set("schema_version", iv(schema_version as i64)),
                    set("trace_id", sv(trace_id)),
                    set("span_id", sv(span_id)),
                    set("parent_span_id", sv(parent_span_id)),
                ],
            )
            .await?;
        }
        // by-global-seq row (the matcher's index)
        self.write_row(
            &rk_journal_gs(global_seq),
            vec![
                set("run_id", sv(run_id)),
                set("seq", iv(seq)),
                set("kind", sv(kind)),
                set("subject", sv(subject)),
                set("payload", sv(payload)),
                set("schema_version", iv(schema_version as i64)),
                set("trace_id", sv(trace_id)),
                set("span_id", sv(span_id)),
                set("parent_span_id", sv(parent_span_id)),
                set("ts", iv(ts)),
            ],
        )
        .await?;
        // Wake the in-process matcher / SubscribeBySubject stream. The push-driven
        // change-stream watcher (if enabled) also pulses this; both paths are
        // additive — extra wake-ups just mean an immediate poll.
        self.notify.notify_waiters();
        Ok(())
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

// ── outbox / non-idempotent contract (Phase L) ─────────────────────────────
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

// ── event-bus (Phase K) ────────────────────────────────────────────────────
fn rk_journal_gs(global_seq: i64) -> String { format!("jg#{global_seq:020}") }
fn rk_meta_global_seq() -> String { "meta#global_seq".to_string() }
fn rk_reaction(reaction_id: &str) -> String { format!("react#{reaction_id}") }
fn rk_cursor(reaction_id: &str, shard: i32) -> String { format!("cursor#{reaction_id}#{shard:010}") }
fn rk_task(task_id: &str) -> String { format!("task#{task_id}") }
fn rk_taskidx(reaction_id: &str, shard: i32, source_global_seq: i64) -> String {
    format!("taskidx#{reaction_id}#{shard:010}#{source_global_seq:020}")
}
fn rk_pending(reaction_id: &str, shard: i32, task_id: &str) -> String {
    format!("pending#{reaction_id}#{shard:010}#{task_id}")
}

// ── decoders ────────────────────────────────────────────────────────────────
fn run_from(run_id: &str, m: &RowMap) -> RunState {
    let scopes_s = m.gs("scopes");
    let scopes: Vec<String> = if scopes_s.is_empty() {
        Vec::new()
    } else {
        serde_json::from_str(&scopes_s).unwrap_or_default()
    };
    let labels_s = m.gs("labels");
    let labels: std::collections::HashMap<String, String> = if labels_s.is_empty() {
        std::collections::HashMap::new()
    } else {
        serde_json::from_str(&labels_s).unwrap_or_default()
    };
    RunState {
        run_id: run_id.to_string(),
        app_name: m.gs("app"), user_id: m.gs("user"), session_id: m.gs("session"),
        invocation_id: m.gs("inv"), status: m.gi("status") as i32, seq_cursor: m.gi("seq"),
        lease_owner: m.gs("lease_owner"), lease_expires_at_ms: m.gi("lease_exp"),
        started_at_ms: m.gi("started"), ended_at_ms: m.gi("ended"), waiting_on_gate: m.gs("waiting_gate"),
        tenant_id: m.gs("tenant_id"), actor: m.gs("actor"), subject: m.gs("subject"),
        agent_id: m.gs("agent_id"), aiplex_instance_id: m.gs("aiplex_instance_id"),
        gateway_route: m.gs("gateway_route"), scopes, labels,
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

fn event_from_journal_gs(m: &RowMap) -> EventEntry {
    EventEntry {
        run_id: m.gs("run_id"), seq: m.gi("seq"), kind: m.gs("kind"),
        payload_json: m.gs("payload"), ts_ms: m.gi("ts"),
        global_seq: 0,                  // filled in by the caller from the row key
        subject: m.gs("subject"),
        schema_version: m.gi("schema_version") as i32,
        trace_id: m.gs("trace_id"), span_id: m.gs("span_id"), parent_span_id: m.gs("parent_span_id"),
    }
}

fn reaction_from(reaction_id: &str, m: &RowMap) -> Reaction {
    Reaction {
        reaction_id: reaction_id.to_string(),
        name: m.gs("name"),
        subject_pattern: m.gs("subject_pattern"),
        predicate_cel: m.gs("predicate_cel"),
        handler_kind: m.gi("handler_kind") as i32,
        agent_app: m.gs("agent_app"),
        publish_target: m.gs("publish_target"),
        max_concurrency: m.gi("max_concurrency") as i32,
        rate_limit_per_s: m.gi("rate_limit_per_s") as i32,
        debounce_ms: m.gi("debounce_ms") as i32,
        retry_max: m.gi("retry_max") as i32,
        retry_backoff_ms: m.gi("retry_backoff_ms") as i32,
        dlq_after_n: m.gi("dlq_after_n") as i32,
        num_shards: m.gi("num_shards") as i32,
        created_at_ms: m.gi("created_at_ms"),
        deleted: m.gi("deleted") != 0,
        // Storage-only flag; not surfaced (same shape as the SQL backend).
        bootstrap_from_head: false,
    }
}

fn task_from(task_id: &str, m: &RowMap) -> Task {
    Task {
        task_id: task_id.to_string(),
        reaction_id: m.gs("reaction_id"),
        shard: m.gi("shard") as i32,
        source_run_id: m.gs("source_run_id"),
        source_global_seq: m.gi("source_global_seq"),
        subject: m.gs("subject"),
        payload_json: m.gs("payload_json"),
        status: m.gi("status") as i32,
        attempts: m.gi("attempts") as i32,
        next_attempt_at_ms: m.gi("next_attempt_at_ms"),
        lease_owner: m.gs("lease_owner"),
        lease_expires_at_ms: m.gi("lease_expires_at_ms"),
        last_error: m.gs("last_error"),
        created_at_ms: m.gi("created_at_ms"),
        trace_id: m.gs("trace_id"),
        parent_span_id: m.gs("parent_span_id"),
    }
}

/// Compose a `CheckAndMutateRow` predicate matching a single (qualifier, exact value)
/// cell in family `m`. Used to enforce CAS on task status / lease_owner transitions.
fn predicate_qualifier_equals(qualifier: &str, value: &[u8]) -> RowFilter {
    // Chain: limit to qualifier == X, then value == Y. Both regex filters use
    // RE2; we escape the value bytes with `\C` semantics by quoting with `\Q…\E`.
    let mut esc = Vec::with_capacity(value.len() + 4);
    esc.extend_from_slice(b"\\Q");
    esc.extend_from_slice(value);
    esc.extend_from_slice(b"\\E");
    let qual_re = regex_escape(qualifier);
    RowFilter {
        filter: Some(row_filter::Filter::Chain(row_filter::Chain {
            filters: vec![
                RowFilter { filter: Some(row_filter::Filter::FamilyNameRegexFilter(FAM.to_string())) },
                RowFilter { filter: Some(row_filter::Filter::ColumnQualifierRegexFilter(qual_re.into_bytes())) },
                RowFilter { filter: Some(row_filter::Filter::CellsPerColumnLimitFilter(1)) },
                RowFilter { filter: Some(row_filter::Filter::ValueRegexFilter(esc)) },
            ],
        })),
    }
}

fn regex_escape(s: &str) -> String {
    // Wrap the literal qualifier in \Q...\E so RE2 treats it as a literal.
    format!("\\Q{}\\E", s)
}

async fn check_and_mutate(
    bt_store: &BigtableRunStore,
    row_key: &str,
    predicate: RowFilter,
    true_mutations: Vec<Mutation>,
) -> StoreResult<bool> {
    let mut bt = bt_store.bt();
    let resp = bt
        .check_and_mutate_row(CheckAndMutateRowRequest {
            table_name: bt_store.table.clone(),
            row_key: row_key.as_bytes().to_vec(),
            predicate_filter: Some(predicate),
            true_mutations,
            false_mutations: vec![],
            ..Default::default()
        })
        .await
        .map_err(e)?;
    Ok(resp.predicate_matched)
}

#[async_trait]
impl RunStore for BigtableRunStore {
    fn journal_notify(&self) -> Arc<tokio::sync::Notify> { self.notify.clone() }

    // ── run lifecycle ───────────────────────────────────────────────────────
    async fn begin_run(&self, app: &str, user: &str, session: &str, invocation: &str,
                       identity: &RunIdentity<'_>,
                       lease_owner: &str, lease_ttl_ms: i64) -> StoreResult<BeginRunResponse> {
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
        let scopes_json = if identity.scopes_json.is_empty() { "[]" } else { identity.scopes_json };
        let labels_json = if identity.labels_json.is_empty() { "{}" } else { identity.labels_json };
        self.write_row(&rk_run(&run_id), vec![
            set("status", iv(RunStatus::Running as i64)), set("app", sv(app)), set("user", sv(user)),
            set("session", sv(session)), set("inv", sv(invocation)), set("seq", iv(0)),
            set("lease_owner", sv(lease_owner)), set("lease_exp", iv(lease_exp)), set("started", iv(ts)),
            set("tenant_id", sv(identity.tenant_id)), set("actor", sv(identity.actor)),
            set("subject", sv(identity.subject)), set("agent_id", sv(identity.agent_id)),
            set("aiplex_instance_id", sv(identity.aiplex_instance_id)),
            set("gateway_route", sv(identity.gateway_route)),
            set("scopes", sv(scopes_json)), set("labels", sv(labels_json)),
        ]).await?;
        self.write_row(&rk_idx(app, user, session, invocation), vec![set("run", sv(run_id.clone()))]).await?;
        self.write_row(&rk_route(app, user, session), vec![set("run", sv(run_id.clone())), set("started", iv(ts))]).await?;
        // Run-lifecycle journal — /tape/run/running/<app>/<user>/<session>/<run_id>.
        // Best-effort: the run is already committed.
        let seq = self.next_seq(&run_id).await.unwrap_or(0);
        let payload = serde_json::json!({
            "app": app, "user": user, "session": session,
            "run_id": run_id, "invocation_id": invocation, "status": "running",
            "tenant_id": identity.tenant_id, "actor": identity.actor,
            "subject": identity.subject, "agent_id": identity.agent_id,
            "aiplex_instance_id": identity.aiplex_instance_id,
        }).to_string();
        let _ = self.journal(&run_id, seq, "run", &payload, ts).await;
        Ok(BeginRunResponse { run_id, resumed: false, next_seq: 0, status: RunStatus::Running as i32 })
    }
    async fn resume_run(&self, run_id: &str, lease_owner: &str, lease_ttl_ms: i64) -> StoreResult<Option<RunState>> {
        self.write_row(&rk_run(run_id), vec![set("status", iv(RunStatus::Running as i64)), set("lease_owner", sv(lease_owner)), set("lease_exp", iv(now_ms() + lease_ttl_ms.max(0)))]).await?;
        self.run_state(run_id).await
    }
    async fn end_run(&self, run_id: &str, status: i32, detail_json: &str) -> StoreResult<Option<RunState>> {
        let ts = now_ms();
        self.write_row(&rk_run(run_id), vec![set("status", iv(status as i64)), set("ended", iv(ts)), set("detail", sv(detail_json)), set("lease_owner", sv(""))]).await?;
        let cur = self.run_state(run_id).await?;
        if let Some(ref r) = cur {
            let status_str = match RunStatus::try_from(status) {
                Ok(RunStatus::Terminal) => "terminal",
                Ok(RunStatus::Failed) => "failed",
                Ok(RunStatus::Stuck) => "stuck",
                Ok(RunStatus::Cancelled) => "cancelled",
                Ok(RunStatus::Compensating) => "compensating",
                _ => "ended",
            };
            let seq = self.next_seq(run_id).await.unwrap_or(0);
            let payload = serde_json::json!({
                "app": r.app_name, "user": r.user_id, "session": r.session_id,
                "run_id": run_id, "status": status_str,
            }).to_string();
            let _ = self.journal(run_id, seq, "run", &payload, ts).await;
        }
        Ok(cur)
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
                global_seq: m.gi("global_seq"),
                subject: m.gs("subject"),
                schema_version: if m.contains_key("schema_version") { m.gi("schema_version") as i32 } else { 1 },
                trace_id: m.gs("trace_id"), span_id: m.gs("span_id"), parent_span_id: m.gs("parent_span_id"),
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
        // P2 fix: business_key requires connector — same reasoning as SQL.
        // On Bigtable the bk# pointer-row key is `bk#<connector>#<key>`, so
        // an empty connector would collapse all keyless effects into one
        // pointer-row collision (the inverse of dedupe). Refuse cleanly.
        if !business_key.is_empty() && connector.is_empty() {
            return Err(StoreError::msg(
                "begin_effect: business_key requires connector \
                 (cross-run dedupe is per-(connector, business_key))"));
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
        // Session-event journal — /tape/event/appended/<app>/<user>/<session>.
        let payload = serde_json::json!({
            "app": app, "user": user, "session": session,
            "event_id": event.id, "invocation_id": event.invocation_id, "author": event.author,
        }).to_string();
        let _ = self.journal_full("", 0, "event", &subjects::derive("event", &serde_json::json!({
            "app": app, "user": user, "session": session,
        })), &payload, ts, 1, "", "", "").await;
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
        // Journal: /tape/value/changed/<ns>/<key>. run_id is empty; the value
        // surface is run-agnostic. Best-effort — the value write committed; a
        // missed journal row is recoverable on the next write.
        let payload = serde_json::json!({
            "namespace": namespace, "key": key, "version": next_v, "writer": writer,
            "value": {"namespace": namespace, "key": key, "value_json": value_json, "version": next_v},
        }).to_string();
        let _ = self.journal_full("", 0, "value", &subjects::derive("value", &serde_json::json!({
            "namespace": namespace, "key": key,
        })), &payload, ts, 1, "", "", "").await;
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
        // Journal: /tape/value/deleted/<ns>/<key>.
        let payload = serde_json::json!({
            "namespace": namespace, "key": key, "version": next_v, "deleted": true,
        }).to_string();
        let _ = self.journal_full("", 0, "value", &subjects::derive("value", &serde_json::json!({
            "namespace": namespace, "key": key, "deleted": true,
        })), &payload, ts, 1, "", "", "").await;
        Ok((true, next_v))
    }

    // ── the WAL tail ────────────────────────────────────────────────────────
    async fn events_since(&self, _from_ts_ms: i64, run_id: &str, kind: &str, limit: i64) -> StoreResult<Vec<EventEntry>> {
        // A cross-run, time-ordered tail isn't expressible against Bigtable's
        // row-key layout — that's what the `jg#` index (events_by_subject) is for.
        // The per-run feed (SubscribeRun / journal_range) still works.
        if !run_id.is_empty() {
            return Ok(self.journal_range(run_id, 0).await?.into_iter()
                .filter(|j| kind.is_empty() || j.kind == kind)
                .take(limit.max(1) as usize)
                .map(|j| EventEntry {
                    run_id: run_id.into(), seq: j.seq, kind: j.kind,
                    payload_json: j.payload_json, ts_ms: j.ts_ms,
                    global_seq: j.global_seq, subject: j.subject, schema_version: j.schema_version,
                    trace_id: j.trace_id, span_id: j.span_id, parent_span_id: j.parent_span_id,
                })
                .collect());
        }
        // Cross-run path: fall through to the by-global-seq index.
        self.read_journal_after(0, limit).await
    }

    // ── event-bus surface (§6.3) ────────────────────────────────────────────
    async fn read_journal_after(&self, from_global_seq: i64, limit: i64) -> StoreResult<Vec<EventEntry>> {
        // The jg# index is ordered by global_seq via zero-padded row keys, so a
        // range read of [jg#<from+1:020>, jg#~) returns entries in order.
        let start = format!("jg#{:020}", from_global_seq.saturating_add(1));
        // The end is the lexicographic successor of "jg#" — i.e. "jg$".
        let end = "jg$".to_string();
        let rows = self.read_range(&start, Some(&end), limit.max(1)).await?;
        let mut out: Vec<EventEntry> = rows
            .into_iter()
            .map(|(key, m)| {
                let mut e = event_from_journal_gs(&m);
                // global_seq is encoded in the row key: "jg#<gs:020>"
                e.global_seq = key
                    .trim_start_matches("jg#")
                    .parse()
                    .unwrap_or(0);
                e
            })
            .collect();
        out.sort_by_key(|e| e.global_seq);
        Ok(out)
    }
    async fn events_by_subject(&self, from_global_seq: i64, subject_pattern: &str, limit: i64)
        -> StoreResult<Vec<EventEntry>>
    {
        // Bigtable has no native subject index. We scan the jg# range from the
        // cursor onward and filter in memory. This reads more than the caller
        // consumes when the subject pattern is selective; a proper impl would
        // maintain a secondary index `subj#<subject>#<gs:020>`. Acceptable for v1.
        let entries = self.read_journal_after(from_global_seq, limit.max(1).saturating_mul(8)).await?;
        let mut out: Vec<EventEntry> = entries
            .into_iter()
            .filter(|e| subjects::matches(subject_pattern, &e.subject))
            .take(limit.max(1) as usize)
            .collect();
        out.sort_by_key(|e| e.global_seq);
        Ok(out)
    }

    // ── reactions ───────────────────────────────────────────────────────────
    async fn register_reaction(&self, r: &Reaction) -> StoreResult<Reaction> {
        let rid = if r.reaction_id.is_empty() { uuid::Uuid::new_v4().to_string() } else { r.reaction_id.clone() };
        let now = now_ms();
        let max_conc = if r.max_concurrency > 0 { r.max_concurrency } else { 1 };
        let num_shards = if r.num_shards > 0 { r.num_shards } else { 1 };
        let created = if r.created_at_ms > 0 { r.created_at_ms } else { now };
        let dlq = if r.dlq_after_n > 0 { r.dlq_after_n } else { 5 };
        let retry_max = if r.retry_max > 0 { r.retry_max } else { 5 };
        let backoff = if r.retry_backoff_ms > 0 { r.retry_backoff_ms } else { 1000 };

        // First-time detection: bootstrap_from_head is honoured only on initial
        // creation. A re-registration must NOT reset cursors (same shape as SQL).
        let pre_exists = self.read_row(&rk_reaction(&rid)).await?.is_some();

        self.write_row(&rk_reaction(&rid), vec![
            set("name", sv(r.name.clone())),
            set("subject_pattern", sv(r.subject_pattern.clone())),
            set("predicate_cel", sv(r.predicate_cel.clone())),
            set("handler_kind", iv(r.handler_kind as i64)),
            set("agent_app", sv(r.agent_app.clone())),
            set("publish_target", sv(r.publish_target.clone())),
            set("max_concurrency", iv(max_conc as i64)),
            set("rate_limit_per_s", iv(r.rate_limit_per_s as i64)),
            set("debounce_ms", iv(r.debounce_ms as i64)),
            set("retry_max", iv(retry_max as i64)),
            set("retry_backoff_ms", iv(backoff as i64)),
            set("dlq_after_n", iv(dlq as i64)),
            set("num_shards", iv(num_shards as i64)),
            set("created_at_ms", iv(created)),
            set("deleted", iv(0)),
        ]).await?;

        if !pre_exists && r.bootstrap_from_head {
            // Seed each shard's cursor at the current journal head. The head is
            // the value of the meta#global_seq counter — `next_global_seq` would
            // *increment*, so we read it instead.
            let head_cell = self.read_row(&rk_meta_global_seq()).await?;
            let head: i64 = head_cell
                .and_then(|m| m.get("v").map(|v| {
                    if v.len() >= 8 {
                        let mut buf = [0u8; 8];
                        buf.copy_from_slice(&v[..8]);
                        i64::from_be_bytes(buf)
                    } else { 0 }
                }))
                .unwrap_or(0);
            for s in 0..num_shards {
                self.write_row(&rk_cursor(&rid, s), vec![
                    set("last_global_seq", iv(head)),
                    set("last_processed_at_ms", iv(now)),
                ]).await?;
            }
        }

        // Return the canonical stored shape.
        let m = self.read_row(&rk_reaction(&rid)).await?
            .ok_or_else(|| StoreError::msg("register_reaction: row vanished after write"))?;
        Ok(reaction_from(&rid, &m))
    }
    async fn deregister_reaction(&self, reaction_id: &str) -> StoreResult<bool> {
        let existed = self.read_row(&rk_reaction(reaction_id)).await?.is_some();
        if existed {
            self.write_row(&rk_reaction(reaction_id), vec![set("deleted", iv(1))]).await?;
        }
        Ok(existed)
    }
    async fn list_reactions(&self, subject_pattern: &str) -> StoreResult<Vec<Reaction>> {
        let rows = self.read_prefix("react#", 100_000).await?;
        let mut out: Vec<Reaction> = rows.into_iter().filter_map(|(key, m)| {
            if m.gi("deleted") != 0 { return None; }
            let rid = key.trim_start_matches("react#");
            if !subject_pattern.is_empty() && m.gs("subject_pattern") != subject_pattern {
                return None;
            }
            Some(reaction_from(rid, &m))
        }).collect();
        out.sort_by_key(|r| r.created_at_ms);
        Ok(out)
    }
    async fn get_reaction_cursor(&self, reaction_id: &str, shard: i32) -> StoreResult<i64> {
        Ok(self.read_row(&rk_cursor(reaction_id, shard)).await?
            .map(|m| m.gi("last_global_seq"))
            .unwrap_or(0))
    }
    async fn set_reaction_cursor(&self, reaction_id: &str, shard: i32, global_seq: i64, now_ms: i64) -> StoreResult<()> {
        self.write_row(&rk_cursor(reaction_id, shard), vec![
            set("last_global_seq", iv(global_seq)),
            set("last_processed_at_ms", iv(now_ms)),
        ]).await
    }

    // ── tasks ───────────────────────────────────────────────────────────────
    async fn create_task(&self, t: &Task) -> StoreResult<Task> {
        // UNIQUE-on-(reaction_id, shard, source_global_seq): check the index row.
        // CheckAndMutateRow gives us an atomic if-not-exists by predicating on the
        // presence of any cell in the index row (PassAll captured + 0 mutations =>
        // returns predicate_matched=true if row exists, with no writes); we use
        // it the other way round: a non-existent row means we win the race.
        let idx_key = rk_taskidx(&t.reaction_id, t.shard, t.source_global_seq);
        if let Some(idx) = self.read_row(&idx_key).await? {
            // Duplicate matcher hit: return the existing task.
            let existing_tid = idx.gs("task_id");
            if !existing_tid.is_empty() {
                if let Some(m) = self.read_row(&rk_task(&existing_tid)).await? {
                    return Ok(task_from(&existing_tid, &m));
                }
            }
        }
        let tid = if t.task_id.is_empty() { uuid::Uuid::new_v4().to_string() } else { t.task_id.clone() };
        let created = if t.created_at_ms > 0 { t.created_at_ms } else { now_ms() };

        // Write the task row.
        self.write_row(&rk_task(&tid), vec![
            set("reaction_id", sv(t.reaction_id.clone())),
            set("shard", iv(t.shard as i64)),
            set("source_run_id", sv(t.source_run_id.clone())),
            set("source_global_seq", iv(t.source_global_seq)),
            set("subject", sv(t.subject.clone())),
            set("payload_json", sv(t.payload_json.clone())),
            set("status", iv(TaskStatus::Pending as i64)),
            set("attempts", iv(0)),
            set("next_attempt_at_ms", iv(0)),
            set("lease_owner", sv("")),
            set("lease_expires_at_ms", iv(0)),
            set("last_error", sv("")),
            set("created_at_ms", iv(created)),
            set("trace_id", sv(t.trace_id.clone())),
            set("parent_span_id", sv(t.parent_span_id.clone())),
        ]).await?;
        // Index row (UNIQUE constraint) — written last so a crash in between
        // leaves an orphan task that's invisible to dedup but still listable;
        // the caller's retry will create another. Acceptable v1 behaviour.
        self.write_row(&idx_key, vec![set("task_id", sv(tid.clone()))]).await?;
        // Pending tracking index — speeds up claim_tasks / find_pending.
        self.write_row(&rk_pending(&t.reaction_id, t.shard, &tid), vec![
            set("task_id", sv(tid.clone())),
            set("subject", sv(t.subject.clone())),
            set("created_at_ms", iv(created)),
        ]).await?;

        let m = self.read_row(&rk_task(&tid)).await?
            .ok_or_else(|| StoreError::msg("create_task: row vanished after write"))?;
        Ok(task_from(&tid, &m))
    }
    async fn claim_tasks(&self, reaction_id: &str, shard: i32, owner: &str, lease_ms: i64, max: i32, now_ms: i64) -> StoreResult<Vec<Task>> {
        if owner.is_empty() {
            // See the SQL backend: a claim with `lease_owner=''` would alias
            // unclaimed PENDING rows in subsequent complete/nack predicates.
            return Err(StoreError::msg("claim_tasks: owner is required"));
        }
        let lease_ms = if lease_ms > 0 { lease_ms } else { 60_000 };
        let max = if max > 0 { max } else { 16 };
        let now = if now_ms > 0 { now_ms } else { super::now_ms() };
        let lease_exp = now + lease_ms;

        // Read the pending-task index for this reaction (and shard if specified).
        let prefix = if shard < 0 {
            format!("pending#{reaction_id}#")
        } else {
            format!("pending#{reaction_id}#{shard:010}#")
        };
        let candidates = self.read_prefix(&prefix, (max as i64).saturating_mul(8).max(64)).await?;
        let mut out = Vec::new();
        let pending = TaskStatus::Pending as i64;
        let claimed = TaskStatus::Claimed as i64;
        for (_, idx) in candidates {
            if out.len() as i32 >= max { break; }
            let tid = idx.gs("task_id");
            if tid.is_empty() { continue; }
            // Read the current task row to filter on next_attempt_at_ms / lease_expires_at_ms.
            let Some(m) = self.read_row(&rk_task(&tid)).await? else { continue; };
            let status = m.gi("status");
            let nx = m.gi("next_attempt_at_ms");
            let lex = m.gi("lease_expires_at_ms");
            let attempts = m.gi("attempts");
            let eligible = (status == pending && nx <= now) || (status == claimed && lex < now);
            if !eligible { continue; }
            // CAS: predicate on "status == <expected>" — a one-shot value-regex
            // filter. Two writers racing here will see one CAS succeed; the
            // loser sees predicate_matched=false and moves on.
            let status_bytes = status.to_string().into_bytes();
            let predicate = predicate_qualifier_equals("status", &status_bytes);
            let ok = check_and_mutate(self, &rk_task(&tid), predicate, vec![
                set("status", iv(claimed)),
                set("lease_owner", sv(owner)),
                set("lease_expires_at_ms", iv(lease_exp)),
                set("attempts", iv(attempts + 1)),
            ]).await?;
            if ok {
                if let Some(m2) = self.read_row(&rk_task(&tid)).await? {
                    out.push(task_from(&tid, &m2));
                }
            }
        }
        Ok(out)
    }
    async fn complete_task(&self, task_id: &str, owner: &str) -> StoreResult<Option<Task>> {
        if owner.is_empty() {
            // Bigtable's CheckAndMutateRow only enforces lease_owner==owner;
            // owner="" would match unleased PENDING rows. Reject up front so
            // the CAS predicate can't accidentally fire on unclaimed work.
            return Err(StoreError::msg("complete_task: owner is required"));
        }
        // CAS on lease_owner: predicate matches only if lease_owner == owner.
        // The lease_owner invariant (only set non-empty by claim_tasks, which
        // simultaneously sets status=CLAIMED; only reset to '' by complete_
        // task / nack_task, which simultaneously clear the claimed state)
        // means matching lease_owner==owner implies status==CLAIMED on this
        // backend without a second predicate hop.
        let predicate = predicate_qualifier_equals("lease_owner", owner.as_bytes());
        let ok = check_and_mutate(self, &rk_task(task_id), predicate, vec![
            set("status", iv(TaskStatus::Done as i64)),
            set("completed_at_ms", iv(now_ms())),
            set("lease_owner", sv("")),
            set("lease_expires_at_ms", iv(0)),
        ]).await?;
        if !ok { return Ok(None); }
        if let Some(m) = self.read_row(&rk_task(task_id)).await? {
            // Remove the pending tracking index (best-effort).
            let reaction_id = m.gs("reaction_id");
            let shard = m.gi("shard") as i32;
            let _ = self.delete_row(&rk_pending(&reaction_id, shard, task_id)).await;
            Ok(Some(task_from(task_id, &m)))
        } else {
            Ok(None)
        }
    }
    async fn nack_task(&self, task_id: &str, owner: &str, error: &str, permanent: bool, now_ms: i64) -> StoreResult<Option<Task>> {
        if owner.is_empty() {
            return Err(StoreError::msg("nack_task: owner is required"));
        }
        let now = if now_ms > 0 { now_ms } else { super::now_ms() };
        let Some(m) = self.read_row(&rk_task(task_id)).await? else { return Ok(None); };
        // Defensive — the CAS below is the atomic boundary; this in-memory
        // check just skips the work of computing the next attempt when the
        // lease clearly isn't ours.
        if m.gs("lease_owner") != owner { return Ok(None); }
        if m.gi("status") as i32 != TaskStatus::Claimed as i32 { return Ok(None); }
        let attempts = m.gi("attempts") as i32;
        let reaction_id = m.gs("reaction_id");
        let shard = m.gi("shard") as i32;
        let (dlq_after, backoff_ms) = match self.read_row(&rk_reaction(&reaction_id)).await? {
            Some(r) => (
                if r.gi("dlq_after_n") > 0 { r.gi("dlq_after_n") as i32 } else { 5 },
                if r.gi("retry_backoff_ms") > 0 { r.gi("retry_backoff_ms") } else { 1000 },
            ),
            None => (5, 1000),
        };
        let to_dlq = permanent || attempts >= dlq_after;
        // CAS on lease_owner (re-check the predicate in the CAS itself).
        let predicate = predicate_qualifier_equals("lease_owner", owner.as_bytes());
        let muts = if to_dlq {
            vec![
                set("status", iv(TaskStatus::Dlq as i64)),
                set("last_error", sv(error)),
                set("lease_owner", sv("")),
                set("lease_expires_at_ms", iv(0)),
            ]
        } else {
            let shift = (attempts.max(1) - 1).min(20) as u32;
            let delay = backoff_ms.saturating_mul(1i64.checked_shl(shift).unwrap_or(i64::MAX));
            let delay = delay.min(3_600_000);
            vec![
                set("status", iv(TaskStatus::Pending as i64)),
                set("next_attempt_at_ms", iv(now + delay)),
                set("last_error", sv(error)),
                set("lease_owner", sv("")),
                set("lease_expires_at_ms", iv(0)),
            ]
        };
        let ok = check_and_mutate(self, &rk_task(task_id), predicate, muts).await?;
        if !ok { return Ok(None); }
        if to_dlq {
            let _ = self.delete_row(&rk_pending(&reaction_id, shard, task_id)).await;
        }
        Ok(self.read_row(&rk_task(task_id)).await?.map(|m| task_from(task_id, &m)))
    }
    async fn list_tasks(&self, reaction_id: &str, status: i32, limit: i64) -> StoreResult<Vec<Task>> {
        let limit = if limit > 0 { limit } else { 200 };
        // Scan task# rows and filter by reaction_id in memory. Inefficient at
        // scale (every task ever); a secondary `task_by_reaction#<rid>#<task_id>`
        // would fix this. Acceptable for v1 — list_tasks is for observability
        // and DLQ inspection, not the hot path.
        let rows = self.read_prefix("task#", limit.saturating_mul(8).max(200)).await?;
        let mut out: Vec<Task> = rows.into_iter().filter_map(|(key, m)| {
            if m.gs("reaction_id") != reaction_id { return None; }
            if status != 0 && m.gi("status") as i32 != status { return None; }
            let tid = key.trim_start_matches("task#");
            Some(task_from(tid, &m))
        }).collect();
        out.sort_by_key(|t| std::cmp::Reverse(t.created_at_ms));
        out.truncate(limit as usize);
        Ok(out)
    }

    async fn find_pending_task_for_subject(&self, reaction_id: &str, subject: &str)
        -> StoreResult<Option<Task>>
    {
        // Scan the pending# tracking index for this reaction (across all shards).
        // The index row carries the subject so we can filter without re-reading
        // the task body for non-matches; we read the task body only for hits.
        // Inefficient relative to a `pending_by_subject#<rid>#<subject>#…` index;
        // acceptable for v1 (debounce is a soft optimisation, not a correctness
        // requirement).
        let rows = self.read_prefix(&format!("pending#{reaction_id}#"), 100_000).await?;
        let mut best: Option<(i64, String)> = None;
        for (_, idx) in rows {
            if idx.gs("subject") != subject { continue; }
            let tid = idx.gs("task_id");
            let created = idx.gi("created_at_ms");
            if best.as_ref().map(|(c, _)| created > *c).unwrap_or(true) {
                best = Some((created, tid));
            }
        }
        let Some((_, tid)) = best else { return Ok(None); };
        let Some(m) = self.read_row(&rk_task(&tid)).await? else { return Ok(None); };
        // The pending index can lag behind status transitions (a CompleteTask
        // race might have removed the row but the index entry is still there).
        // Filter to PENDING here to match SQL semantics.
        if m.gi("status") != TaskStatus::Pending as i64 { return Ok(None); }
        Ok(Some(task_from(&tid, &m)))
    }

    async fn coalesce_task(&self, task_id: &str, source_global_seq: i64, payload_json: &str,
                           trace_id: &str, parent_span_id: &str) -> StoreResult<Option<Task>>
    {
        // CAS on status==PENDING: a concurrent claim_tasks that flipped the row
        // to CLAIMED makes the predicate fail and we return None — the matcher
        // then falls through to a fresh create_task.
        let pending_bytes = (TaskStatus::Pending as i64).to_string().into_bytes();
        let predicate = predicate_qualifier_equals("status", &pending_bytes);
        let ok = check_and_mutate(self, &rk_task(task_id), predicate, vec![
            set("source_global_seq", iv(source_global_seq)),
            set("payload_json", sv(payload_json)),
            set("trace_id", sv(trace_id)),
            set("parent_span_id", sv(parent_span_id)),
        ]).await?;
        if !ok { return Ok(None); }
        Ok(self.read_row(&rk_task(task_id)).await?.map(|m| task_from(task_id, &m)))
    }
}

fn rk_timer(run_id: &str, timer_id: &str) -> String { format!("tmr#{run_id}#{timer_id}") }
fn rk_value(namespace: &str, key: &str) -> String { format!("val#{namespace}#{key}") }
