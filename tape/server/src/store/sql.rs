//! The SQL implementation of [`RunStore`].
//!
//!   * `SqlBackend` — a four-method portable surface (`exec` / `query` /
//!     `query_opt` / `tx`) over `?N`-placeholdered SQL;
//!   * `SqliteBackend` (pooled rusqlite, WAL) — the default, and the dev/test
//!     store;
//!   * `PostgresBackend` (pooled `postgres`) — the production store, and AlloyDB
//!     (which is PostgreSQL-wire-compatible);
//!   * `SqlRunStore` — implements `RunStore` over any `SqlBackend`; all the SQL
//!     lives here, once.
//!
//! The two backends differ only in placeholder style (`?N` vs `$N` — rewritten
//! here) and in int/float type names in the migration. Both run blocking DB work
//! on a blocking thread, behind an `r2d2` connection pool.

use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use postgres::types::{ToSql as PgToSql, Type as PgType};
use postgres::{Config as PgConfig, NoTls};
use r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::types::{ToSqlOutput, ValueRef};
use rusqlite::ToSql as SqliteToSql;

use super::{derive_key, merge_json, now_ms, CompactReport, RunIdentity, RunStore, StoreError, StoreResult};
use crate::pb::*;
use crate::subjects;

const SCHEMA_SQLITE: &str = include_str!("../../migrations/0001_init.sqlite.sql");
const SCHEMA_PG: &str = include_str!("../../migrations/0001_init.postgres.sql");
const SCHEMA_SQLITE_002: &str = include_str!("../../migrations/0002_event_bus.sqlite.sql");
const SCHEMA_PG_002: &str = include_str!("../../migrations/0002_event_bus.postgres.sql");

fn e<E: std::fmt::Display>(err: E) -> StoreError {
    StoreError::Msg(err.to_string())
}

// ── value / row plumbing ────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub enum Val {
    Int(i64),
    Real(f64),
    Text(String),
    Null,
}
impl From<i64> for Val { fn from(v: i64) -> Self { Val::Int(v) } }
impl From<i32> for Val { fn from(v: i32) -> Self { Val::Int(v as i64) } }
impl From<f64> for Val { fn from(v: f64) -> Self { Val::Real(v) } }
impl From<&str> for Val { fn from(v: &str) -> Self { Val::Text(v.to_string()) } }
impl From<String> for Val { fn from(v: String) -> Self { Val::Text(v) } }
impl From<&String> for Val { fn from(v: &String) -> Self { Val::Text(v.clone()) } }

pub type Row = Vec<Val>;

pub trait RowExt {
    fn i64(&self, i: usize) -> i64;
    fn f64(&self, i: usize) -> f64;
    fn str(&self, i: usize) -> String;
    fn i32(&self, i: usize) -> i32 { self.i64(i) as i32 }
}
impl RowExt for Row {
    fn i64(&self, i: usize) -> i64 {
        match self.get(i) {
            Some(Val::Int(v)) => *v,
            Some(Val::Real(v)) => *v as i64,
            Some(Val::Text(s)) => s.parse().unwrap_or(0),
            _ => 0,
        }
    }
    fn f64(&self, i: usize) -> f64 {
        match self.get(i) {
            Some(Val::Real(v)) => *v,
            Some(Val::Int(v)) => *v as f64,
            Some(Val::Text(s)) => s.parse().unwrap_or(0.0),
            _ => 0.0,
        }
    }
    fn str(&self, i: usize) -> String {
        match self.get(i) {
            Some(Val::Text(s)) => s.clone(),
            Some(Val::Int(v)) => v.to_string(),
            Some(Val::Real(v)) => v.to_string(),
            _ => String::new(),
        }
    }
}

/// Rewrite SQLite `?N` placeholders to Postgres `$N`. (No `?N` is reused in
/// Tape's SQL, so it's a straight token swap.)
fn pg_sql(sql: &str) -> String {
    let b = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'?' && i + 1 < b.len() && b[i + 1].is_ascii_digit() {
            out.push('$');
        } else {
            out.push(b[i] as char);
        }
        i += 1;
    }
    out
}

#[async_trait]
pub trait SqlBackend: Send + Sync {
    async fn migrate(&self) -> StoreResult<()>;
    async fn exec(&self, sql: &str, params: Vec<Val>) -> StoreResult<u64>;
    async fn query(&self, sql: &str, params: Vec<Val>) -> StoreResult<Vec<Row>>;
    async fn query_opt(&self, sql: &str, params: Vec<Val>) -> StoreResult<Option<Row>> {
        Ok(self.query(sql, params).await?.into_iter().next())
    }
    async fn tx(&self, stmts: Vec<(String, Vec<Val>)>) -> StoreResult<()>;
    /// Allocate the next `global_seq` value. SQLite bumps a single-row counter;
    /// Postgres calls `nextval` on the journal sequence (the column default
    /// would do the same on insert, but the matcher and the SQLite path both
    /// want the value up front).
    async fn next_global_seq(&self) -> StoreResult<i64>;
    /// `true` for Postgres / AlloyDB. Lets the SqlRunStore branch on dialect
    /// (LIKE-ESCAPE, FOR UPDATE SKIP LOCKED, etc.).
    fn is_postgres(&self) -> bool {
        false
    }
}

// ── SQLite backend ──────────────────────────────────────────────────────────

pub struct SqliteBackend {
    pool: Pool<SqliteConnectionManager>,
}
impl SqliteBackend {
    pub fn file(path: &str) -> StoreResult<Self> {
        let mgr = SqliteConnectionManager::file(path)
            .with_init(|c| c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;"));
        Ok(Self { pool: Pool::builder().max_size(16).build(mgr).map_err(e)? })
    }
    pub fn memory() -> StoreResult<Self> {
        let mgr = SqliteConnectionManager::memory().with_init(|c| c.execute_batch("PRAGMA foreign_keys=ON;"));
        Ok(Self { pool: Pool::builder().max_size(1).build(mgr).map_err(e)? })
    }
    async fn with<T, F>(&self, f: F) -> StoreResult<T>
    where T: Send + 'static, F: FnOnce(&mut rusqlite::Connection) -> StoreResult<T> + Send + 'static {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || { let mut c = pool.get().map_err(e)?; f(&mut c) }).await.map_err(e)?
    }
}
impl SqliteToSql for Val {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match self {
            Val::Int(v) => ToSqlOutput::from(*v),
            Val::Real(v) => ToSqlOutput::from(*v),
            Val::Text(v) => ToSqlOutput::from(v.as_str()),
            Val::Null => ToSqlOutput::from(rusqlite::types::Null),
        })
    }
}
fn sqlite_query(conn: &rusqlite::Connection, sql: &str, params: &[Val]) -> StoreResult<Vec<Row>> {
    let mut stmt = conn.prepare(sql).map_err(e)?;
    let n = stmt.column_count();
    let bound: Vec<&dyn SqliteToSql> = params.iter().map(|v| v as &dyn SqliteToSql).collect();
    let rows = stmt
        .query_map(bound.as_slice(), |row| {
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                out.push(match row.get_ref(i)? {
                    ValueRef::Null => Val::Null,
                    ValueRef::Integer(v) => Val::Int(v),
                    ValueRef::Real(v) => Val::Real(v),
                    ValueRef::Text(t) => Val::Text(String::from_utf8_lossy(t).into_owned()),
                    ValueRef::Blob(b) => Val::Text(String::from_utf8_lossy(b).into_owned()),
                });
            }
            Ok(out)
        })
        .map_err(e)?;
    rows.collect::<rusqlite::Result<Vec<Row>>>().map_err(e)
}
#[async_trait]
impl SqlBackend for SqliteBackend {
    async fn migrate(&self) -> StoreResult<()> {
        self.with(|c| {
            c.execute_batch(SCHEMA_SQLITE).map_err(e)?;
            // Apply 0002 only if its first column (global_seq) is missing —
            // old SQLite doesn't honour ADD COLUMN IF NOT EXISTS.
            let mut stmt = c.prepare("PRAGMA table_info(tape_journal)").map_err(e)?;
            let cols: Vec<String> = stmt
                .query_map([], |row| row.get::<_, String>(1))
                .map_err(e)?
                .filter_map(|r| r.ok())
                .map(|n| n.to_lowercase())
                .collect();
            if !cols.iter().any(|n| n == "global_seq") {
                c.execute_batch(SCHEMA_SQLITE_002).map_err(e)?;
            }
            Ok(())
        })
        .await
    }
    async fn exec(&self, sql: &str, params: Vec<Val>) -> StoreResult<u64> {
        let sql = sql.to_string();
        self.with(move |c| {
            let b: Vec<&dyn SqliteToSql> = params.iter().map(|v| v as &dyn SqliteToSql).collect();
            c.execute(&sql, b.as_slice()).map(|n| n as u64).map_err(e)
        }).await
    }
    async fn query(&self, sql: &str, params: Vec<Val>) -> StoreResult<Vec<Row>> {
        let sql = sql.to_string();
        self.with(move |c| sqlite_query(c, &sql, &params)).await
    }
    async fn tx(&self, stmts: Vec<(String, Vec<Val>)>) -> StoreResult<()> {
        self.with(move |c| {
            let t = c.transaction().map_err(e)?;
            for (sql, params) in &stmts {
                let b: Vec<&dyn SqliteToSql> = params.iter().map(|v| v as &dyn SqliteToSql).collect();
                t.execute(sql, b.as_slice()).map_err(e)?;
            }
            t.commit().map_err(e)
        }).await
    }
    async fn next_global_seq(&self) -> StoreResult<i64> {
        // SQLite serializes writes (WAL); the UPDATE+SELECT under one
        // connection is effectively atomic for the counter row.
        self.with(|c| {
            let t = c.transaction().map_err(e)?;
            t.execute("UPDATE tape_global_seq SET v = v + 1 WHERE id = 1", []).map_err(e)?;
            let v: i64 = t
                .query_row("SELECT v FROM tape_global_seq WHERE id = 1", [], |r| r.get(0))
                .map_err(e)?;
            t.commit().map_err(e)?;
            Ok(v)
        })
        .await
    }
}

// ── Postgres backend (also AlloyDB) ─────────────────────────────────────────

type PgMgr = PostgresConnectionManager<NoTls>;
pub struct PostgresBackend {
    pool: Pool<PgMgr>,
}
impl PostgresBackend {
    pub fn connect(url: &str) -> StoreResult<Self> {
        let cfg = PgConfig::from_str(url).map_err(e)?;
        let mgr = PostgresConnectionManager::new(cfg, NoTls);
        Ok(Self { pool: Pool::builder().max_size(16).build(mgr).map_err(e)? })
    }
    async fn with<T, F>(&self, f: F) -> StoreResult<T>
    where T: Send + 'static, F: FnOnce(&mut postgres::Client) -> StoreResult<T> + Send + 'static {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || { let mut c = pool.get().map_err(e)?; f(&mut c) }).await.map_err(e)?
    }
}
fn pg_boxed(params: &[Val]) -> Vec<Box<dyn PgToSql + Sync + Send>> {
    params.iter().map(|v| -> Box<dyn PgToSql + Sync + Send> {
        match v {
            Val::Int(i) => Box::new(*i),
            Val::Real(f) => Box::new(*f),
            Val::Text(s) => Box::new(s.clone()),
            Val::Null => Box::new(Option::<String>::None),
        }
    }).collect()
}
fn pg_refs<'a>(boxes: &'a [Box<dyn PgToSql + Sync + Send>]) -> Vec<&'a (dyn PgToSql + Sync)> {
    boxes.iter().map(|b| b.as_ref() as &(dyn PgToSql + Sync)).collect()
}
fn pg_col(row: &postgres::Row, i: usize) -> Val {
    let ty = row.columns()[i].type_().clone();
    if ty == PgType::INT8 { row.get::<_, Option<i64>>(i).map(Val::Int).unwrap_or(Val::Null) }
    else if ty == PgType::INT4 { row.get::<_, Option<i32>>(i).map(|v| Val::Int(v as i64)).unwrap_or(Val::Null) }
    else if ty == PgType::INT2 { row.get::<_, Option<i16>>(i).map(|v| Val::Int(v as i64)).unwrap_or(Val::Null) }
    else if ty == PgType::FLOAT8 { row.get::<_, Option<f64>>(i).map(Val::Real).unwrap_or(Val::Null) }
    else if ty == PgType::FLOAT4 { row.get::<_, Option<f32>>(i).map(|v| Val::Real(v as f64)).unwrap_or(Val::Null) }
    else if ty == PgType::BOOL { row.get::<_, Option<bool>>(i).map(|v| Val::Int(v as i64)).unwrap_or(Val::Null) }
    else { row.get::<_, Option<String>>(i).map(Val::Text).unwrap_or(Val::Null) }
}
#[async_trait]
impl SqlBackend for PostgresBackend {
    async fn migrate(&self) -> StoreResult<()> {
        self.with(|c| {
            c.batch_execute(SCHEMA_PG).map_err(e)?;
            c.batch_execute(SCHEMA_PG_002).map_err(e)?;
            Ok(())
        })
        .await
    }
    async fn next_global_seq(&self) -> StoreResult<i64> {
        self.with(|c| {
            let row = c
                .query_one("SELECT nextval('tape_journal_global_seq_seq')", &[])
                .map_err(e)?;
            Ok(row.get::<_, i64>(0))
        })
        .await
    }
    fn is_postgres(&self) -> bool {
        true
    }
    async fn exec(&self, sql: &str, params: Vec<Val>) -> StoreResult<u64> {
        let sql = pg_sql(sql);
        self.with(move |c| { let bx = pg_boxed(&params); c.execute(sql.as_str(), pg_refs(&bx).as_slice()).map_err(e) }).await
    }
    async fn query(&self, sql: &str, params: Vec<Val>) -> StoreResult<Vec<Row>> {
        let sql = pg_sql(sql);
        self.with(move |c| {
            let bx = pg_boxed(&params);
            let rows = c.query(sql.as_str(), pg_refs(&bx).as_slice()).map_err(e)?;
            Ok(rows.iter().map(|r| (0..r.columns().len()).map(|i| pg_col(r, i)).collect::<Row>()).collect())
        }).await
    }
    async fn tx(&self, stmts: Vec<(String, Vec<Val>)>) -> StoreResult<()> {
        self.with(move |c| {
            let mut t = c.transaction().map_err(e)?;
            for (sql, params) in &stmts {
                let sql = pg_sql(sql); let bx = pg_boxed(params);
                t.execute(sql.as_str(), pg_refs(&bx).as_slice()).map_err(e)?;
            }
            t.commit().map_err(e)
        }).await
    }
}

// ── SqlRunStore: RunStore over a SqlBackend ─────────────────────────────────

pub struct SqlRunStore {
    db: Arc<dyn SqlBackend>,
    notify: Arc<tokio::sync::Notify>,
    // Per-process scope cache. Scopes are set at `begin_run` and never
    // mutated for the lifetime of the run, so we can cache the parsed
    // grant set and skip the SELECT + JSON parse on every effect.
    // Bounded with a coarse cap (clear-on-overflow) to keep memory
    // bounded under pathological run-cardinality without pulling in
    // an LRU dependency.
    scope_cache: Arc<std::sync::Mutex<std::collections::HashMap<String, Vec<String>>>>,
}

const SCOPE_CACHE_MAX: usize = 8192;

impl SqlRunStore {
    pub async fn sqlite_file(path: &str) -> StoreResult<Self> {
        let db: Arc<dyn SqlBackend> = Arc::new(SqliteBackend::file(path)?);
        db.migrate().await?;
        Ok(Self {
            db,
            notify: Arc::new(tokio::sync::Notify::new()),
            scope_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        })
    }
    pub async fn sqlite_memory() -> StoreResult<Self> {
        let db: Arc<dyn SqlBackend> = Arc::new(SqliteBackend::memory()?);
        db.migrate().await?;
        Ok(Self {
            db,
            notify: Arc::new(tokio::sync::Notify::new()),
            scope_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        })
    }
    pub async fn postgres(url: &str) -> StoreResult<Self> {
        let db: Arc<dyn SqlBackend> = Arc::new(PostgresBackend::connect(url)?);
        db.migrate().await?;
        let notify = Arc::new(tokio::sync::Notify::new());
        // Best-effort Postgres LISTEN/NOTIFY listener: pumps `pg_notify` from
        // the `tape_journal_notify_trg` trigger into the in-process notify so
        // subscribers wake on insert instead of polling. If the connection
        // drops we reconnect with exponential backoff and the polling fallback
        // (1s in subscribe_*) keeps things moving in the meantime.
        spawn_pg_listener(url.to_string(), notify.clone());
        Ok(Self {
            db,
            notify,
            scope_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        })
    }

    /// Insert a run's scopes into the cache. Called from `begin_run` so
    /// the very first effect on a brand-new run hits the cache instead of
    /// the SELECT path.
    fn cache_scopes(&self, run_id: &str, scopes: Vec<String>) {
        let mut g = self.scope_cache.lock().unwrap();
        // Coarse cap: when full, drop the whole map. This is amortised
        // because the steady state is "active runs" and active runs
        // re-populate on the next effect lookup.
        if g.len() >= SCOPE_CACHE_MAX {
            g.clear();
        }
        g.insert(run_id.to_string(), scopes);
    }

    /// Evict a run from the scope cache. Called when a run reaches a
    /// terminal state so we don't pin its scopes forever.
    fn evict_scopes(&self, run_id: &str) {
        let _ = self.scope_cache.lock().unwrap().remove(run_id);
    }

    fn d(&self) -> &dyn SqlBackend { self.db.as_ref() }

    async fn next_seq(&self, run_id: &str) -> StoreResult<i64> {
        self.d().exec("UPDATE tape_runs SET seq_cursor = seq_cursor + 1 WHERE run_id = ?1",
                      vec![run_id.into()]).await?;
        Ok(self.d().query_opt("SELECT seq_cursor FROM tape_runs WHERE run_id = ?1", vec![run_id.into()])
            .await?.map(|r| r.i64(0)).unwrap_or(0))
    }

    /// Read the run's scope grant set. Consults the per-process cache
    /// first (populated by `begin_run`) and falls back to a SELECT +
    /// JSON parse if the entry has been evicted or the process is
    /// cold. Used by `begin_effect` for the authz scope-membership
    /// check. Returns an empty vector when the run has no scopes or
    /// doesn't exist (callers handle the absent-row case via the scope
    /// check itself — an empty grant set never satisfies a non-empty
    /// required scope).
    async fn run_scopes_for(&self, run_id: &str) -> StoreResult<Vec<String>> {
        if let Some(cached) = self.scope_cache.lock().unwrap().get(run_id).cloned() {
            return Ok(cached);
        }
        let row = self.d().query_opt(
            "SELECT scopes_json FROM tape_runs WHERE run_id = ?1",
            vec![run_id.into()]).await?;
        let scopes = row.map(|r| parse_scopes(&r.str(0))).unwrap_or_default();
        // Backfill the cache so subsequent effects skip the SELECT.
        self.cache_scopes(run_id, scopes.clone());
        Ok(scopes)
    }

    /// Append a journal row with the event-bus fields populated.
    /// `payload` is the canonical payload_json; `subject` is derived by the
    /// caller via [`subjects::derive`]. OTel fields default to empty when the
    /// caller has no current span. Pulses the in-process notify so streams
    /// wake immediately.
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
        let gs = self.d().next_global_seq().await?;
        self.d()
            .exec(
                "INSERT INTO tape_journal (run_id, seq, kind, payload_json, ts_ms, global_seq, subject, schema_version, trace_id, span_id, parent_span_id) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                vec![
                    run_id.into(), seq.into(), kind.into(), payload.into(), ts.into(),
                    gs.into(), subject.into(), schema_version.into(),
                    trace_id.into(), span_id.into(), parent_span_id.into(),
                ],
            )
            .await?;
        self.notify.notify_waiters();
        Ok(())
    }

    /// Convenience: subject is derived from `kind` + parsed `payload`. OTel
    /// fields are empty (the RPC layer doesn't propagate them yet).
    async fn journal(
        &self,
        run_id: &str,
        seq: i64,
        kind: &str,
        payload: &str,
        ts: i64,
    ) -> StoreResult<()> {
        let payload_v: serde_json::Value =
            serde_json::from_str(payload).unwrap_or(serde_json::Value::Null);
        // Inject run_id into the payload so subjects::derive can find it; it's
        // also helpful for downstream consumers.
        let mut p = payload_v;
        if let Some(o) = p.as_object_mut() {
            if !o.contains_key("run_id") {
                o.insert("run_id".to_string(), serde_json::Value::String(run_id.to_string()));
            }
        }
        let subject = subjects::derive(kind, &p);
        self.journal_full(run_id, seq, kind, &subject, payload, ts, 1, "", "", "").await
    }
}

const RUN_COLS: &str = "run_id, app_name, user_id, session_id, invocation_id, status, seq_cursor, \
    lease_owner, lease_expires_at_ms, started_at_ms, ended_at_ms, waiting_on_gate, \
    tenant_id, actor, subject, agent_id, aiplex_instance_id, gateway_route, scopes_json, labels_json";
fn run_of(r: &Row) -> RunState {
    RunState {
        run_id: r.str(0), app_name: r.str(1), user_id: r.str(2), session_id: r.str(3),
        invocation_id: r.str(4), status: r.i32(5), seq_cursor: r.i64(6), lease_owner: r.str(7),
        lease_expires_at_ms: r.i64(8), started_at_ms: r.i64(9), ended_at_ms: r.i64(10), waiting_on_gate: r.str(11),
        tenant_id: r.str(12), actor: r.str(13), subject: r.str(14), agent_id: r.str(15),
        aiplex_instance_id: r.str(16), gateway_route: r.str(17),
        scopes: parse_scopes(&r.str(18)),
        labels: parse_labels(&r.str(19)),
    }
}

/// Decode the JSON-encoded `scopes_json` column back into a `Vec<String>`.
/// Empty / malformed input degrades to `vec![]` (defensive — the SDK writes
/// valid JSON arrays, but old rows or hand-edited DBs may not).
fn parse_scopes(s: &str) -> Vec<String> {
    if s.is_empty() { return Vec::new(); }
    serde_json::from_str::<Vec<String>>(s).unwrap_or_default()
}

/// Decode the JSON-encoded `labels_json` column into a `HashMap<String, String>`.
fn parse_labels(s: &str) -> std::collections::HashMap<String, String> {
    if s.is_empty() { return std::collections::HashMap::new(); }
    serde_json::from_str::<std::collections::HashMap<String, String>>(s).unwrap_or_default()
}
const EFFECT_COLS: &str = "run_id, seq, decision_index, tool_name, idempotency_key, status, \
    request_json, response_json, error_json, ts_ms, \
    semantics, dispatch_mode, business_key, connector, dispatch_attempts, \
    next_dispatch_at_ms, external_ref, dispatch_claimed_by, \
    dispatch_claim_expires_at_ms, last_dispatch_error, scope";
fn effect_of(r: &Row) -> EffectRecord {
    EffectRecord {
        run_id: r.str(0), seq: r.i64(1), decision_index: r.i64(2), tool_name: r.str(3),
        idempotency_key: r.str(4), status: r.i32(5), request_json: r.str(6),
        response_json: r.str(7), error_json: r.str(8), ts_ms: r.i64(9),
        semantics: r.i32(10), dispatch_mode: r.i32(11),
        business_key: r.str(12), connector: r.str(13),
        dispatch_attempts: r.i32(14), next_dispatch_at_ms: r.i64(15),
        external_ref: r.str(16),
        dispatch_claimed_by: r.str(17), dispatch_claim_expires_at_ms: r.i64(18),
        last_dispatch_error: r.str(19), scope: r.str(20),
    }
}
const DECISION_COLS: &str =
    "run_id, seq, decision_index, model, request_json, response_json, rationale, policy_version, ts_ms";
fn decision_of(r: &Row) -> DecisionRecord {
    DecisionRecord {
        run_id: r.str(0), seq: r.i64(1), decision_index: r.i64(2), model: r.str(3),
        request_json: r.str(4), response_json: r.str(5), rationale: r.str(6),
        policy_version: r.str(7), ts_ms: r.i64(8),
    }
}
const OBLIGATION_COLS: &str = "run_id, seq, effect_key, kind, payload_json, status, ts_ms, \
    compensator_ref, attempts, max_attempts, next_attempt_at_ms, last_error, \
    claimed_by, claim_expires_at_ms, result_json";
fn obligation_of(r: &Row) -> ObligationRecord {
    ObligationRecord {
        run_id: r.str(0), seq: r.i64(1), effect_key: r.str(2), kind: r.str(3),
        payload_json: r.str(4), status: r.i32(5), ts_ms: r.i64(6),
        compensator_ref: r.str(7), attempts: r.i32(8), max_attempts: r.i32(9),
        next_attempt_at_ms: r.i64(10), last_error: r.str(11),
        claimed_by: r.str(12), claim_expires_at_ms: r.i64(13), result_json: r.str(14),
    }
}
const DEFAULT_MAX_ATTEMPTS: i32 = 5;
const DEFAULT_LEASE_MS: i64 = 60_000;

#[async_trait]
impl RunStore for SqlRunStore {
    async fn ping(&self) -> StoreResult<()> {
        // `SELECT 1` round-trips through the connection pool and confirms
        // the backing database is reachable + responsive. SQLite returns
        // instantly; Postgres exercises a real network hop so the
        // readiness signal reflects DB health.
        let _ = self.d().query_opt("SELECT 1", vec![]).await?;
        Ok(())
    }
    // ── run lifecycle ───────────────────────────────────────────────────────
    async fn begin_run(&self, app: &str, user: &str, session: &str, invocation: &str,
                       identity: &RunIdentity<'_>,
                       lease_owner: &str, lease_ttl_ms: i64) -> StoreResult<BeginRunResponse> {
        let ts = now_ms();
        let lease_exp = ts + lease_ttl_ms.max(0);
        if let Some(row) = self.d().query_opt(
            "SELECT run_id, seq_cursor FROM tape_runs WHERE app_name=?1 AND user_id=?2 AND session_id=?3 AND invocation_id=?4",
            vec![app.into(), user.into(), session.into(), invocation.into()]).await? {
            let run_id = row.str(0);
            let seq_cursor = row.i64(1);
            self.d().exec(
                "UPDATE tape_runs SET status=?2, lease_owner=?3, lease_expires_at_ms=?4 WHERE run_id=?1 AND status NOT IN (?5,?6)",
                vec![run_id.clone().into(), (RunStatus::Running as i32).into(), lease_owner.into(), lease_exp.into(),
                     (RunStatus::Terminal as i32).into(), (RunStatus::Stuck as i32).into()]).await?;
            let cur = self.get_run(&run_id).await?.unwrap();
            return Ok(BeginRunResponse { run_id, resumed: true, next_seq: seq_cursor, status: cur.status });
        }
        let run_id = uuid::Uuid::new_v4().to_string();
        let scopes_json = if identity.scopes_json.is_empty() { "[]" } else { identity.scopes_json };
        let labels_json = if identity.labels_json.is_empty() { "{}" } else { identity.labels_json };
        self.d().exec(
            "INSERT INTO tape_runs (run_id, app_name, user_id, session_id, invocation_id, status, seq_cursor, lease_owner, lease_expires_at_ms, started_at_ms, \
                tenant_id, actor, subject, agent_id, aiplex_instance_id, gateway_route, scopes_json, labels_json) \
             VALUES (?1,?2,?3,?4,?5,?6,0,?7,?8,?9, ?10,?11,?12,?13,?14,?15,?16,?17)",
            vec![run_id.clone().into(), app.into(), user.into(), session.into(), invocation.into(),
                 (RunStatus::Running as i32).into(), lease_owner.into(), lease_exp.into(), ts.into(),
                 identity.tenant_id.into(), identity.actor.into(), identity.subject.into(),
                 identity.agent_id.into(), identity.aiplex_instance_id.into(),
                 identity.gateway_route.into(), scopes_json.into(), labels_json.into()]).await?;
        // Populate the scope cache while we have the parsed scope set in
        // memory — the very first effect on this run hits the cache.
        self.cache_scopes(&run_id, parse_scopes(scopes_json));
        // Run-lifecycle journal: /tape/run/running/<app>/<user>/<session>/<run_id>
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
        self.d().exec("UPDATE tape_runs SET status=?2, lease_owner=?3, lease_expires_at_ms=?4 WHERE run_id=?1",
            vec![run_id.into(), (RunStatus::Running as i32).into(), lease_owner.into(), (now_ms() + lease_ttl_ms.max(0)).into()]).await?;
        self.get_run(run_id).await
    }
    async fn end_run(&self, run_id: &str, status: i32, detail_json: &str) -> StoreResult<Option<RunState>> {
        let ts = now_ms();
        self.d().exec("UPDATE tape_runs SET status=?2, ended_at_ms=?3, detail_json=?4, lease_owner='' WHERE run_id=?1",
            vec![run_id.into(), status.into(), ts.into(), detail_json.into()]).await?;
        // A terminal run won't accept more effects, so its scopes are
        // dead weight in the cache.
        self.evict_scopes(run_id);
        let cur = self.get_run(run_id).await?;
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
    async fn get_run(&self, run_id: &str) -> StoreResult<Option<RunState>> {
        Ok(self.d().query_opt(&format!("SELECT {RUN_COLS} FROM tape_runs WHERE run_id=?1"), vec![run_id.into()]).await?.map(|r| run_of(&r)))
    }
    async fn list_runs_to_recover(&self, now_ms: i64, limit: i64) -> StoreResult<Vec<RunState>> {
        let sql = format!(
            "SELECT {RUN_COLS} FROM tape_runs r WHERE status = ?1 \
             OR (status = ?2 AND lease_expires_at_ms < ?3) \
             OR (status = ?4 AND EXISTS (SELECT 1 FROM tape_signals s WHERE s.run_id = r.run_id AND s.delivered = 1 AND s.consumed = 0)) \
             LIMIT ?5");
        let rows = self.d().query(&sql, vec![(RunStatus::Runnable as i32).into(), (RunStatus::Running as i32).into(),
            now_ms.into(), (RunStatus::Waiting as i32).into(), limit.into()]).await?;
        Ok(rows.iter().map(run_of).collect())
    }
    async fn journal_range(&self, run_id: &str, from_seq: i64) -> StoreResult<Vec<JournalEntry>> {
        let rows = self.d().query(
            "SELECT seq, kind, payload_json, ts_ms, COALESCE(global_seq, 0), COALESCE(subject, ''), \
                    COALESCE(schema_version, 1), COALESCE(trace_id, ''), COALESCE(span_id, ''), COALESCE(parent_span_id, '') \
             FROM tape_journal WHERE run_id=?1 AND seq>=?2 ORDER BY seq",
            vec![run_id.into(), from_seq.into()]).await?;
        Ok(rows.iter().map(|r| JournalEntry {
            seq: r.i64(0), kind: r.str(1), payload_json: r.str(2), ts_ms: r.i64(3),
            global_seq: r.i64(4), subject: r.str(5), schema_version: r.i32(6),
            trace_id: r.str(7), span_id: r.str(8), parent_span_id: r.str(9),
        }).collect())
    }

    // ── compaction (PR 13) ─────────────────────────────────────────────────

    async fn list_compactable_runs(&self, before_ms: i64, limit: i64) -> StoreResult<Vec<RunState>> {
        // Coarse filter: terminal-ish status, never compacted, ended
        // before the cutoff. The reactor verifies "settled" (no open
        // obligations, no UNKNOWN effects) per row before invoking
        // compact_run on it.
        let lim = if limit > 0 { limit } else { 100 };
        let sql = format!(
            "SELECT {RUN_COLS} FROM tape_runs \
             WHERE status IN (?1, ?2, ?3, ?4) \
               AND compacted_at_ms = 0 \
               AND ended_at_ms > 0 AND ended_at_ms < ?5 \
             ORDER BY ended_at_ms ASC LIMIT ?6"
        );
        let rows = self.d().query(&sql, vec![
            (RunStatus::Terminal as i32).into(),
            (RunStatus::Failed as i32).into(),
            (RunStatus::Cancelled as i32).into(),
            (RunStatus::Stuck as i32).into(),
            before_ms.into(),
            lim.into(),
        ]).await?;
        Ok(rows.iter().map(run_of).collect())
    }

    async fn compact_run(&self, run_id: &str, ts_ms: i64) -> StoreResult<CompactReport> {
        // CompactRun is an admin RPC; it doesn't have to come from
        // `list_compactable_runs`, so the precondition has to live in
        // the store, not in the reactor. We require the run to be
        // terminal AND settled — zeroing decisions/effects on a
        // RUNNING / WAITING run breaks replay/recovery for an active
        // execution. Idempotent on a second call: a row with
        // compacted_at_ms > 0 short-circuits to already_compacted.
        let row = self.d().query_opt(
            "SELECT status, ended_at_ms, compacted_at_ms FROM tape_runs WHERE run_id = ?1",
            vec![run_id.into()]).await?
            .ok_or_else(|| StoreError::msg(format!("compact_run: run_id {run_id} not found")))?;
        let status = row.i64(0);
        let ended_at_ms = row.i64(1);
        let compacted_at_ms = row.i64(2);
        if compacted_at_ms > 0 {
            return Ok(CompactReport { already_compacted: true, ..Default::default() });
        }
        let terminal = matches!(
            RunStatus::try_from(status as i32),
            Ok(RunStatus::Terminal | RunStatus::Failed | RunStatus::Cancelled | RunStatus::Stuck),
        );
        if !terminal || ended_at_ms <= 0 {
            return Err(StoreError::msg(format!(
                "compact_run: run {run_id} is not terminal (status={status}, ended_at_ms={ended_at_ms}); refuse to zero live state")));
        }

        // Settlement check: bail out if there are unresolved obligations
        // or UNKNOWN effects. Belt-and-braces against a terminal run
        // that still has dangling external work.
        let open_obligations = self.d().query_opt(
            "SELECT COUNT(*) FROM tape_obligations WHERE run_id=?1 AND status IN (?2, ?3)",
            vec![
                run_id.into(),
                (ObligationStatus::Pending as i32).into(),
                (ObligationStatus::Committed as i32).into(),
            ]).await?.map(|r| r.i64(0)).unwrap_or(0);
        if open_obligations > 0 {
            return Err(StoreError::msg(format!(
                "compact_run: run {run_id} has {open_obligations} open obligation(s); not settled")));
        }
        let unknown_effects = self.d().query_opt(
            "SELECT COUNT(*) FROM tape_effects WHERE run_id=?1 AND status=?2",
            vec![run_id.into(), (EffectStatus::Unknown as i32).into()]).await?
                .map(|r| r.i64(0)).unwrap_or(0);
        if unknown_effects > 0 {
            return Err(StoreError::msg(format!(
                "compact_run: run {run_id} has {unknown_effects} UNKNOWN effect(s); not settled")));
        }

        // Sum the bytes-saved for telemetry BEFORE zeroing.
        let bytes_saved = self.d().query_opt(
            "SELECT \
                COALESCE(SUM(LENGTH(request_json) + LENGTH(response_json)), 0) \
             FROM tape_decisions WHERE run_id=?1",
            vec![run_id.into()]).await?.map(|r| r.i64(0)).unwrap_or(0) +
            self.d().query_opt(
            "SELECT \
                COALESCE(SUM(LENGTH(request_json) + LENGTH(response_json) + LENGTH(error_json)), 0) \
             FROM tape_effects WHERE run_id=?1",
            vec![run_id.into()]).await?.map(|r| r.i64(0)).unwrap_or(0);

        // Zero decision payloads — keep ts_ms / model / decision_index
        // / policy_version (audit envelope), drop request_json /
        // response_json / rationale (the LLM bodies).
        let dec_n = self.d().exec(
            "UPDATE tape_decisions SET request_json='', response_json='', rationale='' \
             WHERE run_id=?1 AND (request_json != '' OR response_json != '' OR rationale != '')",
            vec![run_id.into()]).await?;

        // Zero effect payloads — keep tool_name / idempotency_key /
        // business_key / connector / scope / status (audit envelope),
        // drop request_json / response_json / error_json.
        let eff_n = self.d().exec(
            "UPDATE tape_effects SET request_json='', response_json='', error_json='' \
             WHERE run_id=?1 AND (request_json != '' OR response_json != '' OR error_json != '')",
            vec![run_id.into()]).await?;

        // Stamp the run + emit a `run.compacted` journal entry. The
        // entry rides the existing outbox / event-bus path so AIPlex
        // sees the state change in its run timeline.
        self.d().exec(
            "UPDATE tape_runs SET compacted_at_ms=?2 WHERE run_id=?1",
            vec![run_id.into(), ts_ms.into()]).await?;
        let seq = self.next_seq(run_id).await.unwrap_or(0);
        let payload = serde_json::json!({
            "run_id": run_id,
            "compacted_at_ms": ts_ms,
            "decisions_zeroed": dec_n,
            "effects_zeroed": eff_n,
            "bytes_saved": bytes_saved,
        }).to_string();
        let _ = self.journal(run_id, seq, "run", &payload, ts_ms).await;

        Ok(CompactReport {
            decisions_zeroed: dec_n as i64,
            effects_zeroed: eff_n as i64,
            bytes_saved,
            already_compacted: false,
        })
    }

    // ── decisions ───────────────────────────────────────────────────────────
    async fn record_decision(&self, run_id: &str, decision_index: i64, model: &str, request_json: &str,
                             response_json: &str, rationale: &str, policy_version: &str) -> StoreResult<DecisionRecord> {
        let sql = format!("SELECT {DECISION_COLS} FROM tape_decisions WHERE run_id=?1 AND decision_index=?2");
        if let Some(row) = self.d().query_opt(&sql, vec![run_id.into(), decision_index.into()]).await? {
            return Ok(decision_of(&row));
        }
        let ts = now_ms();
        let seq = self.next_seq(run_id).await?;
        self.d().exec(
            "INSERT INTO tape_decisions (run_id, seq, decision_index, model, request_json, response_json, rationale, policy_version, ts_ms) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            vec![run_id.into(), seq.into(), decision_index.into(), model.into(), request_json.into(),
                 response_json.into(), rationale.into(), policy_version.into(), ts.into()]).await?;
        let payload = serde_json::json!({"decision_index": decision_index, "model": model, "policy_version": policy_version, "rationale": rationale}).to_string();
        self.journal(run_id, seq, "decision", &payload, ts).await?;
        Ok(DecisionRecord { run_id: run_id.into(), seq, decision_index, model: model.into(),
            request_json: request_json.into(), response_json: response_json.into(),
            rationale: rationale.into(), policy_version: policy_version.into(), ts_ms: ts })
    }
    async fn get_decision(&self, run_id: &str, decision_index: i64) -> StoreResult<Option<DecisionRecord>> {
        let sql = format!("SELECT {DECISION_COLS} FROM tape_decisions WHERE run_id=?1 AND decision_index=?2");
        Ok(self.d().query_opt(&sql, vec![run_id.into(), decision_index.into()]).await?.map(|r| decision_of(&r)))
    }

    // ── effects ─────────────────────────────────────────────────────────────
    async fn begin_effect(&self, run_id: &str, decision_index: i64, tool_name: &str, call_index: i32,
                          request_json: &str, custom_key: &str,
                          semantics: i32, dispatch_mode: i32,
                          business_key: &str, connector: &str,
                          scope: &str) -> StoreResult<EffectRecord> {
        // Default semantics/dispatch_mode are IDEMPOTENT/INLINE — preserves the
        // original contract for callers that don't opt into the outbox model.
        let sem = if semantics == EffectSemantics::Unspecified as i32 {
            EffectSemantics::Idempotent as i32
        } else { semantics };
        let dmode = if dispatch_mode == EffectDispatchMode::Unspecified as i32 {
            EffectDispatchMode::Inline as i32
        } else { dispatch_mode };
        // The safety rule the whole plan is built around: NON_IDEMPOTENT
        // upstreams cannot be dispatched inline. The tool body would call the
        // counterparty directly, and a crash mid-call leaves a no-blind-retry
        // ambiguity the server can't resolve — by construction the outbox
        // reactor + reconciler are the only safe path. Refuse at registration.
        if sem == EffectSemantics::NonIdempotent as i32 && dmode == EffectDispatchMode::Inline as i32 {
            return Err(StoreError::msg(
                "begin_effect: NON_IDEMPOTENT semantics requires OUTBOX dispatch \
                 (a non-idempotent counterparty cannot be safely re-driven inline)"));
        }
        // PR 12 item B: server-side scope enforcement on the wire.
        //
        // The Python SDK's @tape.effect(semantics="non_idempotent") refuses
        // at decoration time when scope is empty. A custom client or
        // outdated SDK could bypass that and send BeginEffectRequest
        // {semantics=NON_IDEMPOTENT, scope=""}. Without this server-side
        // check, the audit trail would carry the side effect but the
        // compactor's retained scope column would be empty — making
        // "was this attempted with authorization?" unanswerable after
        // archival.
        //
        // Failing on the wire instead of just at SDK construction is
        // exactly the "safety invariants enforced at construction AND
        // on the wire" principle from CLAUDE.md.
        if sem == EffectSemantics::NonIdempotent as i32 && scope.is_empty() {
            return Err(StoreError::denied(
                "<required>",
                "non_idempotent effects must declare an authorization scope on the wire"));
        }
        // P2 fix: a non-empty business_key without a connector is a
        // misconfiguration — cross-run dedupe is per-(connector, business_key)
        // and the partial UNIQUE index is meaningless (and footgun-prone)
        // without a routing key. Refuse the contract with a deterministic
        // error so it can't surface as a flaky unique-constraint failure on
        // the second writer.
        if !business_key.is_empty() && connector.is_empty() {
            return Err(StoreError::msg(
                "begin_effect: business_key requires connector \
                 (cross-run dedupe is per-(connector, business_key))"));
        }
        // Authorization (AIPlex integration PR 2). When the effect declares a
        // scope, it must appear in the run's `scopes` array. Empty scope
        // skips the check — idempotent effects can be unscoped. Defence-in-
        // depth: the SDK checks before getting here; the server re-checks so
        // an outdated or non-Python client can't bypass authz.
        if !scope.is_empty() {
            let run_scopes = self.run_scopes_for(run_id).await?;
            if !run_scopes.iter().any(|s| s == scope) {
                let ts = now_ms();
                let seq = self.next_seq(run_id).await?;
                let payload = serde_json::json!({
                    "tool": tool_name,
                    "decision_index": decision_index,
                    "required_scope": scope,
                    "granted_scopes": run_scopes,
                    "violation": "scope_not_granted",
                }).to_string();
                // Best-effort: even if the journal write fails we still deny.
                let _ = self.journal(run_id, seq, "policy", &payload, ts).await;
                return Err(StoreError::denied(
                    scope,
                    format!("effect {tool_name} requires scope {scope:?} not present on run {run_id}"),
                ));
            }
        }
        let key = if custom_key.is_empty() {
            derive_key(run_id, decision_index, tool_name, call_index)
        } else { custom_key.to_string() };
        // (run_id, idempotency_key) is the primary key → idempotent on retry.
        if let Some(rec) = self.get_effect(run_id, &key).await? {
            return Ok(rec);
        }
        // (connector, business_key) is the cross-run business-level dedupe key:
        // if a different run already claimed this business identity, return
        // *that* row rather than inserting a new effect that would race the
        // unique index — this is what "deduplicate by the operation's identity,
        // not by the call site" means.
        if !business_key.is_empty() && !connector.is_empty() {
            let sql = format!("SELECT {EFFECT_COLS} FROM tape_effects WHERE connector=?1 AND business_key=?2");
            if let Some(row) = self.d().query_opt(&sql, vec![connector.into(), business_key.into()]).await? {
                return Ok(effect_of(&row));
            }
        }
        let ts = now_ms();
        let seq = self.next_seq(run_id).await?;
        // OUTBOX effects are eligible immediately (next_dispatch_at_ms = ts).
        let next_dispatch = if dmode == EffectDispatchMode::Outbox as i32 { ts } else { 0 };
        self.d().exec(
            "INSERT INTO tape_effects (run_id, seq, decision_index, tool_name, idempotency_key, status, \
                 request_json, response_json, error_json, ts_ms, \
                 semantics, dispatch_mode, business_key, connector, dispatch_attempts, \
                 next_dispatch_at_ms, external_ref, dispatch_claimed_by, dispatch_claim_expires_at_ms, \
                 last_dispatch_error, scope) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,'','',?8, ?9,?10,?11,?12,0,?13,'','',0,'',?14)",
            vec![run_id.into(), seq.into(), decision_index.into(), tool_name.into(), key.clone().into(),
                 (EffectStatus::Pending as i32).into(), request_json.into(), ts.into(),
                 sem.into(), dmode.into(), business_key.into(), connector.into(), next_dispatch.into(),
                 scope.into()]).await?;
        let payload = serde_json::json!({
            "tool": tool_name, "decision_index": decision_index, "idempotency_key": key,
            "status": "pending", "semantics": sem, "dispatch_mode": dmode,
            "business_key": business_key, "connector": connector, "scope": scope,
        }).to_string();
        self.journal(run_id, seq, "effect", &payload, ts).await?;
        Ok(EffectRecord {
            run_id: run_id.into(), seq, decision_index, tool_name: tool_name.into(),
            idempotency_key: key, status: EffectStatus::Pending as i32, request_json: request_json.into(),
            response_json: String::new(), error_json: String::new(), ts_ms: ts,
            semantics: sem, dispatch_mode: dmode,
            business_key: business_key.into(), connector: connector.into(),
            dispatch_attempts: 0, next_dispatch_at_ms: next_dispatch,
            external_ref: String::new(),
            dispatch_claimed_by: String::new(), dispatch_claim_expires_at_ms: 0,
            last_dispatch_error: String::new(), scope: scope.into(),
        })
    }
    async fn complete_effect(&self, run_id: &str, key: &str, status: i32, response_json: &str, error_json: &str) -> StoreResult<Option<EffectRecord>> {
        let Some(existing) = self.get_effect(run_id, key).await? else { return Ok(None); };
        if existing.status != EffectStatus::Pending as i32 { return Ok(Some(existing)); }
        let ts = now_ms();
        self.d().exec("UPDATE tape_effects SET status=?3, response_json=?4, error_json=?5, ts_ms=?6 WHERE run_id=?1 AND idempotency_key=?2",
            vec![run_id.into(), key.into(), status.into(), response_json.into(), error_json.into(), ts.into()]).await?;
        let seq = self.next_seq(run_id).await?;
        let label = match EffectStatus::try_from(status) { Ok(EffectStatus::Confirmed) => "confirmed", Ok(EffectStatus::Failed) => "failed", Ok(EffectStatus::Unknown) => "unknown", _ => "completed" };
        self.journal(run_id, seq, "effect", &serde_json::json!({"tool": existing.tool_name, "idempotency_key": key, "status": label}).to_string(), ts).await?;
        Ok(Some(EffectRecord { status, response_json: response_json.into(), error_json: error_json.into(), ts_ms: ts, ..existing }))
    }
    async fn get_effect(&self, run_id: &str, key: &str) -> StoreResult<Option<EffectRecord>> {
        let sql = format!("SELECT {EFFECT_COLS} FROM tape_effects WHERE run_id=?1 AND idempotency_key=?2");
        Ok(self.d().query_opt(&sql, vec![run_id.into(), key.into()]).await?.map(|r| effect_of(&r)))
    }
    async fn reconcile_effect(&self, run_id: &str, key: &str, resolved_status: i32, response_json: &str, error_json: &str) -> StoreResult<Option<EffectRecord>> {
        let Some(existing) = self.get_effect(run_id, key).await? else { return Ok(None); };
        if existing.status == EffectStatus::Confirmed as i32 || existing.status == EffectStatus::Failed as i32 { return Ok(Some(existing)); }
        let ts = now_ms();
        self.d().exec("UPDATE tape_effects SET status=?3, response_json=?4, error_json=?5, ts_ms=?6 WHERE run_id=?1 AND idempotency_key=?2",
            vec![run_id.into(), key.into(), resolved_status.into(), response_json.into(), error_json.into(), ts.into()]).await?;
        let seq = self.next_seq(run_id).await?;
        self.journal(run_id, seq, "effect", &serde_json::json!({"tool": existing.tool_name, "idempotency_key": key, "status": "reconciled", "resolved_to": resolved_status}).to_string(), ts).await?;
        Ok(Some(EffectRecord { status: resolved_status, response_json: response_json.into(), error_json: error_json.into(), ts_ms: ts, ..existing }))
    }

    // ── outbox dispatch ─────────────────────────────────────────────────────
    async fn list_effects_to_dispatch(&self, now_ms: i64, connector: &str, limit: i64)
        -> StoreResult<Vec<EffectRecord>> {
        let now = if now_ms > 0 { now_ms } else { super::now_ms() };
        let lim = if limit > 0 { limit } else { 200 };
        let pending = EffectStatus::Pending as i32;
        let outbox = EffectDispatchMode::Outbox as i32;
        let rows = if connector.is_empty() {
            let sql = format!(
                "SELECT {EFFECT_COLS} FROM tape_effects \
                 WHERE status=?1 AND dispatch_mode=?2 AND next_dispatch_at_ms<=?3 \
                   AND (dispatch_claimed_by='' OR dispatch_claim_expires_at_ms<=?3) \
                 ORDER BY next_dispatch_at_ms ASC, ts_ms ASC LIMIT ?4");
            self.d().query(&sql, vec![pending.into(), outbox.into(), now.into(), lim.into()]).await?
        } else {
            let sql = format!(
                "SELECT {EFFECT_COLS} FROM tape_effects \
                 WHERE status=?1 AND dispatch_mode=?2 AND next_dispatch_at_ms<=?3 \
                   AND (dispatch_claimed_by='' OR dispatch_claim_expires_at_ms<=?3) \
                   AND connector=?4 \
                 ORDER BY next_dispatch_at_ms ASC, ts_ms ASC LIMIT ?5");
            self.d().query(&sql, vec![pending.into(), outbox.into(), now.into(),
                                       connector.into(), lim.into()]).await?
        };
        Ok(rows.iter().map(effect_of).collect())
    }

    async fn claim_effect_dispatch(&self, run_id: &str, key: &str, claimer: &str,
                                   lease_ttl_ms: i64, now_ms: i64)
        -> StoreResult<(bool, Option<EffectRecord>)> {
        let now = if now_ms > 0 { now_ms } else { super::now_ms() };
        let ttl = if lease_ttl_ms > 0 { lease_ttl_ms } else { DEFAULT_LEASE_MS };
        let lease_exp = now + ttl;
        let pending = EffectStatus::Pending as i32;
        let outbox = EffectDispatchMode::Outbox as i32;
        // Atomic CAS via the UPDATE … WHERE guard: succeeds only if the row is
        // claimable right now (PENDING + OUTBOX + due, with no live lease or
        // an expired one).
        let updated = self.d().exec(
            "UPDATE tape_effects \
             SET dispatch_claimed_by=?5, dispatch_claim_expires_at_ms=?6, ts_ms=?6 \
             WHERE run_id=?1 AND idempotency_key=?2 \
               AND status=?3 AND dispatch_mode=?4 AND next_dispatch_at_ms<=?7 \
               AND (dispatch_claimed_by='' OR dispatch_claim_expires_at_ms<=?7)",
            vec![run_id.into(), key.into(),
                 pending.into(), outbox.into(),
                 claimer.into(), lease_exp.into(), now.into()]).await?;
        let row = self.get_effect(run_id, key).await?;
        Ok((updated > 0, row))
    }

    async fn record_dispatch_attempt(&self, run_id: &str, key: &str,
                                     error: &str, next_dispatch_at_ms: i64)
        -> StoreResult<Option<EffectRecord>> {
        let Some(existing) = self.get_effect(run_id, key).await? else { return Ok(None); };
        let ts = now_ms();
        let new_attempts = existing.dispatch_attempts + 1;
        let to_unknown = next_dispatch_at_ms <= 0;
        // UNKNOWN here is the explicit "ambiguity" exit from the outbox loop —
        // do *not* retry on next tick; the reconciler must resolve via
        // observation. This is the safety claim for non-idempotent upstreams.
        let new_status = if to_unknown {
            EffectStatus::Unknown as i32
        } else {
            EffectStatus::Pending as i32
        };
        let next_at = if to_unknown { 0_i64 } else { next_dispatch_at_ms };
        self.d().exec(
            "UPDATE tape_effects \
             SET status=?3, dispatch_attempts=?4, last_dispatch_error=?5, \
                 next_dispatch_at_ms=?6, dispatch_claimed_by='', \
                 dispatch_claim_expires_at_ms=0, ts_ms=?7 \
             WHERE run_id=?1 AND idempotency_key=?2",
            vec![run_id.into(), key.into(), new_status.into(), new_attempts.into(),
                 error.into(), next_at.into(), ts.into()]).await?;
        let seq = self.next_seq(run_id).await?;
        let transition = if to_unknown { "dispatch-unknown" } else { "dispatch-retry-scheduled" };
        let status_label = if to_unknown { "unknown" } else { "pending" };
        self.journal(run_id, seq, "effect",
            &serde_json::json!({
                "tool": existing.tool_name, "idempotency_key": key,
                "status": status_label, "transition": transition,
                "dispatch_attempts": new_attempts, "error": error,
                "next_dispatch_at_ms": next_at,
            }).to_string(), ts).await?;
        Ok(self.get_effect(run_id, key).await?)
    }

    async fn record_external_observation(&self, run_id: &str, key: &str,
                                         resolution: i32, external_ref: &str,
                                         response_json: &str, error_json: &str,
                                         compensate_on_duplicate_kind: &str)
        -> StoreResult<Option<EffectRecord>> {
        let Some(existing) = self.get_effect(run_id, key).await? else { return Ok(None); };
        // Map EffectResolution → EffectStatus. ABSENT for IDEMPOTENT reopens as
        // PENDING (the outbox reactor may safely re-dispatch); ABSENT for
        // NON_IDEMPOTENT lands FAILED (a human or the saga decides what to do
        // — re-issuing without upstream confirmation would risk a duplicate).
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
        let ts = now_ms();
        // When ABSENT + IDEMPOTENT, also make the row immediately re-eligible
        // for the outbox dispatcher (next_dispatch_at_ms = now).
        let next_dispatch = if resolution == EffectResolution::Absent as i32
            && existing.semantics == EffectSemantics::Idempotent as i32
        { ts } else { existing.next_dispatch_at_ms };
        self.d().exec(
            "UPDATE tape_effects \
             SET status=?3, response_json=?4, error_json=?5, external_ref=?6, \
                 next_dispatch_at_ms=?7, dispatch_claimed_by='', \
                 dispatch_claim_expires_at_ms=0, ts_ms=?8 \
             WHERE run_id=?1 AND idempotency_key=?2",
            vec![run_id.into(), key.into(), target_status.into(),
                 response_json.into(), error_json.into(), external_ref.into(),
                 next_dispatch.into(), ts.into()]).await?;
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
            }).to_string(), ts).await?;
        // DUPLICATE → register a compensation obligation if the caller named the
        // inverse: an extra side effect landed at the counterparty (the same
        // logical operation, executed twice), and the saga must unwind it.
        if resolution == EffectResolution::Duplicate as i32 && !compensate_on_duplicate_kind.is_empty() {
            let payload = serde_json::json!({
                "reason": "duplicate-observed",
                "external_ref": external_ref,
                "idempotency_key": key,
            }).to_string();
            let _ = self.register_compensation(run_id, key, compensate_on_duplicate_kind,
                                               &payload, "", 0).await?;
        }
        Ok(self.get_effect(run_id, key).await?)
    }

    // ── obligations ─────────────────────────────────────────────────────────
    async fn register_compensation(&self, run_id: &str, effect_key: &str, kind: &str,
                                   payload_json: &str, compensator_ref: &str,
                                   max_attempts: i32) -> StoreResult<ObligationRecord> {
        // idempotent on (run_id, effect_key, kind): a repeat returns the existing row.
        let sql = format!("SELECT {OBLIGATION_COLS} FROM tape_obligations WHERE run_id=?1 AND effect_key=?2 AND kind=?3");
        if let Some(row) = self.d().query_opt(&sql, vec![run_id.into(), effect_key.into(), kind.into()]).await? {
            return Ok(obligation_of(&row));
        }
        let ts = now_ms();
        let seq = self.next_seq(run_id).await?;
        let status = ObligationStatus::Pending as i32;
        let max_att = if max_attempts <= 0 { DEFAULT_MAX_ATTEMPTS } else { max_attempts };
        self.d().exec(
            "INSERT INTO tape_obligations (run_id, seq, effect_key, kind, payload_json, status, ts_ms, \
             compensator_ref, attempts, max_attempts, next_attempt_at_ms, last_error, claimed_by, claim_expires_at_ms, result_json) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,0,?9,?10,'','',0,'')",
            vec![run_id.into(), seq.into(), effect_key.into(), kind.into(), payload_json.into(),
                 status.into(), ts.into(), compensator_ref.into(), max_att.into(), ts.into()]).await?;
        self.journal(run_id, seq, "obligation",
            &serde_json::json!({"effect_key": effect_key, "kind": kind, "status": "pending", "transition": "registered"}).to_string(),
            ts).await?;
        Ok(ObligationRecord {
            run_id: run_id.into(), seq, effect_key: effect_key.into(), kind: kind.into(),
            payload_json: payload_json.into(), status, ts_ms: ts,
            compensator_ref: compensator_ref.into(), attempts: 0, max_attempts: max_att,
            next_attempt_at_ms: ts, last_error: String::new(),
            claimed_by: String::new(), claim_expires_at_ms: 0, result_json: String::new(),
        })
    }
    async fn list_obligations(&self, run_id: &str, only_unresolved: bool, status_filter: i32) -> StoreResult<Vec<ObligationRecord>> {
        let mut sql = format!("SELECT {OBLIGATION_COLS} FROM tape_obligations WHERE run_id=?1");
        let mut params: Vec<Val> = vec![run_id.into()];
        if status_filter > 0 {
            sql.push_str(" AND status=?2");
            params.push(status_filter.into());
        } else if only_unresolved {
            sql.push_str(" AND status NOT IN (?2,?3)");
            params.push((ObligationStatus::Compensated as i32).into());
            params.push((ObligationStatus::Stuck as i32).into());
        }
        sql.push_str(" ORDER BY seq DESC");
        Ok(self.d().query(&sql, params).await?.iter().map(obligation_of).collect())
    }
    async fn list_unresolved_obligations(&self, now_ms: i64, include_pending: bool,
                                         include_stuck: bool, include_committed_expired: bool,
                                         limit: i64) -> StoreResult<Vec<ObligationRecord>> {
        // Build the OR list dynamically — every store backend will hit `idx_obligations_drain`
        // (status, next_attempt_at_ms) for the PENDING arm and `idx_obligations_lease`
        // (status, claim_expires_at_ms) for the COMMITTED-expired arm.
        let now = if now_ms > 0 { now_ms } else { super::now_ms() };
        let lim = if limit > 0 { limit } else { 500 };
        let mut clauses: Vec<String> = Vec::new();
        let mut params: Vec<Val> = Vec::new();
        let bind = |c: &mut Vec<String>, p: &mut Vec<Val>, expr: String, values: Vec<Val>| {
            let mut e = expr;
            for v in values {
                p.push(v);
                let n = p.len();
                e = e.replacen("?N", &format!("?{n}"), 1);
            }
            c.push(format!("({e})"));
        };
        if include_pending {
            bind(&mut clauses, &mut params,
                 "status=?N AND next_attempt_at_ms<=?N".into(),
                 vec![(ObligationStatus::Pending as i32).into(), now.into()]);
        }
        if include_committed_expired {
            bind(&mut clauses, &mut params,
                 "status=?N AND claim_expires_at_ms<=?N AND claim_expires_at_ms>0".into(),
                 vec![(ObligationStatus::Committed as i32).into(), now.into()]);
        }
        if include_stuck {
            bind(&mut clauses, &mut params,
                 "status=?N".into(),
                 vec![(ObligationStatus::Stuck as i32).into()]);
        }
        if clauses.is_empty() {
            return Ok(Vec::new());
        }
        let where_ = clauses.join(" OR ");
        let lim_n = params.len() + 1;
        params.push(lim.into());
        let sql = format!("SELECT {OBLIGATION_COLS} FROM tape_obligations WHERE {where_} ORDER BY next_attempt_at_ms ASC, ts_ms ASC LIMIT ?{lim_n}");
        Ok(self.d().query(&sql, params).await?.iter().map(obligation_of).collect())
    }
    async fn claim_obligation(&self, run_id: &str, obligation_seq: i64, claimer: &str,
                              lease_ttl_ms: i64, now_ms: i64)
        -> StoreResult<(bool, Option<ObligationRecord>)> {
        let now = if now_ms > 0 { now_ms } else { super::now_ms() };
        let ttl = if lease_ttl_ms > 0 { lease_ttl_ms } else { DEFAULT_LEASE_MS };
        let lease_exp = now + ttl;
        // Atomic CAS via the UPDATE … WHERE guard: succeeds only if the row is
        // claimable right now (PENDING + due, or COMMITTED + lease expired).
        let pending = ObligationStatus::Pending as i32;
        let committed = ObligationStatus::Committed as i32;
        // `?7` (now_ms) is bound once and referenced three times — pg_sql() rewrites
        // it to `$7`, which Postgres allows reusing. rusqlite's `?7` is positional
        // and reuses by index.
        let updated = self.d().exec(
            "UPDATE tape_obligations \
             SET status=?4, claimed_by=?5, claim_expires_at_ms=?6, ts_ms=?7 \
             WHERE run_id=?1 AND seq=?2 \
               AND ((status=?3 AND next_attempt_at_ms<=?7) \
                 OR (status=?4 AND claim_expires_at_ms<=?7 AND claim_expires_at_ms>0))",
            vec![run_id.into(), obligation_seq.into(),
                 pending.into(), committed.into(),
                 claimer.into(), lease_exp.into(), now.into()]).await?;
        let sql = format!("SELECT {OBLIGATION_COLS} FROM tape_obligations WHERE run_id=?1 AND seq=?2");
        let row = self.d().query_opt(&sql, vec![run_id.into(), obligation_seq.into()]).await?.map(|r| obligation_of(&r));
        if updated > 0 {
            if let Some(rec) = &row {
                let jseq = self.next_seq(run_id).await?;
                self.journal(run_id, jseq, "obligation",
                    &serde_json::json!({"obligation_seq": rec.seq, "effect_key": rec.effect_key, "kind": rec.kind, "status": "committed", "transition": "claimed", "claimer": claimer}).to_string(),
                    now).await?;
            }
            Ok((true, row))
        } else {
            Ok((false, row))
        }
    }
    async fn record_obligation_attempt(&self, run_id: &str, obligation_seq: i64,
                                       error: &str, next_attempt_at_ms: i64)
        -> StoreResult<Option<ObligationRecord>> {
        let sql = format!("SELECT {OBLIGATION_COLS} FROM tape_obligations WHERE run_id=?1 AND seq=?2");
        let Some(row) = self.d().query_opt(&sql, vec![run_id.into(), obligation_seq.into()]).await? else { return Ok(None); };
        let existing = obligation_of(&row);
        let now = super::now_ms();
        let new_attempts = existing.attempts + 1;
        let terminal = next_attempt_at_ms <= 0 || new_attempts >= existing.max_attempts;
        let (new_status, next_at) = if terminal {
            (ObligationStatus::Stuck as i32, 0_i64)
        } else {
            (ObligationStatus::Pending as i32, next_attempt_at_ms)
        };
        self.d().exec(
            "UPDATE tape_obligations \
             SET status=?3, attempts=?4, last_error=?5, next_attempt_at_ms=?6, \
                 claimed_by='', claim_expires_at_ms=0, ts_ms=?7 \
             WHERE run_id=?1 AND seq=?2",
            vec![run_id.into(), obligation_seq.into(), new_status.into(),
                 new_attempts.into(), error.into(), next_at.into(), now.into()]).await?;
        let transition = if terminal { "stuck" } else { "retry-scheduled" };
        let label = if terminal { "stuck" } else { "pending" };
        let jseq = self.next_seq(run_id).await?;
        self.journal(run_id, jseq, "obligation",
            &serde_json::json!({"obligation_seq": existing.seq, "effect_key": existing.effect_key, "kind": existing.kind,
                                "status": label, "transition": transition,
                                "attempts": new_attempts, "error": error,
                                "next_attempt_at_ms": next_at}).to_string(),
            now).await?;
        Ok(self.d().query_opt(&sql, vec![run_id.into(), obligation_seq.into()]).await?.map(|r| obligation_of(&r)))
    }
    async fn resolve_obligation(&self, run_id: &str, obligation_seq: i64, status: i32, result_json: &str) -> StoreResult<Option<ObligationRecord>> {
        let sql = format!("SELECT {OBLIGATION_COLS} FROM tape_obligations WHERE run_id=?1 AND seq=?2");
        let Some(row) = self.d().query_opt(&sql, vec![run_id.into(), obligation_seq.into()]).await? else { return Ok(None); };
        let existing = obligation_of(&row);
        let now = super::now_ms();
        // Terminal-only: ignore non-terminal status arguments to keep the state
        // machine honest (use claim/record_attempt for non-terminal transitions).
        let target = if status == ObligationStatus::Compensated as i32 || status == ObligationStatus::Stuck as i32 {
            status
        } else {
            ObligationStatus::Compensated as i32
        };
        self.d().exec(
            "UPDATE tape_obligations \
             SET status=?3, result_json=?4, claimed_by='', claim_expires_at_ms=0, ts_ms=?5 \
             WHERE run_id=?1 AND seq=?2",
            vec![run_id.into(), obligation_seq.into(), target.into(), result_json.into(), now.into()]).await?;
        let label = if target == ObligationStatus::Compensated as i32 { "compensated" } else { "stuck" };
        let jseq = self.next_seq(run_id).await?;
        self.journal(run_id, jseq, "obligation",
            &serde_json::json!({"obligation_seq": existing.seq, "effect_key": existing.effect_key, "kind": existing.kind,
                                "status": label, "transition": "resolved"}).to_string(),
            now).await?;
        Ok(self.d().query_opt(&sql, vec![run_id.into(), obligation_seq.into()]).await?.map(|r| obligation_of(&r)))
    }

    // ── budget ──────────────────────────────────────────────────────────────
    async fn set_budget(&self, run_id: &str, usd_cap: f64, token_cap: i64) -> StoreResult<BudgetState> {
        self.d().exec("INSERT INTO tape_budget (run_id, usd_cap, token_cap, usd_spent, tokens_spent) VALUES (?1,?2,?3,0,0) \
             ON CONFLICT(run_id) DO UPDATE SET usd_cap=excluded.usd_cap, token_cap=excluded.token_cap",
            vec![run_id.into(), usd_cap.into(), token_cap.into()]).await?;
        self.get_budget(run_id).await
    }
    async fn get_budget(&self, run_id: &str) -> StoreResult<BudgetState> {
        Ok(match self.d().query_opt("SELECT usd_cap, token_cap, usd_spent, tokens_spent FROM tape_budget WHERE run_id=?1", vec![run_id.into()]).await? {
            Some(r) => BudgetState { run_id: run_id.into(), usd_cap: r.f64(0), token_cap: r.i64(1), usd_spent: r.f64(2), tokens_spent: r.i64(3) },
            None => BudgetState { run_id: run_id.into(), usd_cap: 0.0, token_cap: 0, usd_spent: 0.0, tokens_spent: 0 },
        })
    }
    async fn charge_budget(&self, run_id: &str, usd: f64, tokens: i64) -> StoreResult<BudgetState> {
        self.d().exec("INSERT INTO tape_budget (run_id, usd_spent, tokens_spent) VALUES (?1,?2,?3) \
             ON CONFLICT(run_id) DO UPDATE SET usd_spent = tape_budget.usd_spent + ?2, tokens_spent = tape_budget.tokens_spent + ?3",
            vec![run_id.into(), usd.into(), tokens.into()]).await?;
        self.get_budget(run_id).await
    }

    // ── gates ───────────────────────────────────────────────────────────────
    async fn await_signal(&self, run_id: &str, gate_name: &str, payload_json: &str) -> StoreResult<(bool, String)> {
        let ts = now_ms();
        if let Some(row) = self.d().query_opt("SELECT delivered, resolution_json FROM tape_signals WHERE run_id=?1 AND gate_name=?2",
            vec![run_id.into(), gate_name.into()]).await? {
            if row.i64(0) == 1 {
                self.d().exec("UPDATE tape_signals SET consumed=1 WHERE run_id=?1 AND gate_name=?2", vec![run_id.into(), gate_name.into()]).await?;
                return Ok((true, row.str(1)));
            }
        }
        self.d().exec("INSERT INTO tape_signals (run_id, gate_name, context_json, awaited, created_at_ms) VALUES (?1,?2,?3,1,?4) \
             ON CONFLICT(run_id, gate_name) DO UPDATE SET awaited=1, context_json=excluded.context_json",
            vec![run_id.into(), gate_name.into(), payload_json.into(), ts.into()]).await?;
        let seq = self.next_seq(run_id).await?;
        self.d().exec("UPDATE tape_runs SET status=?2, waiting_on_gate=?3, lease_owner='' WHERE run_id=?1",
            vec![run_id.into(), (RunStatus::Waiting as i32).into(), gate_name.into()]).await?;
        self.journal(run_id, seq, "gate", &serde_json::json!({"gate": gate_name, "status": "waiting"}).to_string(), ts).await?;
        Ok((false, String::new()))
    }
    async fn send_signal(&self, run_id: &str, app: &str, user: &str, session: &str, gate_name: &str, resolution_json: &str) -> StoreResult<(String, i32)> {
        let ts = now_ms();
        let run_id = if !run_id.is_empty() { run_id.to_string() } else {
            self.d().query_opt("SELECT run_id FROM tape_runs WHERE app_name=?1 AND user_id=?2 AND session_id=?3 ORDER BY started_at_ms DESC LIMIT 1",
                vec![app.into(), user.into(), session.into()]).await?.map(|r| r.str(0))
                .ok_or_else(|| StoreError::msg("no run for that session"))?
        };
        self.d().exec("INSERT INTO tape_signals (run_id, gate_name, resolution_json, delivered, created_at_ms) VALUES (?1,?2,?3,1,?4) \
             ON CONFLICT(run_id, gate_name) DO UPDATE SET resolution_json=excluded.resolution_json, delivered=1",
            vec![run_id.clone().into(), gate_name.into(), resolution_json.into(), ts.into()]).await?;
        let mut run_status = RunStatus::Unspecified as i32;
        if let Some(row) = self.d().query_opt("SELECT waiting_on_gate FROM tape_runs WHERE run_id=?1 AND status=?2",
            vec![run_id.clone().into(), (RunStatus::Waiting as i32).into()]).await? {
            if row.str(0) == gate_name {
                self.d().exec("UPDATE tape_runs SET status=?2, waiting_on_gate='' WHERE run_id=?1",
                    vec![run_id.clone().into(), (RunStatus::Runnable as i32).into()]).await?;
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
        self.d().exec("INSERT INTO tape_sessions (app_name, user_id, session_id, state_json, last_update_time_ms) VALUES (?1,?2,?3,?4,?5) \
             ON CONFLICT(app_name, user_id, session_id) DO UPDATE SET state_json=excluded.state_json, last_update_time_ms=excluded.last_update_time_ms",
            vec![app.into(), user.into(), session_id.clone().into(), state.clone().into(), ts.into()]).await?;
        Ok(Session { app_name: app.into(), user_id: user.into(), session_id, state_json: state, events: vec![], last_update_time_ms: ts })
    }
    async fn get_session(&self, app: &str, user: &str, session: &str, max_events: i64) -> StoreResult<Option<Session>> {
        let Some(meta) = self.d().query_opt("SELECT state_json, last_update_time_ms FROM tape_sessions WHERE app_name=?1 AND user_id=?2 AND session_id=?3",
            vec![app.into(), user.into(), session.into()]).await? else { return Ok(None); };
        let limit = if max_events > 0 { max_events } else { i64::MAX };
        let rows = self.d().query("SELECT event_id, invocation_id, author, branch, content_json, actions_json, timestamp_ms \
             FROM tape_events WHERE app_name=?1 AND user_id=?2 AND session_id=?3 ORDER BY ord LIMIT ?4",
            vec![app.into(), user.into(), session.into(), limit.into()]).await?;
        let events = rows.iter().map(|r| EventRecord { id: r.str(0), invocation_id: r.str(1), author: r.str(2), branch: r.str(3),
            content_json: r.str(4), actions_json: r.str(5), timestamp_ms: r.i64(6) }).collect();
        Ok(Some(Session { app_name: app.into(), user_id: user.into(), session_id: session.into(),
            state_json: meta.str(0), events, last_update_time_ms: meta.i64(1) }))
    }
    async fn list_sessions(&self, app: &str, user: &str) -> StoreResult<Vec<Session>> {
        let rows = self.d().query("SELECT session_id, state_json, last_update_time_ms FROM tape_sessions WHERE app_name=?1 AND user_id=?2 ORDER BY last_update_time_ms DESC",
            vec![app.into(), user.into()]).await?;
        Ok(rows.iter().map(|r| Session { app_name: app.into(), user_id: user.into(), session_id: r.str(0), state_json: r.str(1), events: vec![], last_update_time_ms: r.i64(2) }).collect())
    }
    async fn delete_session(&self, app: &str, user: &str, session: &str) -> StoreResult<bool> {
        let n = self.d().exec("DELETE FROM tape_sessions WHERE app_name=?1 AND user_id=?2 AND session_id=?3", vec![app.into(), user.into(), session.into()]).await?;
        self.d().exec("DELETE FROM tape_events WHERE app_name=?1 AND user_id=?2 AND session_id=?3", vec![app.into(), user.into(), session.into()]).await?;
        Ok(n > 0)
    }
    async fn append_event(&self, app: &str, user: &str, session: &str, event: EventRecord, state_delta_json: &str) -> StoreResult<(EventRecord, i64)> {
        let ts = if event.timestamp_ms > 0 { event.timestamp_ms } else { now_ms() };
        let ord = self.d().query_opt("SELECT COALESCE(MAX(ord), -1) + 1 FROM tape_events WHERE app_name=?1 AND user_id=?2 AND session_id=?3",
            vec![app.into(), user.into(), session.into()]).await?.map(|r| r.i64(0)).unwrap_or(0);
        let cur = self.d().query_opt("SELECT state_json FROM tape_sessions WHERE app_name=?1 AND user_id=?2 AND session_id=?3",
            vec![app.into(), user.into(), session.into()]).await?.map(|r| r.str(0)).unwrap_or_else(|| "{}".into());
        let delta = if state_delta_json.is_empty() { "{}".to_string() } else { state_delta_json.to_string() };
        let merged = merge_json(&cur, &delta);
        self.d().tx(vec![
            ("INSERT INTO tape_events (app_name, user_id, session_id, ord, event_id, invocation_id, author, branch, content_json, actions_json, timestamp_ms) \
              VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)".to_string(),
             vec![app.into(), user.into(), session.into(), ord.into(), event.id.clone().into(), event.invocation_id.clone().into(),
                  event.author.clone().into(), event.branch.clone().into(), event.content_json.clone().into(), event.actions_json.clone().into(), ts.into()]),
            ("INSERT INTO tape_sessions (app_name, user_id, session_id, state_json, last_update_time_ms) VALUES (?1,?2,?3,?4,?5) \
              ON CONFLICT(app_name, user_id, session_id) DO UPDATE SET state_json=excluded.state_json, last_update_time_ms=excluded.last_update_time_ms".to_string(),
             vec![app.into(), user.into(), session.into(), merged.into(), ts.into()]),
        ]).await?;
        Ok((EventRecord { timestamp_ms: ts, ..event }, ts))
    }

    // ── reconciliation ──────────────────────────────────────────────────────
    async fn list_pending_effects(&self, older_than_ms: i64, include_pending: bool, include_unknown: bool, limit: i64) -> StoreResult<Vec<EffectRecord>> {
        let (ip, iu) = if !include_pending && !include_unknown { (true, true) } else { (include_pending, include_unknown) };
        let unk: i64 = if iu { EffectStatus::Unknown as i64 } else { -1 };
        let pend: i64 = if ip { EffectStatus::Pending as i64 } else { -1 };
        let sql = format!(
            "SELECT {EFFECT_COLS} FROM tape_effects WHERE status = ?2 OR (status = ?3 AND (?4 = 0 OR ts_ms < ?4)) ORDER BY ts_ms LIMIT ?1");
        Ok(self.d().query(&sql, vec![limit.max(1).into(), unk.into(), pend.into(), older_than_ms.into()]).await?.iter().map(effect_of).collect())
    }

    // ── timers ──────────────────────────────────────────────────────────────
    async fn set_timer(&self, run_id: &str, timer_id: &str, fire_at_ms: i64, kind: &str, payload_json: &str) -> StoreResult<TimerRecord> {
        let tid = if timer_id.is_empty() { uuid::Uuid::new_v4().to_string() } else { timer_id.to_string() };
        let ts = now_ms();
        self.d().exec(
            "INSERT INTO tape_timers (run_id, timer_id, fire_at_ms, kind, payload_json, fired, created_at_ms) VALUES (?1,?2,?3,?4,?5,0,?6) \
             ON CONFLICT(run_id, timer_id) DO UPDATE SET fire_at_ms=excluded.fire_at_ms, kind=excluded.kind, payload_json=excluded.payload_json, fired=0",
            vec![run_id.into(), tid.clone().into(), fire_at_ms.into(), kind.into(), payload_json.into(), ts.into()]).await?;
        let row = self.d().query_opt("SELECT run_id, timer_id, fire_at_ms, kind, payload_json, fired, created_at_ms FROM tape_timers WHERE run_id=?1 AND timer_id=?2",
            vec![run_id.into(), tid.into()]).await?.unwrap();
        Ok(timer_of(&row))
    }
    async fn cancel_timer(&self, run_id: &str, timer_id: &str) -> StoreResult<bool> {
        Ok(self.d().exec("DELETE FROM tape_timers WHERE run_id=?1 AND timer_id=?2", vec![run_id.into(), timer_id.into()]).await? > 0)
    }
    async fn list_due_timers(&self, now_ms: i64, limit: i64, claim: bool) -> StoreResult<Vec<TimerRecord>> {
        let rows = self.d().query(
            "SELECT run_id, timer_id, fire_at_ms, kind, payload_json, fired, created_at_ms FROM tape_timers WHERE fired=0 AND fire_at_ms <= ?1 ORDER BY fire_at_ms LIMIT ?2",
            vec![now_ms.into(), limit.max(1).into()]).await?;
        let mut out = Vec::new();
        for row in &rows {
            let t = timer_of(row);
            if claim {
                // Claim it: only this reactor proceeds if the conditional update hits.
                let n = self.d().exec("UPDATE tape_timers SET fired=1 WHERE run_id=?1 AND timer_id=?2 AND fired=0", vec![t.run_id.clone().into(), t.timer_id.clone().into()]).await?;
                if n == 1 { out.push(TimerRecord { fired: true, ..t }); }
            } else {
                out.push(t);
            }
        }
        Ok(out)
    }

    // ── reactive key-value store ────────────────────────────────────────────
    async fn write_value(&self, namespace: &str, key: &str, value_json: &str, if_version: i64, writer: &str) -> StoreResult<ValueRecord> {
        // Best-effort CAS: read, check, then unconditional upsert (TOCTOU race
        // is acceptable for v1; a single-statement conditional UPDATE per
        // backend would tighten it).
        if if_version >= 0 {
            let cur = self.get_value(namespace, key).await?;
            let cur_v = cur.as_ref().map(|r| r.version).unwrap_or(0);
            if cur_v != if_version {
                return Err(StoreError::msg(format!("write_value: version conflict (have {cur_v}, expected {if_version})")));
            }
        }
        let ts = now_ms();
        self.d().exec(
            "INSERT INTO tape_values (namespace, key, value_json, version, ts_ms, writer, deleted) \
             VALUES (?1, ?2, ?3, 1, ?4, ?5, 0) \
             ON CONFLICT(namespace, key) DO UPDATE SET \
               value_json = excluded.value_json, \
               version = tape_values.version + 1, \
               ts_ms = excluded.ts_ms, \
               writer = excluded.writer, \
               deleted = 0",
            vec![namespace.into(), key.into(), value_json.into(), ts.into(), writer.into()],
        ).await?;
        let rec = self.get_value(namespace, key).await?.ok_or_else(|| StoreError::msg("write_value: row vanished after upsert"))?;
        // Journal: /tape/value/changed/<ns>/<key>. run_id is empty; the value
        // surface is run-agnostic. Errors here are best-effort (the value
        // write committed; a missed journal row is recoverable).
        let payload = serde_json::json!({
            "namespace": namespace, "key": key, "version": rec.version, "writer": writer,
            "value": {"namespace": namespace, "key": key, "value_json": value_json, "version": rec.version},
        }).to_string();
        let _ = self.journal_full("", 0, "value", &subjects::derive("value", &serde_json::json!({
            "namespace": namespace, "key": key,
        })), &payload, ts, 1, "", "", "").await;
        Ok(rec)
    }
    async fn get_value(&self, namespace: &str, key: &str) -> StoreResult<Option<ValueRecord>> {
        Ok(self.d().query_opt(
            "SELECT namespace, key, value_json, version, ts_ms, writer, deleted FROM tape_values \
             WHERE namespace = ?1 AND key = ?2",
            vec![namespace.into(), key.into()],
        ).await?.map(|r| ValueRecord {
            namespace: r.str(0), key: r.str(1), value_json: r.str(2),
            version: r.i64(3), ts_ms: r.i64(4), writer: r.str(5),
            deleted: r.i64(6) != 0,
        }))
    }
    async fn get_value_if_newer(&self, namespace: &str, key: &str, from_version: i64) -> StoreResult<Option<ValueRecord>> {
        let cur = self.get_value(namespace, key).await?;
        Ok(cur.filter(|r| r.version > from_version))
    }
    async fn delete_value(&self, namespace: &str, key: &str) -> StoreResult<(bool, i64)> {
        // tombstone: keep the row, bump version, set deleted=1, so subscribers
        // see the delete as a ValueEvent.
        let cur = self.get_value(namespace, key).await?;
        if cur.is_none() {
            return Ok((false, 0));
        }
        let ts = now_ms();
        self.d().exec(
            "UPDATE tape_values SET version = version + 1, ts_ms = ?3, value_json = '', deleted = 1 \
             WHERE namespace = ?1 AND key = ?2",
            vec![namespace.into(), key.into(), ts.into()],
        ).await?;
        let new_v = self.get_value(namespace, key).await?.map(|r| r.version).unwrap_or(0);
        // Journal: /tape/value/deleted/<ns>/<key>.
        let payload = serde_json::json!({
            "namespace": namespace, "key": key, "version": new_v, "deleted": true,
        }).to_string();
        let _ = self.journal_full("", 0, "value", &subjects::derive("value", &serde_json::json!({
            "namespace": namespace, "key": key, "deleted": true,
        })), &payload, ts, 1, "", "", "").await;
        Ok((true, new_v))
    }

    // ── the WAL tail ────────────────────────────────────────────────────────
    async fn events_since(&self, from_ts_ms: i64, run_id: &str, kind: &str, limit: i64) -> StoreResult<Vec<EventEntry>> {
        let rows = self.d().query(
            "SELECT run_id, seq, kind, payload_json, ts_ms, COALESCE(global_seq, 0), COALESCE(subject, ''), \
                    COALESCE(schema_version, 1), COALESCE(trace_id, ''), COALESCE(span_id, ''), COALESCE(parent_span_id, '') \
             FROM tape_journal WHERE ts_ms >= ?1 AND (?2 = '' OR run_id = ?2) AND (?3 = '' OR kind = ?3) \
             ORDER BY ts_ms, run_id, seq LIMIT ?4",
            vec![from_ts_ms.into(), run_id.into(), kind.into(), limit.max(1).into()]).await?;
        Ok(rows.iter().map(|r| EventEntry {
            run_id: r.str(0), seq: r.i64(1), kind: r.str(2), payload_json: r.str(3), ts_ms: r.i64(4),
            global_seq: r.i64(5), subject: r.str(6), schema_version: r.i32(7),
            trace_id: r.str(8), span_id: r.str(9), parent_span_id: r.str(10),
        }).collect())
    }

    // ── event-bus surface (subject-routed, global_seq-cursored) ─────────────
    async fn events_by_subject(&self, from_global_seq: i64, subject_pattern: &str, limit: i64)
        -> StoreResult<Vec<EventEntry>> {
        let like = subjects::pattern_to_sql_like(subject_pattern);
        let rows = self.d().query(
            "SELECT run_id, seq, kind, payload_json, ts_ms, COALESCE(global_seq, 0), COALESCE(subject, ''), \
                    COALESCE(schema_version, 1), COALESCE(trace_id, ''), COALESCE(span_id, ''), COALESCE(parent_span_id, '') \
             FROM tape_journal WHERE COALESCE(global_seq, 0) > ?1 AND COALESCE(subject, '') LIKE ?2 ESCAPE '\\' \
             ORDER BY global_seq ASC LIMIT ?3",
            vec![from_global_seq.into(), like.into(), limit.max(1).into()]).await?;
        let mut out = Vec::new();
        for r in &rows {
            let e = EventEntry {
                run_id: r.str(0), seq: r.i64(1), kind: r.str(2), payload_json: r.str(3), ts_ms: r.i64(4),
                global_seq: r.i64(5), subject: r.str(6), schema_version: r.i32(7),
                trace_id: r.str(8), span_id: r.str(9), parent_span_id: r.str(10),
            };
            // Second-pass strict matcher: SQL LIKE can't enforce single-segment
            // semantics, so we filter precisely here.
            if subjects::matches(subject_pattern, &e.subject) {
                out.push(e);
            }
        }
        Ok(out)
    }
    async fn read_journal_after(&self, from_global_seq: i64, limit: i64) -> StoreResult<Vec<EventEntry>> {
        let rows = self.d().query(
            "SELECT run_id, seq, kind, payload_json, ts_ms, COALESCE(global_seq, 0), COALESCE(subject, ''), \
                    COALESCE(schema_version, 1), COALESCE(trace_id, ''), COALESCE(span_id, ''), COALESCE(parent_span_id, '') \
             FROM tape_journal WHERE COALESCE(global_seq, 0) > ?1 ORDER BY global_seq ASC LIMIT ?2",
            vec![from_global_seq.into(), limit.max(1).into()]).await?;
        Ok(rows.iter().map(|r| EventEntry {
            run_id: r.str(0), seq: r.i64(1), kind: r.str(2), payload_json: r.str(3), ts_ms: r.i64(4),
            global_seq: r.i64(5), subject: r.str(6), schema_version: r.i32(7),
            trace_id: r.str(8), span_id: r.str(9), parent_span_id: r.str(10),
        }).collect())
    }

    fn journal_notify(&self) -> Arc<tokio::sync::Notify> { self.notify.clone() }

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
        // Was this reaction_id already present? We read before the upsert so we
        // can detect first-creation deterministically across both SQLite and
        // Postgres (RETURNING (xmax = 0) AS inserted is PG-only). The
        // bootstrap_from_head flag is only honoured on first creation — a
        // re-registration must never reset cursors (treats a redeploy as a
        // no-op for the head-bootstrap semantics).
        let pre_exists = self.d().query_opt(
            "SELECT 1 FROM tape_reactions WHERE reaction_id=?1",
            vec![rid.clone().into()],
        ).await?.is_some();
        // Upsert. Use ON CONFLICT (reaction_id) DO UPDATE … on both backends.
        self.d().exec(
            "INSERT INTO tape_reactions (reaction_id, name, subject_pattern, predicate_cel, handler_kind, \
                                          agent_app, publish_target, max_concurrency, rate_limit_per_s, \
                                          debounce_ms, retry_max, retry_backoff_ms, dlq_after_n, num_shards, \
                                          created_at_ms, deleted) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,0) \
             ON CONFLICT(reaction_id) DO UPDATE SET \
                 name=excluded.name, subject_pattern=excluded.subject_pattern, predicate_cel=excluded.predicate_cel, \
                 handler_kind=excluded.handler_kind, agent_app=excluded.agent_app, publish_target=excluded.publish_target, \
                 max_concurrency=excluded.max_concurrency, rate_limit_per_s=excluded.rate_limit_per_s, \
                 debounce_ms=excluded.debounce_ms, retry_max=excluded.retry_max, retry_backoff_ms=excluded.retry_backoff_ms, \
                 dlq_after_n=excluded.dlq_after_n, num_shards=excluded.num_shards, deleted=0",
            vec![rid.clone().into(), r.name.clone().into(), r.subject_pattern.clone().into(),
                 r.predicate_cel.clone().into(), (r.handler_kind as i32).into(),
                 r.agent_app.clone().into(), r.publish_target.clone().into(),
                 max_conc.into(), r.rate_limit_per_s.into(), r.debounce_ms.into(),
                 retry_max.into(), backoff.into(), dlq.into(), num_shards.into(),
                 created.into()],
        ).await?;
        // First-time creation + bootstrap_from_head: seed each shard's cursor
        // at the current journal head so the reaction skips the entire backlog
        // and only sees entries written after registration.
        if !pre_exists && r.bootstrap_from_head {
            let head_row = self.d().query_opt(
                "SELECT COALESCE(MAX(global_seq), 0) FROM tape_journal",
                vec![],
            ).await?;
            let head = head_row.map(|r| r.i64(0)).unwrap_or(0);
            for s in 0..num_shards {
                self.d().exec(
                    "INSERT INTO tape_reaction_cursors (reaction_id, shard, last_global_seq, last_processed_at_ms) \
                     VALUES (?1,?2,?3,?4) ON CONFLICT(reaction_id, shard) DO NOTHING",
                    vec![rid.clone().into(), s.into(), head.into(), now.into()],
                ).await?;
            }
        }
        Ok(self
            .list_reactions("")
            .await?
            .into_iter()
            .find(|x| x.reaction_id == rid)
            .ok_or_else(|| StoreError::msg("register_reaction: row vanished after upsert"))?)
    }
    async fn deregister_reaction(&self, reaction_id: &str) -> StoreResult<bool> {
        let n = self.d().exec(
            "UPDATE tape_reactions SET deleted=1 WHERE reaction_id=?1",
            vec![reaction_id.into()],
        ).await?;
        Ok(n > 0)
    }
    async fn list_reactions(&self, subject_pattern: &str) -> StoreResult<Vec<Reaction>> {
        let (sql, params): (String, Vec<Val>) = if subject_pattern.is_empty() {
            ("SELECT reaction_id, name, subject_pattern, predicate_cel, handler_kind, agent_app, publish_target, \
                     max_concurrency, rate_limit_per_s, debounce_ms, retry_max, retry_backoff_ms, dlq_after_n, \
                     num_shards, created_at_ms, deleted \
              FROM tape_reactions WHERE deleted=0 ORDER BY created_at_ms".to_string(), vec![])
        } else {
            ("SELECT reaction_id, name, subject_pattern, predicate_cel, handler_kind, agent_app, publish_target, \
                     max_concurrency, rate_limit_per_s, debounce_ms, retry_max, retry_backoff_ms, dlq_after_n, \
                     num_shards, created_at_ms, deleted \
              FROM tape_reactions WHERE deleted=0 AND subject_pattern=?1 ORDER BY created_at_ms".to_string(),
             vec![subject_pattern.into()])
        };
        let rows = self.d().query(&sql, params).await?;
        Ok(rows.iter().map(|r| Reaction {
            reaction_id: r.str(0), name: r.str(1), subject_pattern: r.str(2), predicate_cel: r.str(3),
            handler_kind: r.i32(4), agent_app: r.str(5), publish_target: r.str(6),
            max_concurrency: r.i32(7), rate_limit_per_s: r.i32(8), debounce_ms: r.i32(9),
            retry_max: r.i32(10), retry_backoff_ms: r.i32(11), dlq_after_n: r.i32(12),
            num_shards: r.i32(13), created_at_ms: r.i64(14), deleted: r.i64(15) != 0,
            // Storage-only column; the proto flag is a registration-time intent,
            // not a queryable property of the stored row.
            bootstrap_from_head: false,
        }).collect())
    }
    async fn get_reaction_cursor(&self, reaction_id: &str, shard: i32) -> StoreResult<i64> {
        Ok(self.d().query_opt(
            "SELECT last_global_seq FROM tape_reaction_cursors WHERE reaction_id=?1 AND shard=?2",
            vec![reaction_id.into(), shard.into()],
        ).await?.map(|r| r.i64(0)).unwrap_or(0))
    }
    async fn set_reaction_cursor(&self, reaction_id: &str, shard: i32, global_seq: i64, now_ms: i64)
        -> StoreResult<()> {
        self.d().exec(
            "INSERT INTO tape_reaction_cursors (reaction_id, shard, last_global_seq, last_processed_at_ms) \
             VALUES (?1,?2,?3,?4) ON CONFLICT(reaction_id, shard) DO UPDATE SET \
               last_global_seq=excluded.last_global_seq, last_processed_at_ms=excluded.last_processed_at_ms",
            vec![reaction_id.into(), shard.into(), global_seq.into(), now_ms.into()],
        ).await.map(|_| ())
    }

    // ── tasks ───────────────────────────────────────────────────────────────
    async fn create_task(&self, t: &Task) -> StoreResult<Task> {
        let tid = if t.task_id.is_empty() { uuid::Uuid::new_v4().to_string() } else { t.task_id.clone() };
        let created = if t.created_at_ms > 0 { t.created_at_ms } else { now_ms() };
        // ON CONFLICT (reaction_id, shard, source_global_seq) DO NOTHING; if a
        // duplicate matcher run hits, we just keep the existing row.
        self.d().exec(
            "INSERT INTO tape_tasks (task_id, reaction_id, shard, source_run_id, source_global_seq, subject, payload_json, \
                                     status, attempts, next_attempt_at_ms, lease_owner, lease_expires_at_ms, last_error, \
                                     created_at_ms, trace_id, parent_span_id) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,0,0,'',0,'',?9,?10,?11) \
             ON CONFLICT (reaction_id, shard, source_global_seq) DO NOTHING",
            vec![tid.clone().into(), t.reaction_id.clone().into(), t.shard.into(),
                 t.source_run_id.clone().into(), t.source_global_seq.into(),
                 t.subject.clone().into(), t.payload_json.clone().into(),
                 (TaskStatus::Pending as i32).into(),
                 created.into(),
                 t.trace_id.clone().into(), t.parent_span_id.clone().into()],
        ).await?;
        // Return the row that ended up there (may be the existing one).
        let row = self.d().query_opt(
            "SELECT task_id, reaction_id, shard, source_run_id, source_global_seq, subject, payload_json, \
                    status, attempts, next_attempt_at_ms, lease_owner, lease_expires_at_ms, last_error, \
                    created_at_ms, trace_id, parent_span_id \
             FROM tape_tasks WHERE reaction_id=?1 AND shard=?2 AND source_global_seq=?3",
            vec![t.reaction_id.clone().into(), t.shard.into(), t.source_global_seq.into()],
        ).await?.ok_or_else(|| StoreError::msg("create_task: row vanished after upsert"))?;
        Ok(task_of(&row))
    }
    async fn claim_tasks(&self, reaction_id: &str, shard: i32, owner: &str, lease_ms: i64,
                         max: i32, now_ms: i64) -> StoreResult<Vec<Task>> {
        if owner.is_empty() {
            // A claim with an empty owner would set `lease_owner=''` on the task,
            // which is the same sentinel new (un-leased) rows carry — that
            // breaks `complete_task` / `nack_task` lease checks (they'd then
            // match unclaimed PENDING tasks). Reject up front.
            return Err(StoreError::msg("claim_tasks: owner is required"));
        }
        let lease_ms = if lease_ms > 0 { lease_ms } else { 60_000 };
        let max = if max > 0 { max } else { 16 };
        let now = if now_ms > 0 { now_ms } else { super::now_ms() };
        let lease_exp = now + lease_ms;
        // Eligibility: PENDING with next_attempt_at_ms <= now, OR CLAIMED with
        // lease_expires_at_ms < now (stolen lease — the previous owner died).
        let pending = TaskStatus::Pending as i32;
        let claimed = TaskStatus::Claimed as i32;

        if self.d().is_postgres() {
            // Postgres path: SELECT … FOR UPDATE SKIP LOCKED then UPDATE.
            // We do it in a single CTE-style UPDATE … RETURNING for atomicity.
            let shard_clause = if shard < 0 { "" } else { " AND shard=?7" };
            let sql = format!(
                "WITH picked AS ( \
                   SELECT task_id FROM tape_tasks \
                   WHERE reaction_id=?1 \
                     AND ((status=?2 AND next_attempt_at_ms<=?3) OR (status=?4 AND lease_expires_at_ms<?3)) {shard_clause} \
                   ORDER BY next_attempt_at_ms, created_at_ms \
                   FOR UPDATE SKIP LOCKED \
                   LIMIT ?5 \
                 ) \
                 UPDATE tape_tasks t SET status=?4, lease_owner=?6, lease_expires_at_ms=?8, attempts=attempts+1 \
                 FROM picked WHERE t.task_id = picked.task_id \
                 RETURNING t.task_id, t.reaction_id, t.shard, t.source_run_id, t.source_global_seq, t.subject, t.payload_json, \
                           t.status, t.attempts, t.next_attempt_at_ms, t.lease_owner, t.lease_expires_at_ms, t.last_error, \
                           t.created_at_ms, t.trace_id, t.parent_span_id"
            );
            let mut params: Vec<Val> = vec![
                reaction_id.into(), pending.into(), now.into(), claimed.into(),
                (max as i64).into(), owner.into(),
            ];
            if shard >= 0 { params.push(shard.into()); }
            params.push(lease_exp.into());
            let rows = self.d().query(&sql, params).await?;
            return Ok(rows.iter().map(task_of).collect());
        }

        // SQLite path: pick candidates, then claim each with a conditional UPDATE.
        let shard_clause = if shard < 0 { "".to_string() } else { format!(" AND shard={}", shard) };
        let sql = format!(
            "SELECT task_id FROM tape_tasks \
             WHERE reaction_id=?1 \
               AND ((status=?2 AND next_attempt_at_ms<=?3) OR (status=?4 AND lease_expires_at_ms<?3)) {shard_clause} \
             ORDER BY next_attempt_at_ms, created_at_ms LIMIT ?5"
        );
        let candidates = self.d().query(
            &sql,
            vec![reaction_id.into(), pending.into(), now.into(), claimed.into(), (max as i64).into()],
        ).await?;
        let mut out = Vec::new();
        for c in candidates {
            let tid = c.str(0);
            let n = self.d().exec(
                "UPDATE tape_tasks SET status=?2, lease_owner=?3, lease_expires_at_ms=?4, attempts=attempts+1 \
                 WHERE task_id=?1 AND ((status=?5 AND next_attempt_at_ms<=?6) OR (status=?2 AND lease_expires_at_ms<?6))",
                vec![tid.clone().into(), claimed.into(), owner.into(), lease_exp.into(), pending.into(), now.into()],
            ).await?;
            if n == 0 { continue; }
            let row = self.d().query_opt(
                "SELECT task_id, reaction_id, shard, source_run_id, source_global_seq, subject, payload_json, \
                        status, attempts, next_attempt_at_ms, lease_owner, lease_expires_at_ms, last_error, \
                        created_at_ms, trace_id, parent_span_id \
                 FROM tape_tasks WHERE task_id=?1",
                vec![tid.into()],
            ).await?;
            if let Some(r) = row { out.push(task_of(&r)); }
        }
        Ok(out)
    }
    async fn complete_task(&self, task_id: &str, owner: &str) -> StoreResult<Option<Task>> {
        if owner.is_empty() {
            // owner="" would match unleased PENDING rows (lease_owner='' is the
            // default), letting a caller mark not-yet-claimed work as DONE and
            // silently drop it. The contract is: claim → complete (with the
            // same owner). Reject empty owner unambiguously.
            return Err(StoreError::msg("complete_task: owner is required"));
        }
        let now = now_ms();
        // Conditional UPDATE includes both `lease_owner = owner` AND
        // `status = CLAIMED`. This (a) prevents a stale caller from clobbering
        // a row another dispatcher has reclaimed after lease expiry, and (b)
        // refuses to complete a task that was never claimed in the first place.
        let n = self.d().exec(
            "UPDATE tape_tasks SET status=?2, completed_at_ms=?3, lease_owner='', lease_expires_at_ms=0 \
             WHERE task_id=?1 AND lease_owner=?4 AND status=?5",
            vec![task_id.into(), (TaskStatus::Done as i32).into(), now.into(), owner.into(),
                 (TaskStatus::Claimed as i32).into()],
        ).await?;
        if n == 0 { return Ok(None); }
        let row = self.d().query_opt(
            "SELECT task_id, reaction_id, shard, source_run_id, source_global_seq, subject, payload_json, \
                    status, attempts, next_attempt_at_ms, lease_owner, lease_expires_at_ms, last_error, \
                    created_at_ms, trace_id, parent_span_id \
             FROM tape_tasks WHERE task_id=?1",
            vec![task_id.into()],
        ).await?;
        Ok(row.as_ref().map(task_of))
    }
    async fn nack_task(&self, task_id: &str, owner: &str, error: &str, permanent: bool, now_ms: i64)
        -> StoreResult<Option<Task>> {
        if owner.is_empty() {
            // owner="" would match new PENDING rows (lease_owner='' default),
            // letting a caller "nack" tasks they never claimed and silently
            // bump them toward the DLQ. Same reasoning as complete_task.
            return Err(StoreError::msg("nack_task: owner is required"));
        }
        let now = if now_ms > 0 { now_ms } else { super::now_ms() };
        // Read attempts / backoff / dlq_after to decide retry vs DLQ. The
        // SELECT is gated on lease_owner so it returns None if the caller
        // never had the lease — but the SELECT itself is not the safety
        // boundary: the UPDATE below re-checks the predicate atomically so
        // a TOCTOU with `claim_tasks` after this SELECT cannot win.
        let cur = self.d().query_opt(
            "SELECT attempts, COALESCE((SELECT retry_backoff_ms FROM tape_reactions WHERE reaction_id=t.reaction_id),1000), \
                    COALESCE((SELECT dlq_after_n FROM tape_reactions WHERE reaction_id=t.reaction_id),5) \
             FROM tape_tasks t WHERE task_id=?1 AND lease_owner=?2 AND status=?3",
            vec![task_id.into(), owner.into(), (TaskStatus::Claimed as i32).into()],
        ).await?;
        let Some(row) = cur else { return Ok(None); };
        let attempts = row.i32(0);
        let backoff = row.i64(1).max(0);
        let dlq_after = row.i32(2);
        let to_dlq = permanent || attempts >= dlq_after;
        // CRITICALLY: the UPDATE re-checks `lease_owner = owner AND
        // status = CLAIMED`. If the lease has expired and another dispatcher
        // reclaimed the row between the SELECT and this UPDATE, the predicate
        // fails (lease_owner has been overwritten with the new owner) and we
        // return None — we do NOT clobber the new claim.
        let claimed = TaskStatus::Claimed as i32;
        let updated = if to_dlq {
            self.d().exec(
                "UPDATE tape_tasks SET status=?2, last_error=?3, lease_owner='', lease_expires_at_ms=0 \
                 WHERE task_id=?1 AND lease_owner=?4 AND status=?5",
                vec![task_id.into(), (TaskStatus::Dlq as i32).into(), error.into(), owner.into(), claimed.into()],
            ).await?
        } else {
            // exponential backoff: backoff * 2^(attempts-1), capped at 1h.
            let shift = (attempts.max(1) - 1).min(20) as u32;
            let delay = backoff.saturating_mul(1i64.checked_shl(shift).unwrap_or(i64::MAX));
            let delay = delay.min(3_600_000);
            self.d().exec(
                "UPDATE tape_tasks SET status=?2, next_attempt_at_ms=?3, last_error=?4, \
                                       lease_owner='', lease_expires_at_ms=0 \
                 WHERE task_id=?1 AND lease_owner=?5 AND status=?6",
                vec![task_id.into(), (TaskStatus::Pending as i32).into(), (now + delay).into(), error.into(),
                     owner.into(), claimed.into()],
            ).await?
        };
        if updated == 0 { return Ok(None); }
        let row = self.d().query_opt(
            "SELECT task_id, reaction_id, shard, source_run_id, source_global_seq, subject, payload_json, \
                    status, attempts, next_attempt_at_ms, lease_owner, lease_expires_at_ms, last_error, \
                    created_at_ms, trace_id, parent_span_id \
             FROM tape_tasks WHERE task_id=?1",
            vec![task_id.into()],
        ).await?;
        Ok(row.as_ref().map(task_of))
    }
    async fn list_tasks(&self, reaction_id: &str, status: i32, limit: i64) -> StoreResult<Vec<Task>> {
        let limit = if limit > 0 { limit } else { 200 };
        let rows = if status == 0 {
            self.d().query(
                "SELECT task_id, reaction_id, shard, source_run_id, source_global_seq, subject, payload_json, \
                        status, attempts, next_attempt_at_ms, lease_owner, lease_expires_at_ms, last_error, \
                        created_at_ms, trace_id, parent_span_id \
                 FROM tape_tasks WHERE reaction_id=?1 ORDER BY created_at_ms DESC LIMIT ?2",
                vec![reaction_id.into(), limit.into()],
            ).await?
        } else {
            self.d().query(
                "SELECT task_id, reaction_id, shard, source_run_id, source_global_seq, subject, payload_json, \
                        status, attempts, next_attempt_at_ms, lease_owner, lease_expires_at_ms, last_error, \
                        created_at_ms, trace_id, parent_span_id \
                 FROM tape_tasks WHERE reaction_id=?1 AND status=?2 ORDER BY created_at_ms DESC LIMIT ?3",
                vec![reaction_id.into(), status.into(), limit.into()],
            ).await?
        };
        Ok(rows.iter().map(task_of).collect())
    }
    async fn find_pending_task_for_subject(&self, reaction_id: &str, subject: &str)
        -> StoreResult<Option<Task>> {
        let pending = TaskStatus::Pending as i32;
        let row = self.d().query_opt(
            "SELECT task_id, reaction_id, shard, source_run_id, source_global_seq, subject, payload_json, \
                    status, attempts, next_attempt_at_ms, lease_owner, lease_expires_at_ms, last_error, \
                    created_at_ms, trace_id, parent_span_id \
             FROM tape_tasks WHERE reaction_id=?1 AND subject=?2 AND status=?3 \
             ORDER BY created_at_ms DESC LIMIT 1",
            vec![reaction_id.into(), subject.into(), pending.into()],
        ).await?;
        Ok(row.as_ref().map(task_of))
    }
    async fn coalesce_task(&self, task_id: &str, source_global_seq: i64, payload_json: &str,
                           trace_id: &str, parent_span_id: &str) -> StoreResult<Option<Task>> {
        let pending = TaskStatus::Pending as i32;
        if self.d().is_postgres() {
            // Postgres: do the conditional UPDATE and the read in one round-trip.
            let rows = self.d().query(
                "UPDATE tape_tasks SET source_global_seq=?2, payload_json=?3, trace_id=?4, parent_span_id=?5 \
                 WHERE task_id=?1 AND status=?6 \
                 RETURNING task_id, reaction_id, shard, source_run_id, source_global_seq, subject, payload_json, \
                           status, attempts, next_attempt_at_ms, lease_owner, lease_expires_at_ms, last_error, \
                           created_at_ms, trace_id, parent_span_id",
                vec![task_id.into(), source_global_seq.into(), payload_json.into(),
                     trace_id.into(), parent_span_id.into(), pending.into()],
            ).await?;
            return Ok(rows.first().map(task_of));
        }
        // SQLite: conditional UPDATE then SELECT; the WHERE clause guarantees
        // we only mutate a still-PENDING row.
        let n = self.d().exec(
            "UPDATE tape_tasks SET source_global_seq=?2, payload_json=?3, trace_id=?4, parent_span_id=?5 \
             WHERE task_id=?1 AND status=?6",
            vec![task_id.into(), source_global_seq.into(), payload_json.into(),
                 trace_id.into(), parent_span_id.into(), pending.into()],
        ).await?;
        if n == 0 { return Ok(None); }
        let row = self.d().query_opt(
            "SELECT task_id, reaction_id, shard, source_run_id, source_global_seq, subject, payload_json, \
                    status, attempts, next_attempt_at_ms, lease_owner, lease_expires_at_ms, last_error, \
                    created_at_ms, trace_id, parent_span_id \
             FROM tape_tasks WHERE task_id=?1",
            vec![task_id.into()],
        ).await?;
        Ok(row.as_ref().map(task_of))
    }
}

fn task_of(r: &Row) -> Task {
    Task {
        task_id: r.str(0), reaction_id: r.str(1), shard: r.i32(2),
        source_run_id: r.str(3), source_global_seq: r.i64(4),
        subject: r.str(5), payload_json: r.str(6),
        status: r.i32(7), attempts: r.i32(8), next_attempt_at_ms: r.i64(9),
        lease_owner: r.str(10), lease_expires_at_ms: r.i64(11), last_error: r.str(12),
        created_at_ms: r.i64(13), trace_id: r.str(14), parent_span_id: r.str(15),
    }
}

fn timer_of(r: &Row) -> TimerRecord {
    TimerRecord {
        run_id: r.str(0), timer_id: r.str(1), fire_at_ms: r.i64(2), kind: r.str(3),
        payload_json: r.str(4), fired: r.i64(5) != 0, created_at_ms: r.i64(6),
    }
}

/// Spawn a long-running task that LISTENs on `tape_journal` and pulses
/// `notify` on every NOTIFY payload. Reconnects with exponential backoff on
/// connection loss. The payload (per migration 0002 trigger) is
/// `<global_seq>:<subject>` but we only need the wake-up here.
fn spawn_pg_listener(url: String, notify: Arc<tokio::sync::Notify>) {
    tokio::spawn(async move {
        use futures_util::StreamExt;
        let mut backoff_ms: u64 = 250;
        loop {
            // tokio-postgres uses a slightly different URL flavour than the
            // sync `postgres` crate but they accept the same DSN.
            let (client, mut conn) = match tokio_postgres::connect(&url, tokio_postgres::NoTls).await {
                Ok(pair) => pair,
                Err(err) => {
                    tracing::warn!(%err, "pg listen: connect failed, retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(30_000);
                    continue;
                }
            };
            backoff_ms = 250;

            // tokio-postgres requires polling the connection on a separate task
            // and exposes notifications via a stream-like API on the connection.
            // We use the lower-level approach: poll_message via futures::stream.
            let (notif_tx, mut notif_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
            let conn_task = tokio::spawn(async move {
                // Stream all messages; forward AsyncMessage::Notification as a wake-up.
                let mut stream = futures_util::stream::poll_fn(move |cx| conn.poll_message(cx));
                while let Some(msg) = stream.next().await {
                    match msg {
                        Ok(tokio_postgres::AsyncMessage::Notification(_n)) => {
                            let _ = notif_tx.send(());
                        }
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!(%err, "pg listen: connection error");
                            break;
                        }
                    }
                }
            });

            if let Err(err) = client.batch_execute("LISTEN tape_journal").await {
                tracing::warn!(%err, "pg listen: LISTEN failed");
                let _ = conn_task.abort();
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                continue;
            }
            tracing::info!("pg listen: LISTEN tape_journal active");

            // Drain notifications, waking the in-process Notify on each.
            while notif_rx.recv().await.is_some() {
                notify.notify_waiters();
            }
            tracing::warn!("pg listen: notification channel closed, reconnecting");
            conn_task.abort();
            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
        }
    });
}
