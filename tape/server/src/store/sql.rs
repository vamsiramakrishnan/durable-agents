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

use super::{derive_key, merge_json, now_ms, RunStore, StoreError, StoreResult};
use crate::pb::*;

const SCHEMA_SQLITE: &str = include_str!("../../migrations/0001_init.sqlite.sql");
const SCHEMA_PG: &str = include_str!("../../migrations/0001_init.postgres.sql");

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
    async fn migrate(&self) -> StoreResult<()> { self.with(|c| c.execute_batch(SCHEMA_SQLITE).map_err(e)).await }
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
    async fn migrate(&self) -> StoreResult<()> { self.with(|c| c.batch_execute(SCHEMA_PG).map_err(e)).await }
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
}

impl SqlRunStore {
    pub async fn sqlite_file(path: &str) -> StoreResult<Self> {
        let db: Arc<dyn SqlBackend> = Arc::new(SqliteBackend::file(path)?);
        db.migrate().await?;
        Ok(Self { db })
    }
    pub async fn sqlite_memory() -> StoreResult<Self> {
        let db: Arc<dyn SqlBackend> = Arc::new(SqliteBackend::memory()?);
        db.migrate().await?;
        Ok(Self { db })
    }
    pub async fn postgres(url: &str) -> StoreResult<Self> {
        let db: Arc<dyn SqlBackend> = Arc::new(PostgresBackend::connect(url)?);
        db.migrate().await?;
        Ok(Self { db })
    }

    fn d(&self) -> &dyn SqlBackend { self.db.as_ref() }

    async fn next_seq(&self, run_id: &str) -> StoreResult<i64> {
        self.d().exec("UPDATE tape_runs SET seq_cursor = seq_cursor + 1 WHERE run_id = ?1",
                      vec![run_id.into()]).await?;
        Ok(self.d().query_opt("SELECT seq_cursor FROM tape_runs WHERE run_id = ?1", vec![run_id.into()])
            .await?.map(|r| r.i64(0)).unwrap_or(0))
    }
    async fn journal(&self, run_id: &str, seq: i64, kind: &str, payload: &str, ts: i64) -> StoreResult<()> {
        self.d().exec("INSERT INTO tape_journal (run_id, seq, kind, payload_json, ts_ms) VALUES (?1,?2,?3,?4,?5)",
                      vec![run_id.into(), seq.into(), kind.into(), payload.into(), ts.into()]).await.map(|_| ())
    }
}

const RUN_COLS: &str = "run_id, app_name, user_id, session_id, invocation_id, status, seq_cursor, \
    lease_owner, lease_expires_at_ms, started_at_ms, ended_at_ms, waiting_on_gate";
fn run_of(r: &Row) -> RunState {
    RunState {
        run_id: r.str(0), app_name: r.str(1), user_id: r.str(2), session_id: r.str(3),
        invocation_id: r.str(4), status: r.i32(5), seq_cursor: r.i64(6), lease_owner: r.str(7),
        lease_expires_at_ms: r.i64(8), started_at_ms: r.i64(9), ended_at_ms: r.i64(10), waiting_on_gate: r.str(11),
    }
}
const EFFECT_COLS: &str = "run_id, seq, decision_index, tool_name, idempotency_key, status, \
    request_json, response_json, error_json, ts_ms";
fn effect_of(r: &Row) -> EffectRecord {
    EffectRecord {
        run_id: r.str(0), seq: r.i64(1), decision_index: r.i64(2), tool_name: r.str(3),
        idempotency_key: r.str(4), status: r.i32(5), request_json: r.str(6),
        response_json: r.str(7), error_json: r.str(8), ts_ms: r.i64(9),
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
    // ── run lifecycle ───────────────────────────────────────────────────────
    async fn begin_run(&self, app: &str, user: &str, session: &str, invocation: &str,
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
        self.d().exec(
            "INSERT INTO tape_runs (run_id, app_name, user_id, session_id, invocation_id, status, seq_cursor, lease_owner, lease_expires_at_ms, started_at_ms) \
             VALUES (?1,?2,?3,?4,?5,?6,0,?7,?8,?9)",
            vec![run_id.clone().into(), app.into(), user.into(), session.into(), invocation.into(),
                 (RunStatus::Running as i32).into(), lease_owner.into(), lease_exp.into(), ts.into()]).await?;
        Ok(BeginRunResponse { run_id, resumed: false, next_seq: 0, status: RunStatus::Running as i32 })
    }
    async fn resume_run(&self, run_id: &str, lease_owner: &str, lease_ttl_ms: i64) -> StoreResult<Option<RunState>> {
        self.d().exec("UPDATE tape_runs SET status=?2, lease_owner=?3, lease_expires_at_ms=?4 WHERE run_id=?1",
            vec![run_id.into(), (RunStatus::Running as i32).into(), lease_owner.into(), (now_ms() + lease_ttl_ms.max(0)).into()]).await?;
        self.get_run(run_id).await
    }
    async fn end_run(&self, run_id: &str, status: i32, detail_json: &str) -> StoreResult<Option<RunState>> {
        self.d().exec("UPDATE tape_runs SET status=?2, ended_at_ms=?3, detail_json=?4, lease_owner='' WHERE run_id=?1",
            vec![run_id.into(), status.into(), now_ms().into(), detail_json.into()]).await?;
        self.get_run(run_id).await
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
        let rows = self.d().query("SELECT seq, kind, payload_json, ts_ms FROM tape_journal WHERE run_id=?1 AND seq>=?2 ORDER BY seq",
            vec![run_id.into(), from_seq.into()]).await?;
        Ok(rows.iter().map(|r| JournalEntry { seq: r.i64(0), kind: r.str(1), payload_json: r.str(2), ts_ms: r.i64(3) }).collect())
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
                          request_json: &str, custom_key: &str) -> StoreResult<EffectRecord> {
        let key = if custom_key.is_empty() { derive_key(run_id, decision_index, tool_name, call_index) } else { custom_key.to_string() };
        if let Some(rec) = self.get_effect(run_id, &key).await? {
            return Ok(rec);
        }
        let ts = now_ms();
        let seq = self.next_seq(run_id).await?;
        self.d().exec(
            "INSERT INTO tape_effects (run_id, seq, decision_index, tool_name, idempotency_key, status, request_json, response_json, error_json, ts_ms) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,'','',?8)",
            vec![run_id.into(), seq.into(), decision_index.into(), tool_name.into(), key.clone().into(),
                 (EffectStatus::Pending as i32).into(), request_json.into(), ts.into()]).await?;
        let payload = serde_json::json!({"tool": tool_name, "decision_index": decision_index, "idempotency_key": key, "status": "pending"}).to_string();
        self.journal(run_id, seq, "effect", &payload, ts).await?;
        Ok(EffectRecord { run_id: run_id.into(), seq, decision_index, tool_name: tool_name.into(),
            idempotency_key: key, status: EffectStatus::Pending as i32, request_json: request_json.into(),
            response_json: String::new(), error_json: String::new(), ts_ms: ts })
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
        self.get_value(namespace, key).await?.ok_or_else(|| StoreError::msg("write_value: row vanished after upsert"))
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
        Ok((true, new_v))
    }

    // ── the WAL tail ────────────────────────────────────────────────────────
    async fn events_since(&self, from_ts_ms: i64, run_id: &str, kind: &str, limit: i64) -> StoreResult<Vec<EventEntry>> {
        let rows = self.d().query(
            "SELECT run_id, seq, kind, payload_json, ts_ms FROM tape_journal WHERE ts_ms >= ?1 AND (?2 = '' OR run_id = ?2) AND (?3 = '' OR kind = ?3) ORDER BY ts_ms, run_id, seq LIMIT ?4",
            vec![from_ts_ms.into(), run_id.into(), kind.into(), limit.max(1).into()]).await?;
        Ok(rows.iter().map(|r| EventEntry { run_id: r.str(0), seq: r.i64(1), kind: r.str(2), payload_json: r.str(3), ts_ms: r.i64(4) }).collect())
    }
}

fn timer_of(r: &Row) -> TimerRecord {
    TimerRecord {
        run_id: r.str(0), timer_id: r.str(1), fire_at_ms: r.i64(2), kind: r.str(3),
        payload_json: r.str(4), fired: r.i64(5) != 0, created_at_ms: r.i64(6),
    }
}
