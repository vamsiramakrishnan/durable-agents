//! The storage layer — pluggable, chosen by URL at deploy time.
//!
//! `TAPE_STORE` (or `--store`) is a URL:
//!   * `sqlite:./tape.db`   — a file-backed SQLite store (the default)
//!   * `sqlite::memory:` / `memory` — an ephemeral in-process store (tests, demos)
//!   * `postgres://user:pass@host:5432/db` — a pooled PostgreSQL store (production / HA)
//!   * `bigtable://project/instance/table` — reserved for v2 (see design-principles/tape.md §13)
//!
//! The wiring is automatic: the server parses the URL on startup, builds the
//! matching `Store`, runs migrations, and serves. Nothing else changes — the
//! gRPC contract, the SDKs, the agents are all unaffected. Run N replicas of the
//! server behind a load balancer against the same Postgres and you have a
//! horizontally scalable Tape: the server holds no state between requests; "one
//! driver per run at a time" is enforced by the per-run lease in `tape_runs`;
//! every mutating RPC is idempotent, so a double-drive (two recovery workers
//! racing) is harmless — the loser short-circuits.
//!
//! Implementations share a tiny surface — `exec` / `query` / `query_opt` / `tx`
//! over portable SQL with `?N` placeholders — so the SQL lives once, in
//! `service.rs`. SQLite reads `?N` natively; the Postgres store rewrites it to
//! `$N`. Both use an `r2d2` connection pool and run blocking DB work on a
//! blocking thread.

pub mod postgres;
pub mod sqlite;

use std::sync::Arc;

use async_trait::async_trait;

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("tape store: {0}")]
    Msg(String),
}

impl StoreError {
    pub fn msg(s: impl Into<String>) -> Self {
        StoreError::Msg(s.into())
    }
}

/// A SQL value, in or out. Every column in the Tape schema is one of these.
#[derive(Clone, Debug, PartialEq)]
pub enum Val {
    Int(i64),
    Real(f64),
    Text(String),
    Null,
}

impl From<i64> for Val { fn from(v: i64) -> Self { Val::Int(v) } }
impl From<i32> for Val { fn from(v: i32) -> Self { Val::Int(v as i64) } }
impl From<u64> for Val { fn from(v: u64) -> Self { Val::Int(v as i64) } }
impl From<f64> for Val { fn from(v: f64) -> Self { Val::Real(v) } }
impl From<&str> for Val { fn from(v: &str) -> Self { Val::Text(v.to_string()) } }
impl From<String> for Val { fn from(v: String) -> Self { Val::Text(v) } }
impl From<&String> for Val { fn from(v: &String) -> Self { Val::Text(v.clone()) } }

/// A result row: positional columns. Use the typed getters.
pub type Row = Vec<Val>;

pub trait RowExt {
    fn i64(&self, i: usize) -> i64;
    fn f64(&self, i: usize) -> f64;
    fn str(&self, i: usize) -> String;
    fn i32(&self, i: usize) -> i32 {
        self.i64(i) as i32
    }
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

#[async_trait]
pub trait Store: Send + Sync {
    /// Create the schema if absent.
    async fn migrate(&self) -> StoreResult<()>;

    /// Run a statement; return rows affected. `?N` placeholders.
    async fn exec(&self, sql: &str, params: Vec<Val>) -> StoreResult<u64>;

    /// Run a query; return all rows.
    async fn query(&self, sql: &str, params: Vec<Val>) -> StoreResult<Vec<Row>>;

    /// Run a query expected to return zero or one row.
    async fn query_opt(&self, sql: &str, params: Vec<Val>) -> StoreResult<Option<Row>> {
        Ok(self.query(sql, params).await?.into_iter().next())
    }

    /// Run several statements in one transaction (all-or-nothing).
    async fn tx(&self, stmts: Vec<(String, Vec<Val>)>) -> StoreResult<()>;
}

/// Parse a store URL and build the matching `Store`, migrated and ready.
pub async fn open(url: &str) -> StoreResult<Arc<dyn Store>> {
    let s: Arc<dyn Store> = if url == "memory" || url == ":memory:" || url == "sqlite::memory:" {
        Arc::new(sqlite::SqliteStore::open_memory()?)
    } else if let Some(path) = url.strip_prefix("sqlite:") {
        let path = path.strip_prefix("//").unwrap_or(path);
        if path == ":memory:" {
            Arc::new(sqlite::SqliteStore::open_memory()?)
        } else {
            Arc::new(sqlite::SqliteStore::open_file(path)?)
        }
    } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        Arc::new(postgres::PostgresStore::connect(url)?)
    } else if url.starts_with("bigtable://") || url.starts_with("spanner://") {
        return Err(StoreError::msg(format!(
            "the '{}' store is reserved for v2 — implement the Store trait in src/store/ \
             (see design-principles/tape.md §13: Pluggable stores and horizontal scaling)",
            url.split(':').next().unwrap_or("?")
        )));
    } else {
        // Bare path -> a SQLite file.
        Arc::new(sqlite::SqliteStore::open_file(url)?)
    };
    s.migrate().await?;
    Ok(s)
}

pub fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Rewrite SQLite-style `?N` placeholders to Postgres `$N`. (No `?N` is reused
/// in Tape's SQL, so this is a straight token swap.)
pub(crate) fn to_pg_placeholders(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'?' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            out.push('$');
            i += 1;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}
