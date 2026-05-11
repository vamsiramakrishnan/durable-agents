//! The SQLite store — the default, and the dev/test backend. A pooled
//! `rusqlite` connection (WAL mode so multiple connections to one file work).

use async_trait::async_trait;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::types::{ToSqlOutput, ValueRef};
use rusqlite::ToSql;

use super::{Row, Store, StoreError, StoreResult, Val};

const SCHEMA: &str = include_str!("../../migrations/0001_init.sqlite.sql");

pub struct SqliteStore {
    pool: Pool<SqliteConnectionManager>,
}

impl SqliteStore {
    pub fn open_file(path: &str) -> StoreResult<Self> {
        let mgr = SqliteConnectionManager::file(path).with_init(|c| {
            c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;")
        });
        let pool = Pool::builder().max_size(16).build(mgr).map_err(e)?;
        Ok(Self { pool })
    }

    pub fn open_memory() -> StoreResult<Self> {
        // One connection only: an in-memory DB is per-connection, so pinning the
        // pool to size 1 keeps a single shared database alive.
        let mgr = SqliteConnectionManager::memory()
            .with_init(|c| c.execute_batch("PRAGMA foreign_keys=ON;"));
        let pool = Pool::builder().max_size(1).build(mgr).map_err(e)?;
        Ok(Self { pool })
    }

    async fn with_conn<T, F>(&self, f: F) -> StoreResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> StoreResult<T> + Send + 'static,
    {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().map_err(e)?;
            f(&mut conn)
        })
        .await
        .map_err(e)?
    }
}

fn e<E: std::fmt::Display>(err: E) -> StoreError {
    StoreError::Msg(err.to_string())
}

impl ToSql for Val {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match self {
            Val::Int(v) => ToSqlOutput::from(*v),
            Val::Real(v) => ToSqlOutput::from(*v),
            Val::Text(v) => ToSqlOutput::from(v.as_str()),
            Val::Null => ToSqlOutput::from(rusqlite::types::Null),
        })
    }
}

fn val_of(r: ValueRef<'_>) -> Val {
    match r {
        ValueRef::Null => Val::Null,
        ValueRef::Integer(i) => Val::Int(i),
        ValueRef::Real(f) => Val::Real(f),
        ValueRef::Text(t) => Val::Text(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) => Val::Text(String::from_utf8_lossy(b).into_owned()),
    }
}

fn run_query(conn: &rusqlite::Connection, sql: &str, params: &[Val]) -> StoreResult<Vec<Row>> {
    let mut stmt = conn.prepare(sql).map_err(e)?;
    let n = stmt.column_count();
    let bound: Vec<&dyn ToSql> = params.iter().map(|v| v as &dyn ToSql).collect();
    let rows = stmt
        .query_map(bound.as_slice(), |row| {
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                out.push(val_of(row.get_ref(i)?));
            }
            Ok(out)
        })
        .map_err(e)?;
    rows.collect::<rusqlite::Result<Vec<Row>>>().map_err(e)
}

#[async_trait]
impl Store for SqliteStore {
    async fn migrate(&self) -> StoreResult<()> {
        self.with_conn(|c| c.execute_batch(SCHEMA).map_err(e)).await
    }

    async fn exec(&self, sql: &str, params: Vec<Val>) -> StoreResult<u64> {
        let sql = sql.to_string();
        self.with_conn(move |c| {
            let bound: Vec<&dyn ToSql> = params.iter().map(|v| v as &dyn ToSql).collect();
            c.execute(&sql, bound.as_slice()).map(|n| n as u64).map_err(e)
        })
        .await
    }

    async fn query(&self, sql: &str, params: Vec<Val>) -> StoreResult<Vec<Row>> {
        let sql = sql.to_string();
        self.with_conn(move |c| run_query(c, &sql, &params)).await
    }

    async fn tx(&self, stmts: Vec<(String, Vec<Val>)>) -> StoreResult<()> {
        self.with_conn(move |c| {
            let t = c.transaction().map_err(e)?;
            for (sql, params) in &stmts {
                let bound: Vec<&dyn ToSql> = params.iter().map(|v| v as &dyn ToSql).collect();
                t.execute(sql, bound.as_slice()).map_err(e)?;
            }
            t.commit().map_err(e)
        })
        .await
    }
}
