//! The PostgreSQL store — the production / horizontally-scalable backend.
//!
//! A pooled connection (`r2d2` over the sync `postgres` client; DB work runs on
//! a blocking thread). The Tape SQL is portable; this store only rewrites the
//! `?N` placeholders to `$N` and uses the BIGINT/DOUBLE PRECISION schema. Point
//! N replicas of the server at one Postgres and you have a scale-out Tape — the
//! per-run lease in `tape_runs` plus the idempotency of every RPC make that safe.

use std::str::FromStr;

use async_trait::async_trait;
use postgres::types::{ToSql, Type};
use postgres::{Config, NoTls};
use r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;

use super::{to_pg_placeholders, Row, Store, StoreError, StoreResult, Val};

const SCHEMA: &str = include_str!("../../migrations/0001_init.postgres.sql");

type Mgr = PostgresConnectionManager<NoTls>;

pub struct PostgresStore {
    pool: Pool<Mgr>,
}

fn e<E: std::fmt::Display>(err: E) -> StoreError {
    StoreError::Msg(err.to_string())
}

impl PostgresStore {
    pub fn connect(url: &str) -> StoreResult<Self> {
        let config = Config::from_str(url).map_err(e)?;
        let mgr = PostgresConnectionManager::new(config, NoTls);
        let pool = Pool::builder().max_size(16).build(mgr).map_err(e)?;
        Ok(Self { pool })
    }

    async fn with_client<T, F>(&self, f: F) -> StoreResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut postgres::Client) -> StoreResult<T> + Send + 'static,
    {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let mut client = pool.get().map_err(e)?;
            f(&mut client)
        })
        .await
        .map_err(e)?
    }
}

/// Box each `Val` as a concrete `ToSql + Sync`.
fn boxed_params(params: &[Val]) -> Vec<Box<dyn ToSql + Sync + Send>> {
    params
        .iter()
        .map(|v| -> Box<dyn ToSql + Sync + Send> {
            match v {
                Val::Int(i) => Box::new(*i),
                Val::Real(f) => Box::new(*f),
                Val::Text(s) => Box::new(s.clone()),
                Val::Null => Box::new(Option::<String>::None),
            }
        })
        .collect()
}

fn refs<'a>(boxes: &'a [Box<dyn ToSql + Sync + Send>]) -> Vec<&'a (dyn ToSql + Sync)> {
    boxes.iter().map(|b| b.as_ref() as &(dyn ToSql + Sync)).collect()
}

fn col_val(row: &postgres::Row, i: usize) -> Val {
    let ty = row.columns()[i].type_().clone();
    if ty == Type::INT8 {
        row.get::<_, Option<i64>>(i).map(Val::Int).unwrap_or(Val::Null)
    } else if ty == Type::INT4 {
        row.get::<_, Option<i32>>(i).map(|v| Val::Int(v as i64)).unwrap_or(Val::Null)
    } else if ty == Type::INT2 {
        row.get::<_, Option<i16>>(i).map(|v| Val::Int(v as i64)).unwrap_or(Val::Null)
    } else if ty == Type::FLOAT8 {
        row.get::<_, Option<f64>>(i).map(Val::Real).unwrap_or(Val::Null)
    } else if ty == Type::FLOAT4 {
        row.get::<_, Option<f32>>(i).map(|v| Val::Real(v as f64)).unwrap_or(Val::Null)
    } else if ty == Type::BOOL {
        row.get::<_, Option<bool>>(i).map(|v| Val::Int(v as i64)).unwrap_or(Val::Null)
    } else {
        row.get::<_, Option<String>>(i).map(Val::Text).unwrap_or(Val::Null)
    }
}

#[async_trait]
impl Store for PostgresStore {
    async fn migrate(&self) -> StoreResult<()> {
        self.with_client(|c| c.batch_execute(SCHEMA).map_err(e)).await
    }

    async fn exec(&self, sql: &str, params: Vec<Val>) -> StoreResult<u64> {
        let sql = to_pg_placeholders(sql);
        self.with_client(move |c| {
            let boxes = boxed_params(&params);
            c.execute(sql.as_str(), refs(&boxes).as_slice()).map_err(e)
        })
        .await
    }

    async fn query(&self, sql: &str, params: Vec<Val>) -> StoreResult<Vec<Row>> {
        let sql = to_pg_placeholders(sql);
        self.with_client(move |c| {
            let boxes = boxed_params(&params);
            let rows = c.query(sql.as_str(), refs(&boxes).as_slice()).map_err(e)?;
            Ok(rows
                .iter()
                .map(|row| (0..row.columns().len()).map(|i| col_val(row, i)).collect::<Row>())
                .collect())
        })
        .await
    }

    async fn tx(&self, stmts: Vec<(String, Vec<Val>)>) -> StoreResult<()> {
        self.with_client(move |c| {
            let mut t = c.transaction().map_err(e)?;
            for (sql, params) in &stmts {
                let sql = to_pg_placeholders(sql);
                let boxes = boxed_params(params);
                t.execute(sql.as_str(), refs(&boxes).as_slice()).map_err(e)?;
            }
            t.commit().map_err(e)
        })
        .await
    }
}
