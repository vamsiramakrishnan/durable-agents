//! The storage layer.
//!
//! v1 ships one concrete store: SQLite (the dev/test backend named in the spec).
//! It is a thin wrapper around a single `rusqlite::Connection` behind a `Mutex`
//! — fine for a reference implementation, where the point is the *shape* of the
//! journal, not its throughput. A Postgres store mirroring this schema
//! column-for-column lives behind the same surface and is the production target;
//! the SQL in `service.rs` is written in the portable subset both speak.
//!
//! Two properties matter and both come for free here:
//!  * Every single `execute` is its own committed transaction (autocommit), so
//!    `BeginEffect`'s "write the intent, commit, *then* let the body run" — the
//!    outbox move — is just an `INSERT`.
//!  * `AppendEvent`'s "the ADK event and the tape projection commit together"
//!    is an explicit `transaction()` spanning both writes.

use std::sync::Mutex;

use anyhow::Result;
use rusqlite::Connection;

const SCHEMA: &str = include_str!("../migrations/0001_init.sql");

pub struct Store {
    conn: Mutex<Connection>,
}

impl Store {
    pub fn open(path: &str) -> Result<Self> {
        let conn = if path == ":memory:" {
            Connection::open_in_memory()?
        } else {
            Connection::open(path)?
        };
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    /// Borrow the connection. The caller must not `.await` while holding it.
    pub fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().expect("tape store mutex poisoned")
    }
}

pub fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
