// Schema definitions for the embedded SQL path. The four tables mirror
// `tape_adk.schemas` in the Python package exactly — same column names,
// same composite primary keys, same UNIQUE on `(connector, business_key)`,
// same indices — so a Python writer + a TS reader (or vice versa) against
// the same SQLite file are compatible.
//
// The Python schema declares foreign keys into ADK's `sessions` table. The
// standalone Node.js path doesn't have ADK, so we make the FK conditional:
// if a `sessions` table exists, we wire it; otherwise the tables are
// self-contained. This is a deliberate deviation from the Python reference
// — see README for the reasoning.
//
// Storage shapes: JSON columns are stored as TEXT in SQLite (matching what
// SQLAlchemy's `DynamicJSON` decoder uses) and JSONB in Postgres (callers
// supply the right column type via the dialect flag).

import type { Database as BetterSqlite3Database } from 'better-sqlite3';

/**
 * Minimal database surface our service needs. Compatible with
 * `better-sqlite3`'s synchronous API. We wrap calls in async functions in
 * the service layer to mirror the Python `await svc.…` shape.
 */
export interface EmbeddedDb {
  prepare(sql: string): EmbeddedStatement;
  exec(sql: string): void;
  transaction<T>(fn: () => T): () => T;
  /** Dialect: 'sqlite' triggers the in-process CAS lock; 'postgres' is a no-op. */
  dialect: 'sqlite' | 'postgres';
}

export interface EmbeddedStatement {
  run(...params: unknown[]): { changes: number; lastInsertRowid: number | bigint };
  get(...params: unknown[]): unknown;
  all(...params: unknown[]): unknown[];
}

/** Adapt a `better-sqlite3` Database instance to the EmbeddedDb shape. */
export function adaptBetterSqlite3(db: BetterSqlite3Database): EmbeddedDb {
  return {
    prepare: (sql: string) => db.prepare(sql),
    exec: (sql: string) => { db.exec(sql); },
    transaction: <T>(fn: () => T) => db.transaction(fn),
    dialect: 'sqlite',
  };
}

// ── DDL ────────────────────────────────────────────────────────────────────

/** `tape_effects` — one row per effect (run/invocation × tool call).
 *  Columns mirror `StorageEffect` in `tape_adk/schemas.py`.
 */
const DDL_EFFECTS = `
CREATE TABLE IF NOT EXISTS tape_effects (
  app_name                       TEXT NOT NULL,
  user_id                        TEXT NOT NULL,
  session_id                     TEXT NOT NULL,
  idempotency_key                TEXT NOT NULL,
  invocation_id                  TEXT NOT NULL DEFAULT '',
  decision_index                 INTEGER NOT NULL DEFAULT -1,
  tool_name                      TEXT NOT NULL DEFAULT '',
  call_index                     INTEGER NOT NULL DEFAULT 0,
  status                         TEXT NOT NULL,
  semantics                      TEXT NOT NULL DEFAULT 'idempotent',
  dispatch_mode                  TEXT NOT NULL DEFAULT 'inline',
  business_key                   TEXT,
  connector                      TEXT,
  external_ref                   TEXT,
  dispatch_attempts              INTEGER NOT NULL DEFAULT 0,
  next_dispatch_at_ms            BIGINT NOT NULL DEFAULT 0,
  dispatch_claimed_by            TEXT,
  dispatch_claim_expires_at_ms   BIGINT NOT NULL DEFAULT 0,
  last_dispatch_error            TEXT,
  request_json                   TEXT,
  response_json                  TEXT,
  error_json                     TEXT,
  ts_ms                          BIGINT NOT NULL,
  PRIMARY KEY (app_name, user_id, session_id, idempotency_key),
  CONSTRAINT uq_tape_effects_connector_business_key UNIQUE (connector, business_key)
);
`;

const IDX_EFFECTS = [
  `CREATE INDEX IF NOT EXISTS ix_tape_effects_status_ts ON tape_effects (status, ts_ms);`,
  `CREATE INDEX IF NOT EXISTS ix_tape_effects_dispatch_ready ON tape_effects (dispatch_mode, status, next_dispatch_at_ms);`,
  `CREATE INDEX IF NOT EXISTS ix_tape_effects_invocation ON tape_effects (invocation_id);`,
];

/** `tape_obligations` — one row per registered compensation. */
const DDL_OBLIGATIONS = `
CREATE TABLE IF NOT EXISTS tape_obligations (
  seq                  INTEGER PRIMARY KEY AUTOINCREMENT,
  app_name             TEXT NOT NULL,
  user_id              TEXT NOT NULL,
  session_id           TEXT NOT NULL,
  invocation_id        TEXT NOT NULL DEFAULT '',
  effect_key           TEXT NOT NULL,
  kind                 TEXT NOT NULL,
  payload_json         TEXT,
  status               TEXT NOT NULL DEFAULT 'pending',
  attempts             INTEGER NOT NULL DEFAULT 0,
  max_attempts         INTEGER NOT NULL DEFAULT 5,
  next_attempt_at_ms   BIGINT NOT NULL DEFAULT 0,
  last_error           TEXT,
  claimed_by           TEXT,
  claim_expires_at_ms  BIGINT NOT NULL DEFAULT 0,
  compensator_ref      TEXT,
  result_json          TEXT,
  ts_ms                BIGINT NOT NULL,
  CONSTRAINT uq_tape_obligations_effect_kind_per_session UNIQUE
    (app_name, user_id, session_id, effect_key, kind)
);
`;

const IDX_OBLIGATIONS = [
  `CREATE INDEX IF NOT EXISTS ix_tape_obligations_status_next ON tape_obligations (status, next_attempt_at_ms);`,
  `CREATE INDEX IF NOT EXISTS ix_tape_obligations_status ON tape_obligations (status);`,
];

/** `tape_timers` — server-side timer registry. */
const DDL_TIMERS = `
CREATE TABLE IF NOT EXISTS tape_timers (
  app_name        TEXT NOT NULL,
  user_id         TEXT NOT NULL,
  session_id      TEXT NOT NULL,
  timer_id        TEXT NOT NULL,
  fire_at_ms      BIGINT NOT NULL,
  kind            TEXT NOT NULL,
  payload_json    TEXT,
  fired           INTEGER NOT NULL DEFAULT 0,
  created_at_ms   BIGINT NOT NULL,
  PRIMARY KEY (app_name, user_id, session_id, timer_id)
);
`;

const IDX_TIMERS = [
  `CREATE INDEX IF NOT EXISTS ix_tape_timers_due ON tape_timers (fired, fire_at_ms);`,
  `CREATE INDEX IF NOT EXISTS ix_tape_timers_fire_at ON tape_timers (fire_at_ms);`,
];

/** `tape_values` — reactive KV with per-key monotonic versioning. */
const DDL_VALUES = `
CREATE TABLE IF NOT EXISTS tape_values (
  namespace   TEXT NOT NULL,
  key         TEXT NOT NULL,
  value_json  TEXT,
  version     INTEGER NOT NULL DEFAULT 0,
  ts_ms       BIGINT NOT NULL,
  writer      TEXT,
  deleted     INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (namespace, key)
);
`;

/** `tape_effect_snapshots` — one row per session: a cumulative JSON map of
 *  terminal-effect short-circuit data keyed by idempotency_key.
 *
 *  Mirrors `StorageEffectSnapshot` in `tape_adk/schemas.py` exactly — same
 *  composite primary key (app_name, user_id, session_id), same JSON-typed
 *  `effects_json` blob, same watermark / counter / timestamp columns. The FK
 *  cascade with `sessions` (when that table exists) matches the other tape_*
 *  tables; in the standalone Node.js path the FK is omitted, just like the
 *  effects/obligations/timers tables.
 *
 *  Why this exists: the compactor prunes terminal effect rows past their
 *  TTL so the journal doesn't grow without bound. But the idempotency-key
 *  short-circuit in `beginEffect` reads `tape_effects` — once pruned,
 *  `beginEffect` would create a fresh PENDING row for the same key and
 *  re-dispatch the work. Double-spend. The snapshot solves that by holding
 *  the *minimum data the short-circuit needs* (status + response + the
 *  cross-run dedup fields) in one row. `beginEffect` reads the live row
 *  first and falls back to the snapshot when the live row is gone, so the
 *  compactor is free to delete the underlying rows that were captured.
 */
const DDL_EFFECT_SNAPSHOTS = `
CREATE TABLE IF NOT EXISTS tape_effect_snapshots (
  app_name        TEXT NOT NULL,
  user_id         TEXT NOT NULL,
  session_id      TEXT NOT NULL,
  effects_json    TEXT,
  up_to_ts_ms     BIGINT NOT NULL DEFAULT 0,
  effects_count   INTEGER NOT NULL DEFAULT 0,
  created_at_ms   BIGINT NOT NULL,
  updated_at_ms   BIGINT NOT NULL,
  PRIMARY KEY (app_name, user_id, session_id)
);
`;

/** Create all five tables + indices. Idempotent — safe to call repeatedly. */
export function createAllTables(db: EmbeddedDb): void {
  db.exec(DDL_EFFECTS);
  for (const ix of IDX_EFFECTS) db.exec(ix);
  db.exec(DDL_OBLIGATIONS);
  for (const ix of IDX_OBLIGATIONS) db.exec(ix);
  db.exec(DDL_TIMERS);
  for (const ix of IDX_TIMERS) db.exec(ix);
  db.exec(DDL_VALUES);
  db.exec(DDL_EFFECT_SNAPSHOTS);
}

// ── row types (what we hand back to callers) ──────────────────────────────

export interface EffectRow {
  app_name: string;
  user_id: string;
  session_id: string;
  idempotency_key: string;
  invocation_id: string;
  decision_index: number;
  tool_name: string;
  call_index: number;
  status: string;
  semantics: string;
  dispatch_mode: string;
  business_key: string | null;
  connector: string | null;
  external_ref: string | null;
  dispatch_attempts: number;
  next_dispatch_at_ms: number;
  dispatch_claimed_by: string | null;
  dispatch_claim_expires_at_ms: number;
  last_dispatch_error: string | null;
  request_json: string | null;
  response_json: string | null;
  error_json: string | null;
  ts_ms: number;
}

export interface ObligationRow {
  seq: number;
  app_name: string;
  user_id: string;
  session_id: string;
  invocation_id: string;
  effect_key: string;
  kind: string;
  payload_json: string | null;
  status: string;
  attempts: number;
  max_attempts: number;
  next_attempt_at_ms: number;
  last_error: string | null;
  claimed_by: string | null;
  claim_expires_at_ms: number;
  compensator_ref: string | null;
  result_json: string | null;
  ts_ms: number;
}

export interface TimerRow {
  app_name: string;
  user_id: string;
  session_id: string;
  timer_id: string;
  fire_at_ms: number;
  kind: string;
  payload_json: string | null;
  fired: number;
  created_at_ms: number;
}

export interface ValueRow {
  namespace: string;
  key: string;
  value_json: string | null;
  version: number;
  ts_ms: number;
  writer: string | null;
  deleted: number;
}

export interface EffectSnapshotRow {
  app_name: string;
  user_id: string;
  session_id: string;
  /** JSON-encoded `{idempotency_key: CapturedEffect}` map. SQLite stores
   *  JSON as TEXT — matching SQLAlchemy's `DynamicJSON` serialisation on
   *  the Python side, so a SQLite file written by one SDK can be read by
   *  the other. */
  effects_json: string | null;
  up_to_ts_ms: number;
  effects_count: number;
  created_at_ms: number;
  updated_at_ms: number;
}
