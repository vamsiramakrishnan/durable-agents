-- Tape schema. SQLite dialect (the dev/test store); the Postgres store mirrors
-- it column-for-column behind the same Store trait. All tables are append-only
-- except the explicitly mutable cursors: tape_runs, tape_budget,
-- tape_effects.status, tape_obligations.status, tape_signals.consumed.

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS tape_runs (
  run_id              TEXT PRIMARY KEY,
  app_name            TEXT NOT NULL,
  user_id             TEXT NOT NULL,
  session_id          TEXT NOT NULL,
  invocation_id       TEXT NOT NULL,
  status              INTEGER NOT NULL,         -- RunStatus enum
  seq_cursor          INTEGER NOT NULL DEFAULT 0,
  lease_owner         TEXT NOT NULL DEFAULT '',
  lease_expires_at_ms INTEGER NOT NULL DEFAULT 0,
  waiting_on_gate     TEXT NOT NULL DEFAULT '',
  detail_json         TEXT NOT NULL DEFAULT '',
  started_at_ms       INTEGER NOT NULL,
  ended_at_ms         INTEGER NOT NULL DEFAULT 0,
  UNIQUE(app_name, user_id, session_id, invocation_id)
);
CREATE INDEX IF NOT EXISTS idx_runs_recover ON tape_runs(status, lease_expires_at_ms);
CREATE INDEX IF NOT EXISTS idx_runs_route ON tape_runs(app_name, user_id, session_id);

-- The journal: one row per durable step, in (run_id, seq) order. The decision,
-- effect, and obligation projections below carry the typed detail; this table
-- is the ordered spine and the SubscribeRun feed.
CREATE TABLE IF NOT EXISTS tape_journal (
  run_id       TEXT NOT NULL,
  seq          INTEGER NOT NULL,
  kind         TEXT NOT NULL,                   -- decision | effect | obligation | gate | state | event
  payload_json TEXT NOT NULL,
  ts_ms        INTEGER NOT NULL,
  PRIMARY KEY (run_id, seq)
);

CREATE TABLE IF NOT EXISTS tape_decisions (
  run_id         TEXT NOT NULL,
  seq            INTEGER NOT NULL,
  decision_index INTEGER NOT NULL,
  model          TEXT NOT NULL,
  request_json   TEXT NOT NULL,
  response_json  TEXT NOT NULL,
  rationale      TEXT NOT NULL DEFAULT '',
  policy_version TEXT NOT NULL DEFAULT '',
  ts_ms          INTEGER NOT NULL,
  PRIMARY KEY (run_id, decision_index)
);

CREATE TABLE IF NOT EXISTS tape_effects (
  run_id          TEXT NOT NULL,
  seq             INTEGER NOT NULL,
  decision_index  INTEGER NOT NULL,
  tool_name       TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  status          INTEGER NOT NULL,             -- EffectStatus enum
  request_json    TEXT NOT NULL DEFAULT '',
  response_json   TEXT NOT NULL DEFAULT '',
  error_json      TEXT NOT NULL DEFAULT '',
  ts_ms           INTEGER NOT NULL,
  PRIMARY KEY (run_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_effects_status ON tape_effects(status);

CREATE TABLE IF NOT EXISTS tape_obligations (
  run_id       TEXT NOT NULL,
  seq          INTEGER NOT NULL,
  effect_key   TEXT NOT NULL,
  kind         TEXT NOT NULL,
  payload_json TEXT NOT NULL DEFAULT '',
  status       INTEGER NOT NULL,                -- ObligationStatus enum
  ts_ms        INTEGER NOT NULL,
  PRIMARY KEY (run_id, seq)
);

CREATE TABLE IF NOT EXISTS tape_budget (
  run_id       TEXT PRIMARY KEY,
  usd_cap      REAL NOT NULL DEFAULT 0,
  token_cap    INTEGER NOT NULL DEFAULT 0,
  usd_spent    REAL NOT NULL DEFAULT 0,
  tokens_spent INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS tape_signals (
  run_id          TEXT NOT NULL,
  gate_name       TEXT NOT NULL,
  context_json    TEXT NOT NULL DEFAULT '',
  resolution_json TEXT NOT NULL DEFAULT '',
  delivered       INTEGER NOT NULL DEFAULT 0,   -- 1 once SendSignal has landed
  awaited         INTEGER NOT NULL DEFAULT 0,   -- 1 once the run has parked on it
  consumed        INTEGER NOT NULL DEFAULT 0,   -- 1 once the resumed run has read it
  created_at_ms   INTEGER NOT NULL,
  PRIMARY KEY (run_id, gate_name)
);

-- The ADK session/event mirror.
CREATE TABLE IF NOT EXISTS tape_sessions (
  app_name           TEXT NOT NULL,
  user_id            TEXT NOT NULL,
  session_id         TEXT NOT NULL,
  state_json         TEXT NOT NULL DEFAULT '{}',
  last_update_time_ms INTEGER NOT NULL,
  PRIMARY KEY (app_name, user_id, session_id)
);

CREATE TABLE IF NOT EXISTS tape_events (
  app_name      TEXT NOT NULL,
  user_id       TEXT NOT NULL,
  session_id    TEXT NOT NULL,
  ord           INTEGER NOT NULL,               -- append order within the session
  event_id      TEXT NOT NULL,
  invocation_id TEXT NOT NULL DEFAULT '',
  author        TEXT NOT NULL DEFAULT '',
  branch        TEXT NOT NULL DEFAULT '',
  content_json  TEXT NOT NULL DEFAULT '',
  actions_json  TEXT NOT NULL DEFAULT '',
  timestamp_ms  INTEGER NOT NULL,
  PRIMARY KEY (app_name, user_id, session_id, ord)
);

-- App- and user-scoped state (the "user:" / "app:" prefixes ADK supports).
CREATE TABLE IF NOT EXISTS tape_scoped_state (
  app_name  TEXT NOT NULL,
  scope     TEXT NOT NULL,                      -- 'app' | 'user'
  scope_key TEXT NOT NULL DEFAULT '',           -- '' for app scope, user_id for user scope
  key       TEXT NOT NULL,
  value_json TEXT NOT NULL,
  PRIMARY KEY (app_name, scope, scope_key, key)
);
