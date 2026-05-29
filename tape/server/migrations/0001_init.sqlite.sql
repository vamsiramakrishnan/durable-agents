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
  -- Identity & authorization context (BeginRunRequest §"Identity"). The first
  -- five are indexable; scopes and labels are JSON blobs (arrays / objects) so
  -- the column count stays sane across SQLite / Postgres / Bigtable.
  tenant_id           TEXT NOT NULL DEFAULT '',
  actor               TEXT NOT NULL DEFAULT '',
  subject             TEXT NOT NULL DEFAULT '',
  agent_id            TEXT NOT NULL DEFAULT '',
  aiplex_instance_id  TEXT NOT NULL DEFAULT '',
  gateway_route       TEXT NOT NULL DEFAULT '',
  scopes_json         TEXT NOT NULL DEFAULT '[]',  -- JSON-encoded string array
  labels_json         TEXT NOT NULL DEFAULT '{}',  -- JSON-encoded map<string,string>
  -- Compaction (PR 13). NULL until the compactor reactor zeroes the
  -- decision/effect payloads on this run. The envelope (seq/kind/
  -- tool_name/idempotency_key/business_key/scope) and the projected
  -- RunState are preserved — only the LLM-context-bearing JSON blobs
  -- are dropped. Compacted runs are still queryable + auditable;
  -- they just can't be replayed step-by-step.
  compacted_at_ms     INTEGER NOT NULL DEFAULT 0,
  UNIQUE(app_name, user_id, session_id, invocation_id)
);
CREATE INDEX IF NOT EXISTS idx_runs_recover ON tape_runs(status, lease_expires_at_ms);
CREATE INDEX IF NOT EXISTS idx_runs_route ON tape_runs(app_name, user_id, session_id);
-- AIPlex run timeline: list runs by (tenant, agent) ordered by start time.
CREATE INDEX IF NOT EXISTS idx_runs_tenant ON tape_runs(tenant_id, agent_id, started_at_ms);
-- Operator queries: who acted, or which subject did this work for.
CREATE INDEX IF NOT EXISTS idx_runs_actor   ON tape_runs(actor, started_at_ms);
CREATE INDEX IF NOT EXISTS idx_runs_subject ON tape_runs(subject, started_at_ms);
-- Compactor's hot query: terminal+settled runs whose ended_at_ms is
-- older than the (hot_window) cutoff and compacted_at_ms is still 0.
CREATE INDEX IF NOT EXISTS idx_runs_compactable
  ON tape_runs(status, compacted_at_ms, ended_at_ms);

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
  -- Outbox / non-idempotent contract (see proto: EffectSemantics, EffectDispatchMode).
  semantics                    INTEGER NOT NULL DEFAULT 1,    -- IDEMPOTENT
  dispatch_mode                INTEGER NOT NULL DEFAULT 1,    -- INLINE
  business_key                 TEXT    NOT NULL DEFAULT '',
  connector                    TEXT    NOT NULL DEFAULT '',
  dispatch_attempts            INTEGER NOT NULL DEFAULT 0,
  next_dispatch_at_ms          INTEGER NOT NULL DEFAULT 0,
  external_ref                 TEXT    NOT NULL DEFAULT '',
  dispatch_claimed_by          TEXT    NOT NULL DEFAULT '',
  dispatch_claim_expires_at_ms INTEGER NOT NULL DEFAULT 0,
  last_dispatch_error          TEXT    NOT NULL DEFAULT '',
  -- Authorization (see proto: BeginEffectRequest.scope). The server verifies
  -- the scope is in the run's scopes_json before persisting; populated only
  -- once verified, so a row's presence implies admission was granted.
  scope                        TEXT    NOT NULL DEFAULT '',
  PRIMARY KEY (run_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_effects_status ON tape_effects(status);
-- The outbox dispatcher's hot query: PENDING + OUTBOX + due.
CREATE INDEX IF NOT EXISTS idx_effects_outbox
  ON tape_effects(status, dispatch_mode, next_dispatch_at_ms);
-- Business-level dedupe (when the agent/connector supplies a key): no two
-- effects may share the same (connector, business_key). Partial index, so
-- the default empty-string is allowed many times.
--
-- Gated on connector <> '' as well as business_key <> '' — server-side
-- begin_effect refuses business_key without connector, but the index guard
-- keeps the DB defensive against a bypassing impl (without this, two
-- effects with `business_key='X', connector=''` would collide on the
-- (connector='', key) index entry).
CREATE UNIQUE INDEX IF NOT EXISTS idx_effects_business_key
  ON tape_effects(connector, business_key)
  WHERE business_key <> '' AND connector <> '';

CREATE TABLE IF NOT EXISTS tape_obligations (
  run_id              TEXT NOT NULL,
  seq                 INTEGER NOT NULL,
  effect_key          TEXT NOT NULL,
  kind                TEXT NOT NULL,
  payload_json        TEXT NOT NULL DEFAULT '',
  status              INTEGER NOT NULL,                -- ObligationStatus enum
  ts_ms               INTEGER NOT NULL,
  compensator_ref     TEXT NOT NULL DEFAULT '',
  attempts            INTEGER NOT NULL DEFAULT 0,
  max_attempts        INTEGER NOT NULL DEFAULT 5,
  next_attempt_at_ms  INTEGER NOT NULL DEFAULT 0,
  last_error          TEXT NOT NULL DEFAULT '',
  claimed_by          TEXT NOT NULL DEFAULT '',
  claim_expires_at_ms INTEGER NOT NULL DEFAULT 0,
  result_json         TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (run_id, seq)
);
-- The drainer's hot queries: ready-to-run PENDING and expired-lease COMMITTED.
CREATE INDEX IF NOT EXISTS idx_obligations_drain ON tape_obligations(status, next_attempt_at_ms);
CREATE INDEX IF NOT EXISTS idx_obligations_lease ON tape_obligations(status, claim_expires_at_ms);

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

CREATE TABLE IF NOT EXISTS tape_timers (
  run_id       TEXT NOT NULL,
  timer_id     TEXT NOT NULL,
  fire_at_ms   INTEGER NOT NULL,
  kind         TEXT NOT NULL DEFAULT '',
  payload_json TEXT NOT NULL DEFAULT '',
  fired        INTEGER NOT NULL DEFAULT 0,
  created_at_ms INTEGER NOT NULL,
  PRIMARY KEY (run_id, timer_id)
);
CREATE INDEX IF NOT EXISTS idx_timers_due ON tape_timers(fired, fire_at_ms);

CREATE TABLE IF NOT EXISTS tape_values (
  namespace    TEXT NOT NULL,
  key          TEXT NOT NULL,
  value_json   TEXT NOT NULL DEFAULT '',
  version      INTEGER NOT NULL DEFAULT 0,
  ts_ms        INTEGER NOT NULL,
  writer       TEXT NOT NULL DEFAULT '',
  deleted      INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (namespace, key)
);
CREATE INDEX IF NOT EXISTS idx_values_changed ON tape_values(namespace, key, version);
