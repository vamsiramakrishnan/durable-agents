-- Tape schema — PostgreSQL dialect. Mirrors 0001_init.sqlite.sql column-for-column;
-- the only differences are int/float type names and the absence of SQLite PRAGMAs.
-- Run with batch_execute (multiple statements in one call).

CREATE TABLE IF NOT EXISTS tape_runs (
  run_id              TEXT PRIMARY KEY,
  app_name            TEXT NOT NULL,
  user_id             TEXT NOT NULL,
  session_id          TEXT NOT NULL,
  invocation_id       TEXT NOT NULL,
  status              BIGINT NOT NULL,
  seq_cursor          BIGINT NOT NULL DEFAULT 0,
  lease_owner         TEXT NOT NULL DEFAULT '',
  lease_expires_at_ms BIGINT NOT NULL DEFAULT 0,
  waiting_on_gate     TEXT NOT NULL DEFAULT '',
  detail_json         TEXT NOT NULL DEFAULT '',
  started_at_ms       BIGINT NOT NULL,
  ended_at_ms         BIGINT NOT NULL DEFAULT 0,
  -- Identity & authorization context (BeginRunRequest §"Identity").
  tenant_id           TEXT NOT NULL DEFAULT '',
  actor               TEXT NOT NULL DEFAULT '',
  subject             TEXT NOT NULL DEFAULT '',
  agent_id            TEXT NOT NULL DEFAULT '',
  aiplex_instance_id  TEXT NOT NULL DEFAULT '',
  gateway_route       TEXT NOT NULL DEFAULT '',
  scopes_json         TEXT NOT NULL DEFAULT '[]',
  labels_json         TEXT NOT NULL DEFAULT '{}',
  -- Compaction (PR 13). 0 until the compactor zeroes the run's payloads.
  compacted_at_ms     BIGINT  NOT NULL DEFAULT 0,
  UNIQUE(app_name, user_id, session_id, invocation_id)
);
CREATE INDEX IF NOT EXISTS idx_runs_recover ON tape_runs(status, lease_expires_at_ms);
CREATE INDEX IF NOT EXISTS idx_runs_route ON tape_runs(app_name, user_id, session_id);
CREATE INDEX IF NOT EXISTS idx_runs_tenant  ON tape_runs(tenant_id, agent_id, started_at_ms);
CREATE INDEX IF NOT EXISTS idx_runs_actor   ON tape_runs(actor, started_at_ms);
CREATE INDEX IF NOT EXISTS idx_runs_subject ON tape_runs(subject, started_at_ms);
CREATE INDEX IF NOT EXISTS idx_runs_compactable
  ON tape_runs(status, compacted_at_ms, ended_at_ms);

CREATE TABLE IF NOT EXISTS tape_journal (
  run_id       TEXT NOT NULL,
  seq          BIGINT NOT NULL,
  kind         TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  ts_ms        BIGINT NOT NULL,
  PRIMARY KEY (run_id, seq)
);

CREATE TABLE IF NOT EXISTS tape_decisions (
  run_id         TEXT NOT NULL,
  seq            BIGINT NOT NULL,
  decision_index BIGINT NOT NULL,
  model          TEXT NOT NULL,
  request_json   TEXT NOT NULL,
  response_json  TEXT NOT NULL,
  rationale      TEXT NOT NULL DEFAULT '',
  policy_version TEXT NOT NULL DEFAULT '',
  ts_ms          BIGINT NOT NULL,
  PRIMARY KEY (run_id, decision_index)
);

CREATE TABLE IF NOT EXISTS tape_effects (
  run_id          TEXT NOT NULL,
  seq             BIGINT NOT NULL,
  decision_index  BIGINT NOT NULL,
  tool_name       TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  status          BIGINT NOT NULL,
  request_json    TEXT NOT NULL DEFAULT '',
  response_json   TEXT NOT NULL DEFAULT '',
  error_json      TEXT NOT NULL DEFAULT '',
  ts_ms           BIGINT NOT NULL,
  -- Outbox / non-idempotent contract (see proto: EffectSemantics, EffectDispatchMode).
  semantics                       INTEGER NOT NULL DEFAULT 1,    -- IDEMPOTENT
  dispatch_mode                   INTEGER NOT NULL DEFAULT 1,    -- INLINE
  business_key                    TEXT    NOT NULL DEFAULT '',
  connector                       TEXT    NOT NULL DEFAULT '',
  dispatch_attempts               INTEGER NOT NULL DEFAULT 0,
  next_dispatch_at_ms             BIGINT  NOT NULL DEFAULT 0,
  external_ref                    TEXT    NOT NULL DEFAULT '',
  dispatch_claimed_by             TEXT    NOT NULL DEFAULT '',
  dispatch_claim_expires_at_ms    BIGINT  NOT NULL DEFAULT 0,
  last_dispatch_error             TEXT    NOT NULL DEFAULT '',
  -- Authorization (see proto: BeginEffectRequest.scope).
  scope                           TEXT    NOT NULL DEFAULT '',
  PRIMARY KEY (run_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_effects_status ON tape_effects(status);
-- The outbox dispatcher's hot query: PENDING + OUTBOX + due.
CREATE INDEX IF NOT EXISTS idx_effects_outbox
  ON tape_effects(status, dispatch_mode, next_dispatch_at_ms);
-- Business-level dedupe (where the agent/connector supplied a key): no two
-- effects may share the same (connector, business_key). Partial index, so
-- the default empty-string is allowed many times.
--
-- Gated on connector <> '' as well as business_key <> '' — the server-side
-- begin_effect already refuses business_key without connector, but the
-- index guard makes the DB defensive even if a future store impl bypasses
-- that check (a non-empty business_key with an empty connector would
-- otherwise collapse all such rows under the same (connector='', key)
-- index entry and raise a unique-constraint error on the second insert).
CREATE UNIQUE INDEX IF NOT EXISTS idx_effects_business_key
  ON tape_effects(connector, business_key)
  WHERE business_key <> '' AND connector <> '';

CREATE TABLE IF NOT EXISTS tape_obligations (
  run_id              TEXT NOT NULL,
  seq                 BIGINT NOT NULL,
  effect_key          TEXT NOT NULL,
  kind                TEXT NOT NULL,
  payload_json        TEXT NOT NULL DEFAULT '',
  status              BIGINT NOT NULL,
  ts_ms               BIGINT NOT NULL,
  compensator_ref     TEXT NOT NULL DEFAULT '',
  attempts            INTEGER NOT NULL DEFAULT 0,
  max_attempts        INTEGER NOT NULL DEFAULT 5,
  next_attempt_at_ms  BIGINT NOT NULL DEFAULT 0,
  last_error          TEXT NOT NULL DEFAULT '',
  claimed_by          TEXT NOT NULL DEFAULT '',
  claim_expires_at_ms BIGINT NOT NULL DEFAULT 0,
  result_json         TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (run_id, seq)
);
-- The drainer's hot query: ready-to-run PENDING and expired-lease COMMITTED.
CREATE INDEX IF NOT EXISTS idx_obligations_drain ON tape_obligations(status, next_attempt_at_ms);
CREATE INDEX IF NOT EXISTS idx_obligations_lease ON tape_obligations(status, claim_expires_at_ms);

CREATE TABLE IF NOT EXISTS tape_budget (
  run_id       TEXT PRIMARY KEY,
  usd_cap      DOUBLE PRECISION NOT NULL DEFAULT 0,
  token_cap    BIGINT NOT NULL DEFAULT 0,
  usd_spent    DOUBLE PRECISION NOT NULL DEFAULT 0,
  tokens_spent BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS tape_signals (
  run_id          TEXT NOT NULL,
  gate_name       TEXT NOT NULL,
  context_json    TEXT NOT NULL DEFAULT '',
  resolution_json TEXT NOT NULL DEFAULT '',
  delivered       BIGINT NOT NULL DEFAULT 0,
  awaited         BIGINT NOT NULL DEFAULT 0,
  consumed        BIGINT NOT NULL DEFAULT 0,
  created_at_ms   BIGINT NOT NULL,
  PRIMARY KEY (run_id, gate_name)
);

CREATE TABLE IF NOT EXISTS tape_sessions (
  app_name           TEXT NOT NULL,
  user_id            TEXT NOT NULL,
  session_id         TEXT NOT NULL,
  state_json         TEXT NOT NULL DEFAULT '{}',
  last_update_time_ms BIGINT NOT NULL,
  PRIMARY KEY (app_name, user_id, session_id)
);

CREATE TABLE IF NOT EXISTS tape_events (
  app_name      TEXT NOT NULL,
  user_id       TEXT NOT NULL,
  session_id    TEXT NOT NULL,
  ord           BIGINT NOT NULL,
  event_id      TEXT NOT NULL,
  invocation_id TEXT NOT NULL DEFAULT '',
  author        TEXT NOT NULL DEFAULT '',
  branch        TEXT NOT NULL DEFAULT '',
  content_json  TEXT NOT NULL DEFAULT '',
  actions_json  TEXT NOT NULL DEFAULT '',
  timestamp_ms  BIGINT NOT NULL,
  PRIMARY KEY (app_name, user_id, session_id, ord)
);

CREATE TABLE IF NOT EXISTS tape_scoped_state (
  app_name  TEXT NOT NULL,
  scope     TEXT NOT NULL,
  scope_key TEXT NOT NULL DEFAULT '',
  key       TEXT NOT NULL,
  value_json TEXT NOT NULL,
  PRIMARY KEY (app_name, scope, scope_key, key)
);

CREATE TABLE IF NOT EXISTS tape_timers (
  run_id       TEXT NOT NULL,
  timer_id     TEXT NOT NULL,
  fire_at_ms   BIGINT NOT NULL,
  kind         TEXT NOT NULL DEFAULT '',
  payload_json TEXT NOT NULL DEFAULT '',
  fired        BIGINT NOT NULL DEFAULT 0,
  created_at_ms BIGINT NOT NULL,
  PRIMARY KEY (run_id, timer_id)
);
CREATE INDEX IF NOT EXISTS idx_timers_due ON tape_timers(fired, fire_at_ms);

CREATE TABLE IF NOT EXISTS tape_values (
  namespace    TEXT NOT NULL,
  key          TEXT NOT NULL,
  value_json   TEXT NOT NULL DEFAULT '',
  version      BIGINT NOT NULL DEFAULT 0,
  ts_ms        BIGINT NOT NULL,
  writer       TEXT NOT NULL DEFAULT '',
  deleted      BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (namespace, key)
);
CREATE INDEX IF NOT EXISTS idx_values_changed ON tape_values(namespace, key, version);
