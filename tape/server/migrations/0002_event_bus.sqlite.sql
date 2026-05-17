-- Tape J/K → event-bus rebuild (see design-principles/tape-event-bus.md).
-- SQLite dialect. Idempotent: every change is guarded so an existing dev DB
-- migrates in place. The Postgres mirror is 0002_event_bus.postgres.sql.

-- ── tape_journal: global_seq + subject + schema_version + OTel cols ─────────
-- SQLite doesn't let us add an INTEGER PK AUTOINCREMENT column in place. So
-- we add `global_seq INTEGER` (nullable for legacy rows, populated for new
-- ones via a per-insert max+1 in code), and a unique index on it. New rows
-- get global_seq from a dedicated counter table to keep allocation monotonic
-- across processes.

ALTER TABLE tape_journal ADD COLUMN global_seq INTEGER;
ALTER TABLE tape_journal ADD COLUMN subject TEXT NOT NULL DEFAULT '';
ALTER TABLE tape_journal ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE tape_journal ADD COLUMN trace_id TEXT NOT NULL DEFAULT '';
ALTER TABLE tape_journal ADD COLUMN span_id TEXT NOT NULL DEFAULT '';
ALTER TABLE tape_journal ADD COLUMN parent_span_id TEXT NOT NULL DEFAULT '';

CREATE UNIQUE INDEX IF NOT EXISTS idx_journal_global_seq ON tape_journal(global_seq);
CREATE INDEX IF NOT EXISTS idx_journal_subject ON tape_journal(subject, global_seq);

-- The global_seq counter. One row, one column, single source of truth.
-- Allocation is "UPDATE … SET v = v + 1 RETURNING v" inside the journal-write
-- transaction; SQLite serializes those writes (WAL), so monotonic.
CREATE TABLE IF NOT EXISTS tape_global_seq (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  v  INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO tape_global_seq (id, v) VALUES (1, 0);

-- Backfill global_seq for any pre-existing journal rows (rowid order is
-- insertion order in WAL-mode SQLite). After this, all rows have a unique
-- global_seq and the counter is at the max.
UPDATE tape_journal SET global_seq = rowid WHERE global_seq IS NULL;
UPDATE tape_global_seq SET v = COALESCE((SELECT MAX(global_seq) FROM tape_journal), 0) WHERE id = 1;

-- ── reactions ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tape_reactions (
  reaction_id      TEXT PRIMARY KEY,
  name             TEXT NOT NULL DEFAULT '',
  subject_pattern  TEXT NOT NULL,
  predicate_cel    TEXT NOT NULL DEFAULT '',
  handler_kind     INTEGER NOT NULL,                       -- 1=agent 2=task 3=publish
  agent_app        TEXT NOT NULL DEFAULT '',
  publish_target   TEXT NOT NULL DEFAULT '',
  max_concurrency  INTEGER NOT NULL DEFAULT 1,
  rate_limit_per_s INTEGER NOT NULL DEFAULT 0,
  debounce_ms      INTEGER NOT NULL DEFAULT 0,
  retry_max        INTEGER NOT NULL DEFAULT 5,
  retry_backoff_ms INTEGER NOT NULL DEFAULT 1000,
  dlq_after_n      INTEGER NOT NULL DEFAULT 5,
  num_shards       INTEGER NOT NULL DEFAULT 1,
  created_at_ms    INTEGER NOT NULL,
  deleted          INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_reactions_active ON tape_reactions(deleted, reaction_id);

-- ── per-reaction, per-shard cursor over tape_journal.global_seq ─────────────
CREATE TABLE IF NOT EXISTS tape_reaction_cursors (
  reaction_id          TEXT NOT NULL,
  shard                INTEGER NOT NULL,
  last_global_seq      INTEGER NOT NULL DEFAULT 0,
  last_processed_at_ms INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (reaction_id, shard)
);

-- ── tasks: cheap ephemeral handler invocations ──────────────────────────────
CREATE TABLE IF NOT EXISTS tape_tasks (
  task_id              TEXT PRIMARY KEY,
  reaction_id          TEXT NOT NULL,
  shard                INTEGER NOT NULL DEFAULT 0,
  source_run_id        TEXT NOT NULL,
  source_global_seq    INTEGER NOT NULL,
  subject              TEXT NOT NULL,
  payload_json         TEXT NOT NULL DEFAULT '',
  status               INTEGER NOT NULL,                   -- 1=pending 2=claimed 3=done 4=failed 5=dlq
  attempts             INTEGER NOT NULL DEFAULT 0,
  next_attempt_at_ms   INTEGER NOT NULL DEFAULT 0,
  lease_owner          TEXT NOT NULL DEFAULT '',
  lease_expires_at_ms  INTEGER NOT NULL DEFAULT 0,
  last_error           TEXT NOT NULL DEFAULT '',
  created_at_ms        INTEGER NOT NULL,
  completed_at_ms      INTEGER NOT NULL DEFAULT 0,
  trace_id             TEXT NOT NULL DEFAULT '',
  parent_span_id       TEXT NOT NULL DEFAULT '',
  UNIQUE (reaction_id, shard, source_global_seq)
);
CREATE INDEX IF NOT EXISTS idx_tasks_claim
  ON tape_tasks(reaction_id, shard, status, next_attempt_at_ms);
CREATE INDEX IF NOT EXISTS idx_tasks_lease
  ON tape_tasks(status, lease_expires_at_ms);
