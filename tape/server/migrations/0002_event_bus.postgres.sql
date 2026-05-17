-- Tape J/K → event-bus rebuild (see design-principles/tape-event-bus.md).
-- PostgreSQL dialect. Mirrors 0002_event_bus.sqlite.sql; differences:
--   * `global_seq` uses a real BIGSERIAL sequence (and a backfill);
--   * we install a NOTIFY trigger so SubscribeBySubject can wake on insert
--     instead of polling.
-- Run with batch_execute (multiple statements in one call). Idempotent.

-- ── tape_journal: global_seq + subject + schema_version + OTel cols ─────────
ALTER TABLE tape_journal ADD COLUMN IF NOT EXISTS global_seq      BIGINT;
ALTER TABLE tape_journal ADD COLUMN IF NOT EXISTS subject         TEXT NOT NULL DEFAULT '';
ALTER TABLE tape_journal ADD COLUMN IF NOT EXISTS schema_version  SMALLINT NOT NULL DEFAULT 1;
ALTER TABLE tape_journal ADD COLUMN IF NOT EXISTS trace_id        TEXT NOT NULL DEFAULT '';
ALTER TABLE tape_journal ADD COLUMN IF NOT EXISTS span_id         TEXT NOT NULL DEFAULT '';
ALTER TABLE tape_journal ADD COLUMN IF NOT EXISTS parent_span_id  TEXT NOT NULL DEFAULT '';

CREATE SEQUENCE IF NOT EXISTS tape_journal_global_seq_seq AS BIGINT INCREMENT BY 1 MINVALUE 1 START 1 NO CYCLE;

-- Backfill any pre-existing rows (one-time, idempotent — only rows still NULL).
DO $$
DECLARE
  rec RECORD;
  v   BIGINT;
BEGIN
  FOR rec IN SELECT ctid FROM tape_journal WHERE global_seq IS NULL ORDER BY ts_ms, run_id, seq LOOP
    v := nextval('tape_journal_global_seq_seq');
    UPDATE tape_journal SET global_seq = v WHERE ctid = rec.ctid;
  END LOOP;
END $$;

ALTER TABLE tape_journal ALTER COLUMN global_seq SET NOT NULL;
ALTER TABLE tape_journal ALTER COLUMN global_seq SET DEFAULT nextval('tape_journal_global_seq_seq');
-- Re-sync the sequence past any backfilled max (the producers will use the
-- default via column omission going forward).
SELECT setval('tape_journal_global_seq_seq',
              GREATEST((SELECT COALESCE(MAX(global_seq), 0) FROM tape_journal), 1));

CREATE UNIQUE INDEX IF NOT EXISTS idx_journal_global_seq ON tape_journal(global_seq);
CREATE INDEX IF NOT EXISTS idx_journal_subject ON tape_journal(subject text_pattern_ops, global_seq);

-- The NOTIFY trigger — server-side wake-ups for SubscribeBySubject /
-- SubscribeEvents. The payload is `<global_seq>:<subject>` so a listener can
-- skip the SELECT if the subject doesn't match its pattern.
CREATE OR REPLACE FUNCTION tape_journal_notify() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('tape_journal', NEW.global_seq::text || ':' || COALESCE(NEW.subject, ''));
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tape_journal_notify_trg ON tape_journal;
CREATE TRIGGER tape_journal_notify_trg
  AFTER INSERT ON tape_journal
  FOR EACH ROW EXECUTE FUNCTION tape_journal_notify();

-- ── reactions ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tape_reactions (
  reaction_id      TEXT PRIMARY KEY,
  name             TEXT NOT NULL DEFAULT '',
  subject_pattern  TEXT NOT NULL,
  predicate_cel    TEXT NOT NULL DEFAULT '',
  handler_kind     SMALLINT NOT NULL,
  agent_app        TEXT NOT NULL DEFAULT '',
  publish_target   TEXT NOT NULL DEFAULT '',
  max_concurrency  INTEGER NOT NULL DEFAULT 1,
  rate_limit_per_s INTEGER NOT NULL DEFAULT 0,
  debounce_ms      INTEGER NOT NULL DEFAULT 0,
  retry_max        INTEGER NOT NULL DEFAULT 5,
  retry_backoff_ms INTEGER NOT NULL DEFAULT 1000,
  dlq_after_n      INTEGER NOT NULL DEFAULT 5,
  num_shards       INTEGER NOT NULL DEFAULT 1,
  created_at_ms    BIGINT NOT NULL,
  deleted          SMALLINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_reactions_active ON tape_reactions(deleted, reaction_id);

-- ── per-reaction, per-shard cursor ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tape_reaction_cursors (
  reaction_id          TEXT NOT NULL,
  shard                INTEGER NOT NULL,
  last_global_seq      BIGINT NOT NULL DEFAULT 0,
  last_processed_at_ms BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (reaction_id, shard)
);

-- ── tasks ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tape_tasks (
  task_id              TEXT PRIMARY KEY,
  reaction_id          TEXT NOT NULL,
  shard                INTEGER NOT NULL DEFAULT 0,
  source_run_id        TEXT NOT NULL,
  source_global_seq    BIGINT NOT NULL,
  subject              TEXT NOT NULL,
  payload_json         TEXT NOT NULL DEFAULT '',
  status               SMALLINT NOT NULL,
  attempts             INTEGER NOT NULL DEFAULT 0,
  next_attempt_at_ms   BIGINT NOT NULL DEFAULT 0,
  lease_owner          TEXT NOT NULL DEFAULT '',
  lease_expires_at_ms  BIGINT NOT NULL DEFAULT 0,
  last_error           TEXT NOT NULL DEFAULT '',
  created_at_ms        BIGINT NOT NULL,
  completed_at_ms      BIGINT NOT NULL DEFAULT 0,
  trace_id             TEXT NOT NULL DEFAULT '',
  parent_span_id       TEXT NOT NULL DEFAULT '',
  UNIQUE (reaction_id, shard, source_global_seq)
);
CREATE INDEX IF NOT EXISTS idx_tasks_claim
  ON tape_tasks(reaction_id, shard, status, next_attempt_at_ms);
CREATE INDEX IF NOT EXISTS idx_tasks_lease
  ON tape_tasks(status, lease_expires_at_ms);
