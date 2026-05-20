package embedded

// schema.go — the four tables that mirror the Python `tape_adk.schemas`
// module byte-for-byte at the column level. Same names, same SQL types,
// same composite primary keys, same UNIQUE constraint on
// (connector, business_key), same supporting indices.
//
// Two intentional differences from the SQLAlchemy version:
//
//   * The Python `Base` extends ADK's session base so `Base.metadata.create_all()`
//     also creates `sessions`. The Go embedded form has no ADK-Go integration
//     yet (see doc.go); we emit the FOREIGN KEY clauses anyway so that — once
//     the user does have a `sessions` table in the same DB — cascade-delete
//     works. SQLite ignores unknown-target FKs unless `PRAGMA foreign_keys=ON`
//     is set AND the parent table exists; Postgres rejects the DDL outright.
//     The `WithoutSessionFK` option on `CreateAllTables` lets standalone
//     callers skip the FK clauses for portability.
//
//   * SQLite's `INTEGER PRIMARY KEY AUTOINCREMENT` is the on-disk equivalent
//     of SQLAlchemy's `Integer, primary_key=True, autoincrement=True` for the
//     obligations.seq column; Postgres uses `BIGSERIAL`. The dialect detector
//     picks the right one.

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Dialect — minimal driver discriminator. The CAS path and a couple of
// DDL fragments differ between SQLite and Postgres; everything else is
// portable.
type Dialect int

const (
	DialectUnknown Dialect = iota
	DialectSQLite
	DialectPostgres
)

func (d Dialect) String() string {
	switch d {
	case DialectSQLite:
		return "sqlite"
	case DialectPostgres:
		return "postgres"
	}
	return "unknown"
}

// DetectDialect — sniff the driver name from a probe row. We can't
// inspect a `*sql.DB`'s driver name through the public API, so callers
// either pass the dialect explicitly to `NewTapeSessionService` or rely
// on a runtime probe.
func DetectDialect(ctx context.Context, db *sql.DB) Dialect {
	// Postgres has `version()`; SQLite has `sqlite_version()`. Try
	// SQLite first because it's the more common embedded case.
	var s string
	if err := db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&s); err == nil {
		return DialectSQLite
	}
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&s); err == nil {
		return DialectPostgres
	}
	return DialectUnknown
}

// CreateAllOpts — options for CreateAllTables.
type CreateAllOpts struct {
	// Dialect — if zero, autodetected via DetectDialect.
	Dialect Dialect
	// WithoutSessionFK — emit DDL with no FOREIGN KEY references to the
	// ADK `sessions` table. Set this when running standalone (no ADK in
	// the same DB). Defaults to false: FK clauses are emitted, matching
	// Python's schema.
	WithoutSessionFK bool
}

// CreateAllTables — issue the four CREATE TABLE statements (idempotent
// via IF NOT EXISTS) plus the supporting indices. Safe to call on every
// startup; mirrors the Python `Base.metadata.create_all(engine)`.
func CreateAllTables(ctx context.Context, db *sql.DB, opts ...CreateAllOpts) error {
	var o CreateAllOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.Dialect == DialectUnknown {
		o.Dialect = DetectDialect(ctx, db)
		if o.Dialect == DialectUnknown {
			// Fall through to SQLite syntax — works for most engines.
			o.Dialect = DialectSQLite
		}
	}
	for _, stmt := range ddlStatements(o) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("embedded.CreateAllTables: %q: %w", firstLine(stmt), err)
		}
	}
	return nil
}

func firstLine(s string) string {
	if i := strings.Index(s, "\n"); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	return strings.TrimSpace(s)
}

// ddlStatements — split out so tests can inspect them.
func ddlStatements(o CreateAllOpts) []string {
	pgSeq := o.Dialect == DialectPostgres
	seqType := "INTEGER PRIMARY KEY AUTOINCREMENT"
	if pgSeq {
		seqType = "BIGSERIAL PRIMARY KEY"
	}
	// JSON columns: Python uses a DynamicJSON wrapper that picks JSONB on
	// Postgres and TEXT on SQLite. We mirror that.
	jsonType := "TEXT"
	if pgSeq {
		jsonType = "JSONB"
	}
	boolType := "INTEGER"
	if pgSeq {
		boolType = "BOOLEAN"
	}

	effectsFK := ""
	if !o.WithoutSessionFK {
		effectsFK = `,
  FOREIGN KEY (app_name, user_id, session_id)
    REFERENCES sessions(app_name, user_id, id) ON DELETE CASCADE`
	}
	obligationsFK := effectsFK
	timersFK := effectsFK

	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS tape_effects (
  app_name VARCHAR(128) NOT NULL,
  user_id VARCHAR(128) NOT NULL,
  session_id VARCHAR(128) NOT NULL,
  idempotency_key VARCHAR(256) NOT NULL,
  invocation_id VARCHAR(256) NOT NULL,
  decision_index INTEGER NOT NULL DEFAULT -1,
  tool_name VARCHAR(128) NOT NULL,
  call_index INTEGER NOT NULL DEFAULT 0,
  status VARCHAR(16) NOT NULL,
  semantics VARCHAR(16) NOT NULL DEFAULT 'idempotent',
  dispatch_mode VARCHAR(16) NOT NULL DEFAULT 'inline',
  business_key VARCHAR(256),
  connector VARCHAR(128),
  external_ref VARCHAR(256),
  dispatch_attempts INTEGER NOT NULL DEFAULT 0,
  next_dispatch_at_ms BIGINT NOT NULL DEFAULT 0,
  dispatch_claimed_by VARCHAR(128),
  dispatch_claim_expires_at_ms BIGINT NOT NULL DEFAULT 0,
  last_dispatch_error %s,
  request_json %s,
  response_json %s,
  error_json %s,
  ts_ms BIGINT NOT NULL,
  PRIMARY KEY (app_name, user_id, session_id, idempotency_key),
  CONSTRAINT uq_tape_effects_connector_business_key UNIQUE (connector, business_key)%s
)`, jsonType, jsonType, jsonType, jsonType, effectsFK),

		`CREATE INDEX IF NOT EXISTS ix_tape_effects_status_ts ON tape_effects (status, ts_ms)`,
		`CREATE INDEX IF NOT EXISTS ix_tape_effects_dispatch_ready ON tape_effects (dispatch_mode, status, next_dispatch_at_ms)`,
		`CREATE INDEX IF NOT EXISTS ix_tape_effects_invocation ON tape_effects (invocation_id)`,

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS tape_obligations (
  seq %s,
  app_name VARCHAR(128) NOT NULL,
  user_id VARCHAR(128) NOT NULL,
  session_id VARCHAR(128) NOT NULL,
  invocation_id VARCHAR(256) NOT NULL DEFAULT '',
  effect_key VARCHAR(256) NOT NULL,
  kind VARCHAR(128) NOT NULL,
  payload_json %s,
  status VARCHAR(16) NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 5,
  next_attempt_at_ms BIGINT NOT NULL DEFAULT 0,
  last_error %s,
  claimed_by VARCHAR(128),
  claim_expires_at_ms BIGINT NOT NULL DEFAULT 0,
  compensator_ref VARCHAR(256),
  result_json %s,
  ts_ms BIGINT NOT NULL,
  CONSTRAINT uq_tape_obligations_effect_kind_per_session
    UNIQUE (app_name, user_id, session_id, effect_key, kind)%s
)`, seqType, jsonType, jsonType, jsonType, obligationsFK),

		`CREATE INDEX IF NOT EXISTS ix_tape_obligations_status_next ON tape_obligations (status, next_attempt_at_ms)`,
		`CREATE INDEX IF NOT EXISTS ix_tape_obligations_status ON tape_obligations (status)`,

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS tape_timers (
  app_name VARCHAR(128) NOT NULL,
  user_id VARCHAR(128) NOT NULL,
  session_id VARCHAR(128) NOT NULL,
  timer_id VARCHAR(256) NOT NULL,
  fire_at_ms BIGINT NOT NULL,
  kind VARCHAR(128) NOT NULL,
  payload_json %s,
  fired %s NOT NULL DEFAULT 0,
  created_at_ms BIGINT NOT NULL,
  PRIMARY KEY (app_name, user_id, session_id, timer_id)%s
)`, jsonType, boolType, timersFK),

		`CREATE INDEX IF NOT EXISTS ix_tape_timers_fire ON tape_timers (fire_at_ms)`,
		`CREATE INDEX IF NOT EXISTS ix_tape_timers_due ON tape_timers (fired, fire_at_ms)`,

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS tape_values (
  namespace VARCHAR(128) NOT NULL,
  key VARCHAR(256) NOT NULL,
  value_json %s,
  version INTEGER NOT NULL DEFAULT 0,
  ts_ms BIGINT NOT NULL,
  writer VARCHAR(128),
  deleted %s NOT NULL DEFAULT 0,
  PRIMARY KEY (namespace, key)
)`, jsonType, boolType),
	}
}
