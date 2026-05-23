package embedded

// service.go — the fourteen methods, all `database/sql` parameterised
// queries, mirroring `tape_adk.service.TapeSessionService` in shape and
// semantics. Status enums are string-typed constants matching the
// Python ones byte-for-byte.
//
// CAS — the load-bearing primitive — is the same UPDATE…WHERE pattern
// the Python sibling uses: the eligibility predicate (status, lease
// expiry, dispatch-mode, next-dispatch window) lives inline in the
// WHERE clause; `RowsAffected() == 1` means "we won". On SQLite that
// pattern is only safe if BEGIN/UPDATE/COMMIT can't interleave across
// concurrent goroutines on the shared connection — so we wrap the CAS
// path in `mu.Lock()` when the dialect is SQLite. Postgres needs no
// such gate.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// ── status enums (mirror proto + Python) ──────────────────────────────────

// EffectStatus is the effect ledger state machine.
type EffectStatus = string

const (
	EffectStatusPending   EffectStatus = "pending"
	EffectStatusConfirmed EffectStatus = "confirmed"
	EffectStatusFailed    EffectStatus = "failed"
	EffectStatusUnknown   EffectStatus = "unknown"
)

// EffectSemantics — the upstream's safety contract.
type EffectSemantics = string

const (
	EffectSemanticsIdempotent    EffectSemantics = "idempotent"
	EffectSemanticsNonIdempotent EffectSemantics = "non_idempotent"
	EffectSemanticsObserveOnly   EffectSemantics = "observe_only"
)

// EffectDispatchMode — inline (call the body, dedupe by key) vs outbox
// (journal an intent, reactor dispatches).
type EffectDispatchMode = string

const (
	EffectDispatchInline EffectDispatchMode = "inline"
	EffectDispatchOutbox EffectDispatchMode = "outbox"
)

// EffectResolution — what the reconciler observed upstream.
type EffectResolution = string

const (
	EffectResolutionConfirmed EffectResolution = "confirmed"
	EffectResolutionFailed    EffectResolution = "failed"
	EffectResolutionAbsent    EffectResolution = "absent"
	EffectResolutionDuplicate EffectResolution = "duplicate"
	EffectResolutionStuck     EffectResolution = "stuck"
)

// ObligationStatus — the obligation lifecycle.
type ObligationStatus = string

const (
	ObligationStatusPending      ObligationStatus = "pending"
	ObligationStatusCommitted    ObligationStatus = "committed"
	ObligationStatusCompensated  ObligationStatus = "compensated"
	ObligationStatusStuck        ObligationStatus = "stuck"
)

// ── records (lightweight structs returned by service methods) ─────────────

// EffectRecord — the effect ledger row, JSON columns decoded to `any`.
type EffectRecord struct {
	AppName                  string
	UserID                   string
	SessionID                string
	IdempotencyKey           string
	InvocationID             string
	DecisionIndex            int
	ToolName                 string
	CallIndex                int
	Status                   string
	Semantics                string
	DispatchMode             string
	BusinessKey              string
	Connector                string
	ExternalRef              string
	DispatchAttempts         int
	NextDispatchAtMs         int64
	DispatchClaimedBy        string
	DispatchClaimExpiresAtMs int64
	LastDispatchError        any
	RequestJSON              any
	ResponseJSON             any
	ErrorJSON                any
	TsMs                     int64
}

// ObligationRecord — the obligation ledger row.
type ObligationRecord struct {
	Seq               int64
	AppName           string
	UserID            string
	SessionID         string
	InvocationID      string
	EffectKey         string
	Kind              string
	PayloadJSON       any
	Status            string
	Attempts          int
	MaxAttempts       int
	NextAttemptAtMs   int64
	LastError         any
	ClaimedBy         string
	ClaimExpiresAtMs  int64
	CompensatorRef    string
	ResultJSON        any
	TsMs              int64
}

// TimerRecord — a row in `tape_timers`.
type TimerRecord struct {
	AppName     string
	UserID      string
	SessionID   string
	TimerID     string
	FireAtMs    int64
	Kind        string
	PayloadJSON any
	Fired       bool
	CreatedAtMs int64
}

// ValueRecord — a row in `tape_values`.
type ValueRecord struct {
	Namespace string
	Key       string
	ValueJSON any
	Version   int
	TsMs      int64
	Writer    string
	Deleted   bool
}

// ── helpers ───────────────────────────────────────────────────────────────

func nowMs() int64 { return time.Now().UnixNano() / int64(time.Millisecond) }

// nowOr — `nowMs` unless the caller provided a value (mirrors Python's
// `now_ms or _now_ms()` idiom).
func nowOr(v int64) int64 {
	if v != 0 {
		return v
	}
	return nowMs()
}

// jsonOrNil — JSON-encode `v` for storage. Nil/empty → SQL NULL.
func jsonOrNil(v any) any {
	if v == nil {
		return nil
	}
	if s, ok := v.(string); ok && s == "" {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return string(b)
}

// decodeJSON — best-effort: returns the raw string on parse failure.
func decodeJSON(raw any) any {
	if raw == nil {
		return nil
	}
	var s string
	switch v := raw.(type) {
	case string:
		s = v
	case []byte:
		s = string(v)
	default:
		return raw
	}
	if s == "" {
		return nil
	}
	var out any
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return s
	}
	return out
}

// ns / nb — nullable-string and nullable-int64 helpers.
func ns(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// scanString — pulls a NULL-able string out of a *sql.NullString.
func scanString(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

// scanInt64 — pulls a NULL-able int64 out of a *sql.NullInt64.
func scanInt64(ni sql.NullInt64) int64 {
	if ni.Valid {
		return ni.Int64
	}
	return 0
}

// boolToInt — SQLite stores booleans as 0/1 INTEGERs; Postgres has real
// BOOLEAN. driver.Value accepts both — we send int for SQLite, bool for
// Postgres, but `database/sql` is lenient enough we can just always
// pass `bool` to Exec and let the driver do the right thing.

// ── the service ───────────────────────────────────────────────────────────

// TapeSessionService — the standalone embedded-mode counterpart of the
// Python `tape_adk.TapeSessionService`. Bring your own `*sql.DB`. Call
// `CreateAllTables(ctx, db)` once at startup to provision the schema.
type TapeSessionService struct {
	db      *sql.DB
	dialect Dialect

	// casMu — serialises CAS attempts when the dialect is SQLite (see
	// service.go header). No-op on Postgres.
	casMu sync.Mutex
}

// NewTapeSessionService — wrap a `*sql.DB`. Pass `dialect=DialectUnknown`
// to autodetect via the driver's version function.
func NewTapeSessionService(ctx context.Context, db *sql.DB, dialect Dialect) *TapeSessionService {
	if dialect == DialectUnknown {
		dialect = DetectDialect(ctx, db)
		if dialect == DialectUnknown {
			dialect = DialectSQLite
		}
	}
	return &TapeSessionService{db: db, dialect: dialect}
}

// DB — exposes the underlying handle for callers that want to share
// connections with their app's own queries.
func (s *TapeSessionService) DB() *sql.DB { return s.db }

// Dialect — exposes the resolved dialect.
func (s *TapeSessionService) Dialect() Dialect { return s.dialect }

// withCASLock — call `fn` under the CAS mutex on SQLite, raw on Postgres.
func (s *TapeSessionService) withCASLock(fn func() error) error {
	if s.dialect == DialectSQLite {
		s.casMu.Lock()
		defer s.casMu.Unlock()
	}
	return fn()
}

// placeholders — Postgres uses $1,$2; SQLite uses ?. We hand-write
// every query and choose at call time.
func (s *TapeSessionService) ph(n int) string {
	if s.dialect == DialectPostgres {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}

// rewritePlaceholders — for queries built with `?` (the common case), swap
// to `$1, $2, …` when running against Postgres.
func (s *TapeSessionService) rew(q string) string {
	if s.dialect != DialectPostgres {
		return q
	}
	var b strings.Builder
	b.Grow(len(q) + 8)
	n := 0
	for i := 0; i < len(q); i++ {
		if q[i] == '?' {
			n++
			fmt.Fprintf(&b, "$%d", n)
			continue
		}
		b.WriteByte(q[i])
	}
	return b.String()
}

// ── effect ledger ─────────────────────────────────────────────────────────

// BeginEffectOpts — keyword-arg analogue of the Python signature.
type BeginEffectOpts struct {
	AppName       string
	UserID        string
	SessionID     string
	InvocationID  string
	DecisionIndex int
	ToolName      string
	CallIndex     int
	RequestJSON   any
	CustomKey     string
	Semantics     string
	DispatchMode  string
	BusinessKey   string
	Connector     string
}

// BeginEffect — idempotent on the derived key
// (`<invocation>/decision-<idx>/<tool>/<call>`) or `CustomKey` if set.
// Server-side safety: refuses NON_IDEMPOTENT+INLINE and OUTBOX without
// a connector. (connector, business_key) UNIQUE clashes raise a
// "business_key already exists" error — that's the cross-run dedup.
func (s *TapeSessionService) BeginEffect(ctx context.Context, o BeginEffectOpts) (EffectRecord, error) {
	if o.Semantics == "" {
		o.Semantics = EffectSemanticsIdempotent
	}
	if o.DispatchMode == "" {
		o.DispatchMode = EffectDispatchInline
	}
	if o.Semantics == EffectSemanticsNonIdempotent && o.DispatchMode == EffectDispatchInline {
		return EffectRecord{}, errors.New("BeginEffect: NON_IDEMPOTENT effects must use OUTBOX dispatch")
	}
	if o.DispatchMode == EffectDispatchOutbox && o.Connector == "" {
		return EffectRecord{}, errors.New("BeginEffect: OUTBOX dispatch requires a `Connector` name")
	}

	key := o.CustomKey
	if key == "" {
		key = fmt.Sprintf("%s/decision-%d/%s/%d", o.InvocationID, o.DecisionIndex, o.ToolName, o.CallIndex)
	}

	// Try to read an existing row — idempotent on replay.
	existing, err := s.GetEffect(ctx, o.AppName, o.UserID, o.SessionID, key)
	if err != nil {
		return EffectRecord{}, err
	}
	if existing != nil {
		return *existing, nil
	}

	// Snapshot fallback: the live row may have been pruned by the
	// compactor. If we have a terminal-state snapshot for this key,
	// synthesise the short-circuit EffectRecord from it so the caller
	// sees the same idempotent behaviour they'd see with the row still
	// present. No row is created here — the snapshot IS the durable
	// record. The lookup is the SECOND check (live row first), so a
	// disagreement between a still-present live row and a stale snapshot
	// entry resolves in favour of the live row.
	if rec, found, err := s.lookupSnapshotEntry(
		ctx, o.AppName, o.UserID, o.SessionID, key,
		o.ToolName, o.CallIndex, o.Semantics, o.DispatchMode,
	); err != nil {
		return EffectRecord{}, err
	} else if found {
		return rec, nil
	}

	now := nowMs()
	q := s.rew(`INSERT INTO tape_effects (
  app_name, user_id, session_id, idempotency_key, invocation_id,
  decision_index, tool_name, call_index, status, semantics, dispatch_mode,
  business_key, connector, external_ref,
  dispatch_attempts, next_dispatch_at_ms, dispatch_claimed_by, dispatch_claim_expires_at_ms,
  last_dispatch_error, request_json, response_json, error_json, ts_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	_, err = s.db.ExecContext(ctx, q,
		o.AppName, o.UserID, o.SessionID, key, o.InvocationID,
		o.DecisionIndex, o.ToolName, o.CallIndex, EffectStatusPending, o.Semantics, o.DispatchMode,
		ns(o.BusinessKey), ns(o.Connector), nil,
		0, 0, nil, 0,
		nil, jsonOrNil(o.RequestJSON), nil, nil, now,
	)
	if err != nil {
		// Unique-constraint violation on (connector, business_key) →
		// translate to the Python-style ValueError text so test asserts
		// match.
		if isUniqueViolation(err) {
			return EffectRecord{}, fmt.Errorf(
				"BeginEffect: business_key already exists for connector=%q: %w",
				o.Connector, err)
		}
		return EffectRecord{}, err
	}
	got, err := s.GetEffect(ctx, o.AppName, o.UserID, o.SessionID, key)
	if err != nil {
		return EffectRecord{}, err
	}
	if got == nil {
		return EffectRecord{}, errors.New("BeginEffect: row vanished after insert")
	}
	return *got, nil
}

// CompleteEffect — flip an effect's terminal status. No-op if already
// terminal — returns the current row as-is (mirrors proto semantics).
func (s *TapeSessionService) CompleteEffect(
	ctx context.Context,
	appName, userID, sessionID, idempotencyKey string,
	status string,
	responseJSON, errorJSON any,
) (*EffectRecord, error) {
	if status != EffectStatusConfirmed && status != EffectStatusFailed && status != EffectStatusUnknown {
		return nil, fmt.Errorf("CompleteEffect: invalid status %q", status)
	}
	existing, err := s.GetEffect(ctx, appName, userID, sessionID, idempotencyKey)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, nil
	}
	if existing.Status != EffectStatusPending {
		return existing, nil
	}
	now := nowMs()
	q := s.rew(`UPDATE tape_effects
SET status = ?, response_json = ?, error_json = ?, ts_ms = ?,
    dispatch_claimed_by = NULL, dispatch_claim_expires_at_ms = 0
WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?`)
	_, err = s.db.ExecContext(ctx, q,
		status, jsonOrNil(responseJSON), jsonOrNil(errorJSON), now,
		appName, userID, sessionID, idempotencyKey)
	if err != nil {
		return nil, err
	}
	return s.GetEffect(ctx, appName, userID, sessionID, idempotencyKey)
}

// GetEffect — single-row read by composite key.
func (s *TapeSessionService) GetEffect(
	ctx context.Context, appName, userID, sessionID, idempotencyKey string,
) (*EffectRecord, error) {
	q := s.rew(`SELECT ` + effectCols + ` FROM tape_effects
WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?`)
	row := s.db.QueryRowContext(ctx, q, appName, userID, sessionID, idempotencyKey)
	r, err := scanEffect(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// ── outbox: dispatch claim (CAS) + attempt recording ─────────────────────

// ClaimEffectDispatch — atomic CAS lease on the dispatch slot. Returns
// (acquired, current row). acquired=true ⇒ caller may dispatch.
func (s *TapeSessionService) ClaimEffectDispatch(
	ctx context.Context, appName, userID, sessionID, idempotencyKey, claimer string,
	leaseTTLMs int64, nowOverrideMs int64,
) (bool, *EffectRecord, error) {
	now := nowOr(nowOverrideMs)
	if leaseTTLMs <= 0 {
		leaseTTLMs = 60_000
	}
	expires := now + leaseTTLMs

	var acquired bool
	err := s.withCASLock(func() error {
		q := s.rew(`UPDATE tape_effects
SET dispatch_claimed_by = ?, dispatch_claim_expires_at_ms = ?
WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?
  AND status = ? AND dispatch_mode = ? AND next_dispatch_at_ms <= ?
  AND (dispatch_claimed_by IS NULL OR dispatch_claimed_by = '' OR dispatch_claim_expires_at_ms <= ?)`)
		res, err := s.db.ExecContext(ctx, q,
			claimer, expires,
			appName, userID, sessionID, idempotencyKey,
			EffectStatusPending, EffectDispatchOutbox, now,
			now)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		acquired = n == 1
		return nil
	})
	if err != nil {
		return false, nil, err
	}
	row, err := s.GetEffect(ctx, appName, userID, sessionID, idempotencyKey)
	if err != nil {
		return acquired, nil, err
	}
	return acquired, row, nil
}

// RecordDispatchAttempt — report a failed dispatch. `nextDispatchAtMs=0`
// is the load-bearing case: it flips PENDING → UNKNOWN so the
// reconciler takes over and the outbox stops blindly retrying.
func (s *TapeSessionService) RecordDispatchAttempt(
	ctx context.Context, appName, userID, sessionID, idempotencyKey, errMsg string,
	nextDispatchAtMs int64,
) (*EffectRecord, error) {
	existing, err := s.GetEffect(ctx, appName, userID, sessionID, idempotencyKey)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, nil
	}
	now := nowMs()
	attempts := existing.DispatchAttempts + 1
	status := existing.Status
	next := nextDispatchAtMs
	if nextDispatchAtMs <= 0 {
		status = EffectStatusUnknown
		next = 0
	}
	q := s.rew(`UPDATE tape_effects
SET dispatch_attempts = ?, last_dispatch_error = ?, dispatch_claimed_by = NULL,
    dispatch_claim_expires_at_ms = 0, status = ?, next_dispatch_at_ms = ?, ts_ms = ?
WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?`)
	_, err = s.db.ExecContext(ctx, q,
		attempts, jsonOrNil(errMsg), status, next, now,
		appName, userID, sessionID, idempotencyKey)
	if err != nil {
		return nil, err
	}
	return s.GetEffect(ctx, appName, userID, sessionID, idempotencyKey)
}

// RecordExternalObservationOpts — the reconciler's write path.
type RecordExternalObservationOpts struct {
	AppName                   string
	UserID                    string
	SessionID                 string
	IdempotencyKey            string
	Resolution                string
	ExternalRef               string
	ResponseJSON              any
	ErrorJSON                 any
	CompensateOnDuplicateKind string
}

// RecordExternalObservation — map EffectResolution → EffectStatus, and on
// DUPLICATE+CompensateOnDuplicateKind atomically register a compensation
// obligation in the same transaction. Returns the post-state.
func (s *TapeSessionService) RecordExternalObservation(
	ctx context.Context, o RecordExternalObservationOpts,
) (*EffectRecord, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	q := s.rew(`SELECT ` + effectCols + ` FROM tape_effects
WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?`)
	row := tx.QueryRowContext(ctx, q, o.AppName, o.UserID, o.SessionID, o.IdempotencyKey)
	existing, err := scanEffect(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	now := nowMs()
	newStatus := existing.Status
	newExternalRef := existing.ExternalRef
	newResponse := existing.ResponseJSON
	newError := existing.ErrorJSON

	switch o.Resolution {
	case EffectResolutionConfirmed:
		newStatus = EffectStatusConfirmed
		if o.ExternalRef != "" {
			newExternalRef = o.ExternalRef
		}
		newResponse = o.ResponseJSON
	case EffectResolutionFailed:
		newStatus = EffectStatusFailed
		newError = o.ErrorJSON
	case EffectResolutionAbsent:
		if existing.Semantics == EffectSemanticsNonIdempotent {
			newStatus = EffectStatusUnknown
		}
		if o.ErrorJSON != nil {
			newError = o.ErrorJSON
		}
	case EffectResolutionDuplicate:
		newStatus = EffectStatusConfirmed
		if o.ExternalRef != "" {
			newExternalRef = o.ExternalRef
		}
		newResponse = o.ResponseJSON
		if o.CompensateOnDuplicateKind != "" {
			payload := map[string]any{
				"external_ref": firstNonEmpty(o.ExternalRef, existing.ExternalRef),
				"reason":       "duplicate observed by reconciler",
			}
			insertQ := s.rew(`INSERT INTO tape_obligations (
  app_name, user_id, session_id, invocation_id, effect_key, kind, payload_json,
  status, attempts, max_attempts, next_attempt_at_ms, last_error,
  claimed_by, claim_expires_at_ms, compensator_ref, result_json, ts_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
			if _, err := tx.ExecContext(ctx, insertQ,
				o.AppName, o.UserID, o.SessionID, existing.InvocationID,
				existing.IdempotencyKey, o.CompensateOnDuplicateKind,
				jsonOrNil(payload),
				ObligationStatusPending, 0, 5, now, nil,
				nil, 0, nil, nil, now); err != nil {
				return nil, err
			}
		}
	case EffectResolutionStuck:
		newStatus = EffectStatusFailed
		if o.ErrorJSON != nil {
			newError = o.ErrorJSON
		} else {
			newError = map[string]any{
				"resolution": "stuck",
				"detail":     "reconciler couldn't resolve",
			}
		}
	default:
		return nil, fmt.Errorf("RecordExternalObservation: unknown resolution %q", o.Resolution)
	}

	updateQ := s.rew(`UPDATE tape_effects
SET status = ?, external_ref = ?, response_json = ?, error_json = ?, ts_ms = ?
WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?`)
	if _, err := tx.ExecContext(ctx, updateQ,
		newStatus, ns(newExternalRef), jsonOrNil(newResponse), jsonOrNil(newError), now,
		o.AppName, o.UserID, o.SessionID, o.IdempotencyKey); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s.GetEffect(ctx, o.AppName, o.UserID, o.SessionID, o.IdempotencyKey)
}

// ── reconciler / outbox queues ────────────────────────────────────────────

// ListPendingEffectsOpts — cross-session reconciler feed selectors.
type ListPendingEffectsOpts struct {
	OlderThanMs    int64
	IncludePending bool
	IncludeUnknown bool
	Limit          int
}

// ListPendingEffects — the reconciler's hot set. Defaults
// (IncludePending=true, IncludeUnknown=true, Limit=200) match Python.
func (s *TapeSessionService) ListPendingEffects(ctx context.Context, o ListPendingEffectsOpts) ([]EffectRecord, error) {
	if o.Limit <= 0 {
		o.Limit = 200
	}
	statuses := []string{}
	if o.IncludePending {
		statuses = append(statuses, EffectStatusPending)
	}
	if o.IncludeUnknown {
		statuses = append(statuses, EffectStatusUnknown)
	}
	if len(statuses) == 0 {
		return nil, nil
	}

	var (
		args   []any
		filter string
	)
	switch {
	case o.OlderThanMs > 0 && o.IncludePending && !o.IncludeUnknown:
		// Only PENDING; filter by age.
		filter = `status = ? AND ts_ms < ?`
		args = append(args, EffectStatusPending, o.OlderThanMs)
	case o.OlderThanMs > 0 && o.IncludePending && o.IncludeUnknown:
		filter = `(status = ? OR (status = ? AND ts_ms < ?))`
		args = append(args, EffectStatusUnknown, EffectStatusPending, o.OlderThanMs)
	default:
		// Status list.
		placeholders := strings.Repeat("?,", len(statuses))
		placeholders = placeholders[:len(placeholders)-1]
		filter = "status IN (" + placeholders + ")"
		for _, st := range statuses {
			args = append(args, st)
		}
	}
	q := s.rew(`SELECT ` + effectCols + ` FROM tape_effects
WHERE ` + filter + `
ORDER BY ts_ms LIMIT ?`)
	args = append(args, o.Limit)
	return s.queryEffects(ctx, q, args...)
}

// ListEffectsToDispatchOpts — outbox dispatcher feed selectors.
type ListEffectsToDispatchOpts struct {
	NowMs     int64
	Connector string
	Limit     int
}

// ListEffectsToDispatch — PENDING+OUTBOX where next_dispatch_at_ms<=now
// AND (lease free or expired). Ordered by ts_ms ascending.
func (s *TapeSessionService) ListEffectsToDispatch(ctx context.Context, o ListEffectsToDispatchOpts) ([]EffectRecord, error) {
	if o.Limit <= 0 {
		o.Limit = 200
	}
	now := nowOr(o.NowMs)
	q := `SELECT ` + effectCols + ` FROM tape_effects
WHERE status = ? AND dispatch_mode = ? AND next_dispatch_at_ms <= ?
  AND (dispatch_claimed_by IS NULL OR dispatch_claimed_by = '' OR dispatch_claim_expires_at_ms <= ?)`
	args := []any{EffectStatusPending, EffectDispatchOutbox, now, now}
	if o.Connector != "" {
		q += ` AND connector = ?`
		args = append(args, o.Connector)
	}
	q += ` ORDER BY ts_ms LIMIT ?`
	args = append(args, o.Limit)
	return s.queryEffects(ctx, s.rew(q), args...)
}

// ── obligation ledger ─────────────────────────────────────────────────────

// RegisterCompensationOpts — keyword-args.
type RegisterCompensationOpts struct {
	AppName        string
	UserID         string
	SessionID      string
	InvocationID   string
	EffectKey      string
	Kind           string
	PayloadJSON    any
	CompensatorRef string
	MaxAttempts    int
}

// RegisterCompensation — idempotent on (session, effect_key, kind).
func (s *TapeSessionService) RegisterCompensation(ctx context.Context, o RegisterCompensationOpts) (ObligationRecord, error) {
	// Idempotent on (session, effect_key, kind).
	q := s.rew(`SELECT ` + obligationCols + ` FROM tape_obligations
WHERE app_name = ? AND user_id = ? AND session_id = ? AND effect_key = ? AND kind = ?`)
	row := s.db.QueryRowContext(ctx, q, o.AppName, o.UserID, o.SessionID, o.EffectKey, o.Kind)
	existing, err := scanObligation(row)
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ObligationRecord{}, err
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 5
	}
	now := nowMs()
	insertQ := s.rew(`INSERT INTO tape_obligations (
  app_name, user_id, session_id, invocation_id, effect_key, kind, payload_json,
  status, attempts, max_attempts, next_attempt_at_ms, last_error,
  claimed_by, claim_expires_at_ms, compensator_ref, result_json, ts_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if _, err := s.db.ExecContext(ctx, insertQ,
		o.AppName, o.UserID, o.SessionID, o.InvocationID, o.EffectKey, o.Kind,
		jsonOrNil(o.PayloadJSON),
		ObligationStatusPending, 0, o.MaxAttempts, now, nil,
		nil, 0, ns(o.CompensatorRef), nil, now); err != nil {
		return ObligationRecord{}, err
	}
	row = s.db.QueryRowContext(ctx, q, o.AppName, o.UserID, o.SessionID, o.EffectKey, o.Kind)
	return scanObligation(row)
}

// ListObligationsOpts — per-session list selectors.
type ListObligationsOpts struct {
	AppName        string
	UserID         string
	SessionID      string
	OnlyUnresolved bool
	StatusFilter   string
}

// ListObligations — per-session, LIFO (seq DESC). OnlyUnresolved=true
// excludes COMPENSATED+STUCK; StatusFilter overrides to exact match.
func (s *TapeSessionService) ListObligations(ctx context.Context, o ListObligationsOpts) ([]ObligationRecord, error) {
	args := []any{o.AppName, o.UserID, o.SessionID}
	q := `SELECT ` + obligationCols + ` FROM tape_obligations
WHERE app_name = ? AND user_id = ? AND session_id = ?`
	switch {
	case o.StatusFilter != "":
		q += ` AND status = ?`
		args = append(args, o.StatusFilter)
	case o.OnlyUnresolved:
		q += ` AND status IN (?, ?)`
		args = append(args, ObligationStatusPending, ObligationStatusCommitted)
	}
	q += ` ORDER BY seq DESC`
	return s.queryObligations(ctx, s.rew(q), args...)
}

// ListUnresolvedObligationsOpts — drainer feed selectors.
type ListUnresolvedObligationsOpts struct {
	NowMs                   int64
	Limit                   int
	IncludePending          bool
	IncludeStuck            bool
	IncludeCommittedExpired bool
}

// ListUnresolvedObligations — cross-session drainer feed. Defaults
// (IncludePending=true, IncludeCommittedExpired=true, Limit=500) match
// Python.
func (s *TapeSessionService) ListUnresolvedObligations(ctx context.Context, o ListUnresolvedObligationsOpts) ([]ObligationRecord, error) {
	if o.Limit <= 0 {
		o.Limit = 500
	}
	now := nowOr(o.NowMs)
	var conds []string
	var args []any
	if o.IncludePending {
		conds = append(conds, `(status = ? AND next_attempt_at_ms <= ?)`)
		args = append(args, ObligationStatusPending, now)
	}
	if o.IncludeCommittedExpired {
		conds = append(conds, `(status = ? AND claim_expires_at_ms <= ?)`)
		args = append(args, ObligationStatusCommitted, now)
	}
	if o.IncludeStuck {
		conds = append(conds, `(status = ?)`)
		args = append(args, ObligationStatusStuck)
	}
	if len(conds) == 0 {
		return nil, nil
	}
	q := `SELECT ` + obligationCols + ` FROM tape_obligations
WHERE ` + strings.Join(conds, " OR ") + `
ORDER BY seq DESC LIMIT ?`
	args = append(args, o.Limit)
	return s.queryObligations(ctx, s.rew(q), args...)
}

// ClaimObligation — atomic CAS — single winner under contention. Also
// reclaims COMMITTED rows whose claim_expires_at_ms <= now.
func (s *TapeSessionService) ClaimObligation(
	ctx context.Context, seq int64, claimer string, leaseTTLMs int64, nowOverrideMs int64,
) (bool, *ObligationRecord, error) {
	if leaseTTLMs <= 0 {
		leaseTTLMs = 60_000
	}
	now := nowOr(nowOverrideMs)
	expires := now + leaseTTLMs

	var acquired bool
	err := s.withCASLock(func() error {
		q := s.rew(`UPDATE tape_obligations
SET status = ?, claimed_by = ?, claim_expires_at_ms = ?
WHERE seq = ?
  AND ((status = ? AND next_attempt_at_ms <= ?) OR (status = ? AND claim_expires_at_ms <= ?))`)
		res, err := s.db.ExecContext(ctx, q,
			ObligationStatusCommitted, claimer, expires,
			seq,
			ObligationStatusPending, now, ObligationStatusCommitted, now)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		acquired = n == 1
		return nil
	})
	if err != nil {
		return false, nil, err
	}
	row, err := s.getObligationBySeq(ctx, seq)
	if err != nil {
		return acquired, nil, err
	}
	return acquired, row, nil
}

// RecordObligationAttempt — failed compensation. nextAttemptAtMs=0
// forces STUCK; else attempts++ and STUCK once >= max_attempts.
func (s *TapeSessionService) RecordObligationAttempt(
	ctx context.Context, seq int64, errMsg string, nextAttemptAtMs int64,
) (*ObligationRecord, error) {
	existing, err := s.getObligationBySeq(ctx, seq)
	if err != nil || existing == nil {
		return existing, err
	}
	attempts := existing.Attempts + 1
	status := ObligationStatusPending
	next := nextAttemptAtMs
	if nextAttemptAtMs <= 0 || attempts >= existing.MaxAttempts {
		status = ObligationStatusStuck
		next = 0
	}
	now := nowMs()
	q := s.rew(`UPDATE tape_obligations
SET attempts = ?, last_error = ?, claimed_by = NULL, claim_expires_at_ms = 0,
    status = ?, next_attempt_at_ms = ?, ts_ms = ?
WHERE seq = ?`)
	if _, err := s.db.ExecContext(ctx, q,
		attempts, jsonOrNil(errMsg), status, next, now, seq); err != nil {
		return nil, err
	}
	return s.getObligationBySeq(ctx, seq)
}

// ResolveObligation — terminal: COMPENSATED or STUCK.
func (s *TapeSessionService) ResolveObligation(
	ctx context.Context, seq int64, status string, resultJSON any,
) (*ObligationRecord, error) {
	if status != ObligationStatusCompensated && status != ObligationStatusStuck {
		return nil, fmt.Errorf(
			"ResolveObligation: status must be COMPENSATED or STUCK, got %q", status)
	}
	existing, err := s.getObligationBySeq(ctx, seq)
	if err != nil || existing == nil {
		return existing, err
	}
	now := nowMs()
	q := s.rew(`UPDATE tape_obligations
SET status = ?, result_json = ?, claimed_by = NULL, claim_expires_at_ms = 0, ts_ms = ?
WHERE seq = ?`)
	if _, err := s.db.ExecContext(ctx, q, status, jsonOrNil(resultJSON), now, seq); err != nil {
		return nil, err
	}
	return s.getObligationBySeq(ctx, seq)
}

func (s *TapeSessionService) getObligationBySeq(ctx context.Context, seq int64) (*ObligationRecord, error) {
	q := s.rew(`SELECT ` + obligationCols + ` FROM tape_obligations WHERE seq = ?`)
	row := s.db.QueryRowContext(ctx, q, seq)
	r, err := scanObligation(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// ── timers ────────────────────────────────────────────────────────────────

// SetTimerOpts — keyword-args.
type SetTimerOpts struct {
	AppName     string
	UserID      string
	SessionID   string
	TimerID     string
	FireAtMs    int64
	Kind        string
	PayloadJSON any
}

// SetTimer — idempotent on (session, timer_id) — repeat call returns
// the existing row, original fire_at_ms preserved.
func (s *TapeSessionService) SetTimer(ctx context.Context, o SetTimerOpts) (TimerRecord, error) {
	// Idempotent read first.
	q := s.rew(`SELECT ` + timerCols + ` FROM tape_timers
WHERE app_name = ? AND user_id = ? AND session_id = ? AND timer_id = ?`)
	row := s.db.QueryRowContext(ctx, q, o.AppName, o.UserID, o.SessionID, o.TimerID)
	existing, err := scanTimer(row)
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return TimerRecord{}, err
	}
	now := nowMs()
	insertQ := s.rew(`INSERT INTO tape_timers (
  app_name, user_id, session_id, timer_id, fire_at_ms, kind,
  payload_json, fired, created_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if _, err := s.db.ExecContext(ctx, insertQ,
		o.AppName, o.UserID, o.SessionID, o.TimerID,
		o.FireAtMs, o.Kind, jsonOrNil(o.PayloadJSON), false, now); err != nil {
		return TimerRecord{}, err
	}
	row = s.db.QueryRowContext(ctx, q, o.AppName, o.UserID, o.SessionID, o.TimerID)
	return scanTimer(row)
}

// ListDueTimersOpts — selector for due timer enumeration.
type ListDueTimersOpts struct {
	NowMs int64
	Limit int
	Claim bool
}

// ListDueTimers — timers with fire_at_ms <= now AND fired=false. With
// Claim=true, atomically marks them fired in the same txn.
func (s *TapeSessionService) ListDueTimers(ctx context.Context, o ListDueTimersOpts) ([]TimerRecord, error) {
	if o.Limit <= 0 {
		o.Limit = 200
	}
	now := nowOr(o.NowMs)

	if !o.Claim {
		q := s.rew(`SELECT ` + timerCols + ` FROM tape_timers
WHERE fired = ? AND fire_at_ms <= ?
ORDER BY fire_at_ms LIMIT ?`)
		return s.queryTimers(ctx, q, false, now, o.Limit)
	}

	// Claim path — fetch+mark in a single transaction.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	q := s.rew(`SELECT ` + timerCols + ` FROM tape_timers
WHERE fired = ? AND fire_at_ms <= ?
ORDER BY fire_at_ms LIMIT ?`)
	rows, err := tx.QueryContext(ctx, q, false, now, o.Limit)
	if err != nil {
		return nil, err
	}
	var out []TimerRecord
	for rows.Next() {
		t, err := scanTimer(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if len(out) > 0 {
		// Mark each fired.
		upd := s.rew(`UPDATE tape_timers SET fired = ?
WHERE app_name = ? AND user_id = ? AND session_id = ? AND timer_id = ?`)
		for _, t := range out {
			if _, err := tx.ExecContext(ctx, upd, true, t.AppName, t.UserID, t.SessionID, t.TimerID); err != nil {
				return nil, err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// CancelTimer — delete by composite key. Returns whether a row was removed.
func (s *TapeSessionService) CancelTimer(
	ctx context.Context, appName, userID, sessionID, timerID string,
) (bool, error) {
	q := s.rew(`DELETE FROM tape_timers
WHERE app_name = ? AND user_id = ? AND session_id = ? AND timer_id = ?`)
	res, err := s.db.ExecContext(ctx, q, appName, userID, sessionID, timerID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// ── reactive KV ───────────────────────────────────────────────────────────

// WriteValue — optimistic CAS write. ifVersion<0 disables CAS (last
// writer wins). ifVersion==current advances; mismatch returns an error
// matching Python's "stale CAS" text so tests can assert on it.
func (s *TapeSessionService) WriteValue(
	ctx context.Context, namespace, key string, valueJSON any, ifVersion int, writer string,
) (ValueRecord, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ValueRecord{}, err
	}
	defer func() { _ = tx.Rollback() }()

	q := s.rew(`SELECT namespace, key, value_json, version, ts_ms, writer, deleted
FROM tape_values WHERE namespace = ? AND key = ?`)
	row := tx.QueryRowContext(ctx, q, namespace, key)
	existing, err := scanValue(row)
	notFound := errors.Is(err, sql.ErrNoRows)
	if err != nil && !notFound {
		return ValueRecord{}, err
	}
	now := nowMs()
	if notFound {
		if ifVersion >= 0 && ifVersion != 0 {
			return ValueRecord{}, fmt.Errorf(
				"WriteValue: if_version=%d but no prior row exists (version 0)", ifVersion)
		}
		ins := s.rew(`INSERT INTO tape_values (namespace, key, value_json, version, ts_ms, writer, deleted)
VALUES (?, ?, ?, ?, ?, ?, ?)`)
		if _, err := tx.ExecContext(ctx, ins,
			namespace, key, jsonOrNil(valueJSON), 1, now, ns(writer), false); err != nil {
			return ValueRecord{}, err
		}
		if err := tx.Commit(); err != nil {
			return ValueRecord{}, err
		}
		return ValueRecord{
			Namespace: namespace, Key: key, ValueJSON: valueJSON,
			Version: 1, TsMs: now, Writer: writer, Deleted: false,
		}, nil
	}

	if ifVersion >= 0 && ifVersion != existing.Version {
		return ValueRecord{}, fmt.Errorf(
			"WriteValue: stale CAS — if_version=%d, current=%d", ifVersion, existing.Version)
	}
	newVersion := existing.Version + 1
	newWriter := writer
	if newWriter == "" {
		newWriter = existing.Writer
	}
	upd := s.rew(`UPDATE tape_values
SET value_json = ?, version = ?, ts_ms = ?, writer = ?, deleted = ?
WHERE namespace = ? AND key = ?`)
	if _, err := tx.ExecContext(ctx, upd,
		jsonOrNil(valueJSON), newVersion, now, ns(newWriter), false,
		namespace, key); err != nil {
		return ValueRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return ValueRecord{}, err
	}
	return ValueRecord{
		Namespace: namespace, Key: key, ValueJSON: valueJSON,
		Version: newVersion, TsMs: now, Writer: newWriter, Deleted: false,
	}, nil
}

// ── continue-as-new (mechanism #4 in the compaction roadmap) ────────────

// ContinueAsNewOpts — options for `ContinueAsNew`.
type ContinueAsNewOpts struct {
	AppName         string
	UserID          string
	SessionID       string
	OldInvocationID string
	NewInvocationID string
	CarriedState    any  // when nil, no `tape_values` write happens
	HasCarriedState bool // discriminator for "I want to write a nil/0 state" vs "skip"
	PruneOld        bool // default true via NewContinueAsNewOpts
	prunePolicySet  bool
}

// NewContinueAsNewOpts — builds an opts value with PruneOld defaulted to
// true (matching the Python signature default).
func NewContinueAsNewOpts(appName, userID, sessionID, oldInv, newInv string) ContinueAsNewOpts {
	return ContinueAsNewOpts{
		AppName:         appName,
		UserID:          userID,
		SessionID:       sessionID,
		OldInvocationID: oldInv,
		NewInvocationID: newInv,
		PruneOld:        true,
		prunePolicySet:  true,
	}
}

// ContinueAsNewResult — what one `ContinueAsNew` call did. Mirrors the
// Python dict return type so callers can audit-log it.
type ContinueAsNewResult struct {
	EffectsPruned   int
	ObligationsKept int
	StateWritten    bool
}

// ContinueAsNew — end one invocation chapter, start a new one in the
// same session, with optional state carried forward.
//
// Atomic: one SQL transaction commits the prune + the carried-state
// write together. Temporal's `continue-as-new` mapped onto the embedded
// model: there's no separate "run" lifecycle to close (the session is
// the long-lived unit), only an `invocation_id` to retire. The caller
// continues issuing RPCs under `NewInvocationID`.
//
// `PruneOld=true` (default, via `NewContinueAsNewOpts`): delete the old
// invocation's terminal effects that aren't pinned by an active
// obligation. Same NOT EXISTS guard as the compactor — the pinning
// mechanism doesn't get a special case here. Effects in non-terminal
// states under the old invocation are kept (a still-PENDING effect
// under a retired invocation is a bug elsewhere; surface it, don't
// silently delete it).
//
// `CarriedState`, when set via `HasCarriedState=true` or via
// `o.CarriedState != nil` shorthand, is written to a `tape_values` row
// at `namespace='tape:continue-as-new:<SessionID>'`,
// `key=<NewInvocationID>` — a small protocol the new invocation can
// read on startup to pick up where the old one left off.
func (s *TapeSessionService) ContinueAsNew(ctx context.Context, o ContinueAsNewOpts) (ContinueAsNewResult, error) {
	// Default PruneOld to true when the caller used a literal
	// `ContinueAsNewOpts{}` (so the zero-value of bool, false, is
	// indistinguishable from "they didn't set it").
	if !o.prunePolicySet {
		o.PruneOld = true
	}
	hasState := o.HasCarriedState || o.CarriedState != nil

	var result ContinueAsNewResult
	now := nowMs()
	ns := fmt.Sprintf("tape:continue-as-new:%s", o.SessionID)

	err := s.withCASLock(func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		if o.PruneOld {
			// Prune old-invocation terminal effects, pinning-respecting.
			// The NOT EXISTS predicate is identical to the compactor's
			// — the pinning mechanism lives in the WHERE clause.
			pruneQ := s.rew(`DELETE FROM tape_effects
WHERE app_name = ? AND user_id = ? AND session_id = ?
  AND invocation_id = ?
  AND status IN (?, ?)
  AND NOT EXISTS (
    SELECT 1 FROM tape_obligations o
     WHERE o.session_id = tape_effects.session_id
       AND o.effect_key = tape_effects.idempotency_key
       AND o.status IN (?, ?)
  )`)
			r, err := tx.ExecContext(ctx, pruneQ,
				o.AppName, o.UserID, o.SessionID,
				o.OldInvocationID,
				EffectStatusConfirmed, EffectStatusFailed,
				ObligationStatusPending, ObligationStatusCommitted)
			if err != nil {
				return fmt.Errorf("ContinueAsNew: prune: %w", err)
			}
			n, _ := r.RowsAffected()
			result.EffectsPruned = int(n)

			// Surface any obligations that still pin OLD-invocation
			// effects — they're the reason this continue_as_new didn't
			// fully reset the slate. The pinning relationship is via
			// `effect_key` → `idempotency_key`, NOT the obligation's own
			// invocation_id; an obligation registered in a later
			// invocation can still pin an earlier invocation's row.
			keptQ := s.rew(`SELECT COUNT(*) FROM tape_obligations
WHERE app_name = ? AND user_id = ? AND session_id = ?
  AND status IN (?, ?)
  AND effect_key IN (
    SELECT idempotency_key FROM tape_effects
     WHERE app_name = ? AND user_id = ? AND session_id = ?
       AND invocation_id = ?
  )`)
			var kept int
			if err := tx.QueryRowContext(ctx, keptQ,
				o.AppName, o.UserID, o.SessionID,
				ObligationStatusPending, ObligationStatusCommitted,
				o.AppName, o.UserID, o.SessionID, o.OldInvocationID,
			).Scan(&kept); err != nil {
				return fmt.Errorf("ContinueAsNew: count-kept: %w", err)
			}
			result.ObligationsKept = kept
		}

		if hasState {
			// Carry state forward as a tape_value row. Idempotent on
			// (namespace, key): a repeat call updates the existing row
			// (version++), so callers driving continue_as_new twice with
			// the same new invocation id get last-writer-wins semantics.
			selQ := s.rew(`SELECT namespace, key, value_json, version, ts_ms, writer, deleted
FROM tape_values WHERE namespace = ? AND key = ?`)
			row := tx.QueryRowContext(ctx, selQ, ns, o.NewInvocationID)
			existing, err := scanValue(row)
			notFound := errors.Is(err, sql.ErrNoRows)
			if err != nil && !notFound {
				return fmt.Errorf("ContinueAsNew: select-value: %w", err)
			}
			if notFound {
				ins := s.rew(`INSERT INTO tape_values
(namespace, key, value_json, version, ts_ms, writer, deleted)
VALUES (?, ?, ?, ?, ?, ?, ?)`)
				if _, err := tx.ExecContext(ctx, ins,
					ns, o.NewInvocationID, jsonOrNil(o.CarriedState),
					1, now, "continue_as_new", false); err != nil {
					return fmt.Errorf("ContinueAsNew: insert-value: %w", err)
				}
			} else {
				upd := s.rew(`UPDATE tape_values
SET value_json = ?, version = ?, ts_ms = ?, writer = ?, deleted = ?
WHERE namespace = ? AND key = ?`)
				if _, err := tx.ExecContext(ctx, upd,
					jsonOrNil(o.CarriedState), existing.Version+1, now,
					"continue_as_new", false,
					ns, o.NewInvocationID); err != nil {
					return fmt.Errorf("ContinueAsNew: update-value: %w", err)
				}
			}
			result.StateWritten = true
		}
		return tx.Commit()
	})
	if err != nil {
		return ContinueAsNewResult{}, err
	}
	return result, nil
}

// GetValue — single-row read.
func (s *TapeSessionService) GetValue(ctx context.Context, namespace, key string) (*ValueRecord, error) {
	q := s.rew(`SELECT namespace, key, value_json, version, ts_ms, writer, deleted
FROM tape_values WHERE namespace = ? AND key = ?`)
	row := s.db.QueryRowContext(ctx, q, namespace, key)
	v, err := scanValue(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &v, nil
}

// ── scan helpers ─────────────────────────────────────────────────────────

// effectCols / obligationCols / timerCols — kept in one place so
// projections + scan functions agree.
const effectCols = `app_name, user_id, session_id, idempotency_key, invocation_id,
  decision_index, tool_name, call_index, status, semantics, dispatch_mode,
  business_key, connector, external_ref,
  dispatch_attempts, next_dispatch_at_ms, dispatch_claimed_by, dispatch_claim_expires_at_ms,
  last_dispatch_error, request_json, response_json, error_json, ts_ms`

const obligationCols = `seq, app_name, user_id, session_id, invocation_id, effect_key,
  kind, payload_json, status, attempts, max_attempts, next_attempt_at_ms,
  last_error, claimed_by, claim_expires_at_ms, compensator_ref, result_json, ts_ms`

const timerCols = `app_name, user_id, session_id, timer_id, fire_at_ms, kind,
  payload_json, fired, created_at_ms`

// scanner — common interface both *sql.Row and *sql.Rows satisfy.
type scanner interface {
	Scan(dest ...any) error
}

func scanEffect(sc scanner) (EffectRecord, error) {
	var r EffectRecord
	var (
		businessKey, connector, externalRef, dispatchClaimedBy sql.NullString
		lastDispatchError, requestJSON, responseJSON, errorJSON sql.NullString
	)
	if err := sc.Scan(
		&r.AppName, &r.UserID, &r.SessionID, &r.IdempotencyKey, &r.InvocationID,
		&r.DecisionIndex, &r.ToolName, &r.CallIndex, &r.Status, &r.Semantics, &r.DispatchMode,
		&businessKey, &connector, &externalRef,
		&r.DispatchAttempts, &r.NextDispatchAtMs, &dispatchClaimedBy, &r.DispatchClaimExpiresAtMs,
		&lastDispatchError, &requestJSON, &responseJSON, &errorJSON, &r.TsMs,
	); err != nil {
		return EffectRecord{}, err
	}
	r.BusinessKey = scanString(businessKey)
	r.Connector = scanString(connector)
	r.ExternalRef = scanString(externalRef)
	r.DispatchClaimedBy = scanString(dispatchClaimedBy)
	r.LastDispatchError = decodeJSON(scanString(lastDispatchError))
	r.RequestJSON = decodeJSON(scanString(requestJSON))
	r.ResponseJSON = decodeJSON(scanString(responseJSON))
	r.ErrorJSON = decodeJSON(scanString(errorJSON))
	return r, nil
}

func (s *TapeSessionService) queryEffects(ctx context.Context, q string, args ...any) ([]EffectRecord, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EffectRecord
	for rows.Next() {
		r, err := scanEffect(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func scanObligation(sc scanner) (ObligationRecord, error) {
	var r ObligationRecord
	var (
		payloadJSON, lastError, claimedBy, compensatorRef, resultJSON sql.NullString
	)
	if err := sc.Scan(
		&r.Seq, &r.AppName, &r.UserID, &r.SessionID, &r.InvocationID, &r.EffectKey,
		&r.Kind, &payloadJSON, &r.Status, &r.Attempts, &r.MaxAttempts, &r.NextAttemptAtMs,
		&lastError, &claimedBy, &r.ClaimExpiresAtMs, &compensatorRef, &resultJSON, &r.TsMs,
	); err != nil {
		return ObligationRecord{}, err
	}
	r.PayloadJSON = decodeJSON(scanString(payloadJSON))
	r.LastError = decodeJSON(scanString(lastError))
	r.ClaimedBy = scanString(claimedBy)
	r.CompensatorRef = scanString(compensatorRef)
	r.ResultJSON = decodeJSON(scanString(resultJSON))
	return r, nil
}

func (s *TapeSessionService) queryObligations(ctx context.Context, q string, args ...any) ([]ObligationRecord, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ObligationRecord
	for rows.Next() {
		r, err := scanObligation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func scanTimer(sc scanner) (TimerRecord, error) {
	var t TimerRecord
	var (
		payloadJSON sql.NullString
		firedAny    any
	)
	if err := sc.Scan(
		&t.AppName, &t.UserID, &t.SessionID, &t.TimerID,
		&t.FireAtMs, &t.Kind, &payloadJSON, &firedAny, &t.CreatedAtMs,
	); err != nil {
		return TimerRecord{}, err
	}
	t.PayloadJSON = decodeJSON(scanString(payloadJSON))
	t.Fired = anyToBool(firedAny)
	return t, nil
}

func (s *TapeSessionService) queryTimers(ctx context.Context, q string, args ...any) ([]TimerRecord, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TimerRecord
	for rows.Next() {
		t, err := scanTimer(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func scanValue(sc scanner) (ValueRecord, error) {
	var v ValueRecord
	var (
		valueJSON, writer sql.NullString
		deletedAny        any
	)
	if err := sc.Scan(
		&v.Namespace, &v.Key, &valueJSON, &v.Version, &v.TsMs, &writer, &deletedAny,
	); err != nil {
		return ValueRecord{}, err
	}
	v.ValueJSON = decodeJSON(scanString(valueJSON))
	v.Writer = scanString(writer)
	v.Deleted = anyToBool(deletedAny)
	return v, nil
}

// anyToBool — coerce SQLite's int and Postgres's bool into a Go bool.
func anyToBool(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case int64:
		return x != 0
	case int:
		return x != 0
	case []byte:
		return len(x) > 0 && x[0] != '0' && x[0] != 'f' && x[0] != 'F'
	case string:
		return x == "1" || x == "true" || x == "t" || x == "TRUE"
	}
	return false
}

// firstNonEmpty — pick the first non-empty string.
func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// isUniqueViolation — driver-specific error matching. SQLite reports
// "UNIQUE constraint failed"; Postgres exposes a 23505 SQLSTATE on a
// `pq.Error` / `*pgconn.PgError`. We match on the universal message
// fragments to stay driver-agnostic.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") ||
		strings.Contains(msg, "duplicate key value violates unique constraint") ||
		strings.Contains(strings.ToLower(msg), "constraint failed") && strings.Contains(strings.ToLower(msg), "unique")
}
