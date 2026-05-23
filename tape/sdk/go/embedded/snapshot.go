package embedded

// snapshot.go — primitive #3 in the compaction roadmap: the durable
// short-circuit that survives compaction.
//
// The compactor (`compact.go`) prunes terminal effect rows past their
// TTL. But the idempotency-key short-circuit in `BeginEffect` reads
// `tape_effects` — once pruned, a `BeginEffect` call with the same
// derived key would create a fresh PENDING row and re-dispatch the
// work. Double-spend.
//
// The snapshot row is the durable short-circuit that survives that
// pruning:
//
//   * One row per session in `tape_effect_snapshots`, cumulative JSON
//     map keyed by idempotency_key with the minimum data the
//     short-circuit needs (status, response, semantics, business_key,
//     connector, external_ref, ts_ms).
//
//   * `TakeSnapshot` captures terminal effects up to a watermark and
//     MERGES with the existing snapshot (last-write-wins per key). The
//     whole thing runs in one transaction.
//
//   * `BeginEffect` consults the snapshot AFTER missing the live row;
//     if found, returns a synthetic `EffectRecord` reconstructed from
//     the captured fields. No row is created — the snapshot IS the
//     durable record.
//
// Operator recipe: "snapshot, then prune."
//
// Schema parity: the column names mirror `tape_adk.schemas.StorageEffectSnapshot`
// byte-for-byte, and `effects_json` is a JSON-encoded TEXT/JSONB blob —
// so a SQLite file written by the Python SDK is readable here and vice
// versa.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// EffectSnapshot — the per-session snapshot row, JSON column decoded.
// `EffectsJSON` is a `map[string]map[string]any` (idempotency_key →
// captured-data dict). Use `GetSnapshot` to read.
type EffectSnapshot struct {
	AppName      string
	UserID       string
	SessionID    string
	EffectsJSON  map[string]map[string]any
	UpToTsMs     int64
	EffectsCount int
	CreatedAtMs  int64
	UpdatedAtMs  int64
}

// TakeSnapshotOpts — keyword-args for `TakeSnapshot`.
type TakeSnapshotOpts struct {
	AppName   string
	UserID    string
	SessionID string
	// UpToTsMs — bound the read window. 0 means "use wall-clock now"
	// (capture everything terminal). Pass a fixed value to make the
	// snapshot bounded for very large sessions.
	UpToTsMs int64
}

// TakeSnapshotResult — what one `TakeSnapshot` call did, mirroring the
// Python dict return type (`{captured, merged_total, up_to_ts_ms}`).
type TakeSnapshotResult struct {
	// Captured — count of distinct idempotency_keys read from the
	// effects table this call (i.e., the size of the delta merged in).
	Captured int
	// MergedTotal — count of distinct idempotency_keys in the snapshot
	// AFTER the merge (i.e., the row's `effects_count`).
	MergedTotal int
	// UpToTsMs — the watermark used for this snapshot (wall-clock when
	// `UpToTsMs=0` was passed, else the caller's value).
	UpToTsMs int64
}

// GetSnapshotOpts — keyword-args for `GetSnapshot`.
type GetSnapshotOpts struct {
	AppName   string
	UserID    string
	SessionID string
}

// TakeSnapshot — capture terminal effects under this session into the
// per-session snapshot row. Merges with the existing snapshot — repeat
// calls are cumulative, last-write-wins per idempotency_key.
//
// After a snapshot, the compactor can safely prune the underlying
// terminal effect rows: `BeginEffect` falls back to the snapshot's JSON
// map for the idempotency-key short-circuit, so re-dispatch is
// prevented even when the source row is gone.
//
// One SQL transaction: read terminal effects, read existing snapshot,
// merge, UPSERT the snapshot row. Under the same CAS lock all other
// mutating methods use on SQLite.
func (s *TapeSessionService) TakeSnapshot(ctx context.Context, o TakeSnapshotOpts) (TakeSnapshotResult, error) {
	watermark := nowOr(o.UpToTsMs)
	var result TakeSnapshotResult
	result.UpToTsMs = watermark

	err := s.withCASLock(func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		// 1) Read all terminal effects under this session up to the
		//    watermark.
		captured, err := s.collectTerminalForSnapshot(ctx, tx, o, watermark)
		if err != nil {
			return err
		}
		result.Captured = len(captured)

		// 2) Read existing snapshot, if any.
		existing, existingMap, existingWatermark, err := s.readSnapshotRowTx(
			ctx, tx, o.AppName, o.UserID, o.SessionID)
		if err != nil {
			return err
		}

		// 3) Set-union, last-write-wins on collision.
		merged := make(map[string]map[string]any, len(existingMap)+len(captured))
		for k, v := range existingMap {
			merged[k] = v
		}
		for k, v := range captured {
			merged[k] = v
		}

		blob, err := json.Marshal(merged)
		if err != nil {
			return fmt.Errorf("TakeSnapshot: marshal: %w", err)
		}
		now := nowMs()

		if !existing {
			ins := s.rew(`INSERT INTO tape_effect_snapshots (
  app_name, user_id, session_id,
  effects_json, up_to_ts_ms, effects_count,
  created_at_ms, updated_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
			if _, err := tx.ExecContext(ctx, ins,
				o.AppName, o.UserID, o.SessionID,
				string(blob), watermark, len(merged),
				now, now,
			); err != nil {
				return fmt.Errorf("TakeSnapshot: insert: %w", err)
			}
		} else {
			// Preserve created_at_ms (not updated); advance up_to_ts_ms
			// monotonically (Python: `max(snap.up_to_ts_ms or 0, watermark)`).
			newWatermark := watermark
			if existingWatermark > newWatermark {
				newWatermark = existingWatermark
			}
			upd := s.rew(`UPDATE tape_effect_snapshots
SET effects_json = ?, up_to_ts_ms = ?, effects_count = ?, updated_at_ms = ?
WHERE app_name = ? AND user_id = ? AND session_id = ?`)
			if _, err := tx.ExecContext(ctx, upd,
				string(blob), newWatermark, len(merged), now,
				o.AppName, o.UserID, o.SessionID,
			); err != nil {
				return fmt.Errorf("TakeSnapshot: update: %w", err)
			}
		}

		result.MergedTotal = len(merged)
		return tx.Commit()
	})
	if err != nil {
		return TakeSnapshotResult{}, err
	}
	return result, nil
}

// GetSnapshot — read the snapshot row for a session. Returns nil if no
// snapshot has ever been taken (matches Python's `None`).
func (s *TapeSessionService) GetSnapshot(ctx context.Context, o GetSnapshotOpts) (*EffectSnapshot, error) {
	q := s.rew(`SELECT app_name, user_id, session_id, effects_json,
  up_to_ts_ms, effects_count, created_at_ms, updated_at_ms
FROM tape_effect_snapshots
WHERE app_name = ? AND user_id = ? AND session_id = ?`)
	row := s.db.QueryRowContext(ctx, q, o.AppName, o.UserID, o.SessionID)
	r, err := scanSnapshot(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// ── internals ─────────────────────────────────────────────────────────────

// collectTerminalForSnapshot — read all CONFIRMED/FAILED effects under
// `o`'s session with `ts_ms <= watermark`. Returns a map matching the
// Python `captured` shape, with the minimum data the short-circuit
// needs. Read inside the supplied transaction so we see a consistent
// view of the effect table for the whole `TakeSnapshot` call.
func (s *TapeSessionService) collectTerminalForSnapshot(
	ctx context.Context, tx *sql.Tx, o TakeSnapshotOpts, watermark int64,
) (map[string]map[string]any, error) {
	q := s.rew(`SELECT ` + effectCols + ` FROM tape_effects
WHERE app_name = ? AND user_id = ? AND session_id = ?
  AND status IN (?, ?)
  AND ts_ms <= ?`)
	rows, err := tx.QueryContext(ctx, q,
		o.AppName, o.UserID, o.SessionID,
		EffectStatusConfirmed, EffectStatusFailed,
		watermark,
	)
	if err != nil {
		return nil, fmt.Errorf("TakeSnapshot: select terminal: %w", err)
	}
	defer rows.Close()
	out := make(map[string]map[string]any)
	for rows.Next() {
		r, err := scanEffect(rows)
		if err != nil {
			return nil, err
		}
		out[r.IdempotencyKey] = effectToCaptured(r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// effectToCaptured — the per-effect blob the snapshot stores. Keep it
// minimal — the snapshot row is one JSON column and we don't want it to
// balloon. Shape matches Python's `take_snapshot` capture dict exactly.
func effectToCaptured(r EffectRecord) map[string]any {
	return map[string]any{
		"status":         r.Status,
		"semantics":      r.Semantics,
		"dispatch_mode":  r.DispatchMode,
		"business_key":   r.BusinessKey,
		"connector":      r.Connector,
		"external_ref":   r.ExternalRef,
		"request_json":   r.RequestJSON,
		"response_json":  r.ResponseJSON,
		"error_json":     r.ErrorJSON,
		"invocation_id":  r.InvocationID,
		"decision_index": r.DecisionIndex,
		"tool_name":      r.ToolName,
		"call_index":     r.CallIndex,
		"ts_ms":          r.TsMs,
	}
}

// readSnapshotRowTx — load the snapshot row inside the supplied
// transaction. Returns (exists, effects-map, up_to_ts_ms, err). When
// exists=false the other return values are zero. The watermark return
// is used by `TakeSnapshot` to advance `up_to_ts_ms` monotonically.
func (s *TapeSessionService) readSnapshotRowTx(
	ctx context.Context, tx *sql.Tx, appName, userID, sessionID string,
) (bool, map[string]map[string]any, int64, error) {
	q := s.rew(`SELECT effects_json, up_to_ts_ms FROM tape_effect_snapshots
WHERE app_name = ? AND user_id = ? AND session_id = ?`)
	row := tx.QueryRowContext(ctx, q, appName, userID, sessionID)
	var (
		blob      sql.NullString
		watermark int64
	)
	if err := row.Scan(&blob, &watermark); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil, 0, nil
		}
		return false, nil, 0, err
	}
	out := map[string]map[string]any{}
	if blob.Valid && blob.String != "" {
		if err := unmarshalSnapshotBlob(blob.String, &out); err != nil {
			return false, nil, 0, fmt.Errorf("readSnapshotRowTx: %w", err)
		}
	}
	return true, out, watermark, nil
}

// lookupSnapshotEntry — read the snapshot's captured entry for the
// given idempotency_key. Returns (record, found, err). When `found` is
// true, the returned `EffectRecord` is synthetic — it has no underlying
// row in `tape_effects`. Used by `BeginEffect` as the second-level
// short-circuit when the live row has been pruned.
func (s *TapeSessionService) lookupSnapshotEntry(
	ctx context.Context, appName, userID, sessionID, key string,
	toolNameFallback string, callIndexFallback int,
	semanticsFallback, dispatchModeFallback string,
) (EffectRecord, bool, error) {
	q := s.rew(`SELECT effects_json FROM tape_effect_snapshots
WHERE app_name = ? AND user_id = ? AND session_id = ?`)
	row := s.db.QueryRowContext(ctx, q, appName, userID, sessionID)
	var blob sql.NullString
	if err := row.Scan(&blob); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return EffectRecord{}, false, nil
		}
		return EffectRecord{}, false, err
	}
	if !blob.Valid || blob.String == "" {
		return EffectRecord{}, false, nil
	}
	m := map[string]map[string]any{}
	if err := unmarshalSnapshotBlob(blob.String, &m); err != nil {
		return EffectRecord{}, false, fmt.Errorf("snapshot fallback: %w", err)
	}
	captured, ok := m[key]
	if !ok {
		return EffectRecord{}, false, nil
	}
	rec := EffectRecord{
		AppName:        appName,
		UserID:         userID,
		SessionID:      sessionID,
		IdempotencyKey: key,
		// Default the status to CONFIRMED to match the Python fallback's
		// behaviour when the snapshot blob is missing the field.
		Status:       EffectStatusConfirmed,
		ToolName:     toolNameFallback,
		CallIndex:    callIndexFallback,
		Semantics:    semanticsFallback,
		DispatchMode: dispatchModeFallback,
	}
	if v, ok := captured["status"].(string); ok && v != "" {
		rec.Status = v
	}
	if v, ok := captured["semantics"].(string); ok && v != "" {
		rec.Semantics = v
	}
	if v, ok := captured["dispatch_mode"].(string); ok && v != "" {
		rec.DispatchMode = v
	}
	if v, ok := captured["business_key"].(string); ok {
		rec.BusinessKey = v
	}
	if v, ok := captured["connector"].(string); ok {
		rec.Connector = v
	}
	if v, ok := captured["external_ref"].(string); ok {
		rec.ExternalRef = v
	}
	if v, ok := captured["invocation_id"].(string); ok {
		rec.InvocationID = v
	}
	if v, ok := captured["tool_name"].(string); ok && v != "" {
		rec.ToolName = v
	}
	rec.DecisionIndex = anyToInt(captured["decision_index"], -1)
	rec.CallIndex = anyToInt(captured["call_index"], callIndexFallback)
	rec.TsMs = anyToInt64(captured["ts_ms"], 0)
	rec.RequestJSON = captured["request_json"]
	rec.ResponseJSON = captured["response_json"]
	rec.ErrorJSON = captured["error_json"]
	return rec, true, nil
}

// unmarshalSnapshotBlob — decode the JSON blob into the
// map-of-map shape. JSON encodes the inner maps as `map[string]any`,
// not `map[string]map[string]any`, so we unmarshal into the loose form
// and copy keys over.
func unmarshalSnapshotBlob(s string, dst *map[string]map[string]any) error {
	loose := map[string]any{}
	if err := json.Unmarshal([]byte(s), &loose); err != nil {
		return err
	}
	out := map[string]map[string]any{}
	for k, v := range loose {
		m, ok := v.(map[string]any)
		if !ok {
			continue
		}
		out[k] = m
	}
	*dst = out
	return nil
}

// anyToInt / anyToInt64 — JSON numbers decode as float64 in Go; coerce.
func anyToInt(v any, fallback int) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case int64:
		return int(x)
	}
	return fallback
}

func anyToInt64(v any, fallback int64) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int:
		return int64(x)
	case int64:
		return x
	}
	return fallback
}

// scanSnapshot — row → EffectSnapshot.
func scanSnapshot(sc scanner) (EffectSnapshot, error) {
	var (
		r    EffectSnapshot
		blob sql.NullString
	)
	if err := sc.Scan(
		&r.AppName, &r.UserID, &r.SessionID, &blob,
		&r.UpToTsMs, &r.EffectsCount, &r.CreatedAtMs, &r.UpdatedAtMs,
	); err != nil {
		return EffectSnapshot{}, err
	}
	r.EffectsJSON = map[string]map[string]any{}
	if blob.Valid && blob.String != "" {
		if err := unmarshalSnapshotBlob(blob.String, &r.EffectsJSON); err != nil {
			return EffectSnapshot{}, err
		}
	}
	return r, nil
}
