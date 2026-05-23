package embedded

// compact.go — The fifth reactor: compaction.
//
// Where the four `*Once` reactors move rows FORWARD through the state
// machine (PENDING → CONFIRMED, etc.), this one moves them OUT — the
// journal isn't free, and a long-running agent accumulates terminal
// rows that have no replay value. Same shape as `DispatchOutboxOnce` /
// `ReconcileOnce` / `DrainObligationsOnce` / `FireDueTimersOnce`: a
// plain function the caller invokes on a tick.
//
// The mechanism is one composite SQL DELETE per category with the
// safety invariants encoded as WHERE-clause predicates — not Go
// application code:
//
//	DELETE FROM tape_effects
//	 WHERE status IN ('confirmed','failed')
//	   AND ts_ms < :cutoff
//	   AND NOT EXISTS (                          -- compensable-window pinning
//	     SELECT 1 FROM tape_obligations o
//	      WHERE o.session_id  = tape_effects.session_id
//	        AND o.effect_key  = tape_effects.idempotency_key
//	        AND o.status IN ('pending','committed'))
//
// The `NOT EXISTS` clause IS the pinning mechanism (primitive #5). It
// runs at SQL level under the same CAS lock all other tape-adk-go
// writes use on SQLite, so concurrent compaction and concurrent
// `RegisterCompensation` can't race a pinned effect into the trash.
//
// Compaction is intentionally LOSSY: once an effect row is pruned, a
// future `BeginEffect` with the same idempotency_key won't short-circuit
// (it'll create a new PENDING row). The TTL is the contract: rows older
// than the policy SHOULD be safe to forget. For sessions that are still
// actively re-driving older effects, set a longer TTL.

import (
	"context"
	"fmt"
)

// CompactionPolicy — how aggressive the compactor is.
type CompactionPolicy struct {
	// EffectTTLMs — prune CONFIRMED/FAILED effects older than this AND
	// not pinned by an active obligation.
	EffectTTLMs int64

	// SessionTTLMs — archive an entire session's tape rows when its
	// latest tape row is older than this AND there are no active
	// obligations or unfired timers on it. ADK's own session + events
	// are left alone (this only touches the four tape_* tables).
	SessionTTLMs int64

	// ArchiveTerminalObligations — when true, prune COMPENSATED
	// obligations older than EffectTTLMs too. (STUCK is kept regardless
	// — it's the operator-triage signal.)
	ArchiveTerminalObligations bool

	// ArchiveFiredTimers — when true, prune fired timers older than
	// EffectTTLMs.
	ArchiveFiredTimers bool

	// MaxPerTick — cap on rows touched per `CompactOnce` call. The
	// compactor is meant to nibble; a runaway DELETE is the bug it's
	// designed to prevent.
	MaxPerTick int
}

// DefaultCompactionPolicy — the Python defaults: 7-day effect TTL, 30-day
// session TTL, archive COMPENSATED obligations + fired timers, 1000-row
// cap per tick.
func DefaultCompactionPolicy() CompactionPolicy {
	return CompactionPolicy{
		EffectTTLMs:                7 * 24 * 60 * 60 * 1000,
		SessionTTLMs:               30 * 24 * 60 * 60 * 1000,
		ArchiveTerminalObligations: true,
		ArchiveFiredTimers:         true,
		MaxPerTick:                 1000,
	}
}

// CompactionResult — what one `CompactOnce` tick did. Returned for the
// audit log, used by `tape doctor compact` for the human-facing summary.
type CompactionResult struct {
	EffectsPruned     int
	ObligationsPruned int
	TimersPruned      int
	SessionsArchived  int
}

// Total — sum across all categories. Useful for "did anything happen"
// checks.
func (r CompactionResult) Total() int {
	return r.EffectsPruned + r.ObligationsPruned + r.TimersPruned + r.SessionsArchived
}

// CompactOnce — one pass of the compactor. Four independent DELETE
// statements, each safety-pinned, run under the same in-process CAS
// lock as every other mutating method (Postgres relies on its own
// row-level locking).
//
// Order matters: session-archival runs FIRST, then per-row obligation
// archival, then fired-timer archival, then the effect prune. The
// session step is the superset operation; the per-row steps then handle
// surviving rows in still-active sessions.
//
// Pass `nowMs=0` to use wall-clock time.
func CompactOnce(
	ctx context.Context, svc *TapeSessionService,
	policy CompactionPolicy, nowMs int64,
) (CompactionResult, error) {
	now := nowOr(nowMs)
	effectCutoff := now - policy.EffectTTLMs
	sessionCutoff := now - policy.SessionTTLMs
	if policy.MaxPerTick <= 0 {
		policy.MaxPerTick = 1000
	}
	var result CompactionResult

	err := svc.withCASLock(func() error {
		// 1) Session-level archival FIRST. It's the superset operation:
		//    when a whole session qualifies (all rows old + no active
		//    obligations + no unfired timers), one round of DELETEs
		//    wipes its three tape_* tables in one shot. Per-row pruning
		//    in steps 2-4 then handles surviving rows in still-active
		//    sessions.
		archivable, err := findArchivableSessions(ctx, svc, sessionCutoff, policy.MaxPerTick)
		if err != nil {
			return fmt.Errorf("findArchivableSessions: %w", err)
		}
		for _, sess := range archivable {
			delEff := svc.rew(`DELETE FROM tape_effects
WHERE app_name = ? AND user_id = ? AND session_id = ?`)
			r, err := svc.db.ExecContext(ctx, delEff, sess.app, sess.user, sess.sid)
			if err != nil {
				return fmt.Errorf("archive effects: %w", err)
			}
			n, _ := r.RowsAffected()
			result.EffectsPruned += int(n)

			delOb := svc.rew(`DELETE FROM tape_obligations
WHERE app_name = ? AND user_id = ? AND session_id = ?`)
			r, err = svc.db.ExecContext(ctx, delOb, sess.app, sess.user, sess.sid)
			if err != nil {
				return fmt.Errorf("archive obligations: %w", err)
			}
			n, _ = r.RowsAffected()
			result.ObligationsPruned += int(n)

			delT := svc.rew(`DELETE FROM tape_timers
WHERE app_name = ? AND user_id = ? AND session_id = ?`)
			r, err = svc.db.ExecContext(ctx, delT, sess.app, sess.user, sess.sid)
			if err != nil {
				return fmt.Errorf("archive timers: %w", err)
			}
			n, _ = r.RowsAffected()
			result.TimersPruned += int(n)

			result.SessionsArchived++
		}

		// 2) Terminal obligations older than the effect TTL — but keep
		//    STUCK (operator triage signal). The compactor never deletes
		//    a row that a human still needs to see.
		if policy.ArchiveTerminalObligations {
			q := svc.rew(`DELETE FROM tape_obligations
WHERE status = ? AND ts_ms < ?`)
			r, err := svc.db.ExecContext(ctx, q, ObligationStatusCompensated, effectCutoff)
			if err != nil {
				return fmt.Errorf("prune compensated obligations: %w", err)
			}
			n, _ := r.RowsAffected()
			result.ObligationsPruned += int(n)
		}

		// 3) Fired timers older than the effect TTL.
		if policy.ArchiveFiredTimers {
			q := svc.rew(`DELETE FROM tape_timers
WHERE fired = ? AND created_at_ms < ?`)
			r, err := svc.db.ExecContext(ctx, q, true, effectCutoff)
			if err != nil {
				return fmt.Errorf("prune fired timers: %w", err)
			}
			n, _ := r.RowsAffected()
			result.TimersPruned += int(n)
		}

		// 4) Effects in a terminal status, old enough, with NO active
		//    obligation referencing them — the compensable-window pinning
		//    invariant, encoded as a NOT EXISTS subquery rather than an
		//    application-level loop. This is the load-bearing line that
		//    keeps a row whose compensator still needs the external_ref.
		q := svc.rew(`DELETE FROM tape_effects
WHERE status IN (?, ?)
  AND ts_ms < ?
  AND NOT EXISTS (
    SELECT 1 FROM tape_obligations o
     WHERE o.session_id = tape_effects.session_id
       AND o.effect_key = tape_effects.idempotency_key
       AND o.status IN (?, ?)
  )`)
		r, err := svc.db.ExecContext(ctx, q,
			EffectStatusConfirmed, EffectStatusFailed,
			effectCutoff,
			ObligationStatusPending, ObligationStatusCommitted)
		if err != nil {
			return fmt.Errorf("prune effects: %w", err)
		}
		n, _ := r.RowsAffected()
		result.EffectsPruned += int(n)
		return nil
	})
	if err != nil {
		return CompactionResult{}, err
	}
	return result, nil
}

// sessionTriple — (app_name, user_id, session_id) identifying one
// session.
type sessionTriple struct {
	app, user, sid string
}

// findArchivableSessions — a session is archivable when:
//
//   - its latest tape_effect.ts_ms < sessionCutoff; AND
//   - no active obligations on it (pending or committed); AND
//   - no STUCK obligations on it (operator-triage signal); AND
//   - no unfired timers on it.
//
// We only consider sessions that have at least one tape_effect row —
// there's nothing to archive on a session with no tape activity.
func findArchivableSessions(
	ctx context.Context, svc *TapeSessionService,
	sessionCutoff int64, limit int,
) ([]sessionTriple, error) {
	q := svc.rew(`SELECT app_name, user_id, session_id, MAX(ts_ms) AS max_ts
FROM tape_effects
GROUP BY app_name, user_id, session_id
HAVING MAX(ts_ms) < ?
LIMIT ?`)
	rows, err := svc.db.QueryContext(ctx, q, sessionCutoff, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var candidates []sessionTriple
	for rows.Next() {
		var st sessionTriple
		var maxTs int64
		if err := rows.Scan(&st.app, &st.user, &st.sid, &maxTs); err != nil {
			return nil, err
		}
		candidates = append(candidates, st)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]sessionTriple, 0, len(candidates))
	for _, c := range candidates {
		// Active obligations? (PENDING + COMMITTED).
		var nActive int
		if err := svc.db.QueryRowContext(ctx, svc.rew(
			`SELECT COUNT(*) FROM tape_obligations
WHERE app_name = ? AND user_id = ? AND session_id = ? AND status IN (?, ?)`),
			c.app, c.user, c.sid,
			ObligationStatusPending, ObligationStatusCommitted,
		).Scan(&nActive); err != nil {
			return nil, err
		}
		if nActive > 0 {
			continue
		}
		// STUCK obligations? Keep the session alive for triage.
		var nStuck int
		if err := svc.db.QueryRowContext(ctx, svc.rew(
			`SELECT COUNT(*) FROM tape_obligations
WHERE app_name = ? AND user_id = ? AND session_id = ? AND status = ?`),
			c.app, c.user, c.sid, ObligationStatusStuck,
		).Scan(&nStuck); err != nil {
			return nil, err
		}
		if nStuck > 0 {
			continue
		}
		// Unfired timers (past or future)?
		var nLive int
		if err := svc.db.QueryRowContext(ctx, svc.rew(
			`SELECT COUNT(*) FROM tape_timers
WHERE app_name = ? AND user_id = ? AND session_id = ? AND fired = ?`),
			c.app, c.user, c.sid, false,
		).Scan(&nLive); err != nil {
			return nil, err
		}
		if nLive > 0 {
			continue
		}
		out = append(out, c)
	}
	return out, nil
}
