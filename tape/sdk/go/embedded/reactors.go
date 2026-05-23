package embedded

// reactors.go — four `*Once` functions, one tick each. Same semantics
// as `tape_adk.reactors`. Run them on whatever scheduler you've got —
// a goroutine + time.Ticker, a Cloud Run Job, a Kubernetes CronJob.
//
// Crash-safety is built into the data model: claims have TTLs, so a
// process that dies mid-tick releases its work to the next runner.
// Returns from each function are small `[]ActionLog` slices for the
// audit trail — same shape (key, outcome, optional extras) as Python's
// list-of-dicts.

import (
	"context"
	"fmt"
	"log"
)

// ActionLog — one entry per (effect/obligation/timer) the reactor
// touched in a single tick. Mirrors the Python `list[dict]` shape: a
// loose bag of fields; the `Outcome` and `Skip` fields are the most
// informative, with the others context-dependent.
type ActionLog struct {
	Key         string // effect idempotency_key
	Seq         int64  // obligation seq
	TimerID     string // timer id
	Outcome     string // "confirmed" / "unknown" / "failed-retry" / etc.
	Skip        string // populated when the reactor skipped (no connector / lost claim)
	ExternalRef string
	BackoffMs   int64
}

// ── outbox dispatcher ─────────────────────────────────────────────────────

// DispatchOutboxOpts — knobs for one tick of the outbox loop.
type DispatchOutboxOpts struct {
	Claimer          string
	Limit            int
	LeaseTTLMs       int64
	DefaultBackoffMs int64
	MaxBackoffMs     int64
}

// DispatchOutboxOnce — one tick of the outbox loop. Mirrors
// `tape_adk.reactors.dispatch_outbox_once`. Walks ListEffectsToDispatch,
// claims via CAS, dispatches via the matching connector, transitions
// the effect.
func DispatchOutboxOnce(
	ctx context.Context, svc *TapeSessionService,
	connectors map[string]Connector, o DispatchOutboxOpts,
) ([]ActionLog, error) {
	if o.Limit <= 0 {
		o.Limit = 50
	}
	if o.LeaseTTLMs <= 0 {
		o.LeaseTTLMs = 60_000
	}
	if o.DefaultBackoffMs <= 0 {
		o.DefaultBackoffMs = 5_000
	}
	if o.MaxBackoffMs <= 0 {
		o.MaxBackoffMs = 300_000
	}
	now := nowMs()
	effects, err := svc.ListEffectsToDispatch(ctx, ListEffectsToDispatchOpts{NowMs: now, Limit: o.Limit})
	if err != nil {
		return nil, fmt.Errorf("DispatchOutboxOnce: list: %w", err)
	}

	var out []ActionLog
	for _, eff := range effects {
		conn, ok := connectors[eff.Connector]
		if !ok {
			out = append(out, ActionLog{Key: eff.IdempotencyKey,
				Skip: fmt.Sprintf("no connector for %q", eff.Connector)})
			continue
		}
		acquired, _, err := svc.ClaimEffectDispatch(ctx,
			eff.AppName, eff.UserID, eff.SessionID, eff.IdempotencyKey,
			o.Claimer, o.LeaseTTLMs, now)
		if err != nil {
			return nil, fmt.Errorf("DispatchOutboxOnce: claim %s: %w", eff.IdempotencyKey, err)
		}
		if !acquired {
			out = append(out, ActionLog{Key: eff.IdempotencyKey, Skip: "lost the claim"})
			continue
		}
		// Re-read to make sure the row is still PENDING after the claim.
		fresh, err := svc.GetEffect(ctx, eff.AppName, eff.UserID, eff.SessionID, eff.IdempotencyKey)
		if err != nil {
			return nil, fmt.Errorf("DispatchOutboxOnce: re-read %s: %w", eff.IdempotencyKey, err)
		}
		if fresh == nil || fresh.Status != EffectStatusPending {
			out = append(out, ActionLog{Key: eff.IdempotencyKey, Skip: "not PENDING after claim"})
			continue
		}

		outcome, dispatchErr := conn.Dispatch(ctx, *fresh)
		if dispatchErr != nil {
			attempts := fresh.DispatchAttempts + 1
			backoff := expoBackoff(o.DefaultBackoffMs, attempts, o.MaxBackoffMs)
			if _, err := svc.RecordDispatchAttempt(ctx,
				fresh.AppName, fresh.UserID, fresh.SessionID, fresh.IdempotencyKey,
				fmt.Sprintf("%T: %v", dispatchErr, dispatchErr),
				now+backoff); err != nil {
				return nil, fmt.Errorf("DispatchOutboxOnce: record-attempt %s: %w", fresh.IdempotencyKey, err)
			}
			out = append(out, ActionLog{Key: fresh.IdempotencyKey,
				Outcome: "exception", BackoffMs: backoff})
			continue
		}

		switch outcome.Status {
		case "confirmed":
			if _, err := svc.CompleteEffect(ctx,
				fresh.AppName, fresh.UserID, fresh.SessionID, fresh.IdempotencyKey,
				EffectStatusConfirmed, outcome.Response, nil); err != nil {
				return nil, fmt.Errorf("DispatchOutboxOnce: complete %s: %w", fresh.IdempotencyKey, err)
			}
			if outcome.ExternalRef != "" {
				// Direct UPDATE — the effect is already CONFIRMED so the
				// observation path's status-overwrite isn't what we want;
				// just attach the ref.
				q := svc.rew(`UPDATE tape_effects SET external_ref = ?
WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?`)
				if _, err := svc.db.ExecContext(ctx, q,
					outcome.ExternalRef, fresh.AppName, fresh.UserID, fresh.SessionID, fresh.IdempotencyKey); err != nil {
					return nil, fmt.Errorf("DispatchOutboxOnce: attach external_ref %s: %w", fresh.IdempotencyKey, err)
				}
			}
			out = append(out, ActionLog{Key: fresh.IdempotencyKey,
				Outcome: "confirmed", ExternalRef: outcome.ExternalRef})

		case "unknown":
			errMsg := "ack lost"
			if outcome.Error != nil {
				errMsg = fmt.Sprintf("%v", outcome.Error)
			}
			if _, err := svc.RecordDispatchAttempt(ctx,
				fresh.AppName, fresh.UserID, fresh.SessionID, fresh.IdempotencyKey,
				errMsg, 0); err != nil {
				return nil, fmt.Errorf("DispatchOutboxOnce: record-attempt %s: %w", fresh.IdempotencyKey, err)
			}
			out = append(out, ActionLog{Key: fresh.IdempotencyKey, Outcome: "unknown"})

		case "failed":
			errMsg := "dispatch failed"
			if outcome.Error != nil {
				errMsg = fmt.Sprintf("%v", outcome.Error)
			}
			if outcome.RetryAfterMs < 0 {
				// Terminal — give up.
				if _, err := svc.CompleteEffect(ctx,
					fresh.AppName, fresh.UserID, fresh.SessionID, fresh.IdempotencyKey,
					EffectStatusFailed, nil, outcome.Error); err != nil {
					return nil, fmt.Errorf("DispatchOutboxOnce: terminal-fail %s: %w", fresh.IdempotencyKey, err)
				}
				out = append(out, ActionLog{Key: fresh.IdempotencyKey, Outcome: "failed-terminal"})
			} else {
				attempts := fresh.DispatchAttempts + 1
				backoff := outcome.RetryAfterMs
				if backoff == 0 {
					backoff = expoBackoff(o.DefaultBackoffMs, attempts, o.MaxBackoffMs)
				}
				if _, err := svc.RecordDispatchAttempt(ctx,
					fresh.AppName, fresh.UserID, fresh.SessionID, fresh.IdempotencyKey,
					errMsg, now+backoff); err != nil {
					return nil, fmt.Errorf("DispatchOutboxOnce: record-attempt %s: %w", fresh.IdempotencyKey, err)
				}
				out = append(out, ActionLog{Key: fresh.IdempotencyKey,
					Outcome: "failed-retry", BackoffMs: backoff})
			}

		default:
			log.Printf("embedded.DispatchOutboxOnce: connector %q returned unknown status %q for %s",
				eff.Connector, outcome.Status, fresh.IdempotencyKey)
		}
	}
	return out, nil
}

// ── reconciler ────────────────────────────────────────────────────────────

// ReconcileOpts — knobs for one tick of the reconciler.
type ReconcileOpts struct {
	StalePendingMs int64
	Limit          int
}

// ReconcileOnce — one tick of the reconciler. Walks UNKNOWN effects
// (and optionally stale PENDING effects), asks connector.observe, maps
// the result to a status transition via RecordExternalObservation.
func ReconcileOnce(
	ctx context.Context, svc *TapeSessionService,
	connectors map[string]Connector, o ReconcileOpts,
) ([]ActionLog, error) {
	if o.Limit <= 0 {
		o.Limit = 50
	}
	cutoff := int64(0)
	if o.StalePendingMs > 0 {
		cutoff = nowMs() - o.StalePendingMs
	}
	effects, err := svc.ListPendingEffects(ctx, ListPendingEffectsOpts{
		OlderThanMs:    cutoff,
		IncludePending: o.StalePendingMs > 0,
		IncludeUnknown: true,
		Limit:          o.Limit,
	})
	if err != nil {
		return nil, fmt.Errorf("ReconcileOnce: list: %w", err)
	}

	var out []ActionLog
	for _, eff := range effects {
		conn, ok := connectors[eff.Connector]
		if !ok {
			out = append(out, ActionLog{Key: eff.IdempotencyKey,
				Skip: fmt.Sprintf("no connector for %q", eff.Connector)})
			continue
		}
		obs, err := conn.Observe(ctx, eff)
		if err != nil {
			out = append(out, ActionLog{Key: eff.IdempotencyKey,
				Skip: fmt.Sprintf("observe raised: %v", err)})
			continue
		}
		if _, err := svc.RecordExternalObservation(ctx, RecordExternalObservationOpts{
			AppName: eff.AppName, UserID: eff.UserID, SessionID: eff.SessionID,
			IdempotencyKey:            eff.IdempotencyKey,
			Resolution:                obs.Status,
			ExternalRef:               obs.ExternalRef,
			ResponseJSON:              obs.Response,
			ErrorJSON:                 obs.Error,
			CompensateOnDuplicateKind: obs.CompensateKind,
		}); err != nil {
			return nil, fmt.Errorf("ReconcileOnce: record-observation %s: %w", eff.IdempotencyKey, err)
		}
		out = append(out, ActionLog{Key: eff.IdempotencyKey,
			Outcome: obs.Status, ExternalRef: obs.ExternalRef})
	}
	return out, nil
}

// ── compensation drainer ─────────────────────────────────────────────────

// DrainObligationsOpts — knobs for one tick of the drainer.
type DrainObligationsOpts struct {
	Claimer          string
	Limit            int
	LeaseTTLMs       int64
	DefaultBackoffMs int64
	MaxBackoffMs     int64
}

// DrainObligationsOnce — one tick of the compensation drain. LIFO order
// (seq DESC) matches the proto. For each unresolved obligation: find
// the connector (by the originating effect's `connector`, falling back
// to ob.kind), CAS-claim, run compensate, record outcome.
func DrainObligationsOnce(
	ctx context.Context, svc *TapeSessionService,
	connectors map[string]Connector, o DrainObligationsOpts,
) ([]ActionLog, error) {
	if o.Limit <= 0 {
		o.Limit = 50
	}
	if o.LeaseTTLMs <= 0 {
		o.LeaseTTLMs = 60_000
	}
	if o.DefaultBackoffMs <= 0 {
		o.DefaultBackoffMs = 5_000
	}
	if o.MaxBackoffMs <= 0 {
		o.MaxBackoffMs = 300_000
	}
	if o.Claimer == "" {
		o.Claimer = "drainer"
	}
	now := nowMs()
	obligations, err := svc.ListUnresolvedObligations(ctx, ListUnresolvedObligationsOpts{
		NowMs:                   now,
		Limit:                   o.Limit,
		IncludePending:          true,
		IncludeCommittedExpired: true,
		IncludeStuck:            false,
	})
	if err != nil {
		return nil, fmt.Errorf("DrainObligationsOnce: list: %w", err)
	}

	var out []ActionLog
	for _, ob := range obligations {
		// Determine connector — prefer effect.connector, fall back to ob.Kind.
		connectorName := ob.Kind
		if ob.EffectKey != "" {
			eff, err := svc.GetEffect(ctx, ob.AppName, ob.UserID, ob.SessionID, ob.EffectKey)
			if err != nil {
				return nil, fmt.Errorf("DrainObligationsOnce: get-effect %d: %w", ob.Seq, err)
			}
			if eff != nil && eff.Connector != "" {
				connectorName = eff.Connector
			}
		}
		conn, ok := connectors[connectorName]
		if !ok {
			out = append(out, ActionLog{Seq: ob.Seq,
				Skip: fmt.Sprintf("no connector for %q", connectorName)})
			continue
		}

		acquired, _, err := svc.ClaimObligation(ctx, ob.Seq, o.Claimer, o.LeaseTTLMs, now)
		if err != nil {
			return nil, fmt.Errorf("DrainObligationsOnce: claim %d: %w", ob.Seq, err)
		}
		if !acquired {
			out = append(out, ActionLog{Seq: ob.Seq, Skip: "lost the claim"})
			continue
		}

		outcome, compErr := conn.Compensate(ctx, ob)
		if compErr != nil {
			attempts := ob.Attempts + 1
			backoff := expoBackoff(o.DefaultBackoffMs, attempts, o.MaxBackoffMs)
			if _, err := svc.RecordObligationAttempt(ctx, ob.Seq,
				fmt.Sprintf("%T: %v", compErr, compErr), now+backoff); err != nil {
				return nil, fmt.Errorf("DrainObligationsOnce: record-attempt %d: %w", ob.Seq, err)
			}
			out = append(out, ActionLog{Seq: ob.Seq, Outcome: "exception", BackoffMs: backoff})
			continue
		}

		switch outcome.Status {
		case "compensated":
			if _, err := svc.ResolveObligation(ctx, ob.Seq, ObligationStatusCompensated, outcome.Response); err != nil {
				return nil, fmt.Errorf("DrainObligationsOnce: resolve %d: %w", ob.Seq, err)
			}
			out = append(out, ActionLog{Seq: ob.Seq, Outcome: "compensated"})
		case "failed":
			backoff := outcome.RetryAfterMs
			if backoff == 0 {
				backoff = expoBackoff(o.DefaultBackoffMs, ob.Attempts+1, o.MaxBackoffMs)
			}
			errMsg := "compensate failed"
			if outcome.Error != nil {
				errMsg = fmt.Sprintf("%v", outcome.Error)
			}
			if _, err := svc.RecordObligationAttempt(ctx, ob.Seq, errMsg, now+backoff); err != nil {
				return nil, fmt.Errorf("DrainObligationsOnce: record-attempt %d: %w", ob.Seq, err)
			}
			out = append(out, ActionLog{Seq: ob.Seq, Outcome: "failed-retry", BackoffMs: backoff})
		default:
			log.Printf("embedded.DrainObligationsOnce: compensate returned %q for seq %d",
				outcome.Status, ob.Seq)
		}
	}
	return out, nil
}

// ── timer firer ──────────────────────────────────────────────────────────

// TimerDispatcher — callback that runs for each fired timer.
type TimerDispatcher func(ctx context.Context, t TimerRecord) error

// FireDueTimersOpts — knobs for one tick of the timer firer.
type FireDueTimersOpts struct {
	Limit      int
	Dispatcher TimerDispatcher
}

// FireDueTimersOnce — claim all due timers and hand each to
// `Dispatcher`. With Dispatcher=nil the timers are just marked fired.
func FireDueTimersOnce(
	ctx context.Context, svc *TapeSessionService, o FireDueTimersOpts,
) ([]ActionLog, error) {
	if o.Limit <= 0 {
		o.Limit = 100
	}
	timers, err := svc.ListDueTimers(ctx, ListDueTimersOpts{NowMs: nowMs(), Limit: o.Limit, Claim: true})
	if err != nil {
		return nil, fmt.Errorf("FireDueTimersOnce: %w", err)
	}
	var out []ActionLog
	for _, t := range timers {
		if o.Dispatcher == nil {
			out = append(out, ActionLog{TimerID: t.TimerID, Outcome: "marked-fired"})
			continue
		}
		if err := o.Dispatcher(ctx, t); err != nil {
			out = append(out, ActionLog{TimerID: t.TimerID,
				Outcome: fmt.Sprintf("dispatcher raised: %v", err)})
			continue
		}
		out = append(out, ActionLog{TimerID: t.TimerID, Outcome: "fired"})
	}
	return out, nil
}

// expoBackoff — `default * 2^(attempts-1)` capped at `max`. Matches the
// Python computation.
func expoBackoff(defaultMs int64, attempts int, maxMs int64) int64 {
	if attempts < 1 {
		attempts = 1
	}
	v := defaultMs
	for i := 1; i < attempts; i++ {
		v *= 2
		if v >= maxMs {
			return maxMs
		}
	}
	return v
}
