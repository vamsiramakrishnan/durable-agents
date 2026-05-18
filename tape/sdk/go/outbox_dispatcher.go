package tape

// Outbox-reactor dispatcher — Go counterpart of Python's
// `tape.reactors.outbox`. One pass:
//
//	list effects to dispatch (PENDING + OUTBOX + due)
//	for each:
//	    claim (atomic CAS lease)
//	    look up the connector
//	    dispatch through it
//	    record result:
//	        confirmed → CompleteEffect(CONFIRMED)
//	        failed    → RecordDispatchAttempt(next_at=backoff) (eventually FAILED)
//	        unknown   → RecordDispatchAttempt(next_at=0) (status UNKNOWN; the
//	                    reconciler resolves — do NOT blindly retry; that is the
//	                    entire safety claim for non-idempotent upstreams)
//
// Safety: the server's CAS on ClaimEffectDispatch enforces non-blind-retry;
// this reactor double-checks (refuses to act if status != PENDING after the
// claim).

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/connectors"
	pb "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/tapepb"
)

// OutboxOutcome — per-effect outcome of one dispatch attempt.
type OutboxOutcome struct {
	RunID          string `json:"run_id"`
	IdempotencyKey string `json:"idempotency_key"`
	Connector      string `json:"connector"`
	Tool           string `json:"tool"`
	Status         string `json:"status"` // confirmed | unknown | failed | retry-scheduled | skipped | error
	Reason         string `json:"reason,omitempty"`
	ExternalRef    string `json:"external_ref,omitempty"`
	Error          string `json:"error,omitempty"`
	NextAtMs       int64  `json:"next_at_ms,omitempty"`
	Attempts       int    `json:"attempts,omitempty"`
}

// OutboxOptions — configuration for one dispatch pass.
type OutboxOptions struct {
	Connector            string                // restrict to one connector name
	Registry             *connectors.Registry  // defaults to connectors.Default
	Claimer              string                // identity for dispatch_claimed_by
	Limit                int64                 // ListEffectsToDispatch limit (default 200)
	DispatchMaxAttempts  int                   // give up after N (default 5)
}

func claimerID() string {
	if v := os.Getenv("TAPE_DISPATCH_CLAIMER"); v != "" {
		return v
	}
	host, _ := os.Hostname()
	return fmt.Sprintf("%s:%d", host, os.Getpid())
}

func backoffMs(attempt int) int64 {
	delayS := math.Min(1.0*math.Pow(2, float64(max(attempt-1, 0))), 60.0)
	return int64(delayS * 1000)
}

func max(a, b int) int { if a > b { return a }; return b }

func toConnectorEffect(eff *pb.EffectRecord) connectors.Effect {
	var payload any = eff.GetRequestJson()
	if s, ok := payload.(string); ok && s != "" {
		var p any
		if json.Unmarshal([]byte(s), &p) == nil { payload = p }
	}
	sem := "idempotent"
	if int32(eff.GetSemantics()) == EffectSemanticsNonIdempotent {
		sem = "non_idempotent"
	}
	return connectors.Effect{
		RunID:          eff.GetRunId(),
		IdempotencyKey: eff.GetIdempotencyKey(),
		ToolName:       eff.GetToolName(),
		Connector:      eff.GetConnector(),
		Payload:        payload,
		BusinessKey:    eff.GetBusinessKey(),
		Attempt:        int(eff.GetDispatchAttempts()) + 1,
		Semantics:      sem,
	}
}

// DispatchOne runs a single effect through its connector and records the
// result. Returns the per-effect outcome.
func DispatchOne(ctx context.Context, c *Client, eff *pb.EffectRecord, opt OutboxOptions) OutboxOutcome {
	out := OutboxOutcome{
		RunID:          eff.GetRunId(),
		IdempotencyKey: eff.GetIdempotencyKey(),
		Connector:      eff.GetConnector(),
		Tool:           eff.GetToolName(),
		Status:         "skipped",
	}
	reg := opt.Registry
	if reg == nil { reg = connectors.Default }

	conn, err := reg.Get(eff.GetConnector())
	if err != nil { out.Reason = fmt.Sprintf("no connector registered: %q", eff.GetConnector()); return out }

	claim, err := c.ClaimEffectDispatch(ctx, eff.GetRunId(), eff.GetIdempotencyKey(), opt.Claimer, 60_000)
	if err != nil { out.Status = "error"; out.Error = err.Error(); return out }
	if !claim.GetAcquired() { out.Reason = "lease contended"; return out }

	cur := claim.GetEffect()
	if int32(cur.GetStatus()) != EffectStatusPending {
		out.Reason = fmt.Sprintf("unexpected status after claim: %s", cur.GetStatus())
		return out
	}

	isNonIdem := int32(cur.GetSemantics()) == EffectSemanticsNonIdempotent

	result, derr := conn.Dispatch(ctx, toConnectorEffect(cur))
	if derr != nil {
		// Connector raised: UNKNOWN for non-idempotent, retryable failure otherwise.
		if isNonIdem {
			_, _ = c.RecordDispatchAttempt(ctx, cur.GetRunId(), cur.GetIdempotencyKey(),
				fmt.Sprintf("connector raised: %v", derr), 0)
			out.Status = "unknown"; out.Error = derr.Error(); return out
		}
		attempts := int(cur.GetDispatchAttempts()) + 1
		nextAt := time.Now().UnixMilli() + backoffMs(attempts)
		_, _ = c.RecordDispatchAttempt(ctx, cur.GetRunId(), cur.GetIdempotencyKey(),
			fmt.Sprintf("connector raised: %v", derr), nextAt)
		out.Status = "retry-scheduled"; out.Error = derr.Error(); out.NextAtMs = nextAt; out.Attempts = attempts
		return out
	}

	switch result.Outcome {
	case connectors.DispatchConfirmed:
		body := map[string]any{"external_ref": result.DispatchID}
		if r, ok := result.Response.(map[string]any); ok {
			for k, v := range r { body[k] = v }
		} else if result.Response != nil {
			body["value"] = result.Response
		}
		rj, _ := json.Marshal(body)
		_, _ = c.CompleteEffect(ctx, cur.GetRunId(), cur.GetIdempotencyKey(),
			EffectStatusConfirmed, string(rj), "")
		out.Status = "confirmed"; out.ExternalRef = result.DispatchID
		return out

	case connectors.DispatchUnknown:
		errJSON, _ := json.Marshal(map[string]any{"reason": "ack lost", "error": result.Error})
		_, _ = c.RecordDispatchAttempt(ctx, cur.GetRunId(), cur.GetIdempotencyKey(),
			string(errJSON), 0)
		out.Status = "unknown"
		return out

	default:
		// failed | pending
		attempts := int(cur.GetDispatchAttempts()) + 1
		maxAttempts := opt.DispatchMaxAttempts
		if maxAttempts <= 0 { maxAttempts = 5 }
		if attempts >= maxAttempts {
			errJSON, _ := json.Marshal(map[string]any{
				"final": true, "attempts": attempts, "last": result.Error,
			})
			_, _ = c.RecordExternalObservation(ctx, RecordExternalObservationOpts{
				RunID: cur.GetRunId(), Key: cur.GetIdempotencyKey(),
				Resolution: EffectResolutionFailed, ErrorJSON: string(errJSON),
			})
			out.Status = "failed"; out.Attempts = attempts
			return out
		}
		nextAt := time.Now().UnixMilli() + backoffMs(attempts)
		if result.RetryAfterMs > 0 { nextAt = time.Now().UnixMilli() + int64(result.RetryAfterMs) }
		errJSON, _ := json.Marshal(map[string]any{"error": result.Error})
		_, _ = c.RecordDispatchAttempt(ctx, cur.GetRunId(), cur.GetIdempotencyKey(),
			string(errJSON), nextAt)
		out.Status = "retry-scheduled"; out.NextAtMs = nextAt; out.Attempts = attempts
		return out
	}
}

// OutboxDispatchOnce runs one pass of the outbox dispatcher.
func OutboxDispatchOnce(ctx context.Context, c *Client, opt OutboxOptions) ([]OutboxOutcome, error) {
	if opt.Claimer == "" { opt.Claimer = claimerID() }
	if opt.Limit <= 0   { opt.Limit = 200 }
	resp, err := c.ListEffectsToDispatch(ctx, opt.Connector, opt.Limit)
	if err != nil { return nil, err }
	outs := make([]OutboxOutcome, 0, len(resp.Effects))
	for _, e := range resp.Effects {
		outs = append(outs, DispatchOne(ctx, c, e, opt))
	}
	return outs, nil
}

// RunOutboxOptions — long-lived dispatcher.
type RunOutboxOptions struct {
	OutboxOptions
	Interval time.Duration
	Once     bool
	OnTick   func(outcomes []OutboxOutcome)
}

// RunOutboxDispatcher polls the outbox forever (or once). Cancel via ctx.
func RunOutboxDispatcher(ctx context.Context, c *Client, opt RunOutboxOptions) error {
	if opt.Interval == 0 { opt.Interval = 1 * time.Second }
	for {
		outs, err := OutboxDispatchOnce(ctx, c, opt.OutboxOptions)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[tape outbox] tick error: %v\n", err)
		}
		if opt.OnTick != nil { opt.OnTick(outs) }
		if opt.Once { return nil }
		select {
		case <-ctx.Done(): return ctx.Err()
		case <-time.After(opt.Interval):
		}
	}
}
