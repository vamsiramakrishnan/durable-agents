package tape

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"

	pb "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/tapepb"
)

// RedriveFn — how a reactor re-invokes a stalled run. On Vertex AI Agent
// Engine the implementation calls :streamQuery; on Cloud Run / GKE / locally
// it's typically a wrapper that hands control back to the agent process.
type RedriveFn func(ctx context.Context, run *pb.RunState) error

// ──── registries (compensators / status checks) ─────────────────────────────

var (
	compMu        sync.RWMutex
	compensators  = map[string]func(ctx context.Context, payload []byte) error{}
	statusMu      sync.RWMutex
	statusChecks  = map[string]func(ctx context.Context, key string) (StatusCheckResult, error){}
)

// StatusCheckResult — the per-tool status_check returns one of these. Found =
// "the counterparty acknowledges this idempotency_key (the request did land)".
// ResponseJSON is what to record on the effect when Found.
type StatusCheckResult struct {
	Found        bool
	ResponseJSON string
}

func RegisterCompensator(kind string, fn func(ctx context.Context, payload []byte) error) {
	compMu.Lock()
	defer compMu.Unlock()
	compensators[kind] = fn
}
func GetCompensator(kind string) func(ctx context.Context, payload []byte) error {
	compMu.RLock()
	defer compMu.RUnlock()
	return compensators[kind]
}
func RegisterStatusCheck(tool string, fn func(ctx context.Context, key string) (StatusCheckResult, error)) {
	statusMu.Lock()
	defer statusMu.Unlock()
	statusChecks[tool] = fn
}
func GetStatusCheck(tool string) func(ctx context.Context, key string) (StatusCheckResult, error) {
	statusMu.RLock()
	defer statusMu.RUnlock()
	return statusChecks[tool]
}

// ──── recovery ──────────────────────────────────────────────────────────────

func RecoverOnce(ctx context.Context, c *Client, redrive RedriveFn) ([]string, error) {
	resp, err := c.ListRunsToRecover(ctx, 50)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(resp.Runs))
	for _, r := range resp.Runs {
		if redrive != nil {
			if err := redrive(ctx, r); err != nil {
				continue
			}
		}
		out = append(out, r.RunId)
	}
	return out, nil
}

// ──── reconciler ────────────────────────────────────────────────────────────

func ReconcileOnce(ctx context.Context, c *Client, reconcilePendingAfter time.Duration) (int, error) {
	includePending := reconcilePendingAfter > 0
	olderThan := int64(0)
	if includePending {
		olderThan = time.Now().Add(-reconcilePendingAfter).UnixMilli()
	}
	resp, err := c.ListPendingEffects(ctx, olderThan, includePending, true, 500)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, e := range resp.Effects {
		check := GetStatusCheck(e.ToolName)
		if check == nil {
			continue
		}
		res, err := check(ctx, e.IdempotencyKey)
		if err != nil {
			continue
		}
		if res.Found {
			if _, err := c.ReconcileEffect(ctx, e.RunId, e.IdempotencyKey, EffectStatusConfirmed, res.ResponseJSON, ""); err == nil {
				n++
			}
		} else if int32(e.Status) == EffectStatusUnknown {
			if _, err := c.ReconcileEffect(ctx, e.RunId, e.IdempotencyKey, EffectStatusFailed, "", `{"reconciled":"absent at counterparty"}`); err == nil {
				n++
			}
		}
	}
	return n, nil
}

// ──── timer reactor ─────────────────────────────────────────────────────────

func FireDueTimersOnce(ctx context.Context, c *Client, redrive RedriveFn) (int, error) {
	resp, err := c.ListDueTimers(ctx, 0, 500, true)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, t := range resp.Timers {
		switch t.Kind {
		case "gate_timeout":
			var payload map[string]any
			_ = json.Unmarshal([]byte(t.PayloadJson), &payload)
			gate, _ := payload["gate"].(string)
			res := map[string]any{"timed_out": true}
			if r, ok := payload["resolution"].(map[string]any); ok {
				for k, v := range r {
					res[k] = v
				}
			}
			rj, _ := json.Marshal(res)
			if _, err := c.SendSignal(ctx, SendSignalOpts{RunID: t.RunId, GateName: gate, ResolutionJSON: string(rj)}); err == nil {
				n++
			}
		case "redrive":
			if redrive != nil {
				run, err := c.GetRun(ctx, t.RunId)
				if err == nil {
					_ = redrive(ctx, run)
					n++
				}
			}
		}
	}
	return n, nil
}

// ──── the loop ──────────────────────────────────────────────────────────────

type RunReactorsOptions struct {
	Redrive                RedriveFn
	Recover                bool
	Reconcile              bool
	Timers                 bool
	Interval               time.Duration
	ReconcilePendingAfter  time.Duration
	Once                   bool
	OnTick                 func(tick map[string]any)
}

func RunReactors(ctx context.Context, c *Client, opt RunReactorsOptions) error {
	if opt.Interval == 0 {
		opt.Interval = 2 * time.Second
	}
	if !opt.Recover && !opt.Reconcile && !opt.Timers {
		opt.Recover, opt.Reconcile, opt.Timers = true, true, true
	}
	for {
		tick := map[string]any{}
		if opt.Recover && opt.Redrive != nil {
			if recovered, err := RecoverOnce(ctx, c, opt.Redrive); err != nil {
				tick["recover_error"] = err.Error()
			} else {
				tick["recovered"] = recovered
			}
		}
		if opt.Reconcile {
			if n, err := ReconcileOnce(ctx, c, opt.ReconcilePendingAfter); err != nil {
				tick["reconcile_error"] = err.Error()
			} else {
				tick["reconciled"] = n
			}
		}
		if opt.Timers {
			if n, err := FireDueTimersOnce(ctx, c, opt.Redrive); err != nil {
				tick["timer_error"] = err.Error()
			} else {
				tick["timers_fired"] = n
			}
		}
		if opt.OnTick != nil {
			opt.OnTick(tick)
		}
		if opt.Once {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(opt.Interval):
		}
	}
}

// ──── WAL fan-out ───────────────────────────────────────────────────────────

// RunEventFanout is the legacy timestamp-cursored WAL fan-out. Preserved for
// back-compat — new code should use RunEventFanoutBySubject (global_seq
// cursored) or RunEventFanoutWith (the option-struct form supporting both
// cursors plus a subject pattern).
//
// Deprecated: prefer RunEventFanoutBySubject for global_seq cursoring.
func RunEventFanout(ctx context.Context, c *Client, fromTsMs int64, runID, kind string, sink func(entry *pb.EventEntry) error) error {
	stream, err := c.SubscribeEvents(ctx, fromTsMs, runID, kind)
	if err != nil {
		return err
	}
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := sink(entry); err != nil {
			return err
		}
	}
}

// RunEventFanoutBySubject tails the journal via SubscribeBySubject, cursored
// on `global_seq`. The matching subject pattern follows the path-style grammar
// (`*` = one segment, `**` = trailing segments). `predicateCEL` is an optional
// server-side CEL filter on the envelope.
func RunEventFanoutBySubject(ctx context.Context, c *Client, subjectPattern, predicateCEL string, fromGlobalSeq int64, sink func(entry *pb.EventEntry) error) error {
	stream, err := c.SubscribeBySubject(ctx, subjectPattern, predicateCEL, fromGlobalSeq)
	if err != nil {
		return err
	}
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := sink(entry); err != nil {
			return err
		}
	}
}

// RunEventFanoutOpts mirrors SubscribeEventsOpts: pass `FromGlobalSeq` /
// `SubjectPattern` for the event-bus path or `FromTsMs` / `RunID` / `Kind`
// for the legacy filters.
type RunEventFanoutOpts struct {
	FromTsMs       int64
	RunID          string
	Kind           string
	FromGlobalSeq  int64
	SubjectPattern string
}

// RunEventFanoutWith is the option-struct form of RunEventFanout. Honours
// both the legacy and the event-bus filter fields.
func RunEventFanoutWith(ctx context.Context, c *Client, o RunEventFanoutOpts, sink func(entry *pb.EventEntry) error) error {
	stream, err := c.SubscribeEventsWith(ctx, SubscribeEventsOpts{
		FromTsMs:       o.FromTsMs,
		RunID:          o.RunID,
		Kind:           o.Kind,
		FromGlobalSeq:  o.FromGlobalSeq,
		SubjectPattern: o.SubjectPattern,
	})
	if err != nil {
		return err
	}
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := sink(entry); err != nil {
			return err
		}
	}
}
