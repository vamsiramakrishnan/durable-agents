package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	tape "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go"
	"github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/tapepb"
)

func ok(name, detail string) InvariantResult     { return InvariantResult{Name: name, Passed: true, Detail: detail} }
func notOk(name, detail string) InvariantResult { return InvariantResult{Name: name, Passed: false, Detail: detail} }

// ── exactly_one ──────────────────────────────────────────────────────────

// ExactlyOneOpts — selector for ExactlyOne. Exactly one of Connector/Tool must be set.
type ExactlyOneOpts struct {
	Connector string
	Tool      string
	By        string // default: "business_key"
}

type exactlyOneInv struct{ opts ExactlyOneOpts }

func (i *exactlyOneInv) Name() string {
	target := i.opts.Connector
	if target == "" {
		target = i.opts.Tool
	}
	by := i.opts.By
	if by == "" {
		by = "business_key"
	}
	return fmt.Sprintf("exactly_one(%q, by=%q)", target, by)
}

func (i *exactlyOneInv) Check(ctx context.Context, client *tape.Client, _ string) InvariantResult {
	pattern := "/tape/effect/confirmed/**"
	if i.opts.Tool != "" {
		pattern = fmt.Sprintf("/tape/effect/confirmed/%s/**", i.opts.Tool)
	}
	by := i.opts.By
	if by == "" {
		by = "business_key"
	}
	counts := map[string]int{}
	stream, err := client.SubscribeBySubject(ctx, pattern, "", 1)
	if err != nil {
		return notOk(i.Name(), fmt.Sprintf("SubscribeBySubject failed: %v", err))
	}
	// Read until the stream ends (deadline-bound by caller via ctx).
	for {
		evt, err := stream.Recv()
		if err != nil {
			break
		}
		var payload map[string]any
		if json.Unmarshal([]byte(evt.PayloadJson), &payload) != nil {
			continue
		}
		if i.opts.Connector != "" && payload["connector"] != i.opts.Connector {
			continue
		}
		k, _ := payload[by].(string)
		if k == "" {
			continue
		}
		counts[k]++
	}
	var dupes []string
	for k, v := range counts {
		if v > 1 {
			dupes = append(dupes, fmt.Sprintf("%s:%d", k, v))
		}
	}
	if len(dupes) > 0 {
		return notOk(i.Name(), fmt.Sprintf("duplicate business keys: %s", strings.Join(dupes, ",")))
	}
	return ok(i.Name(), fmt.Sprintf("unique business keys: %d", len(counts)))
}

// ExactlyOne — "one wire, one record": every CONFIRMED effect under the
// selector with a non-empty business_key has count == 1.
func ExactlyOne(opts ExactlyOneOpts) Invariant {
	if opts.Connector == "" && opts.Tool == "" {
		panic("chaos.ExactlyOne: needs Connector or Tool")
	}
	return &exactlyOneInv{opts: opts}
}

// ── no_stuck_obligations ─────────────────────────────────────────────────

type noStuckInv struct{}

func (noStuckInv) Name() string { return "no_stuck_obligations" }
func (noStuckInv) Check(ctx context.Context, client *tape.Client, runID string) InvariantResult {
	resp, err := client.ListUnresolvedObligations(ctx, tape.ListUnresolvedObligationsOpts{
		IncludeStuck: true, Limit: 500,
	})
	if err != nil {
		return notOk("no_stuck_obligations", fmt.Sprintf("ListUnresolvedObligations failed: %v", err))
	}
	stuck := 0
	for _, o := range resp.Obligations {
		if o.Status != tapepb.ObligationStatus_OBLIGATION_STATUS_STUCK {
			continue
		}
		if runID != "" && o.RunId != runID {
			continue
		}
		stuck++
	}
	if stuck > 0 {
		return notOk("no_stuck_obligations", fmt.Sprintf("%d stuck obligation(s)", stuck))
	}
	return ok("no_stuck_obligations", "0 stuck")
}

// NoStuckObligations — every registered compensation eventually drains.
var NoStuckObligations Invariant = noStuckInv{}

// ── no_blind_non_idempotent_retry ────────────────────────────────────────

type noBlindNIRInv struct{}

func (noBlindNIRInv) Name() string { return "no_blind_non_idempotent_retry" }
func (noBlindNIRInv) Check(ctx context.Context, client *tape.Client, runID string) InvariantResult {
	resp, err := client.ListPendingEffects(ctx, 0, true, true, 500)
	if err != nil {
		return notOk("no_blind_non_idempotent_retry", fmt.Sprintf("ListPendingEffects failed: %v", err))
	}
	var bad []string
	for _, e := range resp.Effects {
		if runID != "" && e.RunId != runID {
			continue
		}
		if e.Semantics != tapepb.EffectSemantics_EFFECT_SEMANTICS_NON_IDEMPOTENT {
			continue
		}
		if e.DispatchAttempts > 1 && e.Status == tapepb.EffectStatus_EFFECT_STATUS_PENDING && e.ExternalRef == "" {
			bad = append(bad, fmt.Sprintf("%s/%s@%d", e.RunId, e.IdempotencyKey, e.DispatchAttempts))
		}
	}
	if len(bad) > 0 {
		head := bad
		if len(head) > 3 {
			head = head[:3]
		}
		return notOk("no_blind_non_idempotent_retry",
			fmt.Sprintf("%d non-idempotent effect(s) re-dispatched without observation: %s",
				len(bad), strings.Join(head, ", ")))
	}
	return ok("no_blind_non_idempotent_retry", "no blind retries on non-idempotent effects")
}

// NoBlindNonIdempotentRetry — for every NON_IDEMPOTENT effect, attempts > 1
// implies observation has been recorded (external_ref set).
var NoBlindNonIdempotentRetry Invariant = noBlindNIRInv{}

// ── no_orphan_compensation ───────────────────────────────────────────────

type noOrphanInv struct{}

func (noOrphanInv) Name() string { return "no_orphan_compensation" }
func (noOrphanInv) Check(ctx context.Context, client *tape.Client, runID string) InvariantResult {
	if runID == "" {
		return ok("no_orphan_compensation", "no runID; skipped")
	}
	resp, err := client.ListObligations(ctx, runID, false)
	if err != nil {
		return notOk("no_orphan_compensation", fmt.Sprintf("ListObligations failed: %v", err))
	}
	var orphans []string
	for _, o := range resp.Obligations {
		got, err := client.GetEffect(ctx, runID, o.EffectKey)
		if err != nil || !got.Found {
			orphans = append(orphans, o.EffectKey)
		}
	}
	if len(orphans) > 0 {
		head := orphans
		if len(head) > 3 {
			head = head[:3]
		}
		return notOk("no_orphan_compensation",
			fmt.Sprintf("%d obligation(s) with no effect: %s", len(orphans), strings.Join(head, ", ")))
	}
	return ok("no_orphan_compensation", fmt.Sprintf("all %d obligation(s) have an effect", len(resp.Obligations)))
}

// NoOrphanCompensation — every obligation references an existing effect.
var NoOrphanCompensation Invariant = noOrphanInv{}

// ── no_budget_overrun (v1 stub; full check is Phase 3.5) ─────────────────

type noBudgetInv struct{}

func (noBudgetInv) Name() string { return "no_budget_overrun" }
func (noBudgetInv) Check(_ context.Context, _ *tape.Client, runID string) InvariantResult {
	if runID == "" {
		return ok("no_budget_overrun", "no runID; skipped")
	}
	return ok("no_budget_overrun", "budget projection check is a Phase-3 invariant; v1 stub")
}

// NoBudgetOverrun — placeholder matching Python/TS stub. Phase 3.5 wires
// a real budget projection.
var NoBudgetOverrun Invariant = noBudgetInv{}
