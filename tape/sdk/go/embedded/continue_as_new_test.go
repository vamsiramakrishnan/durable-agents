package embedded

// continue_as_new_test.go — Go port of `tape_adk/tests/test_continue_as_new.py`.
// Seven tests; same safety invariant (compensable-window pinning) and
// same `tape_values` namespace contract.

import (
	"context"
	"reflect"
	"sync"
	"testing"
)

// _callSeq — per-test counter for synthesising unique call_index values
// when seeding multiple effects under one invocation_id.
var (
	_callSeqMu sync.Mutex
	_callSeq   map[string]int
)

func resetCallSeq() {
	_callSeqMu.Lock()
	defer _callSeqMu.Unlock()
	_callSeq = map[string]int{}
}

func nextCallIndex(invocation string) int {
	_callSeqMu.Lock()
	defer _callSeqMu.Unlock()
	n := _callSeq[invocation]
	_callSeq[invocation] = n + 1
	return n
}

// _confirmedEffect — seed a CONFIRMED effect under the given invocation
// and business key. Each call under the same invocation gets a fresh
// call_index so the derived idempotency key is unique.
func _confirmedEffect(t *testing.T, svc *TapeSessionService, invocation, key string) string {
	t.Helper()
	ctx := context.Background()
	ci := nextCallIndex(invocation)
	e, err := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "a", UserID: "u", SessionID: "s",
		InvocationID: invocation, DecisionIndex: 0,
		ToolName: "bank.wire", CallIndex: ci,
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  key, Connector: "bank.wire",
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := svc.CompleteEffect(ctx, "a", "u", "s", e.IdempotencyKey,
		EffectStatusConfirmed, map[string]any{"id": key}, nil); err != nil {
		t.Fatalf("complete: %v", err)
	}
	return e.IdempotencyKey
}

// ── happy path ───────────────────────────────────────────────────────────

func TestContinueAsNewPrunesOldInvocationsTerminalEffects(t *testing.T) {
	resetCallSeq()
	svc := newTestSvc(t)
	ctx := context.Background()
	var keys []string
	for i := 0; i < 3; i++ {
		keys = append(keys, _confirmedEffect(t, svc, "inv-old", "k-"+itoa(i)))
	}
	// Sanity: 3 rows exist.
	for _, k := range keys {
		eff, _ := svc.GetEffect(ctx, "a", "u", "s", k)
		if eff == nil {
			t.Fatalf("missing pre-prune row %q", k)
		}
	}

	r, err := svc.ContinueAsNew(ctx, NewContinueAsNewOpts("a", "u", "s", "inv-old", "inv-new"))
	if err != nil {
		t.Fatalf("continue: %v", err)
	}
	if r.EffectsPruned != 3 {
		t.Fatalf("EffectsPruned=%d, want 3", r.EffectsPruned)
	}
	for _, k := range keys {
		eff, _ := svc.GetEffect(ctx, "a", "u", "s", k)
		if eff != nil {
			t.Fatalf("row not pruned: %q", k)
		}
	}
}

func TestContinueAsNewKeepsOtherInvocationsEffects(t *testing.T) {
	resetCallSeq()
	svc := newTestSvc(t)
	ctx := context.Background()
	kOld := _confirmedEffect(t, svc, "inv-old", "k-old")
	kOther := _confirmedEffect(t, svc, "inv-other", "k-other")

	r, err := svc.ContinueAsNew(ctx, NewContinueAsNewOpts("a", "u", "s", "inv-old", "inv-new"))
	if err != nil {
		t.Fatalf("continue: %v", err)
	}
	if r.EffectsPruned != 1 {
		t.Fatalf("EffectsPruned=%d, want 1", r.EffectsPruned)
	}
	if eff, _ := svc.GetEffect(ctx, "a", "u", "s", kOld); eff != nil {
		t.Fatalf("old row still present: %+v", eff)
	}
	if eff, _ := svc.GetEffect(ctx, "a", "u", "s", kOther); eff == nil {
		t.Fatalf("other-invocation row was pruned")
	}
}

// ── safety: pinning by an active obligation ──────────────────────────────

func TestContinueAsNewDoesNotPrunePinnedEffectEvenUnderOldInvocation(t *testing.T) {
	resetCallSeq()
	svc := newTestSvc(t)
	ctx := context.Background()
	key := _confirmedEffect(t, svc, "inv-old", "k-1")
	if _, err := svc.RegisterCompensation(ctx, RegisterCompensationOpts{
		AppName: "a", UserID: "u", SessionID: "s",
		EffectKey: key, Kind: "reverse_wire",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	r, err := svc.ContinueAsNew(ctx, NewContinueAsNewOpts("a", "u", "s", "inv-old", "inv-new"))
	if err != nil {
		t.Fatalf("continue: %v", err)
	}
	if r.EffectsPruned != 0 {
		t.Fatalf("EffectsPruned=%d, want 0 (PINNED)", r.EffectsPruned)
	}
	if r.ObligationsKept != 1 {
		t.Fatalf("ObligationsKept=%d, want 1", r.ObligationsKept)
	}
	if eff, _ := svc.GetEffect(ctx, "a", "u", "s", key); eff == nil {
		t.Fatalf("pinned effect was pruned")
	}
}

// ── state carry ──────────────────────────────────────────────────────────

func TestContinueAsNewCarriedStateIsReadableUnderNewInvocationID(t *testing.T) {
	resetCallSeq()
	svc := newTestSvc(t)
	ctx := context.Background()
	o := NewContinueAsNewOpts("a", "u", "s", "inv-old", "inv-new")
	o.CarriedState = map[string]any{"checkpoint": "after sweep", "balance": float64(42)}
	o.HasCarriedState = true
	if _, err := svc.ContinueAsNew(ctx, o); err != nil {
		t.Fatalf("continue: %v", err)
	}
	val, err := svc.GetValue(ctx, "tape:continue-as-new:s", "inv-new")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if val == nil {
		t.Fatalf("expected a tape_values row, got nil")
	}
	want := map[string]any{"checkpoint": "after sweep", "balance": float64(42)}
	got, ok := val.ValueJSON.(map[string]any)
	if !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("value=%#v, want %#v", val.ValueJSON, want)
	}
	if val.Writer != "continue_as_new" {
		t.Fatalf("writer=%q, want continue_as_new", val.Writer)
	}
}

func TestContinueAsNewIsAtomic(t *testing.T) {
	resetCallSeq()
	svc := newTestSvc(t)
	ctx := context.Background()
	key := _confirmedEffect(t, svc, "inv-old", "k-1")
	o := NewContinueAsNewOpts("a", "u", "s", "inv-old", "inv-new")
	o.CarriedState = map[string]any{"x": float64(1)}
	o.HasCarriedState = true
	r, err := svc.ContinueAsNew(ctx, o)
	if err != nil {
		t.Fatalf("continue: %v", err)
	}
	if r.EffectsPruned != 1 {
		t.Fatalf("EffectsPruned=%d, want 1", r.EffectsPruned)
	}
	if !r.StateWritten {
		t.Fatalf("StateWritten=false, want true")
	}
	if eff, _ := svc.GetEffect(ctx, "a", "u", "s", key); eff != nil {
		t.Fatalf("effect not pruned: %+v", eff)
	}
	val, _ := svc.GetValue(ctx, "tape:continue-as-new:s", "inv-new")
	if val == nil {
		t.Fatalf("tape_values row missing")
	}
}

func TestContinueAsNewNoPruneWhenPruneOldFalse(t *testing.T) {
	resetCallSeq()
	svc := newTestSvc(t)
	ctx := context.Background()
	key := _confirmedEffect(t, svc, "inv-old", "k-1")
	o := NewContinueAsNewOpts("a", "u", "s", "inv-old", "inv-new")
	o.PruneOld = false
	o.CarriedState = map[string]any{"x": float64(1)}
	o.HasCarriedState = true
	r, err := svc.ContinueAsNew(ctx, o)
	if err != nil {
		t.Fatalf("continue: %v", err)
	}
	if r.EffectsPruned != 0 {
		t.Fatalf("EffectsPruned=%d, want 0", r.EffectsPruned)
	}
	if !r.StateWritten {
		t.Fatalf("StateWritten=false, want true")
	}
	if eff, _ := svc.GetEffect(ctx, "a", "u", "s", key); eff == nil {
		t.Fatalf("effect was pruned with PruneOld=false")
	}
}

// ── idempotency: carrying state twice updates, doesn't duplicate ─────────

func TestContinueAsNewRepeatedUpdatesState(t *testing.T) {
	resetCallSeq()
	svc := newTestSvc(t)
	ctx := context.Background()
	o1 := NewContinueAsNewOpts("a", "u", "s", "inv-1", "inv-2")
	o1.CarriedState = map[string]any{"step": float64(1)}
	o1.HasCarriedState = true
	if _, err := svc.ContinueAsNew(ctx, o1); err != nil {
		t.Fatalf("continue 1: %v", err)
	}
	o2 := NewContinueAsNewOpts("a", "u", "s", "inv-2", "inv-2")
	o2.CarriedState = map[string]any{"step": float64(2)}
	o2.HasCarriedState = true
	if _, err := svc.ContinueAsNew(ctx, o2); err != nil {
		t.Fatalf("continue 2: %v", err)
	}
	val, err := svc.GetValue(ctx, "tape:continue-as-new:s", "inv-2")
	if err != nil || val == nil {
		t.Fatalf("get: val=%v err=%v", val, err)
	}
	got, _ := val.ValueJSON.(map[string]any)
	want := map[string]any{"step": float64(2)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("value=%#v, want %#v", val.ValueJSON, want)
	}
	if val.Version != 2 {
		t.Fatalf("version=%d, want 2", val.Version)
	}
}
