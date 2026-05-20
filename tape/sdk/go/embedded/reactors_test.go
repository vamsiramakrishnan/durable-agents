package embedded

// reactors_test.go — Go port of `tape_adk/tests/test_reactors.py`.
// Mirrors the FakeBank + configurable BankConnector approach: a tiny
// in-memory upstream that dedupes on business_key, wrapped in a
// connector that can inject UNKNOWN, raise once, etc.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// ── a tiny upstream + connector for tests ────────────────────────────────

// fakeBank — models the real bank's contract: dedupe on business_key.
type fakeBank struct {
	mu     sync.Mutex
	ledger map[string]map[string]any // business_key (or composite) → record
}

func newFakeBank() *fakeBank {
	return &fakeBank{ledger: map[string]map[string]any{}}
}

func (b *fakeBank) wire(bk string, amount int, account string) map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	if r, ok := b.ledger[bk]; ok {
		return r
	}
	wid := fmt.Sprintf("wire-%04d", len(b.ledger)+1)
	r := map[string]any{"wire_id": wid, "amount": amount, "account": account, "business_key": bk}
	b.ledger[bk] = r
	return r
}

func (b *fakeBank) find(bk string) map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ledger[bk]
}

func (b *fakeBank) reverse(wireID string) map[string]any {
	return map[string]any{"reversal_id": "rev-of-" + wireID}
}

func (b *fakeBank) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.ledger)
}

// bankConnector — configurable connector around fakeBank.
type bankConnector struct {
	bank              *fakeBank
	NameStr           string
	InjectUnknownOnce bool
	RaiseOnce         bool

	mu           sync.Mutex
	nDispatches  int
	observeOver  func(e EffectRecord) (ObservationResult, error) // optional override
}

func (c *bankConnector) Name() string {
	if c.NameStr == "" {
		return "bank.wire"
	}
	return c.NameStr
}

func (c *bankConnector) Dispatch(_ context.Context, e EffectRecord) (DispatchResult, error) {
	c.mu.Lock()
	c.nDispatches++
	n := c.nDispatches
	c.mu.Unlock()

	req, _ := e.RequestJSON.(map[string]any)
	bk := e.BusinessKey
	// Always write — the call landed; the faults below model what happens AFTER.
	amount := 0
	if v, ok := req["amount"]; ok {
		switch x := v.(type) {
		case int:
			amount = x
		case float64:
			amount = int(x)
		}
	}
	account := "?"
	if v, ok := req["account"].(string); ok {
		account = v
	}
	wire := c.bank.wire(bk, amount, account)

	if c.InjectUnknownOnce && n == 1 {
		return DispatchResult{Status: "unknown",
			Error: map[string]any{"reason": "simulated lost ack"}}, nil
	}
	if c.RaiseOnce && n == 1 {
		return DispatchResult{}, errors.New("simulated transient network error")
	}
	wid := wire["wire_id"].(string)
	return DispatchResult{Status: "confirmed", ExternalRef: wid,
		Response: map[string]any{"wire_id": wid}}, nil
}

func (c *bankConnector) Observe(_ context.Context, e EffectRecord) (ObservationResult, error) {
	if c.observeOver != nil {
		return c.observeOver(e)
	}
	rec := c.bank.find(e.BusinessKey)
	if rec == nil {
		return ObservationResult{Status: "absent"}, nil
	}
	wid := rec["wire_id"].(string)
	return ObservationResult{Status: "confirmed", ExternalRef: wid,
		Response: map[string]any{"wire_id": wid}}, nil
}

func (c *bankConnector) Compensate(_ context.Context, o ObligationRecord) (CompensationResult, error) {
	payload, _ := o.PayloadJSON.(map[string]any)
	wid, _ := payload["external_ref"].(string)
	if wid == "" {
		return CompensationResult{Status: "failed",
			Error: map[string]any{"reason": "no wire_id"}}, nil
	}
	return CompensationResult{Status: "compensated", Response: c.bank.reverse(wid)}, nil
}

// ── the full UNKNOWN → reconcile loop ────────────────────────────────────

func TestFullUnknownReconcileLoop(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	bank := newFakeBank()
	conn := &bankConnector{bank: bank, InjectUnknownOnce: true}
	connectors := map[string]Connector{"bank.wire": conn}

	e, err := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "t", UserID: "u", SessionID: "s", InvocationID: "inv-1",
		ToolName:     "bank.wire",
		RequestJSON:  map[string]any{"amount": 2_000_000, "account": "acct-1"},
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  "acct1:2m:2026-05-18", Connector: "bank.wire",
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if e.Status != EffectStatusPending {
		t.Fatalf("status=%q, want pending", e.Status)
	}

	// Tick 1: outbox dispatches — UNKNOWN.
	r1, err := DispatchOutboxOnce(ctx, svc, connectors, DispatchOutboxOpts{Claimer: "d-1"})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if !anyOutcome(r1, "unknown") {
		t.Fatalf("expected an 'unknown' outcome, got %+v", r1)
	}
	eff, _ := svc.GetEffect(ctx, "t", "u", "s", e.IdempotencyKey)
	if eff.Status != EffectStatusUnknown {
		t.Fatalf("after dispatch: status=%q, want unknown", eff.Status)
	}
	if bank.len() != 1 {
		t.Fatalf("bank.len()=%d, want 1 (call DID land)", bank.len())
	}

	// Tick 2: reconciler observes → CONFIRMED.
	r2, err := ReconcileOnce(ctx, svc, connectors, ReconcileOpts{})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !anyOutcome(r2, "confirmed") {
		t.Fatalf("expected 'confirmed' outcome, got %+v", r2)
	}
	eff, _ = svc.GetEffect(ctx, "t", "u", "s", e.IdempotencyKey)
	if eff.Status != EffectStatusConfirmed {
		t.Fatalf("after reconcile: status=%q, want confirmed", eff.Status)
	}
	if eff.ExternalRef != "wire-0001" {
		t.Fatalf("external_ref=%q, want wire-0001", eff.ExternalRef)
	}
	if bank.len() != 1 {
		t.Fatalf("bank.len()=%d, want 1 (exactly-once held)", bank.len())
	}
}

// ── outbox loop with backoff on generic failure ──────────────────────────

func TestOutboxBacksOffOnGenericException(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	conn := &bankConnector{bank: newFakeBank(), RaiseOnce: true}
	connectors := map[string]Connector{"bank.wire": conn}

	e, _ := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "t", UserID: "u", SessionID: "s", InvocationID: "inv-1",
		ToolName:     "bank.wire",
		RequestJSON:  map[string]any{"amount": 100, "account": "x"},
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  "x:100:2026", Connector: "bank.wire",
	})
	now := time.Now().UnixMilli()
	r1, err := DispatchOutboxOnce(ctx, svc, connectors, DispatchOutboxOpts{
		Claimer: "d-1", DefaultBackoffMs: 10_000,
	})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if !anyOutcome(r1, "exception") {
		t.Fatalf("expected 'exception' outcome, got %+v", r1)
	}
	eff, _ := svc.GetEffect(ctx, "t", "u", "s", e.IdempotencyKey)
	if eff.Status != EffectStatusPending {
		t.Fatalf("status=%q, want still pending", eff.Status)
	}
	if eff.NextDispatchAtMs <= now {
		t.Fatalf("next_dispatch_at_ms=%d, want > %d", eff.NextDispatchAtMs, now)
	}
	if eff.DispatchAttempts != 1 {
		t.Fatalf("attempts=%d, want 1", eff.DispatchAttempts)
	}
}

// ── outbox CAS race ──────────────────────────────────────────────────────

func TestTwoDispatchersDispatchEachEffectAtMostOnce(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	bank := newFakeBank()
	conn := &bankConnector{bank: bank}
	connectors := map[string]Connector{"bank.wire": conn}

	for i := 0; i < 3; i++ {
		if _, err := svc.BeginEffect(ctx, BeginEffectOpts{
			AppName: "t", UserID: "u", SessionID: "s",
			InvocationID: fmt.Sprintf("inv-%d", i),
			ToolName:     "bank.wire",
			RequestJSON:  map[string]any{"amount": i, "account": "x"},
			Semantics:    EffectSemanticsNonIdempotent,
			DispatchMode: EffectDispatchOutbox,
			BusinessKey:  fmt.Sprintf("x:%d:2026", i), Connector: "bank.wire",
		}); err != nil {
			t.Fatalf("begin %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var allResults []ActionLog
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(claimer string) {
			defer wg.Done()
			r, err := DispatchOutboxOnce(ctx, svc, connectors, DispatchOutboxOpts{Claimer: claimer})
			if err != nil {
				t.Errorf("%s: %v", claimer, err)
				return
			}
			mu.Lock()
			allResults = append(allResults, r...)
			mu.Unlock()
		}(fmt.Sprintf("d-%d", i+1))
	}
	wg.Wait()

	confirmedCount := 0
	for _, r := range allResults {
		if r.Outcome == "confirmed" {
			confirmedCount++
		}
	}
	if confirmedCount != 3 {
		t.Fatalf("confirmed=%d, want 3 (one per effect)", confirmedCount)
	}
	if bank.len() != 3 {
		t.Fatalf("bank.len()=%d, want 3", bank.len())
	}
	pending, _ := svc.ListPendingEffects(ctx, ListPendingEffectsOpts{
		IncludePending: true, IncludeUnknown: true,
	})
	if len(pending) != 0 {
		t.Fatalf("pending after dispatch=%d, want 0", len(pending))
	}
}

// ── compensation drain end-to-end ────────────────────────────────────────

func TestDrainCompensatesPendingObligation(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	bank := newFakeBank()
	bank.wire("x:1:2026", 1, "x")
	conn := &bankConnector{bank: bank}
	connectors := map[string]Connector{"bank.wire": conn}

	if _, err := svc.RegisterCompensation(ctx, RegisterCompensationOpts{
		AppName: "t", UserID: "u", SessionID: "s",
		EffectKey: "ek-1", Kind: "bank.wire",
		PayloadJSON: map[string]any{"external_ref": "wire-0001"},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	r, err := DrainObligationsOnce(ctx, svc, connectors, DrainObligationsOpts{Claimer: "dr-1"})
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if !anyOutcome(r, "compensated") {
		t.Fatalf("expected 'compensated' outcome, got %+v", r)
	}
	all, _ := svc.ListObligations(ctx, ListObligationsOpts{
		AppName: "t", UserID: "u", SessionID: "s", OnlyUnresolved: false,
	})
	if len(all) != 1 || all[0].Status != ObligationStatusCompensated {
		t.Fatalf("after drain: %v", all)
	}
}

// ── DUPLICATE flow: reconciler → drainer ─────────────────────────────────

func TestDuplicateFlowRegistersAndDrainsCompensation(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	bank := newFakeBank()
	bank.ledger["x:1:2026"] = map[string]any{"wire_id": "wire-A",
		"business_key": "x:1:2026", "amount": 1, "account": "x"}
	bank.ledger["x:1:2026:dup"] = map[string]any{"wire_id": "wire-B",
		"business_key": "x:1:2026", "amount": 1, "account": "x"}

	conn := &bankConnector{
		bank: bank,
		observeOver: func(e EffectRecord) (ObservationResult, error) {
			return ObservationResult{
				Status: "duplicate", ExternalRef: "wire-B",
				CompensateKind: "bank.wire",
			}, nil
		},
	}
	connectors := map[string]Connector{"bank.wire": conn}

	e, _ := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "t", UserID: "u", SessionID: "s", InvocationID: "inv-1",
		ToolName:     "bank.wire",
		RequestJSON:  map[string]any{"amount": 1, "account": "x"},
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  "x:1:2026", Connector: "bank.wire",
	})
	if _, err := svc.RecordDispatchAttempt(ctx, "t", "u", "s", e.IdempotencyKey,
		"ack lost", 0); err != nil {
		t.Fatalf("record-attempt: %v", err)
	}

	r1, err := ReconcileOnce(ctx, svc, connectors, ReconcileOpts{})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !anyOutcome(r1, "duplicate") {
		t.Fatalf("expected 'duplicate' outcome, got %+v", r1)
	}
	obs, _ := svc.ListObligations(ctx, ListObligationsOpts{AppName: "t", UserID: "u", SessionID: "s"})
	if len(obs) != 1 || obs[0].Kind != "bank.wire" || obs[0].Status != ObligationStatusPending {
		t.Fatalf("obligations: %v", obs)
	}

	r2, err := DrainObligationsOnce(ctx, svc, connectors, DrainObligationsOpts{Claimer: "dr-1"})
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if !anyOutcome(r2, "compensated") {
		t.Fatalf("expected 'compensated' outcome, got %+v", r2)
	}

	eff, _ := svc.GetEffect(ctx, "t", "u", "s", e.IdempotencyKey)
	if eff.Status != EffectStatusConfirmed {
		t.Fatalf("after drain: effect.Status=%q, want confirmed", eff.Status)
	}
}

// ── timer firing ─────────────────────────────────────────────────────────

func TestFireDueTimersInvokesDispatcher(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	var firedMu sync.Mutex
	var fired []string
	dispatcher := func(_ context.Context, tr TimerRecord) error {
		firedMu.Lock()
		fired = append(fired, tr.TimerID)
		firedMu.Unlock()
		return nil
	}
	now := time.Now().UnixMilli()
	if _, err := svc.SetTimer(ctx, SetTimerOpts{
		AppName: "t", UserID: "u", SessionID: "s",
		TimerID: "t-1", FireAtMs: now - 100, Kind: "redrive",
	}); err != nil {
		t.Fatalf("set 1: %v", err)
	}
	if _, err := svc.SetTimer(ctx, SetTimerOpts{
		AppName: "t", UserID: "u", SessionID: "s",
		TimerID: "t-future", FireAtMs: now + 60_000, Kind: "redrive",
	}); err != nil {
		t.Fatalf("set 2: %v", err)
	}
	r, err := FireDueTimersOnce(ctx, svc, FireDueTimersOpts{Dispatcher: dispatcher})
	if err != nil {
		t.Fatalf("fire: %v", err)
	}
	if len(fired) != 1 || fired[0] != "t-1" {
		t.Fatalf("fired=%v, want [t-1]", fired)
	}
	if !anyOutcome(r, "fired") {
		t.Fatalf("expected 'fired' outcome, got %+v", r)
	}
}

// ── helpers ──────────────────────────────────────────────────────────────

func anyOutcome(rs []ActionLog, want string) bool {
	for _, r := range rs {
		if r.Outcome == want {
			return true
		}
	}
	return false
}
