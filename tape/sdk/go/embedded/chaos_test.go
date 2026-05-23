package embedded

// chaos_test.go — Go port of `tape_adk/tests/test_chaos.py`. Same ten
// tests, same invariant proofs, same strict-faults guard. All run
// against an in-memory SQLite via `modernc.org/sqlite`.

import (
	"context"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"
)

// ── a configurable fake ledger connector (test fixture) ──────────────────

// ledgerConnector — a trivial idempotent ledger. CONFIRMED on dispatch;
// mirrors the business_key dedupe a real bank's API provides.
type ledgerConnector struct {
	nameStr string

	mu      sync.Mutex
	ledger  map[string]string // business_key → wire_id
	delayMs int
}

func newLedgerConnector() *ledgerConnector {
	return &ledgerConnector{nameStr: "bank.wire", ledger: map[string]string{}}
}

func (l *ledgerConnector) Name() string { return l.nameStr }

func (l *ledgerConnector) Dispatch(ctx context.Context, e EffectRecord) (DispatchResult, error) {
	if l.delayMs > 0 {
		select {
		case <-time.After(time.Duration(l.delayMs) * time.Millisecond):
		case <-ctx.Done():
			return DispatchResult{}, ctx.Err()
		}
	}
	bk := e.BusinessKey
	if bk == "" {
		bk = e.IdempotencyKey
	}
	l.mu.Lock()
	wid, ok := l.ledger[bk]
	if !ok {
		wid = padWireID(len(l.ledger))
		l.ledger[bk] = wid
	}
	l.mu.Unlock()
	return DispatchResult{Status: "confirmed", ExternalRef: wid,
		Response: map[string]any{"wire_id": wid}}, nil
}

func (l *ledgerConnector) Observe(_ context.Context, e EffectRecord) (ObservationResult, error) {
	bk := e.BusinessKey
	if bk == "" {
		bk = e.IdempotencyKey
	}
	l.mu.Lock()
	wid, ok := l.ledger[bk]
	l.mu.Unlock()
	if ok {
		return ObservationResult{Status: "confirmed", ExternalRef: wid,
			Response: map[string]any{"wire_id": wid}}, nil
	}
	return ObservationResult{Status: "absent"}, nil
}

func (l *ledgerConnector) Compensate(_ context.Context, _ ObligationRecord) (CompensationResult, error) {
	return CompensationResult{Status: "compensated"}, nil
}

func (l *ledgerConnector) ledgerLen() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.ledger)
}

// padWireID — "w-0000", "w-0001", … (same shape as Python).
func padWireID(n int) string {
	digits := []byte{'0', '0', '0', '0'}
	for i := 3; i >= 0 && n > 0; i-- {
		digits[i] = byte('0' + n%10)
		n /= 10
	}
	return "w-" + string(digits)
}

// fixedEffect — synthesize an EffectRecord for unit tests that need to
// drive `ChaosConnector` directly without going through the service.
func fixedEffect(tool, bk string) EffectRecord {
	return EffectRecord{
		AppName: "a", UserID: "u", SessionID: "s",
		IdempotencyKey: "k-" + tool, InvocationID: "inv-1",
		DecisionIndex: 0, ToolName: tool, CallIndex: 0,
		Status: "pending", Semantics: "non_idempotent",
		DispatchMode: "outbox",
		BusinessKey:  bk, Connector: "bank.wire",
	}
}

// ── ChaosConnector: the fault mechanism, in isolation ────────────────────

func TestLoseAckFlipsConfirmedToUnknown(t *testing.T) {
	inner := newLedgerConnector()
	f := MustLoseAck("bank.wire", "", 1.0)
	wrapped := NewChaosConnector(inner, []Fault{f}, rand.New(rand.NewPCG(1, 2)))

	effect := fixedEffect("wire", "bk-1")
	result, err := wrapped.Dispatch(context.Background(), effect)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if result.Status != "unknown" {
		t.Fatalf("status=%q, want unknown", result.Status)
	}
	// The inner call did land (the wrapper's contract).
	if inner.ledgerLen() != 1 {
		t.Fatalf("ledger=%d, want 1 (inner call must land)", inner.ledgerLen())
	}
}

func TestDelayConnectorBlocksDispatch(t *testing.T) {
	inner := newLedgerConnector()
	wrapped := NewChaosConnector(inner,
		[]Fault{DelayConnector("bank.wire", 80, 0)},
		rand.New(rand.NewPCG(1, 2)))

	effect := EffectRecord{
		AppName: "a", UserID: "u", SessionID: "s",
		IdempotencyKey: "k", InvocationID: "inv-1",
		DecisionIndex: 0, ToolName: "wire", CallIndex: 0,
		Status: "pending", Semantics: "idempotent",
		DispatchMode: "inline", Connector: "bank.wire",
	}
	t0 := time.Now()
	if _, err := wrapped.Dispatch(context.Background(), effect); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if d := time.Since(t0); d < 70*time.Millisecond {
		t.Fatalf("elapsed=%v, want >= 70ms (delay honoured)", d)
	}
}

func TestToolScopedFaultOnlyFiresOnMatchingTool(t *testing.T) {
	inner := newLedgerConnector()
	f := MustLoseAck("", "wire", 1.0)
	wrapped := NewChaosConnector(inner, []Fault{f}, rand.New(rand.NewPCG(1, 2)))

	rWire, err := wrapped.Dispatch(context.Background(), fixedEffect("wire", "bk-wire"))
	if err != nil {
		t.Fatalf("dispatch wire: %v", err)
	}
	rPost, err := wrapped.Dispatch(context.Background(), fixedEffect("post_gl", "bk-post"))
	if err != nil {
		t.Fatalf("dispatch post_gl: %v", err)
	}
	if rWire.Status != "unknown" {
		t.Fatalf("wire status=%q, want unknown (tool matches → fault fires)", rWire.Status)
	}
	if rPost.Status != "confirmed" {
		t.Fatalf("post_gl status=%q, want confirmed (tool doesn't match → passthrough)", rPost.Status)
	}
}

// ── strict_faults: the silent-skip false-positive guard ──────────────────

func TestStrictFaultsFailsOnMissingConnector(t *testing.T) {
	scen := Scenario{
		Name:       "missing-target",
		Faults:     []Fault{MustLoseAck("bank.wire", "", 1.0)},
		Invariants: []Invariant{NoStuckObligations},
	}
	svc := newTestSvc(t)
	report, err := RunScenario(context.Background(), scen,
		func(_ context.Context, _ map[string]Connector) error { return nil },
		OpenChaosSessionOpts{Connectors: map[string]Connector{}, Svc: svc})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if report.Passed {
		t.Fatalf("expected FAIL, got pass: %v", report)
	}
	foundStrict := false
	for _, r := range report.InvariantResults {
		if r.Name == "strict_faults" {
			foundStrict = true
		}
	}
	if !foundStrict {
		t.Fatalf("expected strict_faults invariant in report, got %+v", report.InvariantResults)
	}
}

func TestStrictFaultsOffAllowsSkip(t *testing.T) {
	scen := Scenario{
		Name:         "optional-target",
		Faults:       []Fault{MustLoseAck("bank.wire", "", 1.0)},
		Invariants:   []Invariant{NoStuckObligations},
		StrictFaults: StrictFaultsOff(),
	}
	svc := newTestSvc(t)
	report, err := RunScenario(context.Background(), scen, nil,
		OpenChaosSessionOpts{Connectors: map[string]Connector{}, Svc: svc})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !report.Passed {
		t.Fatalf("expected pass with StrictFaults=off, got: %v", report)
	}
	foundNote := false
	for _, n := range report.Notes {
		if strings.Contains(n, "not in `connectors` dict") {
			foundNote = true
		}
	}
	if !foundNote {
		t.Fatalf("expected 'not in connectors dict' note, got %+v", report.Notes)
	}
}

// ── invariants: read the embedded tables ─────────────────────────────────

func TestNoStuckObligationsPassesOnCleanStore(t *testing.T) {
	scen := Scenario{
		Name:       "smoke",
		Invariants: []Invariant{NoStuckObligations},
	}
	svc := newTestSvc(t)
	report, err := RunScenario(context.Background(), scen, nil,
		OpenChaosSessionOpts{Connectors: map[string]Connector{}, Svc: svc})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !report.Passed {
		t.Fatalf("expected pass on clean store, got: %v", report)
	}
}

func TestNoStuckObligationsFailsWhenOneIsStuck(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	ob, err := svc.RegisterCompensation(ctx, RegisterCompensationOpts{
		AppName: "a", UserID: "u", SessionID: "s",
		EffectKey: "ek", Kind: "reverse_wire", MaxAttempts: 1,
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := svc.ResolveObligation(ctx, ob.Seq, ObligationStatusStuck, nil); err != nil {
		t.Fatalf("resolve: %v", err)
	}

	scen := Scenario{Name: "stuck", Invariants: []Invariant{NoStuckObligations}}
	report, err := RunScenario(ctx, scen, nil,
		OpenChaosSessionOpts{Connectors: map[string]Connector{}, Svc: svc})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if report.Passed {
		t.Fatalf("expected FAIL with one stuck obligation, got pass: %v", report)
	}
	foundStuck := false
	for _, r := range report.InvariantResults {
		if strings.Contains(strings.ToLower(r.String()), "stuck") {
			foundStuck = true
		}
	}
	if !foundStuck {
		t.Fatalf("expected an invariant result mentioning 'stuck', got %+v", report.InvariantResults)
	}
}

func TestExactlyOneInvariant(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	e, err := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "a", UserID: "u", SessionID: "s", InvocationID: "inv-1",
		DecisionIndex: 0, ToolName: "wire", CallIndex: 0,
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  "bk-1", Connector: "bank.wire",
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := svc.CompleteEffect(ctx, "a", "u", "s", e.IdempotencyKey,
		EffectStatusConfirmed, map[string]any{"id": "1"}, nil); err != nil {
		t.Fatalf("complete: %v", err)
	}

	scen := Scenario{
		Name:       "one",
		Invariants: []Invariant{MustExactlyOne("bank.wire", "")},
	}
	report, err := RunScenario(ctx, scen, nil,
		OpenChaosSessionOpts{Connectors: map[string]Connector{}, Svc: svc})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !report.Passed {
		t.Fatalf("expected pass with exactly one CONFIRMED, got: %v", report)
	}
}

// ── end-to-end: lose_ack → reconcile loop drives an UNKNOWN to CONFIRMED ─

func TestLoseAckE2EWithReconciler(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	if _, err := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "a", UserID: "u", SessionID: "s", InvocationID: "inv-1",
		DecisionIndex: 0, ToolName: "wire", CallIndex: 0,
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  "bk-1", Connector: "bank.wire",
	}); err != nil {
		t.Fatalf("begin: %v", err)
	}
	bank := newLedgerConnector()

	scen := Scenario{
		Name:   "unknown-then-reconcile",
		Faults: []Fault{MustLoseAck("bank.wire", "", 1.0)},
		Invariants: []Invariant{
			NoStuckObligations,
			MustExactlyOne("bank.wire", ""),
		},
	}

	body := func(ctx context.Context, wrapped map[string]Connector) error {
		// Tick 1: dispatch gets UNKNOWN.
		r1, err := DispatchOutboxOnce(ctx, svc, wrapped, DispatchOutboxOpts{Claimer: "d-1"})
		if err != nil {
			return err
		}
		if !anyOutcome(r1, "unknown") {
			t.Fatalf("expected 'unknown' outcome, got %+v", r1)
		}
		// The bank's ledger has exactly one wire (the inner call landed).
		if bank.ledgerLen() != 1 {
			t.Fatalf("bank=%d, want 1", bank.ledgerLen())
		}
		// Tick 2: reconcile (unwrapped bank → CONFIRMED).
		r2, err := ReconcileOnce(ctx, svc, map[string]Connector{"bank.wire": bank}, ReconcileOpts{})
		if err != nil {
			return err
		}
		if !anyOutcome(r2, "confirmed") {
			t.Fatalf("expected 'confirmed' outcome, got %+v", r2)
		}
		return nil
	}

	report, err := RunScenario(ctx, scen, body, OpenChaosSessionOpts{
		Connectors: map[string]Connector{"bank.wire": bank}, Svc: svc,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !report.Passed {
		t.Fatalf("expected pass, got: %v", report)
	}
	if bank.ledgerLen() != 1 {
		t.Fatalf("bank=%d, want 1 (exactly-once held)", bank.ledgerLen())
	}
}

// ── construction-time refusal of bad fault config ────────────────────────

func TestLoseAckRefusesBothConnectorAndTool(t *testing.T) {
	if _, err := LoseAck("bank.wire", "wire", 1.0); err == nil {
		t.Fatalf("expected error when both connector and tool set")
	}
	if _, err := LoseAck("", "", 1.0); err == nil {
		t.Fatalf("expected error when neither connector nor tool set")
	}
}
