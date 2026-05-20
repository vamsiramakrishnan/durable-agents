package embedded

// service_test.go — the Go port of `tape_adk/tests/test_service.py`.
// Same invariants, same ordering, same assertions.

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// newTestSvc — fresh in-memory DB per test. The `?cache=shared` +
// distinct DSN per test keeps tests independent while still allowing
// multiple connections to see the same DB (matches the Python fixture's
// behaviour against ADK's StaticPool).
func newTestSvc(t *testing.T) *TapeSessionService {
	t.Helper()
	// Unique DSN per test so two parallel tests don't share state. Using
	// `file:<name>?mode=memory&cache=shared` so multiple connections
	// within ONE test still see the same DB.
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	dsn = strings.ReplaceAll(dsn, "/", "_")
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Keep the connection pool small to surface the shared-connection
	// CAS hazard the in-process mutex is supposed to handle.
	db.SetMaxOpenConns(4)
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	if err := CreateAllTables(ctx, db, CreateAllOpts{
		Dialect:          DialectSQLite,
		WithoutSessionFK: true, // no ADK sessions table in the test DB
	}); err != nil {
		t.Fatalf("create tables: %v", err)
	}
	return NewTapeSessionService(ctx, db, DialectSQLite)
}

// ── effect ledger basics ─────────────────────────────────────────────────

func TestBeginEffectIsIdempotentOnKey(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()

	o := BeginEffectOpts{
		AppName: "t", UserID: "u", SessionID: "s", InvocationID: "inv-1",
		DecisionIndex: 0, ToolName: "bank.wire", CallIndex: 0,
		RequestJSON: map[string]any{"amount": 2_000_000},
	}
	e1, err := svc.BeginEffect(ctx, o)
	if err != nil {
		t.Fatalf("begin 1: %v", err)
	}
	e2, err := svc.BeginEffect(ctx, o)
	if err != nil {
		t.Fatalf("begin 2: %v", err)
	}
	if e1.IdempotencyKey != e2.IdempotencyKey {
		t.Fatalf("keys differ: %q vs %q", e1.IdempotencyKey, e2.IdempotencyKey)
	}
	if e2.Status != EffectStatusPending {
		t.Fatalf("e2.Status=%q, want pending", e2.Status)
	}
	pending, err := svc.ListPendingEffects(ctx, ListPendingEffectsOpts{
		IncludePending: true, IncludeUnknown: true,
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("len(pending)=%d, want 1", len(pending))
	}
}

func TestCompleteEffectIsTerminalIdempotent(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	e, _ := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "t", UserID: "u", SessionID: "s", InvocationID: "inv-1",
		ToolName: "bank.wire",
	})
	r1, err := svc.CompleteEffect(ctx, "t", "u", "s", e.IdempotencyKey,
		EffectStatusConfirmed, map[string]any{"wire_id": "w-1"}, nil)
	if err != nil {
		t.Fatalf("complete 1: %v", err)
	}
	if r1.Status != EffectStatusConfirmed {
		t.Fatalf("r1.Status=%q, want confirmed", r1.Status)
	}
	// Second call: should NOT overwrite.
	r2, err := svc.CompleteEffect(ctx, "t", "u", "s", e.IdempotencyKey,
		EffectStatusFailed, nil, map[string]any{"err": "x"})
	if err != nil {
		t.Fatalf("complete 2: %v", err)
	}
	if r2.Status != EffectStatusConfirmed {
		t.Fatalf("r2.Status=%q, want still confirmed", r2.Status)
	}
	m, ok := r2.ResponseJSON.(map[string]any)
	if !ok || m["wire_id"] != "w-1" {
		t.Fatalf("response not preserved: %#v", r2.ResponseJSON)
	}
}

// ── load-bearing safety invariants ───────────────────────────────────────

func TestNonIdempotentInlineIsRefused(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	_, err := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "t", UserID: "u", SessionID: "s", InvocationID: "inv-1",
		ToolName:     "bank.wire",
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchInline,
	})
	if err == nil || !strings.Contains(err.Error(), "NON_IDEMPOTENT") {
		t.Fatalf("expected NON_IDEMPOTENT refusal, got: %v", err)
	}
}

func TestOutboxWithoutConnectorIsRefused(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	_, err := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "t", UserID: "u", SessionID: "s", InvocationID: "inv-1",
		ToolName:     "bank.wire",
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
	})
	if err == nil || !strings.Contains(err.Error(), "OUTBOX") || !strings.Contains(err.Error(), "Connector") {
		t.Fatalf("expected OUTBOX/Connector refusal, got: %v", err)
	}
}

func TestBusinessKeyDedupAcrossRuns(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	bk := "acct1:2m:2026-05-18"
	if _, err := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "t", UserID: "u", SessionID: "s1", InvocationID: "inv-1",
		ToolName:     "bank.wire",
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  bk, Connector: "bank.wire",
	}); err != nil {
		t.Fatalf("begin 1: %v", err)
	}
	_, err := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "t", UserID: "u", SessionID: "s2", InvocationID: "inv-2",
		ToolName:     "bank.wire",
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  bk, Connector: "bank.wire",
	})
	if err == nil || !strings.Contains(err.Error(), "business_key already exists") {
		t.Fatalf("expected UNIQUE violation, got: %v", err)
	}
}

// ── CAS lease — the primitive ADK can't express ─────────────────────────

func TestClaimEffectDispatchSingleWinner(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	e, err := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "t", UserID: "u", SessionID: "s", InvocationID: "inv-1",
		ToolName:     "bank.wire",
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  "acct:2m:2026", Connector: "bank.wire",
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	const N = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	var acquired int
	var winners []string
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			claimer := "dispatcher-" + string(rune('A'+i))
			got, _, err := svc.ClaimEffectDispatch(ctx,
				"t", "u", "s", e.IdempotencyKey, claimer, 60_000, 0)
			if err != nil {
				t.Errorf("claim %d: %v", i, err)
				return
			}
			if got {
				mu.Lock()
				acquired++
				winners = append(winners, claimer)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if acquired != 1 {
		t.Fatalf("acquired=%d, want exactly 1 winner; winners=%v", acquired, winners)
	}
}

func TestExpiredDispatchLeaseIsReclaimable(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	e, _ := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "t", UserID: "u", SessionID: "s", InvocationID: "inv-1",
		ToolName:     "bank.wire",
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  "acct:2m:2026", Connector: "bank.wire",
	})
	acq, _, err := svc.ClaimEffectDispatch(ctx, "t", "u", "s", e.IdempotencyKey, "A", 1, 0)
	if err != nil || !acq {
		t.Fatalf("first claim: acq=%v err=%v", acq, err)
	}
	future := time.Now().UnixMilli() + 1000
	acq2, eff, err := svc.ClaimEffectDispatch(ctx, "t", "u", "s", e.IdempotencyKey, "B", 60_000, future)
	if err != nil || !acq2 {
		t.Fatalf("second claim: acq=%v err=%v", acq2, err)
	}
	if eff == nil || eff.DispatchClaimedBy != "B" {
		t.Fatalf("dispatch_claimed_by=%q, want B", eff.DispatchClaimedBy)
	}
}

// ── UNKNOWN transition ──────────────────────────────────────────────────

func TestDispatchAttemptZeroTransitionsToUnknown(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	e, _ := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "t", UserID: "u", SessionID: "s", InvocationID: "inv-1",
		ToolName:     "bank.wire",
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  "acct:2m:2026", Connector: "bank.wire",
	})
	r, err := svc.RecordDispatchAttempt(ctx, "t", "u", "s", e.IdempotencyKey,
		"simulated lost ack", 0)
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	if r.Status != EffectStatusUnknown {
		t.Fatalf("status=%q, want unknown", r.Status)
	}
	if r.DispatchAttempts != 1 {
		t.Fatalf("attempts=%d, want 1", r.DispatchAttempts)
	}
	if r.DispatchClaimedBy != "" {
		t.Fatalf("dispatch_claimed_by=%q, want empty", r.DispatchClaimedBy)
	}
	unknowns, err := svc.ListPendingEffects(ctx, ListPendingEffectsOpts{
		IncludePending: false, IncludeUnknown: true,
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(unknowns) != 1 || unknowns[0].Status != EffectStatusUnknown {
		t.Fatalf("unknowns=%v", unknowns)
	}
}

// ── reconciler write path ───────────────────────────────────────────────

func TestExternalObservationConfirmedResolvesUnknown(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	e, _ := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "t", UserID: "u", SessionID: "s", InvocationID: "inv-1",
		ToolName:     "bank.wire",
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  "acct:2m:2026", Connector: "bank.wire",
	})
	if _, err := svc.RecordDispatchAttempt(ctx, "t", "u", "s", e.IdempotencyKey,
		"ack lost", 0); err != nil {
		t.Fatalf("record-attempt: %v", err)
	}
	r, err := svc.RecordExternalObservation(ctx, RecordExternalObservationOpts{
		AppName: "t", UserID: "u", SessionID: "s",
		IdempotencyKey: e.IdempotencyKey,
		Resolution:     EffectResolutionConfirmed,
		ExternalRef:    "wire-0001",
		ResponseJSON:   map[string]any{"wire_id": "wire-0001"},
	})
	if err != nil {
		t.Fatalf("observe: %v", err)
	}
	if r.Status != EffectStatusConfirmed {
		t.Fatalf("status=%q, want confirmed", r.Status)
	}
	if r.ExternalRef != "wire-0001" {
		t.Fatalf("external_ref=%q, want wire-0001", r.ExternalRef)
	}
}

func TestDuplicateObservationAtomicallyRegistersCompensation(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	e, _ := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "t", UserID: "u", SessionID: "s", InvocationID: "inv-1",
		ToolName:     "bank.wire",
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  "acct:2m:2026", Connector: "bank.wire",
	})
	if _, err := svc.RecordDispatchAttempt(ctx, "t", "u", "s", e.IdempotencyKey,
		"ack lost", 0); err != nil {
		t.Fatalf("record-attempt: %v", err)
	}
	r, err := svc.RecordExternalObservation(ctx, RecordExternalObservationOpts{
		AppName: "t", UserID: "u", SessionID: "s",
		IdempotencyKey:            e.IdempotencyKey,
		Resolution:                EffectResolutionDuplicate,
		ExternalRef:               "wire-A",
		CompensateOnDuplicateKind: "reverse_wire",
	})
	if err != nil {
		t.Fatalf("observe: %v", err)
	}
	if r.Status != EffectStatusConfirmed {
		t.Fatalf("status=%q, want confirmed", r.Status)
	}
	obs, err := svc.ListObligations(ctx, ListObligationsOpts{AppName: "t", UserID: "u", SessionID: "s"})
	if err != nil {
		t.Fatalf("list-obligations: %v", err)
	}
	if len(obs) != 1 {
		t.Fatalf("obligations=%d, want 1", len(obs))
	}
	if obs[0].Kind != "reverse_wire" {
		t.Fatalf("kind=%q, want reverse_wire", obs[0].Kind)
	}
	if obs[0].Status != ObligationStatusPending {
		t.Fatalf("ob.status=%q, want pending", obs[0].Status)
	}
	if obs[0].EffectKey != e.IdempotencyKey {
		t.Fatalf("ob.effect_key=%q, want %q", obs[0].EffectKey, e.IdempotencyKey)
	}
}

func TestAbsentForNonIdempotentStaysUnknown(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	e, _ := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "t", UserID: "u", SessionID: "s", InvocationID: "inv-1",
		ToolName:     "bank.wire",
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  "acct:2m:2026", Connector: "bank.wire",
	})
	if _, err := svc.RecordDispatchAttempt(ctx, "t", "u", "s", e.IdempotencyKey,
		"ack lost", 0); err != nil {
		t.Fatalf("record-attempt: %v", err)
	}
	r, err := svc.RecordExternalObservation(ctx, RecordExternalObservationOpts{
		AppName: "t", UserID: "u", SessionID: "s",
		IdempotencyKey: e.IdempotencyKey,
		Resolution:     EffectResolutionAbsent,
	})
	if err != nil {
		t.Fatalf("observe: %v", err)
	}
	if r.Status != EffectStatusUnknown {
		t.Fatalf("status=%q, want unknown", r.Status)
	}
}

// ── obligation ledger ───────────────────────────────────────────────────

func TestRegisterCompensationIsIdempotent(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	o1, err := svc.RegisterCompensation(ctx, RegisterCompensationOpts{
		AppName: "t", UserID: "u", SessionID: "s",
		EffectKey: "ek-1", Kind: "reverse_wire",
		PayloadJSON: map[string]any{"amount": 1},
	})
	if err != nil {
		t.Fatalf("register 1: %v", err)
	}
	o2, err := svc.RegisterCompensation(ctx, RegisterCompensationOpts{
		AppName: "t", UserID: "u", SessionID: "s",
		EffectKey: "ek-1", Kind: "reverse_wire",
		PayloadJSON: map[string]any{"amount": 2},
	})
	if err != nil {
		t.Fatalf("register 2: %v", err)
	}
	if o1.Seq != o2.Seq {
		t.Fatalf("seq differ: %d vs %d", o1.Seq, o2.Seq)
	}
	all, _ := svc.ListObligations(ctx, ListObligationsOpts{AppName: "t", UserID: "u", SessionID: "s"})
	if len(all) != 1 {
		t.Fatalf("obligations=%d, want 1", len(all))
	}
}

func TestClaimObligationSingleWinner(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	o, _ := svc.RegisterCompensation(ctx, RegisterCompensationOpts{
		AppName: "t", UserID: "u", SessionID: "s",
		EffectKey: "ek-1", Kind: "reverse_wire",
	})
	const N = 6
	var wg sync.WaitGroup
	var mu sync.Mutex
	var acquired int
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got, _, err := svc.ClaimObligation(ctx, o.Seq, string(rune('A'+i)), 60_000, 0)
			if err != nil {
				t.Errorf("claim %d: %v", i, err)
				return
			}
			if got {
				mu.Lock()
				acquired++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if acquired != 1 {
		t.Fatalf("acquired=%d, want 1", acquired)
	}
}

func TestRecordObligationAttemptRetriesThenStucks(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	o, _ := svc.RegisterCompensation(ctx, RegisterCompensationOpts{
		AppName: "t", UserID: "u", SessionID: "s",
		EffectKey: "ek-1", Kind: "reverse_wire", MaxAttempts: 3,
	})
	future := time.Now().UnixMilli() + 10_000

	r1, err := svc.RecordObligationAttempt(ctx, o.Seq, "boom", future)
	if err != nil {
		t.Fatalf("attempt 1: %v", err)
	}
	if r1.Status != ObligationStatusPending || r1.Attempts != 1 {
		t.Fatalf("after 1: status=%q attempts=%d", r1.Status, r1.Attempts)
	}
	r2, _ := svc.RecordObligationAttempt(ctx, o.Seq, "boom again", future)
	if r2.Status != ObligationStatusPending || r2.Attempts != 2 {
		t.Fatalf("after 2: status=%q attempts=%d", r2.Status, r2.Attempts)
	}
	r3, _ := svc.RecordObligationAttempt(ctx, o.Seq, "boom 3", future)
	if r3.Status != ObligationStatusStuck || r3.Attempts != 3 {
		t.Fatalf("after 3: status=%q attempts=%d", r3.Status, r3.Attempts)
	}
}

func TestTerminalNowAttemptForcesStuck(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	o, _ := svc.RegisterCompensation(ctx, RegisterCompensationOpts{
		AppName: "t", UserID: "u", SessionID: "s",
		EffectKey: "ek-1", Kind: "reverse_wire", MaxAttempts: 10,
	})
	r, err := svc.RecordObligationAttempt(ctx, o.Seq, "business rule says no", 0)
	if err != nil {
		t.Fatalf("attempt: %v", err)
	}
	if r.Status != ObligationStatusStuck {
		t.Fatalf("status=%q, want stuck", r.Status)
	}
	if r.Attempts != 1 {
		t.Fatalf("attempts=%d, want 1", r.Attempts)
	}
}

// ── timers ──────────────────────────────────────────────────────────────

func TestSetTimerIdempotentOnTimerID(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	t1, _ := svc.SetTimer(ctx, SetTimerOpts{
		AppName: "t", UserID: "u", SessionID: "s",
		TimerID: "redrive-1", FireAtMs: 12345, Kind: "redrive",
	})
	t2, _ := svc.SetTimer(ctx, SetTimerOpts{
		AppName: "t", UserID: "u", SessionID: "s",
		TimerID: "redrive-1", FireAtMs: 99999, Kind: "redrive",
	})
	if t1.FireAtMs != t2.FireAtMs {
		t.Fatalf("fire_at_ms differ: %d vs %d (should be idempotent on timer_id)", t1.FireAtMs, t2.FireAtMs)
	}
}

func TestListDueTimersClaimMarksFired(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	now := time.Now().UnixMilli()
	_, _ = svc.SetTimer(ctx, SetTimerOpts{
		AppName: "t", UserID: "u", SessionID: "s",
		TimerID: "due-1", FireAtMs: now - 1000, Kind: "redrive",
	})
	_, _ = svc.SetTimer(ctx, SetTimerOpts{
		AppName: "t", UserID: "u", SessionID: "s",
		TimerID: "future-1", FireAtMs: now + 60_000, Kind: "redrive",
	})
	due, err := svc.ListDueTimers(ctx, ListDueTimersOpts{NowMs: now, Claim: true})
	if err != nil {
		t.Fatalf("list 1: %v", err)
	}
	if len(due) != 1 || due[0].TimerID != "due-1" {
		t.Fatalf("due=%v", due)
	}
	due2, err := svc.ListDueTimers(ctx, ListDueTimersOpts{NowMs: now, Claim: false})
	if err != nil {
		t.Fatalf("list 2: %v", err)
	}
	if len(due2) != 0 {
		t.Fatalf("second call should see none, got %d", len(due2))
	}
}

// ── reactive KV ─────────────────────────────────────────────────────────

func TestWriteValueCAS(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	v1, err := svc.WriteValue(ctx, "treasury", "fx_rate", map[string]any{"USD": 1.0}, 0, "")
	if err != nil {
		t.Fatalf("write 1: %v", err)
	}
	if v1.Version != 1 {
		t.Fatalf("v1.Version=%d, want 1", v1.Version)
	}
	v2, err := svc.WriteValue(ctx, "treasury", "fx_rate", map[string]any{"USD": 1.01}, 1, "")
	if err != nil {
		t.Fatalf("write 2: %v", err)
	}
	if v2.Version != 2 {
		t.Fatalf("v2.Version=%d, want 2", v2.Version)
	}
	_, err = svc.WriteValue(ctx, "treasury", "fx_rate", map[string]any{"USD": 1.02}, 1, "")
	if err == nil || !strings.Contains(err.Error(), "stale CAS") {
		t.Fatalf("expected stale CAS error, got: %v", err)
	}
}

// ── cross-session list queries ─────────────────────────────────────────

func TestListUnresolvedObligationsIncludesPendingAndCommittedExpired(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	o1, _ := svc.RegisterCompensation(ctx, RegisterCompensationOpts{
		AppName: "t", UserID: "u", SessionID: "s1",
		EffectKey: "ek-1", Kind: "reverse_wire",
	})
	if _, _, err := svc.ClaimObligation(ctx, o1.Seq, "A", 1, 0); err != nil {
		t.Fatalf("claim: %v", err)
	}
	future := time.Now().UnixMilli() + 1000
	rows, err := svc.ListUnresolvedObligations(ctx, ListUnresolvedObligationsOpts{
		NowMs: future, IncludePending: true, IncludeCommittedExpired: true,
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	found := false
	for _, o := range rows {
		if o.Seq == o1.Seq {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected seq %d in list, got %v", o1.Seq, rows)
	}
}

// ── decorator construction-time refusal ────────────────────────────────

func TestOutboxToolRefusesMissingConnector(t *testing.T) {
	fn := func(args map[string]any) (any, error) { return nil, nil }
	_, err := OutboxTool(fn, OutboxToolOpts{
		BusinessKeyStatic: "x",
	})
	if err == nil || !strings.Contains(err.Error(), "Connector") {
		t.Fatalf("expected Connector refusal, got: %v", err)
	}
}

func TestOutboxToolRefusesMissingBusinessKey(t *testing.T) {
	fn := func(args map[string]any) (any, error) { return nil, nil }
	_, err := OutboxTool(fn, OutboxToolOpts{
		Connector: "bank.wire",
	})
	if err == nil || !strings.Contains(err.Error(), "BusinessKey") {
		t.Fatalf("expected BusinessKey refusal, got: %v", err)
	}
}

func TestEffectIsOK(t *testing.T) {
	fn := func(args map[string]any) (any, error) { return nil, nil }
	tool := Effect(fn)
	m := MetaOf(tool)
	if m.Semantics != EffectSemanticsIdempotent {
		t.Fatalf("semantics=%q, want idempotent", m.Semantics)
	}
	if m.DispatchMode != EffectDispatchInline {
		t.Fatalf("dispatch_mode=%q, want inline", m.DispatchMode)
	}
}
