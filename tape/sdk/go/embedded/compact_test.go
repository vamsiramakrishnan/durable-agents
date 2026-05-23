package embedded

// compact_test.go — Go port of `tape_adk/tests/test_compact.py`. Twelve
// tests, all proving SAFETY INVARIANTS rather than DELETE mechanics:
//
//   * a CONFIRMED effect with an unresolved obligation referencing it
//     must NOT be pruned;
//   * a session with a STUCK obligation must NOT be archived;
//   * a session with an unfired timer must NOT be archived;
//   * the compactor is idempotent across ticks.

import (
	"context"
	"testing"
)

// _backdateEffect — direct UPDATE to set ts_ms; the test fixture
// equivalent of "pretend this effect is N seconds old".
func _backdateEffect(t *testing.T, svc *TapeSessionService, key string, ts int64) {
	t.Helper()
	if _, err := svc.db.ExecContext(context.Background(),
		svc.rew(`UPDATE tape_effects SET ts_ms = ? WHERE idempotency_key = ?`),
		ts, key); err != nil {
		t.Fatalf("backdate effect: %v", err)
	}
}

func _backdateObligation(t *testing.T, svc *TapeSessionService, seq int64, ts int64) {
	t.Helper()
	if _, err := svc.db.ExecContext(context.Background(),
		svc.rew(`UPDATE tape_obligations SET ts_ms = ? WHERE seq = ?`),
		ts, seq); err != nil {
		t.Fatalf("backdate obligation: %v", err)
	}
}

// _beginConfirmed — seed a CONFIRMED effect at the given ts_ms (i.e.,
// backdate it after creation).
func _beginConfirmed(
	t *testing.T, svc *TapeSessionService,
	key, invocationID, tool string, ts int64,
) string {
	t.Helper()
	ctx := context.Background()
	e, err := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "a", UserID: "u", SessionID: "s",
		InvocationID: invocationID, DecisionIndex: 0,
		ToolName: tool, CallIndex: 0,
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
	_backdateEffect(t, svc, e.IdempotencyKey, ts)
	return e.IdempotencyKey
}

// ── mechanism 1+2: terminal-state pruning gated by TTL ───────────────────

func TestCompactPrunesOldTerminalEffect(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	key := _beginConfirmed(t, svc, "ek-1", "inv-1", "bank.wire", 1000)
	policy := DefaultCompactionPolicy()
	policy.EffectTTLMs = 1
	r, err := CompactOnce(ctx, svc, policy, 100_000)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if r.EffectsPruned != 1 {
		t.Fatalf("EffectsPruned=%d, want 1", r.EffectsPruned)
	}
	eff, _ := svc.GetEffect(ctx, "a", "u", "s", key)
	if eff != nil {
		t.Fatalf("row still exists: %+v", eff)
	}
}

func TestCompactKeepsFreshTerminalEffect(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	key := _beginConfirmed(t, svc, "ek-1", "inv-1", "bank.wire", 99_999)
	policy := DefaultCompactionPolicy()
	policy.EffectTTLMs = 1_000_000
	r, err := CompactOnce(ctx, svc, policy, 100_000)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if r.EffectsPruned != 0 {
		t.Fatalf("EffectsPruned=%d, want 0", r.EffectsPruned)
	}
	eff, _ := svc.GetEffect(ctx, "a", "u", "s", key)
	if eff == nil {
		t.Fatalf("row missing — should have been kept")
	}
}

func TestCompactKeepsPendingEffectRegardlessOfAge(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	e, err := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "a", UserID: "u", SessionID: "s", InvocationID: "inv-1",
		DecisionIndex: 0, ToolName: "bank.wire", CallIndex: 0,
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  "bk-1", Connector: "bank.wire",
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	_backdateEffect(t, svc, e.IdempotencyKey, 0)
	policy := DefaultCompactionPolicy()
	policy.EffectTTLMs = 1
	r, err := CompactOnce(ctx, svc, policy, 100_000)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if r.EffectsPruned != 0 {
		t.Fatalf("EffectsPruned=%d, want 0 (PENDING is not in terminal set)", r.EffectsPruned)
	}
}

// ── mechanism 5: compensable-window pinning ──────────────────────────────

func TestCompactPinningRefusesToPruneEffectWithActiveObligation(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	key := _beginConfirmed(t, svc, "ek-1", "inv-1", "bank.wire", 1000)
	if _, err := svc.RegisterCompensation(ctx, RegisterCompensationOpts{
		AppName: "a", UserID: "u", SessionID: "s",
		EffectKey: key, Kind: "reverse_wire",
		PayloadJSON: map[string]any{"external_ref": "wire-1"},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	policy := DefaultCompactionPolicy()
	policy.EffectTTLMs = 1
	r, err := CompactOnce(ctx, svc, policy, 100_000)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if r.EffectsPruned != 0 {
		t.Fatalf("EffectsPruned=%d, want 0 (PINNED)", r.EffectsPruned)
	}
	eff, _ := svc.GetEffect(ctx, "a", "u", "s", key)
	if eff == nil {
		t.Fatalf("pinned row vanished")
	}
}

func TestCompactPinningReleasesWhenObligationResolved(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	key := _beginConfirmed(t, svc, "ek-1", "inv-1", "bank.wire", 1000)
	ob, err := svc.RegisterCompensation(ctx, RegisterCompensationOpts{
		AppName: "a", UserID: "u", SessionID: "s",
		EffectKey: key, Kind: "reverse_wire",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := svc.ResolveObligation(ctx, ob.Seq, ObligationStatusCompensated, nil); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	_backdateObligation(t, svc, ob.Seq, 1000)

	policy := DefaultCompactionPolicy()
	policy.EffectTTLMs = 1
	r, err := CompactOnce(ctx, svc, policy, 100_000)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if r.ObligationsPruned != 1 {
		t.Fatalf("ObligationsPruned=%d, want 1", r.ObligationsPruned)
	}
	if r.EffectsPruned != 1 {
		t.Fatalf("EffectsPruned=%d, want 1 (unpinned after obligation gone)", r.EffectsPruned)
	}
}

func TestCompactPinningWithStuckObligationStillPrunes(t *testing.T) {
	// STUCK is NOT in the active-pin set, so the effect IS prunable.
	// This documents that policy choice — see test_compact.py:test_pinning_keeps_effect_with_stuck_obligation.
	svc := newTestSvc(t)
	ctx := context.Background()
	key := _beginConfirmed(t, svc, "ek-1", "inv-1", "bank.wire", 1000)
	ob, err := svc.RegisterCompensation(ctx, RegisterCompensationOpts{
		AppName: "a", UserID: "u", SessionID: "s",
		EffectKey: key, Kind: "reverse_wire",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := svc.ResolveObligation(ctx, ob.Seq, ObligationStatusStuck, nil); err != nil {
		t.Fatalf("resolve stuck: %v", err)
	}
	policy := DefaultCompactionPolicy()
	policy.EffectTTLMs = 1
	r, err := CompactOnce(ctx, svc, policy, 100_000)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if r.EffectsPruned != 1 {
		t.Fatalf("EffectsPruned=%d, want 1 (STUCK doesn't pin)", r.EffectsPruned)
	}
	// And the obligation itself stays (STUCK is not in the
	// archive_terminal_obligations target — only COMPENSATED is).
	obs, _ := svc.ListObligations(ctx, ListObligationsOpts{
		AppName: "a", UserID: "u", SessionID: "s",
	})
	if len(obs) != 1 || obs[0].Status != ObligationStatusStuck {
		t.Fatalf("expected one STUCK obligation kept, got %+v", obs)
	}
}

// ── obligation archival ─────────────────────────────────────────────────

func TestCompactPrunesOldCompensatedObligation(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	ob, err := svc.RegisterCompensation(ctx, RegisterCompensationOpts{
		AppName: "a", UserID: "u", SessionID: "s",
		EffectKey: "ek-orphan", Kind: "reverse_wire",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := svc.ResolveObligation(ctx, ob.Seq, ObligationStatusCompensated, nil); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	_backdateObligation(t, svc, ob.Seq, 1000)
	policy := DefaultCompactionPolicy()
	policy.EffectTTLMs = 1
	r, err := CompactOnce(ctx, svc, policy, 100_000)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if r.ObligationsPruned != 1 {
		t.Fatalf("ObligationsPruned=%d, want 1", r.ObligationsPruned)
	}
}

func TestCompactKeepsStuckObligationRegardlessOfAge(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	ob, err := svc.RegisterCompensation(ctx, RegisterCompensationOpts{
		AppName: "a", UserID: "u", SessionID: "s",
		EffectKey: "ek", Kind: "reverse_wire",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := svc.ResolveObligation(ctx, ob.Seq, ObligationStatusStuck, nil); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	_backdateObligation(t, svc, ob.Seq, 0)
	policy := DefaultCompactionPolicy()
	policy.EffectTTLMs = 1
	r, err := CompactOnce(ctx, svc, policy, 100_000)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if r.ObligationsPruned != 0 {
		t.Fatalf("ObligationsPruned=%d, want 0 (STUCK never pruned)", r.ObligationsPruned)
	}
}

// ── session archival (mechanism 3) ──────────────────────────────────────

func TestCompactArchivesIdleTerminalSession(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_beginConfirmed(t, svc, "k-"+itoa(i), "inv-"+itoa(i), "bank.wire", 1000)
	}
	policy := DefaultCompactionPolicy()
	policy.EffectTTLMs = 1
	policy.SessionTTLMs = 1
	r, err := CompactOnce(ctx, svc, policy, 100_000)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if r.SessionsArchived != 1 {
		t.Fatalf("SessionsArchived=%d, want 1", r.SessionsArchived)
	}
}

func TestCompactDoesNotArchiveSessionWithActiveObligation(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	_beginConfirmed(t, svc, "k-0", "inv-0", "bank.wire", 1000)
	if _, err := svc.RegisterCompensation(ctx, RegisterCompensationOpts{
		AppName: "a", UserID: "u", SessionID: "s",
		EffectKey: "k-orphan", Kind: "reverse_wire",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	policy := DefaultCompactionPolicy()
	policy.EffectTTLMs = 1
	policy.SessionTTLMs = 1
	r, err := CompactOnce(ctx, svc, policy, 100_000)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if r.SessionsArchived != 0 {
		t.Fatalf("SessionsArchived=%d, want 0 (active obligation pins session)", r.SessionsArchived)
	}
}

func TestCompactDoesNotArchiveSessionWithUnfiredTimer(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	_beginConfirmed(t, svc, "k-0", "inv-0", "bank.wire", 1000)
	if _, err := svc.SetTimer(ctx, SetTimerOpts{
		AppName: "a", UserID: "u", SessionID: "s",
		TimerID: "redrive-1", FireAtMs: 99_999_999, Kind: "redrive",
	}); err != nil {
		t.Fatalf("set timer: %v", err)
	}
	policy := DefaultCompactionPolicy()
	policy.EffectTTLMs = 1
	policy.SessionTTLMs = 1
	r, err := CompactOnce(ctx, svc, policy, 100_000)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if r.SessionsArchived != 0 {
		t.Fatalf("SessionsArchived=%d, want 0 (unfired timer pins session)", r.SessionsArchived)
	}
}

// ── idempotency: running compaction twice == once ───────────────────────

func TestCompactIsIdempotentAcrossTicks(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_beginConfirmed(t, svc, "k-"+itoa(i), "inv-"+itoa(i), "bank.wire", 1000)
	}
	policy := DefaultCompactionPolicy()
	policy.EffectTTLMs = 1
	policy.SessionTTLMs = 1
	r1, err := CompactOnce(ctx, svc, policy, 100_000)
	if err != nil {
		t.Fatalf("compact 1: %v", err)
	}
	r2, err := CompactOnce(ctx, svc, policy, 100_000)
	if err != nil {
		t.Fatalf("compact 2: %v", err)
	}
	if r1.Total() == 0 {
		t.Fatalf("first tick total=0, want > 0")
	}
	if r2.Total() != 0 {
		t.Fatalf("second tick total=%d, want 0 (idempotent)", r2.Total())
	}
}

// ── helpers ─────────────────────────────────────────────────────────────

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
