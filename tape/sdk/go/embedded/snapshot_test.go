package embedded

// snapshot_test.go — Go port of `tape_adk/tests/test_snapshot.py`. Nine
// tests, same invariants:
//
//   * `TakeSnapshot` captures terminal effects (CONFIRMED + FAILED) and
//     excludes non-terminal (PENDING + UNKNOWN);
//   * repeat snapshots merge cumulatively, last-write-wins per key;
//   * `BeginEffect` falls back to the snapshot when the live row has
//     been pruned (the LOAD-BEARING test — this is the whole point);
//   * `BeginEffect` prefers the live row over a disagreeing snapshot;
//   * snapshot + compactor cooperate: snapshot, then prune, still
//     short-circuits;
//   * watermark bounds the read window;
//   * graceful handling of "no terminal effects yet" and "no snapshot
//     yet".

import (
	"context"
	"testing"
)

// _snapConfirmedEffect — seed a CONFIRMED outbox effect under the test
// session. The derived idempotency_key is uniquely pinned by
// `callIndex`. Returns the key for later assertions.
func _snapConfirmedEffect(
	t *testing.T, svc *TapeSessionService,
	key string, response map[string]any, callIndex int,
) string {
	t.Helper()
	ctx := context.Background()
	e, err := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "a", UserID: "u", SessionID: "s",
		InvocationID: "inv-1", DecisionIndex: 0,
		ToolName: "bank.wire", CallIndex: callIndex,
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  key, Connector: "bank.wire",
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := svc.CompleteEffect(ctx, "a", "u", "s", e.IdempotencyKey,
		EffectStatusConfirmed, response, nil); err != nil {
		t.Fatalf("complete: %v", err)
	}
	return e.IdempotencyKey
}

// ── basic capture/exclude ────────────────────────────────────────────────

func TestTakeSnapshot_CapturesTerminalEffects(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	k1 := _snapConfirmedEffect(t, svc, "k1", map[string]any{"id": "wire-1"}, 0)
	k2 := _snapConfirmedEffect(t, svc, "k2", map[string]any{"id": "wire-2"}, 1)

	r, err := svc.TakeSnapshot(ctx, TakeSnapshotOpts{
		AppName: "a", UserID: "u", SessionID: "s",
	})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if r.Captured != 2 {
		t.Fatalf("Captured=%d, want 2", r.Captured)
	}
	if r.MergedTotal != 2 {
		t.Fatalf("MergedTotal=%d, want 2", r.MergedTotal)
	}

	snap, err := svc.GetSnapshot(ctx, GetSnapshotOpts{
		AppName: "a", UserID: "u", SessionID: "s",
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if snap == nil {
		t.Fatalf("snapshot row missing")
	}
	if len(snap.EffectsJSON) != 2 {
		t.Fatalf("keys=%d, want 2", len(snap.EffectsJSON))
	}
	if _, ok := snap.EffectsJSON[k1]; !ok {
		t.Fatalf("k1 missing from snapshot")
	}
	if _, ok := snap.EffectsJSON[k2]; !ok {
		t.Fatalf("k2 missing from snapshot")
	}
	// k1's captured response_json.id must equal "wire-1".
	got := snap.EffectsJSON[k1]["response_json"]
	if m, ok := got.(map[string]any); !ok || m["id"] != "wire-1" {
		t.Fatalf("k1 response_json=%#v, want {id: wire-1}", got)
	}
	// k2's status must be CONFIRMED.
	if snap.EffectsJSON[k2]["status"] != EffectStatusConfirmed {
		t.Fatalf("k2 status=%v, want confirmed", snap.EffectsJSON[k2]["status"])
	}
}

func TestTakeSnapshot_ExcludesNonTerminalEffects(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	// PENDING — never completed.
	if _, err := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "a", UserID: "u", SessionID: "s",
		InvocationID: "inv-1", DecisionIndex: 0,
		ToolName: "bank.wire", CallIndex: 0,
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  "k-pending", Connector: "bank.wire",
	}); err != nil {
		t.Fatalf("begin: %v", err)
	}
	r, err := svc.TakeSnapshot(ctx, TakeSnapshotOpts{
		AppName: "a", UserID: "u", SessionID: "s",
	})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if r.Captured != 0 {
		t.Fatalf("Captured=%d, want 0 (PENDING is not terminal)", r.Captured)
	}
}

// ── cumulative merge ─────────────────────────────────────────────────────

func TestRepeatedSnapshot_IsCumulative(t *testing.T) {
	svc := newTestSvc(t)
	ctx := context.Background()
	k1 := _snapConfirmedEffect(t, svc, "k1", map[string]any{"v": 1}, 0)
	r1, err := svc.TakeSnapshot(ctx, TakeSnapshotOpts{
		AppName: "a", UserID: "u", SessionID: "s",
	})
	if err != nil {
		t.Fatalf("snapshot 1: %v", err)
	}
	if r1.MergedTotal != 1 {
		t.Fatalf("r1.MergedTotal=%d, want 1", r1.MergedTotal)
	}

	k2 := _snapConfirmedEffect(t, svc, "k2", map[string]any{"v": 2}, 1)
	r2, err := svc.TakeSnapshot(ctx, TakeSnapshotOpts{
		AppName: "a", UserID: "u", SessionID: "s",
	})
	if err != nil {
		t.Fatalf("snapshot 2: %v", err)
	}
	// Both rows still terminal, both captured (Python semantics:
	// `captured` is the size of the read this call, not the delta).
	if r2.Captured != 2 {
		t.Fatalf("r2.Captured=%d, want 2", r2.Captured)
	}
	if r2.MergedTotal != 2 {
		t.Fatalf("r2.MergedTotal=%d, want 2", r2.MergedTotal)
	}

	snap, err := svc.GetSnapshot(ctx, GetSnapshotOpts{
		AppName: "a", UserID: "u", SessionID: "s",
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, ok := snap.EffectsJSON[k1]; !ok {
		t.Fatalf("k1 dropped from cumulative snapshot")
	}
	if _, ok := snap.EffectsJSON[k2]; !ok {
		t.Fatalf("k2 missing from cumulative snapshot")
	}
}

// ── the load-bearing invariant: short-circuit survives row deletion ─────

func TestBeginEffect_ShortCircuitsViaSnapshotAfterRowPruned(t *testing.T) {
	// The whole point. Snapshot the effect, brute-force DELETE the live
	// row (simulating the compactor having pruned it), and verify
	// `BeginEffect` with the same derived key returns the snapshot data
	// instead of creating a fresh PENDING row.
	//
	// If this test fails the compactor can break the idempotency
	// contract — the bug the snapshot exists to prevent.
	svc := newTestSvc(t)
	ctx := context.Background()
	k := _snapConfirmedEffect(t, svc, "k1", map[string]any{"id": "wire-1"}, 0)
	if _, err := svc.TakeSnapshot(ctx, TakeSnapshotOpts{
		AppName: "a", UserID: "u", SessionID: "s",
	}); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Brute-force delete the live row (no compactor TTL nonsense — we
	// want to test the fallback path, not the policy).
	if _, err := svc.db.ExecContext(ctx,
		svc.rew(`DELETE FROM tape_effects WHERE idempotency_key = ?`),
		k); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Sanity: the live row really is gone.
	if eff, _ := svc.GetEffect(ctx, "a", "u", "s", k); eff != nil {
		t.Fatalf("DELETE didn't take: %+v", eff)
	}

	// Now `BeginEffect` with the same (invocation, decision, tool,
	// call_index) — which derives to the same idempotency_key — should
	// NOT create a new PENDING row. It should return the snapshot's
	// captured CONFIRMED record.
	e, err := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "a", UserID: "u", SessionID: "s",
		InvocationID: "inv-1", DecisionIndex: 0,
		ToolName: "bank.wire", CallIndex: 0,
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  "k1", Connector: "bank.wire",
	})
	if err != nil {
		t.Fatalf("begin (post-prune): %v", err)
	}
	if e.IdempotencyKey != k {
		t.Fatalf("derived key drifted: got %q, want %q", e.IdempotencyKey, k)
	}
	if e.Status != EffectStatusConfirmed {
		t.Fatalf("status=%q, want confirmed (from snapshot)", e.Status)
	}
	resp, ok := e.ResponseJSON.(map[string]any)
	if !ok || resp["id"] != "wire-1" {
		t.Fatalf("response_json=%#v, want {id: wire-1}", e.ResponseJSON)
	}

	// And the live row is STILL gone — no resurrection.
	if eff, _ := svc.GetEffect(ctx, "a", "u", "s", k); eff != nil {
		t.Fatalf("row was re-created: %+v", eff)
	}
}

func TestBeginEffect_PrefersLiveRowOverSnapshot(t *testing.T) {
	// When BOTH the live row and a snapshot entry exist for the same
	// key, the live row wins — it's authoritative. Snapshot is purely a
	// fallback for the row-pruned case.
	svc := newTestSvc(t)
	ctx := context.Background()
	k := _snapConfirmedEffect(t, svc, "k1", map[string]any{"id": "live"}, 0)
	if _, err := svc.TakeSnapshot(ctx, TakeSnapshotOpts{
		AppName: "a", UserID: "u", SessionID: "s",
	}); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Mutate the snapshot to disagree with the live row.
	stale := `{"` + k + `":{"status":"confirmed","response_json":{"id":"stale-snapshot"}}}`
	if _, err := svc.db.ExecContext(ctx,
		svc.rew(`UPDATE tape_effect_snapshots SET effects_json = ?
WHERE app_name = ? AND user_id = ? AND session_id = ?`),
		stale, "a", "u", "s"); err != nil {
		t.Fatalf("mutate snapshot: %v", err)
	}

	// `BeginEffect` should still return the LIVE row (id=live), not
	// the stale snapshot (id=stale-snapshot).
	e, err := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "a", UserID: "u", SessionID: "s",
		InvocationID: "inv-1", DecisionIndex: 0,
		ToolName: "bank.wire", CallIndex: 0,
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  "k1", Connector: "bank.wire",
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	resp, ok := e.ResponseJSON.(map[string]any)
	if !ok || resp["id"] != "live" {
		t.Fatalf("response_json=%#v, want {id: live} (live row authoritative)", e.ResponseJSON)
	}
}

// ── snapshot + compactor: the integration that makes pruning safe ──────

func TestSnapshotThenCompactThenBeginEffect_ShortCircuits(t *testing.T) {
	// End-to-end: snapshot, compact (which prunes the underlying row),
	// then `BeginEffect` still short-circuits. This is the real operator
	// path — `snapshot, then prune.`
	svc := newTestSvc(t)
	ctx := context.Background()
	k := _snapConfirmedEffect(t, svc, "k1", map[string]any{"id": "wire-1"}, 0)
	if _, err := svc.TakeSnapshot(ctx, TakeSnapshotOpts{
		AppName: "a", UserID: "u", SessionID: "s",
	}); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Compact with EffectTTLMs=0 so the (just-confirmed) effect is
	// immediately eligible for pruning. The snapshot row is NOT in the
	// compactor's purview — it isn't touched. We pass an explicit
	// `nowMs` far enough in the future of the effect's `ts_ms` that the
	// compactor's `ts_ms < cutoff` predicate fires (the row was written
	// at wall-clock `now`, so `cutoff = nowMs - EffectTTLMs = nowMs`
	// equals it — strict-less-than misses the boundary).
	policy := CompactionPolicy{
		EffectTTLMs:                0,
		SessionTTLMs:               1 << 40, // effectively forever
		ArchiveTerminalObligations: false,
		ArchiveFiredTimers:         false,
	}
	future := nowMs() + 1_000_000
	r, err := CompactOnce(ctx, svc, policy, future)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if r.EffectsPruned != 1 {
		t.Fatalf("EffectsPruned=%d, want 1", r.EffectsPruned)
	}

	// Live row gone; snapshot row remains.
	if eff, _ := svc.GetEffect(ctx, "a", "u", "s", k); eff != nil {
		t.Fatalf("live row still present after compact")
	}
	snap, err := svc.GetSnapshot(ctx, GetSnapshotOpts{
		AppName: "a", UserID: "u", SessionID: "s",
	})
	if err != nil {
		t.Fatalf("get snap: %v", err)
	}
	if snap == nil {
		t.Fatalf("snapshot row was wiped by the compactor (bug)")
	}

	// Short-circuit through the snapshot.
	e, err := svc.BeginEffect(ctx, BeginEffectOpts{
		AppName: "a", UserID: "u", SessionID: "s",
		InvocationID: "inv-1", DecisionIndex: 0,
		ToolName: "bank.wire", CallIndex: 0,
		Semantics:    EffectSemanticsNonIdempotent,
		DispatchMode: EffectDispatchOutbox,
		BusinessKey:  "k1", Connector: "bank.wire",
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if e.Status != EffectStatusConfirmed {
		t.Fatalf("status=%q, want confirmed", e.Status)
	}
	resp, ok := e.ResponseJSON.(map[string]any)
	if !ok || resp["id"] != "wire-1" {
		t.Fatalf("response_json=%#v, want {id: wire-1}", e.ResponseJSON)
	}
}

// ── watermark + graceful no-data paths ─────────────────────────────────

func TestTakeSnapshot_RespectsUpToTsMs(t *testing.T) {
	// `UpToTsMs` bounds the read window — effects with `ts_ms` beyond
	// the watermark are NOT captured. The watermark on the snapshot row
	// reflects what's been captured.
	svc := newTestSvc(t)
	ctx := context.Background()
	_ = _snapConfirmedEffect(t, svc, "k-early", map[string]any{"v": 1}, 0)
	// Snapshot at ts=1 — far in the past, so the (just-confirmed)
	// effect (ts_ms = real wall-clock) is excluded.
	r, err := svc.TakeSnapshot(ctx, TakeSnapshotOpts{
		AppName: "a", UserID: "u", SessionID: "s",
		UpToTsMs: 1,
	})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if r.Captured != 0 {
		t.Fatalf("Captured=%d, want 0", r.Captured)
	}
	if r.UpToTsMs != 1 {
		t.Fatalf("UpToTsMs=%d, want 1", r.UpToTsMs)
	}
}

func TestSnapshot_HandlesNoEffectsGracefully(t *testing.T) {
	// `TakeSnapshot` on a session with zero terminal effects creates a
	// snapshot row with an empty map — safe and idempotent.
	svc := newTestSvc(t)
	ctx := context.Background()
	r, err := svc.TakeSnapshot(ctx, TakeSnapshotOpts{
		AppName: "a", UserID: "u", SessionID: "s",
	})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if r.Captured != 0 {
		t.Fatalf("Captured=%d, want 0", r.Captured)
	}
	if r.MergedTotal != 0 {
		t.Fatalf("MergedTotal=%d, want 0", r.MergedTotal)
	}
	snap, err := svc.GetSnapshot(ctx, GetSnapshotOpts{
		AppName: "a", UserID: "u", SessionID: "s",
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if snap == nil {
		t.Fatalf("snapshot row missing — should exist with empty map")
	}
	if len(snap.EffectsJSON) != 0 {
		t.Fatalf("EffectsJSON=%#v, want empty", snap.EffectsJSON)
	}
}

func TestGetSnapshot_ReturnsNilForNoSnapshot(t *testing.T) {
	// No snapshot taken — `GetSnapshot` is nil, not an empty row.
	svc := newTestSvc(t)
	ctx := context.Background()
	snap, err := svc.GetSnapshot(ctx, GetSnapshotOpts{
		AppName: "a", UserID: "u", SessionID: "s",
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if snap != nil {
		t.Fatalf("snapshot=%+v, want nil", snap)
	}
}
