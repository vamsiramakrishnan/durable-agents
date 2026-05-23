// Effect-ledger snapshot rows — the durable short-circuit that survives
// compaction.
//
// The contract: `takeSnapshot` captures terminal effects into a per-session
// JSON blob; `beginEffect` falls back to that blob when the live row is
// gone, so the compactor is free to prune underlying rows without breaking
// the idempotency-key short-circuit. Mirrors
// `tape/sdk/python-adk/tests/test_snapshot.py` verbatim in semantics.

import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';

import {
  adaptBetterSqlite3,
  TapeSessionService,
  EffectStatus,
  EffectSemantics,
  EffectDispatchMode,
  type EmbeddedDb,
} from '../src/embedded/index.ts';
import { compactOnce, compactionPolicy } from '../src/embedded/index.ts';

function newSvc(): TapeSessionService {
  const db: EmbeddedDb = adaptBetterSqlite3(new Database(':memory:'));
  return new TapeSessionService(db);
}

// Helper: make a CONFIRMED outbox effect. `key` is the businessKey; the
// derived `idempotencyKey` comes from
// (invocation, decision_index, tool, call_index) so we pin `callIndex`
// here to keep them distinct.
async function confirmedEffect(svc: TapeSessionService, args: {
  key: string;
  response: Record<string, unknown>;
  invocation?: string;
  callIndex?: number;
  businessKey?: string;
  connector?: string;
}): Promise<string> {
  const invocation = args.invocation ?? 'inv-1';
  const callIndex = args.callIndex ?? 0;
  const e = await svc.beginEffect({
    appName: 'a', userId: 'u', sessionId: 's',
    invocationId: invocation,
    decisionIndex: 0, toolName: 'bank.wire', callIndex,
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: args.businessKey ?? args.key,
    connector: args.connector ?? 'bank.wire',
  });
  await svc.completeEffect({
    appName: 'a', userId: 'u', sessionId: 's',
    idempotencyKey: e.idempotencyKey,
    status: EffectStatus.CONFIRMED,
    responseJson: args.response,
  });
  return e.idempotencyKey;
}

// ── basic ─────────────────────────────────────────────────────────────────

test('take_snapshot_captures_terminal_effects', async () => {
  const svc = newSvc();
  const k1 = await confirmedEffect(svc, {
    key: 'k1', response: { id: 'wire-1' }, callIndex: 0,
  });
  const k2 = await confirmedEffect(svc, {
    key: 'k2', response: { id: 'wire-2' }, callIndex: 1,
  });
  const r = await svc.takeSnapshot({
    appName: 'a', userId: 'u', sessionId: 's',
  });
  assert.equal(r.captured, 2);
  assert.equal(r.mergedTotal, 2);

  const snap = await svc.getSnapshot({
    appName: 'a', userId: 'u', sessionId: 's',
  });
  assert.ok(snap);
  assert.deepEqual(new Set(Object.keys(snap.effectsJson)), new Set([k1, k2]));
  assert.deepEqual(snap.effectsJson[k1]!.response_json, { id: 'wire-1' });
  assert.equal(snap.effectsJson[k2]!.status, EffectStatus.CONFIRMED);
});

test('snapshot_excludes_non_terminal_effects', async () => {
  // PENDING — never completed.
  const svc = newSvc();
  await svc.beginEffect({
    appName: 'a', userId: 'u', sessionId: 's', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'bank.wire', callIndex: 0,
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: 'k-pending', connector: 'bank.wire',
  });
  const r = await svc.takeSnapshot({
    appName: 'a', userId: 'u', sessionId: 's',
  });
  assert.equal(r.captured, 0);
});

test('repeated_snapshot_is_cumulative', async () => {
  // Second `takeSnapshot` MERGES — it doesn't reset. The snapshot row
  // accumulates across calls, last-write-wins per idempotency_key.
  const svc = newSvc();
  const k1 = await confirmedEffect(svc, {
    key: 'k1', response: { v: 1 }, callIndex: 0,
  });
  const r1 = await svc.takeSnapshot({
    appName: 'a', userId: 'u', sessionId: 's',
  });
  assert.equal(r1.mergedTotal, 1);

  const k2 = await confirmedEffect(svc, {
    key: 'k2', response: { v: 2 }, callIndex: 1,
  });
  const r2 = await svc.takeSnapshot({
    appName: 'a', userId: 'u', sessionId: 's',
  });
  assert.equal(r2.captured, 2); // both rows still terminal, both captured
  assert.equal(r2.mergedTotal, 2);

  const snap = await svc.getSnapshot({
    appName: 'a', userId: 'u', sessionId: 's',
  });
  assert.ok(snap);
  assert.deepEqual(new Set(Object.keys(snap.effectsJson)), new Set([k1, k2]));
});

// ── the load-bearing invariant: short-circuit survives row deletion ─────

test('begin_effect_short_circuits_via_snapshot_after_row_pruned', async () => {
  // The whole point. Snapshot the effect, manually delete the live row
  // (simulating the compactor), and verify `beginEffect` with the same
  // derived key returns the snapshot data instead of creating a fresh
  // PENDING row.
  //
  // If this test fails the compactor can break the idempotency contract
  // — the bug the snapshot exists to prevent.
  const svc = newSvc();
  const k = await confirmedEffect(svc, {
    key: 'k1', response: { id: 'wire-1' }, callIndex: 0,
  });
  await svc.takeSnapshot({ appName: 'a', userId: 'u', sessionId: 's' });

  // Brute-force delete the live row (no compactor TTL nonsense — we want
  // to test the fallback path, not the policy).
  svc.db.prepare(`DELETE FROM tape_effects WHERE idempotency_key = ?`).run(k);
  assert.equal(await svc.getEffect({
    appName: 'a', userId: 'u', sessionId: 's', idempotencyKey: k,
  }), null);

  // Now `beginEffect` with the same (invocation, decision, tool,
  // call_index) — which derives to the same idempotency_key — should NOT
  // create a new PENDING row. It should return the snapshot's captured
  // CONFIRMED record.
  const e = await svc.beginEffect({
    appName: 'a', userId: 'u', sessionId: 's', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'bank.wire', callIndex: 0,
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: 'k1', connector: 'bank.wire',
  });
  assert.equal(e.idempotencyKey, k);
  assert.equal(e.status, EffectStatus.CONFIRMED);
  assert.deepEqual(e.responseJson, { id: 'wire-1' });

  // And the live row is STILL gone — no resurrection.
  assert.equal(await svc.getEffect({
    appName: 'a', userId: 'u', sessionId: 's', idempotencyKey: k,
  }), null);
});

test('begin_effect_prefers_live_row_over_snapshot', async () => {
  // When BOTH the live row and a snapshot entry exist for the same key,
  // the live row wins — it's authoritative. Snapshot is purely a fallback
  // for the row-pruned case.
  const svc = newSvc();
  const k = await confirmedEffect(svc, {
    key: 'k1', response: { id: 'live' }, callIndex: 0,
  });
  // Take snapshot, then mutate the snapshot to disagree with the live
  // row. `beginEffect` should still return the live row.
  await svc.takeSnapshot({ appName: 'a', userId: 'u', sessionId: 's' });
  const snap = await svc.getSnapshot({
    appName: 'a', userId: 'u', sessionId: 's',
  });
  assert.ok(snap);
  const mutated = {
    ...snap.effectsJson,
    [k]: {
      ...snap.effectsJson[k]!,
      response_json: { id: 'stale-snapshot' },
    },
  };
  svc.db.prepare(`
    UPDATE tape_effect_snapshots
    SET effects_json = ?
    WHERE app_name = ? AND user_id = ? AND session_id = ?
  `).run(JSON.stringify(mutated), 'a', 'u', 's');

  const e = await svc.beginEffect({
    appName: 'a', userId: 'u', sessionId: 's', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'bank.wire', callIndex: 0,
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: 'k1', connector: 'bank.wire',
  });
  assert.deepEqual(e.responseJson, { id: 'live' });
});

// ── snapshot + compactor: the integration that makes pruning safe ───────

test('snapshot_then_compact_then_begin_effect_short_circuits', async () => {
  // End-to-end: snapshot, compact (which prunes the underlying row), then
  // `beginEffect` still short-circuits. This is the real operator path.
  const svc = newSvc();
  const k = await confirmedEffect(svc, {
    key: 'k1', response: { id: 'wire-1' }, callIndex: 0,
  });
  await svc.takeSnapshot({ appName: 'a', userId: 'u', sessionId: 's' });

  // Compact with effectTtlMs=0 so the (just-confirmed) effect is
  // immediately eligible for pruning. The snapshot row is NOT in the
  // compactor's purview — it isn't touched. Also push sessionTtlMs out
  // far so we don't accidentally archive the whole session (which would
  // also wipe the snapshot via session-level cascade-like logic).
  //
  // We pass an explicit `nowMs` far in the future so the WHERE-clause's
  // strict `ts_ms < cutoff` comparison fires deterministically — the
  // Python test relies on real wall-clock drift between completeEffect
  // and compactOnce, which doesn't show up under better-sqlite3's
  // synchronous fast path.
  const result = await compactOnce(svc, {
    policy: compactionPolicy({
      effectTtlMs: 0,
      sessionTtlMs: 10 ** 12,
      archiveTerminalObligations: false,
      archiveFiredTimers: false,
    }),
    nowMs: Date.now() + 1_000,
  });
  assert.equal(result.effectsPruned, 1);

  // Live row gone; snapshot row remains.
  assert.equal(await svc.getEffect({
    appName: 'a', userId: 'u', sessionId: 's', idempotencyKey: k,
  }), null);
  assert.ok(await svc.getSnapshot({
    appName: 'a', userId: 'u', sessionId: 's',
  }));

  // Short-circuit through the snapshot.
  const e = await svc.beginEffect({
    appName: 'a', userId: 'u', sessionId: 's', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'bank.wire', callIndex: 0,
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: 'k1', connector: 'bank.wire',
  });
  assert.equal(e.status, EffectStatus.CONFIRMED);
  assert.deepEqual(e.responseJson, { id: 'wire-1' });
});

// ── watermark ──────────────────────────────────────────────────────────────

test('take_snapshot_respects_up_to_ts_ms', async () => {
  // `upToTsMs` bounds the read window — effects with `ts_ms` beyond the
  // watermark are NOT captured. The watermark on the snapshot row
  // reflects what's been captured so a later snapshot knows where to
  // resume from.
  const svc = newSvc();
  await confirmedEffect(svc, {
    key: 'k-early', response: { v: 1 }, callIndex: 0,
  });
  // Snapshot at ts=1 — far in the past, so the (just-now-completed)
  // effect is not included.
  const r = await svc.takeSnapshot({
    appName: 'a', userId: 'u', sessionId: 's', upToTsMs: 1,
  });
  assert.equal(r.captured, 0);
  assert.equal(r.upToTsMs, 1);
});

test('snapshot_handles_no_effects_gracefully', async () => {
  // `takeSnapshot` on a session with zero terminal effects creates a
  // snapshot row with an empty map — safe and idempotent.
  const svc = newSvc();
  const r = await svc.takeSnapshot({
    appName: 'a', userId: 'u', sessionId: 's',
  });
  assert.equal(r.captured, 0);
  assert.equal(r.mergedTotal, 0);
  const snap = await svc.getSnapshot({
    appName: 'a', userId: 'u', sessionId: 's',
  });
  assert.ok(snap);
  assert.deepEqual(snap.effectsJson, {});
});

test('get_snapshot_returns_none_for_no_snapshot', async () => {
  // No snapshot taken — `getSnapshot` is null, not an empty row.
  const svc = newSvc();
  assert.equal(await svc.getSnapshot({
    appName: 'a', userId: 'u', sessionId: 's',
  }), null);
});
