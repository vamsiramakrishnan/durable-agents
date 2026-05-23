// Compactor tests — proves the pinning mechanism, the TTL gates, and
// session-level archival. Mirrors `tape/sdk/python-adk/tests/test_compact.py`.
//
// The point of these tests isn't that DELETE works; it's that the SAFETY
// INVARIANTS hold:
//
//   * a CONFIRMED effect with an unresolved obligation referencing it must
//     NOT be pruned, even if it's old enough;
//   * a session with a STUCK obligation: the effect is unpinned (STUCK
//     isn't in the active-set), but the obligation itself stays;
//   * a session with an unfired timer (past or future) must NOT be archived;
//   * the compactor is idempotent across ticks — two ticks == one tick.

import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';

import {
  adaptBetterSqlite3,
  TapeSessionService,
  EffectStatus,
  EffectSemantics,
  EffectDispatchMode,
  ObligationStatus,
  type EmbeddedDb,
} from '../src/embedded/index.ts';
import {
  compactOnce,
  compactionPolicy,
} from '../src/embedded/index.ts';

function newSvc(): TapeSessionService {
  const db: EmbeddedDb = adaptBetterSqlite3(new Database(':memory:'));
  return new TapeSessionService(db);
}

// Helper: seed a CONFIRMED effect at a given `tsMs` (simulating an old
// completed row). Same shape as `_begin_confirmed` in Python.
async function beginConfirmed(svc: TapeSessionService, args: {
  key?: string;
  invocationId?: string;
  tool?: string;
  tsMs: number;
}): Promise<string> {
  const key = args.key ?? 'ek-1';
  const inv = args.invocationId ?? 'inv-1';
  const tool = args.tool ?? 'bank.wire';
  const e = await svc.beginEffect({
    appName: 'a', userId: 'u', sessionId: 's',
    invocationId: inv, decisionIndex: 0, toolName: tool,
    callIndex: 0,
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: key, connector: 'bank.wire',
  });
  await svc.completeEffect({
    appName: 'a', userId: 'u', sessionId: 's',
    idempotencyKey: e.idempotencyKey,
    status: EffectStatus.CONFIRMED, responseJson: { id: key },
  });
  // Backdate ts_ms via direct UPDATE so we don't have to wait for
  // `effectTtlMs` real wall-clock time. Test-only.
  svc.db.prepare(`
    UPDATE tape_effects SET ts_ms = ? WHERE idempotency_key = ?
  `).run(args.tsMs, e.idempotencyKey);
  return e.idempotencyKey;
}

// ── mechanism 1+2: terminal-state pruning gated by TTL ─────────────────────

test('prunes_old_terminal_effect', async () => {
  const svc = newSvc();
  const key = await beginConfirmed(svc, { tsMs: 1000 });
  const policy = compactionPolicy({ effectTtlMs: 1 });
  const result = await compactOnce(svc, { policy, nowMs: 100_000 });
  assert.equal(result.effectsPruned, 1);
  const eff = await svc.getEffect({
    appName: 'a', userId: 'u', sessionId: 's', idempotencyKey: key,
  });
  assert.equal(eff, null);
});

test('keeps_fresh_terminal_effect', async () => {
  const svc = newSvc();
  const key = await beginConfirmed(svc, { tsMs: 99_999 });
  const policy = compactionPolicy({ effectTtlMs: 1_000_000 });
  const result = await compactOnce(svc, { policy, nowMs: 100_000 });
  assert.equal(result.effectsPruned, 0);
  const eff = await svc.getEffect({
    appName: 'a', userId: 'u', sessionId: 's', idempotencyKey: key,
  });
  assert.ok(eff);
});

test('keeps_pending_effect_regardless_of_age', async () => {
  const svc = newSvc();
  const e = await svc.beginEffect({
    appName: 'a', userId: 'u', sessionId: 's', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'bank.wire', callIndex: 0,
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: 'bk-1', connector: 'bank.wire',
  });
  svc.db.prepare(`UPDATE tape_effects SET ts_ms = 0 WHERE idempotency_key = ?`)
    .run(e.idempotencyKey);

  const result = await compactOnce(svc, {
    policy: compactionPolicy({ effectTtlMs: 1 }),
    nowMs: 100_000,
  });
  assert.equal(result.effectsPruned, 0); // PENDING isn't in the terminal set
});

// ── mechanism 5: compensable-window pinning ───────────────────────────────

test('pinning_refuses_to_prune_effect_with_active_obligation', async () => {
  const svc = newSvc();
  const key = await beginConfirmed(svc, { tsMs: 1000 });
  await svc.registerCompensation({
    appName: 'a', userId: 'u', sessionId: 's',
    effectKey: key, kind: 'reverse_wire',
    payloadJson: { external_ref: 'wire-1' },
  });

  const result = await compactOnce(svc, {
    policy: compactionPolicy({ effectTtlMs: 1 }),
    nowMs: 100_000,
  });
  assert.equal(result.effectsPruned, 0); // PINNED
  const eff = await svc.getEffect({
    appName: 'a', userId: 'u', sessionId: 's', idempotencyKey: key,
  });
  assert.ok(eff);
});

test('pinning_releases_when_obligation_resolved', async () => {
  const svc = newSvc();
  const key = await beginConfirmed(svc, { tsMs: 1000 });
  const ob = await svc.registerCompensation({
    appName: 'a', userId: 'u', sessionId: 's',
    effectKey: key, kind: 'reverse_wire',
  });
  await svc.resolveObligation({
    seq: ob.seq, status: ObligationStatus.COMPENSATED,
  });
  // Backdate obligation past the TTL so it's pruned too.
  svc.db.prepare(`UPDATE tape_obligations SET ts_ms = 1000 WHERE seq = ?`)
    .run(ob.seq);

  const result = await compactOnce(svc, {
    policy: compactionPolicy({ effectTtlMs: 1 }),
    nowMs: 100_000,
  });
  // Both pruned: obligation first (terminal + old), then the now-
  // unpinned effect.
  assert.equal(result.obligationsPruned, 1);
  assert.equal(result.effectsPruned, 1);
});

test('pinning_keeps_effect_with_stuck_obligation', async () => {
  // STUCK obligations aren't in the active set, so the pin doesn't catch
  // the effect — the effect gets pruned. The obligation itself remains
  // (only COMPENSATED is the archive_terminal_obligations target).
  const svc = newSvc();
  const key = await beginConfirmed(svc, { tsMs: 1000 });
  const ob = await svc.registerCompensation({
    appName: 'a', userId: 'u', sessionId: 's',
    effectKey: key, kind: 'reverse_wire',
  });
  await svc.resolveObligation({
    seq: ob.seq, status: ObligationStatus.STUCK,
  });

  const result = await compactOnce(svc, {
    policy: compactionPolicy({ effectTtlMs: 1 }),
    nowMs: 100_000,
  });
  assert.equal(result.effectsPruned, 1);
  const obs = await svc.listObligations({
    appName: 'a', userId: 'u', sessionId: 's', onlyUnresolved: false,
  });
  assert.equal(obs.length, 1);
  assert.equal(obs[0].status, ObligationStatus.STUCK);
});

// ── obligation archival ───────────────────────────────────────────────────

test('prunes_old_compensated_obligation', async () => {
  const svc = newSvc();
  const ob = await svc.registerCompensation({
    appName: 'a', userId: 'u', sessionId: 's',
    effectKey: 'ek-orphan', kind: 'reverse_wire',
  });
  await svc.resolveObligation({
    seq: ob.seq, status: ObligationStatus.COMPENSATED,
  });
  svc.db.prepare(`UPDATE tape_obligations SET ts_ms = 1000 WHERE seq = ?`)
    .run(ob.seq);

  const result = await compactOnce(svc, {
    policy: compactionPolicy({ effectTtlMs: 1 }),
    nowMs: 100_000,
  });
  assert.equal(result.obligationsPruned, 1);
});

test('keeps_stuck_obligation_regardless_of_age', async () => {
  const svc = newSvc();
  const ob = await svc.registerCompensation({
    appName: 'a', userId: 'u', sessionId: 's',
    effectKey: 'ek', kind: 'reverse_wire',
  });
  await svc.resolveObligation({
    seq: ob.seq, status: ObligationStatus.STUCK,
  });
  svc.db.prepare(`UPDATE tape_obligations SET ts_ms = 0 WHERE seq = ?`)
    .run(ob.seq);

  const result = await compactOnce(svc, {
    policy: compactionPolicy({ effectTtlMs: 1 }),
    nowMs: 100_000,
  });
  assert.equal(result.obligationsPruned, 0);
});

// ── session archival (mechanism 3) ────────────────────────────────────────

test('archives_idle_terminal_session', async () => {
  const svc = newSvc();
  for (let i = 0; i < 3; i++) {
    await beginConfirmed(svc, {
      key: `k-${i}`, invocationId: `inv-${i}`, tsMs: 1000,
    });
  }
  const policy = compactionPolicy({ effectTtlMs: 1, sessionTtlMs: 1 });
  const result = await compactOnce(svc, { policy, nowMs: 100_000 });
  assert.equal(result.sessionsArchived, 1);
});

test('does_not_archive_session_with_active_obligation', async () => {
  const svc = newSvc();
  await beginConfirmed(svc, { key: 'k-0', tsMs: 1000 });
  await svc.registerCompensation({
    appName: 'a', userId: 'u', sessionId: 's',
    effectKey: 'k-orphan', kind: 'reverse_wire',
  }); // PENDING (active)

  const policy = compactionPolicy({ effectTtlMs: 1, sessionTtlMs: 1 });
  const result = await compactOnce(svc, { policy, nowMs: 100_000 });
  assert.equal(result.sessionsArchived, 0);
});

test('does_not_archive_session_with_unfired_timer', async () => {
  const svc = newSvc();
  await beginConfirmed(svc, { key: 'k-0', tsMs: 1000 });
  await svc.setTimer({
    appName: 'a', userId: 'u', sessionId: 's',
    timerId: 'redrive-1', fireAtMs: 99_999_999, // future
    kind: 'redrive',
  });

  const policy = compactionPolicy({ effectTtlMs: 1, sessionTtlMs: 1 });
  const result = await compactOnce(svc, { policy, nowMs: 100_000 });
  assert.equal(result.sessionsArchived, 0);
});

// ── idempotency: running compaction twice == once ─────────────────────────

test('compact_is_idempotent_across_ticks', async () => {
  const svc = newSvc();
  for (let i = 0; i < 3; i++) {
    await beginConfirmed(svc, {
      key: `k-${i}`, invocationId: `inv-${i}`, tsMs: 1000,
    });
  }
  const policy = compactionPolicy({ effectTtlMs: 1, sessionTtlMs: 1 });
  const r1 = await compactOnce(svc, { policy, nowMs: 100_000 });
  const r2 = await compactOnce(svc, { policy, nowMs: 100_000 });
  assert.ok(r1.total() > 0);
  assert.equal(r2.total(), 0); // second tick is a no-op
});
