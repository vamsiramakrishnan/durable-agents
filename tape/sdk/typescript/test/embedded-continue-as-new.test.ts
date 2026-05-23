// `continueAsNew` — close one invocation chapter, start another.
//
// Same safety invariant as the compactor (compensable-window pinning): an
// old invocation's effect that has an active obligation pointing at it is
// NOT pruned, even when continue_as_new asks to wipe the slate. Mirrors
// `tape/sdk/python-adk/tests/test_continue_as_new.py`.

import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';

import {
  adaptBetterSqlite3,
  TapeSessionService,
  EffectStatus,
  EffectSemantics,
  EffectDispatchMode,
} from '../src/embedded/index.ts';

function newSvc(): TapeSessionService {
  return new TapeSessionService(adaptBetterSqlite3(new Database(':memory:')));
}

// Each call gets a fresh call_index so the derived idempotency_key is
// unique (beginEffect derives the key from
// (invocation, decision_index, tool, call_index)).
const _callSeq = new Map<string, number>();

async function confirmedEffect(svc: TapeSessionService, args: {
  invocation: string; key: string;
}): Promise<string> {
  const ci = _callSeq.get(args.invocation) ?? 0;
  _callSeq.set(args.invocation, ci + 1);
  const e = await svc.beginEffect({
    appName: 'a', userId: 'u', sessionId: 's',
    invocationId: args.invocation,
    decisionIndex: 0, toolName: 'bank.wire', callIndex: ci,
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: args.key, connector: 'bank.wire',
  });
  await svc.completeEffect({
    appName: 'a', userId: 'u', sessionId: 's',
    idempotencyKey: e.idempotencyKey,
    status: EffectStatus.CONFIRMED, responseJson: { id: args.key },
  });
  return e.idempotencyKey;
}

function resetSeq() { _callSeq.clear(); }

// ── happy path ────────────────────────────────────────────────────────────

test('prunes_old_invocations_terminal_effects', async () => {
  resetSeq();
  const svc = newSvc();
  const keys: string[] = [];
  for (let i = 0; i < 3; i++) {
    keys.push(await confirmedEffect(svc, { invocation: 'inv-old', key: `k-${i}` }));
  }
  // Sanity: 3 rows exist.
  for (const k of keys) {
    assert.ok(await svc.getEffect({
      appName: 'a', userId: 'u', sessionId: 's', idempotencyKey: k,
    }));
  }

  const r = await svc.continueAsNew({
    appName: 'a', userId: 'u', sessionId: 's',
    oldInvocationId: 'inv-old', newInvocationId: 'inv-new',
  });
  assert.equal(r.effectsPruned, 3);
  for (const k of keys) {
    assert.equal(await svc.getEffect({
      appName: 'a', userId: 'u', sessionId: 's', idempotencyKey: k,
    }), null);
  }
});

test('keeps_other_invocations_effects', async () => {
  resetSeq();
  const svc = newSvc();
  const kOld = await confirmedEffect(svc, { invocation: 'inv-old', key: 'k-old' });
  const kOther = await confirmedEffect(svc, { invocation: 'inv-other', key: 'k-other' });

  const r = await svc.continueAsNew({
    appName: 'a', userId: 'u', sessionId: 's',
    oldInvocationId: 'inv-old', newInvocationId: 'inv-new',
  });
  assert.equal(r.effectsPruned, 1);
  assert.equal(await svc.getEffect({
    appName: 'a', userId: 'u', sessionId: 's', idempotencyKey: kOld,
  }), null);
  // The other invocation's effect is still there.
  assert.ok(await svc.getEffect({
    appName: 'a', userId: 'u', sessionId: 's', idempotencyKey: kOther,
  }));
});

// ── safety: pinning by an active obligation ──────────────────────────────

test('does_not_prune_pinned_effect_even_under_old_invocation', async () => {
  resetSeq();
  const svc = newSvc();
  const key = await confirmedEffect(svc, { invocation: 'inv-old', key: 'k-1' });
  await svc.registerCompensation({
    appName: 'a', userId: 'u', sessionId: 's',
    effectKey: key, kind: 'reverse_wire',
  }); // PENDING (active)

  const r = await svc.continueAsNew({
    appName: 'a', userId: 'u', sessionId: 's',
    oldInvocationId: 'inv-old', newInvocationId: 'inv-new',
  });
  assert.equal(r.effectsPruned, 0);
  assert.equal(r.obligationsKept, 1);
  // Effect still there — the compensator may still need its external_ref.
  assert.ok(await svc.getEffect({
    appName: 'a', userId: 'u', sessionId: 's', idempotencyKey: key,
  }));
});

// ── state carry ──────────────────────────────────────────────────────────

test('carried_state_is_readable_under_new_invocation_id', async () => {
  resetSeq();
  const svc = newSvc();
  await svc.continueAsNew({
    appName: 'a', userId: 'u', sessionId: 's',
    oldInvocationId: 'inv-old', newInvocationId: 'inv-new',
    carriedState: { checkpoint: 'after sweep', balance: 42 },
  });

  const val = await svc.getValue({
    namespace: 'tape:continue-as-new:s', key: 'inv-new',
  });
  assert.ok(val);
  assert.deepEqual(val?.valueJson, { checkpoint: 'after sweep', balance: 42 });
  assert.equal(val?.writer, 'continue_as_new');
});

test('continue_as_new_is_atomic', async () => {
  resetSeq();
  const svc = newSvc();
  const key = await confirmedEffect(svc, { invocation: 'inv-old', key: 'k-1' });
  const r = await svc.continueAsNew({
    appName: 'a', userId: 'u', sessionId: 's',
    oldInvocationId: 'inv-old', newInvocationId: 'inv-new',
    carriedState: { x: 1 },
  });
  assert.equal(r.effectsPruned, 1);
  assert.equal(r.stateWritten, true);
  assert.equal(await svc.getEffect({
    appName: 'a', userId: 'u', sessionId: 's', idempotencyKey: key,
  }), null);
  const val = await svc.getValue({
    namespace: 'tape:continue-as-new:s', key: 'inv-new',
  });
  assert.ok(val);
});

test('no_prune_when_prune_old_false', async () => {
  resetSeq();
  const svc = newSvc();
  const key = await confirmedEffect(svc, { invocation: 'inv-old', key: 'k-1' });
  const r = await svc.continueAsNew({
    appName: 'a', userId: 'u', sessionId: 's',
    oldInvocationId: 'inv-old', newInvocationId: 'inv-new',
    carriedState: { x: 1 }, pruneOld: false,
  });
  assert.equal(r.effectsPruned, 0);
  assert.equal(r.stateWritten, true);
  assert.ok(await svc.getEffect({
    appName: 'a', userId: 'u', sessionId: 's', idempotencyKey: key,
  }));
});

// ── idempotency: carrying state twice updates, doesn't duplicate ─────────

test('repeated_continue_as_new_updates_state', async () => {
  resetSeq();
  const svc = newSvc();
  await svc.continueAsNew({
    appName: 'a', userId: 'u', sessionId: 's',
    oldInvocationId: 'inv-1', newInvocationId: 'inv-2',
    carriedState: { step: 1 },
  });
  await svc.continueAsNew({
    appName: 'a', userId: 'u', sessionId: 's',
    oldInvocationId: 'inv-2', newInvocationId: 'inv-2',
    carriedState: { step: 2 },
  });
  const val = await svc.getValue({
    namespace: 'tape:continue-as-new:s', key: 'inv-2',
  });
  assert.deepEqual(val?.valueJson, { step: 2 });
  assert.equal(val?.version, 2);
});
