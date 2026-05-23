// Embedded SQL parity tests — mirrors `tape/sdk/python-adk/tests/test_service.py`
// and `test_reactors.py` against the TypeScript `tape-ts/embedded` surface.
// Uses `better-sqlite3` in-memory mode so tests are fast + hermetic.

import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';

import {
  adaptBetterSqlite3,
  TapeSessionService,
  EffectStatus,
  EffectSemantics,
  EffectDispatchMode,
  EffectResolution,
  ObligationStatus,
  LogConnector,
  type Connector,
  type DispatchResult,
  type ObservationResult,
  type CompensationResult,
  type EffectRecord,
  type ObligationRecord,
  dispatchOutboxOnce,
  reconcileOnce,
  drainObligationsOnce,
  fireDueTimersOnce,
  effect,
  outboxTool,
  metaOf,
} from '../src/embedded/index.ts';

function newSvc(): TapeSessionService {
  const db = new Database(':memory:');
  return new TapeSessionService(adaptBetterSqlite3(db));
}

// ── effect ledger basics ───────────────────────────────────────────────────

test('beginEffect is idempotent on key', async () => {
  const svc = newSvc();
  const args = {
    appName: 't', userId: 'u', sessionId: 's', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'bank.wire', callIndex: 0,
    requestJson: { amount: 2_000_000 },
  };
  const e1 = await svc.beginEffect(args);
  const e2 = await svc.beginEffect(args);
  assert.equal(e1.idempotencyKey, e2.idempotencyKey);
  assert.equal(e2.status, EffectStatus.PENDING);
  const pending = await svc.listPendingEffects();
  assert.equal(pending.length, 1);
});

test('completeEffect is terminal-idempotent', async () => {
  const svc = newSvc();
  const e = await svc.beginEffect({
    appName: 't', userId: 'u', sessionId: 's', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'bank.wire',
  });
  const r1 = await svc.completeEffect({
    appName: 't', userId: 'u', sessionId: 's',
    idempotencyKey: e.idempotencyKey,
    status: EffectStatus.CONFIRMED,
    responseJson: { wireId: 'w-1' },
  });
  assert.equal(r1?.status, EffectStatus.CONFIRMED);
  // Second call doesn't overwrite.
  const r2 = await svc.completeEffect({
    appName: 't', userId: 'u', sessionId: 's',
    idempotencyKey: e.idempotencyKey,
    status: EffectStatus.FAILED, errorJson: { err: 'x' },
  });
  assert.equal(r2?.status, EffectStatus.CONFIRMED);
  assert.deepEqual(r2?.responseJson, { wireId: 'w-1' });
});

// ── load-bearing safety invariant ──────────────────────────────────────────

test('NON_IDEMPOTENT + INLINE is refused', async () => {
  const svc = newSvc();
  await assert.rejects(
    () => svc.beginEffect({
      appName: 't', userId: 'u', sessionId: 's', invocationId: 'inv-1',
      decisionIndex: 0, toolName: 'bank.wire',
      semantics: EffectSemantics.NON_IDEMPOTENT,
      dispatchMode: EffectDispatchMode.INLINE,
    }),
    /NON_IDEMPOTENT.*OUTBOX/,
  );
});

test('OUTBOX without connector is refused', async () => {
  const svc = newSvc();
  await assert.rejects(
    () => svc.beginEffect({
      appName: 't', userId: 'u', sessionId: 's', invocationId: 'inv-1',
      decisionIndex: 0, toolName: 'bank.wire',
      semantics: EffectSemantics.NON_IDEMPOTENT,
      dispatchMode: EffectDispatchMode.OUTBOX,
    }),
    /OUTBOX.*connector/,
  );
});

test('business_key UNIQUE across runs', async () => {
  const svc = newSvc();
  const bk = 'acct1:2m:2026-05-18';
  await svc.beginEffect({
    appName: 't', userId: 'u', sessionId: 's1', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'bank.wire',
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: bk, connector: 'bank.wire',
  });
  await assert.rejects(
    () => svc.beginEffect({
      appName: 't', userId: 'u', sessionId: 's2', invocationId: 'inv-2',
      decisionIndex: 0, toolName: 'bank.wire',
      semantics: EffectSemantics.NON_IDEMPOTENT,
      dispatchMode: EffectDispatchMode.OUTBOX,
      businessKey: bk, connector: 'bank.wire',
    }),
    /business_key.*already exists/,
  );
});

// ── CAS lease ─────────────────────────────────────────────────────────────

test('claimEffectDispatch — single winner under Promise.all', async () => {
  const svc = newSvc();
  const e = await svc.beginEffect({
    appName: 't', userId: 'u', sessionId: 's', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'bank.wire',
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: 'acct:2m:2026', connector: 'bank.wire',
  });
  const [r1, r2] = await Promise.all([
    svc.claimEffectDispatch({
      appName: 't', userId: 'u', sessionId: 's',
      idempotencyKey: e.idempotencyKey, claimer: 'dispatcher-A', leaseTtlMs: 60_000,
    }),
    svc.claimEffectDispatch({
      appName: 't', userId: 'u', sessionId: 's',
      idempotencyKey: e.idempotencyKey, claimer: 'dispatcher-B', leaseTtlMs: 60_000,
    }),
  ]);
  const winners = [r1, r2].filter(([acquired]) => acquired);
  assert.equal(winners.length, 1, JSON.stringify([r1, r2]));
  const winnerEffect = r1[0] ? r1[1] : r2[1];
  assert.ok(winnerEffect);
  assert.ok(['dispatcher-A', 'dispatcher-B'].includes(winnerEffect!.dispatchClaimedBy ?? ''));
});

test('expired dispatch lease is reclaimable', async () => {
  const svc = newSvc();
  const e = await svc.beginEffect({
    appName: 't', userId: 'u', sessionId: 's', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'bank.wire',
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: 'acct:2m:2026', connector: 'bank.wire',
  });
  const [acq] = await svc.claimEffectDispatch({
    appName: 't', userId: 'u', sessionId: 's',
    idempotencyKey: e.idempotencyKey, claimer: 'A', leaseTtlMs: 1,
  });
  assert.ok(acq);
  // From the future, the lease appears expired.
  const future = Date.now() + 1000;
  const [acq2, eff] = await svc.claimEffectDispatch({
    appName: 't', userId: 'u', sessionId: 's',
    idempotencyKey: e.idempotencyKey, claimer: 'B', leaseTtlMs: 60_000, nowMs: future,
  });
  assert.ok(acq2);
  assert.equal(eff?.dispatchClaimedBy, 'B');
});

// ── UNKNOWN transition ─────────────────────────────────────────────────────

test('recordDispatchAttempt(nextDispatchAtMs=0) → UNKNOWN', async () => {
  const svc = newSvc();
  const e = await svc.beginEffect({
    appName: 't', userId: 'u', sessionId: 's', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'bank.wire',
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: 'acct:2m:2026', connector: 'bank.wire',
  });
  const r = await svc.recordDispatchAttempt({
    appName: 't', userId: 'u', sessionId: 's',
    idempotencyKey: e.idempotencyKey,
    error: 'simulated lost ack', nextDispatchAtMs: 0,
  });
  assert.equal(r?.status, EffectStatus.UNKNOWN);
  assert.equal(r?.dispatchAttempts, 1);
  assert.ok(r?.dispatchClaimedBy === null || r?.dispatchClaimedBy === '');

  const unknowns = await svc.listPendingEffects({
    includePending: false, includeUnknown: true,
  });
  assert.equal(unknowns.length, 1);
  assert.equal(unknowns[0].status, EffectStatus.UNKNOWN);
});

test('recordDispatchAttempt(positive) reschedules — stays PENDING', async () => {
  const svc = newSvc();
  const e = await svc.beginEffect({
    appName: 't', userId: 'u', sessionId: 's', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'bank.wire',
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: 'acct:2m:2026', connector: 'bank.wire',
  });
  const future = Date.now() + 60_000;
  const r = await svc.recordDispatchAttempt({
    appName: 't', userId: 'u', sessionId: 's',
    idempotencyKey: e.idempotencyKey,
    error: 'connection refused', nextDispatchAtMs: future,
  });
  assert.equal(r?.status, EffectStatus.PENDING);
  assert.equal(r?.nextDispatchAtMs, future);
  assert.equal(r?.dispatchAttempts, 1);
});

// ── reconciler write path ──────────────────────────────────────────────────

test('recordExternalObservation(CONFIRMED) resolves UNKNOWN', async () => {
  const svc = newSvc();
  const e = await svc.beginEffect({
    appName: 't', userId: 'u', sessionId: 's', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'bank.wire',
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: 'acct:2m:2026', connector: 'bank.wire',
  });
  await svc.recordDispatchAttempt({
    appName: 't', userId: 'u', sessionId: 's',
    idempotencyKey: e.idempotencyKey,
    error: 'ack lost', nextDispatchAtMs: 0,
  });
  const r = await svc.recordExternalObservation({
    appName: 't', userId: 'u', sessionId: 's',
    idempotencyKey: e.idempotencyKey,
    resolution: EffectResolution.CONFIRMED,
    externalRef: 'wire-0001',
    responseJson: { wireId: 'wire-0001' },
  });
  assert.equal(r?.status, EffectStatus.CONFIRMED);
  assert.equal(r?.externalRef, 'wire-0001');
});

test('DUPLICATE observation atomically registers compensation', async () => {
  const svc = newSvc();
  const e = await svc.beginEffect({
    appName: 't', userId: 'u', sessionId: 's', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'bank.wire',
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: 'acct:2m:2026', connector: 'bank.wire',
  });
  await svc.recordDispatchAttempt({
    appName: 't', userId: 'u', sessionId: 's',
    idempotencyKey: e.idempotencyKey,
    error: 'ack lost', nextDispatchAtMs: 0,
  });
  const r = await svc.recordExternalObservation({
    appName: 't', userId: 'u', sessionId: 's',
    idempotencyKey: e.idempotencyKey,
    resolution: EffectResolution.DUPLICATE,
    externalRef: 'wire-A',
    compensateOnDuplicateKind: 'reverse_wire',
  });
  assert.equal(r?.status, EffectStatus.CONFIRMED);
  const obs = await svc.listObligations({
    appName: 't', userId: 'u', sessionId: 's',
  });
  assert.equal(obs.length, 1);
  assert.equal(obs[0].kind, 'reverse_wire');
  assert.equal(obs[0].status, ObligationStatus.PENDING);
  assert.equal(obs[0].effectKey, e.idempotencyKey);
});

test('ABSENT on NON-IDEMPOTENT stays UNKNOWN', async () => {
  const svc = newSvc();
  const e = await svc.beginEffect({
    appName: 't', userId: 'u', sessionId: 's', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'bank.wire',
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: 'acct:2m:2026', connector: 'bank.wire',
  });
  await svc.recordDispatchAttempt({
    appName: 't', userId: 'u', sessionId: 's',
    idempotencyKey: e.idempotencyKey,
    error: 'ack lost', nextDispatchAtMs: 0,
  });
  const r = await svc.recordExternalObservation({
    appName: 't', userId: 'u', sessionId: 's',
    idempotencyKey: e.idempotencyKey,
    resolution: EffectResolution.ABSENT,
  });
  assert.equal(r?.status, EffectStatus.UNKNOWN);
});

// ── obligation ledger ──────────────────────────────────────────────────────

test('registerCompensation is idempotent on (session, effect_key, kind)', async () => {
  const svc = newSvc();
  const o1 = await svc.registerCompensation({
    appName: 't', userId: 'u', sessionId: 's',
    effectKey: 'ek-1', kind: 'reverse_wire', payloadJson: { amount: 1 },
  });
  const o2 = await svc.registerCompensation({
    appName: 't', userId: 'u', sessionId: 's',
    effectKey: 'ek-1', kind: 'reverse_wire', payloadJson: { amount: 2 },
  });
  assert.equal(o1.seq, o2.seq);
  const obs = await svc.listObligations({
    appName: 't', userId: 'u', sessionId: 's',
  });
  assert.equal(obs.length, 1);
});

test('claimObligation — single winner under Promise.all', async () => {
  const svc = newSvc();
  const o = await svc.registerCompensation({
    appName: 't', userId: 'u', sessionId: 's',
    effectKey: 'ek-1', kind: 'reverse_wire',
  });
  const [r1, r2] = await Promise.all([
    svc.claimObligation({ seq: o.seq, claimer: 'A', leaseTtlMs: 60_000 }),
    svc.claimObligation({ seq: o.seq, claimer: 'B', leaseTtlMs: 60_000 }),
  ]);
  const winners = [r1, r2].filter(([acquired]) => acquired);
  assert.equal(winners.length, 1);
});

test('recordObligationAttempt — retries then STUCK', async () => {
  const svc = newSvc();
  const o = await svc.registerCompensation({
    appName: 't', userId: 'u', sessionId: 's',
    effectKey: 'ek-1', kind: 'reverse_wire', maxAttempts: 3,
  });
  const future = Date.now() + 10_000;
  const r1 = await svc.recordObligationAttempt({
    seq: o.seq, error: 'boom', nextAttemptAtMs: future,
  });
  assert.equal(r1?.status, ObligationStatus.PENDING);
  assert.equal(r1?.attempts, 1);
  const r2 = await svc.recordObligationAttempt({
    seq: o.seq, error: 'boom again', nextAttemptAtMs: future,
  });
  assert.equal(r2?.status, ObligationStatus.PENDING);
  assert.equal(r2?.attempts, 2);
  const r3 = await svc.recordObligationAttempt({
    seq: o.seq, error: 'boom 3', nextAttemptAtMs: future,
  });
  assert.equal(r3?.status, ObligationStatus.STUCK);
  assert.equal(r3?.attempts, 3);
});

test('terminal-now (nextAttemptAtMs=0) forces STUCK', async () => {
  const svc = newSvc();
  const o = await svc.registerCompensation({
    appName: 't', userId: 'u', sessionId: 's',
    effectKey: 'ek-1', kind: 'reverse_wire', maxAttempts: 10,
  });
  const r = await svc.recordObligationAttempt({
    seq: o.seq, error: 'business rule says no', nextAttemptAtMs: 0,
  });
  assert.equal(r?.status, ObligationStatus.STUCK);
  assert.equal(r?.attempts, 1);
});

test('listUnresolvedObligations includes COMMITTED-expired', async () => {
  const svc = newSvc();
  const o = await svc.registerCompensation({
    appName: 't', userId: 'u', sessionId: 's1',
    effectKey: 'ek-1', kind: 'reverse_wire',
  });
  await svc.claimObligation({ seq: o.seq, claimer: 'A', leaseTtlMs: 1 });
  const future = Date.now() + 1000;
  const rows = await svc.listUnresolvedObligations({ nowMs: future });
  assert.ok(rows.some((x) => x.seq === o.seq));
});

// ── timer registry ───────────────────────────────────────────────────────

test('setTimer is idempotent on (session, timer_id)', async () => {
  const svc = newSvc();
  const t1 = await svc.setTimer({
    appName: 't', userId: 'u', sessionId: 's',
    timerId: 'redrive-1', fireAtMs: 12345, kind: 'redrive',
  });
  const t2 = await svc.setTimer({
    appName: 't', userId: 'u', sessionId: 's',
    timerId: 'redrive-1', fireAtMs: 99999, kind: 'redrive',
  });
  assert.equal(t1.fireAtMs, t2.fireAtMs);
});

test('listDueTimers(claim=true) marks fired', async () => {
  const svc = newSvc();
  const now = Date.now();
  await svc.setTimer({
    appName: 't', userId: 'u', sessionId: 's',
    timerId: 'due-1', fireAtMs: now - 1000, kind: 'redrive',
  });
  await svc.setTimer({
    appName: 't', userId: 'u', sessionId: 's',
    timerId: 'future-1', fireAtMs: now + 60_000, kind: 'redrive',
  });
  const due = await svc.listDueTimers({ nowMs: now, claim: true });
  assert.equal(due.length, 1);
  assert.equal(due[0].timerId, 'due-1');
  const due2 = await svc.listDueTimers({ nowMs: now, claim: false });
  assert.deepEqual(due2, []);
});

// ── reactive KV ──────────────────────────────────────────────────────────

test('writeValue — CAS rejects stale ifVersion', async () => {
  const svc = newSvc();
  const v1 = await svc.writeValue({
    namespace: 'treasury', key: 'fx_rate',
    valueJson: { USD: 1.0 }, ifVersion: 0,
  });
  assert.equal(v1.version, 1);
  const v2 = await svc.writeValue({
    namespace: 'treasury', key: 'fx_rate',
    valueJson: { USD: 1.01 }, ifVersion: 1,
  });
  assert.equal(v2.version, 2);
  await assert.rejects(
    () => svc.writeValue({
      namespace: 'treasury', key: 'fx_rate',
      valueJson: { USD: 1.02 }, ifVersion: 1,
    }),
    /stale CAS/,
  );
});

// ── reactor: end-to-end UNKNOWN → reconcile ──────────────────────────────

interface FakeBankRecord {
  wireId: string;
  amount: number;
  account: string;
  businessKey: string;
}

class FakeBank {
  ledger: Map<string, FakeBankRecord> = new Map();
  wire(businessKey: string, amount: number, account: string): FakeBankRecord {
    const existing = this.ledger.get(businessKey);
    if (existing) return existing;
    const wireId = `wire-${String(this.ledger.size + 1).padStart(4, '0')}`;
    const rec = { wireId, amount, account, businessKey };
    this.ledger.set(businessKey, rec);
    return rec;
  }
  find(businessKey: string): FakeBankRecord | undefined {
    return this.ledger.get(businessKey);
  }
  reverse(wireId: string): { reversalId: string } {
    return { reversalId: `rev-of-${wireId}` };
  }
}

class BankConnector implements Connector {
  name = 'bank.wire';
  nDispatches = 0;
  bank: FakeBank;
  injectUnknownOnce: boolean;
  raiseOnce: boolean;
  constructor(bank: FakeBank, injectUnknownOnce = false, raiseOnce = false) {
    this.bank = bank;
    this.injectUnknownOnce = injectUnknownOnce;
    this.raiseOnce = raiseOnce;
  }

  async dispatch(effect: EffectRecord): Promise<DispatchResult> {
    this.nDispatches++;
    const req = (effect.requestJson as { amount?: number; account?: string }) || {};
    const bk = effect.businessKey ?? '';
    // Always write the wire — it's the "the call landed" part. Faults below
    // model what happens AFTER the wire lands.
    const wire = this.bank.wire(bk, req.amount ?? 0, req.account ?? '?');
    if (this.injectUnknownOnce && this.nDispatches === 1) {
      return { status: 'unknown', error: { reason: 'simulated lost ack' } };
    }
    if (this.raiseOnce && this.nDispatches === 1) {
      throw new Error('simulated transient network error');
    }
    return {
      status: 'confirmed',
      externalRef: wire.wireId,
      response: { wireId: wire.wireId },
    };
  }

  async observe(effect: EffectRecord): Promise<ObservationResult> {
    const rec = this.bank.find(effect.businessKey ?? '');
    if (!rec) return { status: 'absent' };
    return {
      status: 'confirmed',
      externalRef: rec.wireId,
      response: { wireId: rec.wireId },
    };
  }

  async compensate(obligation: ObligationRecord): Promise<CompensationResult> {
    const wid = (obligation.payloadJson as { external_ref?: string } | null)?.external_ref;
    if (!wid) return { status: 'failed', error: { reason: 'no wire_id' } };
    const rev = this.bank.reverse(wid);
    return { status: 'compensated', response: rev };
  }
}

test('end-to-end UNKNOWN → reconcile via dispatchOutboxOnce + reconcileOnce', async () => {
  const svc = newSvc();
  const bank = new FakeBank();
  const connector = new BankConnector(bank, true);

  const e = await svc.beginEffect({
    appName: 't', userId: 'u', sessionId: 's', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'bank.wire',
    requestJson: { amount: 2_000_000, account: 'acct-1' },
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: 'acct1:2m:2026-05-18', connector: 'bank.wire',
  });
  assert.equal(e.status, EffectStatus.PENDING);

  const r1 = await dispatchOutboxOnce(svc, {
    connectors: { 'bank.wire': connector }, claimer: 'd-1',
  });
  assert.ok(r1.some((x) => x.outcome === 'unknown'), JSON.stringify(r1));
  let eff = await svc.getEffect({
    appName: 't', userId: 'u', sessionId: 's',
    idempotencyKey: e.idempotencyKey,
  });
  assert.equal(eff?.status, EffectStatus.UNKNOWN);
  assert.equal(bank.ledger.size, 1);

  const r2 = await reconcileOnce(svc, { connectors: { 'bank.wire': connector } });
  assert.ok(r2.some((x) => x.outcome === 'confirmed'), JSON.stringify(r2));
  eff = await svc.getEffect({
    appName: 't', userId: 'u', sessionId: 's',
    idempotencyKey: e.idempotencyKey,
  });
  assert.equal(eff?.status, EffectStatus.CONFIRMED);
  assert.equal(eff?.externalRef, 'wire-0001');
  assert.equal(bank.ledger.size, 1);
});

test('outbox backs off on generic exception', async () => {
  const svc = newSvc();
  const connector = new BankConnector(new FakeBank(), false, true);
  const e = await svc.beginEffect({
    appName: 't', userId: 'u', sessionId: 's', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'bank.wire',
    requestJson: { amount: 100, account: 'x' },
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: 'x:100:2026', connector: 'bank.wire',
  });
  const now = Date.now();
  const r1 = await dispatchOutboxOnce(svc, {
    connectors: { 'bank.wire': connector },
    claimer: 'd-1', defaultBackoffMs: 10_000,
  });
  assert.ok(r1.some((x) => x.outcome === 'exception'));
  const eff = await svc.getEffect({
    appName: 't', userId: 'u', sessionId: 's',
    idempotencyKey: e.idempotencyKey,
  });
  assert.equal(eff?.status, EffectStatus.PENDING);
  assert.ok((eff?.nextDispatchAtMs ?? 0) > now);
  assert.equal(eff?.dispatchAttempts, 1);
});

test('two concurrent dispatchers + three effects → each dispatched exactly once', async () => {
  const svc = newSvc();
  const bank = new FakeBank();
  const connector = new BankConnector(bank);

  for (let i = 0; i < 3; i++) {
    await svc.beginEffect({
      appName: 't', userId: 'u', sessionId: 's',
      invocationId: `inv-${i}`, decisionIndex: 0,
      toolName: 'bank.wire',
      requestJson: { amount: i, account: 'x' },
      semantics: EffectSemantics.NON_IDEMPOTENT,
      dispatchMode: EffectDispatchMode.OUTBOX,
      businessKey: `x:${i}:2026`, connector: 'bank.wire',
    });
  }

  const [r1, r2] = await Promise.all([
    dispatchOutboxOnce(svc, { connectors: { 'bank.wire': connector }, claimer: 'd-1' }),
    dispatchOutboxOnce(svc, { connectors: { 'bank.wire': connector }, claimer: 'd-2' }),
  ]);
  const confirmed = [...r1, ...r2].filter((x) => x.outcome === 'confirmed');
  assert.equal(confirmed.length, 3);
  assert.equal(bank.ledger.size, 3);
  const pending = await svc.listPendingEffects();
  assert.deepEqual(pending, []);
});

test('drain compensates pending obligation', async () => {
  const svc = newSvc();
  const bank = new FakeBank();
  bank.wire('x:1:2026', 1, 'x');
  const connector = new BankConnector(bank);
  await svc.registerCompensation({
    appName: 't', userId: 'u', sessionId: 's',
    effectKey: 'ek-1', kind: 'bank.wire',
    payloadJson: { external_ref: 'wire-0001' },
  });
  const r = await drainObligationsOnce(svc, {
    connectors: { 'bank.wire': connector }, claimer: 'dr-1',
  });
  assert.ok(r.some((x) => x.outcome === 'compensated'), JSON.stringify(r));
  const after = await svc.listObligations({
    appName: 't', userId: 'u', sessionId: 's', onlyUnresolved: false,
  });
  assert.equal(after.length, 1);
  assert.equal(after[0].status, ObligationStatus.COMPENSATED);
});

test('fireDueTimers invokes dispatcher', async () => {
  const svc = newSvc();
  const firedIds: string[] = [];
  const now = Date.now();
  await svc.setTimer({
    appName: 't', userId: 'u', sessionId: 's',
    timerId: 't-1', fireAtMs: now - 100, kind: 'redrive',
  });
  await svc.setTimer({
    appName: 't', userId: 'u', sessionId: 's',
    timerId: 't-future', fireAtMs: now + 60_000, kind: 'redrive',
  });
  const r = await fireDueTimersOnce(svc, {
    dispatcher: async (t) => { firedIds.push(t.timerId); },
  });
  assert.deepEqual(firedIds, ['t-1']);
  assert.ok(r.some((x) => x.outcome === 'fired'));
});

// ── decorators ───────────────────────────────────────────────────────────

test('effect() stamps idempotent+inline metadata', () => {
  const fn = effect(async (_args: Record<string, unknown>) => 'ok');
  const meta = metaOf(fn);
  assert.ok(meta);
  assert.equal(meta?.semantics, EffectSemantics.IDEMPOTENT);
  assert.equal(meta?.dispatchMode, EffectDispatchMode.INLINE);
});

test('outboxTool() stamps non_idempotent+outbox metadata', () => {
  const fn = outboxTool(
    { businessKey: 'static-key', connector: 'bank.wire', compensate: 'reverse_wire' },
    async (_args: Record<string, unknown>) => 'queued',
  );
  const meta = metaOf(fn);
  assert.ok(meta);
  assert.equal(meta?.semantics, EffectSemantics.NON_IDEMPOTENT);
  assert.equal(meta?.dispatchMode, EffectDispatchMode.OUTBOX);
  assert.equal(meta?.connector, 'bank.wire');
  assert.equal(meta?.businessKeyStatic, 'static-key');
  assert.equal(meta?.compensate, 'reverse_wire');
});

test('outboxTool() refuses missing connector at construction', () => {
  assert.throws(
    // @ts-expect-error — exercising the refusal
    () => outboxTool({ businessKey: 'k' }, async () => 'x'),
    /connector.*required/,
  );
});

test('outboxTool() refuses missing businessKey at construction', () => {
  assert.throws(
    // @ts-expect-error — exercising the refusal
    () => outboxTool({ connector: 'bank.wire' }, async () => 'x'),
    /businessKey.*required/,
  );
});

// ── LogConnector smoke ───────────────────────────────────────────────────

test('LogConnector records calls + returns CONFIRMED', async () => {
  const c = new LogConnector();
  const eff: EffectRecord = {
    appName: 't', userId: 'u', sessionId: 's',
    idempotencyKey: '12345678-extra',
    invocationId: 'inv', decisionIndex: 0, toolName: 'x', callIndex: 0,
    status: EffectStatus.PENDING,
    semantics: EffectSemantics.IDEMPOTENT,
    dispatchMode: EffectDispatchMode.INLINE,
    businessKey: null, connector: 'log', externalRef: null,
    dispatchAttempts: 0, nextDispatchAtMs: 0,
    dispatchClaimedBy: null, dispatchClaimExpiresAtMs: 0,
    lastDispatchError: null,
    requestJson: null, responseJson: null, errorJson: null,
    tsMs: Date.now(),
  };
  const r = await c.dispatch(eff);
  assert.equal(r.status, 'confirmed');
  assert.equal(c.dispatches.length, 1);
});
