// Embedded-tier chaos tests — mirrors `tape/sdk/python-adk/tests/test_chaos.py`
// against the TS `TapeSessionService`. Proves the same invariants:
//   * lose_ack flips CONFIRMED → UNKNOWN
//   * delay honours the wall-clock
//   * tool-scoped faults only fire on matching tools
//   * strict_faults FAILS on missing connector targets (the silent-skip guard)
//   * no_stuck_obligations / exactly_one read the embedded tables directly
//   * Invariant.__call__ uniformity — bare or called both work

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
  dispatchOutboxOnce,
  reconcileOnce,
  type Connector,
  type DispatchResult,
  type ObservationResult,
  type CompensationResult,
  type EffectRecord,
  type ObligationRecord,
  // chaos surface
  ChaosConnector,
  scenario,
  loseAck,
  duplicate as _duplicate,
  delayConnector,
  noStuckObligations,
  noBlindNonIdempotentRetry as _noBlindNonIdempotentRetry,
  exactlyOne,
  chaosRun,
} from '../src/embedded/index.ts';

function newSvc(): TapeSessionService {
  const db = new Database(':memory:');
  return new TapeSessionService(adaptBetterSqlite3(db));
}

// ── a trivial fake bank connector (test fixture) ──────────────────────────

class LedgerConnector implements Connector {
  name = 'bank.wire';
  ledger: Map<string, string> = new Map();
  delayMs = 0;
  async dispatch(effect: EffectRecord): Promise<DispatchResult> {
    if (this.delayMs > 0) {
      await new Promise((res) => setTimeout(res, this.delayMs));
    }
    const bk = effect.businessKey || effect.idempotencyKey;
    let wid = this.ledger.get(bk);
    if (!wid) {
      wid = `w-${String(this.ledger.size).padStart(4, '0')}`;
      this.ledger.set(bk, wid);
    }
    return { status: 'confirmed', externalRef: wid, response: { wire_id: wid } };
  }
  async observe(effect: EffectRecord): Promise<ObservationResult> {
    const bk = effect.businessKey || effect.idempotencyKey;
    const w = this.ledger.get(bk);
    if (w) return { status: 'confirmed', externalRef: w };
    return { status: 'absent' };
  }
  async compensate(_o: ObligationRecord): Promise<CompensationResult> {
    return { status: 'compensated' };
  }
}

function mkEffect(over: Partial<EffectRecord> = {}): EffectRecord {
  return {
    appName: 'a', userId: 'u', sessionId: 's',
    idempotencyKey: 'k', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'wire', callIndex: 0,
    status: 'pending',
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: 'bk-1', connector: 'bank.wire', externalRef: null,
    dispatchAttempts: 0, nextDispatchAtMs: 0,
    dispatchClaimedBy: null, dispatchClaimExpiresAtMs: 0,
    lastDispatchError: null,
    requestJson: {}, responseJson: null, errorJson: null,
    tsMs: 0,
    ...over,
  };
}

// ── ChaosConnector: the fault mechanism, in isolation ─────────────────────

test('lose_ack flips confirmed to unknown', async () => {
  const inner = new LedgerConnector();
  const wrapped = new ChaosConnector(
    inner,
    [loseAck({ connector: 'bank.wire', probability: 1.0 })],
  );
  const result = await wrapped.dispatch(mkEffect());
  assert.equal(result.status, 'unknown');
  // The inner call did land (the wrapper's contract).
  assert.equal(inner.ledger.get('bk-1'), 'w-0000');
});

test('delay_connector blocks dispatch', async () => {
  const inner = new LedgerConnector();
  const wrapped = new ChaosConnector(
    inner,
    [delayConnector({ connector: 'bank.wire', ms: 80 })],
  );
  const t0 = Date.now();
  await wrapped.dispatch(mkEffect());
  assert.ok((Date.now() - t0) >= 70, `elapsed ${Date.now() - t0}ms`);
});

test('tool-scoped fault only fires on matching tool', async () => {
  const inner = new LedgerConnector();
  const wrapped = new ChaosConnector(
    inner,
    [loseAck({ tool: 'wire', probability: 1.0 })],
  );
  const rWire = await wrapped.dispatch(mkEffect({
    toolName: 'wire', idempotencyKey: 'k-wire', businessKey: 'bk-wire',
  }));
  const rPost = await wrapped.dispatch(mkEffect({
    toolName: 'post_gl', idempotencyKey: 'k-post', businessKey: 'bk-post',
  }));
  assert.equal(rWire.status, 'unknown', 'tool matches → fault fires');
  assert.equal(rPost.status, 'confirmed', 'tool doesn\'t match → passthrough');
});

// ── strict_faults: the silent-skip false-positive guard ───────────────────

test('strict_faults fails on missing connector', async () => {
  const svc = newSvc();
  const scen = scenario({
    name: 'missing-target',
    faults: [loseAck({ connector: 'bank.wire' })],
    invariants: [noStuckObligations],
  });
  const report = await chaosRun(scen, async () => { /* noop */ },
    { svc, connectors: {} }); // empty — bank.wire missing
  assert.equal(report.passed, false);
  assert.ok(
    report.invariantResults.some((r) => r.name === 'strict_faults' && !r.passed),
    JSON.stringify(report.invariantResults),
  );
});

test('strict_faults off allows skip', async () => {
  const svc = newSvc();
  const scen = scenario({
    name: 'optional-target',
    faults: [loseAck({ connector: 'bank.wire' })],
    invariants: [noStuckObligations],
    strictFaults: false,
  });
  const report = await chaosRun(scen, async () => { /* noop */ },
    { svc, connectors: {} });
  assert.equal(report.passed, true);
  assert.ok(
    report.notes.some((n) => n.includes('not in `connectors` dict')),
    JSON.stringify(report.notes),
  );
});

// ── invariants: read the embedded tables ──────────────────────────────────

test('noStuckObligations passes on clean store', async () => {
  const svc = newSvc();
  const scen = scenario({
    name: 'smoke',
    invariants: [noStuckObligations],
  });
  const report = await chaosRun(scen, async () => { /* noop */ },
    { svc, connectors: {} });
  assert.equal(report.passed, true);
});

test('noStuckObligations fails when one is stuck', async () => {
  const svc = newSvc();
  const ob = await svc.registerCompensation({
    appName: 'a', userId: 'u', sessionId: 's',
    effectKey: 'ek', kind: 'reverse_wire', maxAttempts: 1,
  });
  await svc.resolveObligation({ seq: ob.seq, status: ObligationStatus.STUCK });

  const scen = scenario({
    name: 'stuck',
    invariants: [noStuckObligations],
  });
  const report = await chaosRun(scen, async () => { /* noop */ },
    { svc, connectors: {} });
  assert.equal(report.passed, false);
  assert.ok(
    report.invariantResults.some((r) => r.detail.toLowerCase().includes('stuck')),
    JSON.stringify(report.invariantResults),
  );
});

test('exactlyOne invariant', async () => {
  const svc = newSvc();
  const e = await svc.beginEffect({
    appName: 'a', userId: 'u', sessionId: 's', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'wire', callIndex: 0,
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: 'bk-1', connector: 'bank.wire',
  });
  await svc.completeEffect({
    appName: 'a', userId: 'u', sessionId: 's',
    idempotencyKey: e.idempotencyKey,
    status: EffectStatus.CONFIRMED, responseJson: { id: '1' },
  });

  const scen = scenario({
    name: 'one',
    invariants: [exactlyOne({ connector: 'bank.wire' })],
  });
  const report = await chaosRun(scen, async () => { /* noop */ },
    { svc, connectors: {} });
  assert.equal(report.passed, true, JSON.stringify(report));
});

// ── end-to-end: lose_ack → reconcile loop drives an UNKNOWN to CONFIRMED ─

test('lose_ack e2e with reconciler', async () => {
  const svc = newSvc();
  await svc.beginEffect({
    appName: 'a', userId: 'u', sessionId: 's', invocationId: 'inv-1',
    decisionIndex: 0, toolName: 'wire', callIndex: 0,
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    businessKey: 'bk-1', connector: 'bank.wire',
  });
  const bank = new LedgerConnector();

  const scen = scenario({
    name: 'unknown-then-reconcile',
    faults: [loseAck({ connector: 'bank.wire', probability: 1.0 })],
    invariants: [
      noStuckObligations,
      exactlyOne({ connector: 'bank.wire' }),
    ],
  });

  const report = await chaosRun(scen, async (wrappedConnectors) => {
    // Two reactor ticks: dispatch (gets UNKNOWN), then reconcile.
    const r1 = await dispatchOutboxOnce(svc, {
      connectors: wrappedConnectors, claimer: 'd-1',
    });
    assert.ok(r1.some((x) => x.outcome === 'unknown'), JSON.stringify(r1));
    // The bank's ledger has exactly one wire (the inner call landed).
    assert.equal(bank.ledger.size, 1);
    // Reconcile against the unwrapped bank — same as Python e2e.
    const r2 = await reconcileOnce(svc, { connectors: { 'bank.wire': bank } });
    assert.ok(r2.some((x) => x.outcome === 'confirmed'), JSON.stringify(r2));
  }, { svc, connectors: { 'bank.wire': bank } });

  assert.equal(report.passed, true, JSON.stringify(report));
  assert.equal(bank.ledger.size, 1);
});

// ── invariant API uniformity (matches the Python SDK fix) ─────────────────

test('Invariant callable uniformly', () => {
  // Both `noStuckObligations` and `noStuckObligations()` work — calling
  // a parameter-free invariant returns the same singleton.
  const bare = noStuckObligations;
  const called = noStuckObligations();
  assert.equal(bare, called);
  assert.throws(
    () => noStuckObligations('oops'),
    /takes no construction arguments/,
  );
});
