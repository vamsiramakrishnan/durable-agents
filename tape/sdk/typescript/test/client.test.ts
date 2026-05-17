// End-to-end smoke: spawn the Rust tape-server with an in-memory store, then
// round-trip the full effect lifecycle (begin_run -> record_decision ->
// begin_effect/begin_effect-short-circuit -> complete_effect -> get_effect ->
// register_compensation -> set/admit/charge_budget -> end_run).
// Skips if the tape-server binary isn't built.

import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import * as net from 'node:net';
import { TapeClient, RunStatus, EffectStatus } from '../src/index.ts';

const __dirname = dirname(fileURLToPath(import.meta.url));
const SERVER_BIN = join(__dirname, '..', '..', '..', 'server', 'target', 'debug', 'tape-server');

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const s = net.createServer();
    s.listen(0, '127.0.0.1', () => {
      const p = (s.address() as net.AddressInfo).port;
      s.close(() => resolve(p));
    });
    s.on('error', reject);
  });
}

async function waitFor(host: string, port: number, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const ok = await new Promise<boolean>((r) => {
      const s = net.connect(port, host);
      s.on('connect', () => { s.end(); r(true); });
      s.on('error', () => { r(false); });
    });
    if (ok) return;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`server never came up at ${host}:${port}`);
}

let proc: ChildProcess | null = null;
let url = '';

test.before(async () => {
  if (!existsSync(SERVER_BIN)) {
    test.skip(`tape-server not built — run \`cargo build\` in ${SERVER_BIN}/../..`);
    return;
  }
  const port = await freePort();
  proc = spawn(SERVER_BIN, ['--listen', `127.0.0.1:${port}`, '--store', 'memory'], {
    stdio: 'ignore',
    env: { ...process.env, RUST_LOG: 'tape_server=warn' },
  });
  await waitFor('127.0.0.1', port);
  url = `tape://127.0.0.1:${port}`;
});

test.after(() => {
  if (proc) { proc.kill('SIGTERM'); proc = null; }
});

test('TapeClient round-trips a full effect lifecycle', async () => {
  if (!proc) return; // skipped
  const c = new TapeClient(url);
  try {
    const begun: any = await c.beginRun({ appName: 'a', userId: 'u', sessionId: 'ts-smoke',
      invocationId: 'inv-ts', leaseOwner: 'test', leaseTtlMs: 60_000 });
    assert.equal(begun.resumed, false);
    const rid: string = begun.runId;

    await c.recordDecision({ runId: rid, decisionIndex: 0, model: 'm',
      responseJson: '{"plan":1}', rationale: '', policyVersion: 'p1' });
    const got: any = await c.getDecision({ runId: rid, decisionIndex: 0 });
    assert.equal(got.found, true);

    const be: any = await c.beginEffect({ runId: rid, decisionIndex: 0,
      toolName: 'execute_sweep', callIndex: 0, requestJson: '{}' });
    assert.equal(be.status, EffectStatus.PENDING);
    assert.equal(be.idempotencyKey, `${rid}/decision-0/execute_sweep/0`);

    // second begin -> short-circuits to the existing PENDING (the same effect row)
    const be2: any = await c.beginEffect({ runId: rid, decisionIndex: 0,
      toolName: 'execute_sweep', callIndex: 0, requestJson: '{}' });
    assert.equal(be2.idempotencyKey, be.idempotencyKey);
    assert.equal(be2.status, EffectStatus.PENDING);

    await c.completeEffect({ runId: rid, idempotencyKey: be.idempotencyKey,
      status: EffectStatus.CONFIRMED, responseJson: '{"wire_id":"w1"}' });
    const ge: any = await c.getEffect({ runId: rid, idempotencyKey: be.idempotencyKey });
    assert.equal(ge.found, true);
    assert.equal(ge.effect.status, EffectStatus.CONFIRMED);
    assert.match(ge.effect.responseJson, /wire_id/);

    await c.registerCompensation({ runId: rid, effectKey: be.idempotencyKey,
      kind: 'reverse_wire', payloadJson: '{}' });
    const obs: any = await c.listObligations({ runId: rid, onlyUnresolved: true });
    assert.equal(obs.obligations.length, 1);

    // budget admit/charge
    await c.setBudget({ runId: rid, usdCap: 1.0, tokenCap: 0 });
    let adm: any = await c.admitBudget({ runId: rid, usdEstimate: 0.5 });
    assert.equal(adm.admitted, true);
    await c.chargeBudget({ runId: rid, usd: 0.9 });
    adm = await c.admitBudget({ runId: rid, usdEstimate: 0.5 });
    assert.equal(adm.admitted, false);

    // timer roundtrip
    const tr: any = await c.setTimer({ runId: rid, fireAtMs: Date.now() - 1000,
      kind: 'gate_timeout', payloadJson: '{"gate":"g1"}' });
    const due: any = await c.listDueTimers({ claim: true, limit: 50 });
    assert.ok(due.timers.some((t: any) => t.timerId === tr.timerId));

    await c.endRun({ runId: rid, status: RunStatus.TERMINAL });
    const fresh: any = await c.getRun(rid);
    assert.equal(fresh.status, RunStatus.TERMINAL);

    // a re-begin_run finds the existing (TERMINAL) run
    const again: any = await c.beginRun({ appName: 'a', userId: 'u', sessionId: 'ts-smoke',
      invocationId: 'inv-ts', leaseOwner: 'test', leaseTtlMs: 60_000 });
    assert.equal(again.resumed, true);
    assert.equal(again.runId, rid);
    assert.equal(again.status, RunStatus.TERMINAL);
  } finally { c.close(); }
});
