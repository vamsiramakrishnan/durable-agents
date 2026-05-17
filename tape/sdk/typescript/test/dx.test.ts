// Smoke tests for the standalone-DX additions (durable / outbox /
// connectors / obs / tenancy). No tape-server required.

import { test } from 'node:test';
import { strict as assert } from 'node:assert';

import {
  outboxTool, isOutboxEnvelope, outboxMetaOf, OutboxConfigError,
  ConnectorRegistry, LogConnector,
  logJson, ALL_SPANS, SPAN_BEGIN_EFFECT,
  tenancyDefaults, tenancyFromObject, warnIfHardButUnenforced,
} from '../src/index.ts';

test('outboxTool rejects unsafe non_idempotent', () => {
  assert.throws(
    () => outboxTool(
      ({ x }: { x: string }) => ({ x }),
      { name: 'wire', connector: 'bank.wire', semantics: 'non_idempotent' },
    ),
    OutboxConfigError,
  );
});

test('outboxTool produces envelope + business_key', () => {
  const wire = outboxTool(
    ({ account, amount, beneficiary }: { account: string; amount: number; beneficiary: string }) =>
      ({ account, amount, beneficiary }),
    {
      name: 'wire_money',
      connector: 'bank.wire',
      semantics: 'non_idempotent',
      businessKey: (p) => `${p.account}:${p.amount}`,
      waitForResult: true,
    },
  );
  const env = wire({ account: 'ACME-1', amount: 100, beneficiary: 'bob' });
  assert.equal(env.__outbox__, true);
  assert.equal(env.connector, 'bank.wire');
  assert.equal(env.tool, 'wire_money');
  assert.equal(env.business_key, 'ACME-1:100');
  assert.equal(env.wait_for_result, true);
  assert.deepEqual(env.payload, { account: 'ACME-1', amount: 100, beneficiary: 'bob' });
  assert.ok(isOutboxEnvelope(env));
  const meta = outboxMetaOf(wire);
  assert.equal(meta?.connector, 'bank.wire');
  assert.equal(meta?.semantics, 'non_idempotent');
});

test('outboxTool rejects async + non-object returns', () => {
  // Non-object return is enforced at call time.
  const bogus = outboxTool((_: any) => 'not-an-object' as any, {
    name: 'b', connector: 'c', semantics: 'idempotent',
  });
  assert.throws(() => bogus({}), OutboxConfigError);
});

test('ConnectorRegistry round-trips', async () => {
  const r = new ConnectorRegistry();
  const c = new LogConnector('/tmp/tape-ts-test.jsonl');
  r.register('log', c);
  assert.throws(() => r.register('log', c), /already registered/);
  assert.equal(r.get('log'), c);
  assert.throws(() => r.get('missing'), /unknown connector/);
  const res = await c.dispatch({
    runId: 'r1', idempotencyKey: 'k1', toolName: 't',
    connector: 'log', payload: { x: 1 },
  });
  assert.equal(res.outcome, 'confirmed');
});

test('logJson emits JSON with canonical ordering', () => {
  // Capture stderr.
  const chunks: string[] = [];
  const orig = process.stderr.write.bind(process.stderr);
  (process.stderr as any).write = (chunk: any) => { chunks.push(String(chunk)); return true; };
  try {
    logJson('hello', { run_id: 'r1', app_name: 'a', reactor: 'recovery', extra: 'x' });
  } finally {
    process.stderr.write = orig;
  }
  const line = chunks.join('');
  const parsed = JSON.parse(line.trim());
  assert.equal(parsed.run_id, 'r1');
  assert.equal(parsed.app_name, 'a');
  assert.equal(parsed.reactor, 'recovery');
  assert.equal(parsed.extra, 'x');
});

test('span constants', () => {
  assert.ok(ALL_SPANS.includes(SPAN_BEGIN_EFFECT));
  assert.equal(ALL_SPANS.length, 11);
});

test('tenancy warns on hard mode', () => {
  const t = tenancyFromObject({ mode: 'hard_multi_tenant', tenantId: 'x' });
  assert.equal(warnIfHardButUnenforced(t).length, 1);
  assert.equal(warnIfHardButUnenforced(tenancyDefaults()).length, 0);
});
