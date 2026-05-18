// Tape quickstart — TypeScript. The same scenario as the Python / Go / Java siblings.
//
//   node --experimental-strip-types --no-warnings examples/quickstart.ts

import { TapeClient, EffectStatus } from '../tape/sdk/typescript/src/index.ts';

const LANG = 'typescript';
const URL  = process.env.TAPE_URL ?? 'tape://127.0.0.1:7878';

async function main(): Promise<number> {
  console.log(`[quickstart/${LANG}] connecting to ${URL}`);
  const c = new TapeClient(URL);
  try {
    const invocation = `qs-${LANG}-${Date.now()}`;
    const run: any = await c.beginRun({
      appName: 'quickstart', userId: 'quickstart-user',
      sessionId: invocation, invocationId: invocation,
      leaseOwner: `qs-${LANG}`, leaseTtlMs: 60_000,
    });
    console.log(`[quickstart/${LANG}] begin_run    → run-id=${run.runId}`);

    await c.recordDecision({
      runId: run.runId, decisionIndex: 0,
      model: 'quickstart', requestJson: '{}', responseJson: '{}',
    });
    console.log(`[quickstart/${LANG}] record_decision  decision_index=0`);

    const be: any = await c.beginEffect({
      runId: run.runId, decisionIndex: 0,
      toolName: 'hello', callIndex: 0,
      requestJson: JSON.stringify({ who: LANG }),
    });
    console.log(`[quickstart/${LANG}] begin_effect   → key=${be.idempotencyKey}  status=${be.status}`);

    await c.completeEffect({
      runId: run.runId, idempotencyKey: be.idempotencyKey,
      status: EffectStatus.CONFIRMED,
      responseJson: JSON.stringify({ ok: true, who: LANG }),
    });
    console.log(`[quickstart/${LANG}] complete_effect → status=CONFIRMED`);

    const eff: any = await c.getEffect({ runId: run.runId, idempotencyKey: be.idempotencyKey });
    console.log(`[quickstart/${LANG}] get_effect     status=${eff.effect.status}  response=${eff.effect.responseJson}`);
  } finally {
    c.close();
  }
  return 0;
}

main().then(
  (rc) => process.exit(rc),
  (ex) => { console.error(`quickstart/${LANG}: ${ex?.message ?? ex}`); process.exit(1); },
);
