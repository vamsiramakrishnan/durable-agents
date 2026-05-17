# tape-ts

The TypeScript / Node SDK for [Tape](../../../design-principles/tape.md) — a
durable-execution substrate for ADK agents.

```bash
cd tape/sdk/typescript
npm install
npm test         # spawns the Rust tape-server (in-memory) and round-trips the lifecycle
```

```ts
import { TapeClient, RunStatus, EffectStatus, effect } from 'tape-ts';

const c = new TapeClient('tape://localhost:7878');
const run = await c.beginRun({ appName: 'treasury', userId: 'cfo',
  sessionId: '2026-05-11', invocationId: 'inv-1', leaseOwner: 'me' });

// short-circuit on a confirmed effect on re-drive
const be = await c.beginEffect({ runId: run.runId, decisionIndex: 0,
  toolName: 'execute_sweep', callIndex: 0, requestJson: '{}' });
if (be.status === EffectStatus.CONFIRMED) {
  return JSON.parse(be.responseJson);
}
// ... call bank.wire(..., idempotency_key=be.idempotencyKey) ...
await c.completeEffect({ runId: run.runId, idempotencyKey: be.idempotencyKey,
  status: EffectStatus.CONFIRMED, responseJson: '{"wire_id":"w1"}' });

// retry-policied tool
const flaky = effect(async (account: string) => callBank(account), {
  retry: { maxAttempts: 5, initialIntervalMs: 200, backoffCoefficient: 2,
           retryOn: [BankBusy] },
});
```

`tapes://host` opens a TLS channel and auto-attaches a Google OIDC ID token (via
`google-auth-library`, an optional dep) for IAM-protected endpoints like Cloud
Run — the caller's SA just needs `roles/run.invoker`. Pass `auth: false`,
`audience`, or `idToken` in the constructor's options to override.

### What's wired

A full `TapeClient` covering every RPC (run lifecycle, decisions, effects with
the dedup short-circuit, obligations, budget, gates, timers, reconciliation, the
WAL tail, sessions), the `@tape.effect`-style annotation + `RetryPolicy`,
reactor helpers (`recoverOnce`, `reconcileOnce`, `fireDueTimersOnce`,
`runReactors`, `runEventFanout`), and `tape://` / `tapes://` URL handling. A
smoke test (`npm test`) round-trips the full lifecycle against a real Rust
`tape-server`.

### What's a scaffold

A `TapePlugin` / `TapeSessionService` for the JS/TS port of ADK — mechanical
work once that port settles; the protocol is the stable surface and the Python
adapter in [`../python/tape/adk/`](../python/tape/adk/) is the reference.

### Re-syncing the proto

```bash
npm run regen-proto   # cp ../../proto/tape.proto -> ./proto/tape.proto
```

The proto is loaded at runtime by `@grpc/proto-loader` (no codegen step). The
local copy in `./proto/` is what ships in the npm package.
