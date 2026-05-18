# tape-ts

The TypeScript / Node SDK for [Tape](../../../design-principles/tape.md) — a
durable-execution substrate for ADK agents.

|                                                            |
|------------------------------------------------------------|
| **Install** · `npm install tape-ts` *(or, from a clone: `cd tape/sdk/typescript && npm install`)* |
| **30-second example** · the snippet below ↓                |
| **Reference** · <https://vamsiramakrishnan.github.io/durable-agents/reference/typescript/> |
| **What's wired** · `TapeClient` + every RPC, `@effect`, `outboxTool`, `durableApp`, connectors, **outbox dispatcher** (`tape-outbox-ts`), **sinks** (Log/Webhook/PubSub) |
| **Parity** · idiom parity with Python · see [`SDK_PARITY.md`](../../../SDK_PARITY.md) |
| **Contribute** · `make sdk-test-ts` · `make sdk-parity` · [`CLAUDE.md`](../../../CLAUDE.md) |

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

### Reactions (event bus)

The TS SDK exposes the event-bus surface in [`design-principles/tape-event-bus.md`](../../../design-principles/tape-event-bus.md):
register a server-side reaction (subject pattern + optional CEL predicate +
handler kind), claim and run tasks via the in-proc dispatcher, or forward
PUBLISH-kind tasks to a Cloud Pub/Sub topic.

```ts
import { on, onValueChange, registerAll, runDispatcher } from 'tape-ts';

// Declare reactions at startup. Nothing is sent to the server yet.
on('/tape/effect/failed/**', async ({ task, payload }) => {
  console.error('effect failed:', task.subject, payload);
}, { maxConcurrency: 4, retryMax: 3, dlqAfterN: 3 });

onValueChange('treasury', 'fx_rate', async ({ payload }) => {
  await repriceBook(payload.value.value_json);
}, { predicate: 'double(payload.value.value_json) > 1.10',
     maxConcurrency: 8, debounceMs: 500 });

// Push to the server, then run the in-proc dispatcher loop.
await registerAll({ url: 'tape://localhost:7878' });
await runDispatcher({ url: 'tape://localhost:7878', register: false });
```

For low-level access, `TapeClient` exposes every new RPC directly
(`registerReaction`, `deregisterReaction`, `listReactions`, `claimTasks`,
`completeTask`, `nackTask`, `listTasks`, `subscribeBySubject`) plus the new
`HandlerKind` / `TaskStatus` enums. A `runPubSubBridge({ project, topic })`
helper bridges PUBLISH-kind tasks to Pub/Sub (lazy-imports
`@google-cloud/pubsub`).

### Standalone DX — parity with `tape-py`

The TS SDK ships the same standalone-DX surface as Python's
`tape.adk.durable_app` / `@tape.outbox_tool` / `tape.connectors` /
`tape.obs` / `tape.tenancy`.

#### `durableApp(...)` — the wiring entrypoint

```ts
import { durableApp } from 'tape-ts';

const app = durableApp({
  name: 'treasury',
  budget: { usdCap: 50, tokenCap: 2_000_000 },
});
try {
  const run = await app.client.beginRun({
    appName: app.name, userId: 'cfo', sessionId: 's1',
    invocationId: 'inv-1', leaseOwner: app.leaseOwner,
  });
} finally {
  await app.close();
}
```

`durableApp` honours `$TAPE_URL` and `$TAPE_LEASE_MS`. When a TS ADK port
lands, its `Runner` constructor will accept the `DurableApp` directly.

#### `outboxTool(...)` — non-idempotent upstreams, enforced

```ts
import { outboxTool, isOutboxEnvelope } from 'tape-ts';

const wire = outboxTool(
  ({ account, amount, beneficiary, date }: {
    account: string; amount: number; beneficiary: string; date: string;
  }) => ({ account, amount, beneficiary, date }),
  {
    name: 'wire_money',
    connector: 'bank.wire',
    semantics: 'non_idempotent',
    businessKey: (p) => `${p.account}:${p.amount}:${p.date}`,
    waitForResult: true,
  },
);
// `semantics: 'non_idempotent'` without businessKey / statusCheck /
// compensate / humanGate throws OutboxConfigError at decoration time.

const env = wire({ account: 'ACME-1', amount: 100_000,
                   beneficiary: 'MMF-A', date: '2026-05-17' });
isOutboxEnvelope(env);  // true
```

#### Capability connectors

```ts
import { CONNECTORS, HttpConnector, PubSubConnector } from 'tape-ts';

CONNECTORS.register('bank.wire', new HttpConnector({
  url: 'https://bank.example/wires',
  observeUrl: 'https://bank.example/wires/lookup',
  compensateUrl: 'https://bank.example/wires/reverse',
}));
// Built-ins: LogConnector, HttpConnector, PubSubConnector
// (`@google-cloud/pubsub` is lazy-imported), CloudTasksConnector
// (`@google-cloud/tasks` is lazy-imported).
```

#### Observability + tenancy

```ts
import { logJson, setSpanHook, SPAN_DISPATCH_EFFECT,
         tenancyFromObject, warnIfHardButUnenforced } from 'tape-ts';

logJson('effect.dispatched', { run_id: 'r-1', tool: 'wire_money', reactor: 'outbox' });

setSpanHook((name, attrs) => {
  // open a span via your tracer; return its end callback
  return () => { /* close it */ };
});

const t = tenancyFromObject({ mode: 'hard_multi_tenant', tenantId: 'x' });
for (const w of warnIfHardButUnenforced(t)) console.warn(w);
```

### Re-syncing the proto

```bash
npm run regen-proto   # cp ../../proto/tape.proto -> ./proto/tape.proto
```

The proto is loaded at runtime by `@grpc/proto-loader` (no codegen step). The
local copy in `./proto/` is what ships in the npm package.

## Parity

The Python SDK is the reference; this SDK aims for **idiom parity** (not
verbatim parity). See [`../../../SDK_PARITY.md`](../../../SDK_PARITY.md) for
the live scorecard. G1 (outbox daemon), G2 (Webhook/PubSub sinks), and G3
(cross-SDK parity harness) are now green — the TS dispatcher is shipped as
`bin/tape-outbox-ts.ts` (`npm run outbox`) and `src/outbox_reactor.ts`; the
sinks live in `src/sinks.ts`.

## Contribute

`make sdk-test-ts` runs the round-trip test; `make sdk-parity` runs the
cross-SDK parity harness (drives the same scenario through Python/TS/Go/Java
and asserts identical journal state). See
[`../../../CLAUDE.md`](../../../CLAUDE.md).
