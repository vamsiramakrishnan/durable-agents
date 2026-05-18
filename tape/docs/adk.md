# ADK on Tape — `durable_app(...)` in 15 lines

The wiring entrypoint. One call, four guarantees:

- Every model call is journalled (so a re-driven run doesn't re-ask the
  model for choices it already made).
- Every tool call is an [**effect**](concepts/effects.md) with a declared
  semantics and status.
- The `Runner` reconnects to the same session on re-drive — ADK's
  resumability is on.
- The [**budget**](reference/cheatsheet.md#budget) is enforced *before*
  each costed boundary and journalled *after*.

```python
import tape
from tape.adk import durable_app

root_agent = ...   # your ADK LlmAgent

app, runner = durable_app(
    name="treasury",
    agent=root_agent,
    budget=tape.Budget(usd_cap=50, token_cap=2_000_000),
)
```

That's the whole wiring. → [`durable_app` reference](reference/python/durable.md)

## The two tool patterns

### `@tape.effect` — idempotent tools

For tools whose body performs IO and is **idempotent on `(run_id,
idempotency_key)`** — typically because the upstream supports an idempotency
key.

```python
@tape.effect(compensate=reverse_wire, status_check=bank.wire_status)
def execute_sweep(account_id, amount_minor, target_mmf, tool_context):
    key = tape.idempotency_key(tool_context)
    return {"wire_id": bank.wire(account_id, amount_minor, target_mmf,
                                  idempotency_key=key)}
```

### `@tape.outbox_tool` — non-idempotent upstreams

For tools whose upstream **cannot** be made naturally idempotent (a SWIFT wire,
a one-shot side effect, anything where a double-fire is destructive). The tool
body **returns an intent payload only**; the outbox reactor performs the dispatch.

```python
@tape.outbox_tool(
    connector="bank.wire",
    business_key=lambda account, amount, date, **_: f"{account}:{amount}:{date}",
    status_check=find_wire,
    compensate=reverse_wire,
)
def wire_money(account: str, amount: int, beneficiary: str, date: str):
    return {"account": account, "amount": amount,
            "beneficiary": beneficiary, "date": date}
```

The decorator **rejects** at decoration time any non-idempotent tool that lacks
`business_key`, `status_check`, or `compensate` (override with
`allow_unsafe=True` after explicit review). The whole point is that an UNKNOWN
outcome can be resolved — without one of those, there is no safe path forward.
The server enforces the same contract at `BeginEffect`-time.

## Capability connectors

The outbox reactor dispatches via a named **connector**. Register them at
project import time — the scaffold's `app/connectors.py` shows the pattern:

```python
from tape import connectors
from tape.connectors.http import HTTPConnector

connectors.register(HTTPConnector(
    name="bank.wire",
    endpoint="https://bank.example/wires",
    observe_endpoint="https://bank.example/wires/lookup",
    compensate_endpoint="https://bank.example/wires/reverse",
))
```

Built-ins: `HTTPConnector` (POST + `X-Tape-*` headers; `urllib`-based, no extra
deps), `PubSubConnector` (publish to a topic + the matching subscriber helper).
Implement your own by implementing the `EffectConnector` protocol — `dispatch`,
`observe`, `compensate`.

## Running the reactor

The reactor process needs to be able to rebuild your `Runner`. Provide a
`build_runner` factory:

```python
def build_runner():
    _app, runner = durable_app(
        name="treasury",
        agent=_build_root_agent(),  # fresh agent each call
        budget=tape.Budget(usd_cap=50),
    )
    return runner
```

Then:

```bash
tape-reactors --runner-from app.agent:build_runner --url tape://localhost:7878
```

or, in `tape.yaml`, set `agent.runner_factory: app.agent:build_runner` and
`tape dev` / `tape deploy gcp` wire it through.

## Java { #java }

Google's [Java ADK](https://google.github.io/adk-docs/) ships at
`com.google.adk:google-adk` on Maven Central. Tape's Java adapter mirrors
the Python surface — `TapePlugin` + `TapeSessionService`, with a
convenience wiring entrypoint.

The dep is `provided`-scope in `tape-java`, so non-ADK callers of
`TapeClient` aren't forced to pull it in. Add `google-adk` to your agent's
pom alongside `tape`:

```xml
<dependency>
  <groupId>dev.tape</groupId>
  <artifactId>tape</artifactId>
  <version>0.1.0</version>
</dependency>
<dependency>
  <groupId>com.google.adk</groupId>
  <artifactId>google-adk</artifactId>
  <version>1.2.0</version>
</dependency>
```

### `TapeAdkApp.wire(...)` — the Java port of `durable_app`

```java
import dev.tape.DurableApp;
import dev.tape.adk.TapeAdkApp;
import com.google.adk.agents.Runner;

try (TapeAdkApp app = TapeAdkApp.wire(
        new DurableApp.Config()
            .name("treasury")
            .budget(new DurableApp.Budget(50.0, 2_000_000)))) {

    Runner runner = Runner.builder(rootAgent)
        .plugins(List.of(app.plugin()))
        .sessionService(app.sessionService())
        .build();

    runner.runAsync(/* ... */).blockingSubscribe();
}
```

`TapeAdkApp.wire(...)` returns a bundle that shares one `TapeClient`
between the plugin and the session service — closing the bundle closes
the client.

### What the plugin wires

| ADK callback              | Tape RPC                                       |
|---------------------------|------------------------------------------------|
| `beforeRunCallback`       | `BeginRun`                                     |
| `afterModelCallback`      | `RecordDecision`                               |
| `beforeToolCallback`      | `BeginEffect` (with CONFIRMED short-circuit)   |
| `afterToolCallback`       | `CompleteEffect(CONFIRMED)`                    |
| `onToolErrorCallback`     | `CompleteEffect(FAILED)`                       |
| `afterRunCallback`        | `EndRun(TERMINAL)`                             |

Position is by call order: the k-th model call is `decision_index = k-1`;
each tool call is keyed to the most-recent decision plus a per-(decision,
tool) call index — same alignment as the Python adapter. The plugin is
safe to share across concurrent invocations (per-invocation bookkeeping
is keyed by `invocationId`).

### `TapeSessionService` — the persistence seam

Implements ADK Java's `BaseSessionService` over Tape's gRPC. The in-memory
base contract runs first (state-delta application, `temp:` filter,
last-update bookkeeping); **committed events** (`partial != true`) then
persist to Tape in one round-trip. Partial streaming events stay in
memory.

```java
TapeClient client = new TapeClient("tape://localhost:7878");
BaseSessionService sessionService = new TapeSessionService(client);

Session created = sessionService.createSession("treasury", "cfo",
    new ConcurrentHashMap<>(), /* sessionId */ "")
    .blockingGet();

Session fetched = sessionService.getSession("treasury", "cfo",
    created.id(), Optional.empty()).blockingGet();
```

### Status (G4 in the parity matrix)

Shipped: wiring contract — `BeginRun`, `RecordDecision`, `BeginEffect` /
`CompleteEffect`, `EndRun`, session persistence.

Follow-ups tracked in
[`SDK_PARITY.md`](https://github.com/vamsiramakrishnan/durable-agents/blob/main/SDK_PARITY.md):
model replay (short-circuit a recorded `LlmResponse` on re-drive) and
budget admit/charge. Both are additive and don't change the wiring
contract above.

## TypeScript & Go

Tape's TS and Go SDKs ship `DurableApp` / `OutboxTool` / connector
registries that mirror the Python surface (see the
[wiring table in `tape/README.md`](https://github.com/vamsiramakrishnan/durable-agents/blob/main/tape/README.md#same-dx-in-go-typescript-and-java)).
There's no Google ADK port for TypeScript or Go at the time of writing —
when there is, the adapter shape will be the same as the Java one: a
plugin + a session service over the existing `TapeClient`.

For now, TS / Go agents talk to Tape directly via the
[`TapeClient`](reference/typescript/index.md) /
[`tape.Client`](reference/go/index.md) RPCs, or via `durableApp(...)` /
`tape.NewDurableApp(...)` if they want the wiring helper without the
framework adapter.

## See also

- [**Concepts: effects & idempotency**](concepts/effects.md) — what the
  decorators *mean*.
- [**Non-idempotent upstreams**](non-idempotent-upstreams.md) — the
  outbox pattern, end-to-end, with a real bank example.
- [**Custom connector**](how-to/custom-connector.md) — when one of the
  built-ins doesn't fit your upstream.
- [**Reactors**](reactors.md) — how the runner factory is used during
  recovery.
- [**Outbox dispatcher in any language**](how-to/outbox-daemon.md) — the
  same dispatch loop in Python, TS, Go, and Java.
- [**Cross-SDK parity harness**](how-to/cross-sdk-parity.md) — one
  scenario, four languages, identical journal state.
