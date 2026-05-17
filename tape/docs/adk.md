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

## See also

- [**Concepts: effects & idempotency**](concepts/effects.md) — what the
  decorators *mean*.
- [**Non-idempotent upstreams**](non-idempotent-upstreams.md) — the
  outbox pattern, end-to-end, with a real bank example.
- [**Custom connector**](how-to/custom-connector.md) — when one of the
  built-ins doesn't fit your upstream.
- [**Reactors**](reactors.md) — how the runner factory is used during
  recovery.
