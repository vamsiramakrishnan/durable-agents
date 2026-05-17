# Cheat sheet

Everything Tape on one page. Copy, paste, ship.

## Install

```bash
pip install -e tape/sdk/python    # tape-py
pip install -e tape/cli           # tape
```

## Wire an ADK agent in 15 lines

=== ":material-language-python: Python"

    ```python
    import tape
    from tape.adk import durable_app

    app, runner = durable_app(
        name="treasury",
        agent=root_agent,
        budget=tape.Budget(usd_cap=50, token_cap=2_000_000),
    )
    ```

=== ":simple-go: Go"

    ```go
    import tape "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go"

    d, _ := tape.NewDurableApp(ctx, tape.DurableConfig{
        Name:   "treasury",
        Budget: tape.Budget{USDCap: 50, TokenCap: 2_000_000},
    })
    defer d.Close()
    ```

=== ":material-language-typescript: TypeScript"

    ```ts
    import { durableApp } from 'tape-ts';

    const app = durableApp({
      name: 'treasury',
      budget: { usdCap: 50, tokenCap: 2_000_000 },
    });
    ```

=== ":fontawesome-brands-java: Java"

    ```java
    DurableApp app = DurableApp.wire(new DurableApp.Config()
        .name("treasury")
        .budget(new DurableApp.Budget(50.0, 2_000_000)));
    ```

## Decorate a tool

```python
# Idempotent upstream — bank accepts an idempotency key.
@tape.effect(compensate=reverse_wire, status_check=bank.wire_status)
def execute_sweep(account, amount, target, tool_context):
    key = tape.idempotency_key(tool_context)
    return {"wire_id": bank.wire(account, amount, target, idempotency_key=key)}

# Non-idempotent upstream — return intent; outbox owns dispatch.
@tape.outbox_tool(
    connector="bank.wire",
    business_key=lambda account, amount, date, **_: f"{account}:{amount}:{date}",
    status_check=find_wire,
    compensate=reverse_wire,
)
def wire_money(account, amount, beneficiary, date):
    return {"account": account, "amount": amount,
            "beneficiary": beneficiary, "date": date}
```

## Register a connector

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

## Tool-context helpers

```python
tape.idempotency_key(tool_context)   # "r-abc/d-3/wire/0"
tape.run_id_of(tool_context)
tape.business_key(tool_context, ...)
tape.external_ref(tool_context)
tape.is_cancelled(tool_context)
tape.heartbeat(tool_context)         # extend lease in long tool bodies
tape.now(tool_context)               # journalled wall-clock
tape.uuid(tool_context)              # journalled UUID
tape.sample(tool_context, fn)        # journal an arbitrary read
tape.policy_is(tool_context, "v")
```

## Run lifecycle (operator)

```python
import tape

tape.cancel_run("r-...", reason="user-requested")
tape.compensate_run("r-...")          # LIFO over confirmed effects
tape.redrive("r-...")                 # wake the run up
```

## Reactive KV

```python
tape.set_value(ns="treasury/sweep", key="status", value="confirming",
               if_version=1)        # CAS optional
tape.get_value(ns="treasury/sweep", key="status")
for evt in tape.watch_value(ns="treasury/sweep", key="status", from_version=0):
    # evt.prev_value, evt.value; evt.deleted on tombstone
    ...
tape.delete_value(ns="treasury/sweep", key="status")
```

## Signals & gates

```python
@tape.gate_tool("approval", timeout_ms=15 * 60 * 1000)
async def approval_gate(tool_context):
    return await tape.await_signal(tool_context, "approval")

# From outside the run:
tape.send_signal(run_id="r-...", name="approval", payload={"approved_by": "cfo"})
```

## Timers

```python
tape.set_timer(run_id="r-...", fire_at_ms=t, kind="periodic",
               payload={"shift": "morning"})
tape.cancel_timer(timer_id="t-...")
```

## Budget

```python
budget = tape.Budget(usd_cap=50, token_cap=2_000_000)
# tape.adk.durable_app(..., budget=budget) — TapePlugin admits before each
# model call and charges after.
```

## CLI — most-used commands

```bash
tape init my-agent                              # scaffold a project
tape dev                                         # local: server + reactors + agent
tape dev --kill-resume-demo                      # crash mid-effect; verify one wire
tape doctor                                      # tick/cross local diagnostic
tape doctor --gcp                                # same for GCP
tape provision gcp --target cloud-run --apply    # render & apply Terraform
tape deploy gcp --target cloud-run               # build + push + render specs
tape status                                       # runs / effects / lag
tape logs --follow                                # tail Cloud Logging
tape migrate                                      # schema migrations
```

The full CLI reference is at [reference / cli](cli/index.md).

## Statuses you'll see

```
RunStatus       : RUNNABLE | RUNNING | WAITING | TERMINAL | FAILED | STUCK | CANCELLED
EffectStatus    : PENDING  | CONFIRMED | FAILED | UNKNOWN
EffectSemantics : IDEMPOTENT | NON_IDEMPOTENT | OBSERVE_ONLY
EffectDispatch  : INLINE | OUTBOX
Resolution      : CONFIRMED | FAILED | ABSENT | DUPLICATE | STUCK
```

## Span names (OTel)

```
tape.begin_run             tape.resume_run
tape.record_decision
tape.begin_effect          tape.complete_effect
tape.reconcile_effect      tape.dispatch_effect
tape.compensate            tape.redrive
tape.await_signal          tape.send_signal
```

## Log-based metrics (Cloud Monitoring)

```
tape/runs/running           tape/runs/stuck
tape/effects/unknown
tape/obligations/unresolved
tape/reactor/lag_ms
```

## Storage backend URLs

```
sqlite:./tape.db
sqlite::memory:
postgres://user:pass@host/db
alloydb://...
bigtable://project/instance/table
spanner://project/instance/database     # experimental — gated
```

## Reactor enables

```yaml
agent:
  runner_factory: app.agent:build_runner
tape:
  reactors:
    recovery:     {enabled: true}
    reconciler:   {enabled: true}
    outbox:       {enabled: true}
    timers:       {enabled: true}
    compensation: {enabled: true}
```

## Tenancy modes

```yaml
tenancy:
  mode: single                  # single | trusted_multi_app | hard_multi_tenant
  tenant_id: default
```

## What to alert on

- `tape/runs/stuck > 0` for > 5m → escalate.
- `tape/effects/unknown` rising for > 10m → reconciler can't resolve.
- `tape/obligations/unresolved > 0` for > 15m → compensation failing.
- `tape/reactor/lag_ms > 60_000` → reactor starved.

## See also

- [Quickstart](../quickstart.md) — 10 minutes, end-to-end.
- [Concepts](../concepts/index.md) — the mental model.
- [Glossary](../help/glossary.md) — every Tape-specific term in one place.
