# Tape

A **durable-execution substrate for ADK agents**. Tape is the runtime the
treatise [*When the Orchestrator Isn't Code*](../design-principles/agents-that-act-treatise.md)
argues for, scoped to one framework — Google's
[Agent Development Kit](https://google.github.io/adk-docs/) — and built as a
*separate system*: a high-concurrency, low-latency server (Rust, Postgres- or
SQLite-backed) with a language-agnostic gRPC protocol and thin SDKs that plug
into ADK **with no changes to ADK**, riding only on extension points ADK already
exposes (the plugin system, custom `SessionService`s, `LongRunningFunctionTool`,
`invocation_id`-based resume).

The design is [`design-principles/tape.md`](../design-principles/tape.md). This
directory is the implementation.

```
tape/
  proto/tape.proto            the contract
  server/                     the Rust server  (Tokio · Tonic · sqlx-less SQLite store)
  sdk/python/                 tape-py — the reference SDK + ADK adapter (TapePlugin, TapeSessionService)
  sdk/{typescript,go,java}/   generated clients + ADK-adapter scaffolds (the protocol is the contract)
  examples/treasury/          the treatise's treasury agent, Tape-backed
  tests/                      the kill-and-resume integration test
  docker-compose.yml          Postgres + the Tape server, for local runs
  justfile                    build · test · demo
```

## Quick start

```bash
# 1. build & start the server
cd tape/server && cargo build --release
./target/release/tape-server --listen 127.0.0.1:7878 --db ./tape.db &

# 2. install the Python SDK
pip install -e ../sdk/python

# 3. run the treasury example
cd ../examples && PYTHONPATH=../sdk/python python -m treasury.run --reset
```

Or, with [`just`](https://github.com/casey/just):

```bash
just build      # cargo build + pip install -e
just demo       # start the server, run the treasury example
just test       # cargo test + the kill-and-resume integration test
```

## Wiring Tape into your agent (two lines)

```python
from google.adk.runners import Runner
from google.adk.apps import App
from google.adk.apps.app import ResumabilityConfig
from tape.adk import TapePlugin, TapeSessionService
import tape

app = App(name="treasury", root_agent=agent,
          plugins=[TapePlugin(budget=tape.Budget(usd_cap=50, token_cap=2_000_000))],
          resumability_config=ResumabilityConfig(is_resumable=True))
runner = Runner(app=app, session_service=TapeSessionService("tape://localhost:7878"))
```

Your tool bodies stay plain; `@tape.effect(compensate=..., status_check=...)` is
optional sugar for declaring an inverse and how the reconciler resolves an
`UNKNOWN`. After a crash, `tape.recover_once(runner=runner)` re-drives every
recoverable run — ADK reconstructs the agent, Tape replays the recorded decisions
and short-circuits the confirmed effects, the run finishes once.

There is also a zero-touch mode for an app you'd rather not edit:

```bash
tape run -- python my_adk_app.py        # monkeypatches Runner to inject the plugin + session service
```

## What "with no changes to ADK" means

| Tape primitive | ADK door it rides on |
|---|---|
| record a decision; replay it on re-drive | `Plugin.before_model_callback` / `after_model_callback` |
| journal an effect; skip a confirmed one | `Plugin.before_tool_callback` / `after_tool_callback` / `on_tool_error_callback` |
| mirror the conversation + journal, one txn | a custom `BaseSessionService` (`TapeSessionService.append_event`) |
| action gate = durable suspend-until-signal | `LongRunningFunctionTool` + `SessionService.append_event` |
| budget admit/charge | the `before_*` / `after_*` callbacks |
| run identity & resumption | ADK's `invocation_id` (`runner.run_async(..., invocation_id=...)`) |

## License

[Apache 2.0](../LICENSE).
