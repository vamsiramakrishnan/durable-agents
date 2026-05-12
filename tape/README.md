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
# 1. build & start the server (the store is chosen by URL — that's the whole "wiring")
cd tape/server && cargo build --release
./target/release/tape-server --listen 127.0.0.1:7878 --store sqlite:./tape.db &

# 2. install the Python SDK
pip install -e ../sdk/python

# 3. run the treasury example
cd ../examples && PYTHONPATH=../sdk/python python -m treasury.run --reset
```

Or, with [`just`](https://github.com/casey/just):

```bash
just build      # cargo build + pip install -e
just demo       # start the server, run the treasury example
just demo-resume  # run, kill mid-wire, recover — see one wire, not two
just test       # cargo test + the kill-and-resume integration test
```

## Choose your store — and scale out

The backend is a URL in `TAPE_STORE` (or `--store`). Nothing above the server
changes when you switch it:

| `TAPE_STORE` | backend |
|---|---|
| `sqlite:./tape.db` *(default)* | file-backed SQLite (pooled, WAL) — single node, dev, small prod |
| `sqlite::memory:` / `memory` | ephemeral in-process — tests, demos |
| `postgres://user:pass@host:5432/db` | pooled PostgreSQL — production / horizontally scalable |
| `alloydb://user:pass@host:5432/db` | AlloyDB — it's PostgreSQL-wire-compatible; run the AlloyDB Auth Proxy and point at `127.0.0.1:5432`, or use a private-IP host |
| `bigtable://project/instance/table` | Cloud Bigtable — single-row atomic mutations over one column family `m`; `BIGTABLE_EMULATOR_HOST` honoured. Create the table first: `cbt -project P -instance I createtable tape && … createfamily tape m && … setgcpolicy tape m maxversions=1`. Row-key design + the two Bigtable caveats (explicit table creation; `AppendEvent` isn't a cross-row txn) are in `server/src/store/bigtable.rs` |

Tape's logical operations are a trait, `RunStore` (`server/src/store/`); the SQL
backends (`SqlRunStore` — SQLite, PostgreSQL, AlloyDB) share one set of portable
SQL, and the Bigtable backend implements the same trait over single-row
mutations. `tape/tests/test_bigtable.py` runs the treasury kill-and-resume
scenario against `bigtable://` on the emulator (it self-bootstraps `cbtemulator`
+ `cbt` if they're on PATH / in `/tmp/gobin`). With a network store, the server
is **stateless between requests** — run *N* replicas behind a load balancer
(`docker compose up --scale tape-server=3`, `tape/deploy/k8s/tape.yaml`, an HPA).
Safe with no extra coordination: "one driver per run at a time" is the per-run
lease in `tape_runs`, and every mutating RPC is idempotent, so two recovery
workers racing is harmless — the loser short-circuits. See
[`design-principles/tape.md`](../design-principles/tape.md) §12.

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
`UNKNOWN`. Inline non-determinism a tool body smuggles in (`time.time()`, a
random id, a file read) goes through `tape.now()` / `tape.uuid()` / `tape.random()`
/ `tape.sample(tool_context, fn, *args)` — "make any call an activity": called
once per run, journaled, replayed on a re-drive.

After a crash, run the **reactors** — they watch the journal (the WAL) and react:
`recovery` re-drives recoverable runs; `reconciler` resolves UNKNOWN effects via
the registered status checks; `timers` fires due timers (gate timeouts, delayed
re-drives, …). Run them as a sidecar:

```bash
tape-reactors --runner-from my_app:build_runner --url tape://tape:7878
# or:  python -c "import tape.reactors, my_app; tape.reactors.run_reactors(runner=my_app.build_runner())"
```

Each reactor is idempotent (the lease + replay properties make a double-run
harmless), so run as many copies as you like. `tape.set_timer(run_id=…, fire_at_ms=…,
kind="gate_timeout"|"redrive"|…)` sets a durable "wake me at T"; `SubscribeEvents`
(via `tape.reactors.run_event_fanout(url, sink=…)`) tails the cross-run WAL — wire
`sink` to Pub/Sub / Kafka / a webhook to publish it. (On Bigtable the cross-run
tail is "use Bigtable change streams"; the per-run `SubscribeRun` feed still works.)

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
