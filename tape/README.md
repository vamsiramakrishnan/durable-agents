# Tape

A **durable-execution substrate for ADK agents**. Tape is the runtime the
treatise [*When the Orchestrator Isn't Code*](../design-principles/agents-that-act-treatise.md)
argues for, scoped to one framework — Google's
[Agent Development Kit](https://google.github.io/adk-docs/) — and built as a
*separate system*: a high-concurrency, low-latency server (Rust, Postgres- /
SQLite- / Bigtable-backed) with a language-agnostic gRPC protocol and thin SDKs
that plug into ADK **with no changes to ADK**, riding only on extension points
ADK already exposes (the plugin system, custom `SessionService`s,
`LongRunningFunctionTool`, `invocation_id`-based resume).

> Tape is **not** checkpointing Python. Tape is:
> record every model decision · record every external effect intent & result ·
> replay decisions on resume · skip confirmed effects · stop on ambiguity ·
> reconcile reality · compensate when reality disagrees.

The mental model is one journal with semantic projections:

```text
1 append-only execution journal
+
semantic projections:
  - decisions      — memory of reasoning
  - effects        — memory of reality
  - obligations    — memory of responsibility
  - timers · gates · budgets · reactive KV
```

> The WAL tells you what happened. **The projections tell you what is true now.**

The design is [`design-principles/tape.md`](../design-principles/tape.md). The
canonical engineering explanation is [`docs/architecture.md`](docs/architecture.md).
This directory is the implementation.

```
tape/
  proto/tape.proto              the contract
  server/                       the Rust server  (Tokio · Tonic · sqlx-less SQLite store)
  sdk/python/                   tape-py — the reference SDK + ADK adapter (TapePlugin, TapeSessionService, durable_app)
  sdk/{typescript,go,java}/     generated clients + ADK-adapter scaffolds (the protocol is the contract)
  cli/                          tape-cli — the standalone DX (`tape init|dev|doctor|provision|deploy`)
  deploy/gcp/terraform/         reusable Terraform/OpenTofu modules for GCP
  deploy/gcp/k8s/chart/         Helm chart for GKE Autopilot
  docs/                         journey-shaped docs (quickstart, adk, local-dev, gcp-cloud-run, ...)
  examples/standalone/          self-contained scaffolds: hello-durable-adk, non-idempotent-bank-outbox, ...
  examples/treasury/            the treatise's treasury agent, Tape-backed
  tests/                        the kill-and-resume integration test
  docker-compose.yml            Postgres + the Tape server, for local runs
  justfile                      build · test · demo
```

## The standalone DX

Start here for new projects. See [`docs/quickstart.md`](docs/quickstart.md).

```bash
pip install -e tape/sdk/python      # tape-py: the SDK + ADK adapter
pip install -e tape/cli             # tape: the CLI

tape init treasury                  # scaffold a new project
cd treasury
tape dev                            # server + reactors + agent (sqlite)
tape doctor                         # tick/cross diagnostic

tape provision gcp --apply          # render & apply Terraform
tape deploy gcp --target cloud-run  # render Cloud Run service specs
```

A durable ADK agent is now 15 lines:

```python
import tape
from tape.adk import durable_app

app, runner = durable_app(
    name="treasury",
    agent=root_agent,
    budget=tape.Budget(usd_cap=50, token_cap=2_000_000),
)
```

See [`docs/adk.md`](docs/adk.md), [`docs/non-idempotent-upstreams.md`](docs/non-idempotent-upstreams.md),
and [`docs/gcp-cloud-run.md`](docs/gcp-cloud-run.md) for the full story.

### Same DX in Go, TypeScript, and Java

The standalone DX is mirrored in every SDK. The CLI stays Python-only (it
provisions cloud infrastructure and scaffolds projects — language-agnostic
artifacts); the *agent process* can be in any of the four languages.

| Concern                         | Python                       | Go                                      | TypeScript                          | Java                                       |
|---|---|---|---|---|
| Wire the runtime in one call    | `tape.adk.durable_app(...)`  | `tape.NewDurableApp(ctx, cfg)`          | `durableApp({...})`                 | `DurableApp.wire(new Config().…)`          |
| Outbox tool for non-idempotent  | `@tape.outbox_tool(...)`     | `tape.NewOutboxTool(opts)`              | `outboxTool(fn, opts)`              | `OutboxTool.builder(name, conn).…build()`  |
| Capability connector registry   | `tape.connectors.register(...)` | `connectors.Default`                 | `CONNECTORS`                        | `ConnectorRegistry.DEFAULT`                |
| Built-in connectors             | HTTP / PubSub                | Log / Http / PubSub (`-tags pubsub`) / Tasks (`-tags cloudtasks`) | Log / Http / PubSub / Tasks (lazy)  | Log / Http / PubSub (reflective) / Tasks (reflective) |
| Structured logs + OTel spans    | `tape.obs.log_json` / `span` | `tape.LogJSON` / `tape.Span`            | `logJson` / `span` / `setSpanHook`  | `Obs.logJson` / `Obs.span`                 |
| Tenancy config + DESIGN-ONLY warn | `tape.TenancyConfig`       | `tape.TenancyConfig`                    | `tenancyFromObject` / `warnIf…`     | `Tenancy.Config`                           |

All four enforce the same `non_idempotent` safety rule at decoration /
construction time: no `business_key`, no `status_check`, no `compensate`
⇒ the SDK refuses to build the tool. The point is identical in every
language: an UNKNOWN dispatch must never be blindly retried. The Python
server also enforces the contract at `BeginEffect`-time so even an older
SDK can't slip through.

Python ships only the upstream-shaped built-ins it can verify
(`HTTPConnector`, `PubSubConnector`) — the Go / TS / Java SDKs ship the
broader Log / Tasks set as wire-protocol-only helpers for those agent
processes; their actual dispatch goes through the Python outbox reactor
on the server side.

## Manual quick start (the long way)

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
kind="gate_timeout"|"redrive"|…)` sets a durable "wake me at T". For an
**exactly-once-effective publisher** to external systems, the outbox relay
streams `SubscribeEvents` to a `Sink` with a durable cursor:

```python
from tape.sinks import WebhookSink, PubSubSink, LogSink
import tape.reactors

tape.reactors.run_outbox_relay(
    "tape://localhost:7878",
    sink=PubSubSink(project="my-proj", topic="tape-events"),
    cursor_path="/var/lib/tape/cursor.json",          # durable; a restart resumes from here
)
```

`Sink` is a tiny protocol (`publish(entry) → None`); ships with `LogSink`,
`WebhookSink` (POST + retries + `X-Tape-Event-Id: run_id/seq` for receiver
dedup), and `PubSubSink` (`google-cloud-pubsub`, lazy-imported, `ordering_key =
run_id`). At-least-once-delivery + consumer-side dedup on `(run_id, seq)` =
exactly-once-effective. (On Bigtable the cross-run tail is "use Bigtable change
streams"; the per-run `SubscribeRun` feed still works.)

When the agent isn't a local `Runner` you can call — e.g. it's deployed on
**Vertex AI Agent Engine** — pass `run_reactors(redrive_fn=…)` instead of
`runner=`, where `redrive_fn(run)` re-invokes through whatever API does
(Agent Engine's `:streamQuery`, a REST endpoint, …). And when the Tape server
runs on **Cloud Run**, point at it with the `tapes://host` scheme — the SDK
opens a TLS channel and attaches a Google ID token (Application Default
Credentials) for the Cloud Run audience, so the caller's service account just
needs `roles/run.invoker`. The full GCP topology — Tape server on Cloud Run
(with the AlloyDB Auth Proxy sidecar, or `bigtable://…`), the ADK agent on
Agent Engine, the reactors on Cloud Run — is in [`deploy/gcp/`](deploy/gcp/)
(the [`deploy/k8s/`](deploy/k8s/) manifest is the self-managed-Kubernetes version).

## The reactive key-value store — coordinate through state, not messages

Sometimes one agent has to react when another agent (or an oracle, or a
human-edited config) changes a value. Tape ships a journaled, versioned,
watchable key-value store as a first-class primitive (treatise §IX ⑥):

```python
import tape

# write — monotonic version + optional CAS via if_version
tape.set_value("counters", "X", 70, writer="seed")        # version=1
tape.set_value("counters", "X", 90, writer="updater")     # version=2

# read — never blocks
tape.get_value("counters", "X").value.value_json          # '90'

# watch — streams the snapshot + every change, with the PREVIOUS value attached
for evt in tape.watch_value("counters", "X", from_version=0):
    print(evt.prev_version, "→", evt.value.version, ":",
          evt.prev_value_json, "→", evt.value.value_json)
#  0 → 1 :    → 70           (snapshot)
#  1 → 2 : 70 → 90           (the transition)
```

The point isn't "yet another KV." The point is the *transition* is observable:
a watcher sees X 70 → 90, not just "X is 90 now" — so a reactor that re-prices
on FX moves can act on the *change*. Different from signals (point-to-point,
single-consumer) and the WAL tail (cross-run, everything-in-order); this is
shared state, fan-out, by-key — Tape as the connective tissue between agents.

## Non-idempotent upstreams

Tape does not pretend exactly-once is possible without upstream support. For
counterparties that *do* accept idempotency keys (Stripe, most modern payment
APIs, brokers with `clOrdID`), the v1 model — `@tape.effect(...)` + the
counterparty dedupes on the key Tape mints — is exactly-once-effective.

For counterparties that **don't** — a legacy bank wire API, a fax bridge,
a partner system with no key support — Tape ships an explicit ambiguity
protocol: **outbox dispatch + reconciliation**. The tool body is intent-only;
an outbox reactor calls the counterparty exactly once via a registered
connector; the reconciler resolves any `unknown` outcome by asking the
counterparty (by business key).

| Upstream contract             | Tape pattern                                              |
| ----------------------------- | --------------------------------------------------------- |
| **Idempotent API**            | `@tape.effect()`, inline; auto retry is safe              |
| **Non-idempotent API**        | `@tape.outbox_tool(connector=…, business_key=…)`; no blind retry — outbox + reconciliation |
| **Opaque / partially-known API** | as above, plus `@tape.gate(...)` for a human approval gate |

The minimum to get this in your agent:

```python
import tape

def _bk(*, account, amount, date, tool_context=None):
    return f"{account}:{amount}:{date}"

@tape.outbox_tool(
    connector="bank.wire",
    semantics="non_idempotent",
    business_key=_bk,
    compensate=reverse_wire,
)
def wire_money(account, amount, beneficiary, date, tool_context):
    return {"account": account, "amount": amount,
            "beneficiary": beneficiary, "date": date}
```

…then a connector + the outbox reactor (one Cloud Run service —
`tape/deploy/gcp/outbox.service.yaml`):

```python
# my_app/connectors/bank.py
import tape.connectors as connectors
from tape.connectors.base import DispatchResult, ObservationResult

class BankConnector:
    name = "bank.wire"
    def dispatch(self, effect):
        wire_id = real_bank.wire(**json.loads(effect.request_json))
        return DispatchResult(status="confirmed", external_ref=wire_id)
    def observe(self, effect):
        rows = real_bank.find_by_key(effect.business_key)
        if not rows: return ObservationResult(status="absent")
        if len(rows) > 1: return ObservationResult(status="duplicate",
                                                    external_ref=rows[0].id)
        return ObservationResult(status="confirmed", external_ref=rows[0].id)
    def compensate(self, obligation): ...

connectors.register(BankConnector())
```

```bash
# alongside tape-server and tape-reactors:
tape-outbox --url tapes://tape-server --load my_app.connectors.bank
```

The full walkthrough (with a fake non-idempotent bank, all three failure
modes, and a runnable script) is in
[`examples/non_idempotent_bank/`](examples/non_idempotent_bank/).

## Zero-touch mode

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
| reactive shared state across agents | a Tape-native primitive — `WriteValue`/`WatchValue`/`GetValue`/`DeleteValue`; no ADK hook needed (any process with a `TapeClient` can read or watch) |

## License

[Apache 2.0](../LICENSE).
