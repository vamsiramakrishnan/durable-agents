# `tape-adk` — the default tier

`tape-adk` is Tape's contract running **on top of ADK's own
`DatabaseSessionService`**, with no separate server, no separate database,
and no separate proto wire. It's the right shape for the typical case —
an ADK agent that needs the non-idempotent safety contract (`UNKNOWN`,
outbox, reconciler, compensation) but doesn't need write throughput
beyond what Postgres delivers.

The companion package `tape-server` (Rust gRPC) is the *scale tier* —
for Bigtable / Spanner backends or higher write throughput than ADK's
SessionService offers. Both implement the same logical schema and
invariants.

---

## When to pick which

| Choose `tape-adk` when… | Choose `tape-server` when… |
|---|---|
| You already deploy an ADK agent | You need Bigtable, Spanner, or a non-SQL store |
| You're on Cloud Run / Agent Engine | You need >50k effect writes/sec sustained |
| You want one Python container | You want to share a journal across many ADK / non-ADK clients in different languages |
| You're starting from scratch | You're integrating with an existing Tape deployment |

Most teams start with `tape-adk` and never need to graduate.

---

## What it adds to ADK

ADK's `DatabaseSessionService` already gives you a durable conversation
log (atomic event commits, row-level locking, optimistic-concurrency
markers, SQLAlchemy backends with schema migrations). What it *doesn't*
give you, and what `tape-adk` adds:

* **`UNKNOWN` as a first-class effect status.** When a tool call's
  acknowledgement is lost (network glitch, timeout, intermediate-proxy
  crash) — the upstream may or may not have actually run. ADK's model
  is success / exception, both of which assume the tool's outcome is
  knowable. UNKNOWN says "I don't know, the reconciler will resolve."
* **Non-idempotent contract.** `@outbox_tool` refuses construction
  without `business_key` + `connector` + `compensate`. The
  `(connector, business_key)` UNIQUE constraint prevents cross-run
  duplicates structurally.
* **Outbox pattern.** Tools decorated `@outbox_tool` never run inline.
  An intent row goes into the journal; the outbox dispatcher reactor
  calls the upstream connector later, separately.
* **Reconciler.** A small async function that walks UNKNOWN effects,
  asks the connector's `observe(business_key)` what the counterparty
  says, and transitions the row.
* **Compensation ledger.** Registered obligations drain LIFO; if a
  compensation gives up retries, the obligation lands `STUCK` and a
  human gets paged via the `tape doctor` view.
* **Row-level CAS.** The primitive ADK's session-grained lock can't
  express — two outbox dispatchers race for one specific tool call;
  exactly one wins via `UPDATE … WHERE … RETURNING rowcount==1`.

---

## The smallest end-to-end app

```python
from google.adk.agents.llm_agent import LlmAgent
from google.adk.apps.app import App, ResumabilityConfig
from google.adk.runners import Runner
from google.adk.tools.function_tool import FunctionTool

from tape_adk import (
    NonIdempotentSafetyPlugin,
    TapeSessionService,
    effect,
    outbox_tool,
)

# 1. The session store. Same place ADK puts events; we add four sibling
#    tables behind the same SQLAlchemy engine.
session_service = TapeSessionService(
    db_url="sqlite+aiosqlite:///./tape.db",
    # …or postgres://… in production.
)

# 2. An idempotent inline tool. Plugin journals an intent before each call,
#    completes it after, and short-circuits on replay.
@effect
def lookup_customer(customer_id: str) -> dict:
    return {"id": customer_id, "tier": "gold"}

# 3. A non-idempotent outbox tool. Plugin refuses to call inline; the
#    outbox dispatcher resolves it later via the connector.
@outbox_tool(
    business_key=lambda account, amount, date, **_: f"{account}:{amount}:{date}",
    connector="bank.wire",
    compensate="reverse_wire",
)
def wire(account: str, amount: int, date: str) -> dict:
    # This body NEVER runs through the agent path — it's executed
    # by the outbox dispatcher in a separate process. The decorator
    # treats the function as a declaration of what should happen.
    raise RuntimeError("unreachable — outbox dispatcher runs this body")

# 4. Wire the plugin into the agent + Runner.
plugin = NonIdempotentSafetyPlugin(session_service=session_service)
agent = LlmAgent(
    name="treasury",
    model="gemini-2.0-flash",
    instruction="Use wire to move money; use lookup_customer to verify.",
    tools=[FunctionTool(func=lookup_customer), FunctionTool(func=wire)],
)
app = App(name="treasury", root_agent=agent, plugins=[plugin],
          resumability_config=ResumabilityConfig(is_resumable=True))
runner = Runner(app=app, session_service=session_service)

# 5. In a separate process / Cloud Run Job / GKE CronJob, run the
#    outbox dispatcher + reconciler + compensation drainer:
#
#       from tape_adk import (dispatch_outbox_once, reconcile_once,
#                              drain_obligations_once)
#       from your_app.connectors import BankConnector
#
#       async def main():
#           svc = TapeSessionService(db_url=...)
#           connectors = {"bank.wire": BankConnector()}
#           while True:
#               await dispatch_outbox_once(svc, connectors=connectors,
#                                          claimer=f"d-{hostname}")
#               await reconcile_once(svc, connectors=connectors)
#               await drain_obligations_once(svc, connectors=connectors)
#               await asyncio.sleep(1)
```

---

## How it stays atomic

The whole point of riding ADK's storage is that *the effect-status
change and the function_call event commit through the same SQLAlchemy
engine*. There's no two-database divergence to worry about.

Specifically, when the plugin's `before_tool_callback` calls
`begin_effect`:

* The INSERT into `tape_effects` goes through the same async engine
  that ADK uses for `append_event`.
* Same per-session asyncio lock + row-level lock (when supported) +
  optimistic-concurrency marker.
* If either the effect insert OR the subsequent `append_event` fails,
  both roll back together (the txn shape ADK uses).

The CAS primitives (`claim_effect_dispatch`, `claim_obligation`) work the
same way: single UPDATE with the eligibility predicate inline, `rowcount
== 1` means "we won." On Postgres the SQL-level row locking serializes
concurrent claimers across processes. On SQLite (where ADK uses a single
shared connection via `StaticPool`), the service holds an additional
in-process `asyncio.Lock` around CAS to serialize concurrent claims in
the same Python process — cross-process correctness on SQLite is out of
scope, which is the case anyway because SQLite isn't a multi-writer DB.

---

## Constructing a connector

A connector is three async methods. Drop one into a registry the
dispatcher reads:

```python
from tape_adk import Connector, DispatchResult, ObservationResult, CompensationResult

class BankConnector:
    name = "bank.wire"

    async def dispatch(self, effect) -> DispatchResult:
        # The wire MUST be keyed by effect.business_key — that's the
        # bank's own dedup primitive. Even on retry, the same key →
        # same wire.
        try:
            wire_id = await bank.create_wire(
                account=effect.request_json["account"],
                amount=effect.request_json["amount"],
                idempotency_key=effect.business_key,
            )
        except AckLostFromBank:
            return DispatchResult(status="unknown")
        return DispatchResult(status="confirmed",
                               external_ref=wire_id,
                               response={"wire_id": wire_id})

    async def observe(self, effect) -> ObservationResult:
        # The reconciler asks: "bank, do you have this business_key?"
        rec = await bank.find_wire(business_key=effect.business_key)
        if rec is None:
            return ObservationResult(status="absent")
        return ObservationResult(status="confirmed",
                                  external_ref=rec.wire_id)

    async def compensate(self, obligation) -> CompensationResult:
        external_ref = (obligation.payload_json or {}).get("external_ref")
        rev = await bank.reverse_wire(external_ref)
        return CompensationResult(status="compensated",
                                   response={"reversal_id": rev})
```

The same connector implementation runs against either `tape-adk` or
`tape-server` — `DispatchResult` / `ObservationResult` /
`CompensationResult` have the same shape in both packages.

---

## What's `tape-adk` *not* trying to be

* **A cross-language SDK.** ADK-Python is Python. ADK-Java and ADK-Go are
  separate ports with their own `DatabaseSessionService` equivalents. If
  you want the Tape contract on those, you either port the
  `TapeSessionService` extension to that language's ADK or talk to a
  remote `tape-server`.
* **A scale tier.** Postgres + ADK + `tape-adk` will get you to typical
  agent traffic without trouble. For higher writes — or for Bigtable /
  Spanner — the Rust `tape-server` is the right tool.
* **A replacement for `tape-server`.** Both live; `tape-adk` is the
  default, `tape-server` is the scale tier. Same logical schema, same
  invariants, two transports.
