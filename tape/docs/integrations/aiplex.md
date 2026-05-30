# AIPlex ↔ Tape Integration

How a Tape-backed agent runs under
[AIPlex](https://github.com/vamsiramakrishnan/aiplex)'s identity &
authorization contract. The guiding invariant:

> AIPlex decides whether an agent is allowed to act.
> Tape proves what happened when it acted.

AIPlex governs identity, scopes, consent, routing, catalog, deployment
and policy. Tape governs run journals, model decisions, effects,
replay, leases, gates, timers, budgets, reconciliation and
compensation. They share a wire contract (`tape/proto/tape.proto`); they
don't share a build dependency. AIPlex-specific values flow through
generic fields (`tenant_id`, `actor`, `subject`, `agent_id`, `scopes`,
`labels`) — Tape itself stays independently usable.

The runnable example for this guide lives at
[`tape/examples/standalone/aiplex-integration/`](https://github.com/vamsiramakrishnan/durable-agents/tree/main/tape/examples/standalone/aiplex-integration).

---

## What ships today

| Surface | Status | Where |
| --- | :---: | --- |
| Identity on every run (`tenant_id`, `actor`, `subject`, `agent_id`, `aiplex_instance_id`, `gateway_route`, `scopes`, `labels`) | ✅ | `BeginRunRequest` / `RunState`; indexed columns on `tape_runs` |
| `RunIdentity.from_env()` reading `AIPLEX_*` env vars | ✅ Python · Java | `tape.adk.identity.RunIdentity`, `dev.tape.RunIdentity` |
| `@tape.effect(scope=...)` declaring the authorization scope a side effect requires | ✅ Python (decoration-time refusal) | `tape/sdk/python/tape/effect.py` |
| Server-side scope check at `BeginEffect` (`PermissionDenied` on mismatch) | ✅ all SDKs reach it via wire | `tape/server/src/store/sql.rs` |
| `kind="policy"` journal entry on denial — audit ingestion surface | ✅ | `policy.violation` schema below |

Open follow-ups: see `SDK_PARITY.md` rows **G8** (run identity ergonomics
helper in Go/TS), **G9** (identity in the embedded `tape-adk` tier),
**G10** (decoration-time refusal in Java/Go/TS).

---

## The integration story in one diagram

```
┌────────────────────────────┐
│  AIPlex controller         │  exports env vars on the pod:
│  (deploy time)             │     AIPLEX_TENANT_ID, AIPLEX_ACTOR (SPIFFE),
│                            │     AIPLEX_SUBJECT, AIPLEX_AGENT_ID,
│                            │     AIPLEX_INSTANCE_ID, AIPLEX_ROUTE,
│                            │     AIPLEX_SCOPES, AIPLEX_LABELS,
│                            │     TAPE_URL
└──────────────┬─────────────┘
               │ pod starts
               ▼
┌────────────────────────────┐
│  Agent process             │  durable_app(identity=RunIdentity.from_env())
│  (Tape Python SDK)         │  → identity threaded onto every BeginRun
│                            │  → run.scopes cached for the scope pre-check
└──────────────┬─────────────┘
               │ gRPC
               ▼
┌────────────────────────────┐
│  tape-server               │  begin_run writes identity columns on tape_runs.
│                            │  begin_effect:
│                            │    • if effect.scope ∈ run.scopes → admit
│                            │    • else → PermissionDenied
│                            │           + kind="policy" journal entry
└──────────────┬─────────────┘
               │ outbox / event-bus stream
               ▼
┌────────────────────────────┐
│  AIPlex audit ingestion    │  reads the journal (via outbox sink in PR 3
│  (consumer)                │  of the AIPlex side); surfaces run timelines,
│                            │  scope denials, UNKNOWN effects, obligations.
└────────────────────────────┘
```

---

## The contract, field by field

### `BeginRunRequest` / `RunState` identity

```proto
message BeginRunRequest {
  // ... existing v1 fields ...

  // Identity & authorization context (AIPlex integration PR 1).
  string tenant_id           = 20;
  string actor               = 21;  // SPIFFE-style workload identity
  string subject             = 22;  // human principal, distinct from user_id
  string agent_id            = 23;  // stable AIPlex catalog id
  string aiplex_instance_id  = 24;
  string gateway_route       = 25;
  repeated string scopes     = 26;
  map<string, string> labels = 27;
}
```

`RunState` mirrors these as columns. Indexes on `(tenant_id, agent_id,
started_at_ms)`, `(actor, started_at_ms)`, `(subject, started_at_ms)`
drive the AIPlex run-timeline queries.

### `BeginEffectRequest.scope`

```proto
message BeginEffectRequest {
  // ... existing v1 fields ...
  string scope = 11;  // e.g. "mcp:tools:bank_wire"; empty skips the check
}
```

`EffectRecord.scope` (field 21) reflects what was admitted.

### The `policy` journal entry

When the server denies a scoped effect, it appends:

```json
{
  "tool": "bank_wire",
  "decision_index": 0,
  "required_scope": "mcp:tools:bank_wire",
  "granted_scopes": ["mcp:tools:read_balance"],
  "violation": "scope_not_granted"
}
```

with `JournalEntry.kind = "policy"`. AIPlex audit ingestion (PR 6 on the
AIPlex side) reads these out of Tape's outbox stream and projects them
into the run-timeline UI as a denied-attempt event.

---

## Threading identity through `durable_app`

In Python — the reference SDK — `durable_app(...)` defaults
`identity=RunIdentity.from_env()`. An AIPlex-deployed agent gets identity
threaded for free:

```python
from tape.adk import durable_app

# RunIdentity.from_env() reads the AIPLEX_* env vars the AIPlex controller
# set on the pod. Pass identity=RunIdentity() explicitly to opt out.
app, runner = durable_app(name="treasury", agent=root_agent)
```

The `RunIdentity` dataclass:

```python
@dataclass(frozen=True)
class RunIdentity:
    tenant_id: str = ""
    actor: str = ""
    subject: str = ""
    agent_id: str = ""
    aiplex_instance_id: str = ""
    gateway_route: str = ""
    scopes: list[str] = field(default_factory=list)
    labels: dict[str, str] = field(default_factory=dict)
```

Env-var mapping (used by `from_env()`):

| Env var | Field |
| --- | --- |
| `AIPLEX_TENANT_ID`   | `tenant_id` |
| `AIPLEX_ACTOR`       | `actor` |
| `AIPLEX_SUBJECT`     | `subject` |
| `AIPLEX_AGENT_ID`    | `agent_id` |
| `AIPLEX_INSTANCE_ID` | `aiplex_instance_id` |
| `AIPLEX_ROUTE`       | `gateway_route` |
| `AIPLEX_SCOPES`      | `scopes` (comma- *or* whitespace-separated) |
| `AIPLEX_LABELS`      | `labels` (comma-separated `k=v` pairs) |

Java has the mirror at `dev.tape.RunIdentity.fromEnv()`. Go and TS expose
the wire fields but no typed helper yet — tracked as G8 in
`SDK_PARITY.md`.

---

## Declaring a scoped effect

```python
import tape

@tape.effect(
    # The safety trio (existing): one of status_check / compensate /
    # business_key is required for non_idempotent.
    business_key=lambda args, ctx: args["business_key"],
    status_check=bank.wire_status,
    compensate=reverse_wire,

    # Outbox contract (existing).
    semantics="non_idempotent",
    dispatch="outbox",
    connector="bank.wire",

    # NEW in PR 2 — the authorization scope.
    scope="mcp:tools:bank_wire",
)
def bank_wire(*, account_id, amount_minor, target, business_key, tool_context):
    return {...}   # body only journals intent; outbox dispatches via connector
```

The decorator refuses `semantics="non_idempotent"` without `scope=` at
**decoration time** — i.e. at import. `allow_unsafe=True` is the
documented escape hatch.

The plugin then:

1. At `before_run_callback`: caches `run.scopes` (the grant set) for the
   active run. On a re-drive in a new process, it fetches `RunState.scopes`
   from the server so the cache stays correct.
2. At `before_tool_callback`: checks `effect.scope ∈ run.scopes`. On miss,
   returns `{error, scope_denied: true, required_scope, tool}` and the
   tool body never runs.
3. Forwards `scope` on `BeginEffectRequest`. The server re-checks (defence
   in depth — an outdated SDK can't bypass).

On denial, callers can catch the typed `tape.effect.ScopeDenied`
exception if they want to handle the case explicitly.

---

## The runnable example

`tape/examples/standalone/aiplex-integration/` is a ~150-line worked
example:

- `app/agent.py` — a treasury agent with two scoped tools: `read_balance`
  (idempotent, `mcp:tools:read_balance`) and `bank_wire` (non-idempotent
  + outbox, `mcp:tools:bank_wire`).
- `run.py` — drives the gRPC primitives directly so the admit/deny paths
  are visible step by step. No LLM API key needed.

Run it:

```bash
cargo build --release --manifest-path tape/server/Cargo.toml
./tape/server/target/release/tape-server &

pip install -e tape/sdk/python
pip install -e tape/examples/standalone/aiplex-integration

cd tape/examples/standalone/aiplex-integration
python run.py
```

By default the example grants only `mcp:tools:read_balance`, so the
`bank_wire` attempt is denied — the demo is showing you the **failure
path**, because that's the interesting case. To see the happy path:

```bash
export AIPLEX_SCOPES="mcp:tools:read_balance mcp:tools:bank_wire"
python run.py
```

---

## The AIPlex side (forward references)

On the AIPlex side, the integration arrives in PR 4–10:

- **PR 4** — `Instance.Runtime` config model on the AIPlex `Instance`
  type (`internal/models/instance.go`).
- **PR 5** — AIPlex deployment engine generates the `tape-server` +
  reactors manifests and injects `AIPLEX_*` + `TAPE_URL` env vars onto
  the agent pod.
- **PR 6** — `/internal/tape/events` ingestion endpoint reading Tape's
  outbox stream into AIPlex audit storage with `(run_id, seq)`
  idempotency.
- **PR 7** — `/api/runs/...` read API on top of the ingested events.
- **PR 8** — Console "Runs" tab projecting the run timeline.
- **PR 9** — E2E treasury demo (`aiplex dev up --with-tape`).
- **PR 10** — Operator actions (redrive / reconcile / cancel / signal /
  compensate) under new `aiplex:runs:*` scopes.

See the AIPlex-side survey at
[`aiplex/docs/integration/aiplex-tape-survey.md`](https://github.com/vamsiramakrishnan/aiplex/blob/main/docs/integration/aiplex-tape-survey.md)
for the file-paths and shapes of those PRs.
