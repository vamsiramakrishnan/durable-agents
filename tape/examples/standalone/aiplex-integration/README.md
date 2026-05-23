# aiplex-integration

A runnable example showing how a Tape-backed agent runs under AIPlex's
identity & authorization contract. Demonstrates:

- `tape.adk.identity.RunIdentity.from_env()` reading the `AIPLEX_*` env
  vars that an AIPlex-deployed agent pod receives at startup, and
  threading them onto every `BeginRun`.
- `@tape.effect(scope=..., semantics="non_idempotent", ...)` declaring
  the authorization scope a side effect requires. The Python SDK refuses
  to construct the decorator if a non-idempotent effect is missing its
  scope; the server re-checks at `BeginEffect` time.
- The denial path. When the run's granted scopes don't include the
  effect's required scope, the server returns `PermissionDenied`, the
  tool body never runs, and a `kind="policy"` journal entry records
  what was attempted — exactly the trail AIPlex audit ingestion picks
  up.

This is the *worked example* for the integration guide at
`tape/docs/integrations/aiplex.md`.

---

## What's in the box

```
aiplex-integration/
├── app/
│   ├── __init__.py
│   ├── agent.py        # the ADK agent + scoped @tape.effect tools
│   └── fake_bank.py    # in-memory counterparty (so the demo runs offline)
├── pyproject.toml
├── tape.yaml           # standalone-scaffold config
├── run.py              # drives the integration story end-to-end
└── README.md           # you are here
```

The agent has two tools:

| Tool | Semantics | Scope |
| --- | --- | --- |
| `read_balance` | idempotent (read-only) | `mcp:tools:read_balance` |
| `bank_wire`    | **non_idempotent** + outbox | `mcp:tools:bank_wire` |

`bank_wire` is the interesting one: it costs real money. The decorator
declares `business_key`, `status_check`, and `compensate` (the existing
safety trio) **plus** the new `scope`. The Python SDK refuses to build a
non-idempotent effect that's missing any of them.

---

## Running it

You need a `tape-server` reachable at `TAPE_URL` (default
`tape://localhost:7878`). From the repo root:

```bash
# 1. Build and start the server.
cargo build --release --manifest-path tape/server/Cargo.toml
./tape/server/target/release/tape-server &

# 2. Install the example (editable, so `python run.py` resolves `app.*`).
pip install -e tape/sdk/python
pip install -e tape/examples/standalone/aiplex-integration

# 3. Run the demo.
cd tape/examples/standalone/aiplex-integration
python run.py
```

You should see output like:

```
=== AIPlex identity ===
  tenant_id          : acme
  actor              : spiffe://aiplex/ns/treasury/sa/agent
  subject            : vamsi@example.com
  agent_id           : aiplex-treasury
  ...
  scopes             : ['mcp:tools:read_balance']
  labels             : {'aiplex.plane': 'a2a', 'aiplex.policy': 'treasury-2026.05'}

=== Effect 1: read_balance (scope=mcp:tools:read_balance, GRANTED) ===
  status=PENDING key=<run>/decision-0/read_balance/0

=== Effect 2: bank_wire (scope=mcp:tools:bank_wire, NOT GRANTED) ===
  DENIED with PermissionDenied: tape store: policy violation — scope "mcp:tools:bank_wire" not granted to run

=== Run journal ===
   seq= 1 kind=run        {"app":"aiplex-treasury", ...}
   seq= 2 kind=decision   {...}
   seq= 3 kind=effect     {"tool":"read_balance","status":"pending", ...}
   seq= 4 kind=effect     {"status":"confirmed", ...}
 ! seq= 5 kind=policy     {"tool":"bank_wire","required_scope":"mcp:tools:bank_wire", ...}
```

The `!` line is the **scope-denial journal entry** AIPlex audit ingestion
would surface in the run timeline.

To flip to the happy path (both effects admitted), grant the wire scope:

```bash
export AIPLEX_SCOPES="mcp:tools:read_balance mcp:tools:bank_wire"
python run.py
```

---

## How AIPlex sets this up at deploy time

When AIPlex deploys this agent, its controller:

1. Looks up the agent's grant set in the AIPlex catalog
   (`mcp:tools:read_balance`, `mcp:tools:bank_wire`, …).
2. Injects the AIPlex identity context as env vars on the pod:
   `AIPLEX_TENANT_ID`, `AIPLEX_ACTOR` (SPIFFE), `AIPLEX_SUBJECT`,
   `AIPLEX_AGENT_ID`, `AIPLEX_INSTANCE_ID`, `AIPLEX_ROUTE`,
   `AIPLEX_SCOPES`, `AIPLEX_LABELS`.
3. Points `TAPE_URL` at the managed Tape server it brought up alongside
   the agent.
4. Starts the agent process. `durable_app(identity=RunIdentity.from_env())`
   does the rest — every `BeginRun` carries identity; every
   `@tape.effect(scope=...)` is checked against `AIPLEX_SCOPES`.

When the agent process is replaced (rolling update, crash recovery), the
new process gets the same env, picks up the same grants, and either
re-drives the existing run (if `invocation_id` matches) or starts a
fresh one — all under the same identity.
