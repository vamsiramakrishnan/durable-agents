# Tape Architecture — Durable Execution for ADK Agents

This document explains Tape from the perspective of an agent developer.

Tape is not:

- a workflow DAG builder
- a process checkpointing system
- a Python snapshotting runtime
- a retry wrapper around tools

Tape is:

> a durable execution journal plus semantic recovery runtime for agents that act.

The core idea:

```text
LLMs are not deterministic enough to replay safely.
External systems are not transactional enough to retry safely.
Tape records both reasoning and reality.
```

---

# The architecture at a glance

```text
┌────────────────────────────────────────────────────────────────────┐
│                         ADK Agent Code                              │
│                                                                    │
│  app = App(..., plugins=[TapePlugin(...)])                         │
│  runner = Runner(..., session_service=TapeSessionService(...))      │
│                                                                    │
│  @tape.effect(...)                                                  │
│  def tool(ctx, ...):                                                │
│      ...                                                           │
└───────────────┬───────────────────────────────┬────────────────────┘
                │                               │
                │ control-flow durability        │ session durability
                ▼                               ▼
┌─────────────────────────────┐     ┌────────────────────────────────┐
│        TapePlugin            │     │     TapeSessionService          │
│                             │     │                                │
│ before_model / after_model  │     │ append_event                   │
│ before_tool / after_tool    │     │ state_delta                    │
│ on_tool_error               │     │ get_session for resume         │
└───────────────┬─────────────┘     └───────────────┬────────────────┘
                │                                   │
                └──────────────┬────────────────────┘
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│                         Tape Server                                │
│                                                                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐   │
│  │ Run Ledger   │  │ Decision Log │  │ Effect Ledger             │   │
│  │ leases       │  │ model calls  │  │ tool intent/result/unknown│   │
│  │ status       │  │ responses    │  │ idempotency keys          │   │
│  └──────────────┘  └──────────────┘  └──────────────────────────┘   │
│                                                                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐   │
│  │ Obligations  │  │ Gates        │  │ Timers                   │   │
│  │ compensation │  │ human/event  │  │ durable wakeups          │   │
│  │ LIFO unwind  │  │ suspend      │  │ delayed redrive          │   │
│  └──────────────┘  └──────────────┘  └──────────────────────────┘   │
│                                                                    │
│  ┌──────────────┐  ┌──────────────┐                                │
│  │ Budget       │  │ Reactive KV  │                                │
│  │ admit/charge │  │ watchable    │                                │
│  │ spend caps   │  │ shared state │                                │
│  └──────────────┘  └──────────────┘                                │
└──────────────────────────────┬─────────────────────────────────────┘
                               ▼
                    ┌────────────────────┐
                    │ AlloyDB/Postgres   │
                    │ or SQLite/Bigtable │
                    └────────────────────┘
```

---

# The execution journal

Tape fundamentally has one append-only execution journal.

Conceptually:

```text
(run_id, seq, type, payload)
```

Example:

```text
┌──────┬────────────┬────────────────────────────┐
│ seq  │ type       │ payload                    │
├──────┼────────────┼────────────────────────────┤
│ 101  │ decision   │ model chose wire           │
│ 102  │ effect     │ begin bank wire            │
│ 103  │ effect     │ timeout                    │
│ 104  │ obligation │ refund handle              │
│ 105  │ signal     │ CFO approved               │
└──────┴────────────┴────────────────────────────┘
```

The "three ledgers" are really semantic projections over this journal:

```text
Decision projection   = reasoning
Effect projection     = reality
Obligation projection = responsibility
```

---

# Why replay alone is insufficient

A plain WAL tells you:

```text
what happened in order
```

Tape additionally tracks:

```text
what is confirmed
what is ambiguous
what is replay-safe
what must never replay blindly
what must later compensate
```

This is why Tape stores semantic state, not only ordered events.

---

# Decision ledger — memory of reasoning

The decision ledger stores:

```text
- model requests
- model responses
- tool calls chosen
- rationale
- policy version
```

ASCII:

```text
┌──────────────────────────────────────────────┐
│              Decision Ledger                │
├────┬─────────────────────────────────────────┤
│ #0 │ "read balances"                         │
│ #1 │ "price FX exposure"                     │
│ #2 │ "wire $2m to MMF"                       │
│ #3 │ "post ledger entries"                   │
└────┴─────────────────────────────────────────┘
```

This prevents replay drift.

Without decision replay:

```text
crash
→ re-prompt model
→ model chooses differently
→ duplicate or divergent actions
```

Tape replays the recorded decision instead of re-calling the model.

---

# Effect ledger — memory of reality

The effect ledger stores:

```text
- effect intent
- idempotency key
- request payload
- result
- ambiguity state
- external references
```

Statuses:

```text
PENDING
CONFIRMED
FAILED
UNKNOWN
```

ASCII:

```text
┌────────────────────────────────────────────────────┐
│                 Effect Ledger                      │
├────────────┬──────────────┬────────────────────────┤
│ effect key │ status       │ external reality       │
├────────────┼──────────────┼────────────────────────┤
│ wire#17    │ CONFIRMED    │ money moved            │
│ hedge#18   │ FAILED       │ nothing happened       │
│ email#19   │ UNKNOWN      │ maybe happened         │
└────────────┴──────────────┴────────────────────────┘
```

The key insight:

```text
timeouts are not failures
```

Tape explicitly models ambiguity.

---

# Obligation ledger — memory of responsibility

The obligation ledger stores compensations.

Example:

```text
charge card → refund card
reserve inventory → release inventory
book meeting → cancel meeting
```

ASCII:

```text
┌───────────────────────────────────────────────┐
│             Obligation Ledger                 │
├──────────────┬────────────────────────────────┤
│ effect       │ compensation                   │
├──────────────┼────────────────────────────────┤
│ charge#44    │ refund payment                 │
│ reserve#45   │ release inventory              │
│ hedge#46     │ place offsetting hedge         │
└──────────────┴────────────────────────────────┘
```

This makes sagas durable across crashes.

---

# Crash + resume

```text
Before crash:

  decision #3 recorded
  effect #3.0 intent written
  external call confirmed
  effect result recorded

After restart:

  runner.run(..., invocation_id=same_id)
          │
          ▼
  ADK reloads session from Tape
          │
          ▼
  model call #0, #1, #2, #3 replayed from Decision Log
          │
          ▼
  confirmed tools short-circuit from Effect Ledger
          │
          ▼
  first missing decision/tool executes for real
```

Replay reconstructs execution.
Tape does not snapshot Python processes.

---

# Non-idempotent upstreams

Tape does not pretend exactly-once is possible without upstream cooperation.

For non-idempotent systems, use outbox + reconciliation.

```text
Agent tool
   │
   ▼
Tape records durable intent
   │
   ▼
Outbox reactor claims effect
   │
   ▼
Connector calls bank once
   │
   ▼
Result?
   ├── confirmed ───────────────► record confirmed, resume run
   ├── failed definitive ───────► record failed
   └── timeout / crash ─────────► UNKNOWN, no blind retry
                                      │
                                      ▼
                                Reconciler asks:
                                "Did this business operation happen?"
                                      │
              ┌───────────────────────┼────────────────────────┐
              ▼                       ▼                        ▼
         found once              not found                 duplicate
              │                       │                        │
              ▼                       ▼                        ▼
        confirmed          approval / retry / stuck      compensate / alert
```

---

# Leases

Leases prevent multiple workers from replaying the same run simultaneously.

```text
┌────────────┐
│ Tape Store │
└─────┬──────┘
      │
┌─────┴──────┐
▼            ▼
worker A   worker B
```

Without leases:

```text
both replay simultaneously
both execute tools
both publish events
both mutate state
```

A lease grants temporary authority to extend execution history.

```text
The journal preserves execution history.

The lease grants temporary authority
to extend that history.
```

---

# Reactors

Tape recovery logic is implemented by reactors.

```text
                 ┌──────────────────────────────┐
                 │ Tape Journal / WAL / Tables  │
                 └───────────────┬──────────────┘
                                 │
        ┌────────────────────────┼─────────────────────────┐
        ▼                        ▼                         ▼
┌──────────────┐         ┌────────────────┐        ┌────────────────┐
│ Recovery     │         │ Reconciler     │        │ Outbox         │
│ redrive runs │         │ resolves       │        │ dispatches     │
│ after crash  │         │ UNKNOWN effects│        │ durable intents│
└──────────────┘         └────────────────┘        └────────────────┘

        ▼                        ▼                         ▼
┌──────────────┐         ┌────────────────┐        ┌────────────────┐
│ Timers       │         │ Compensation   │        │ Event Relay    │
│ wake runs    │         │ LIFO unwind    │        │ Pub/Sub/Webhook│
└──────────────┘         └────────────────┘        └────────────────┘
```

---

# Identity hierarchy

Tape currently scopes execution by:

```text
(app_name, user_id, session_id, invocation_id)
```

Conceptually:

```text
Tenant / project
  └── App / agent
        └── User
              └── Session
                    └── Invocation / run
```

Future hard multi-tenancy should promote `tenant_id` into the protocol and storage model.

---

# Mental model

```text
Decision Ledger
  = memory of reasoning

Effect Ledger
  = memory of reality

Obligation Ledger
  = memory of responsibility
```

Tape is not checkpointing Python.

Tape is:

```text
record every model decision
record every external effect intent/result
replay decisions
skip confirmed effects
stop on ambiguity
reconcile reality
compensate when reality disagrees
```
