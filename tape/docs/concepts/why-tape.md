# Why Tape exists

> A chatbot produces words. An agent produces acts. The chatbot's failures
> stay inside the conversation. The agent's failures leak into the world.

This page is the short version. The [treatise](../design/agents-that-act-treatise.md)
is the long version.

## The problem

When an LLM is the orchestrator of an agent that *acts* — sends a wire,
files a ticket, books a flight, posts a message — every tool call is a
boundary between the model's reasoning and the outside world. That
boundary has three properties that conversational chatbots don't have:

1. **Acts are not free.** A wrong sentence is forgiven. A wrong charge is
   disputed. Costs of being wrong scale by category.
2. **Acts are not deterministic.** The network can drop your ack. The
   counterparty can process it twice. The retry decorator can't tell the
   difference.
3. **Acts compose into trajectories.** When the trajectory crashes
   half-way, you don't have a partial state; you have a *partial commit*
   spread across N upstreams.

Most agent frameworks treat these as edge cases. They are not edge cases.
They are the load-bearing case for any agent that does anything irreversible.

## The pattern most teams reach for

```python
@retry(max_attempts=3, backoff=exponential())
def wire_money(account, amount, beneficiary):
    return bank.wire(account, amount, beneficiary)
```

This is wrong in three specific ways:

| What's wrong | Why it matters |
|---|---|
| The retry decorator can't tell *Did the bank receive it?* from *Did the bank receive it and we lost the ack?* | An UNKNOWN is silently turned into a retry, which can double-wire. |
| The retry has no journal. After a process crash, no record exists that the wire was even attempted. | The fix is "look at the bank's books" — but no one wrote the code to do that. |
| The retry is per-tool. The agent above the tool has no idea what state the world is in. | The agent re-asks the model from scratch, which may decide to wire again. |

## Tape's bet

The fix isn't more retry policy. The fix is a journal underneath the agent
and a runtime around it that:

- **Records** every decision and every effect before it happens.
- **Recognises** the third outcome (`UNKNOWN`) and refuses to retry blindly.
- **Owns** the dispatch for non-idempotent acts — the tool returns intent,
  the *outbox reactor* does the work, under a lease.
- **Owns** the compensation when the world commits and we wanted it not
  to.
- **Replays** decisions on resume — the agent doesn't re-ask the model
  for choices it already made.
- **Stops the run** when none of the safety paths apply, instead of
  guessing.

Concretely, Tape is **not** an agent framework. Tape is a **substrate**.
Your ADK agent doesn't change. Tape rides on extension points ADK already
exposes — the plugin system, custom `SessionService`, `LongRunningFunctionTool`,
and `invocation_id`-based resume. The journal lives under the agent; the
reactors live beside it.

## The floor and the ceiling

There are two layers in this stack, and they're not the same problem.

- **The floor** — *durable execution* — has 40 years of literature
  behind it (Lamport, Helland, sagas, idempotent receivers). Temporal,
  Restate, DBOS, Cloud Workflows, Step Functions all live here. The
  primitives are journals, leases, timers, signals.
- **The ceiling** — *agent runtime* — is the newer problem: the action
  space is open, decisions depend on a model, the audit must explain
  *why*, and replay-as-memory is a primary value.

Tape sits at the ceiling, *scoped to ADK*, and uses primitives from the
floor. The trade-off is explicit: we don't try to be a generic durable
workflow engine. ADK is the host; Tape is the journal that ADK didn't
ship with.

## What Tape will not do for you

- Make a non-deterministic tool body deterministic. You still write
  determinism-respecting code (`tape.sample(...)` for `now()`,
  `random()`, external reads).
- Decide for you whether a tool is idempotent. You declare it, and Tape
  enforces the declaration.
- Hide a real outage. If the upstream is down and `observe()` is
  inconclusive, the run becomes `STUCK`. A human gets paged.

## Next

- [The journal](journal.md) — what gets recorded.
- [Effects & idempotency](effects.md) — how to declare a tool's contract.
- [The treatise](../design/agents-that-act-treatise.md) — the
  long-form argument, with citations.
