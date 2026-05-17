# FAQ

Organised by category. If your question isn't here, check the
[**glossary**](glossary.md), then file a [discussion][gh-discuss].

[gh-discuss]: https://github.com/vamsiramakrishnan/durable-agents/discussions

## Conceptual

### What's Tape, in one sentence?

A **durable-execution substrate** for ADK agents. A journal underneath the
agent that survives crashes, makes UNKNOWN a first-class outcome, and
turns non-idempotent dispatch into an explicit, safe contract.

### Why ADK specifically?

Tape rides on ADK's plugin system, custom `SessionService`,
`LongRunningFunctionTool`, and `invocation_id`-based resume. Those are
the extension points that let Tape work *underneath* the agent code,
not bolted on top of it.

The wire protocol is language-agnostic, so the same Tape server can host
non-ADK clients eventually — but the *agent runtime contract* (decisions,
effects, replay-as-memory) was built with ADK's surface in mind.

### Is Tape an agent framework?

No. Tape is the **substrate**. You bring the agent (ADK). Tape gives it
durability, idempotency, replay, budgets, and recovery.

### Do I have to use the outbox pattern for every tool?

No. Use `@tape.effect` for **idempotent** upstreams (the 80% case).
Use `@tape.outbox_tool` only when the upstream **can't** be made
idempotent — wires, one-shot side effects, brittle vendors. The outbox is
strictly more work and is intentionally reserved for cases where the cost
of being wrong is high.

### What's the difference between `UNKNOWN` and `FAILED`?

`FAILED` is a **terminal no** — the upstream rejected the request
(typically a 4xx). No retry, no observe. The tool body got an explicit
"no."

`UNKNOWN` is **ambiguous** — the request *might* have committed; we
don't know. The reconciler reactor will call `observe()` out-of-band to
find out.

Most retry libraries collapse the two. Tape does not. See
[UNKNOWN — the third outcome](../concepts/unknown.md).

### What's `STUCK`?

When `observe()` returns `inconclusive` (the upstream is degraded and
can't tell us if the act happened), or when a compensator fails, the run
moves to `STUCK`. A human gets paged. Tape does *not* guess what
happened.

`STUCK` is good — it's the alternative to silently wrong.

## Setup

### Do I need GCP to use Tape?

No. `tape dev` runs against SQLite (or a local Postgres / Bigtable
emulator). The journal, the reactors, and the recovery pattern all work
locally. GCP is the recommended **production** topology.

### Do I need Docker?

Only if you don't have `cargo` (the Rust toolchain). `tape dev` prefers
native mode and falls back to Docker if `tape-server` isn't on PATH.

### Can I use Tape with a non-Postgres database?

Yes — Tape supports SQLite, Postgres, AlloyDB, Bigtable, and (experimentally)
Spanner. Switch with `TAPE_STORE`. See [stores](../stores.md).

The protocol doesn't change. Your agent doesn't change.

### Can I run multiple Tape servers behind a load balancer?

Yes. The server is stateless. Run N replicas; the per-run lease + the
idempotent RPCs make a double-drive harmless.

## Agent / SDK

### My tool body is non-deterministic — it reads from a DB. What do I do?

Wrap the read in `tape.sample(...)` if it's a read that should be
journalled once per run:

```python
ts = tape.sample(tool_context, lambda: db.get_latest_rate("USD/EUR"))
```

…or, if the read is itself a side effect (it should run on every replay
attempt), make it its own `@tape.effect`.

### Why doesn't my tool re-run on resume?

That's the contract — a `CONFIRMED` effect is **replayed from history**,
not re-executed. If you want the body to re-run, the run is **rewinding**,
not resuming. Use the rewind operator (operator-only, currently a manual
journal truncation).

### Where does the idempotency key come from?

`tape.idempotency_key(tool_context)` returns
`r-<run>/d-<decision>/<tool>/<call_idx>`. It's derived from the journal,
not from your inputs (inputs can recompute differently on replay; the
key must not).

Pass it as the upstream's idempotency header. The upstream's job is to
respect it.

### Can two tools in the same run share state through `tool_context`?

Through the ADK session, yes — that's ADK's surface. Through the journal,
no — the journal is per-decision, not shared scratch space. If you need
shared state, use the reactive KV.

### How do I make a tool body take 30 minutes without the reactor giving up on it?

Call `tape.heartbeat(tool_context)` periodically. The recovery reactor
decides a run is stale when its lease expires; heartbeating keeps it
fresh.

### Can I cancel a tool mid-execution?

Cooperatively. Call `tape.is_cancelled(tool_context)` periodically; if
true, `raise tape.RunCancelled()`. Tape does **not** preempt.

## Reactors

### Do I need to run all five reactors?

You need at least `recovery` if you want crash-survival. The others
become required as you opt into features:

- `reconciler` — if you use `@tape.effect(status_check=...)`.
- `outbox` — if you use `@tape.outbox_tool`.
- `timers` — if you use `tape.set_timer` or `gate_timeout`.
- `compensation` — if you use `compensate=` anywhere.

`tape dev` and the default `tape deploy gcp` enable all five. Trim in
`tape.yaml` if your agent doesn't use a given feature.

### Can I run two reactors of the same kind?

Yes. Each reactor's work is leased per-run (or per-effect / per-obligation
/ per-timer). Two `recovery` workers won't both re-drive the same run.

### Where do reactor logs go?

Same place as the server's: structured JSON, with `reactor`, `run_id`,
`lease_owner` fields. On GCP, that's Cloud Logging. Locally, stdout.

## Deployment

### Why Cloud Run instead of Cloud Functions?

Cloud Run handles long-running connections (h2, gRPC), idle-to-zero
*with* `min-instances >= 1` for the reactors, and lets us scale
horizontally without a worker model. Cloud Functions is shaped for
request/response, not for the reactor pattern.

### Why not Cloud SQL by default?

You can use Cloud SQL — it's a Postgres backend like AlloyDB. AlloyDB
is the default because it autoscales, runs columnar acceleration, and
is operationally similar to Cloud SQL but better-shaped for the
high-write, append-only journal pattern. Cloud SQL is fine for staging
or smaller deployments.

### Do I have to use Terraform?

No — the IAM cheat sheet has the `gcloud` incantation, and you can apply
the rendered Cloud Run service specs yourself. But the Terraform module
is small, readable, and the path most users will actually maintain.

### Can I deploy to AWS / Azure?

The Rust server is platform-agnostic. The deploy modules in this repo
are GCP-only today. The SDKs work anywhere they can reach the server.

Contributions of `tape provision aws --apply` etc. are welcome.

## Operations

### How do I see what runs are stuck?

```bash
tape status                # summary
tape doctor --run r-...    # one run, dumped
```

`STUCK` runs include an explanation: which effect, which observation
result, which compensator failed, etc.

### How do I un-stick a stuck run?

Look at the upstream's books. Decide what really happened. Then either:

- `tape resolve --effect <id> --as confirmed` if the act committed.
- `tape resolve --effect <id> --as failed` if it didn't.
- `tape resolve --obligation <id> --as resolved` if you compensated
  by hand.

Then `tape redrive <run-id>` to wake the run up.

### How do I migrate the schema?

```bash
tape migrate
```

The server runs schema migrations on startup, but `tape migrate` is the
manual hook. `--dry-run` prints what would change.

## Project / status

### Is Tape ready for production?

The reference implementation (Python SDK, Rust server on SQLite /
Postgres / AlloyDB / Bigtable, all five reactors, ADK adapter) is.
Specific surfaces are explicitly **not** ready:

- Spanner backend — experimental, gated.
- Hard multi-tenancy — design-only; needs a proto change.
- Continue-as-new, child workflows, search attributes — roadmap.

The [parity matrix](../design/parity.md) is the honest scorecard.

### Where do I follow development?

- GitHub repo: [vamsiramakrishnan/durable-agents](https://github.com/vamsiramakrishnan/durable-agents)
- Discussions: design + feedback
- Issues: bugs + concrete requests

### How do I contribute?

Open an issue or discussion first. Most of the contribution surface is
in docs, examples, and connectors. Server / proto changes go through a
design discussion to keep the wire stable.
