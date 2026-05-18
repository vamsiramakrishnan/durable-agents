# TapeChaos — A Fault-Injection Substrate for Durable Agents

> Tape proves the agent survived one crash.
> **TapeChaos proves it survives the matrix.**

This is the design spec for fault injection and chaos engineering on top of
Tape. It is a *companion* to `tape.md` (the runtime) and `tape-event-bus.md`
(the reactor surface); read those first.

---

## 0. The one-paragraph version

A durable agent runtime has a journal. A journal is also the perfect chaos
substrate: every fault has a natural coordinate (a `seq`, a decision index,
an effect key, a subject path), every invariant is already projected (one
wire, one record; obligations drained; budgets respected), and every run is
replayable. TapeChaos turns that journal into a chaos engine. It ships four
stacked layers — deterministic simulation underneath, in-process failpoints
at the six primitives, span-keyed faults at the SDK surface, and a
reliability-surface harness on top — so the same scenario can be driven from
a unit test, a soak job, a GameDay, or a CI replay. The result is the first
chaos framework where the *system under test* and the *chaos substrate*
share the same data model.

---

## 1. The thesis

Most chaos frameworks bolt on. They inject a fault, run a workload, and
hope an alert fires. The journal Tape already keeps changes the shape:

  * Faults can be **keyed** to the journal coordinates the system itself
    uses. `tape::begin_effect::post_db` is a real surface, not a wrapping.
  * Invariants can be **read off** the projections that already exist. "One
    wire" is `count(effects WHERE business_key=b AND status=CONFIRMED) = 1`,
    not a custom assertion the test had to compute.
  * Failures can be **replayed** bit-for-bit, because the seed that drives
    the simulator is the seed of every choice the run made.
  * Minimum failure sets can be **derived** from a successful run's lineage,
    not guessed by the test author.

> The journal tells you what happened. **TapeChaos asks the journal what
> would have to break for the answer to change.**

The four properties above — *keyed*, *projected*, *replayed*, *derived* —
are what separate this from "Chaos Monkey for agents."

---

## 2. The taxonomy — six primitives × six failure shapes

Tape's six primitives (`§IX` of the treatise) admit six failure shapes
each. The matrix is the contract: every cell needs a name, an injector, and
an invariant that detects it.

| Primitive    | Crash               | Timeout              | Lost ack            | Duplicate            | Drift                  | Adversarial               |
|--------------|---------------------|----------------------|---------------------|----------------------|------------------------|---------------------------|
| Decision     | model 5xx           | slow token stream    | partial JSON        | retry collision      | schema drift           | prompt injection          |
| Effect       | mid-call exit       | upstream timeout     | `AckLost`           | duplicate dispatch   | replay drift           | poisoned response         |
| Obligation   | compensator throws  | inverse slow         | rollback unknown    | double-compensate    | wrong payload          | malicious payload         |
| Gate         | reactor down        | gate timeout         | duplicate signal    | conflicting resolves | resume race            | spoofed signal            |
| Timer        | reactor lag         | clock skew           | missed fire         | duplicate fire       | timer storm            | adversarial wakeup        |
| KV           | watcher disconnect  | stale read           | CAS miss            | watch gap            | version skew           | hostile writer            |

Every cell here is a real production failure mode someone has hit. Every
cell here is also a *Tape-specific* failure mode — i.e. it admits a
projection-based invariant. That is the unit of test.

---

## 3. The four-layer stack

```text
Layer 4 — agent reliability surface       (Inspect AI, agent-chaos, R(k,ε,λ))
Layer 3 — span-keyed user API             (OTel gen_ai.* semconv, tape.obs.SPAN_*)
Layer 2 — in-process failpoints           (tikv/fail-rs in the Rust core)
Layer 1 — deterministic simulation        (madsim or turmoil, seeded + replayable)
```

No single layer is sufficient; each covers what the others can't.

* **Layer 1 — DST.** A seeded, deterministic simulation of the Rust core's
  IO and time. Same seed = same trace, every time. This is the FoundationDB
  / TigerBeetle pattern, applied to a durable-agent runtime. *Madsim or
  turmoil* — pick one; this spec is agnostic. The killer property is
  **bit-for-bit replay** of any failure CI ever found.

* **Layer 2 — failpoints.** Named injection sites in the Rust server, at
  each of the six primitives' mutating boundaries. Zero-cost when compiled
  without the `chaos` feature; active when compiled with it. Configured by
  env (`FAILPOINTS=tape::begin_effect::post_db=panic`) or by RPC. *tikv's
  `fail` crate* is the de-facto standard in this ecosystem.

* **Layer 3 — span-keyed user API.** The OpenTelemetry `gen_ai.*` semantic
  conventions stabilized in late 2025 (`gen_ai.invoke_agent`,
  `gen_ai.execute_tool`, `gen_ai.create_agent`). They map 1:1 to Tape's
  `tape.obs.SPAN_*` constants (`tape.begin_effect`, `tape.record_decision`,
  …). TapeChaos uses span names as the fault-injection vocabulary, so the
  same scenario runs against any agent framework that emits the same spans.

* **Layer 4 — agent reliability surface.** The harness that drives whole
  runs through scenarios, checks the journal invariants, and emits a
  reliability score. This is *ReliabilityBench*'s R(k, ε, λ) formalism,
  recast for durable runtimes.

The reason for four layers, not one: a single layer always has a blind
spot. DST can't simulate Anthropic's API. Failpoints can't model an
adversarial prompt. Spans can't reproduce a kernel-clock skew. Reliability
scoring can't tell you *where* in the journal the run broke. Stacking is
the design.

---

## 4. The architecture

```text
                ┌────────────────────────────────────────────┐
                │    tape chaos run/soak/replay/fuzz/lint    │   CLI
                └────────────┬───────────────────────────────┘
                             │
   ┌─────────────────────────┼─────────────────────────────┐
   │                         │                             │
┌──▼──────────────┐ ┌────────▼─────────┐ ┌─────────────────▼────────────┐
│ chaos.scenario  │ │ chaos.invariants │ │ chaos.lineage (LDFI)         │
│  faults[]       │ │  ExactlyOne,     │ │  derive minimal fault sets   │
│  invariants[]   │ │  NoStuckOblig,   │ │  from a successful run       │
│  seed           │ │  NoBlindRetry,   │ │                              │
└──┬──────────────┘ │  ReplayDetermin  │ └──────────────────────────────┘
   │                │  Linearizable    │
   │                └──────────────────┘
   │
   │    span-keyed faults
   ▼
┌────────────────────────────┐    ┌───────────────────────────────────┐
│ tape.chaos (SDK)           │    │ ChaosService (gRPC, optional)     │
│  crash, delay, lose_ack,   │◀──▶│  Configure(failpoints map)        │
│  wrap_connector, attach,   │    │  Inject(name, action)             │
│  hypothesize (PBT)         │    │  StartScenario(name, seed)        │
└────────────────────────────┘    └────────────────┬──────────────────┘
                                                   │
                                                   ▼
                                  ┌────────────────────────────────┐
                                  │ tape-server  (Rust)            │
                                  │  fail_point!("tape::*::post")  │   ~80 named sites
                                  │  + madsim/turmoil DST harness  │   seeded; replayable
                                  └────────────────────────────────┘
```

The split is deliberate: the *catalog of injection sites* lives in the
server (because that's where the boundaries are); the *language of
scenarios* lives in the SDK (because that's where the user writes them);
and a small RPC bridges them (because the same Python test should drive
a server it doesn't own).

---

## 5. Failpoint catalog (v1)

Two markers per primitive's mutating RPC: `pre_db` (the request is parsed,
the store has not yet been touched) and `post_db` (the store mutation
completed, the response has not yet been sent). The `post_db` site is the
load-bearing one — it's exactly the window the treatise calls the one
place uncertainty lives, made injectable.

```text
tape::begin_run::{pre_db, post_db}
tape::resume_run::{pre_db, post_db}
tape::end_run::{pre_db, post_db}

tape::record_decision::{pre_db, post_db}
tape::get_decision::{pre_db, post_db}

tape::begin_effect::{pre_db, post_db}
tape::complete_effect::{pre_db, post_db}
tape::reconcile_effect::{pre_db, post_db}
tape::claim_effect_dispatch::{pre_db, post_db}
tape::record_dispatch_attempt::{pre_db, post_db}
tape::record_external_observation::{pre_db, post_db}

tape::register_compensation::{pre_db, post_db}
tape::claim_obligation::{pre_db, post_db}
tape::resolve_obligation::{pre_db, post_db}
tape::record_obligation_attempt::{pre_db, post_db}

tape::await_signal::{pre_db, post_db}
tape::send_signal::{pre_db, post_db}

tape::set_timer::{pre_db, post_db}
tape::cancel_timer::{pre_db, post_db}
tape::list_due_timers::{pre_db, post_db}

tape::write_value::{pre_db, post_db}
tape::delete_value::{pre_db, post_db}

tape::append_event::{pre_db, post_db}
```

Each site supports the `fail` crate's standard actions: `off`, `return`,
`panic`, `sleep(ms)`, `pause`, `yield`, `print(msg)`, with probability
prefixes and chained alternatives (`0.1*panic->return`).

Compile-time: the macro is **zero-cost** when `tape-server` is built without
`--features chaos`. Production binaries pay nothing for this surface.

---

## 6. The scenario — what users write

```python
import tape.chaos as chaos

bank_wire_under_chaos = chaos.scenario(
    name="bank-wire-under-chaos",
    seed=42,
    faults=[
        # at the Tape server
        chaos.crash("tape::complete_effect::post_db",
                    when="tool == 'execute_sweep'", after_n=1),
        chaos.delay("tape::send_signal::pre_db", ms=2_000),

        # at the SDK / span layer
        chaos.crash("tape.dispatch_effect",
                    when="effect.business_key contains 'sweep'"),

        # at the connector
        chaos.lose_ack("bank.wire", probability=0.3),
        chaos.duplicate("bank.wire", probability=0.05),

        # at the infrastructure (Cilium / Chaos Mesh emit these)
        chaos.partition(["tape-server", "reactor"], duration_s=10),
        chaos.clock_skew("reactor", offset_s=300),
    ],
    invariants=[
        chaos.invariant.ExactlyOne(connector="bank.wire", by="business_key"),
        chaos.invariant.NoStuckObligations,
        chaos.invariant.NoBudgetOverrun,
        chaos.invariant.NoBlindNonIdempotentRetry,
        chaos.invariant.ReplayDeterminism,
    ],
)

result = chaos.run(bank_wire_under_chaos, runner_fn=build_runner)
print(result.reliability_surface)   # R(k=20, ε=0, λ=0.99) ✓
```

Every field above maps to a journal projection or a span name. The DSL is
declarative, terse, and the *same shape* as `@tape.on` and
`tape.connectors.register` — Tape's existing user-facing patterns.

---

## 7. Invariants — the oracle

Most chaos frameworks inject and forget to check. The catalog ships with:

| Invariant                       | Read from the journal                                           |
|---------------------------------|-----------------------------------------------------------------|
| `ExactlyOne(connector, by)`     | `count(effects WHERE connector=c AND key=k AND status=CONFIRMED) = 1` |
| `NoStuckObligations`            | `count(obligations WHERE status=STUCK) = 0`                     |
| `NoBudgetOverrun`               | `sum(charges) ≤ cap` per run                                    |
| `NoBlindNonIdempotentRetry`     | for every `non_idempotent` effect, `dispatch_attempts ≤ 1` until `RecordExternalObservation` fires |
| `GatesEventuallyResolve(bound)` | `status=WAITING ⇒ resolution within bound`                      |
| `CompensationLIFO`              | obligations resolve in newest-first order                       |
| `ReplayDeterminism`             | re-drive(seed) produces the same projection                     |
| `LinearizableJournal`           | `SubscribeRun(from=0)` is consistent with per-RPC happens-before (Porcupine) |
| `NoEffectWithoutDecision`       | for every effect, `decision_index ≥ 0 ⇒ decision exists at that index` |
| `NoOrphanCompensation`          | every obligation points to an effect that exists                |

Each invariant is a one-pass query over the projections in
`tape.md §6` — no extra instrumentation, no parallel test ledger. The
journal *is* the oracle.

---

## 8. The killer move — lineage-driven fault injection

Tape's WAL is the lineage graph. Every effect points to a decision; every
obligation to an effect; every signal to a gate. Molly-style **LDFI**
(Alvaro et al., 2015) becomes one pass:

1. Take a successful run's journal.
2. Compute the lineage DAG: edges from each row to the rows it depends on.
3. Enumerate minimal cuts of the DAG.
4. For each cut, re-run the scenario with that cut faulted.
5. If the run still succeeds — the invariant holds against that fault.
6. If not — emit the minimal counterexample.

The reason no existing agent framework does this is *no existing agent
framework has the right journal*. Tape does. The cost is one DAG traversal
per successful run; the value is "the next thing to test, derived rather
than guessed."

---

## 9. Deterministic simulation — Layer 1 in detail

The Rust core compiles with one of two simulation backends behind a cargo
feature:

```toml
[features]
chaos = ["fail/failpoints"]                # Layer 2 only
sim   = ["chaos", "madsim", "madsim-tokio"] # Layer 1 + 2
```

Under `--features sim`, tokio is swapped for madsim; IO, time, RNG, file
system and process control are all virtualized. The seed is an env var
(`TAPE_SIM_SEED`) and is stamped onto every chaos report. A failing seed
re-runs to bit-identity. CI runs one fresh seed per build; nightly fuzzes
1 000 seeds per scenario.

Three properties hold under sim:

  * **Determinism**: same seed ⇒ same trace.
  * **Reduction**: a failing seed shrinks to a minimal counterexample via
    binary search on injected fault count.
  * **Coverage feedback**: each seed emits a coverage vector
    (failpoints triggered, RPC branches taken); the runner steers toward
    unexplored vectors.

This is what FoundationDB built in 2014 and what every serious durable
system has rebuilt since — for the first time on top of an *agent*
runtime.

---

## 10. What this is not

  * **Not another chaos vendor.** Antithesis stays the gold standard for
    proprietary hypervisor-level DST; TapeChaos doesn't reinvent the
    hypervisor.
  * **Not a generic agent eval.** Inspect AI, DeepEval, PromptBench cover
    *capability* evaluation; TapeChaos covers *durability* — the
    intersection of an agent's behaviour and the journal's invariants.
  * **Not Jepsen.** Jepsen tests databases. TapeChaos tests the agent
    runtime *on top of* the database, using Jepsen-style techniques
    (Porcupine) on Tape's journal.
  * **Not a Toxiproxy.** In-process failpoints + a service-mesh layer
    (Cilium L7, Istio Ambient) replace it, faster and deterministically.
  * **Not a UI.** Chaos as code, like everything else in Tape. Reports
    write Markdown; the dashboard is the journal.

---

## 11. Phases — how this lands

| Phase | Deliverable                                                                  | Status |
|-------|------------------------------------------------------------------------------|--------|
| 0     | Failpoint catalogue at the server primitives + this treatise                 | done   |
| 1     | `tape.chaos` Python SDK: scenarios, invariants, connector shim                | done   |
| 2     | Deterministic replay + single-thread DST harness                              | done   |
| 3     | LDFI + Reliability Surface R(k,ε,λ) + DeepSnapshot                            | done   |
| 4     | Agent-layer chaos: `model_proxy`, `mcp_proxy` (HTTP/SSE) — stdlib forward-proxy | done   |
| 4.1   | Stdio MCP proxy — subprocess wrapper for line-delimited JSON-RPC              | done   |
| 2.5   | Madsim DST foundation — virtualised time, deterministic scheduling, seeded RNG | done   |
| 2.6   | Store bridge: sim-only `MemRunStore` so real `TapeService` tests run under `cfg(madsim)` | done   |
| 3.5   | Wing-Gong linearizability checker (Porcupine-style) for the obligation lease  | done   |
| 5     | `tape chaos` CLI + Chaos Mesh manifests (pod-kill, net-delay, time-skew, soak workflow) | done |
| 5.5   | Hypothesis-stateful runner: random-sequence model checker over the obligation lifecycle | done   |
| 6     | Mirror the SDK surface to Go / TypeScript / Java                             |        |

Phase 0 is the foundation: a small, named injection surface in the Rust
core, gated behind a cargo feature, with this treatise pinning the shape.
Everything else is additive.

---

## 12. The one-line elevator

> Tape ships a journal. **TapeChaos turns the journal into the chaos
> engine.** Seed it, drive the matrix of (primitive × failure), check the
> invariants the journal already knows about, score the run, replay any
> failure bit-for-bit. The first chaos engineering framework where the
> system under test and the chaos substrate share the same data model.
