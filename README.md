# Durable Agents — *Tape*

A durable-execution substrate for agents that act. The full docs are at
**<https://vamsiramakrishnan.github.io/durable-agents/>**.

Two halves to this repository: the **argument** for why an agent runtime
must look like a runtime, and the **substrate** that makes it look like
one.

---

## The thesis, in one breath

When an LLM is the orchestrator of an agent that *acts*, every tool call
is a boundary between the model's reasoning and the outside world. That
boundary has three properties chatbots don't: acts are not free, acts are
not deterministic, and acts compose into trajectories that crash
half-way. Most agent frameworks treat these as edge cases. They are the
load-bearing case for any agent that does anything irreversible.

Tape is not "checkpointing Python." Tape is:

- record every model decision
- record every external effect intent and result
- replay decisions on resume
- skip confirmed effects
- stop on ambiguity
- reconcile reality
- compensate when reality disagrees

The journal is the centre. Everything else is a projection or a reactor.

```text
1 append-only execution journal
+
semantic projections:
  - decisions     — memory of reasoning
  - effects       — memory of reality
  - obligations   — memory of responsibility
  - timers · gates · budgets · reactive KV
```

The phrases worth repeating:

> Retry repeats the story. **Resume remembers the story.**
> The first run makes calls. **Replay makes reads.**
> The WAL tells you what happened. **The projections tell you what is true now.**

---

## `design-principles/` — the argument

The treatise [***When the Orchestrator Isn't Code — a treatise on agents
that act***](design-principles/agents-that-act-treatise.md) and its
companion essays and figures. In one sentence: when an LLM is the
orchestrator, an agent that acts needs a **journal underneath it** —
recorded decisions, idempotent effects, an explicit `UNKNOWN` outcome,
action gates, budgets-as-state, compensation, and replay-as-memory — and
the agent-framework layer should put a **durable runtime beneath the
model**, not bolt ceremony on top of it.

Read [`design-principles/agents-that-act-treatise.md`](design-principles/agents-that-act-treatise.md)
first; [`design-principles/tape.md`](design-principles/tape.md) is the
design spec that turns the argument into a system.

## `tape/` — the substrate

**Tape** is that durable runtime, built as a *separate system*: a
high-concurrency, low-latency server (Rust, Postgres- / SQLite- /
Bigtable-backed) with a language-agnostic gRPC protocol and SDKs that
plug into **Google's Agent Development Kit (ADK)** *with no changes to
ADK* — riding only on extension points ADK already exposes (the plugin
system, custom `SessionService`s, `LongRunningFunctionTool`, and
`invocation_id`-based resume).

Tape gives an ADK agent a journal: every model call recorded, every
tool call made exactly-once-effective, every gate a durable
suspend-until-signal, every budget a piece of run state, every
irreversible step compensable — so a crashed run **reconstructs and
continues** instead of re-acting.

> Python will write the agent. Something else will run it. Tape is the
> something else.

See [`tape/README.md`](tape/README.md) for the quickstart, or jump
straight to the docs site.

---

## Documentation

The full docs site is at
**<https://vamsiramakrishnan.github.io/durable-agents/>** — built from
`tape/docs/`, the SDK source (via `mkdocstrings` / `gomarkdoc` /
`typedoc` / `javadoc`), and the live Typer app for the CLI reference.

| If you are… | Start at |
|---|---|
| **New here** | [Quickstart](https://vamsiramakrishnan.github.io/durable-agents/quickstart/) |
| **Wiring an ADK agent** | [ADK on Tape](https://vamsiramakrishnan.github.io/durable-agents/adk/) |
| **Learning the model** | [Architecture](https://vamsiramakrishnan.github.io/durable-agents/architecture/) · [The journal](https://vamsiramakrishnan.github.io/durable-agents/concepts/journal/) · [Replay](https://vamsiramakrishnan.github.io/durable-agents/concepts/replay/) |
| **Operating it on GCP** | [Cloud Run topology](https://vamsiramakrishnan.github.io/durable-agents/gcp-cloud-run/) · [IAM cheat sheet](https://vamsiramakrishnan.github.io/durable-agents/deploy/iam/) |
| **Comparing runtimes** | [Runtime vs. framework](https://vamsiramakrishnan.github.io/durable-agents/runtime-vs-framework/) · [Tape vs. alternatives](https://vamsiramakrishnan.github.io/durable-agents/concepts/alternatives/) |
| **Reading the argument** | [Treatise](https://vamsiramakrishnan.github.io/durable-agents/design/agents-that-act-treatise/) |

To preview locally:

```bash
pip install -r docs/requirements.txt
pip install -e tape/sdk/python -e tape/cli
scripts/docs/gen_all.sh        # mirror design pages + generate per-language API docs
mkdocs serve                   # → http://127.0.0.1:8000
```

The CI workflow `.github/workflows/docs.yml` builds and deploys the site
to GitHub Pages on every push to `main`. Make sure the repository's
**Settings → Pages → Source** is set to **GitHub Actions**.

## License

[Apache 2.0](LICENSE).
