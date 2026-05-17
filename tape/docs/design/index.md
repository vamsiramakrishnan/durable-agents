# Design

The argument, the spec, and the comparisons. Long-form, opinionated,
load-bearing.

<div class="tape-hub" markdown>

- [**Treatise (long-form)**](agents-that-act-treatise.md)
  *When the Orchestrator Isn't Code — a treatise on agents that act.*
  The thesis: when an LLM is the orchestrator, an agent that *acts*
  needs a journal underneath it. Forty-something pages; cites Lamport,
  Helland, Garcia-Molina & Salem.

- [**Rhythmic treatise**](agents-that-act-rhythmic.md)
  The same argument in tighter, numbered prose. Read this if the long
  version is too long.

- [**Tape spec**](tape.md)
  The design that turns the argument into a system. Protocol, primitives,
  reactor topology, replay semantics. The spec is the source of truth
  for what Tape promises.

- [**Event bus**](tape-event-bus.md)
  The WAL fan-out — how `SubscribeEvents` becomes Pub/Sub, webhooks,
  log sinks. The exactly-once-effective publisher (`run_outbox_relay`)
  with durable cursors.

- [**Parity matrix**](parity.md)
  Tape vs. Temporal vs. LangGraph durable vs. Pydantic AI + DBOS.
  Feature-by-feature, honest about the gaps.

</div>

## How to read these

If you've never seen Tape before:

1. Skim [**Why Tape exists**](../concepts/why-tape.md) (one page).
2. If the framing lands, read the [**Rhythmic treatise**](agents-that-act-rhythmic.md).
3. If you want the full argument with citations, read the
   [**long-form treatise**](agents-that-act-treatise.md).
4. The [**Tape spec**](tape.md) is the contract — read it before you
   build anything you'll need to support.

If you're evaluating Tape against an alternative:

- The [**parity matrix**](parity.md) names the trade-offs.
- The [**concepts / alternatives**](../concepts/alternatives.md) page
  has the TL;DR.

## On the figures

The design docs are heavy on diagrams. They live in `design-principles/`
and are mirrored to `tape/docs/design/` at site-build time by
[`scripts/docs/gen_design.sh`](https://github.com/vamsiramakrishnan/durable-agents/blob/main/scripts/docs/gen_design.sh).

If you want the SVG sources, they're in the repo at
`design-principles/*.svg`.
