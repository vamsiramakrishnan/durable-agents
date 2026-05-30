# How-to guides

Each how-to is a recipe for one task. They assume you've read the
[**Concepts**](../concepts/index.md) (or are happy looking things up as
you go).

If you'd rather follow a curated thread instead of browsing a hub, pick a
**[Reading path](../paths/index.md)** — Beginner / Operator / Systems —
each one threads through the how-tos in the order that makes sense for
that persona.

## See it / run it

- [**Inspect the journal (TUI)**](inspect.md) — `tape inspect <run-id>`
  with the Textual app, replay diff, JSONL pipe-to-jq modes.

- [**Triage a live system**](triage.md) — `tape doctor --live`, the
  operator-dashboard view; exit codes that flip on UNKNOWN / STUCK.

- [**The 60-second demos**](demo.md) — `tape demo crash-resume` and
  `tape demo unknown-reconcile`. Self-contained, no setup, exits 0 only
  on exactly-once.

## Build it

- [**Wire a non-idempotent upstream**](../non-idempotent-upstreams.md) —
  wires, payments, one-shot side effects. The outbox pattern end-to-end.

- [**Write a custom connector**](custom-connector.md) — implement
  `EffectConnector`: `dispatch` / `observe` / `compensate`.

- [**Cancel & timeout patterns**](cancel-timeout.md) —
  `tape.cancel_run`, gate timeouts, heartbeats, cooperative cancellation.

- [**Fan the journal out (sinks)**](sinks.md) — `LogSink` /
  `WebhookSink` / `PubSubSink` in every SDK; exactly-once-effective
  delivery with consumer-side dedup.

## Run it

- [**Configure reactors**](../reactors.md) — enable, disable, scale,
  swap polling for event-driven.

- [**Run the outbox dispatcher (any language)**](outbox-daemon.md) —
  `tape-outbox-ts`, `cmd/tape-outbox`, `dev.tape.cli.TapeOutbox`; one
  dispatch loop, one safety contract, four languages.

- [**Pick a storage backend**](../stores.md) — SQLite / Postgres /
  AlloyDB / Bigtable / Spanner; trade-offs + operating notes.

- [**Add observability**](../observability.md) — structured logs, OTel
  spans, log-based metrics, the bundled dashboard.

- [**Configure tenancy**](../tenancy.md) — single, trusted-multi-app,
  hard-multi-tenant.

- [**Plug in to AIPlex (identity & scopes)**](../integrations/aiplex.md) —
  thread `AIPLEX_*` env vars onto every run via `RunIdentity.from_env()`;
  declare `@tape.effect(scope=...)`; surface scope-denials as `policy`
  journal entries for AIPlex audit ingestion.

## Test it

- [**Cross-SDK parity harness**](cross-sdk-parity.md) — one scenario,
  four languages, identical journal projection on every PR.

## How to read these

Each how-to:

- Starts with **what you want to accomplish**.
- Gives **the smallest code change** that does it.
- Lists **the gotchas** that bite people in production.
- Links to the relevant **concept page** and **reference page**.

If you find yourself reading a how-to for context, switch to the concept
page. If you find yourself reading a concept page for "which exact
parameter," switch to the reference. The three are different *modes* of
documentation, on purpose.

## Don't see your task?

- Check the [**FAQ**](../help/faq.md) — many "how do I" questions are
  one-liners.
- File an issue at
  [vamsiramakrishnan/durable-agents](https://github.com/vamsiramakrishnan/durable-agents/issues)
  describing what you wanted to do and what the docs didn't help with.
  That's the fastest way to make this section better.
