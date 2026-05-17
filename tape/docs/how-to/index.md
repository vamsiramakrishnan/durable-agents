# How-to guides

Each how-to is a recipe for one task. They assume you've read the
[**Concepts**](../concepts/index.md) (or are happy looking things up as
you go).

<div class="tape-hub" markdown>

- [**Wire a non-idempotent upstream**](../non-idempotent-upstreams.md)
  Wires, payments, one-shot side effects. The outbox pattern, end-to-end.

- [**Configure reactors**](../reactors.md)
  Enable, disable, scale, swap polling for event-driven.

- [**Pick a storage backend**](../stores.md)
  SQLite, Postgres, AlloyDB, Bigtable, Spanner. Trade-offs and operating
  notes.

- [**Add observability**](../observability.md)
  Structured logs, OTel spans, log-based metrics, the bundled
  dashboard.

- [**Configure tenancy**](../tenancy.md)
  Single, trusted-multi-app, hard-multi-tenant — what each mode means
  today.

- [**Cancel & timeout patterns**](cancel-timeout.md)
  `tape.cancel_run`, gates with `gate_timeout`, `tape.heartbeat`,
  cooperative cancellation.

- [**Write a custom connector**](custom-connector.md)
  Implement `EffectConnector` — `dispatch`, `observe`, `compensate`.

</div>

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
