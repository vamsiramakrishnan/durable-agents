# `@tape.effect` and `@tape.outbox_tool`

The two decorators tools wear. Use `@tape.effect` when the tool body performs IO against an
idempotent upstream; use `@tape.outbox_tool` (sugar for `@tape.effect(dispatch="outbox", ...)`)
when the upstream can't be made naturally idempotent and the outbox reactor should own the
dispatch.

The decoration-time safety rule for non-idempotent effects is enforced here, and the Rust
server enforces the same contract at `BeginEffect`-time.

::: tape.effect.effect
    options:
      heading_level: 2

::: tape.effect.outbox_tool
    options:
      heading_level: 2

---

## Tool-context helpers

::: tape.effect.idempotency_key
    options:
      heading_level: 3
::: tape.effect.run_id_of
    options:
      heading_level: 3
::: tape.effect.business_key
    options:
      heading_level: 3
::: tape.effect.external_ref
    options:
      heading_level: 3
::: tape.effect.effect_semantics
    options:
      heading_level: 3

## Registries

::: tape.effect.register_compensator
    options:
      heading_level: 3
::: tape.effect.register_status_check
    options:
      heading_level: 3
::: tape.effect.get_tool_compensator
    options:
      heading_level: 3
