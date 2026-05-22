"""Your agent's tools.

Two kinds of tool, two decorators:

* `@effect` — an *idempotent* tool. Calling it twice with the same inputs
  is safe (the upstream dedupes, or the body is a pure read). Tape journals
  an intent before each call and the result after; on replay it
  short-circuits to the recorded result. Use this for lookups, reads, and
  writes against APIs that dedupe on a key you pass.

* `@outbox_tool` — a *non-idempotent* tool. Calling it twice would double
  the side effect (a wire, a charge, an email). Tape NEVER runs the body
  inline — it journals an intent, and the outbox reactor dispatches it
  exactly once through a connector. You must declare:
    - `business_key`: the key the upstream uses to dedupe (a callable
      over the tool's args).
    - `connector`: which connector in `app/connectors.py` performs the call.
    - `compensate`: the obligation kind that reverses it, if a duplicate
      is ever observed.
  Omitting any of these raises at import time — the bug never ships.
"""

from __future__ import annotations

from tape_adk import effect, outbox_tool


@effect
def lookup_customer(customer_id: str) -> dict:
    """An idempotent read. Safe to call any number of times."""
    # Replace with a real lookup. The return value is journaled and
    # replayed verbatim on recovery.
    return {"customer_id": customer_id, "tier": "standard", "ok": True}


@outbox_tool(
    business_key=lambda customer_id, amount_cents, **_: (
        f"{customer_id}:{amount_cents}"
    ),
    connector="payment",
    compensate="refund",
)
def charge_customer(customer_id: str, amount_cents: int) -> dict:
    """A NON-idempotent side effect. The body here is a *declaration* of
    intent — Tape's outbox reactor runs the real call via the `payment`
    connector (see app/connectors.py), exactly once, even across crashes.

    This body is never executed inline; it documents the operation.
    """
    raise RuntimeError(
        "unreachable — the outbox reactor dispatches this via the connector")


# The agent's tool surface. `build_root_agent()` wraps these in ADK
# FunctionTools.
ALL_TOOLS = [lookup_customer, charge_customer]
