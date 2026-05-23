"""A treasury agent that runs under AIPlex's identity & authorization contract.

What this example demonstrates
------------------------------

  * `tape.adk.identity.RunIdentity.from_env()` — reading the AIPLEX_* env
    vars an AIPlex-deployed pod receives at startup and threading them
    onto `BeginRunRequest` so every run carries tenant / actor / subject /
    agent_id / scopes / labels.

  * `@tape.effect(scope=..., semantics="non_idempotent", ...)` — declaring
    the authorization scope a side effect requires. The Tape SDK refuses
    to construct the decorator if `non_idempotent` is missing the scope;
    the server re-checks at `BeginEffect` time.

  * The denial path. When AIPlex grants only a read scope but the agent
    attempts a write, the plugin returns `{scope_denied: true, ...}` to
    the tool callback, the tool body never executes, and the server
    journals a `kind="policy"` violation entry that AIPlex audit
    ingestion can surface in the run timeline.

Convention, not contract: the env-var prefix is AIPLEX_* because AIPlex
populates these — but the integration is generic. Any deployer that sets
these vars gets the same behaviour.

Run via `python -m run` after exporting the env vars; see README.md.
"""

from __future__ import annotations

from google.adk.agents import LlmAgent
from google.adk.tools import FunctionTool

import tape
from tape.adk import durable_app, RunIdentity

from .fake_bank import bank

POLICY_VERSION = "aiplex-treasury-2026.05"


# ── tools ──────────────────────────────────────────────────────────────────

def reverse_wire(*, business_key: str, **_kwargs) -> dict:
    """The inverse op. Wired into @tape.effect(compensate=) so a failed
    downstream step can roll back this wire — Tape calls this when it drains
    the obligation. In a real integration this hits the bank's reversal API."""
    return {"note": f"would reverse {business_key} via bank.reverse_wire"}


@tape.effect(
    # Read-only effects can stay unscoped or carry a "read" scope. Here we
    # show the explicit scope so the example renders the full picture in the
    # run timeline.
    scope="mcp:tools:read_balance",
)
def read_balance(account_id: str, tool_context) -> dict:
    """Look up a balance. Idempotent — safe to retry blindly because the
    counterparty's read is side-effect-free. No outbox needed."""
    # In a real integration this would call the bank's read API; for the
    # demo we just return a plausible number.
    return {"account_id": account_id, "balance_minor": 1_000_000_00,
            "currency": "USD"}


@tape.effect(
    # The whole point of the integration: a side effect that costs real
    # money. The contract says:
    #   * semantics="non_idempotent" — the bank does not dedupe blind retries
    #   * dispatch="outbox" — record intent first, dispatcher calls the bank
    #   * connector="bank.wire" — the routing key the outbox reactor matches
    #   * business_key=...      — what the bank itself uses to dedupe a
    #                             *logical* operation across the wire
    #   * status_check=...      — how the reconciler resolves UNKNOWN
    #   * compensate=...        — the inverse op the obligation drains
    #   * scope="mcp:tools:bank_wire" — what AIPlex must have granted the
    #                                   run before this effect can land.
    # Drop any of these and the SDK refuses to build the tool at import time.
    semantics="non_idempotent",
    dispatch="outbox",
    connector="bank.wire",
    business_key=lambda args, ctx: args["business_key"],
    status_check=bank.wire_status,
    compensate=reverse_wire,
    scope="mcp:tools:bank_wire",
)
def bank_wire(*, account_id: str, amount_minor: int, target: str,
              business_key: str, tool_context) -> dict:
    """Wire money to a money-market fund. The body only journals intent —
    the outbox reactor performs the actual bank call via a connector
    registered for "bank.wire". We return the args here so the connector
    has the call payload available."""
    return {"account_id": account_id, "amount_minor": amount_minor,
            "target": target, "business_key": business_key}


# ── the agent ──────────────────────────────────────────────────────────────

root_agent = LlmAgent(
    name="aiplex_treasury",
    model="gemini-2.5-flash",
    instruction=(
        "You are a treasury agent operating under AIPlex authorization. "
        "Read the balance for the account the user names, then if it's "
        f"above the sweep threshold, wire the excess via {bank_wire.__name__}. "
        f"Use policy version {POLICY_VERSION}."
    ),
    tools=[FunctionTool(read_balance), FunctionTool(bank_wire)],
)


# ── durable wiring with AIPlex identity ───────────────────────────────────

# `RunIdentity.from_env()` reads AIPLEX_TENANT_ID / AIPLEX_ACTOR /
# AIPLEX_SUBJECT / AIPLEX_AGENT_ID / AIPLEX_INSTANCE_ID / AIPLEX_ROUTE /
# AIPLEX_SCOPES / AIPLEX_LABELS. Missing vars become empty strings / lists;
# in a real AIPlex deployment every var is set by the controller.
#
# `durable_app(...)` defaults `identity=RunIdentity.from_env()` if you don't
# pass one — so this app picks up identity transparently. We pass it
# explicitly here to make the integration point visible in the example.
identity = RunIdentity.from_env()

app, runner = durable_app(
    name="aiplex_treasury",
    agent=root_agent,
    identity=identity,
    budget=tape.Budget(usd_cap=10),
)


def build_runner():
    """For `tape-reactors --runner-from=...` and any out-of-process recovery
    loop that needs to drive this app."""
    _, r = durable_app(
        name="aiplex_treasury",
        agent=root_agent,
        identity=RunIdentity.from_env(),
        budget=tape.Budget(usd_cap=10),
    )
    return r


__all__ = ["root_agent", "app", "runner", "build_runner", "identity",
           "POLICY_VERSION"]
