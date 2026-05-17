"""An agent that wires money via a non-idempotent upstream. The tool body
returns a pure intent payload; the outbox reactor dispatches it via
`LocalBankConnector`. After UNKNOWN, the reactor calls `observe()`; after a
DUPLICATE, the compensation reactor runs `reverse_wire`.
"""

from __future__ import annotations

import tape
from tape.adk import durable_app
from google.adk.agents import LlmAgent
from google.adk.tools import FunctionTool

from . import connectors  # noqa: F401 — registers the bank.wire connector
from . import fake_bank


def find_wire(idempotency_key: str) -> dict:
    """status_check — given an idempotency key, ask the bank if anything
    matching landed. (For this demo the business key carries the same shape.)"""
    return fake_bank.lookup(business_key=idempotency_key)


def reverse_wire(wire_id: str, **_) -> dict:
    return fake_bank.reverse(wire_id=wire_id)


@tape.outbox_tool(
    connector="bank.wire",
    semantics="non_idempotent",
    business_key=lambda account, amount, date, beneficiary, **_: f"{account}:{amount}:{date}",
    status_check=find_wire,
    compensate=reverse_wire,
    wait_for_result=True,
)
def wire_money(account: str, amount: int, beneficiary: str, date: str):
    return {"account": account, "amount": amount,
            "beneficiary": beneficiary, "date": date}


root_agent = LlmAgent(
    name="bank_agent", model="gemini-2.5-flash",
    instruction=(
        "When asked to send a wire, call `wire_money` exactly once. "
        "Never retry a wire on your own; Tape will resolve UNKNOWN through "
        "the reconciler."
    ),
    tools=[FunctionTool(wire_money)],
)


def build_runner():
    _app, runner = durable_app(name="bank", agent=root_agent,
                               budget=tape.Budget(usd_cap=10))
    return runner
