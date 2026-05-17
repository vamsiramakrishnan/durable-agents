"""Worked example: an agent calling a non-idempotent bank wire API safely.

The bank has no idempotency key — every call lands. We make this safe by:

  1. The tool body declares INTENT only (`@tape.outbox_tool`). It cannot
     accidentally call the bank.
  2. An outbox reactor picks up the intent, runs the bank connector exactly
     once, and records the outcome (confirmed | failed | unknown).
  3. When `unknown` happens, the reconciler asks the bank via `observe()`
     (by business key) and resolves to confirmed, absent → failed, or
     duplicate → register compensation.

See `connectors.py` for the connector, `agent.py` for the agent, and `run.py`
for the runnable demo.
"""

from .bank import bank, FakeNonIdempotentBank   # noqa: F401
from .connectors import BankConnector            # noqa: F401
from .agent import build_runner, agent            # noqa: F401
