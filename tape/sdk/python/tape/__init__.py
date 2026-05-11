"""Tape — a durable-execution substrate for ADK agents (Python SDK).

Quick start::

    from google.adk.runners import Runner
    from google.adk.apps import App
    from google.adk.apps.app import ResumabilityConfig
    from tape.adk import TapePlugin, TapeSessionService
    import tape

    app = App(name="treasury", root_agent=agent, plugins=[TapePlugin(budget=tape.Budget(usd_cap=50))],
              resumability_config=ResumabilityConfig(is_resumable=True))
    runner = Runner(app=app, session_service=TapeSessionService("tape://localhost:7878"))

See ../../../design-principles/tape.md for the design, and ../../examples/treasury
for a worked example.
"""

from __future__ import annotations

import json as _json

from .client import (
    TapeClient,
    DEFAULT_URL,
    RUN_STATUS_RUNNABLE,
    RUN_STATUS_RUNNING,
    RUN_STATUS_WAITING,
    RUN_STATUS_TERMINAL,
    RUN_STATUS_FAILED,
    RUN_STATUS_STUCK,
    EFFECT_STATUS_PENDING,
    EFFECT_STATUS_CONFIRMED,
    EFFECT_STATUS_FAILED,
    EFFECT_STATUS_UNKNOWN,
)
from .effect import effect, idempotency_key, run_id_of, get_compensator, get_status_check
from .gates import AckLost, gate, gate_tool
from .budget import Budget, with_budget
from ._recover import resume, recover_once, compensate_run
from ._gen import tape_pb2 as pb

__all__ = [
    "TapeClient", "DEFAULT_URL", "pb",
    "effect", "idempotency_key", "run_id_of",
    "AckLost", "gate", "gate_tool",
    "Budget", "with_budget",
    "resume", "recover_once", "compensate_run", "send_signal",
    "get_compensator", "get_status_check",
    "RUN_STATUS_RUNNABLE", "RUN_STATUS_RUNNING", "RUN_STATUS_WAITING", "RUN_STATUS_TERMINAL",
    "RUN_STATUS_FAILED", "RUN_STATUS_STUCK",
    "EFFECT_STATUS_PENDING", "EFFECT_STATUS_CONFIRMED", "EFFECT_STATUS_FAILED", "EFFECT_STATUS_UNKNOWN",
]

__version__ = "0.1.0"


def send_signal(gate_name, *, run_id="", app_name="", user_id="", session_id="",
                resolution=None, url: str = DEFAULT_URL):
    """Release a run parked on a gate (or stash the resolution for when it parks)."""
    with TapeClient(url) as c:
        return c.send_signal(run_id=run_id, app_name=app_name, user_id=user_id,
                             session_id=session_id, gate_name=gate_name,
                             resolution_json=_json.dumps(resolution or {}))
