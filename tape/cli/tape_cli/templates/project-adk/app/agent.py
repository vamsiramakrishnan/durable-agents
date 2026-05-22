"""The {{ name }} agent — a durable ADK agent on the embedded (tape-adk) tier.

No separate server. `TapeSessionService` extends ADK's own
`DatabaseSessionService` with the effect ledger; `NonIdempotentSafetyPlugin`
journals every tool call. The reactor loop (started by `tape dev`, or
`python -m tape_adk`) dispatches outbox effects and reconciles UNKNOWNs.
"""

from __future__ import annotations

import os

from google.adk.agents import LlmAgent
from google.adk.apps.app import App, ResumabilityConfig
from google.adk.runners import Runner
from google.adk.tools import FunctionTool

from tape_adk import NonIdempotentSafetyPlugin, TapeSessionService

from .tools import ALL_TOOLS


# The SQLAlchemy store ADK + tape-adk share. Matches `embedded.db_url` in
# tape.yaml; override with $TAPE_ADK_DB_URL.
DB_URL = os.environ.get("TAPE_ADK_DB_URL",
                        "sqlite+aiosqlite:///./.tape/dev.db")


def build_root_agent() -> LlmAgent:
    return LlmAgent(
        name="{{ name }}_agent",
        # Replace with your model; "gemini-2.5-pro" for production.
        model="gemini-2.5-flash",
        instruction=(
            "You are the {{ name }} agent. Use the tools to do the work. "
            "If a tool returns a 'pending' or 'unknown' status, do NOT "
            "retry it yourself — Tape's reactors resolve it."
        ),
        tools=[FunctionTool(t) for t in ALL_TOOLS],
    )


root_agent = build_root_agent()


def build_session_service() -> TapeSessionService:
    """The store. One instance per process; shared by the agent and the
    reactor loop (they point at the same `db_url`)."""
    return TapeSessionService(db_url=DB_URL)


def build_runner() -> Runner:
    """Build a Runner wired with TapeSessionService + the safety plugin.

    `tape dev` and the recovery path both call this. Returning a fresh
    Runner each call keeps callers stateless.
    """
    session_service = build_session_service()
    plugin = NonIdempotentSafetyPlugin(session_service=session_service)
    app = App(
        name="{{ name }}",
        root_agent=build_root_agent(),
        plugins=[plugin],
        # Resumability lets a crashed invocation re-drive from the journal.
        resumability_config=ResumabilityConfig(is_resumable=True),
    )
    return Runner(app=app, session_service=session_service)
