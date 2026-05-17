"""The {{ name }} agent — wired into Tape via `durable_app`.

`durable_app(...)` returns `(App, Runner)`. The runner is what you call from
your entry point; the app is what `tape deploy gcp --target agent-runtime` uses.
"""

from __future__ import annotations

import tape
from tape.adk import durable_app
from google.adk.agents import LlmAgent
from google.adk.tools import FunctionTool

from .tools import all_tools
from . import connectors  # noqa: F401 — registers the project's capability connectors


def _build_root_agent() -> LlmAgent:
    return LlmAgent(
        name="{{ name }}_agent",
        # Replace with your model. Use "gemini-2.5-pro" for production.
        model="gemini-2.5-flash",
        instruction=(
            "You are the {{ name }} agent. Use the tools to do the work. "
            "If a tool fails or returns UNKNOWN, do NOT retry on your own — "
            "Tape's reconciler will resolve it."
        ),
        tools=[FunctionTool(t) for t in all_tools()],
    )


root_agent = _build_root_agent()


def build_runner():
    """Used by the `tape-reactors` process to re-drive runs after recovery.

    Returning a fresh `Runner` each call keeps the reactor stateless.
    """
    app, runner = durable_app(
        name="{{ name }}",
        agent=_build_root_agent(),
        budget=tape.Budget(usd_cap=50, token_cap=2_000_000),
    )
    return runner


# The factory the CLI / Cloud Run service points at to invoke the agent.
def build_app():
    app, _runner = durable_app(
        name="{{ name }}",
        agent=root_agent,
        budget=tape.Budget(usd_cap=50, token_cap=2_000_000),
    )
    return app
