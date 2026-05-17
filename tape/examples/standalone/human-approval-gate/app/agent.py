"""A run that parks on a durable gate until an approver signals it.

To release the gate from another process:

    python -c "import tape; tape.send_signal('cfo-approval', \
        run_id='r-...', resolution={'approved': True, 'by': 'cfo@example.com'})"
"""

from __future__ import annotations

import tape
from tape.adk import durable_app
from google.adk.agents import LlmAgent
from google.adk.tools import FunctionTool


root_agent = LlmAgent(
    name="approval_agent", model="gemini-2.5-flash",
    instruction=("Before doing anything irreversible, call `await_cfo_approval`. "
                 "Use the resolution to decide whether to proceed."),
    tools=[tape.gate_tool("cfo-approval", risk="irreversible")],
)


def build_runner():
    _app, runner = durable_app(name="approval", agent=root_agent,
                               budget=tape.Budget(usd_cap=5))
    return runner
