"""The ADK adapter — `TapePlugin` and `TapeSessionService`.

Two lines wire Tape into an ADK runner::

    from tape.adk import TapePlugin, TapeSessionService
    runner = Runner(app=App(..., plugins=[TapePlugin()]),
                    session_service=TapeSessionService("tape://localhost:7878"))
"""

from .plugin import TapePlugin
from .session import TapeSessionService

__all__ = ["TapePlugin", "TapeSessionService"]
