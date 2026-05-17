"""The Tape reactor service — for an agent deployed on Vertex AI Agent Engine.

It bundles your agent package (`import my_agent.agent` registers the
`@tape.effect(status_check=…)` hooks the reconciler needs) and runs
`tape.reactors.run_reactors`, but its "re-drive" calls the Agent Engine
`:streamQuery` API instead of a local `runner.run_async` — because on Agent
Engine you don't hold the Runner; you invoke the deployed agent through its API.

Env:
  TAPE_URL        tapes://tape-server-xxxxx-uc.a.run.app   (TLS; an ID token is auto-attached)
  AGENT_ENGINE    projects/…/locations/…/reasoningEngines/RESOURCE_ID
  GOOGLE_CLOUD_PROJECT / GOOGLE_CLOUD_LOCATION
  TAPE_RECONCILE_PENDING_AFTER_S   (optional; default 0 = only reconcile UNKNOWN effects)

Run as a Cloud Run service (min-instances ≥ 1 so the loop runs continuously), or
turn it into Pub/Sub-push handlers + Cloud Tasks for a scale-to-zero, event-driven
version (see ../README.md).
"""

import os

import vertexai
from vertexai import agent_engines

import tape
import tape.reactors

# importing the agent registers @tape.effect(status_check=…) — the reconciler needs it
import my_agent.agent  # noqa: F401  (replace with your module)

PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
LOCATION = os.environ.get("GOOGLE_CLOUD_LOCATION", "us-central1")
TAPE_URL = os.environ["TAPE_URL"]
AGENT_RESOURCE = os.environ["AGENT_ENGINE"]

vertexai.init(project=PROJECT, location=LOCATION)
_AGENT = agent_engines.get(AGENT_RESOURCE)


def agent_engine_redrive(run) -> None:
    """Re-invoke a stalled run through the Agent Engine API. `run` is a RunState
    (has user_id / session_id / invocation_id). AdkApp.async_stream_query forwards
    **kwargs to runner.run_async, which accepts invocation_id; if your deployed
    version doesn't, drop it — a resumable app + message=None resumes the latest
    incomplete invocation for that session, and Tape's short-circuits keep it safe."""
    for _ in _AGENT.stream_query(user_id=run.user_id, session_id=run.session_id,
                                 invocation_id=run.invocation_id, message=None):
        pass


def main() -> None:
    tape.reactors.run_reactors(
        redrive_fn=agent_engine_redrive,
        url=TAPE_URL,
        recover=True, reconcile=True, timers=True,
        interval_s=float(os.environ.get("TAPE_REACTOR_INTERVAL_S", "2")),
        reconcile_pending_after_s=float(os.environ.get("TAPE_RECONCILE_PENDING_AFTER_S", "0")),
        on_tick=lambda t: None,  # or log it
    )


if __name__ == "__main__":
    main()
