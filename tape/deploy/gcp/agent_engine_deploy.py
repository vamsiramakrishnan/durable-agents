"""Deploy an ADK agent to Vertex AI Agent Engine with Tape underneath.

Reference — substitute your project / location / agent / Tape server URL. The
only Tape-specific bits are the three lines marked `# tape:` — `TapePlugin` on
the App, `TapeSessionService` as the session backend, and `ResumabilityConfig`
so a re-drive can resume an incomplete invocation. `tape-py` goes in
`requirements`; `TAPE_URL` (a `tapes://…` Cloud Run URL — TLS + an auto-attached
Google ID token for the Cloud Run audience) goes in `env_vars`.

    python tape/deploy/gcp/agent_engine_deploy.py
"""

import os

import vertexai
from vertexai import agent_engines
from vertexai.preview import reasoning_engines

# your agent — its tools carry @tape.effect(compensate=…, status_check=…)
from my_agent.agent import root_agent  # noqa: F401  (replace with your module)

PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
LOCATION = os.environ.get("GOOGLE_CLOUD_LOCATION", "us-central1")
TAPE_URL = os.environ["TAPE_URL"]  # e.g. "tapes://tape-server-xxxxx-uc.a.run.app"

vertexai.init(project=PROJECT, location=LOCATION,
              staging_bucket=f"gs://{PROJECT}-agent-engine")


def build_app():
    from google.adk.apps import App
    from google.adk.apps.app import ResumabilityConfig
    from tape.adk import TapePlugin, TapeSessionService
    import tape

    app = App(
        name="treasury",
        root_agent=root_agent,
        plugins=[TapePlugin(TAPE_URL, budget=tape.Budget(usd_cap=50, token_cap=2_000_000))],  # tape:
        resumability_config=ResumabilityConfig(is_resumable=True),                              # tape:
    )
    return reasoning_engines.AdkApp(
        agent=app,
        session_service_builder=lambda: TapeSessionService(TAPE_URL),                           # tape:
        enable_tracing=True,
    )


if __name__ == "__main__":
    remote = agent_engines.create(
        agent_engine=build_app(),
        requirements=["google-adk>=1.30", "tape-py", "grpcio>=1.60", "google-auth"],
        env_vars={
            "TAPE_URL": TAPE_URL,
            "TAPE_LEASE_MS": "120000",
            "GOOGLE_CLOUD_PROJECT": PROJECT,
            "GOOGLE_CLOUD_LOCATION": LOCATION,
        },
        display_name="treasury (tape-backed)",
    )
    print("deployed:", remote.resource_name)
    # put this resource name in the reactor's AGENT_ENGINE env var (see reactor/main.py)
