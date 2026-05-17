"""`tape.yaml` — the project-level source of truth.

Schema (Pydantic v2). Every field is validated at load time; bad shape => clear
error pointing at the offending key.

    apiVersion: tape.dev/v1
    kind: TapeProject
    project:
      name: treasury
      environment: dev
    agent:
      framework: adk
      app_name: treasury
      entrypoint: app.agent:root_agent
      runner_factory: app.agent:build_runner
      deployment_target: cloud_run
    tape:
      url: ${TAPE_URL:-tape://localhost:7878}
      server:
        image: ghcr.io/vamsiramakrishnan/tape-server:latest
        target: cloud_run
        min_instances: 0
        max_instances: 10
        cpu: "1"
        memory: "512Mi"
      store:
        kind: alloydb
        url_secret: TAPE_STORE_URL
      events:
        kind: pubsub
        topic: tape-events
      reactors:
        recovery: {enabled: true}
        reconciler: {enabled: true}
        outbox: {enabled: true}
        timers: {enabled: true}
        compensation: {enabled: true}
    gcp:
      project_id: ${GOOGLE_CLOUD_PROJECT}
      region: us-central1
      artifact_registry_repository: tape
      service_account_prefix: tape
      network:
        vpc_connector: optional
    tenancy:
      mode: single
      tenant_id: default
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Annotated, Literal, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


# ── env interpolation ──────────────────────────────────────────────────────

_ENV_RE = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)(?::-(.*?))?\}")


def _interpolate(text: str, env: Optional[dict] = None) -> str:
    env = env if env is not None else os.environ

    def repl(m: re.Match) -> str:
        name, default = m.group(1), m.group(2) or ""
        return env.get(name, default)

    return _ENV_RE.sub(repl, text)


def _walk_interpolate(node, env=None):
    if isinstance(node, str):
        return _interpolate(node, env)
    if isinstance(node, list):
        return [_walk_interpolate(x, env) for x in node]
    if isinstance(node, dict):
        return {k: _walk_interpolate(v, env) for k, v in node.items()}
    return node


# ── schema ─────────────────────────────────────────────────────────────────

class ProjectSection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    environment: str = "dev"


class AgentSection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    framework: Literal["adk"] = "adk"
    app_name: str
    entrypoint: str = Field(..., description='Module path: "app.agent:root_agent"')
    runner_factory: Optional[str] = Field(
        None, description='Module path: "app.agent:build_runner" (optional but recommended for reactors).'
    )
    deployment_target: Literal["local", "cloud_run", "gke", "agent_runtime"] = "cloud_run"


class ServerSection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    image: str = "ghcr.io/vamsiramakrishnan/tape-server:latest"
    target: Literal["cloud_run", "gke", "local"] = "cloud_run"
    min_instances: int = 0
    max_instances: int = 10
    cpu: str = "1"
    memory: str = "512Mi"
    ingress: Literal["internal", "all", "internal-and-cloud-load-balancing"] = "internal"


class StoreSection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["sqlite", "postgres", "alloydb", "spanner", "bigtable"] = "sqlite"
    url_secret: Optional[str] = Field(
        None,
        description="Name of a Secret Manager secret containing TAPE_STORE. If unset, an inline URL is used.",
    )
    url: Optional[str] = Field(
        None,
        description="An inline TAPE_STORE URL. Mutually exclusive with url_secret in production; allowed for dev.",
    )

    @model_validator(mode="after")
    def _at_least_one(self):
        if self.kind == "sqlite":
            return self
        if not self.url and not self.url_secret:
            raise ValueError(
                f"store.kind={self.kind} requires either store.url or store.url_secret."
            )
        return self


class EventsSection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["none", "pubsub", "pubsub_emulator"] = "none"
    topic: str = "tape-events"
    outbox_topic: Optional[str] = None
    dlq_topic: Optional[str] = None


class ReactorSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = True
    min_instances: int = 1
    max_instances: int = 3


class ReactorsSection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    recovery: ReactorSpec = Field(default_factory=ReactorSpec)
    reconciler: ReactorSpec = Field(default_factory=ReactorSpec)
    outbox: ReactorSpec = Field(default_factory=ReactorSpec)
    timers: ReactorSpec = Field(default_factory=ReactorSpec)
    compensation: ReactorSpec = Field(default_factory=ReactorSpec)

    def enabled_names(self) -> list[str]:
        return [
            name for name in ("recovery", "reconciler", "outbox", "timers", "compensation")
            if getattr(self, name).enabled
        ]


class TapeSection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    url: str = "tape://localhost:7878"
    server: ServerSection = Field(default_factory=ServerSection)
    store: StoreSection = Field(default_factory=StoreSection)
    events: EventsSection = Field(default_factory=EventsSection)
    reactors: ReactorsSection = Field(default_factory=ReactorsSection)


class NetworkSection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    vpc_connector: Optional[str] = None
    private_ip: bool = True


class GcpSection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    project_id: str = ""
    region: str = "us-central1"
    artifact_registry_repository: str = "tape"
    service_account_prefix: str = "tape"
    network: NetworkSection = Field(default_factory=NetworkSection)


class TenancySection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mode: Literal["single", "trusted_multi_app", "hard_multi_tenant"] = "single"
    tenant_id: str = "default"


class TapeProject(BaseModel):
    model_config = ConfigDict(extra="forbid")
    apiVersion: Literal["tape.dev/v1"] = "tape.dev/v1"
    kind: Literal["TapeProject"] = "TapeProject"
    project: ProjectSection
    agent: AgentSection
    tape: TapeSection = Field(default_factory=TapeSection)
    gcp: GcpSection = Field(default_factory=GcpSection)
    tenancy: TenancySection = Field(default_factory=TenancySection)

    @model_validator(mode="after")
    def _coherence(self):
        # If the user asked for a GCP target, they need a project id.
        if self.agent.deployment_target in ("cloud_run", "gke") and not self.gcp.project_id:
            # We don't fail here — `${GOOGLE_CLOUD_PROJECT}` interpolation may
            # not have happened. We fail later in commands that need it.
            pass
        # Pub/Sub events require a project.
        if self.tape.events.kind == "pubsub" and not self.gcp.project_id:
            pass
        return self

    @classmethod
    def load(cls, path: Path | str = "tape.yaml") -> "TapeProject":
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"no tape.yaml at {p.resolve()}; run `tape init <name>` first.")
        raw = yaml.safe_load(p.read_text()) or {}
        return cls.model_validate(_walk_interpolate(raw))

    def dump_yaml(self) -> str:
        return yaml.safe_dump(self.model_dump(mode="json"), sort_keys=False)


def find_project_root(start: Optional[Path] = None) -> Path:
    cur = (start or Path.cwd()).resolve()
    for p in [cur] + list(cur.parents):
        if (p / "tape.yaml").exists():
            return p
    raise FileNotFoundError("no tape.yaml found in any parent directory.")


__all__ = [
    "TapeProject", "ProjectSection", "AgentSection", "ServerSection",
    "StoreSection", "EventsSection", "ReactorSpec", "ReactorsSection",
    "TapeSection", "NetworkSection", "GcpSection", "TenancySection",
    "find_project_root",
]
