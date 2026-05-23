"""`RunIdentity` — the identity & authorization context attached to a Tape run.

Populated either explicitly when calling `durable_app(..., identity=...)` or
implicitly by `RunIdentity.from_env()`, which reads the conventional `AIPLEX_*`
environment variables an AIPlex-deployed agent receives at startup.

`RunIdentity` is intentionally a plain dataclass — not a proto type — so SDK
callers don't need to import `_gen.tape_pb2` to construct one. The values are
serialised onto `BeginRunRequest` by the Tape client.

Convention, not contract: the env-var prefix is `AIPLEX_*` because AIPlex is
the primary downstream that populates these. Tape itself does not depend on
AIPlex; any deployer can set the same vars to thread identity through Tape.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass(frozen=True)
class RunIdentity:
    """Identity & authorization context for a Tape run.

    Fields:
      tenant_id:           Tenant identifier (AIPlex tenant, organisation, …).
      actor:               SPIFFE-style workload identity — *who is acting*.
      subject:             Human principal label, distinct from ADK's user_id
                           (e.g. "spiffe://.../user/alice", an OIDC sub, etc.).
      agent_id:            Stable agent kind id (the AIPlex catalog id).
      aiplex_instance_id:  AIPlex Instance.ID — joins back to AIPlex's deploy
                           record. Empty when not AIPlex-managed.
      gateway_route:       The gateway route this run came in on.
      scopes:              Authorized scopes (e.g. "mcp:tools:bank_wire"). PR 2
                           will require this to be non-empty for non_idempotent
                           effects; in PR 1 it's a free-form list.
      labels:              Free-form key/value labels. Conventional AIPlex keys
                           are "aiplex.plane", "aiplex.client_id", etc.
    """

    tenant_id: str = ""
    actor: str = ""
    subject: str = ""
    agent_id: str = ""
    aiplex_instance_id: str = ""
    gateway_route: str = ""
    scopes: List[str] = field(default_factory=list)
    labels: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls, env: Optional[Dict[str, str]] = None) -> "RunIdentity":
        """Read identity from `AIPLEX_*` env vars.

        Returns a `RunIdentity` populated from:
          AIPLEX_TENANT_ID        → tenant_id
          AIPLEX_ACTOR            → actor
          AIPLEX_SUBJECT          → subject
          AIPLEX_AGENT_ID         → agent_id
          AIPLEX_INSTANCE_ID      → aiplex_instance_id
          AIPLEX_ROUTE            → gateway_route
          AIPLEX_SCOPES           → scopes (comma- or whitespace-separated)
          AIPLEX_LABELS           → labels (comma-separated k=v pairs)

        Missing vars become empty strings / empty lists. The result is always a
        valid `RunIdentity`; pre-flight strictness (require non-empty
        tenant_id/actor/agent_id) is a deploy-time concern that AIPlex applies
        via a server-side `TAPE_REQUIRE_IDENTITY` flag — not the SDK's job.
        """
        e = env if env is not None else os.environ
        scopes = _parse_scopes(e.get("AIPLEX_SCOPES", ""))
        labels = _parse_labels(e.get("AIPLEX_LABELS", ""))
        return cls(
            tenant_id=e.get("AIPLEX_TENANT_ID", ""),
            actor=e.get("AIPLEX_ACTOR", ""),
            subject=e.get("AIPLEX_SUBJECT", ""),
            agent_id=e.get("AIPLEX_AGENT_ID", ""),
            aiplex_instance_id=e.get("AIPLEX_INSTANCE_ID", ""),
            gateway_route=e.get("AIPLEX_ROUTE", ""),
            scopes=scopes,
            labels=labels,
        )

    def is_empty(self) -> bool:
        """True iff every field is empty. Useful for local dev paths that want
        to log a warning when no identity has been provided."""
        return (not self.tenant_id and not self.actor and not self.subject
                and not self.agent_id and not self.aiplex_instance_id
                and not self.gateway_route and not self.scopes and not self.labels)


def _parse_scopes(s: str) -> List[str]:
    if not s:
        return []
    # Accept both comma and whitespace separators so callers can use either.
    raw = s.replace(",", " ").split()
    return [tok for tok in raw if tok]


def _parse_labels(s: str) -> Dict[str, str]:
    if not s:
        return {}
    out: Dict[str, str] = {}
    for pair in s.split(","):
        pair = pair.strip()
        if not pair or "=" not in pair:
            continue
        k, _, v = pair.partition("=")
        k = k.strip()
        if k:
            out[k] = v.strip()
    return out


__all__ = ["RunIdentity"]
