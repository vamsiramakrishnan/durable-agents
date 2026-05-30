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

    ## ``subject`` vs ADK's ``user_id`` — they are not the same field

    ADK ships its own ``user_id`` on every session: it is the *session
    owner* — the human (or service account) the ADK agent is acting on
    behalf of, used as a sharding key by ``SessionService`` and as the
    primary-key tuple on ``tape_sessions``. Tape preserves it verbatim
    because the ADK plugin reads it back when resuming.

    ``subject`` is something different: it's the **RFC 8693 subject**
    of the authorization context — typically an OIDC ``sub`` or a
    SPIFFE-style principal label (``spiffe://.../user/alice``,
    ``mailto:alice@acme.com``). It anchors the audit trail to a
    governance identity even when ADK's ``user_id`` is an opaque
    session token. Roughly:

    * ``actor``    — *workload* identity (the SPIFFE-style service principal
                     the gateway authenticated).
    * ``subject``  — *governance* identity (the human / customer the
                     workload is acting for).
    * ``user_id``  — *session-owner* identity (the ADK / ``SessionService``
                     key — usually equal to ``subject``, but allowed to differ
                     for headless or service-account flows).

    A correctly-configured AIPlex deployment passes the same value for
    ``subject`` and ``user_id``; standalone Tape deployments often only
    set ``user_id`` (and leave ``subject`` empty). The journal records
    both so a downstream audit consumer can tell them apart.

    Fields:
      tenant_id:           Tenant identifier (AIPlex tenant, organisation, …).
      actor:               SPIFFE-style *workload* identity — the service
                           principal authenticated by the gateway.
      subject:             RFC 8693 *governance* identity — the human or
                           customer the workload is acting for. Distinct
                           from ADK's ``user_id`` (the session-owner key).
                           See the class docstring above.
      agent_id:            Stable agent kind id (the AIPlex catalog id).
      aiplex_instance_id:  AIPlex Instance.ID — joins back to AIPlex's deploy
                           record. Empty when not AIPlex-managed.
      gateway_route:       The gateway route this run came in on.
      scopes:              Authorized scopes (e.g. "mcp:tools:bank_wire"). PR 2
                           will require this to be non-empty for non_idempotent
                           effects; in PR 1 it's a free-form list.
      labels:              Free-form key/value labels. Conventional AIPlex keys
                           are "aiplex.plane", "aiplex.client_id", etc.
                           Identity keys with dedicated columns (``aiplex.tenant``,
                           ``aiplex.actor``, ``aiplex.subject``, ``aiplex.agent``,
                           ``aiplex.instance``, ``aiplex.route``) should NOT
                           be duplicated in ``labels``; AIPlex's deploy engine
                           strips them before serialising ``AIPLEX_LABELS``.
    """

    tenant_id: str = ""
    actor: str = ""
    subject: str = ""
    agent_id: str = ""
    aiplex_instance_id: str = ""
    gateway_route: str = ""
    scopes: List[str] = field(default_factory=list)
    labels: Dict[str, str] = field(default_factory=dict)

    # Fields the strict-mode validator requires non-empty for an
    # AIPlex-managed deployment. The Tape SDK uses this list both at
    # `from_env(strict=True)` and `validate()`. Subjects and labels are
    # advisory; tenant/actor/agent_id are the audit-trail anchors and
    # the compactor's retained columns — they must be present.
    REQUIRED_FOR_AIPLEX: tuple = ("tenant_id", "actor", "agent_id")

    @classmethod
    def from_env(cls, env: Optional[Dict[str, str]] = None,
                 *, strict: Optional[bool] = None) -> "RunIdentity":
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

        Missing vars become empty strings / empty lists. The result is
        always a valid `RunIdentity` unless ``strict`` is True, in which
        case any field in `REQUIRED_FOR_AIPLEX` that's empty raises
        `MissingIdentity` — locking the contract at process start
        instead of letting headless runs slip into the audit trail.

        ``strict`` defaults to True when the env var ``AIPLEX_REQUIRE_IDENTITY=1``
        is set (the production posture AIPlex's deploy engine injects
        in PR 11 item 11 / G8). Otherwise defaults to False.
        """
        e = env if env is not None else os.environ
        if strict is None:
            strict = e.get("AIPLEX_REQUIRE_IDENTITY", "") == "1"
        ident = cls(
            tenant_id=e.get("AIPLEX_TENANT_ID", ""),
            actor=e.get("AIPLEX_ACTOR", ""),
            subject=e.get("AIPLEX_SUBJECT", ""),
            agent_id=e.get("AIPLEX_AGENT_ID", ""),
            aiplex_instance_id=e.get("AIPLEX_INSTANCE_ID", ""),
            gateway_route=e.get("AIPLEX_ROUTE", ""),
            scopes=_parse_scopes(e.get("AIPLEX_SCOPES", "")),
            labels=_parse_labels(e.get("AIPLEX_LABELS", "")),
        )
        if strict:
            ident.validate(cls.REQUIRED_FOR_AIPLEX)
        return ident

    def validate(self, required: tuple = ()) -> None:
        """Raise `MissingIdentity` if any of `required` is empty on this
        instance. The default required set is `REQUIRED_FOR_AIPLEX`;
        callers can pass a tighter set (e.g. `("tenant_id",)`) for
        less strict deployments.

        Why fail at the SDK rather than just at the server: an
        AIPlex-managed pod boots fast and may issue many BeginRun
        calls before an operator notices the audit trail is going in
        with empty identity. Failing at construction means the pod
        crashloops loudly instead of writing headless runs the
        compactor will later retain forever.
        """
        if not required:
            required = self.REQUIRED_FOR_AIPLEX
        missing = [f for f in required if not getattr(self, f, "")]
        if missing:
            raise MissingIdentity(missing)

    def is_empty(self) -> bool:
        """True iff every field is empty. Useful for local dev paths that want
        to log a warning when no identity has been provided."""
        return (not self.tenant_id and not self.actor and not self.subject
                and not self.agent_id and not self.aiplex_instance_id
                and not self.gateway_route and not self.scopes and not self.labels)

    def subject_or(self, fallback_user_id: str) -> str:
        """Return ``subject`` when set, otherwise the supplied ADK
        ``user_id``. Use this whenever you need a single audit anchor
        and don't care which of the two carried it — typical for
        log lines and metric labels. Keeps the journal honest while
        not requiring every standalone Tape deployment to mint a
        synthetic ``subject``."""
        return self.subject or fallback_user_id


class MissingIdentity(ValueError):
    """Raised when `RunIdentity.validate()` finds a required field
    empty. The error message names the missing field(s) so a deploy
    config typo is easy to fix:

        tape.adk.identity.MissingIdentity: required RunIdentity fields
        missing: ['tenant_id', 'actor']. Set the AIPLEX_TENANT_ID and
        AIPLEX_ACTOR env vars (or pass identity=... explicitly) before
        calling durable_app().
    """

    def __init__(self, missing: List[str]):
        self.missing = list(missing)
        msg = (
            "required RunIdentity fields missing: " + repr(self.missing) +
            ". Set the AIPLEX_" + "/AIPLEX_".join(f.upper() for f in self.missing) +
            " env vars (or pass identity=... explicitly) before calling durable_app()."
        )
        super().__init__(msg)


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


__all__ = ["RunIdentity", "MissingIdentity"]
