"""Tenancy — the DX-correct surface for *future* hard multi-tenancy.

Today, the proto/storage layer doesn't carry a first-class `tenant_id`; rows
are scoped by `(app_name, user_id, session_id)`. That's fine for `single` or
`trusted_multi_app`. It is NOT enough for `hard_multi_tenant` (where the
tenant boundary is also an authorization boundary).

This module gives the SDK and CLI a single place to:

  * declare the project's tenancy mode (config-driven, validated);
  * pick a `tenant_id` for the current process / request;
  * tag log records and span attributes consistently;
  * warn loudly when `hard_multi_tenant` is requested but the runtime can't
    enforce it.

`tape doctor` reads this and turns it into a check.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class TenancyMode(str, Enum):
    SINGLE = "single"
    TRUSTED_MULTI_APP = "trusted_multi_app"
    HARD_MULTI_TENANT = "hard_multi_tenant"


@dataclass(frozen=True)
class TenancyConfig:
    mode: TenancyMode = TenancyMode.SINGLE
    tenant_id: str = "default"

    @classmethod
    def from_dict(cls, raw: Optional[dict]) -> "TenancyConfig":
        if not raw:
            return cls()
        mode = TenancyMode(raw.get("mode", "single"))
        tenant = str(raw.get("tenant_id", "default"))
        return cls(mode=mode, tenant_id=tenant)

    @classmethod
    def from_env(cls) -> "TenancyConfig":
        mode = TenancyMode(os.environ.get("TAPE_TENANCY", "single"))
        tenant = os.environ.get("TAPE_TENANT_ID", "default")
        return cls(mode=mode, tenant_id=tenant)

    def is_hard(self) -> bool:
        return self.mode is TenancyMode.HARD_MULTI_TENANT

    def warn_if_hard_but_unenforced(self) -> list[str]:
        """Return a list of human-readable warnings if `hard_multi_tenant` is
        requested but the runtime can't enforce it. Empty list means OK."""
        if not self.is_hard():
            return []
        # As of this release: tenant_id is not in tape.proto, and no per-tenant
        # IAM scoping is shipped. The right surface is "warn loudly; the cluster
        # is single-tenant safe, multi-tenant unsafe."
        return [
            "tenancy.mode=hard_multi_tenant requested but the Tape proto and "
            "stores do not yet carry a first-class tenant_id. Cross-tenant data "
            "isolation cannot be enforced at the runtime; this mode is "
            "DESIGN-ONLY today. Track the proto change before enabling.",
        ]


__all__ = ["TenancyMode", "TenancyConfig"]
