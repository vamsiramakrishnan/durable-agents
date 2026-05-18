"""The shared parity scenario.

`make_pending_outbox_effect(url)` creates a fresh run + decision + a
PENDING+OUTBOX effect against the tape server at `url`. Returns the
(`run_id`, `idempotency_key`, `app_name`, `session_id`) tuple. Each
language's dispatcher is then expected to pick that effect up, run it
through the registered `log` connector, and confirm it.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass

import tape
from tape.client import (
    TapeClient,
    EFFECT_STATUS_CONFIRMED,
    EFFECT_STATUS_PENDING,
    EFFECT_SEMANTICS_NON_IDEMPOTENT,
    EFFECT_DISPATCH_MODE_OUTBOX,
)


@dataclass
class Scenario:
    url: str
    run_id: str
    idempotency_key: str
    app_name: str
    user_id: str
    session_id: str
    business_key: str
    tool_name: str
    connector: str

    def get_effect(self) -> object:
        with TapeClient(self.url) as c:
            return c.get_effect(run_id=self.run_id, idempotency_key=self.idempotency_key).effect

    def wait_for_status(self, status: int, *, timeout_s: float = 10.0) -> object:
        deadline = time.time() + timeout_s
        last = None
        while time.time() < deadline:
            last = self.get_effect()
            if last.status == status:
                return last
            time.sleep(0.1)
        raise AssertionError(
            f"effect {self.idempotency_key} did not reach status {status} within "
            f"{timeout_s}s; last status = {getattr(last, 'status', '?')}"
        )


def make_pending_outbox_effect(url: str, *, language_tag: str) -> Scenario:
    """Drive a fresh run + a PENDING+OUTBOX effect against `url`.

    `language_tag` only colours the (app/user/session/business_key) tuple so
    parallel parity runs don't collide on the same server.
    """
    tag = f"{language_tag}-{uuid.uuid4().hex[:8]}"
    app  = f"parity-{language_tag}"
    user = "parity-harness"
    session = tag
    invocation = f"inv-{tag}"
    business = f"bk-{tag}"
    tool = "log_dispatch"

    with TapeClient(url) as c:
        run = c.begin_run(app_name=app, user_id=user, session_id=session,
                          invocation_id=invocation, lease_owner=tag,
                          lease_ttl_ms=60_000)
        run_id = run.run_id

        c.record_decision(run_id=run_id, decision_index=0,
                          model="parity", request_json="{}", response_json="{}")

        be = c.begin_effect(
            run_id=run_id, decision_index=0, tool_name=tool, call_index=0,
            request_json='{"hello":"parity"}',
            semantics=EFFECT_SEMANTICS_NON_IDEMPOTENT,
            dispatch_mode=EFFECT_DISPATCH_MODE_OUTBOX,
            business_key=business,
            connector="log",
        )
        assert be.status == EFFECT_STATUS_PENDING, (
            f"expected new effect to be PENDING, got {be.status}")

        return Scenario(
            url=url, run_id=run_id, idempotency_key=be.idempotency_key,
            app_name=app, user_id=user, session_id=session,
            business_key=business, tool_name=tool, connector="log",
        )


__all__ = ["Scenario", "make_pending_outbox_effect",
           "EFFECT_STATUS_CONFIRMED", "EFFECT_STATUS_PENDING"]
