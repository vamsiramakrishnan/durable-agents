"""Parity push: retry policies, cancellation, the policy-version branch."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SDK_PY = ROOT / "tape" / "sdk" / "python"
sys.path.insert(0, str(SDK_PY))


# ── retry ────────────────────────────────────────────────────────────────────

class _Busy(Exception):
    pass


def test_retry_policy_succeeds_after_two_busies():
    import tape
    calls = {"n": 0}

    @tape.effect(retry=tape.RetryPolicy(max_attempts=4, initial_interval_s=0.001,
                                        backoff_coefficient=1.0, jitter=0.0,
                                        retry_on=(_Busy,)))
    def flaky(x: int) -> int:
        calls["n"] += 1
        if calls["n"] < 3:
            raise _Busy("not yet")
        return x * 2

    assert flaky(21) == 42
    assert calls["n"] == 3   # first try + two retries


def test_retry_policy_gives_up_on_non_retryable():
    import tape
    calls = {"n": 0}

    @tape.effect(retry=tape.RetryPolicy(max_attempts=5, initial_interval_s=0.001,
                                        backoff_coefficient=1.0, jitter=0.0,
                                        retry_on=(Exception,),
                                        non_retryable=(ValueError,)))
    def fragile() -> None:
        calls["n"] += 1
        raise ValueError("nope")

    import pytest
    with pytest.raises(ValueError):
        fragile()
    assert calls["n"] == 1   # non-retryable -> no retries


def test_retry_policy_max_attempts_exhausts():
    import tape
    calls = {"n": 0}

    @tape.effect(retry=tape.RetryPolicy(max_attempts=3, initial_interval_s=0.001,
                                        backoff_coefficient=1.0, jitter=0.0,
                                        retry_on=(_Busy,)))
    def always_busy() -> None:
        calls["n"] += 1
        raise _Busy("forever")

    import pytest
    with pytest.raises(_Busy):
        always_busy()
    assert calls["n"] == 3


# ── cancellation ─────────────────────────────────────────────────────────────

def test_cancel_run_marks_it_cancelled(tape_server):
    import tape
    from tape.client import TapeClient
    import tape.client as tc

    c = TapeClient(tape_server["url"])
    run = c.begin_run(app_name="a", user_id="u", session_id="cancel-test",
                      invocation_id="inv-cancel", lease_owner="test", lease_ttl_ms=60_000)
    assert c.get_run(run.run_id).status == tc.RUN_STATUS_RUNNING
    tape.cancel_run(run.run_id, reason="user pressed stop", url=tape_server["url"])
    fresh = c.get_run(run.run_id)
    assert fresh.status == tc.RUN_STATUS_CANCELLED, f"expected CANCELLED, got {fresh.status}"
    # CANCELLED runs are not "recoverable" — the reactor won't re-drive them
    assert all(r.run_id != run.run_id for r in c.list_runs_to_recover(limit=50, now_ms=fresh.lease_expires_at_ms + 1).runs)
    c.close()


# ── policy version branch ────────────────────────────────────────────────────

def test_policy_is_branches_on_recorded_version():
    import tape
    from types import SimpleNamespace

    class _State(dict):
        def get(self, k, d=None): return super().get(k, d)

    ctx_new = SimpleNamespace(state=_State({"policy_version": "cfo-2026.05"}))
    ctx_old = SimpleNamespace(state=_State({"policy_version": "cfo-2025.12"}))
    assert tape.policy_is(ctx_new, "cfo-2026.05")
    assert not tape.policy_is(ctx_old, "cfo-2026.05")
    assert not tape.policy_is(SimpleNamespace(state=_State()), "anything")
