"""Effect-scope authorization tests (AIPlex integration PR 2).

Two layers of enforcement get tested here:

  1. Decoration-time (no server, no SDK runtime): the @tape.effect decorator
     refuses non_idempotent without scope; accepts scoped/unscoped idempotent.

  2. Runtime (real Rust server via the parent conftest tape_server fixture):
     begin_effect honours the run's granted scopes — present scope succeeds,
     missing scope denies with PermissionDenied and a policy.violation
     journal entry.
"""

from __future__ import annotations

import json

import grpc
import pytest

import tape
from tape import client as client_mod
from tape.adk.identity import RunIdentity
from tape.effect import effect, ScopeDenied


# ─── Decoration-time tests (no server) ────────────────────────────────────────

def test_idempotent_scope_optional():
    """The default semantics='idempotent' lets scope stay empty."""
    @effect()
    def f(x):
        return x
    assert f._tape_effect["scope"] == ""


def test_idempotent_scope_passthrough():
    """Idempotent effects can opt into a scope check."""
    @effect(scope="mcp:tools:foo")
    def f(x):
        return x
    assert f._tape_effect["scope"] == "mcp:tools:foo"


def test_non_idempotent_without_scope_refused():
    """The whole point of PR 2: non_idempotent + no scope = decoration error."""
    with pytest.raises(ValueError, match="requires scope="):
        @effect(semantics="non_idempotent", dispatch="outbox",
                connector="bank.wire", business_key="bk-1")
        def bank_wire(amount):
            ...


def test_non_idempotent_with_scope_ok():
    """The happy path: scope set, the other safety conditions met."""
    @effect(semantics="non_idempotent", dispatch="outbox",
            connector="bank.wire", business_key="bk-1",
            scope="mcp:tools:bank_wire")
    def bank_wire(amount):
        ...
    assert bank_wire._tape_effect["scope"] == "mcp:tools:bank_wire"


def test_allow_unsafe_bypasses_scope_requirement():
    """allow_unsafe=True is the documented override for both the existing
    safety trio and the new scope requirement."""
    @effect(semantics="non_idempotent", dispatch="outbox",
            connector="bank.wire", business_key="bk-1",
            allow_unsafe=True)
    def bank_wire(amount):
        ...
    assert bank_wire._tape_effect["scope"] == ""


# ─── Server-side runtime tests (Rust tape-server via fixture) ─────────────────

@pytest.fixture
def server_url(tape_server):
    """Extract the gRPC URL from the parent conftest's tape_server dict."""
    return tape_server["url"]


def _make_run(c: client_mod.TapeClient, *, scopes: list[str]) -> str:
    """Spin up a fresh run with the given grant set. Returns run_id."""
    resp = c.begin_run(
        app_name="scope-test", user_id="u",
        session_id=f"sess-{id(scopes)}", invocation_id=f"inv-{id(scopes)}",
        lease_owner="t", lease_ttl_ms=60_000,
        tenant_id="acme", actor="spiffe://test",
        agent_id="scope-test-agent", scopes=scopes)
    # Decision 0 anchors the effect's idempotency key.
    c.record_decision(run_id=resp.run_id, decision_index=0, model="m",
                      request_json="{}", response_json="{}")
    return resp.run_id


def test_server_admits_when_scope_granted(server_url):
    """Granted scope ⇒ effect persists, no error."""
    with client_mod.TapeClient(server_url) as c:
        run_id = _make_run(c, scopes=["mcp:tools:bank_wire"])
        e = c.begin_effect(run_id=run_id, decision_index=0, tool_name="bank_wire",
                           call_index=0, scope="mcp:tools:bank_wire")
        assert e.status == client_mod.EFFECT_STATUS_PENDING


def test_server_denies_when_scope_missing(server_url):
    """Required scope not in grants ⇒ PermissionDenied; no effect row."""
    with client_mod.TapeClient(server_url) as c:
        run_id = _make_run(c, scopes=["mcp:tools:bank_wire"])
        with pytest.raises(grpc.RpcError) as ei:
            c.begin_effect(run_id=run_id, decision_index=0, tool_name="bank_wire",
                           call_index=0, scope="mcp:tools:bank_other")
        assert ei.value.code() == grpc.StatusCode.PERMISSION_DENIED
        # And the effect row was NOT created.
        eff = c.get_effect(run_id=run_id, idempotency_key=f"{run_id}/decision-0/bank_wire/0")
        assert not eff.found


def test_server_skips_check_for_unscoped_effect(server_url):
    """Empty scope ⇒ no check (the v1 path for idempotent effects)."""
    with client_mod.TapeClient(server_url) as c:
        run_id = _make_run(c, scopes=[])  # NO grants at all
        e = c.begin_effect(run_id=run_id, decision_index=0, tool_name="read_balance",
                           call_index=0)  # default scope=""
        assert e.status == client_mod.EFFECT_STATUS_PENDING


def test_denial_appends_policy_journal_entry(server_url):
    """A denial must leave an auditable record on the journal so AIPlex's
    run-timeline / audit ingestion can see what was attempted. Reads the
    journal via a bounded gRPC call so the test can't hang on a stream that
    never ends.
    """
    from tape._gen import tape_pb2 as pb
    from tape._gen import tape_pb2_grpc as pb_grpc
    from tape.client import _target

    with client_mod.TapeClient(server_url) as c:
        run_id = _make_run(c, scopes=["mcp:tools:read"])
        with pytest.raises(grpc.RpcError):
            c.begin_effect(run_id=run_id, decision_index=0, tool_name="dangerous",
                           call_index=0, scope="mcp:tools:write")

    # Read the journal via SubscribeRun with a tight deadline so the test
    # can't hang on a live stream. Pulls a handful of entries, looks for the
    # policy.violation, asserts shape, returns.
    ch = grpc.insecure_channel(_target(server_url))
    try:
        stub = pb_grpc.TapeStub(ch)
        it = stub.SubscribeRun(pb.SubscribeRunRequest(run_id=run_id, from_seq=0),
                               timeout=2.0)
        found = None
        try:
            for entry in it:
                if entry.kind == "policy":
                    found = json.loads(entry.payload_json)
                    break
        except grpc.RpcError:
            # deadline exceeded — we've drained whatever was available
            pass
        finally:
            it.cancel()
    finally:
        ch.close()

    assert found is not None, "expected a policy journal entry after denial"
    assert found["violation"] == "scope_not_granted"
    assert found["required_scope"] == "mcp:tools:write"
    assert found["granted_scopes"] == ["mcp:tools:read"]
    assert found["tool"] == "dangerous"
