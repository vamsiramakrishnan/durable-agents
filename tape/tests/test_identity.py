"""Unit tests for `tape.adk.identity.RunIdentity` — env parsing and round-trip
into `BeginRunRequest`. These tests don't need a running tape-server."""

from __future__ import annotations

from tape.adk.identity import RunIdentity
from tape._gen import tape_pb2 as pb


def test_from_env_empty():
    """No env vars set ⇒ every field empty, `is_empty()` true."""
    ident = RunIdentity.from_env(env={})
    assert ident == RunIdentity()
    assert ident.is_empty()
    assert ident.scopes == []
    assert ident.labels == {}


def test_from_env_populated():
    """The full happy path — every AIPLEX_* var maps to a field."""
    env = {
        "AIPLEX_TENANT_ID": "acme",
        "AIPLEX_ACTOR": "spiffe://aiplex/ns/treasury/sa/agent",
        "AIPLEX_SUBJECT": "vamsi@example.com",
        "AIPLEX_AGENT_ID": "treasury-agent",
        "AIPLEX_INSTANCE_ID": "inst-42",
        "AIPLEX_ROUTE": "/a2a/treasury",
        "AIPLEX_SCOPES": "mcp:tools:bank_wire,llm:model:gemini-2.5-pro",
        "AIPLEX_LABELS": "aiplex.plane=a2a,aiplex.client_id=treasury-agent",
    }
    ident = RunIdentity.from_env(env=env)
    assert ident.tenant_id == "acme"
    assert ident.actor == "spiffe://aiplex/ns/treasury/sa/agent"
    assert ident.subject == "vamsi@example.com"
    assert ident.agent_id == "treasury-agent"
    assert ident.aiplex_instance_id == "inst-42"
    assert ident.gateway_route == "/a2a/treasury"
    assert ident.scopes == ["mcp:tools:bank_wire", "llm:model:gemini-2.5-pro"]
    assert ident.labels == {"aiplex.plane": "a2a", "aiplex.client_id": "treasury-agent"}
    assert not ident.is_empty()


def test_scopes_separator_flexibility():
    """Scopes accept comma- *or* whitespace-separated input."""
    assert RunIdentity.from_env(env={"AIPLEX_SCOPES": "a,b,c"}).scopes == ["a", "b", "c"]
    assert RunIdentity.from_env(env={"AIPLEX_SCOPES": "a b c"}).scopes == ["a", "b", "c"]
    assert RunIdentity.from_env(env={"AIPLEX_SCOPES": "a, b ,c"}).scopes == ["a", "b", "c"]


def test_labels_ignores_malformed():
    """Label pairs without `=` are silently dropped."""
    env = {"AIPLEX_LABELS": "ok=yes,malformed,=novalue,key=val"}
    labels = RunIdentity.from_env(env=env).labels
    assert labels == {"ok": "yes", "key": "val"}


def test_proto_round_trip():
    """`RunIdentity` fields round-trip cleanly into and out of a
    `BeginRunRequest` proto. The wire format is the contract between the SDK
    and the Rust server."""
    ident = RunIdentity(
        tenant_id="acme",
        actor="spiffe://acme/ns/treasury/sa/agent",
        subject="vamsi@example.com",
        agent_id="treasury-agent",
        aiplex_instance_id="inst-42",
        gateway_route="/a2a/treasury",
        scopes=["mcp:tools:bank_wire"],
        labels={"aiplex.plane": "a2a"},
    )
    req = pb.BeginRunRequest(
        app_name="treasury", user_id="vamsi@example.com",
        session_id="s1", invocation_id="inv1",
        lease_owner="t1", lease_ttl_ms=60_000,
        tenant_id=ident.tenant_id, actor=ident.actor, subject=ident.subject,
        agent_id=ident.agent_id, aiplex_instance_id=ident.aiplex_instance_id,
        gateway_route=ident.gateway_route,
        scopes=ident.scopes, labels=ident.labels)
    # Serialise and re-parse: this catches schema drift between the .proto
    # and the regenerated stubs.
    blob = req.SerializeToString()
    parsed = pb.BeginRunRequest.FromString(blob)
    assert parsed.tenant_id == ident.tenant_id
    assert parsed.actor == ident.actor
    assert parsed.subject == ident.subject
    assert parsed.agent_id == ident.agent_id
    assert parsed.aiplex_instance_id == ident.aiplex_instance_id
    assert parsed.gateway_route == ident.gateway_route
    assert list(parsed.scopes) == ident.scopes
    assert dict(parsed.labels) == ident.labels


# ─── Strict-mode validation (PR 12 item A) ────────────────────────────────


def test_validate_passes_when_required_fields_populated():
    ident = RunIdentity(
        tenant_id="acme",
        actor="spiffe://acme/treasury",
        agent_id="treasury-agent",
    )
    ident.validate()  # no raise


def test_validate_raises_missing_identity_with_field_list():
    from tape.adk.identity import MissingIdentity

    ident = RunIdentity(tenant_id="acme")  # missing actor + agent_id
    try:
        ident.validate()
    except MissingIdentity as ex:
        assert ex.missing == ["actor", "agent_id"]
        assert "AIPLEX_ACTOR" in str(ex)
        assert "AIPLEX_AGENT_ID" in str(ex)
    else:
        raise AssertionError("expected MissingIdentity")


def test_from_env_strict_raises_on_missing():
    from tape.adk.identity import MissingIdentity

    # No env at all → all three required fields missing → raise.
    try:
        RunIdentity.from_env(env={}, strict=True)
    except MissingIdentity as ex:
        assert set(ex.missing) >= {"tenant_id", "actor", "agent_id"}
    else:
        raise AssertionError("strict=True with empty env should raise")


def test_from_env_strict_passes_when_aiplex_env_complete():
    env = {
        "AIPLEX_TENANT_ID": "acme",
        "AIPLEX_ACTOR": "spiffe://acme/treasury",
        "AIPLEX_AGENT_ID": "treasury-agent",
    }
    ident = RunIdentity.from_env(env=env, strict=True)
    assert ident.tenant_id == "acme"


def test_from_env_inherits_strict_from_require_identity_env_var():
    """When AIPLEX_REQUIRE_IDENTITY=1 is set in the env, strict mode is
    on by default (AIPlex's deploy engine sets this on every Tape-backed pod)."""
    from tape.adk.identity import MissingIdentity

    env = {"AIPLEX_REQUIRE_IDENTITY": "1"}  # no identity → should raise
    try:
        RunIdentity.from_env(env=env)
    except MissingIdentity:
        pass
    else:
        raise AssertionError("AIPLEX_REQUIRE_IDENTITY=1 should imply strict=True")

    # Same env but explicit strict=False bypasses the auto-strict.
    ident = RunIdentity.from_env(env=env, strict=False)
    assert ident.is_empty()


def test_validate_with_custom_required_set():
    # Some deployments may only require tenant_id (e.g. dev with one
    # actor). The override knob lets them relax without going full
    # non-strict.
    ident = RunIdentity(tenant_id="acme")  # no actor / agent_id
    ident.validate(required=("tenant_id",))  # passes


def test_run_state_carries_identity():
    """`RunState` exposes the same identity fields back to clients (the
    AIPlex run timeline reads from these)."""
    rs = pb.RunState(
        run_id="r1", app_name="treasury", user_id="vamsi@example.com",
        session_id="s1", invocation_id="inv1", status=pb.RUN_STATUS_RUNNING,
        tenant_id="acme", actor="spiffe://x", subject="vamsi@example.com",
        agent_id="treasury-agent", aiplex_instance_id="inst-42",
        gateway_route="/a2a/treasury",
        scopes=["mcp:tools:bank_wire"],
        labels={"aiplex.plane": "a2a"},
    )
    parsed = pb.RunState.FromString(rs.SerializeToString())
    assert parsed.tenant_id == "acme"
    assert parsed.subject == "vamsi@example.com"
    assert list(parsed.scopes) == ["mcp:tools:bank_wire"]
    assert dict(parsed.labels) == {"aiplex.plane": "a2a"}
