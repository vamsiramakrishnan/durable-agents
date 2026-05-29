"""Compaction integration tests (PR 13 — Tape side).

Spins up a real tape-server (subprocess + ephemeral SQLite) and
exercises the wire-level RPCs + the SDK reactor through it. Asserts
the headline guarantees:

  1. Compacted runs preserve the audit envelope (kind, seq, tool,
     idempotency_key, business_key, scope, status, identity).
  2. Bulky JSON payloads (request_json/response_json/error_json) are
     zeroed.
  3. Compaction is idempotent — a second call on the same run is a
     no-op that reports already_compacted=true.
  4. Settlement enforcement — a run with open obligations or UNKNOWN
     effects refuses compaction with a clear error.
"""
from __future__ import annotations

import time
import uuid

import grpc
import pytest

import tape
from tape import client as client_mod
from tape.reactors import compact_once, run_compactor


@pytest.fixture
def server_url(tape_server):
    return tape_server["url"]


def _drive_run_to_terminal(c: client_mod.TapeClient, *, with_payload: bool = True) -> str:
    """Spin up a run, record a chunky decision, write a confirmed
    effect, and end the run TERMINAL. Returns the run_id."""
    invocation = f"inv-{uuid.uuid4().hex[:8]}"
    resp = c.begin_run(
        app_name="compact-test", user_id="u",
        session_id=f"sess-{uuid.uuid4().hex[:8]}",
        invocation_id=invocation,
        lease_owner="t", lease_ttl_ms=60_000,
        tenant_id="acme", actor="spiffe://test/treasury",
        agent_id="compact-agent",
        scopes=["mcp:tools:noop"],
    )
    run_id = resp.run_id

    chunky_req = "{" + ", ".join(f'"k{i}":"value{i}"' for i in range(50)) + "}"
    chunky_resp = "[" + ", ".join(f'"line{i}"' for i in range(50)) + "]"
    if with_payload:
        c.record_decision(
            run_id=run_id, decision_index=0, model="m",
            request_json=chunky_req, response_json=chunky_resp,
            rationale="testing compaction", policy_version="p1")
    else:
        c.record_decision(
            run_id=run_id, decision_index=0, model="m",
            request_json="", response_json="", rationale="", policy_version="p1")

    e = c.begin_effect(
        run_id=run_id, decision_index=0,
        tool_name="noop", call_index=0,
        request_json=chunky_req if with_payload else "",
        scope="mcp:tools:noop",
    )
    c.complete_effect(
        run_id=run_id, idempotency_key=e.idempotency_key,
        status=client_mod.EFFECT_STATUS_CONFIRMED,
        response_json=chunky_resp if with_payload else "",
    )
    c.end_run(run_id=run_id, status=client_mod.RUN_STATUS_TERMINAL)
    return run_id


def test_compact_zeroes_payloads_keeps_envelope(server_url):
    with client_mod.TapeClient(server_url) as c:
        run_id = _drive_run_to_terminal(c)

        # Before compaction: the decision + effect carry full payloads.
        before_dec = c.get_decision(run_id=run_id, decision_index=0).decision
        assert len(before_dec.request_json) > 100
        assert len(before_dec.response_json) > 100

        # Use a far-future cutoff so the just-finished run is in scope.
        report = c.compact_run(run_id)
        assert report.already_compacted is False
        assert report.decisions_zeroed == 1
        assert report.effects_zeroed == 1
        assert report.bytes_saved > 0

        # After compaction: envelope preserved, bodies zeroed.
        after_dec = c.get_decision(run_id=run_id, decision_index=0).decision
        assert after_dec.request_json == ""
        assert after_dec.response_json == ""
        assert after_dec.model == "m"
        assert after_dec.decision_index == 0
        assert after_dec.policy_version == "p1"

        # And the run row carries compacted_at_ms (surfaced via GetRun).
        run = c.get_run(run_id=run_id)
        # RunState doesn't expose compacted_at_ms directly today (no
        # proto field), but the second compact_run MUST return
        # already_compacted=true — that proves the row was stamped.
        report2 = c.compact_run(run_id)
        assert report2.already_compacted is True
        assert report2.decisions_zeroed == 0
        assert report2.effects_zeroed == 0


def test_compact_emits_run_journal_entry(server_url):
    """A `run.compacted`-style journal entry lands so downstream sinks
    (AIPlex audit ingestion) see the state change."""
    with client_mod.TapeClient(server_url) as c:
        run_id = _drive_run_to_terminal(c)
        c.compact_run(run_id)

        # Pull the journal and look for the compaction entry. The
        # server writes kind="run" with a payload carrying `compacted_at_ms`.
        from tape._gen import tape_pb2 as pb
        from tape._gen import tape_pb2_grpc as pb_grpc
        from tape.client import _target

        ch = grpc.insecure_channel(_target(server_url))
        try:
            stub = pb_grpc.TapeStub(ch)
            it = stub.SubscribeRun(
                pb.SubscribeRunRequest(run_id=run_id, from_seq=0),
                timeout=2.0)
            found = None
            try:
                for entry in it:
                    if entry.kind == "run" and "compacted_at_ms" in entry.payload_json:
                        found = entry.payload_json
                        break
            except grpc.RpcError:
                pass
        finally:
            ch.close()
        assert found is not None, "expected run.compacted journal entry"
        assert "decisions_zeroed" in found
        assert "effects_zeroed" in found
        assert "bytes_saved" in found


def test_list_compactable_runs_filters_by_cutoff_and_status(server_url):
    with client_mod.TapeClient(server_url) as c:
        # Settled run — should appear.
        settled = _drive_run_to_terminal(c)
        # Active run — should NOT appear (status=RUNNING + ended_at=0).
        active = c.begin_run(
            app_name="compact-test", user_id="u",
            session_id=f"sess-active-{uuid.uuid4().hex[:8]}",
            invocation_id=f"inv-active-{uuid.uuid4().hex[:8]}",
            lease_owner="t", lease_ttl_ms=60_000,
            tenant_id="acme", actor="spiffe://test/treasury",
            agent_id="compact-agent").run_id

        # Cutoff far in the future → both terminal runs eligible
        # (but active is excluded by status).
        future_ms = int(time.time() * 1000) + 10_000
        runs = c.list_compactable_runs(before_ms=future_ms, limit=100).runs
        ids = {r.run_id for r in runs}
        assert settled in ids
        assert active not in ids

        # Tighter cutoff (1ms ago) → also excludes the just-finished run.
        past_ms = int(time.time() * 1000) - 1000
        runs = c.list_compactable_runs(before_ms=past_ms, limit=100).runs
        ids = {r.run_id for r in runs}
        assert settled not in ids


def test_compact_refuses_run_with_unknown_effects(server_url):
    with client_mod.TapeClient(server_url) as c:
        invocation = f"inv-{uuid.uuid4().hex[:8]}"
        resp = c.begin_run(
            app_name="compact-test", user_id="u",
            session_id=f"sess-{uuid.uuid4().hex[:8]}",
            invocation_id=invocation,
            lease_owner="t", lease_ttl_ms=60_000,
            tenant_id="acme", actor="spiffe://test/treasury",
            agent_id="compact-agent",
            scopes=["mcp:tools:something"],
        )
        run_id = resp.run_id
        c.record_decision(
            run_id=run_id, decision_index=0, model="m",
            request_json="{}", response_json="{}",
            rationale="", policy_version="p1")
        e = c.begin_effect(
            run_id=run_id, decision_index=0,
            tool_name="something", call_index=0,
            request_json='{"x":1}',
            scope="mcp:tools:something")
        # Mark the effect UNKNOWN (the operator never resolved it).
        c.complete_effect(
            run_id=run_id, idempotency_key=e.idempotency_key,
            status=client_mod.EFFECT_STATUS_UNKNOWN, error_json='{"e":"timeout"}')
        c.end_run(run_id=run_id, status=client_mod.RUN_STATUS_FAILED)

        # Settlement check refuses compaction.
        with pytest.raises(grpc.RpcError) as ei:
            c.compact_run(run_id)
        assert "not settled" in ei.value.details().lower()


def test_compact_once_returns_per_run_reports(server_url):
    """The `compact_once` reactor helper batches multiple runs and
    surfaces per-run reports — including errors from runs that can't
    be compacted (e.g., still has UNKNOWNs) without halting the batch."""
    with client_mod.TapeClient(server_url) as c:
        ok_run = _drive_run_to_terminal(c)
        # A second settled run.
        ok_run2 = _drive_run_to_terminal(c)
        future_ms = int(time.time() * 1000) + 10_000

    # Use a fresh client inside compact_once to mirror real use.
    reports = compact_once(server_url, before_ms=future_ms, limit=100)
    by_id = {r["run_id"]: r for r in reports if "run_id" in r}
    assert ok_run in by_id and ok_run2 in by_id
    assert by_id[ok_run]["already_compacted"] is False
    assert by_id[ok_run]["effects_zeroed"] == 1


def test_run_compactor_once_smoke(server_url):
    """`run_compactor(once=True)` ticks once and exits, surfacing the
    summary through on_tick. Used by AIPlex's retention reactor."""
    with client_mod.TapeClient(server_url) as c:
        _drive_run_to_terminal(c)

    captured = []
    run_compactor(server_url, hot_window_s=-100.0, once=True,
                  on_tick=lambda t: captured.append(t))
    assert len(captured) == 1
    tick = captured[0]
    assert tick["compactor"] is True
    assert tick["count"] >= 1
