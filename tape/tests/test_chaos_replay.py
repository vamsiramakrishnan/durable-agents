"""TapeChaos — Phase 2: snapshot + replay tests.

Asserts the determinism claim: same scenario, same seed → bit-identical
journals (after canonicalisation). Drives the surface end-to-end against
the `tape_server` fixture's real server.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SDK_PY = ROOT / "tape" / "sdk" / "python"
sys.path.insert(0, str(SDK_PY))


# ── Canonicalization unit tests (no server needed) ─────────────────────────

def test_canonical_strips_timestamps():
    from tape.chaos.snapshot import _canonical

    payload = {"run_id": "r-abc", "ts_ms": 12345, "tool": "wire", "status": "confirmed"}
    canonical = _canonical(payload, {"r-abc": "run-1"})
    assert "ts_ms" not in canonical
    assert canonical["run_id"] == "run-1"
    assert canonical["tool"] == "wire"


def test_canonical_remaps_runid_inside_strings():
    from tape.chaos.snapshot import _canonical
    # idempotency_key is of the form `<run_id>/decision-<i>/<tool>/<call>`.
    payload = {"idempotency_key": "r-abc/decision-0/wire/0"}
    out = _canonical(payload, {"r-abc": "run-1"})
    assert out["idempotency_key"] == "run-1/decision-0/wire/0"


def test_canonical_strips_lease_owner():
    from tape.chaos.snapshot import _canonical

    payload = {"lease_owner": "host-42:9999", "status": "running"}
    out = _canonical(payload, {})
    assert "lease_owner" not in out


def test_snapshot_eq_ignores_ts_and_lease():
    from tape.chaos.snapshot import Snapshot, JournalLine, _to_tuple

    a = Snapshot(run_id="r-A", lines=(
        JournalLine(kind="run", payload=_to_tuple({"status": "running"})),
        JournalLine(kind="decision", payload=_to_tuple({"decision_index": 0, "model": "m"})),
    ))
    b = Snapshot(run_id="r-B", lines=(
        JournalLine(kind="run", payload=_to_tuple({"status": "running"})),
        JournalLine(kind="decision", payload=_to_tuple({"decision_index": 0, "model": "m"})),
    ))
    assert a == b


def test_snapshot_neq_when_status_differs():
    from tape.chaos.snapshot import Snapshot, JournalLine, _to_tuple

    a = Snapshot(run_id="r-A", lines=(
        JournalLine(kind="run", payload=_to_tuple({"status": "running"})),
    ))
    b = Snapshot(run_id="r-B", lines=(
        JournalLine(kind="run", payload=_to_tuple({"status": "failed"})),
    ))
    assert a != b
    diff = a.diff(b)
    assert len(diff) == 1 and diff[0][1] == "!="


# ── End-to-end replay against a real server ────────────────────────────────

def test_replay_two_identical_runs_are_bit_identical(tape_server):
    """The headline Phase-2 property: same scenario body, same seed,
    same server -> bit-identical journals."""
    import tape.chaos as chaos
    from tape.client import TapeClient

    url = tape_server["url"]
    counter = {"i": 0}

    @chaos.replayable
    def body(client: TapeClient, sess):
        counter["i"] += 1
        # Each pass uses a fresh invocation id so we get distinct runs (the
        # canonicalizer will remap both to "run-1").
        inv = f"inv-{counter['i']}"
        try:
            resp = client.begin_run(app_name="t", user_id="u",
                                     session_id="s", invocation_id=inv)
            rid = resp.run_id
            sess.set_run_id(rid)
            client.record_decision(run_id=rid, decision_index=0, model="m",
                                    request_json="{}", response_json='{"x":1}',
                                    rationale="", policy_version="")
            client.begin_effect(run_id=rid, decision_index=0, tool_name="hello",
                                 call_index=0, request_json='{"k":"v"}',
                                 custom_key="")
            # Look the effect up so we have its idempotency_key, then complete.
            eff = next(iter(client.list_pending_effects(
                include_pending=True, include_unknown=False, limit=10).effects))
            client.complete_effect(run_id=rid, idempotency_key=eff.idempotency_key,
                                    status=2,  # CONFIRMED
                                    response_json='{"ok":true}', error_json="")
            client.end_run(run_id=rid)
            return rid
        finally:
            client.close()

    scen = chaos.scenario(name="identical-pass", seed=42)
    report = chaos.replay(scen, body, url=url, deadline_s=3.0)
    assert report.bit_identical, str(report)
    assert len(report.snap_a.lines) > 0
    assert report.snap_a == report.snap_b


def test_replay_detects_a_drift_we_inject(tape_server):
    """Inject an intentional drift in the second pass — replay must catch it."""
    import tape.chaos as chaos
    from tape.client import TapeClient

    url = tape_server["url"]
    pass_no = {"n": 0}

    @chaos.replayable
    def body(client: TapeClient, sess):
        pass_no["n"] += 1
        inv = f"inv-drift-{pass_no['n']}"
        try:
            resp = client.begin_run(app_name="t", user_id="u",
                                     session_id="s2", invocation_id=inv)
            rid = resp.run_id
            sess.set_run_id(rid)
            # Pass 1: record decision_index=0; pass 2: decision_index=1 (drift).
            idx = 0 if pass_no["n"] == 1 else 1
            client.record_decision(run_id=rid, decision_index=idx, model="m",
                                    request_json="{}", response_json='{}',
                                    rationale="", policy_version="")
            client.end_run(run_id=rid)
            return rid
        finally:
            client.close()

    report = chaos.replay(chaos.scenario(name="drift-canary", seed=1),
                          body, url=url, deadline_s=3.0)
    assert not report.bit_identical, \
        f"replay should have detected the injected drift; got {report}"
    assert any("decision" in s.lower() for s in report.diff_summary), \
        f"diff_summary should mention the decision; got {report.diff_summary}"


def test_snapshot_capture_streams_until_terminal(tape_server):
    """`capture` should stop early when it sees a terminal `run` entry — it
    must not block on the long-lived SubscribeRun stream."""
    import time

    import tape.chaos as chaos
    from tape.client import TapeClient

    client = TapeClient(tape_server["url"])
    try:
        resp = client.begin_run(app_name="t", user_id="u",
                                 session_id="snap", invocation_id="inv-snap")
        rid = resp.run_id
        client.end_run(run_id=rid)
        t0 = time.monotonic()
        snap = chaos.capture_snapshot(client, rid, deadline_s=5.0)
        elapsed = time.monotonic() - t0
        # Without the early-exit we'd block until the 5s deadline. The
        # terminal `run` entry should cut us off in well under 2s.
        assert elapsed < 3.0, f"capture took {elapsed:.1f}s — early-exit didn't fire"
        assert len(snap.lines) >= 1
    finally:
        client.close()
