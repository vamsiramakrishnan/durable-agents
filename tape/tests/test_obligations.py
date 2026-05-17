"""Obligation ledger thickness: claim/CAS, retry/backoff, COMMITTED-as-lease,
registry portability via compensator_ref, cross-run drainer feed, event-driven
reactor wake.

This is the parity test for the four buckets the README walks through:
  1. ambient drainer + retry
  2. status filter / cross-run operator query
  3. COMMITTED state is actually used (as the claim lease)
  4. compensator_ref portability (drainer process didn't import the agent)
"""

from __future__ import annotations

import json
import sqlite3
import sys
import threading
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SDK_PY = ROOT / "tape" / "sdk" / "python"
sys.path.insert(0, str(SDK_PY))


def _query(db: str, sql: str, params=()):
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return con.execute(sql, params).fetchall()
    finally:
        con.close()


def _begin_run_with_one_effect(c, app="t", session_prefix="ob"):
    """Helper: open a run, write one CONFIRMED effect, return (run_id, effect_key)."""
    import tape.client as tc
    sess = f"{session_prefix}-{int(time.time()*1e6) % 1_000_000}"
    run = c.begin_run(app_name=app, user_id="u", session_id=sess, invocation_id=f"inv-{sess}",
                      lease_owner="test", lease_ttl_ms=60_000)
    rid = run.run_id
    c.record_decision(run_id=rid, decision_index=0, response_json="{}")
    be = c.begin_effect(run_id=rid, decision_index=0, tool_name="execute_sweep",
                        call_index=0, request_json="{}")
    c.complete_effect(run_id=rid, idempotency_key=be.idempotency_key,
                       status=tc.EFFECT_STATUS_CONFIRMED, response_json='{"wire_id":"w1"}')
    return rid, be.idempotency_key


# ── bucket 1: ambient drainer + bounded retry ──────────────────────────────

def test_record_attempt_retries_then_stucks(tape_server):
    """A failed attempt with retries left → PENDING with next_attempt_at_ms.
    Once attempts reach max_attempts, the next failure terminally STUCKs."""
    from tape.client import TapeClient
    import tape.client as tc

    c = TapeClient(tape_server["url"])
    rid, ek = _begin_run_with_one_effect(c)
    ob = c.register_compensation(run_id=rid, effect_key=ek, kind="reverse_wire",
                                  payload_json="{}", max_attempts=2)
    assert ob.status == tc.OBLIGATION_STATUS_PENDING
    assert ob.max_attempts == 2
    assert ob.next_attempt_at_ms > 0

    # Claim → COMMITTED with lease + claimer.
    cl = c.claim_obligation(run_id=rid, obligation_seq=ob.seq, claimer="drainer-A",
                             lease_ttl_ms=30_000)
    assert cl.acquired
    assert cl.obligation.status == tc.OBLIGATION_STATUS_COMMITTED
    assert cl.obligation.claimed_by == "drainer-A"
    assert cl.obligation.claim_expires_at_ms > int(time.time() * 1000)

    # Attempt #1 fails → back to PENDING with attempts=1, next_attempt_at_ms set
    # in the near future. The drainer must respect the backoff: a claim before
    # the deadline is rejected; after it, the claim succeeds.
    soon = int(time.time()*1000) + 200
    upd = c.record_obligation_attempt(run_id=rid, obligation_seq=ob.seq,
                                       error="network timeout", next_attempt_at_ms=soon)
    assert upd.status == tc.OBLIGATION_STATUS_PENDING
    assert upd.attempts == 1
    assert upd.last_error == "network timeout"
    assert upd.claimed_by == ""  # lease was cleared

    # An immediate re-claim should fail — backoff hasn't elapsed.
    cl_premature = c.claim_obligation(run_id=rid, obligation_seq=ob.seq, claimer="drainer-A",
                                       lease_ttl_ms=30_000)
    assert not cl_premature.acquired, "backoff was ignored — claim succeeded before next_attempt_at_ms"

    # Wait for the backoff, then re-claim succeeds.
    time.sleep(0.3)
    cl2 = c.claim_obligation(run_id=rid, obligation_seq=ob.seq, claimer="drainer-A",
                              lease_ttl_ms=30_000)
    assert cl2.acquired
    upd2 = c.record_obligation_attempt(run_id=rid, obligation_seq=ob.seq,
                                        error="still failing",
                                        next_attempt_at_ms=int(time.time()*1000)+500)
    assert upd2.status == tc.OBLIGATION_STATUS_STUCK
    assert upd2.attempts == 2
    assert upd2.next_attempt_at_ms == 0
    c.close()


def test_terminal_now_attempt_skips_retries(tape_server):
    """next_attempt_at_ms == 0 forces STUCK even with retries remaining."""
    from tape.client import TapeClient
    import tape.client as tc

    c = TapeClient(tape_server["url"])
    rid, ek = _begin_run_with_one_effect(c)
    ob = c.register_compensation(run_id=rid, effect_key=ek, kind="reverse_wire",
                                  payload_json="{}", max_attempts=10)
    c.claim_obligation(run_id=rid, obligation_seq=ob.seq, claimer="d", lease_ttl_ms=30_000)
    upd = c.record_obligation_attempt(run_id=rid, obligation_seq=ob.seq,
                                       error="permanent: invalid account",
                                       next_attempt_at_ms=0)
    assert upd.status == tc.OBLIGATION_STATUS_STUCK
    assert upd.attempts == 1   # one attempt, terminally stuck
    c.close()


# ── bucket 2: status filter + cross-run operator query ─────────────────────

def test_status_filter_and_cross_run_unresolved(tape_server):
    """list_obligations(status_filter=PENDING) returns only PENDING rows.
    list_unresolved_obligations is cross-run and respects include_* flags."""
    from tape.client import TapeClient
    import tape.client as tc

    c = TapeClient(tape_server["url"])
    rid_a, ek_a = _begin_run_with_one_effect(c, session_prefix="run-a")
    rid_b, ek_b = _begin_run_with_one_effect(c, session_prefix="run-b")
    ob_a = c.register_compensation(run_id=rid_a, effect_key=ek_a, kind="reverse_wire",
                                    payload_json="{}", max_attempts=1)
    ob_b = c.register_compensation(run_id=rid_b, effect_key=ek_b, kind="reverse_wire",
                                    payload_json="{}", max_attempts=1)
    # Run B's obligation: claim it but never resolve — leaves it COMMITTED.
    c.claim_obligation(run_id=rid_b, obligation_seq=ob_b.seq, claimer="d", lease_ttl_ms=60_000)

    # Per-run status filter.
    pending_only = c.list_obligations(run_id=rid_a, status_filter=tc.OBLIGATION_STATUS_PENDING).obligations
    assert len(pending_only) == 1
    assert pending_only[0].seq == ob_a.seq
    committed_only = c.list_obligations(run_id=rid_b, status_filter=tc.OBLIGATION_STATUS_COMMITTED).obligations
    assert len(committed_only) == 1
    assert committed_only[0].seq == ob_b.seq

    # Cross-run feed: PENDING-and-due ⊕ COMMITTED-expired.
    # Default flags: include_pending=True, include_committed_expired=True, include_stuck=False.
    # Run B's lease is fresh (60s), so it should NOT appear in the default feed.
    fresh = c.list_unresolved_obligations(limit=100).obligations
    seqs = {(o.run_id, o.seq) for o in fresh}
    assert (rid_a, ob_a.seq) in seqs
    assert (rid_b, ob_b.seq) not in seqs

    # Force the lease to look expired by passing a future now_ms.
    future = int(time.time() * 1000) + 5 * 60_000
    expired = c.list_unresolved_obligations(limit=100, now_ms=future).obligations
    expired_seqs = {(o.run_id, o.seq) for o in expired}
    assert (rid_b, ob_b.seq) in expired_seqs

    # Operator-dashboard view: include STUCK rows.
    # First, stick something.
    c.record_obligation_attempt(run_id=rid_a, obligation_seq=ob_a.seq,
                                 error="bad data", next_attempt_at_ms=0)
    stuck_view = c.list_unresolved_obligations(include_pending=False, include_stuck=True,
                                                include_committed_expired=False).obligations
    stuck_seqs = {(o.run_id, o.seq) for o in stuck_view}
    assert (rid_a, ob_a.seq) in stuck_seqs
    c.close()


# ── bucket 3: COMMITTED state is the lease — claim CAS works ───────────────

def test_concurrent_claims_one_winner(tape_server):
    """Two drainers race on the same obligation. Exactly one wins; the other
    gets acquired=False with the current row showing the winner's claimed_by."""
    from tape.client import TapeClient
    import tape.client as tc

    c = TapeClient(tape_server["url"])
    rid, ek = _begin_run_with_one_effect(c)
    ob = c.register_compensation(run_id=rid, effect_key=ek, kind="reverse_wire",
                                  payload_json="{}")

    results: list = []
    barrier = threading.Barrier(8)

    def claimer(name):
        client = TapeClient(tape_server["url"])
        try:
            barrier.wait()
            r = client.claim_obligation(run_id=rid, obligation_seq=ob.seq, claimer=name,
                                          lease_ttl_ms=60_000)
            results.append((name, r.acquired, r.obligation.claimed_by if r.obligation else ""))
        finally:
            client.close()

    threads = [threading.Thread(target=claimer, args=(f"d-{i}",)) for i in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()

    winners = [(n, ack, cb) for (n, ack, cb) in results if ack]
    losers = [(n, ack, cb) for (n, ack, cb) in results if not ack]
    assert len(winners) == 1, f"expected exactly one winner, got {winners}"
    winner_name = winners[0][0]
    # Every loser should see the winner's claimed_by — the CAS is honest.
    for (n, ack, cb) in losers:
        assert cb == winner_name, f"loser {n} saw claimed_by={cb!r}, expected {winner_name!r}"
    c.close()


def test_expired_lease_is_reclaimable(tape_server):
    """A COMMITTED row whose lease passed can be re-claimed by another drainer.
    This is what makes the crashed-drainer story work."""
    from tape.client import TapeClient
    import tape.client as tc

    c = TapeClient(tape_server["url"])
    rid, ek = _begin_run_with_one_effect(c)
    ob = c.register_compensation(run_id=rid, effect_key=ek, kind="reverse_wire",
                                  payload_json="{}")
    # Claim with a tiny lease, then "crash" (just don't follow up).
    cl = c.claim_obligation(run_id=rid, obligation_seq=ob.seq, claimer="d-original",
                             lease_ttl_ms=50)
    assert cl.acquired
    # Wait for the lease to expire.
    time.sleep(0.15)
    # A second drainer can now claim it.
    cl2 = c.claim_obligation(run_id=rid, obligation_seq=ob.seq, claimer="d-replacement",
                              lease_ttl_ms=30_000)
    assert cl2.acquired, "an expired lease should be reclaimable"
    assert cl2.obligation.claimed_by == "d-replacement"
    c.close()


# ── bucket 4: compensator_ref portability ──────────────────────────────────

def test_compensator_ref_resolves_when_registry_is_empty(tape_server):
    """A drainer process that didn't import the agent module can still resolve
    the inverse via compensator_ref. We simulate that by clearing the in-process
    registry, then asking the drainer to compensate."""
    from tape.client import TapeClient
    import tape.client as tc
    # `import tape` shadows the `.effect` submodule with a function in the
    # package namespace, so reach the module via sys.modules.
    import sys as _sys
    import tape.effect  # ensure the module is registered
    tape_effect_mod = _sys.modules["tape.effect"]
    import tape

    c = TapeClient(tape_server["url"])
    rid, ek = _begin_run_with_one_effect(c)
    # Register with the compensator_ref pointing at our module-level callable.
    ref = f"{__name__}:_record_inverse_call"
    _INVERSE_CALLS.clear()
    ob = c.register_compensation(run_id=rid, effect_key=ek, kind="reverse_wire",
                                  payload_json=json.dumps({"wire_id": "w1"}),
                                  compensator_ref=ref)
    assert ob.compensator_ref == ref

    # Simulate a fresh drainer: blow away the in-process registry so the
    # only way to find the inverse is via compensator_ref.
    saved = dict(tape_effect_mod._COMPENSATORS)
    tape_effect_mod._COMPENSATORS.clear()
    try:
        r = tape.compensate_one(ob, client=c, claimer="drainer-no-registry")
    finally:
        tape_effect_mod._COMPENSATORS.update(saved)

    assert r["outcome"] == "compensated", r
    assert _INVERSE_CALLS == [{"wire_id": "w1"}], _INVERSE_CALLS
    # Sanity: the obligation row is now terminal COMPENSATED.
    ob_after = [o for o in c.list_obligations(run_id=rid, only_unresolved=False).obligations
                 if o.seq == ob.seq][0]
    assert ob_after.status == tc.OBLIGATION_STATUS_COMPENSATED
    assert json.loads(ob_after.result_json) == {"ok": True}
    c.close()


# A module-level callable that compensator_ref can resolve.
_INVERSE_CALLS: list = []
def _record_inverse_call(**kwargs):
    _INVERSE_CALLS.append(kwargs)
    return {"ok": True}


# ── event-driven reactor: subscribe → drain on transition ──────────────────

@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_event_driven_reactor_drains_on_register(tape_server):
    """The event-driven reactor subscribes to kind="obligation" entries. When
    we register a compensation, the registration emits a journal entry, the
    reactor wakes, calls compensate_once, and the obligation is COMPENSATED."""
    from tape.client import TapeClient
    import tape.client as tc
    import tape.reactors as reactors
    import sys as _sys
    import tape.effect  # noqa: F401 — register the submodule
    tape_effect_mod = _sys.modules["tape.effect"]

    # Register a compensator in this process so the drainer can run it.
    def _comp(**kwargs):
        return {"reversed": kwargs.get("wire_id", "")}
    tape_effect_mod.register_compensator("reverse_wire", _comp)

    url = tape_server["url"]
    c = TapeClient(url)
    rid, ek = _begin_run_with_one_effect(c)

    events: list = []
    stop = threading.Event()

    def runner():
        # The reactor loops until the gRPC stream errors (e.g. server stops).
        # We swallow those errors at test teardown.
        try:
            reactors.run_compensations_event_driven(
                url, claimer="reactor-1", idle_window_s=0.3, catchup=True,
                on_event=lambda e: (events.append(e), None) and None
                                    if not stop.is_set() else None)
        except Exception:  # noqa: BLE001
            pass

    reactor_thread = threading.Thread(target=runner, daemon=True)
    reactor_thread.start()
    time.sleep(0.2)  # let the subscription attach

    try:
        ob = c.register_compensation(run_id=rid, effect_key=ek, kind="reverse_wire",
                                      payload_json=json.dumps({"wire_id": "w1"}))

        deadline = time.time() + 10.0
        final_status = None
        while time.time() < deadline:
            rows = c.list_obligations(run_id=rid, only_unresolved=False).obligations
            if rows and rows[0].status in (tc.OBLIGATION_STATUS_COMPENSATED, tc.OBLIGATION_STATUS_STUCK):
                final_status = rows[0].status
                break
            time.sleep(0.1)
        assert final_status == tc.OBLIGATION_STATUS_COMPENSATED, \
            f"reactor did not compensate the obligation; final={final_status}, events={events[-5:]}"

        outcomes = [e for e in events if isinstance(e, dict) and e.get("outcome")]
        assert any(o.get("outcome") == "compensated" for o in outcomes), outcomes
    finally:
        stop.set()
        c.close()
