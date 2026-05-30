"""The headline tests for the non-idempotent outbox + reconciliation contract.

These tests verify the safety claim of the GCP hardening plan, at the wire:

  > Tape does not pretend exactly-once is possible without upstream support.
  > For non-idempotent upstreams, intent + outbox + observation is the only
  > safe path: a crash mid-dispatch must not cause a blind retry; the
  > reconciler must resolve ambiguity by asking the counterparty.

The fixtures here go straight through the gRPC client (no ADK), so each
behaviour is testable in isolation. The bigger ADK kill-and-resume story is
covered by `examples/non_idempotent_bank/` + the existing kill harness.
"""

from __future__ import annotations

import json
import time
import uuid

import grpc
import pytest

import tape
from tape import connectors
from tape.client import TapeClient
from tape.connectors.base import DispatchResult, ObservationResult, CompensationResult
from tape.reactors import outbox as outbox_reactor
from tape.reactors import reconcile_once


# ── small helpers ──────────────────────────────────────────────────────────

# PR 12: the server's wire-level scope enforcement refuses non-idempotent
# effects without a declared scope, even when the dispatch path would have
# refused them anyway (INLINE / outbox missing connector / etc.). All the
# test helpers below grant this scope on BeginRun and declare it on
# BeginEffect so the wire-level check passes and the test gets to assert
# the *downstream* behaviour it actually cares about.
_DEFAULT_SCOPE = "tape:tools:wire_money"


def _begin_run(c, *, app="test", user="u", session=None, invocation=None,
               scopes=None):
    s = session or f"sess-{uuid.uuid4().hex[:8]}"
    inv = invocation or f"inv-{uuid.uuid4().hex[:8]}"
    return c.begin_run(app_name=app, user_id=user, session_id=s, invocation_id=inv,
                       lease_owner="test", lease_ttl_ms=60_000,
                       scopes=scopes if scopes is not None else [_DEFAULT_SCOPE])


def _begin_outbox_effect(c, run_id, *, tool="wire_money", business_key="",
                         connector="bank.wire",
                         semantics=tape.EFFECT_SEMANTICS_NON_IDEMPOTENT,
                         scope=_DEFAULT_SCOPE):
    return c.begin_effect(
        run_id=run_id, decision_index=0, tool_name=tool, call_index=0,
        request_json='{"amount": 100}',
        semantics=semantics,
        dispatch_mode=tape.EFFECT_DISPATCH_MODE_OUTBOX,
        business_key=business_key, connector=connector,
        scope=scope)


# ── core safety: NON_IDEMPOTENT + INLINE is refused ─────────────────────────

def test_non_idempotent_inline_is_refused(tape_server):
    """The server must refuse to register a non-idempotent effect with inline
    dispatch. The whole plan rests on the agent never calling a non-idempotent
    upstream from a tool body."""
    with TapeClient(tape_server["url"]) as c:
        run = _begin_run(c)
        with pytest.raises(grpc.RpcError) as ex:
            c.begin_effect(
                run_id=run.run_id, decision_index=0, tool_name="wire_money",
                call_index=0, request_json="{}",
                semantics=tape.EFFECT_SEMANTICS_NON_IDEMPOTENT,
                dispatch_mode=tape.EFFECT_DISPATCH_MODE_INLINE,
                scope=_DEFAULT_SCOPE)
        assert "NON_IDEMPOTENT" in ex.value.details()


# ── business_key dedupe ─────────────────────────────────────────────────────

def test_business_key_dedupes_effect_creation(tape_server):
    """Two runs that share a `(connector, business_key)` see the same effect
    row — the second begin_effect returns the existing record instead of
    inserting a duplicate. (The unique index also guarantees this at the
    storage layer.)"""
    with TapeClient(tape_server["url"]) as c:
        run1 = _begin_run(c, invocation="inv-1")
        run2 = _begin_run(c, invocation="inv-2")
        e1 = _begin_outbox_effect(c, run1.run_id, business_key="acct:1000:2026-05-17")
        e2 = _begin_outbox_effect(c, run2.run_id, business_key="acct:1000:2026-05-17")
        # The second call returns the FIRST run's effect (same row).
        assert e1.idempotency_key == e2.idempotency_key
        assert e1.seq == e2.seq


# ── outbox claim is a single winner ─────────────────────────────────────────

def test_outbox_claim_is_single_winner(tape_server):
    """Two concurrent dispatchers asking to claim the same outbox effect: one
    wins, the other gets acquired=False with the current row. The lease keeps
    the loser from doing anything dangerous."""
    with TapeClient(tape_server["url"]) as c:
        run = _begin_run(c)
        e = _begin_outbox_effect(c, run.run_id, business_key="bk-1")
        c1 = c.claim_effect_dispatch(run_id=run.run_id,
                                     idempotency_key=e.idempotency_key,
                                     claimer="dispatcher-A")
        c2 = c.claim_effect_dispatch(run_id=run.run_id,
                                     idempotency_key=e.idempotency_key,
                                     claimer="dispatcher-B")
        assert c1.acquired is True
        assert c2.acquired is False
        # Both responses surface the row; the winner is reflected in `dispatch_claimed_by`.
        assert c1.effect.dispatch_claimed_by == "dispatcher-A"
        assert c2.effect.dispatch_claimed_by == "dispatcher-A"


def test_expired_dispatch_lease_becomes_reclaimable(tape_server):
    """If a dispatcher claims a row and crashes (the lease expires), a later
    claim must succeed — the row is reclaimable, not pinned forever."""
    with TapeClient(tape_server["url"]) as c:
        run = _begin_run(c)
        e = _begin_outbox_effect(c, run.run_id, business_key="bk-lease")
        # Take a 1ms lease and let it expire.
        c1 = c.claim_effect_dispatch(run_id=run.run_id,
                                     idempotency_key=e.idempotency_key,
                                     claimer="dispatcher-A", lease_ttl_ms=1)
        assert c1.acquired is True
        time.sleep(0.05)
        c2 = c.claim_effect_dispatch(run_id=run.run_id,
                                     idempotency_key=e.idempotency_key,
                                     claimer="dispatcher-B")
        assert c2.acquired is True
        assert c2.effect.dispatch_claimed_by == "dispatcher-B"


# ── non-idempotent + unknown → no auto-retry ────────────────────────────────

def test_non_idempotent_unknown_does_not_auto_retry(tape_server):
    """A dispatch that returns UNKNOWN must drive the effect to status
    UNKNOWN with next_dispatch_at_ms = 0 — the outbox dispatcher must not
    pick it up again. The reconciler is the only path to resolution."""
    with TapeClient(tape_server["url"]) as c:
        run = _begin_run(c)
        e = _begin_outbox_effect(c, run.run_id, business_key="bk-unknown")
        # Claim and record an unknown attempt (next_dispatch_at_ms = 0).
        c.claim_effect_dispatch(run_id=run.run_id, idempotency_key=e.idempotency_key,
                                 claimer="d")
        c.record_dispatch_attempt(run_id=run.run_id, idempotency_key=e.idempotency_key,
                                  error="ack lost (test)",
                                  next_dispatch_at_ms=0)
        # The effect is now UNKNOWN.
        got = c.get_effect(run_id=run.run_id, idempotency_key=e.idempotency_key)
        assert got.effect.status == tape.EFFECT_STATUS_UNKNOWN
        # The outbox dispatcher (list_effects_to_dispatch) must NOT return it.
        for eff in c.list_effects_to_dispatch(limit=100).effects:
            assert eff.idempotency_key != e.idempotency_key


def test_idempotent_unknown_can_retry_after_absent(tape_server):
    """When the reconciler observes ABSENT on an IDEMPOTENT effect, the
    server moves it back to PENDING with `next_dispatch_at_ms = now` so the
    outbox dispatcher can safely retry."""
    with TapeClient(tape_server["url"]) as c:
        run = _begin_run(c)
        # IDEMPOTENT semantics, OUTBOX dispatch (e.g. a payment gateway).
        e = c.begin_effect(
            run_id=run.run_id, decision_index=0, tool_name="charge_card",
            call_index=0, request_json='{"amount": 100}',
            semantics=tape.EFFECT_SEMANTICS_IDEMPOTENT,
            dispatch_mode=tape.EFFECT_DISPATCH_MODE_OUTBOX,
            business_key="bk-idem-unknown", connector="cards.charge")
        # Dispatch returns unknown; effect goes to UNKNOWN.
        c.claim_effect_dispatch(run_id=run.run_id, idempotency_key=e.idempotency_key,
                                 claimer="d")
        c.record_dispatch_attempt(run_id=run.run_id, idempotency_key=e.idempotency_key,
                                  error="timeout", next_dispatch_at_ms=0)
        assert c.get_effect(run_id=run.run_id, idempotency_key=e.idempotency_key).effect.status == tape.EFFECT_STATUS_UNKNOWN
        # Reconciler observes ABSENT.
        c.record_external_observation(
            run_id=run.run_id, idempotency_key=e.idempotency_key,
            resolution=tape.EFFECT_RESOLUTION_ABSENT)
        got = c.get_effect(run_id=run.run_id, idempotency_key=e.idempotency_key).effect
        # IDEMPOTENT + ABSENT → safe to re-issue.
        assert got.status == tape.EFFECT_STATUS_PENDING
        # And the outbox dispatcher sees it as eligible again.
        ready = [e for e in c.list_effects_to_dispatch(limit=100).effects
                 if e.idempotency_key == got.idempotency_key]
        assert len(ready) == 1


def test_non_idempotent_absent_becomes_failed_not_pending(tape_server):
    """The mirror case: a NON_IDEMPOTENT effect observed as ABSENT must land
    as FAILED, not back-to-PENDING. Re-issuing without upstream confirmation
    risks a duplicate."""
    with TapeClient(tape_server["url"]) as c:
        run = _begin_run(c)
        e = _begin_outbox_effect(c, run.run_id, business_key="bk-nonidem-absent")
        c.claim_effect_dispatch(run_id=run.run_id, idempotency_key=e.idempotency_key,
                                 claimer="d")
        c.record_dispatch_attempt(run_id=run.run_id, idempotency_key=e.idempotency_key,
                                  error="timeout", next_dispatch_at_ms=0)
        c.record_external_observation(
            run_id=run.run_id, idempotency_key=e.idempotency_key,
            resolution=tape.EFFECT_RESOLUTION_ABSENT)
        got = c.get_effect(run_id=run.run_id, idempotency_key=e.idempotency_key).effect
        assert got.status == tape.EFFECT_STATUS_FAILED


# ── duplicate observation registers compensation ───────────────────────────

def test_duplicate_observation_creates_compensation(tape_server):
    """When the reconciler discovers the counterparty has TWO matching
    operations for the same business key (a duplicate), the server marks the
    effect CONFIRMED and registers a compensation obligation — atomically."""
    with TapeClient(tape_server["url"]) as c:
        run = _begin_run(c)
        e = _begin_outbox_effect(c, run.run_id, business_key="bk-dup")
        c.claim_effect_dispatch(run_id=run.run_id, idempotency_key=e.idempotency_key,
                                 claimer="d")
        c.record_dispatch_attempt(run_id=run.run_id, idempotency_key=e.idempotency_key,
                                  error="timeout", next_dispatch_at_ms=0)
        # Reconciler asks the bank and learns: two wires happened for this key.
        c.record_external_observation(
            run_id=run.run_id, idempotency_key=e.idempotency_key,
            resolution=tape.EFFECT_RESOLUTION_DUPLICATE,
            external_ref="wire-extra-12345",
            compensate_on_duplicate_kind="reverse_wire")
        # The effect's primary status: CONFIRMED + external_ref recorded.
        got = c.get_effect(run_id=run.run_id, idempotency_key=e.idempotency_key).effect
        assert got.status == tape.EFFECT_STATUS_CONFIRMED
        assert got.external_ref == "wire-extra-12345"
        # And a compensation obligation now exists for the duplicate side effect.
        obs = c.list_obligations(run_id=run.run_id, only_unresolved=True).obligations
        assert any(o.kind == "reverse_wire" and o.effect_key == e.idempotency_key
                   for o in obs), obs


# ── end-to-end with a fake non-idempotent bank + the outbox reactor ────────

class FakeNonIdempotentBank:
    """A bank that has no idempotency key support. Every wire() call lands.
    Counts wires per business key, so the test can assert how many calls
    actually crossed the wire."""

    def __init__(self):
        self.wires = []   # list of (business_key, amount)
        self.next_id = 0
        self.dispatch_should_unknown_once = False

    def wire(self, business_key: str, amount: int) -> str:
        self.next_id += 1
        wid = f"wire-{self.next_id}"
        self.wires.append((business_key, amount, wid))
        return wid

    def by_business_key(self, business_key: str):
        return [w for w in self.wires if w[0] == business_key]


class _BankConnector:
    """A connector for the fake bank. dispatch() emulates "the network ate
    our ack" once (so we exercise the UNKNOWN path), then observes truthfully."""

    name = "bank.wire"

    def __init__(self, bank: FakeNonIdempotentBank):
        self.bank = bank

    def dispatch(self, effect):
        # The "interesting" behaviour: the dispatch DOES call the bank, but
        # then the connector returns `unknown` (simulating a lost ack) so the
        # reactor can't tell whether the call landed. This is the exact
        # window the safety claim covers.
        req = json.loads(effect.request_json or "{}")
        self.bank.wire(effect.business_key, req.get("amount", 0))
        if self.bank.dispatch_should_unknown_once:
            self.bank.dispatch_should_unknown_once = False
            return DispatchResult(status="unknown",
                                  error={"reason": "simulated lost ack"})
        # Otherwise: confirmed.
        wid = f"wire-{self.bank.next_id}"
        return DispatchResult(status="confirmed", external_ref=wid,
                              response={"wire_id": wid})

    def observe(self, effect):
        matches = self.bank.by_business_key(effect.business_key)
        if not matches:
            return ObservationResult(status="absent")
        if len(matches) > 1:
            return ObservationResult(status="duplicate",
                                     external_ref=matches[0][2])
        return ObservationResult(status="confirmed", external_ref=matches[0][2])

    def compensate(self, obligation):
        return CompensationResult(status="compensated", response={"reversed": True})


def test_outbox_reactor_drives_one_call_and_reconciler_resolves_unknown(tape_server):
    """The headline scenario: a non-idempotent upstream + a dispatcher that
    returned UNKNOWN once → the reconciler asks the bank and confirms. The
    bank was called exactly once; no blind retry happens."""
    bank = FakeNonIdempotentBank()
    bank.dispatch_should_unknown_once = True
    connectors.clear()
    connectors.register(_BankConnector(bank))

    with TapeClient(tape_server["url"]) as c:
        run = _begin_run(c)
        _begin_outbox_effect(c, run.run_id, business_key="bk-headline")

        # First outbox tick: connector calls the bank (one wire) and returns
        # UNKNOWN. The reactor records UNKNOWN; the effect is parked.
        results = outbox_reactor.outbox_dispatch_once(tape_server["url"])
        assert any(r["status"] == "unknown" for r in results), results
        assert len(bank.wires) == 1, "the bank was called exactly once on first dispatch"

        # Second outbox tick: the effect is UNKNOWN, so list_effects_to_dispatch
        # does NOT return it. The reactor finds nothing to dispatch.
        results2 = outbox_reactor.outbox_dispatch_once(tape_server["url"])
        assert results2 == []
        assert len(bank.wires) == 1, "no blind retry — bank still has one wire"

        # Reconciler: asks the bank via the connector's observe() — finds the
        # one wire, marks the effect CONFIRMED.
        rec = reconcile_once(tape_server["url"])
        # We may also pick up unrelated PENDING effects in the suite; filter.
        ours = [r for r in rec if "observed" in str(r.get("resolved", ""))]
        assert ours, rec
        # The effect is now CONFIRMED.
        eff = c.list_pending_effects(include_pending=False, include_unknown=True).effects
        assert not any(e.business_key == "bk-headline" for e in eff)
        assert len(bank.wires) == 1, "reconciliation does not re-dispatch"


def test_outbox_reactor_confirmed_path(tape_server):
    """Sanity: the happy path — connector returns confirmed; effect lands
    CONFIRMED in one step, bank has one wire, no UNKNOWN cycle."""
    bank = FakeNonIdempotentBank()
    connectors.clear()
    connectors.register(_BankConnector(bank))
    with TapeClient(tape_server["url"]) as c:
        run = _begin_run(c)
        e = _begin_outbox_effect(c, run.run_id, business_key="bk-happy")
        results = outbox_reactor.outbox_dispatch_once(tape_server["url"])
        ours = [r for r in results if r["idempotency_key"] == e.idempotency_key]
        assert ours and ours[0]["status"] == "confirmed", results
        assert len(bank.wires) == 1
        got = c.get_effect(run_id=run.run_id, idempotency_key=e.idempotency_key).effect
        assert got.status == tape.EFFECT_STATUS_CONFIRMED


def test_outbox_skips_when_connector_missing(tape_server):
    """If no connector is registered for an effect's `connector` name, the
    reactor leaves the row PENDING (a later process with the connector loaded
    will pick it up). No crash, no side effect."""
    connectors.clear()
    with TapeClient(tape_server["url"]) as c:
        run = _begin_run(c)
        e = _begin_outbox_effect(c, run.run_id, business_key="bk-missing",
                                 connector="bank.no-such-connector")
        results = outbox_reactor.outbox_dispatch_once(tape_server["url"])
        ours = [r for r in results if r["idempotency_key"] == e.idempotency_key]
        assert ours and ours[0]["status"] == "skipped", ours
        assert "no connector" in ours[0]["reason"]
        got = c.get_effect(run_id=run.run_id, idempotency_key=e.idempotency_key).effect
        assert got.status == tape.EFFECT_STATUS_PENDING


# ── Pub/Sub subscriber: the loop closes on the wire, not on Pub/Sub ─────────

class _FakePubsubMessage:
    """Minimal stand-in for `pubsub_v1.subscriber.message.Message` — just
    enough surface for `PubSubSubscriber._on_message`."""

    def __init__(self, data: bytes, attributes: dict):
        self.data = data
        self.attributes = attributes
        self.acked = False
        self.nacked = False

    def ack(self) -> None:
        self.acked = True

    def nack(self) -> None:
        self.nacked = True


def test_pubsub_subscriber_reports_confirmed_back_to_tape(tape_server):
    """A subscriber that processes a delivered message must claim the
    dispatch slot, run the handler, and call `complete_effect(CONFIRMED)` —
    closing the loop *on Tape*, not on Pub/Sub. The subscriber acks the
    message regardless because Tape is the source of truth."""
    from tape.connectors.base import DispatchResult
    from tape.connectors.pubsub import PubSubSubscriber

    with TapeClient(tape_server["url"]) as c:
        run = _begin_run(c)
        # Effect ready for the outbox.
        e = _begin_outbox_effect(c, run.run_id, business_key="ps-confirmed")

        handled = []

        def handle(payload, attrs):
            handled.append((payload, attrs))
            return DispatchResult(status="confirmed", external_ref="psr-001",
                                  response={"ok": True})

        sub = PubSubSubscriber(
            project="ignored", subscription="ignored",
            tape_url=tape_server["url"], handle=handle,
            tape_client_factory=lambda: TapeClient(tape_server["url"]))
        msg = _FakePubsubMessage(
            data=b'{"amount": 100}',
            attributes={
                "tape_run_id": run.run_id,
                "tape_effect_key": e.idempotency_key,
                "tape_business_key": "ps-confirmed",
                "tape_connector": "bank.wire",
                "tape_tool": "wire_money",
                "tape_semantics": str(tape.EFFECT_SEMANTICS_NON_IDEMPOTENT),
            })
        sub._on_message(msg)
        sub.stop()

        assert msg.acked is True and msg.nacked is False
        assert len(handled) == 1
        got = c.get_effect(run_id=run.run_id, idempotency_key=e.idempotency_key).effect
        assert got.status == tape.EFFECT_STATUS_CONFIRMED


def test_pubsub_subscriber_unknown_drives_effect_unknown(tape_server):
    """When the handler returns `unknown` (lost ack), the subscriber drives
    the effect to UNKNOWN — same safety contract as the local outbox loop."""
    from tape.connectors.base import DispatchResult
    from tape.connectors.pubsub import PubSubSubscriber

    with TapeClient(tape_server["url"]) as c:
        run = _begin_run(c)
        e = _begin_outbox_effect(c, run.run_id, business_key="ps-unknown")

        def handle(payload, attrs):
            return DispatchResult(status="unknown",
                                  error={"reason": "simulated lost ack"})

        sub = PubSubSubscriber(
            project="ignored", subscription="ignored",
            tape_url=tape_server["url"], handle=handle,
            tape_client_factory=lambda: TapeClient(tape_server["url"]))
        msg = _FakePubsubMessage(
            data=b'{}',
            attributes={
                "tape_run_id": run.run_id,
                "tape_effect_key": e.idempotency_key,
                "tape_business_key": "ps-unknown",
                "tape_connector": "bank.wire",
            })
        sub._on_message(msg)
        sub.stop()

        assert msg.acked is True
        got = c.get_effect(run_id=run.run_id, idempotency_key=e.idempotency_key).effect
        assert got.status == tape.EFFECT_STATUS_UNKNOWN


def test_pubsub_subscriber_dedupes_redelivery(tape_server):
    """If Pub/Sub redelivers a message we already handled, the in-memory LRU
    short-circuits and we ack without re-calling the handler — and the
    dispatch lease CAS on Tape provides a second line of defence (if the LRU
    has rotated the entry out)."""
    from tape.connectors.base import DispatchResult
    from tape.connectors.pubsub import PubSubSubscriber

    with TapeClient(tape_server["url"]) as c:
        run = _begin_run(c)
        e = _begin_outbox_effect(c, run.run_id, business_key="ps-dedupe")
        handler_calls = []

        def handle(payload, attrs):
            handler_calls.append(attrs.get("tape_effect_key"))
            return DispatchResult(status="confirmed", external_ref="psr-dedupe-1")

        sub = PubSubSubscriber(
            project="ignored", subscription="ignored",
            tape_url=tape_server["url"], handle=handle,
            tape_client_factory=lambda: TapeClient(tape_server["url"]))
        attrs = {
            "tape_run_id": run.run_id,
            "tape_effect_key": e.idempotency_key,
            "tape_business_key": "ps-dedupe",
            "tape_connector": "bank.wire",
        }
        m1 = _FakePubsubMessage(b'{}', attrs)
        m2 = _FakePubsubMessage(b'{}', attrs)  # redelivery
        sub._on_message(m1)
        sub._on_message(m2)
        sub.stop()

        assert m1.acked and m2.acked
        assert len(handler_calls) == 1, f"handler should run once; got {handler_calls}"
        got = c.get_effect(run_id=run.run_id, idempotency_key=e.idempotency_key).effect
        assert got.status == tape.EFFECT_STATUS_CONFIRMED


# ── P2: business_key without connector is a clean error, not a flaky race ──

def test_business_key_without_connector_is_refused_by_server(tape_server):
    """The partial UNIQUE index on `(connector, business_key) WHERE
    business_key <> '' AND connector <> ''` only makes sense when both are
    set. The server refuses `business_key` without `connector` with a
    deterministic error (rather than letting two concurrent writers race the
    index and surface a flaky unique-constraint failure on the loser)."""
    with TapeClient(tape_server["url"]) as c:
        run = _begin_run(c)
        with pytest.raises(grpc.RpcError) as ex:
            c.begin_effect(
                run_id=run.run_id, decision_index=0, tool_name="wire",
                request_json="{}",
                business_key="bk-no-connector",   # ← no connector=...
            )
        assert "business_key requires connector" in ex.value.details()


def test_business_key_without_connector_is_refused_at_decoration_time():
    """The @tape.effect / @tape.outbox_tool decorator catches the same
    misconfiguration at decoration time so a misdeclared tool blows up at
    import, not at the first call."""
    with pytest.raises(ValueError) as ex:
        @tape.effect(business_key="bk-only")
        def _no_connector(ctx):
            return {}
    assert "connector" in str(ex.value).lower()


# ── P1: outbox confirmed path registers compensation under the right kind ──

def test_outbox_confirmed_registers_compensation_by_tool(tape_server):
    """When the outbox reactor confirms an effect for a tool that declared a
    compensator, the inverse must be enqueued under the *compensator's* kind
    (not under the tool name). Previously this was a silent no-op:
    `get_compensator(tool_name)` looked the inverse up by the wrong key and
    returned None, so confirmed outbox effects never got a rollback
    obligation. The fix uses the per-tool registry stamped by the
    decorator."""
    from tape.connectors.base import DispatchResult, ObservationResult, CompensationResult

    def reverse_wire(wire_id, **kwargs):
        return {"reversal_id": f"rev-{wire_id}"}

    @tape.outbox_tool(
        connector="bank.outbox-comp",
        business_key=lambda account, amount: f"{account}:{amount}",
        compensate=reverse_wire,
        scope=_DEFAULT_SCOPE,
    )
    def wire_money(account, amount):
        return {"account": account, "amount": amount}

    class _ConfirmingConnector:
        name = "bank.outbox-comp"
        def dispatch(self, effect):
            return DispatchResult(status="confirmed", external_ref="wire-comp-001",
                                  response={"wire_id": "wire-comp-001"})
        def observe(self, effect):
            return ObservationResult(status="confirmed", external_ref="wire-comp-001")
        def compensate(self, obligation):
            return CompensationResult(status="compensated")

    connectors.clear()
    connectors.register(_ConfirmingConnector())

    with TapeClient(tape_server["url"]) as c:
        run = _begin_run(c)
        # Begin the effect via the public surface so the plugin's metadata
        # path is exercised; the test drives the dispatcher directly.
        c.begin_effect(
            run_id=run.run_id, decision_index=0, tool_name="wire_money",
            request_json='{"account":"a","amount":100}',
            semantics=tape.EFFECT_SEMANTICS_NON_IDEMPOTENT,
            dispatch_mode=tape.EFFECT_DISPATCH_MODE_OUTBOX,
            business_key="a:100", connector="bank.outbox-comp",
            scope=_DEFAULT_SCOPE)
        results = outbox_reactor.outbox_dispatch_once(tape_server["url"])
        ours = [r for r in results if r["connector"] == "bank.outbox-comp"]
        assert ours and ours[0]["status"] == "confirmed", results
        obs = c.list_obligations(run_id=run.run_id, only_unresolved=True).obligations
        kinds = [o.kind for o in obs]
        assert "reverse_wire" in kinds, (
            f"outbox-confirmed effect must enqueue a compensation under the "
            f"compensator's name; got obligations with kinds={kinds!r}")
        # And the payload should carry the dispatcher's external_ref so the
        # drainer has what reverse_wire(wire_id=...) needs.
        ob = next(o for o in obs if o.kind == "reverse_wire")
        import json as _json
        assert "wire-comp-001" in ob.payload_json, ob.payload_json


# ── P1: business_key resolver supports the documented positional lambda ─────

def test_business_key_resolver_handles_documented_positional_lambda():
    """The docstring on @tape.outbox_tool shows this exact form:

        business_key=lambda account, amount, date: f"{account}:{amount}:{date}"

    The resolver must call it as `value(**tool_args)` — passing tool_context
    as a kwarg blows up because the lambda doesn't declare it, and the
    `value(tool_args, tool_context)` positional fallback blows up because
    the lambda takes three positional args, not two. Previously both paths
    failed and the resolver silently returned "" — losing the cross-run
    dedupe key. The fix tries the kwargs-only shape too."""
    from tape.effect import _resolve_business_key
    bk_fn = lambda account, amount, date: f"{account}:{amount}:{date}"
    got = _resolve_business_key(bk_fn,
                                {"account": "acct-1", "amount": 100, "date": "2026-05-17"},
                                tool_context=None)
    assert got == "acct-1:100:2026-05-17", got


def test_business_key_resolver_handles_all_documented_shapes():
    """The four shapes the resolver supports — confirm each yields the
    expected key, none silently fail."""
    from tape.effect import _resolve_business_key
    args = {"x": 1, "y": 2}
    ctx = object()
    # static string
    assert _resolve_business_key("static-key", args, ctx) == "static-key"
    # kwargs + ctx
    assert _resolve_business_key(
        lambda x, y, tool_context=None: f"{x}-{y}-ctx", args, ctx) == "1-2-ctx"
    # kwargs only (the documented form)
    assert _resolve_business_key(
        lambda x, y: f"{x}+{y}", args, ctx) == "1+2"
    # (dict, ctx) positional
    assert _resolve_business_key(
        lambda a, c: f"{a['x']}/{a['y']}", args, ctx) == "1/2"
    # (dict,) positional
    assert _resolve_business_key(
        lambda a: str(a.get("x", "?")), args, ctx) == "1"
    # genuinely incompatible signature → empty (and no exception). A
    # keyword-only-required arg that isn't in tool_args + can't be passed
    # positionally falls through every shape.
    def _needs_unknown(*, required_only):  # noqa: E306
        return required_only
    assert _resolve_business_key(_needs_unknown, args, ctx) == ""


def test_pubsub_subscriber_drops_malformed_messages(tape_server):
    """A message missing the tape_effect_key / tape_run_id attributes is
    ack-and-dropped (we own it; Pub/Sub redelivery wouldn't help)."""
    from tape.connectors.base import DispatchResult
    from tape.connectors.pubsub import PubSubSubscriber

    handle_called = []
    sub = PubSubSubscriber(
        project="ignored", subscription="ignored",
        tape_url=tape_server["url"],
        handle=lambda p, a: (handle_called.append(True),
                             DispatchResult(status="confirmed"))[1],
        tape_client_factory=lambda: TapeClient(tape_server["url"]))
    msg = _FakePubsubMessage(b'{}', {})   # no attributes
    sub._on_message(msg)
    sub.stop()
    assert msg.acked is True
    assert not handle_called
