"""Hypothesis-stateful runner — property-based model checking against Tape's RPCs.

The single-shot tests under `test_chaos_*` cover scenarios we *thought*
of: "concurrent claims, one wins", "lease expires, retry succeeds",
"compensation registers". A stateful runner generates the scenarios we
*didn't* think of — random sequences of operations whose interleaving
turns out to violate an invariant.

[Hypothesis][1]'s `RuleBasedStateMachine` generates such sequences for
us. Each rule wraps one Tape RPC; each invariant checks a property
that must hold *after every step, in every reachable state*. When an
invariant fails, Hypothesis automatically shrinks the failing sequence
to the minimum reproducer, often half a dozen calls long.

Coverage in this file: the obligation lifecycle — the part of Tape's
state machine with the richest invariant surface. The lease primitive
already has a Wing-Gong linearizability check (`tape/server/src/lin.rs`);
this is the higher-level companion that catches state-transition bugs
the linearizability check can't see, like "after `resolve(Compensated)`
the obligation reappears in `list_obligations(only_unresolved=True)`".

[1]: https://hypothesis.readthedocs.io/en/latest/stateful.html

Stateful runner = Jepsen-style harness, single-process. The "real"
Jepsen story (multi-node, network partitions, clock skew) belongs to
the madsim phases (2.5, 2.6, and the future multi-node phase) —
Hypothesis can't simulate networks, but the *invariant catalogue* it
exercises is the same.
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SDK_PY = ROOT / "tape" / "sdk" / "python"
sys.path.insert(0, str(SDK_PY))

try:
    from hypothesis import HealthCheck, Phase, given, settings, strategies as st
    from hypothesis.stateful import (
        Bundle, RuleBasedStateMachine, consumes, invariant, precondition, rule,
    )
except ImportError:  # pragma: no cover
    pytest.skip("hypothesis not installed", allow_module_level=True)

from tape.client import TapeClient
from tape._gen import tape_pb2 as pb


# Effect / obligation status enums we use across rules / invariants.
EFFECT_CONFIRMED = pb.EFFECT_STATUS_CONFIRMED
OB_PENDING = pb.OBLIGATION_STATUS_PENDING
OB_COMMITTED = pb.OBLIGATION_STATUS_COMMITTED
OB_COMPENSATED = pb.OBLIGATION_STATUS_COMPENSATED
OB_STUCK = pb.OBLIGATION_STATUS_STUCK


# ── The state machine ──────────────────────────────────────────────────────


class TapeObligationMachine(RuleBasedStateMachine):
    """Drives a real Tape server through random obligation-lifecycle
    sequences. Class-level fixtures (`url`, `app`) are injected by the
    pytest harness in `TestTapeObligations`.

    Bundle: `obligations` — the set of obligations we've created.
    Each entry is the dict `{run_id, seq, claimed_by, status}` we
    track in-memory as the *model*, alongside what the server says.
    """

    obligations = Bundle("obligations")

    # Injected by the test harness — see `_machine_with_server`.
    url: str
    app: str

    def __init__(self) -> None:
        super().__init__()
        self.client = TapeClient(url=self.url, auth=False)
        self.run_id: str | None = None
        # In-memory model: {seq → {effect_key, status_model, claimer_model}}.
        # `status_model` is what we *think* the server should say.
        self.model: dict[int, dict] = {}

    def teardown(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass

    # ── setup rule — exactly once per case ────────────────────────────────

    @rule()
    @precondition(lambda self: self.run_id is None)
    def setup_run(self) -> None:
        """First rule: stand up the run + an effect to hang obligations off."""
        inv = f"inv-{uuid.uuid4().hex[:10]}"
        r = self.client.begin_run(
            app_name=self.app, user_id="cfo", session_id=f"sess-{uuid.uuid4().hex[:8]}",
            invocation_id=inv, lease_owner="hyp", lease_ttl_ms=60_000)
        self.run_id = r.run_id
        self.client.record_decision(
            run_id=self.run_id, decision_index=0, model="m",
            request_json="{}", response_json="{}", rationale="", policy_version="")
        be = self.client.begin_effect(
            run_id=self.run_id, decision_index=0, tool_name="wire",
            call_index=0, request_json="{}")
        self.effect_key = be.idempotency_key
        self.client.complete_effect(
            run_id=self.run_id, idempotency_key=self.effect_key,
            status=EFFECT_CONFIRMED, response_json="{}")

    # ── rules: each one wraps a Tape RPC ──────────────────────────────────

    @rule(target=obligations,
          kind=st.sampled_from(["reverse_wire", "refund", "cancel_hold"]))
    @precondition(lambda self: self.run_id is not None)
    def register_compensation(self, kind: str) -> dict:
        rec = self.client.register_compensation(
            run_id=self.run_id, effect_key=self.effect_key, kind=kind,
            payload_json="{}", compensator_ref="", max_attempts=3)
        # NOTE: register_compensation is idempotent on (run_id, effect_key,
        # kind). A repeat returns the existing row. The model tracks
        # whichever seq the server returned (which may be the existing one).
        entry = {
            "seq": rec.seq, "kind": kind,
            "status_model": rec.status, "claimer_model": rec.claimed_by,
        }
        self.model[rec.seq] = entry
        return entry

    @rule(ob=obligations,
          claimer=st.sampled_from(["reactor-A", "reactor-B", "reactor-C"]))
    @precondition(lambda self: self.run_id is not None)
    def claim(self, ob: dict, claimer: str) -> None:
        """Try to claim a lease. The server may grant (acquired=True) or
        deny (already-held). The model mirrors the decision."""
        resp = self.client.claim_obligation(
            run_id=self.run_id, obligation_seq=ob["seq"],
            claimer=claimer, lease_ttl_ms=60_000)
        if resp.acquired:
            # The model says: status → COMMITTED, claimer → us.
            self.model[ob["seq"]]["status_model"] = OB_COMMITTED
            self.model[ob["seq"]]["claimer_model"] = claimer
        # else: lease was held by someone else; model is unchanged.

    @rule(ob=consumes(obligations),
          terminal=st.sampled_from([OB_COMPENSATED, OB_STUCK]))
    @precondition(lambda self: self.run_id is not None)
    def resolve(self, ob: dict, terminal: int) -> None:
        """Resolve an obligation terminally. After this, it must not
        appear in `list_obligations(only_unresolved=True)`."""
        self.client.resolve_obligation(
            run_id=self.run_id, obligation_seq=ob["seq"],
            status=terminal, result_json="{}")
        self.model[ob["seq"]]["status_model"] = terminal
        self.model[ob["seq"]]["claimer_model"] = ""

    # ── invariants: hold AT ALL REACHABLE STATES, after every rule ────────

    @invariant()
    def model_matches_server(self) -> None:
        """For every obligation in our model, the server's view of its
        status agrees with ours. Catches "server lost the update" bugs."""
        if self.run_id is None:
            return
        rows = self.client.list_obligations(
            run_id=self.run_id, only_unresolved=False, status_filter=0).obligations
        by_seq = {o.seq: o for o in rows}
        for seq, m in self.model.items():
            assert seq in by_seq, f"obligation seq={seq} missing from server"
            srv = by_seq[seq]
            assert srv.status == m["status_model"], \
                f"status mismatch on seq={seq}: server={srv.status} model={m['status_model']}"
            # claimer_model is what the *latest accepted* claim set; if the
            # server cleared it (e.g. on resolve), our model should match.
            if m["status_model"] in (OB_COMPENSATED, OB_STUCK):
                assert srv.claimed_by == "", \
                    f"resolved obligation seq={seq} still has claimer={srv.claimed_by!r}"

    @invariant()
    def unresolved_listing_excludes_terminal(self) -> None:
        """`only_unresolved=True` must hide every COMPENSATED / STUCK row.
        A test like this catches the "stale view" / missing index bug."""
        if self.run_id is None:
            return
        unresolved = self.client.list_obligations(
            run_id=self.run_id, only_unresolved=True, status_filter=0).obligations
        for o in unresolved:
            assert o.status not in (OB_COMPENSATED, OB_STUCK), \
                f"terminal obligation seq={o.seq} status={o.status} leaked into only_unresolved listing"

    @invariant()
    def at_most_one_live_claimer_per_obligation(self) -> None:
        """The lease invariant from `lin.rs`, re-stated at the API level:
        the server's row for a COMMITTED obligation has exactly one
        `claimed_by`, never empty, never two."""
        if self.run_id is None:
            return
        rows = self.client.list_obligations(
            run_id=self.run_id, only_unresolved=False, status_filter=0).obligations
        for o in rows:
            if o.status == OB_COMMITTED:
                assert o.claimed_by, \
                    f"COMMITTED obligation seq={o.seq} has empty claimed_by"


# ── pytest integration ─────────────────────────────────────────────────────


@pytest.mark.usefixtures("tape_server")
class TestTapeObligations:
    """The pytest harness binds the `tape_server` fixture to the
    state-machine class, then runs `TapeObligationMachine.TestCase` —
    Hypothesis-generated test class that drives random sequences."""

    @pytest.fixture(autouse=True)
    def _bind_server(self, tape_server) -> None:
        TapeObligationMachine.url = tape_server["url"]
        TapeObligationMachine.app = f"hyp-{uuid.uuid4().hex[:6]}"

    def test_obligation_lifecycle_invariants_hold(self) -> None:
        # `run_state_machine_as_test` runs the machine with custom
        # settings without us having to subclass `.TestCase`. Cap the
        # examples so the suite stays under a minute; bump
        # `TAPE_HYP_EXAMPLES` locally when hunting bugs.
        from hypothesis.stateful import run_state_machine_as_test

        s = settings(
            max_examples=int(os.environ.get("TAPE_HYP_EXAMPLES", "20")),
            # We mutate external state (the server); deadlines / shrink
            # strategies that re-run rules don't make sense.
            deadline=None,
            suppress_health_check=[
                HealthCheck.too_slow,
                HealthCheck.function_scoped_fixture,
            ],
            phases=[Phase.generate, Phase.shrink],
        )
        run_state_machine_as_test(TapeObligationMachine, settings=s)
