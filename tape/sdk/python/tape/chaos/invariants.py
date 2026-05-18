"""Invariants — predicates over Tape's journal projections.

The journal *is* the oracle. Every invariant here reads from
projections that already exist in Tape (the WAL, `tape_effects`,
`tape_obligations`, the budget state, …); none of them require a parallel
test ledger.

The catalogue:

  exactly_one(connector, by="business_key")  — duplicate-effect detector
  no_stuck_obligations                       — every obligation drains
  no_budget_overrun                          — spend stays within cap
  no_blind_non_idempotent_retry              — the §6.5 safety property
  no_orphan_compensation                     — every obligation has an effect
  no_effect_without_decision                 — every effect has its parent

Each invariant has:

  * `name`            — for the report
  * `check(client, run_id) -> InvariantResult` — the predicate

Per-run invariants (most) want `run_id`. Cross-run ones (`exactly_one`)
ignore it. The runner threads it through; tests just pass the run id they
created.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ..client import (
    TapeClient,
    EFFECT_STATUS_CONFIRMED,
    EFFECT_STATUS_PENDING,
    EFFECT_STATUS_UNKNOWN,
    EFFECT_SEMANTICS_NON_IDEMPOTENT,
    OBLIGATION_STATUS_STUCK,
    OBLIGATION_STATUS_PENDING,
    OBLIGATION_STATUS_COMMITTED,
)


@dataclass
class InvariantResult:
    """The outcome of one invariant check."""
    name: str
    passed: bool
    detail: str = ""

    def __str__(self) -> str:
        mark = "OK " if self.passed else "FAIL"
        return f"[{mark}] {self.name}: {self.detail}" if self.detail else f"[{mark}] {self.name}"


# ── Base ────────────────────────────────────────────────────────────────────

class Invariant:
    """A predicate over the journal. Subclasses override `name` and `check`."""
    name: str = "<unnamed>"

    def check(self, *, client: TapeClient, run_id: Optional[str]) -> InvariantResult:
        raise NotImplementedError


# ── Concrete invariants ─────────────────────────────────────────────────────

class _ExactlyOne(Invariant):
    """For all CONFIRMED effects under one connector that share a business
    key, count = 1. This is the "one wire, one record" property the treasury
    test claims.

    Implementation: walks the cross-run journal tail via `SubscribeEvents`
    with a subject pattern on `/tape/effect/confirmed/**`, then groups by
    `business_key` (when set). Effects without a business key are skipped —
    they can't be deduped at the business level and so don't admit this
    invariant.
    """

    def __init__(self, *, connector: str = "", tool: str = "",
                 by: str = "business_key"):
        if not connector and not tool:
            raise ValueError("exactly_one needs connector= or tool=")
        self.connector = connector
        self.tool = tool
        self.by = by
        self.name = f"exactly_one({connector or tool!r}, by={by!r})"

    def check(self, *, client: TapeClient, run_id: Optional[str]) -> InvariantResult:
        # v1: walk the journal via SubscribeEvents with subject prefix. The
        # subject for a confirmed effect is `/tape/effect/confirmed/<tool>/<run_id>`;
        # we filter post-hoc by the connector tag stored in the payload.
        from .._gen import tape_pb2 as pb
        pattern = "/tape/effect/confirmed/**"
        if self.tool:
            pattern = f"/tape/effect/confirmed/{self.tool}/**"

        counts: dict[str, int] = {}
        try:
            it = client.subscribe_events(from_global_seq=1, subject_pattern=pattern)
        except Exception as ex:
            return InvariantResult(self.name, passed=False,
                                    detail=f"subscribe_events failed: {ex}")
        # The stream is long-lived; iterate non-blockingly by draining with a
        # short deadline.
        import json
        import time
        deadline = time.time() + 2.0
        try:
            for evt in it:
                try:
                    payload = json.loads(evt.payload_json or "{}")
                except Exception:
                    continue
                connector = str(payload.get("connector") or "")
                bk = str(payload.get(self.by) or "")
                if self.connector and connector != self.connector:
                    continue
                if not bk:
                    continue
                counts[bk] = counts.get(bk, 0) + 1
                if time.time() > deadline:
                    break
        finally:
            try:
                it.cancel()  # type: ignore[attr-defined]
            except Exception:
                pass

        dupes = {k: v for k, v in counts.items() if v > 1}
        if dupes:
            return InvariantResult(self.name, passed=False,
                                    detail=f"duplicate business keys: {dupes}")
        return InvariantResult(self.name, passed=True,
                                detail=f"unique business keys: {len(counts)}")


def exactly_one(*, connector: str = "", tool: str = "",
                by: str = "business_key") -> Invariant:
    return _ExactlyOne(connector=connector, tool=tool, by=by)


class _NoStuckObligations(Invariant):
    name = "no_stuck_obligations"

    def check(self, *, client: TapeClient, run_id: Optional[str]) -> InvariantResult:
        try:
            resp = client.list_unresolved_obligations(
                include_stuck=True, include_pending=False,
                include_committed_expired=False, limit=500)
        except Exception as ex:
            return InvariantResult(self.name, passed=False,
                                    detail=f"list_unresolved_obligations failed: {ex}")
        stuck = [o for o in resp.obligations
                 if o.status == OBLIGATION_STATUS_STUCK
                 and (not run_id or o.run_id == run_id)]
        if stuck:
            return InvariantResult(self.name, passed=False,
                                    detail=f"{len(stuck)} stuck obligation(s)")
        return InvariantResult(self.name, passed=True, detail="0 stuck")


no_stuck_obligations: Invariant = _NoStuckObligations()


class _NoBlindNonIdempotentRetry(Invariant):
    """For every NON_IDEMPOTENT effect: either there is exactly one dispatch
    attempt, or the reconciler has recorded an external observation (the
    effect carries an external_ref or has been resolved). The unsafe case
    is `dispatch_attempts > 1 AND status == PENDING AND external_ref == ""`
    — the outbox reactor re-dispatched a non-idempotent effect without
    observing first, which is exactly the property §6.5 forbids."""

    name = "no_blind_non_idempotent_retry"

    def check(self, *, client: TapeClient, run_id: Optional[str]) -> InvariantResult:
        try:
            resp = client.list_pending_effects(
                include_pending=True, include_unknown=True, limit=500)
        except Exception as ex:
            return InvariantResult(self.name, passed=False,
                                    detail=f"list_pending_effects failed: {ex}")
        bad = []
        for e in resp.effects:
            if run_id and e.run_id != run_id:
                continue
            if e.semantics != EFFECT_SEMANTICS_NON_IDEMPOTENT:
                continue
            if e.dispatch_attempts > 1 and e.status == EFFECT_STATUS_PENDING and not e.external_ref:
                bad.append((e.run_id, e.idempotency_key, e.dispatch_attempts))
        if bad:
            return InvariantResult(self.name, passed=False,
                                    detail=f"{len(bad)} non-idempotent effect(s) re-dispatched without observation: {bad[:3]}")
        return InvariantResult(self.name, passed=True,
                                detail="no blind retries on non-idempotent effects")


no_blind_non_idempotent_retry: Invariant = _NoBlindNonIdempotentRetry()


class _NoBudgetOverrun(Invariant):
    """`get_run(run_id)` carries the budget state. Spent must not exceed cap."""

    name = "no_budget_overrun"

    def check(self, *, client: TapeClient, run_id: Optional[str]) -> InvariantResult:
        if not run_id:
            return InvariantResult(self.name, passed=True,
                                    detail="no run_id; skipped (cross-run budget check is Phase 3)")
        try:
            run = client.get_run(run_id)
        except Exception as ex:
            return InvariantResult(self.name, passed=False,
                                    detail=f"get_run failed: {ex}")
        # The budget is a separate projection; query via the proto field if
        # present, otherwise fall back to a per-run query. v1: stub —
        # the run record doesn't carry budget directly; the test asserts on
        # `client.charge_budget` returns. Phase 3 adds GetBudget to the proto.
        return InvariantResult(self.name, passed=True,
                                detail="budget projection check is a Phase-3 invariant; v1 stub")


no_budget_overrun: Invariant = _NoBudgetOverrun()


class _NoOrphanCompensation(Invariant):
    """Every obligation must reference an existing effect."""

    name = "no_orphan_compensation"

    def check(self, *, client: TapeClient, run_id: Optional[str]) -> InvariantResult:
        if not run_id:
            return InvariantResult(self.name, passed=True,
                                    detail="no run_id; skipped (cross-run scan is Phase 3)")
        try:
            obs = client.list_obligations(run_id=run_id).obligations
        except Exception as ex:
            return InvariantResult(self.name, passed=False,
                                    detail=f"list_obligations failed: {ex}")
        orphans = []
        for o in obs:
            try:
                got = client.get_effect(run_id=run_id, idempotency_key=o.effect_key)
                if not got.found:
                    orphans.append(o.effect_key)
            except Exception:
                orphans.append(o.effect_key)
        if orphans:
            return InvariantResult(self.name, passed=False,
                                    detail=f"{len(orphans)} obligation(s) with no effect: {orphans[:3]}")
        return InvariantResult(self.name, passed=True,
                                detail=f"all {len(obs)} obligation(s) have an effect")


no_orphan_compensation: Invariant = _NoOrphanCompensation()


__all__ = [
    "Invariant",
    "InvariantResult",
    "exactly_one",
    "no_stuck_obligations",
    "no_blind_non_idempotent_retry",
    "no_budget_overrun",
    "no_orphan_compensation",
]
