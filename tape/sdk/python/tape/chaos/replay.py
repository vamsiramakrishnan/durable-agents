"""Replay — the bit-for-bit determinism check.

The DST claim made executable: run a scenario twice with the same seed,
capture the journal both times, canonicalize, and assert equality. A
failing replay is a determinism bug, an adversarial fault, or a missing
journal entry — exactly the bugs that classic chaos testing surfaces
loudest and earliest.

The simplest form::

    @chaos.replayable
    def drive(client, session):
        rid = client.begin_run(app_name="t", user_id="u",
                               session_id="s", invocation_id="inv-1").run_id
        session.set_run_id(rid)
        client.record_decision(run_id=rid, decision_index=0, model="m",
                               request_json="{}", response_json='{"x":1}',
                               rationale="", policy_version="")
        client.end_run(run_id=rid)
        return rid

    report = chaos.replay(scen, drive, url=URL)
    assert report.bit_identical, report

`replay()` wraps the body in two `session(scen)` invocations against the
same server, with the same seed, takes a `Snapshot` of each run's journal,
and compares. The result is a `ReplayReport` — never raises on a
divergence, just reports.

Note on the "same seed" property: TapeChaos seeds the scenario's
`random.Random`, which the connector wraps inherit. For full determinism
the run body itself must consume non-determinism through `tape.now()` /
`tape.uuid()` / `tape.random()` / `tape.sample()` — the same contract the
treatise puts on any tape agent (§6.5). A body that calls `time.time()`
directly will drift between runs and the report will say so.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional

from ..client import TapeClient, DEFAULT_URL
from .scenarios import Scenario, Session, session as _session
from .snapshot import Snapshot, capture


# `replayable` is a marker. The function it decorates must take
# `(client, session)` and return a `run_id` (or set `session.run_id`). The
# decorator records nothing — it's documentation that the body intends to
# be re-run.
def replayable(fn: Callable[[TapeClient, Session], str]) -> Callable[[TapeClient, Session], str]:
    """No-op marker decorator for a replayable body. See module docstring."""
    fn._tape_replayable = True   # type: ignore[attr-defined]
    return fn


@dataclass
class ReplayReport:
    """The result of one `replay(...)` call."""
    scenario_name: str
    seed: int
    bit_identical: bool
    snap_a: Optional[Snapshot] = None
    snap_b: Optional[Snapshot] = None
    diff_summary: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        head = f"ReplayReport({self.scenario_name!r}: {'DETERMINISTIC' if self.bit_identical else 'DRIFTED'}, seed={self.seed})"
        body = ""
        if not self.bit_identical:
            a_len = len(self.snap_a.lines) if self.snap_a else 0
            b_len = len(self.snap_b.lines) if self.snap_b else 0
            body = f"\n  journal lengths: {a_len} vs {b_len}"
            for ln in self.diff_summary[:5]:
                body += f"\n  - {ln}"
            if len(self.diff_summary) > 5:
                body += f"\n  ... and {len(self.diff_summary) - 5} more"
        notes = "\n".join(f"  ! {n}" for n in self.notes)
        return "\n".join(s for s in (head, body, notes) if s)


def _summarize(snap_a: Snapshot, snap_b: Snapshot) -> List[str]:
    """Render the snapshot diff as human-readable strings (head, not whole)."""
    out = []
    for i, op, a, b in snap_a.diff(snap_b):
        if op == "!=":
            out.append(f"[{i}] {a.kind} differs: A={dict(a.payload)} | B={dict(b.payload)}")
        elif op == ">":
            out.append(f"[{i}] only in A: {a.kind} {dict(a.payload)}")
        elif op == "<":
            out.append(f"[{i}] only in B: {b.kind} {dict(b.payload)}")
    return out


def replay(scen: Scenario,
           body: Callable[[TapeClient, Session], str], *,
           url: str = DEFAULT_URL,
           deadline_s: float = 5.0) -> ReplayReport:
    """Run `body` twice under `scen` and check journal bit-identity.

    `body(client, session)` is invoked once per pass. It must produce a run
    (and either return its `run_id` or set `session.run_id`). The second
    invocation gets a freshly-seeded session so its connector wraps replay
    the same fault sequence as the first.

    Note: the scenario's *server-layer* faults aren't applied here — they
    apply to the server process, which this function does not own. Spawn
    a `--features chaos` server with `FAILPOINTS=$(chaos.failpoints_env(scen))`
    if you want server-side chaos. The replay's `bit_identical` claim then
    asserts: server-side chaos itself is reproducible.
    """
    report = ReplayReport(scenario_name=scen.name, seed=scen.seed,
                          bit_identical=False)
    snapshots: list[Snapshot] = []

    for pass_idx in (1, 2):
        with _session(scen, url=url) as sess:
            try:
                returned = body(TapeClient(url), sess)
            except Exception as ex:
                report.notes.append(f"pass {pass_idx} raised: {type(ex).__name__}: {ex}")
                return report
            rid = sess.run_id or (returned if isinstance(returned, str) else None)
            if not rid:
                report.notes.append(
                    f"pass {pass_idx}: body did not produce a run_id "
                    "(set session.run_id or return it)")
                return report
            client = TapeClient(url)
            try:
                snap = capture(client, rid, deadline_s=deadline_s)
            finally:
                client.close()
            snapshots.append(snap)

    a, b = snapshots
    report.snap_a, report.snap_b = a, b
    report.bit_identical = (a == b)
    if not report.bit_identical:
        report.diff_summary = _summarize(a, b)
    return report


__all__ = ["replay", "replayable", "ReplayReport"]
