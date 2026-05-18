"""Lineage-driven fault injection (LDFI) — derive the next test from a
successful run.

Molly (Alvaro et al., SoCC '15) had the insight: the next fault you want
to inject is one that the protocol *depends* on. Re-running with that
fault is a higher-information experiment than a random fault, and the set
of "things to break" is *finite* — bounded by the lineage DAG of one
successful trace. No widely-adopted modern descendant exists for general
distributed systems; what made this hard everywhere else is easy on Tape
because **Tape's journal is the lineage**.

This module:

  1. Reads one successful run's journal and builds the lineage DAG.
  2. Enumerates the minimal cuts of the DAG — each cut is "a set of
     journal entries that all had to happen for the run to succeed."
  3. Translates each cut into a `Scenario` — `chaos.crash(failpoint, ...)`
     for the failpoint that *would* prevent that cut.
  4. The runner (in `ldfi.run_all`) drives the agent under each derived
     scenario and checks the invariants. Cuts the system survives are
     "proven safe"; cuts it breaks on are minimal counterexamples.

The catalog of tests is **derived**, not authored.

    baseline = drive_agent_once(...)          # one successful trace
    graph = LineageGraph.from_run(client, baseline.run_id)
    derived = derive_scenarios(graph, invariants=[...])
    report = run_all(derived, runner_fn, url=URL)
    print(report)  # per-cut: pass / fail / which invariant broke
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, List, Optional, Tuple

from ..client import TapeClient, DEFAULT_URL
from .scenarios import Scenario, Fault, crash, scenario


# ── The lineage graph ──────────────────────────────────────────────────────

@dataclass(frozen=True)
class LineageNode:
    """One row of the journal, addressed by (kind, seq) plus its lineage edge.

    `parent_seq` is the seq of the node this depended on (the decision that
    authorized an effect; the effect a compensation inverts). 0 = "this is a
    root" (a run record, or a decision_index 0 / no-decision effect)."""
    seq: int
    kind: str                  # "run" | "decision" | "effect" | "obligation" | "gate" | "value"
    payload: dict              # the parsed payload_json
    parent_seq: int = 0
    # The failpoint that, if injected, prevents this node from being
    # journaled (or its result observable). One per node — the canonical
    # injection point for "what if this step had failed?"
    breaking_failpoint: str = ""


@dataclass
class LineageGraph:
    """The DAG of journal entries for one run, plus the failpoints that
    break each edge."""
    run_id: str
    nodes: List[LineageNode] = field(default_factory=list)

    # ── construction ────────────────────────────────────────────────────────

    @classmethod
    def from_run(cls, client: TapeClient, run_id: str, *,
                 deadline_s: float = 5.0) -> "LineageGraph":
        """Walk the run's journal via SubscribeRun and build the lineage
        DAG. Stops at the first terminal `run` entry."""
        import time

        nodes: list[LineageNode] = []
        terminal_statuses = {"terminal", "failed", "cancelled", "stuck"}
        # decision_index -> seq, so an effect can find its parent decision
        decision_seqs: dict[int, int] = {}
        # idempotency_key -> seq, so an obligation can find its parent effect
        effect_seqs: dict[str, int] = {}
        # gate_name -> seq, so a signal can find its parent gate
        gate_seqs: dict[str, int] = {}

        deadline = time.monotonic() + deadline_s
        it = client.subscribe_run(run_id=run_id, from_seq=0)
        try:
            for entry in it:
                try:
                    payload = json.loads(entry.payload_json or "{}")
                except Exception:
                    payload = {"_raw": entry.payload_json}

                parent = 0
                bp = ""
                if entry.kind == "run":
                    bp = "tape::begin_run::post_db" if payload.get("status") == "running" else "tape::end_run::post_db"
                elif entry.kind == "decision":
                    idx = int(payload.get("decision_index", -1) or -1)
                    decision_seqs[idx] = entry.seq
                    parent = decision_seqs.get(idx - 1, 0)
                    bp = "tape::record_decision::post_db"
                elif entry.kind == "effect":
                    status = (payload.get("status") or "").lower()
                    idx = int(payload.get("decision_index", -1) or -1)
                    parent = decision_seqs.get(idx, 0)
                    key = str(payload.get("idempotency_key") or "")
                    if key:
                        # First time we see this effect (PENDING) → register
                        # its seq so obligations can point back.
                        if status == "pending":
                            effect_seqs.setdefault(key, entry.seq)
                            bp = "tape::begin_effect::post_db"
                        elif status == "confirmed":
                            bp = "tape::complete_effect::post_db"
                        elif status in ("failed", "unknown", "reconciled"):
                            bp = "tape::reconcile_effect::post_db"
                        else:
                            bp = "tape::begin_effect::post_db"
                elif entry.kind == "obligation":
                    parent = effect_seqs.get(str(payload.get("effect_key") or ""), 0)
                    status = (payload.get("status") or "").lower()
                    if status in ("compensated", "stuck"):
                        bp = "tape::resolve_obligation::post_db"
                    else:
                        bp = "tape::register_compensation::post_db"
                elif entry.kind == "gate":
                    gate = str(payload.get("gate") or "")
                    if gate:
                        gate_seqs.setdefault(gate, entry.seq)
                    status = (payload.get("status") or "").lower()
                    if status in ("delivered", "resolved"):
                        bp = "tape::send_signal::post_db"
                    else:
                        bp = "tape::await_signal::post_db"
                elif entry.kind == "value":
                    bp = ("tape::delete_value::post_db"
                          if payload.get("deleted") else "tape::write_value::post_db")

                nodes.append(LineageNode(
                    seq=entry.seq, kind=entry.kind, payload=payload,
                    parent_seq=parent, breaking_failpoint=bp,
                ))

                if entry.kind == "run" and (payload.get("status") or "").lower() in terminal_statuses:
                    break
                if time.monotonic() > deadline:
                    break
        finally:
            try:
                it.cancel()
            except Exception:
                pass

        return cls(run_id=run_id, nodes=nodes)

    # ── queries ─────────────────────────────────────────────────────────────

    def of_kind(self, kind: str) -> List[LineageNode]:
        return [n for n in self.nodes if n.kind == kind]

    def edges(self) -> List[Tuple[int, int]]:
        """Return (parent_seq, child_seq) for every node with a real parent."""
        return [(n.parent_seq, n.seq) for n in self.nodes if n.parent_seq > 0]

    # ── minimal cuts ────────────────────────────────────────────────────────

    def minimal_cuts(self, *, max_size: int = 1) -> List[List[LineageNode]]:
        """Enumerate the minimal cuts of the DAG, up to `max_size`. A
        *singleton* cut is one node; pulling it from the trace would break
        every downstream node. For Tape v1 we ship `max_size=1` only —
        every node that has a non-trivial failpoint is its own minimal
        cut. Multi-node cuts (`max_size>=2`) generate combinatorially many
        scenarios and live behind the same parameter for opt-in fuzzing.

        Returns the cuts in journal-seq order so reports are stable."""
        cuts: list[list[LineageNode]] = []
        candidates = [n for n in self.nodes if n.breaking_failpoint]
        for n in candidates:
            cuts.append([n])
        if max_size >= 2:
            for i, a in enumerate(candidates):
                for b in candidates[i + 1:]:
                    if a.breaking_failpoint == b.breaking_failpoint:
                        continue  # duplicate fault
                    cuts.append([a, b])
        return cuts


# ── Scenario derivation ────────────────────────────────────────────────────

def derive_scenarios(graph: LineageGraph, *,
                     invariants: Iterable[Any] = (),
                     max_cut_size: int = 1,
                     base_name: str = "ldfi") -> List[Scenario]:
    """Translate every minimal cut of `graph` into a `Scenario`. Each
    derived scenario crashes the failpoint that breaks the cut and reuses
    `invariants` as its check list — so a derived scenario's claim is
    "the agent reaches a state where the invariants hold even though
    *this* journal entry never got durable."
    """
    out: list[Scenario] = []
    inv = tuple(invariants)
    for cut in graph.minimal_cuts(max_size=max_cut_size):
        faults: list[Fault] = []
        names: list[str] = []
        for node in cut:
            # `after_n=1` means: skip the first hit (necessary so the
            # baseline run can still be set up), crash on the second.
            # For most failpoints the second hit is the one that creates
            # the durable PENDING/CONFIRMED row.
            faults.append(crash(node.breaking_failpoint, after_n=1, probability=1.0))
            names.append(f"{node.kind}@{node.seq}")
        scen = scenario(
            name=f"{base_name}::cut::{'+'.join(names)}",
            faults=tuple(faults),
            invariants=inv,
        )
        out.append(scen)
    return out


# ── The LDFI loop ──────────────────────────────────────────────────────────

@dataclass
class LDFIReport:
    """Aggregated outcome of running every derived scenario."""
    baseline_run_id: str
    derived_count: int
    survived_count: int = 0
    broken_scenarios: List[Tuple[str, List[Any]]] = field(default_factory=list)

    @property
    def survival_rate(self) -> float:
        if self.derived_count == 0:
            return 1.0
        return self.survived_count / self.derived_count

    def __str__(self) -> str:
        head = (f"LDFIReport(baseline={self.baseline_run_id!r}, "
                f"derived={self.derived_count}, survived={self.survived_count}, "
                f"rate={self.survival_rate:.0%})")
        body = []
        for name, broken in self.broken_scenarios[:10]:
            broken_names = ", ".join(getattr(b, "name", "?") for b in broken)
            body.append(f"  - BROKE on {name}: {broken_names}")
        if len(self.broken_scenarios) > 10:
            body.append(f"  ... and {len(self.broken_scenarios) - 10} more")
        return "\n".join([head] + body)


def run_all(derived: Iterable[Scenario],
            runner: Callable[[Scenario], Any], *,
            baseline_run_id: str = "") -> LDFIReport:
    """Drive `runner(scen)` once per derived scenario and aggregate the
    invariant outcomes. `runner` is what spawns the agent under `scen`'s
    faults — the caller owns it because LDFI is agnostic to whether the
    agent is in-process or behind an HTTP API.

    `runner(scen)` should return an object with an `invariant_results`
    attribute (a list of `InvariantResult`) — `ChaosReport` from
    `chaos.session(...)` already does. A scenario "survived" iff every
    invariant in `result.invariant_results` passed."""
    derived = list(derived)
    report = LDFIReport(baseline_run_id=baseline_run_id, derived_count=len(derived))
    for scen in derived:
        result = runner(scen)
        irs = list(getattr(result, "invariant_results", []) or [])
        if all(getattr(ir, "passed", False) for ir in irs):
            report.survived_count += 1
        else:
            failed = [ir for ir in irs if not getattr(ir, "passed", True)]
            report.broken_scenarios.append((scen.name, failed))
    return report


__all__ = [
    "LineageNode",
    "LineageGraph",
    "derive_scenarios",
    "LDFIReport",
    "run_all",
]
