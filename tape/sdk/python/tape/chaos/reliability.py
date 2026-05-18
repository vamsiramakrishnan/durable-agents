"""Reliability Surface — R(k, ε, λ).

The aggregate score for a chaos campaign, in the shape ReliabilityBench
(arXiv 2601.06112) introduced for LLM agents. Recast for durable
runtimes:

  * k — the number of distinct fault scenarios driven (breadth)
  * ε — the invariant-violation rate (correctness — lower is better)
  * λ — the recovery rate (durability — fraction of runs that reached
        a terminal state of TERMINAL despite the faults)

A score of `R(k=20, ε=0.00, λ=0.99)` reads as: 20 scenarios, no
invariant violations, 99% of runs finished. The number is two-axis
*reliability* — not just "did the test pass" but "how broad was the
test, and what fraction recovered."

Pure Python. Accumulates `ChaosReport`s from `chaos.session(...)`
and an optional run-status field, then renders to Markdown.

    rec = chaos.Recorder()
    for scen in derived:
        with chaos.session(scen, url=URL) as sess:
            run_id = drive_agent(sess)
        rec.add(sess.report, terminal=client.get_run(run_id).status == TERMINAL)
    print(rec.surface)
    print(rec.to_markdown())
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass(frozen=True)
class ReliabilitySurface:
    """The two-axis surface: scenarios × outcome."""
    k: int                  # scenarios driven
    epsilon: float          # invariant violation rate (0.0 - 1.0)
    lam: float              # recovery rate / fraction reaching terminal

    def __str__(self) -> str:
        return f"R(k={self.k}, ε={self.epsilon:.2f}, λ={self.lam:.2f})"


@dataclass
class _Row:
    scenario_name: str
    passed: bool
    failed_invariants: List[str] = field(default_factory=list)
    terminal: bool = True
    notes: List[str] = field(default_factory=list)


class Recorder:
    """Accumulator for chaos campaign results. The `surface` property is
    the live R(k, ε, λ) after every `add()`; `to_markdown()` renders the
    full table."""

    def __init__(self) -> None:
        self._rows: List[_Row] = []

    def add(self, report, *, terminal: bool = True) -> None:
        """Record one scenario's outcome. `report` is a `ChaosReport`
        (from `chaos.session(...)`). `terminal=True` means the run
        reached a terminal state despite the faults (recovered).
        `terminal=False` means it didn't (timed out, got stuck)."""
        failed = [ir.name for ir in (report.invariant_results or [])
                  if not ir.passed]
        self._rows.append(_Row(
            scenario_name=report.scenario_name,
            passed=report.passed,
            failed_invariants=failed,
            terminal=terminal,
            notes=list(report.notes or []),
        ))

    @property
    def surface(self) -> ReliabilitySurface:
        k = len(self._rows)
        if k == 0:
            return ReliabilitySurface(k=0, epsilon=0.0, lam=1.0)
        violations = sum(1 for r in self._rows if not r.passed)
        terminal = sum(1 for r in self._rows if r.terminal)
        return ReliabilitySurface(
            k=k, epsilon=violations / k, lam=terminal / k,
        )

    @property
    def rows(self) -> List[_Row]:
        return list(self._rows)

    def to_markdown(self, *, title: str = "TapeChaos campaign") -> str:
        """Render the full table. Stable shape — friendly to checking the
        output into the repo or PR description."""
        s = self.surface
        lines = [
            f"# {title}",
            "",
            f"**Reliability Surface**: `R(k={s.k}, ε={s.epsilon:.2f}, λ={s.lam:.2f})`",
            "",
            f"- {s.k} scenarios",
            f"- {int(s.epsilon * s.k)} invariant violations",
            f"- {int(s.lam * s.k)} runs reached terminal",
            "",
            "| Scenario | Passed | Terminal | Failed invariants |",
            "|---|---|---|---|",
        ]
        for r in self._rows:
            lines.append(
                f"| `{r.scenario_name}` "
                f"| {'OK' if r.passed else 'FAIL'} "
                f"| {'yes' if r.terminal else 'no'} "
                f"| {', '.join(r.failed_invariants) or '—'} |"
            )
        return "\n".join(lines)


def score(reports) -> ReliabilitySurface:
    """Quick-score helper: feed a list of ChaosReports, get the surface."""
    rec = Recorder()
    for r in reports:
        rec.add(r)
    return rec.surface


__all__ = ["ReliabilitySurface", "Recorder", "score"]
