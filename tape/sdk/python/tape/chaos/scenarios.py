"""Scenarios — the declarative bundle of (faults, invariants, seed).

A `Fault` is data. It targets one of two layers:

  * **server**  — a named failpoint from the catalogue in
    `tape/server/src/chaos.rs` (e.g. `tape::begin_effect::post_db`). The
    SDK renders it into the `FAILPOINTS` env-var spec that the server
    parses at startup (and, in Phase 2, a `ChaosService` RPC that flips it
    at runtime).
  * **connector** — a wrap around a registered `tape.connectors`
    connector. Replaces the env-var hacks in
    `examples/non_idempotent_bank/connectors.py:23` and
    `examples/treasury/fake_bank.py:30` with a declarative wrapper.

A `Scenario` bundles faults + invariants + a seed. `session(scen)` is a
context manager that:

  1. computes the `FAILPOINTS` spec from the scenario (returned by
     :func:`failpoints_env` for the caller to pass to a subprocess
     `tape-server`);
  2. wraps every registered connector named in a connector fault;
  3. yields a `Session` the caller drives;
  4. checks each invariant against the live journal on `__exit__`,
     stashing the result in `session.report`.

Invariants raise nothing; they go into the report. A failed scenario is a
report with `passed=False` — not an exception.
"""

from __future__ import annotations

import os
import random
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, List, Optional, Sequence

from ..client import TapeClient, DEFAULT_URL


# ── Fault types ─────────────────────────────────────────────────────────────

# Two layers — see module docstring. The string discriminator keeps the type
# JSON-serializable so a scenario can travel over the wire to a remote runner.
_LAYER_SERVER = "server"
_LAYER_CONNECTOR = "connector"


@dataclass(frozen=True)
class Fault:
    """One declared fault. A scenario is `Sequence[Fault]`.

    For server-layer faults: `target` is a failpoint name from
    `tape/server/src/chaos.rs` (e.g. `tape::begin_effect::post_db`);
    `action` is one of the `fail` crate's actions
    (`panic`, `return(msg)`, `sleep(ms)`, `pause`, `yield`, `print(msg)`,
    `off`). `probability` becomes the `fail`-crate probability prefix.

    For connector-layer faults: `target` is the connector's `.name` (e.g.
    `bank.wire`); `kind` is `lose_ack` / `duplicate` / `delay`. The runner
    looks up the connector via `tape.connectors.get(name)` and wraps it
    with a `ChaosConnector` carrying these flags.

    Tool-scoped connector faults (`lose_ack(tool=...)`, `duplicate(tool=...)`)
    set `target=""` and `tool=<tool_name>`. The runner attaches them to
    every registered connector; `ChaosConnector` then filters by
    `effect.tool_name` at dispatch time. (This is what the v1 docstrings
    on `lose_ack` / `duplicate` promised; the wiring landed in the P1
    review-fix commit.)
    """
    layer: str          # "server" | "connector"
    target: str         # failpoint name OR connector name (empty = fan-out for tool-scoped)
    action: str = ""    # fail-crate action (server) or fault kind (connector)
    probability: float = 1.0
    after_n: int = 0    # fire only after this many hits (server only)
    # Connector-only fields
    ms: int = 0         # delay length
    jitter: float = 0.0
    # Tool selector for connector-layer faults. When non-empty, the fault
    # only fires when `effect.tool_name == tool`.
    tool: str = ""
    # Free-form selector (`when="tool == 'execute_sweep'"`) — Phase 2 will
    # bind this to a CEL evaluator on the server; v1 records it for the
    # report so an operator can see what the fault was scoped to. For tool-
    # scoping, prefer `tool=` (above) — it's wired to the dispatch filter.
    when: str = ""


# ── Server-failpoint constructors ───────────────────────────────────────────

def crash(failpoint: str, *, probability: float = 1.0, after_n: int = 0,
          when: str = "") -> Fault:
    """A server failpoint configured to `panic` — the headline crash fault.
    `failpoint` is a name from the catalogue (`tape::begin_effect::post_db`,
    etc.); `probability` is the per-hit firing chance (0.0 - 1.0);
    `after_n` is the number of hits to skip before firing (`fail` crate's
    `count(n)` prefix). `when` is a free-form selector recorded for the
    report (Phase 2 binds it to CEL on the server)."""
    return Fault(layer=_LAYER_SERVER, target=failpoint, action="panic",
                 probability=probability, after_n=after_n, when=when)


def delay(failpoint: str, *, ms: int, probability: float = 1.0,
          jitter: float = 0.0, when: str = "") -> Fault:
    """A server failpoint configured to `sleep(ms)`. Use to model slow
    upstreams, slow stores, lock contention."""
    return Fault(layer=_LAYER_SERVER, target=failpoint, action="sleep",
                 probability=probability, ms=ms, jitter=jitter, when=when)


def error(failpoint: str, *, msg: str = "chaos", probability: float = 1.0,
          when: str = "") -> Fault:
    """A server failpoint configured to `return(msg)` — the RPC returns
    `Internal: chaos: <failpoint> <msg>` to the caller."""
    return Fault(layer=_LAYER_SERVER, target=failpoint, action="return",
                 probability=probability, ms=0, when=when,
                 # smuggle the message via `when` would collide; use action
                 # spelling like `return(msg)` directly below in to_spec.
                 )._with(_action_msg=msg)  # type: ignore[attr-defined]


# Helper that lets `error()` carry the message without growing the Fault
# struct further. Implemented as a method `_with` patched in below to keep
# Fault frozen+slots-friendly.
def _fault_with(self: Fault, **kw: Any) -> Fault:  # noqa: D401
    """Return a new Fault with extra keys stashed in a private attribute."""
    new = Fault(
        layer=self.layer, target=self.target, action=self.action,
        probability=self.probability, after_n=self.after_n, ms=self.ms,
        jitter=self.jitter, when=self.when,
    )
    object.__setattr__(new, "_extra", {**getattr(self, "_extra", {}), **kw})
    return new


Fault._with = _fault_with  # type: ignore[attr-defined]


# ── Connector-fault constructors ────────────────────────────────────────────

def lose_ack(*, connector: str = "", tool: str = "",
             probability: float = 0.3) -> Fault:
    """The connector calls the upstream, but the ack is lost — Tape records
    the effect as `UNKNOWN` and the reconciler resolves via `observe()`.
    Pass `connector=` (the routing key) **or** `tool=` (the tool name) —
    not both. Connector-scoped faults are attached to that one connector;
    tool-scoped faults attach to every registered connector and only fire
    when `effect.tool_name == tool`."""
    if connector and tool:
        raise ValueError("lose_ack: pass connector= or tool=, not both")
    if not (connector or tool):
        raise ValueError("lose_ack requires connector= or tool=")
    return Fault(layer=_LAYER_CONNECTOR, target=connector, tool=tool,
                 action="lose_ack", probability=probability)


def duplicate(*, connector: str = "", tool: str = "",
              probability: float = 0.05) -> Fault:
    """The connector returns `duplicate` from `observe()` — modelling the
    case where the upstream landed two copies of the same business key.
    The reconciler should register a compensation.

    Same `connector=` / `tool=` semantics as [`lose_ack`][]."""
    if connector and tool:
        raise ValueError("duplicate: pass connector= or tool=, not both")
    if not (connector or tool):
        raise ValueError("duplicate requires connector= or tool=")
    return Fault(layer=_LAYER_CONNECTOR, target=connector, tool=tool,
                 action="duplicate", probability=probability)


def delay_connector(*, connector: str, ms: int, jitter: float = 0.0) -> Fault:
    """Delay the connector's `dispatch()` by `ms` (with optional jitter,
    as a fraction of `ms`). Use to model slow upstreams and probe timeout
    handling."""
    return Fault(layer=_LAYER_CONNECTOR, target=connector, action="delay",
                 probability=1.0, ms=ms, jitter=jitter)


# ── Scenario ────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Scenario:
    """A named bundle of faults + invariants + seed."""
    name: str
    faults: Sequence[Fault] = field(default_factory=tuple)
    invariants: Sequence[Any] = field(default_factory=tuple)   # `Invariant`s
    seed: int = 0


def scenario(*, name: str, faults: Iterable[Fault] = (),
             invariants: Iterable[Any] = (), seed: int = 0) -> Scenario:
    """Sugar for the `Scenario` constructor."""
    return Scenario(name=name, faults=tuple(faults),
                    invariants=tuple(invariants), seed=int(seed))


# ── FAILPOINTS env rendering ────────────────────────────────────────────────

def _to_fail_spec(f: Fault) -> str:
    """Render one server-layer Fault to the `fail` crate's spec string.

    Examples:
      crash("tape::begin_effect::post_db") -> "tape::begin_effect::post_db=panic"
      crash(..., probability=0.5)          -> "tape::begin_effect::post_db=0.5*panic"
      crash(..., after_n=2)                -> "tape::begin_effect::post_db=2*off->panic"
      delay(..., ms=500)                   -> "tape::xxx=sleep(500)"
      error(..., msg="db")                 -> "tape::xxx=return(db)"
    """
    if f.layer != _LAYER_SERVER:
        raise ValueError(f"_to_fail_spec only handles server faults: {f!r}")
    action = f.action
    if action == "sleep":
        action = f"sleep({int(f.ms)})"
    elif action == "return":
        msg = (getattr(f, "_extra", {}) or {}).get("_action_msg", "chaos")
        action = f"return({msg})"
    elif action == "print":
        msg = (getattr(f, "_extra", {}) or {}).get("_action_msg", "chaos")
        action = f"print({msg})"

    p = f.probability
    parts = []
    if f.after_n > 0:
        # `n*off->...` is the fail crate's "skip the first n then fire"
        # idiom — see the `fail` crate's parse_actions().
        parts.append(f"{f.after_n}*off")
    if 0.0 < p < 1.0:
        parts.append(f"{p:g}*{action}")
    else:
        parts.append(action)
    return f"{f.target}={'->'.join(parts)}"


def failpoints_env(scen: Scenario) -> str:
    """Render the server-layer faults in `scen` into the `FAILPOINTS` env-var
    value the `tape-server --features chaos` binary parses at startup. Pass
    this to your subprocess `env={"FAILPOINTS": failpoints_env(scen), …}`
    when spawning the server, or `export FAILPOINTS=…` for a manual run.

    Connector-layer faults are NOT in this string — they're applied to the
    connector registry at `session(...)` entry."""
    specs = [_to_fail_spec(f) for f in scen.faults if f.layer == _LAYER_SERVER]
    return ";".join(specs)


# ── Session — context manager that applies + checks ─────────────────────────

@dataclass
class ChaosReport:
    """The result of one scenario run."""
    scenario_name: str
    seed: int
    failpoints_spec: str
    passed: bool = True
    invariant_results: List[Any] = field(default_factory=list)  # InvariantResult
    notes: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        head = f"ChaosReport({self.scenario_name!r}: {'pass' if self.passed else 'FAIL'}, seed={self.seed})"
        body = "\n".join([f"  - {ir}" for ir in self.invariant_results])
        notes = "\n".join([f"  ! {n}" for n in self.notes])
        return "\n".join(s for s in (head, body, notes) if s)


class Session:
    """Context manager: applies connector wraps on enter, checks invariants on
    exit. The server-side `FAILPOINTS` env is exposed via `.failpoints_spec`
    so a caller spawning the server controls how to pass it (subprocess
    env, k8s secret, etc.)."""

    def __init__(self, scen: Scenario, *, url: str = DEFAULT_URL,
                 run_id: Optional[str] = None):
        self.scenario = scen
        self.url = url
        self.run_id = run_id
        self.failpoints_spec = failpoints_env(scen)
        self.report = ChaosReport(
            scenario_name=scen.name, seed=scen.seed,
            failpoints_spec=self.failpoints_spec,
        )
        # Seed Python's RNG so the connector wraps inherit determinism.
        # Connectors use `self._rng` (passed in via __init__).
        self._rng = random.Random(scen.seed) if scen.seed else random.Random()
        self._connector_unpatches: List[Callable[[], None]] = []

    def set_run_id(self, run_id: str) -> "Session":
        """Tell the invariants which run to check. Per-run invariants need
        this; cross-run ones don't. Returns self for chaining."""
        self.run_id = run_id
        return self

    # ── connector wraps ────────────────────────────────────────────────────
    def _apply_connector_faults(self) -> None:
        from .. import connectors as _connectors
        from .connectors import ChaosConnector

        # Two flavours of connector-layer fault:
        #   * `target=<connector_name>` — attach to that one connector;
        #   * `target=""`, `tool=<tool>` — attach to every connector, and
        #     let ChaosConnector filter by `effect.tool_name` at dispatch.
        # The fan-out is what makes tool-scoped chaos work without the
        # user having to know which connector handles which tool.
        by_connector: dict[str, List[Fault]] = {}
        tool_scoped: List[Fault] = []
        for f in self.scenario.faults:
            if f.layer != _LAYER_CONNECTOR:
                continue
            if f.target:
                by_connector.setdefault(f.target, []).append(f)
            elif f.tool:
                tool_scoped.append(f)
            else:
                self.report.notes.append(
                    "connector fault skipped: neither target nor tool set")

        # Connector-targeted: each fault attaches to exactly that one
        # connector (or skips with a recorded note if it's not registered).
        for name, faults in by_connector.items():
            real = _connectors.get(name)
            if real is None:
                self.report.notes.append(
                    f"connector fault for {name!r} skipped: connector not registered")
                continue
            self._wrap_connector(name, real, list(faults) + tool_scoped)

        # Tool-targeted: fan out to every registered connector that didn't
        # already get its own bundle (those already received `tool_scoped`
        # in the loop above). Avoids double-wrapping the same connector.
        if tool_scoped:
            already_wrapped = set(by_connector.keys())
            for name, real in _connectors.all_registered().items():
                if name in already_wrapped:
                    continue
                self._wrap_connector(name, real, tool_scoped)

    def _wrap_connector(self, name: str, real, faults: List[Fault]) -> None:
        from .. import connectors as _connectors
        from .connectors import ChaosConnector
        wrapped = ChaosConnector(inner=real, faults=tuple(faults),
                                  rng=self._rng)
        _connectors.register(wrapped)
        def _restore(name=name, original=real):
            _connectors.register(original)
        self._connector_unpatches.append(_restore)

    def _check_invariants(self) -> None:
        client = TapeClient(self.url)
        try:
            for inv in self.scenario.invariants:
                try:
                    result = inv.check(client=client, run_id=self.run_id)
                except Exception as ex:
                    from .invariants import InvariantResult
                    result = InvariantResult(name=getattr(inv, "name", "?"),
                                              passed=False,
                                              detail=f"check raised: {type(ex).__name__}: {ex}")
                self.report.invariant_results.append(result)
                if not result.passed:
                    self.report.passed = False
        finally:
            client.close()

    def __enter__(self) -> "Session":
        self._apply_connector_faults()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        for fn in reversed(self._connector_unpatches):
            try:
                fn()
            except Exception:
                pass
        # If the body raised, that's the scenario "failing" — record it but
        # don't swallow the exception (the caller decides). Invariants
        # still get checked over whatever the journal has.
        if exc_type is not None:
            self.report.passed = False
            self.report.notes.append(f"body raised: {exc_type.__name__}: {exc}")
        self._check_invariants()


def session(scen: Scenario, *, url: str = DEFAULT_URL,
            run_id: Optional[str] = None) -> Session:
    """Open a chaos session for `scen`. Use as a context manager — see the
    module docstring."""
    return Session(scen, url=url, run_id=run_id)


def run_scenario(scen: Scenario, body: Callable[["Session"], Any], *,
                 url: str = DEFAULT_URL,
                 run_id: Optional[str] = None) -> ChaosReport:
    """Run `body(session)` under `scen` and return the report. `body` may
    set `session.run_id` once it knows which run it created."""
    with session(scen, url=url, run_id=run_id) as sess:
        body(sess)
    return sess.report


__all__ = [
    "Fault",
    "Scenario",
    "ChaosReport",
    "Session",
    "crash",
    "delay",
    "error",
    "lose_ack",
    "duplicate",
    "delay_connector",
    "scenario",
    "session",
    "run_scenario",
    "failpoints_env",
]
