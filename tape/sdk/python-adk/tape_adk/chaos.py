"""Chaos / fault-injection for the embedded (tape-adk) tier.

Mirrors the connector-layer half of `tape.chaos` against the embedded
`TapeSessionService`. Where the gRPC chaos package targets the Rust
tape-server's failpoints + connector registry, this module targets the
in-process Connector dict the embedded reactor loop dispatches through.

Surface (the single mechanism, applied at three composable layers):

  * `Fault`  — data describing one fault. Same shape as the SDK.
  * `lose_ack(connector=…)`, `duplicate(connector=…)`,
    `delay_connector(connector=…, ms=…)` — fault constructors.
  * `ChaosConnector(inner, faults, rng)` — the actual wrapper: speaks the
    `tape_adk.Connector` protocol, decorates `inner` with `faults`. One
    fault → one decision point.
  * `Invariant` + `no_stuck_obligations`, `exactly_one(connector=…)`,
    `no_blind_non_idempotent_retry` — predicates that read the embedded
    SQL tables directly (no gRPC client).
  * `Scenario` — `(name, faults, invariants, seed, strict_faults)`.
  * `session(scenario, *, db_url, connectors)` — async context manager.
    Yields a wrapped `connectors` dict the caller passes to the reactor
    loop. On exit, runs invariants against the live store and builds a
    `ChaosReport`.

The orchestration is the mechanism: `session(...)` ATOMICALLY validates
that every declared connector-targeted fault has a connector to attach
to (with `strict_faults=True`, the default — same as the SDK), wraps
them, yields, and on exit runs every invariant. No silent-skip
false-positives.

Same logical schema as the SDK chaos package; the wire format is the
embedded SQL store rather than gRPC."""

from __future__ import annotations

import asyncio
import random
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import (Any, AsyncIterator, Callable, Iterable, List, Mapping,
                    Optional, Sequence)

from sqlalchemy import select

from .connectors import (CompensationResult, Connector, DispatchResult,
                         ObservationResult)
from .service import (EffectRecord, EffectSemantics, EffectStatus,
                      ObligationStatus, TapeSessionService)


# ── data: Fault + Scenario (same shape as tape.chaos for portability) ──────


_LAYER_CONNECTOR = "connector"


@dataclass
class Fault:
    """One declarative fault. The embedded module only consumes the
    connector layer; server-layer failpoints are not applicable here
    (there's no separate server)."""
    layer: str = _LAYER_CONNECTOR
    target: str = ""        # connector name when target-scoped
    tool: str = ""          # tool name when tool-scoped
    action: str = ""        # "lose_ack" | "duplicate" | "delay"
    probability: float = 1.0
    ms: int = 0
    jitter: float = 0.0


def lose_ack(*, connector: str = "", tool: str = "",
             probability: float = 0.3) -> Fault:
    """Dispatch returns CONFIRMED → flipped to UNKNOWN. Pass `connector=`
    or `tool=`, not both."""
    if connector and tool:
        raise ValueError("lose_ack: pass connector= or tool=, not both")
    if not (connector or tool):
        raise ValueError("lose_ack requires connector= or tool=")
    return Fault(target=connector, tool=tool, action="lose_ack",
                 probability=probability)


def duplicate(*, connector: str = "", tool: str = "",
              probability: float = 0.05) -> Fault:
    """observe() returns DUPLICATE — the reconciler should register a
    compensation."""
    if connector and tool:
        raise ValueError("duplicate: pass connector= or tool=, not both")
    if not (connector or tool):
        raise ValueError("duplicate requires connector= or tool=")
    return Fault(target=connector, tool=tool, action="duplicate",
                 probability=probability)


def delay_connector(*, connector: str, ms: int, jitter: float = 0.0) -> Fault:
    """Sleep `ms` (± `jitter` as a fraction) before dispatch."""
    return Fault(target=connector, action="delay",
                 probability=1.0, ms=ms, jitter=jitter)


# ── wrapper: ChaosConnector ────────────────────────────────────────────────


@dataclass
class ChaosConnector:
    """A `tape_adk.Connector` that decorates `inner` with `faults`.

    Same semantics as `tape.chaos.ChaosConnector` (the gRPC-tier shape):

    * `lose_ack`  — dispatch's CONFIRMED becomes UNKNOWN. The inner call
                    already landed; the wrapper hides the ack.
    * `duplicate` — observe()'s result becomes DUPLICATE.
    * `delay`     — dispatch sleeps `ms` (± `jitter`) before the inner call.

    A `random.Random(seed)` is the only mutable thread of nondeterminism;
    a seeded scenario is reproducible."""
    inner: Connector
    faults: Sequence[Fault] = field(default_factory=tuple)
    rng: random.Random = field(default_factory=random.Random)

    @property
    def name(self) -> str:
        return getattr(self.inner, "name", "")

    def _matching(self, kind: str, effect: Optional[EffectRecord] = None
                  ) -> Optional[Fault]:
        for f in self.faults:
            if f.action != kind:
                continue
            if f.tool and effect is not None:
                if getattr(effect, "tool_name", "") != f.tool:
                    continue
            if f.probability >= 1.0 or self.rng.random() < f.probability:
                return f
        return None

    async def dispatch(self, effect: EffectRecord) -> DispatchResult:
        # delay → before inner.
        d = self._matching("delay", effect)
        if d is not None and d.ms > 0:
            jitter_factor = 1.0
            if d.jitter > 0:
                jitter_factor = 1.0 + self.rng.uniform(-d.jitter, d.jitter)
            await asyncio.sleep(max(0.0, d.ms / 1000.0 * jitter_factor))

        result = await self.inner.dispatch(effect)

        # lose_ack → CONFIRMED → UNKNOWN (inner already wrote to the upstream).
        if (isinstance(result, DispatchResult)
                and result.status == "confirmed"
                and self._matching("lose_ack", effect) is not None):
            return DispatchResult(
                status="unknown",
                external_ref=result.external_ref,
                response=result.response,
                error={"reason": "tape_adk.chaos: simulated lost ack"},
            )
        return result

    async def observe(self, effect: EffectRecord) -> ObservationResult:
        result = await self.inner.observe(effect)
        if (isinstance(result, ObservationResult)
                and result.status == "confirmed"
                and self._matching("duplicate", effect) is not None):
            return ObservationResult(
                status="duplicate",
                external_ref=result.external_ref,
                response=result.response,
                compensate_kind=getattr(result, "compensate_kind", "") or "",
            )
        return result

    async def compensate(self, obligation) -> CompensationResult:
        # compensate() is the cleanup path; we don't decorate it.
        return await self.inner.compensate(obligation)


# ── invariants: read the embedded tables directly ──────────────────────────


@dataclass
class InvariantResult:
    name: str
    passed: bool
    detail: str = ""

    def __str__(self) -> str:
        mark = "OK " if self.passed else "FAIL"
        return (f"[{mark}] {self.name}: {self.detail}" if self.detail
                else f"[{mark}] {self.name}")


class Invariant:
    """Predicate over the embedded journal. Subclasses override
    `name` and `check`."""
    name: str = "<unnamed>"

    async def check(self, svc: TapeSessionService) -> InvariantResult:
        raise NotImplementedError

    def __call__(self, *args, **kwargs):
        """Calling a parameter-free invariant returns `self`. Lets users
        write either `no_stuck_obligations` or `no_stuck_obligations()`
        — same fix as the gRPC SDK's Invariant base."""
        if args or kwargs:
            raise TypeError(
                f"{type(self).__name__} takes no construction arguments")
        return self


class _NoStuckObligations(Invariant):
    name = "no_stuck_obligations"

    async def check(self, svc: TapeSessionService) -> InvariantResult:
        from .schemas import StorageObligation
        async with svc._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            rows = (await sql.execute(
                select(StorageObligation).where(
                    StorageObligation.status == ObligationStatus.STUCK,
                ))).scalars().all()
        if not rows:
            return InvariantResult(self.name, True, "0 stuck")
        return InvariantResult(
            self.name, False,
            f"{len(rows)} stuck: " + ", ".join(
                f"seq={o.seq} kind={o.kind}" for o in rows[:5]))


no_stuck_obligations: Invariant = _NoStuckObligations()


class _NoBlindNonIdempotentRetry(Invariant):
    """A NON_IDEMPOTENT + OUTBOX effect should never reach
    `dispatch_attempts > 1` while still PENDING — the contract says
    `next_dispatch_at_ms = 0` flips it to UNKNOWN for the reconciler
    instead of a blind retry."""
    name = "no_blind_non_idempotent_retry"

    async def check(self, svc: TapeSessionService) -> InvariantResult:
        from .schemas import StorageEffect
        async with svc._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            rows = (await sql.execute(
                select(StorageEffect).where(
                    StorageEffect.semantics
                    == EffectSemantics.NON_IDEMPOTENT,
                    StorageEffect.status == EffectStatus.PENDING,
                    StorageEffect.dispatch_attempts > 1,
                ))).scalars().all()
        if not rows:
            return InvariantResult(self.name, True, "0 violators")
        return InvariantResult(
            self.name, False,
            f"{len(rows)} NON_IDEMPOTENT effects retried while PENDING")


no_blind_non_idempotent_retry: Invariant = _NoBlindNonIdempotentRetry()


@dataclass
class _ExactlyOne(Invariant):
    """Exactly one CONFIRMED effect matches the filter. Parameterised, so
    used as `exactly_one(connector=…)` or `exactly_one(tool=…)`."""
    connector: str = ""
    tool: str = ""

    def __post_init__(self):
        self.name = ("exactly_one"
                     + (f"(connector={self.connector!r})" if self.connector
                        else f"(tool={self.tool!r})" if self.tool else ""))

    async def check(self, svc: TapeSessionService) -> InvariantResult:
        from .schemas import StorageEffect
        async with svc._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            stmt = select(StorageEffect).where(
                StorageEffect.status == EffectStatus.CONFIRMED)
            if self.connector:
                stmt = stmt.where(StorageEffect.connector == self.connector)
            if self.tool:
                stmt = stmt.where(StorageEffect.tool_name == self.tool)
            rows = (await sql.execute(stmt)).scalars().all()
        n = len(rows)
        if n == 1:
            return InvariantResult(self.name, True, f"1 confirmed")
        return InvariantResult(
            self.name, False, f"{n} confirmed (expected 1)")


def exactly_one(*, connector: str = "", tool: str = "") -> Invariant:
    if connector and tool:
        raise ValueError("exactly_one: pass connector= or tool=, not both")
    if not (connector or tool):
        raise ValueError("exactly_one requires connector= or tool=")
    return _ExactlyOne(connector=connector, tool=tool)


# ── scenario + session ─────────────────────────────────────────────────────


@dataclass
class Scenario:
    """A named bundle of faults + invariants + seed.

    `strict_faults=True` (default): a connector-targeted fault whose
    target isn't in the `connectors` dict FAILS the scenario instead of
    silently passing. Same mechanism as the gRPC SDK — the silent-skip
    false positive is the bug both versions share until this guard fires."""
    name: str
    faults: Sequence[Fault] = field(default_factory=tuple)
    invariants: Sequence[Invariant] = field(default_factory=tuple)
    seed: int = 0
    strict_faults: bool = True


def scenario(*, name: str, faults: Iterable[Fault] = (),
             invariants: Iterable[Invariant] = (), seed: int = 0,
             strict_faults: bool = True) -> Scenario:
    return Scenario(name=name, faults=tuple(faults),
                    invariants=tuple(invariants), seed=int(seed),
                    strict_faults=bool(strict_faults))


@dataclass
class ChaosReport:
    scenario_name: str
    seed: int
    passed: bool = True
    invariant_results: List[InvariantResult] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        head = (f"ChaosReport({self.scenario_name!r}: "
                f"{'pass' if self.passed else 'FAIL'}, seed={self.seed})")
        body = "\n".join(f"  - {ir}" for ir in self.invariant_results)
        notes = "\n".join(f"  ! {n}" for n in self.notes)
        return "\n".join(p for p in (head, body, notes) if p)


@dataclass
class Session:
    """What `chaos.session(...)` yields. `connectors` is the dict the
    caller passes to the reactor loop (wrapped where faults targeted
    them, raw otherwise); `report` is filled in on context exit by the
    invariant checks. Yielding a small object instead of a bare dict
    lets the caller read the report after the `async with` block."""
    connectors: dict[str, Connector]
    report: ChaosReport


@asynccontextmanager
async def session(
    scen: Scenario,
    *,
    connectors: Mapping[str, Connector],
    svc: Optional[TapeSessionService] = None,
    db_url: Optional[str] = None,
) -> AsyncIterator[Session]:
    """Wrap `connectors` with the scenario's faults; yield a `Session`.

    The mechanism:

      1. validate every connector-targeted fault has a target in
         `connectors` — under `strict_faults=True` (default) a missing
         target FAILS the scenario at this point, not silently;
      2. wrap each targeted connector in a `ChaosConnector` with the
         relevant faults bound to a seeded `random.Random`;
      3. yield the wrapped dict + a report shell;
      4. on `__aexit__`, run every invariant against the live store and
         finalize the report.
    """
    rng = random.Random(scen.seed)
    report = ChaosReport(scenario_name=scen.name, seed=scen.seed)
    wrapped: dict[str, Connector] = dict(connectors)

    by_connector: dict[str, list[Fault]] = {}
    tool_scoped: list[Fault] = []
    for f in scen.faults:
        if f.layer != _LAYER_CONNECTOR:
            _skip(report, scen,
                  f"fault layer {f.layer!r} not supported in embedded tier "
                  f"(server failpoints require the gRPC tier)")
            continue
        if f.target:
            by_connector.setdefault(f.target, []).append(f)
        elif f.tool:
            tool_scoped.append(f)
        else:
            _skip(report, scen,
                  "connector fault skipped: neither target nor tool set")

    for name, faults in by_connector.items():
        if name not in connectors:
            _skip(report, scen,
                  f"connector fault for {name!r} skipped: "
                  f"connector not in `connectors` dict")
            continue
        wrapped[name] = ChaosConnector(
            inner=connectors[name],
            faults=tuple(faults + tool_scoped),
            rng=rng,
        )
    if tool_scoped:
        if not connectors:
            _skip(report, scen,
                  "tool-scoped fault(s) skipped: empty `connectors` dict")
        for name, inner in connectors.items():
            if name in by_connector:
                continue
            wrapped[name] = ChaosConnector(
                inner=inner, faults=tuple(tool_scoped), rng=rng)

    if svc is None and db_url is None:
        raise ValueError("session: pass either svc= or db_url=")
    if svc is not None and db_url is not None:
        raise ValueError("session: pass svc= OR db_url=, not both")

    sess = Session(connectors=wrapped, report=report)
    try:
        yield sess
    finally:
        # Reuse the caller's svc when supplied — same engine, same tables
        # (and for SQLite :memory:, same DB). Otherwise spin up a fresh
        # service against db_url.
        check_svc = svc if svc is not None else TapeSessionService(
            db_url=db_url)  # type: ignore[arg-type]
        # Ensure the four embedded tables exist before any invariant
        # tries to query them — a brand-new TapeSessionService creates
        # them lazily on first mutating call, but invariants are reads.
        await check_svc._prepare_tables()  # type: ignore[attr-defined]
        for inv in scen.invariants:
            try:
                ir = await inv.check(check_svc)
            except Exception as ex:  # noqa: BLE001
                ir = InvariantResult(
                    getattr(inv, "name", "<unnamed>"), False,
                    f"raised {type(ex).__name__}: {ex}")
            report.invariant_results.append(ir)
            if not ir.passed:
                report.passed = False
        # If we created the service, dispose of it (best-effort).
        if svc is None:
            try:
                await check_svc.db_engine.dispose()
            except Exception:  # noqa: BLE001
                pass


def _skip(report: ChaosReport, scen: Scenario, message: str) -> None:
    """A declared fault couldn't be applied. Note always; under strict,
    also fail the scenario via a synthetic `strict_faults` invariant
    result. Same mechanism as the SDK fix."""
    report.notes.append(message)
    if scen.strict_faults:
        report.invariant_results.append(InvariantResult(
            name="strict_faults", passed=False, detail=message))
        report.passed = False


async def run(
    scen: Scenario, body,
    *,
    connectors: Mapping[str, Connector],
    svc: Optional[TapeSessionService] = None,
    db_url: Optional[str] = None,
) -> ChaosReport:
    """One-shot convenience: open a `session`, call `body(connectors)`,
    return the report. Pass `svc=` to reuse an existing service or
    `db_url=` to spin one up."""
    async with session(scen, connectors=connectors,
                       svc=svc, db_url=db_url) as sess:
        if body is not None:
            await body(sess.connectors)
    return sess.report


__all__ = [
    # data
    "Fault", "Scenario", "ChaosReport", "InvariantResult", "Session",
    # factories
    "lose_ack", "duplicate", "delay_connector",
    "scenario",
    # invariants
    "Invariant",
    "no_stuck_obligations",
    "no_blind_non_idempotent_retry",
    "exactly_one",
    # wrapper
    "ChaosConnector",
    # orchestration
    "session", "run",
]
