"""ChaosConnector — wrap any registered connector with declarative faults.

Replaces the env-var soup in
`tape/examples/non_idempotent_bank/connectors.py:23-25` and
`tape/examples/treasury/fake_bank.py:30` with a declarative wrapper. The
real connector still does the work; the wrapper either lets the call
through, drops the ack, returns `duplicate` from observe, or delays —
depending on the faults attached to it.

The wrapper is a `tape.connectors.EffectConnector` itself, so it slots
into the existing registry — there are no special-case code paths in the
outbox reactor.

    from tape import connectors
    from tape.chaos.connectors import ChaosConnector
    from tape.chaos import lose_ack, duplicate

    real = MyBankConnector()
    connectors.register(real)                           # normal
    chaotic = ChaosConnector(inner=real, faults=(
        lose_ack(connector="bank.wire", probability=0.3),
        duplicate(connector="bank.wire", probability=0.05),
    ))
    connectors.register(chaotic)                        # overrides under the same name
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Sequence

from ..connectors.base import (DispatchResult, ObservationResult,
                                CompensationResult)


@dataclass
class ChaosConnector:
    """A `tape.connectors.EffectConnector` that decorates an `inner`
    connector with `faults`. The fault kinds it consumes are:

      * ``lose_ack``  — dispatch returns `unknown` after the inner call
        succeeded. Models a lost ack: the request landed, the response
        didn't. The reconciler will resolve via `observe()`.
      * ``duplicate`` — observe() returns `duplicate`. Models the upstream
        having landed two rows for the same business key.
      * ``delay``     — dispatch sleeps `ms` (± `jitter`) before the inner
        call. Models slow upstreams.

    Probabilities are evaluated against a `random.Random(seed)` so a
    seeded scenario is reproducible. The same instance is reused across
    calls — pass `rng=` from the scenario session so faults across
    connectors share one stream.
    """
    inner: object                       # an EffectConnector
    faults: Sequence = field(default_factory=tuple)
    rng: random.Random = field(default_factory=random.Random)

    @property
    def name(self) -> str:
        return getattr(self.inner, "name", "")

    def _fault(self, kind: str):
        """Pick one matching fault, applying probabilities. Returns the
        Fault object if it fires, else None."""
        for f in self.faults:
            if f.action != kind:
                continue
            if f.probability >= 1.0 or self.rng.random() < f.probability:
                return f
        return None

    def dispatch(self, effect) -> DispatchResult:
        # delay → before the inner call (models slow upstream)
        d = self._fault("delay")
        if d is not None and d.ms > 0:
            jitter_factor = 1.0
            if d.jitter > 0:
                jitter_factor = 1.0 + self.rng.uniform(-d.jitter, d.jitter)
            time.sleep(max(0.0, d.ms / 1000.0 * jitter_factor))

        result = self.inner.dispatch(effect)

        # lose_ack → mutate a `confirmed` into an `unknown`. The inner call
        # already landed; we just hide the ack from Tape so the reconciler
        # has to resolve via observe(). This is the same property
        # `TAPE_BANK_DISPATCH_INJECT_UNKNOWN` modelled, declarative.
        if isinstance(result, DispatchResult) and result.status == "confirmed":
            if self._fault("lose_ack") is not None:
                return DispatchResult(
                    status="unknown",
                    external_ref=result.external_ref,
                    response=result.response,
                    error={"reason": "tape.chaos: simulated lost ack"},
                )
        return result

    def observe(self, effect) -> ObservationResult:
        result = self.inner.observe(effect)
        # `duplicate` → force the upstream's view to say "two copies". The
        # reconciler should respond by registering a compensation.
        if isinstance(result, ObservationResult) and result.status == "confirmed":
            if self._fault("duplicate") is not None:
                return ObservationResult(
                    status="duplicate",
                    external_ref=result.external_ref,
                    response=result.response,
                )
        return result

    def compensate(self, obligation) -> CompensationResult:
        # Compensation faults are not modelled in Phase 1 (the compensation
        # path is well-tested by the test_resume kill-and-resume scenarios).
        # Phase 3 will add a compensate-throws fault for the obligations
        # reactor's bounded-retry behaviour.
        return self.inner.compensate(obligation)


def wrap_connector(inner, faults, *, rng: random.Random = None) -> ChaosConnector:
    """Sugar for the dataclass — useful when the caller wants to register
    the wrapped connector themselves."""
    return ChaosConnector(inner=inner, faults=tuple(faults),
                          rng=rng or random.Random())


__all__ = ["ChaosConnector", "wrap_connector"]
