"""Retry policies for `@tape.effect` — Temporal's most-used activity feature, for Tape.

`RetryPolicy(...)` describes how to retry a failing tool body: max attempts,
exponential backoff with jitter, which exceptions to retry on, which to give
up on. The same `idempotency_key` is passed to the counterparty on every
attempt, so a retry that lands "twice" at the network level is deduped at the
floor — exactly the property Tape's effect ledger already gives you, applied
across retries within one call instead of across re-drives across crashes.
"""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    initial_interval_s: float = 1.0
    backoff_coefficient: float = 2.0
    max_interval_s: float = 60.0
    jitter: float = 0.1                   # +/- fraction of the computed delay
    retry_on: tuple = (Exception,)
    non_retryable: tuple = ()

    def should_retry(self, exc: BaseException, attempt: int) -> bool:
        """`attempt` is 1-based: 1 = first try, 2 = first retry, ..."""
        if attempt >= self.max_attempts:
            return False
        if self.non_retryable and isinstance(exc, self.non_retryable):
            return False
        return isinstance(exc, self.retry_on)

    def delay(self, attempt: int) -> float:
        d = min(self.initial_interval_s * (self.backoff_coefficient ** max(0, attempt - 1)),
                self.max_interval_s)
        if self.jitter > 0:
            d *= 1.0 + random.uniform(-self.jitter, self.jitter)
        return max(d, 0.0)
