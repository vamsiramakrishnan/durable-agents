"""`@tape.effect` — the one decorator a tool body needs (and even that is optional).

Without it, `TapePlugin` still journals every tool call as an effect with the
default decision-derived idempotency key. You reach for the decorator when you
want to declare:
  * `compensate=` — the inverse to run if a later step fails (LIFO);
  * `status_check=` — how the reconciler resolves an UNKNOWN effect (ask the
    counterparty about the idempotency key);
  * `retry=tape.RetryPolicy(...)` — auto-retry the tool body with backoff (each
    attempt uses the same idempotency key, so the counterparty dedupes if the
    request did land);
  * `key_from=` / `compensation_payload=` — overrides for unusual cases.

Without `retry=`, the decorator only annotates the function (the tool schema ADK
derives from the signature is untouched). With `retry=`, it wraps the function
in a retry loop and copies the signature via `functools.wraps`.
"""

from __future__ import annotations

import asyncio
import functools
import time
from typing import Any, Callable, Optional

from .retry import RetryPolicy

# kind/name -> callable, for the recovery loop to look up by ObligationRecord.kind
_COMPENSATORS: dict[str, Callable] = {}
# tool name -> status-check callable, for the reconciler
_STATUS_CHECKS: dict[str, Callable] = {}


def register_compensator(name: str, fn: Callable) -> None:
    _COMPENSATORS[name] = fn


def get_compensator(name: str) -> Optional[Callable]:
    return _COMPENSATORS.get(name)


def register_status_check(tool_name: str, fn: Callable) -> None:
    _STATUS_CHECKS[tool_name] = fn


def get_status_check(tool_name: str) -> Optional[Callable]:
    return _STATUS_CHECKS.get(tool_name)


def effect(*, compensate: Optional[Callable] = None, status_check: Optional[Callable] = None,
           key_from: Optional[Callable] = None,
           compensation_payload: Optional[Callable] = None,
           retry: Optional[RetryPolicy] = None) -> Callable:
    def deco(fn: Callable) -> Callable:
        meta = {
            "compensate": compensate,
            "status_check": status_check,
            "key_from": key_from,
            "compensation_payload": compensation_payload,
            "retry": retry,
        }
        if compensate is not None:
            register_compensator(getattr(compensate, "__name__", "compensate"), compensate)
        if status_check is not None:
            register_status_check(getattr(fn, "__name__", ""), status_check)
        if retry is None:
            fn._tape_effect = meta  # type: ignore[attr-defined]
            return fn

        # Wrap with a retry loop; each attempt passes the same idempotency key to
        # the counterparty (the tool body uses tape.idempotency_key), so a retry
        # that lands twice at the network level is deduped at the floor.
        if asyncio.iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def wrapper(*args, **kwargs):
                attempt = 0
                while True:
                    attempt += 1
                    try:
                        return await fn(*args, **kwargs)
                    except BaseException as ex:
                        if not retry.should_retry(ex, attempt):
                            raise
                        await asyncio.sleep(retry.delay(attempt))
        else:
            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                attempt = 0
                while True:
                    attempt += 1
                    try:
                        return fn(*args, **kwargs)
                    except BaseException as ex:
                        if not retry.should_retry(ex, attempt):
                            raise
                        time.sleep(retry.delay(attempt))
        wrapper._tape_effect = meta  # type: ignore[attr-defined]
        return wrapper

    return deco


def effect_meta_of(tool: Any) -> Optional[dict]:
    """Pull the `@tape.effect` metadata off an ADK tool, if any."""
    for attr in ("func", "fn", "_func", "__wrapped__"):
        f = getattr(tool, attr, None)
        if f is not None and hasattr(f, "_tape_effect"):
            return f._tape_effect
    if hasattr(tool, "_tape_effect"):
        return tool._tape_effect
    return None


def tool_name_of(tool: Any) -> str:
    for attr in ("name",):
        n = getattr(tool, attr, None)
        if n:
            return n
    f = getattr(tool, "func", None) or getattr(tool, "fn", None)
    if f is not None:
        return getattr(f, "__name__", tool.__class__.__name__)
    return tool.__class__.__name__


def idempotency_key(tool_context: Any) -> str:
    """Inside a tool body: the idempotency key Tape minted for this call.

    Pass it to the counterparty (`bank.wire(..., idempotency_key=key)`) so a
    re-run after a crash is deduped at the floor.
    """
    try:
        return tool_context.state.get("temp:_tape_idempotency_key", "")
    except Exception:
        return ""


def run_id_of(tool_context: Any) -> str:
    try:
        return tool_context.state.get("temp:_tape_run_id", "")
    except Exception:
        return ""
