"""`@tape.outbox_tool` — the high-level decorator for *non-idempotent* upstreams.

The tool body builds a JSON-serializable *intent* payload and returns it; it
must NOT perform external IO. Tape journals the intent, the **outbox reactor**
dispatches it through the named connector, and (when `wait_for_result=True`) the
ADK run is parked until the dispatch is resolved.

Rules — enforced at decoration time, not at runtime:

  * `semantics="non_idempotent"` MUST supply at least one of
    `business_key`, `status_check`, `compensate`, or `human_gate=True`.
    Otherwise an UNKNOWN dispatch could be blindly retried.
  * `business_key` is preferred for naturally-keyable upstreams (a wire that
    encodes `(account, amount, date)`).
  * `status_check` is preferred for systems that expose a query-by-id after the
    fact.
  * `compensate` is preferred for systems where duplicates are detected only
    post-hoc and reversed.
  * `human_gate=True` parks the run on a gate before dispatch — used when there
    is genuinely no programmatic recovery.

This decorator is *complementary* to `@tape.effect`. Effects encode synchronous
tool bodies that perform their own IO. Outbox tools encode *intents* — the body
returns the dispatch request, and the reactor owns delivery, retries, and
result correlation.

Example::

    @tape.outbox_tool(
        connector="bank.wire",
        semantics="non_idempotent",
        business_key=lambda account, amount, date, **_: f"{account}:{amount}:{date}",
        status_check=find_wire,
        compensate=reverse_wire,
        wait_for_result=True,
    )
    def wire_money(account: str, amount: int, beneficiary: str, date: str):
        return {
            "account": account,
            "amount": amount,
            "beneficiary": beneficiary,
            "date": date,
        }
"""

from __future__ import annotations

import asyncio
import functools
import inspect
from typing import Any, Callable, Literal, Optional

from .effect import register_compensator, register_status_check

OutboxSemantics = Literal["idempotent", "non_idempotent", "at_least_once"]


class OutboxConfigError(ValueError):
    """A misconfigured `@tape.outbox_tool` decorator — surfaced at decoration
    time so a bad config can't ship to production."""


def _has_io_signature(fn: Callable) -> bool:
    """Heuristic: the body should not be async (intents are pure data)."""
    return asyncio.iscoroutinefunction(fn)


def outbox_tool(
    *,
    connector: str,
    semantics: OutboxSemantics = "idempotent",
    business_key: Optional[Callable[..., str]] = None,
    status_check: Optional[Callable[..., Any]] = None,
    compensate: Optional[Callable[..., Any]] = None,
    wait_for_result: bool = True,
    human_gate: bool = False,
    retry_policy: Optional[Any] = None,
    dispatch_timeout_ms: int = 60_000,
    max_attempts: int = 0,
    description: Optional[str] = None,
) -> Callable:
    """Mark a tool as *outbox-dispatched*.

    The decorated function must return a JSON-serializable intent payload. The
    body must not perform external IO — the connector does that.
    """
    if semantics not in ("idempotent", "non_idempotent", "at_least_once"):
        raise OutboxConfigError(f"unknown semantics {semantics!r}")
    if semantics == "non_idempotent":
        if not any((business_key, status_check, compensate, human_gate)):
            raise OutboxConfigError(
                "non_idempotent outbox tools must declare at least one of "
                "business_key, status_check, compensate, or human_gate=True — "
                "otherwise an UNKNOWN dispatch could be blindly retried."
            )

    def deco(fn: Callable) -> Callable:
        if _has_io_signature(fn):
            raise OutboxConfigError(
                f"outbox tool {fn.__name__} is async — outbox bodies must be "
                "synchronous pure functions that return an intent payload."
            )

        meta = {
            "connector": connector,
            "semantics": semantics,
            "business_key": business_key,
            "status_check": status_check,
            "compensate": compensate,
            "wait_for_result": wait_for_result,
            "human_gate": human_gate,
            "retry_policy": retry_policy,
            "dispatch_timeout_ms": int(dispatch_timeout_ms),
            "max_attempts": int(max_attempts),
        }

        if compensate is not None:
            register_compensator(getattr(compensate, "__name__", "compensate"), compensate)
        if status_check is not None:
            register_status_check(fn.__name__, status_check)

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            payload = fn(*args, **kwargs)
            if not isinstance(payload, (dict, list, str, int, float, bool, type(None))):
                raise OutboxConfigError(
                    f"outbox tool {fn.__name__} returned {type(payload).__name__}; "
                    "must return a JSON-serializable intent payload."
                )
            envelope: dict[str, Any] = {
                "__outbox__": True,
                "connector": connector,
                "semantics": semantics,
                "wait_for_result": wait_for_result,
                "human_gate": human_gate,
                "dispatch_timeout_ms": meta["dispatch_timeout_ms"],
                "payload": payload,
            }
            if business_key is not None:
                try:
                    bound = inspect.signature(fn).bind_partial(*args, **kwargs)
                    bound.apply_defaults()
                    envelope["business_key"] = str(business_key(**bound.arguments))
                except Exception as ex:  # pragma: no cover — surfaced as runtime error
                    raise OutboxConfigError(
                        f"business_key for {fn.__name__} raised: {ex}"
                    ) from ex
            return envelope

        wrapper._tape_outbox = meta  # type: ignore[attr-defined]
        wrapper._tape_effect = {
            "compensate": compensate,
            "status_check": status_check,
            "key_from": None,
            "compensation_payload": None,
            "retry": retry_policy,
            "max_attempts": meta["max_attempts"],
            "compensator_ref": "",
        }
        if description:
            wrapper.__doc__ = description
        return wrapper

    return deco


def outbox_meta_of(tool: Any) -> Optional[dict]:
    """Return the `@tape.outbox_tool` metadata for an ADK tool, if any."""
    for attr in ("func", "fn", "_func", "__wrapped__"):
        f = getattr(tool, attr, None)
        if f is not None and hasattr(f, "_tape_outbox"):
            return f._tape_outbox  # type: ignore[attr-defined]
    if hasattr(tool, "_tape_outbox"):
        return tool._tape_outbox  # type: ignore[attr-defined]
    return None


__all__ = ["outbox_tool", "outbox_meta_of", "OutboxConfigError", "OutboxSemantics"]
