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
# tool name -> {kind, fn, compensator_ref, max_attempts, compensation_payload}.
# Distinct from _COMPENSATORS (keyed by compensator kind/name) so the outbox
# reactor and reconciler — which only know the *tool* name at lookup time —
# can resolve "what inverse should I register for this tool?" deterministically.
_TOOL_COMPENSATORS: dict[str, dict] = {}


def register_compensator(name: str, fn: Callable) -> None:
    _COMPENSATORS[name] = fn


def get_compensator(name: str) -> Optional[Callable]:
    return _COMPENSATORS.get(name)


def register_tool_compensator(tool_name: str, fn: Callable, *,
                              compensator_ref: str = "",
                              max_attempts: int = 0,
                              compensation_payload: Optional[Callable] = None) -> None:
    """Index a compensator by the *tool* name it inverses (in addition to the
    plain `register_compensator` keying it by its own name). The outbox
    reactor and reconciler use this so they can look up "what inverse should
    I register for this tool?" from just the tool name on the EffectRecord."""
    if not tool_name or fn is None:
        return
    _TOOL_COMPENSATORS[tool_name] = {
        "kind": getattr(fn, "__name__", "compensate"),
        "fn": fn,
        "compensator_ref": compensator_ref or "",
        "max_attempts": int(max_attempts or 0),
        "compensation_payload": compensation_payload,
    }


def get_tool_compensator(tool_name: str) -> Optional[dict]:
    return _TOOL_COMPENSATORS.get(tool_name)


def register_status_check(tool_name: str, fn: Callable) -> None:
    _STATUS_CHECKS[tool_name] = fn


def get_status_check(tool_name: str) -> Optional[Callable]:
    return _STATUS_CHECKS.get(tool_name)


def compensator_ref_of(fn: Callable) -> str:
    """The fully-qualified "module:attr" for a callable, suitable for a generic
    drainer process to `importlib.import_module` + `getattr` at drain time.
    Returns "" when the callable is anonymous, a lambda, or otherwise can't be
    addressed by name."""
    mod = getattr(fn, "__module__", "") or ""
    name = getattr(fn, "__qualname__", "") or getattr(fn, "__name__", "") or ""
    if not mod or not name or "<" in name:
        return ""
    return f"{mod}:{name}"


# ── effect semantics (mirror of the proto enums; kept as plain strings on the
# Python side for readability — the plugin maps them to pb.EFFECT_SEMANTICS_*) ─
_SEMANTICS = ("idempotent", "non_idempotent", "observe_only")
_DISPATCH = ("inline", "outbox")


def _validate_semantics(*, semantics, dispatch, status_check, compensate,
                        business_key, allow_unsafe: bool) -> None:
    """The safety contract for non-idempotent upstreams (the whole point of the
    GCP hardening plan). Surface mistakes loudly at decoration time, not at the
    first crash in production."""
    import warnings
    if semantics not in _SEMANTICS:
        raise ValueError(f"@tape.effect: semantics must be one of {_SEMANTICS}; got {semantics!r}")
    if dispatch not in _DISPATCH:
        raise ValueError(f"@tape.effect: dispatch must be one of {_DISPATCH}; got {dispatch!r}")
    if semantics == "non_idempotent":
        if dispatch == "inline":
            # The server enforces this too, but a warning at decoration time
            # makes the contract visible in code review.
            warnings.warn(
                "@tape.effect: semantics='non_idempotent' + dispatch='inline' is unsafe — "
                "a crash mid-call leaves an ambiguity the runtime cannot blindly retry. "
                "Use dispatch='outbox' (intent-only tool body) + a connector. "
                "The server will refuse this at begin_effect time.",
                stacklevel=3,
            )
        if not allow_unsafe and status_check is None and compensate is None and business_key is None:
            raise ValueError(
                "@tape.effect: semantics='non_idempotent' requires at least one of "
                "status_check= (so the reconciler can resolve UNKNOWN), compensate= (so "
                "a duplicate or wrong outcome can be unwound), or business_key= (so the "
                "counterparty can be queried by business identity). Pass allow_unsafe=True "
                "to override after explicit review.")


def effect(*, compensate: Optional[Callable] = None, status_check: Optional[Callable] = None,
           key_from: Optional[Callable] = None,
           compensation_payload: Optional[Callable] = None,
           retry: Optional[RetryPolicy] = None,
           max_attempts: int = 0,
           # ── non-idempotent / outbox contract (Phase 1 of the GCP hardening) ──
           semantics: str = "idempotent",
           dispatch: str = "inline",
           connector: str = "",
           business_key: Any = None,
           allow_unsafe: bool = False) -> Callable:
    """Declare an effect.

    `semantics` is one of "idempotent" (default — the counterparty dedupes on
    our key; blind retry is safe), "non_idempotent" (a second call would land
    twice — Tape refuses inline dispatch), or "observe_only" (no side effects).

    `dispatch` is "inline" (the tool body runs and calls the counterparty —
    the v1 model) or "outbox" (the tool body records intent; the outbox
    reactor calls the counterparty via a registered `connector`).

    `connector` names the routing key the outbox reactor matches on (e.g.
    "bank.wire"). Required when `dispatch="outbox"`.

    `business_key` is either a static string or a callable `(tool_args,
    tool_context) -> str` that yields the cross-run business-level dedupe key
    — what the counterparty itself would use to recognise the same logical
    operation. When set, the server enforces uniqueness on
    `(connector, business_key)` across all runs.
    """
    _validate_semantics(semantics=semantics, dispatch=dispatch,
                        status_check=status_check, compensate=compensate,
                        business_key=business_key, allow_unsafe=allow_unsafe)
    if dispatch == "outbox" and not connector:
        raise ValueError("@tape.effect: dispatch='outbox' requires connector=<routing key>")
    # P2: business_key without connector creates a unique-index landmine on
    # the server side (the partial UNIQUE on `tape_effects(connector,
    # business_key) WHERE business_key <> ''` collides for any pair of rows
    # with `connector=''` sharing a business_key). Refuse the contract here
    # so the misconfiguration surfaces at decoration time, not as a flaky
    # gRPC error under retry.
    if business_key is not None and isinstance(business_key, str) and business_key and not connector:
        raise ValueError(
            "@tape.effect: business_key=<str> requires connector=<routing key>; "
            "cross-run dedupe is per-(connector, business_key)")

    def deco(fn: Callable) -> Callable:
        meta = {
            "compensate": compensate,
            "status_check": status_check,
            "key_from": key_from,
            "compensation_payload": compensation_payload,
            "retry": retry,
            # The drainer's own retry budget (server default 5 if unset). Distinct
            # from `retry=` above, which retries the *forward* call.
            "max_attempts": max_attempts,
            # The "module:attr" path so a generic drainer (one that doesn't import
            # this agent module at boot) can resolve the inverse on demand.
            "compensator_ref": compensator_ref_of(compensate) if compensate is not None else "",
            # Outbox contract.
            "semantics": semantics,
            "dispatch": dispatch,
            "connector": connector,
            "business_key": business_key,
        }
        if compensate is not None:
            register_compensator(getattr(compensate, "__name__", "compensate"), compensate)
            # Also key the compensator by the *tool* name so the outbox
            # reactor / reconciler (which only have the EffectRecord's
            # tool_name at lookup time) can resolve the inverse and the
            # compensation_payload deterministically. Fixes the P1 bug
            # where confirmed outbox effects never enqueued rollback
            # obligations.
            register_tool_compensator(getattr(fn, "__name__", ""), compensate,
                                      compensator_ref=meta["compensator_ref"],
                                      max_attempts=max_attempts,
                                      compensation_payload=compensation_payload)
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


def outbox_tool(*, connector: str, semantics: str = "non_idempotent",
                business_key: Any = None, compensate: Optional[Callable] = None,
                status_check: Optional[Callable] = None,
                max_attempts: int = 0, allow_unsafe: bool = False) -> Callable:
    """Sugar for the non-idempotent + outbox pattern.

        @tape.outbox_tool(connector="bank.wire", semantics="non_idempotent",
                          business_key=lambda account, amount, date: f"{account}:{amount}:{date}",
                          compensate=reverse_wire, status_check=find_wire)
        def wire_money(account, amount, beneficiary, date):
            return {"account": account, "amount": amount,
                    "beneficiary": beneficiary, "date": date}

    The tool body **only builds the intent payload** — it must not perform
    external IO. TapePlugin records the intent via BeginEffect and returns a
    synthetic `{tape_status: "accepted", ...}` result to ADK without running
    the body. The outbox reactor picks it up and the registered `connector`
    performs the call exactly once, with explicit ambiguity handling."""
    return effect(
        semantics=semantics,
        dispatch="outbox",
        connector=connector,
        business_key=business_key,
        compensate=compensate,
        status_check=status_check,
        max_attempts=max_attempts,
        allow_unsafe=allow_unsafe,
    )


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


def business_key(tool_context: Any) -> str:
    """The business-level dedupe key Tape derived (from the decorator's
    `business_key=` callable) for the in-flight effect. Empty if no key was
    declared."""
    try:
        return tool_context.state.get("temp:_tape_business_key", "")
    except Exception:
        return ""


def external_ref(tool_context: Any) -> str:
    """The counterparty's identifier for the operation (a wire_id, a payment
    intent id), once Tape has observed it. Empty until known — typically only
    populated on re-drive after a connector + reconciler round-trip."""
    try:
        return tool_context.state.get("temp:_tape_external_ref", "")
    except Exception:
        return ""


def effect_semantics(tool_context: Any) -> str:
    """The declared semantics of the in-flight effect: 'idempotent',
    'non_idempotent', or 'observe_only'. '' if not set."""
    try:
        return tool_context.state.get("temp:_tape_effect_semantics", "")
    except Exception:
        return ""


# ── string → proto-enum mappers (used by the plugin to set BeginEffectRequest) ─

def _semantics_to_pb(s: str) -> int:
    from ._gen import tape_pb2 as _pb
    return {
        "idempotent": _pb.EFFECT_SEMANTICS_IDEMPOTENT,
        "non_idempotent": _pb.EFFECT_SEMANTICS_NON_IDEMPOTENT,
        "observe_only": _pb.EFFECT_SEMANTICS_OBSERVE_ONLY,
    }.get(s, _pb.EFFECT_SEMANTICS_UNSPECIFIED)


def _dispatch_to_pb(s: str) -> int:
    from ._gen import tape_pb2 as _pb
    return {
        "inline": _pb.EFFECT_DISPATCH_MODE_INLINE,
        "outbox": _pb.EFFECT_DISPATCH_MODE_OUTBOX,
    }.get(s, _pb.EFFECT_DISPATCH_MODE_UNSPECIFIED)


def _resolve_business_key(value: Any, tool_args: dict, tool_context: Any) -> str:
    """`business_key=` on the decorator may be a static string, or a callable
    that yields the cross-run dedupe key. The callable can take any of these
    shapes — we try them in order and return the first one that doesn't
    raise:

        lambda **kw, tool_context=None: ...   # the explicit form
        lambda account, amount, date: ...     # the documented form
        lambda tool_args, tool_context: ...   # the (dict, ctx) form
        lambda tool_args: ...                 # the (dict,) form

    Falls through to "" only if every shape raised — i.e. the signature is
    genuinely incompatible. Using `inspect.signature` to dispatch would be
    cleaner but bind() blesses partial matches; the explicit ladder makes
    "what we tried" obvious in tracebacks. (P1 fix: previously the second
    fallback was `value(tool_args, tool_context)`, which failed for the
    documented `lambda account, amount, date: ...` form and silently
    returned "" — losing the cross-run dedupe key.)
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if not callable(value):
        return ""
    args = tool_args or {}
    attempts = (
        lambda: value(**args, tool_context=tool_context),  # kwargs + ctx
        lambda: value(**args),                              # kwargs only (the documented form)
        lambda: value(args, tool_context),                  # positional (dict, ctx)
        lambda: value(args),                                # positional (dict,)
    )
    for attempt in attempts:
        try:
            v = attempt()
        except TypeError:
            continue
        except Exception:
            return ""
        return str(v or "")
    return ""
