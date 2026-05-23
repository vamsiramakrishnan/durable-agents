"""`@effect` and `@outbox_tool` decorators — attach Tape metadata to a tool
function so the `NonIdempotentSafetyPlugin` can recognise it at call time
and journal accordingly.

`@effect(...)` marks an idempotent inline tool — the plugin journals an
intent, lets the tool run, and records the result. Re-runs of the same
(invocation, decision, tool, call_index) short-circuit on the recorded
CONFIRMED status.

`@outbox_tool(...)` marks a NON-IDEMPOTENT tool whose dispatch goes through
the outbox reactor. The plugin never lets the tool run inline; it journals
an OUTBOX intent and returns a pending response that ADK serializes as the
function_response. The outbox dispatcher (separate process or scheduled
task) picks the intent up, calls the connector, completes the effect, and
on confirmation can append a function_response event to the session so the
agent's next run sees the resolution.

Construction-time refusal:

* `@outbox_tool` requires `business_key` (or a callable that derives it
  from the tool args), `connector`, and a `compensate` reference. Missing
  any of those raises `ValueError` at *decoration* time — well before any
  agent ever runs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional, Union

from .service import EffectDispatchMode, EffectSemantics


# Attribute used to stamp tape metadata on a function.
_TAPE_META_ATTR = "_tape_effect_meta"


@dataclass
class _EffectMeta:
    """Metadata the plugin reads off a decorated function at call time."""

    semantics: str
    dispatch_mode: str
    business_key_fn: Optional[Callable[..., str]] = None
    business_key_static: Optional[str] = None
    connector: Optional[str] = None
    compensate: Optional[str] = None
    # An optional default for the tool's idempotency_key beyond the
    # derived (invocation/decision/tool/call_index) shape.
    custom_key_fn: Optional[Callable[..., str]] = None

    def resolve_business_key(self, args: dict[str, Any]) -> Optional[str]:
        if self.business_key_static is not None:
            return self.business_key_static
        if self.business_key_fn is not None:
            try:
                return self.business_key_fn(**args)
            except TypeError:
                # Fallback: pass positionally in the order ADK gave us.
                return self.business_key_fn(*args.values())
        return None


def meta_of(fn_or_tool: Any) -> Optional[_EffectMeta]:
    """Read Tape metadata off a decorated function OR an ADK FunctionTool
    that wraps one. Returns None if the tool isn't Tape-tracked."""
    direct = getattr(fn_or_tool, _TAPE_META_ATTR, None)
    if direct is not None:
        return direct
    # ADK's FunctionTool stores the wrapped function on `.func`.
    func = getattr(fn_or_tool, "func", None)
    if func is not None:
        return getattr(func, _TAPE_META_ATTR, None)
    return None


# ── @effect — idempotent inline ─────────────────────────────────────────────


def effect(
    fn: Optional[Callable] = None,
    *,
    custom_key: Optional[Callable[..., str]] = None,
):
    """Mark a tool as idempotent + inline-journaled. The plugin records the
    intent before the call, the result after, and short-circuits on replay.

    Used as a bare decorator (`@effect`) or with kwargs
    (`@effect(custom_key=lambda **kw: ...)`).

    The tool body MUST be safe to call multiple times — if the agent crashes
    between intent and result, the re-drive will call the body again. The
    upstream is expected to dedupe via its own idempotency key (the proto's
    `idempotency_key` is passed via the request payload), or the body itself
    must be a no-op on repeat invocation.
    """

    def _wrap(f: Callable) -> Callable:
        setattr(f, _TAPE_META_ATTR, _EffectMeta(
            semantics=EffectSemantics.IDEMPOTENT,
            dispatch_mode=EffectDispatchMode.INLINE,
            custom_key_fn=custom_key,
        ))
        return f

    if fn is not None:
        # Used as `@effect` without parens.
        return _wrap(fn)
    return _wrap


# ── @outbox_tool — NON-IDEMPOTENT + OUTBOX ────────────────────────────────


def outbox_tool(
    *,
    business_key: Optional[Union[str, Callable[..., str]]] = None,
    connector: str,
    compensate: Optional[str] = None,
    custom_key: Optional[Callable[..., str]] = None,
):
    """Mark a tool as NON-IDEMPOTENT — its dispatch lives in the outbox.

    The agent calls the tool conceptually; the plugin intercepts and
    journals an intent. The actual upstream call is made later by the
    outbox dispatcher reactor, against the connector named here.

    Required:

    * `business_key` — the key the upstream uses to dedupe. Either a static
      string (rare), or a callable that takes the tool's keyword args and
      returns the key. The (connector, business_key) tuple is UNIQUE across
      the journal — no two effects can share it for the same connector.

    * `connector` — the registry name the outbox dispatcher resolves at
      runtime. Must match the `name` on a registered `Connector`.

    * `compensate` — the obligation `kind` to register on duplicate
      observation OR explicit compensation. Mirrors `compensate_on_duplicate_kind`
      in the proto's `RecordExternalObservation`.

    Construction-time refusal: omitting `business_key` or `connector` raises
    `ValueError` here — the bug never makes it past `import`."""
    if not connector:
        raise ValueError(
            "@outbox_tool: `connector` is required — the outbox dispatcher "
            "needs to know which connector to dispatch through.")
    if business_key is None:
        raise ValueError(
            "@outbox_tool: `business_key` is required — non-idempotent "
            "operations must declare the key the upstream uses to dedupe. "
            "Pass a string OR a callable that derives it from the tool args.")

    def _wrap(f: Callable) -> Callable:
        if isinstance(business_key, str):
            bk_static, bk_fn = business_key, None
        else:
            bk_static, bk_fn = None, business_key
        setattr(f, _TAPE_META_ATTR, _EffectMeta(
            semantics=EffectSemantics.NON_IDEMPOTENT,
            dispatch_mode=EffectDispatchMode.OUTBOX,
            business_key_static=bk_static,
            business_key_fn=bk_fn,
            connector=connector,
            compensate=compensate,
            custom_key_fn=custom_key,
        ))
        return f

    return _wrap


__all__ = ["effect", "outbox_tool", "meta_of"]
