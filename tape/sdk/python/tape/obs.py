"""Observability — structured-log helpers and OpenTelemetry span-name constants.

Span names (use as the operation name when starting a span — emitter is free
to use OTel SDK, the agnostic `tape.obs.span(name, **attrs)` helper here, or
nothing at all):

    tape.begin_run, tape.resume_run,
    tape.record_decision,
    tape.begin_effect, tape.complete_effect,
    tape.reconcile_effect, tape.dispatch_effect,
    tape.compensate, tape.redrive,
    tape.await_signal, tape.send_signal

Structured log fields — emit these as JSON when available::

    tenant_id, app_name, run_id, invocation_id, session_id, seq,
    effect_key, decision_index, reactor, lease_owner

The `log_json(...)` helper writes one JSON line to stderr with stable
key ordering; the `span(...)` helper is a no-op context manager when
opentelemetry is not installed.
"""

from __future__ import annotations

import contextlib
import json
import sys
import time
from typing import Any, Iterator, Optional

# Stable span-name constants — use these instead of stringly-typed magic.
SPAN_BEGIN_RUN = "tape.begin_run"
SPAN_RESUME_RUN = "tape.resume_run"
SPAN_RECORD_DECISION = "tape.record_decision"
SPAN_BEGIN_EFFECT = "tape.begin_effect"
SPAN_COMPLETE_EFFECT = "tape.complete_effect"
SPAN_RECONCILE_EFFECT = "tape.reconcile_effect"
SPAN_DISPATCH_EFFECT = "tape.dispatch_effect"
SPAN_COMPENSATE = "tape.compensate"
SPAN_REDRIVE = "tape.redrive"
SPAN_AWAIT_SIGNAL = "tape.await_signal"
SPAN_SEND_SIGNAL = "tape.send_signal"

ALL_SPANS = (
    SPAN_BEGIN_RUN, SPAN_RESUME_RUN, SPAN_RECORD_DECISION,
    SPAN_BEGIN_EFFECT, SPAN_COMPLETE_EFFECT,
    SPAN_RECONCILE_EFFECT, SPAN_DISPATCH_EFFECT,
    SPAN_COMPENSATE, SPAN_REDRIVE,
    SPAN_AWAIT_SIGNAL, SPAN_SEND_SIGNAL,
)

# The structured-log field order — every emitted record should follow this for
# easy aggregation in Cloud Logging.
STRUCTURED_FIELDS = (
    "ts", "level", "msg",
    "tenant_id", "app_name", "run_id", "invocation_id", "session_id",
    "seq", "effect_key", "decision_index", "reactor", "lease_owner",
)


def log_json(msg: str, *, level: str = "INFO", **fields: Any) -> None:
    """Emit one structured JSON line to stderr, following `STRUCTURED_FIELDS`."""
    record = {"ts": time.time(), "level": level, "msg": msg}
    record.update({k: v for k, v in fields.items() if v not in (None, "")})
    ordered = {k: record[k] for k in STRUCTURED_FIELDS if k in record}
    ordered.update({k: v for k, v in record.items() if k not in STRUCTURED_FIELDS})
    sys.stderr.write(json.dumps(ordered, default=str) + "\n")
    sys.stderr.flush()


@contextlib.contextmanager
def span(name: str, **attrs: Any) -> Iterator[Any]:
    """Open a span if OpenTelemetry is installed, otherwise yield None.

    Importing OTel lazily keeps `tape` usable with no extra deps.
    """
    try:
        from opentelemetry import trace
        tracer = trace.get_tracer("tape")
    except Exception:
        yield None
        return
    with tracer.start_as_current_span(name) as sp:
        for k, v in attrs.items():
            try:
                sp.set_attribute(k, v)
            except Exception:
                pass
        yield sp


def configure_cloud_trace_exporter(project_id: Optional[str] = None) -> bool:
    """Best-effort: register the Cloud Trace exporter if the packages are
    available. Returns True on success, False otherwise (so callers can warn
    once at startup)."""
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
    except Exception:
        return False
    provider = TracerProvider()
    provider.add_span_processor(BatchSpanProcessor(
        CloudTraceSpanExporter(project_id=project_id) if project_id
        else CloudTraceSpanExporter()
    ))
    trace.set_tracer_provider(provider)
    return True


__all__ = [
    "SPAN_BEGIN_RUN", "SPAN_RESUME_RUN", "SPAN_RECORD_DECISION",
    "SPAN_BEGIN_EFFECT", "SPAN_COMPLETE_EFFECT",
    "SPAN_RECONCILE_EFFECT", "SPAN_DISPATCH_EFFECT",
    "SPAN_COMPENSATE", "SPAN_REDRIVE",
    "SPAN_AWAIT_SIGNAL", "SPAN_SEND_SIGNAL",
    "ALL_SPANS", "STRUCTURED_FIELDS",
    "log_json", "span", "configure_cloud_trace_exporter",
]
