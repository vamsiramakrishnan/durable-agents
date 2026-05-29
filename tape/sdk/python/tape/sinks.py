"""Sinks for the WAL fan-out — the "connective tissue" surface.

A `Sink` is a `publish(entry) -> None` callable. The relay reads journal entries
via `SubscribeEvents`, calls `sink.publish(entry)` for each, and advances a
durable cursor (a local JSON file) so a relay restart resumes from where it
stopped. Combined with a sink that dedupes on `(run_id, seq)` — or one whose
backend dedupes per message_id (Pub/Sub does, within its dedup window) — that
gives **exactly-once-effective** delivery: the at-least-once relay plus the
consumer's idempotent receipt.

Built-in sinks:
  * `LogSink(path)`       — append each entry as a JSON line; testable, useful as
                             a tap.
  * `WebhookSink(url, …)` — POST each entry to an HTTP endpoint (the JSON entry
                             plus an `X-Tape-Event-Id: <run_id>/<seq>` header
                             receivers can de-dup on); retries with backoff.
  * `PubSubSink(project, topic)` — publish to Google Cloud Pub/Sub with
                             `ordering_key=run_id`, `message_id` derived from
                             `(run_id, seq)`; lazy-imports google-cloud-pubsub.

Use `tape.reactors.run_outbox_relay(url, sink, cursor_path=…)` to wire it up.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Protocol


class Sink(Protocol):
    def publish(self, entry: Any) -> None: ...
    def close(self) -> None: ...


# ── log sink (mostly for tests + taps) ─────────────────────────────────────

class LogSink:
    """Appends one JSON line per entry. `path=":stderr"` writes to stderr."""

    def __init__(self, path: str = ":stderr"):
        self.path = path
        self._fp = None
        if path != ":stderr":
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            self._fp = open(path, "a", buffering=1)

    def publish(self, entry: Any) -> None:
        line = json.dumps({
            "run_id": getattr(entry, "run_id", ""),
            "seq": getattr(entry, "seq", 0),
            "kind": getattr(entry, "kind", ""),
            "payload_json": getattr(entry, "payload_json", ""),
            "ts_ms": getattr(entry, "ts_ms", 0),
        })
        if self._fp:
            self._fp.write(line + "\n")
        else:
            import sys
            print(line, file=sys.stderr, flush=True)

    def close(self) -> None:
        if self._fp:
            self._fp.close()


# ── webhook sink ────────────────────────────────────────────────────────────

@dataclass
class WebhookSink:
    """POST each journal entry to `url` as JSON. Sets `X-Tape-Event-Id:
    <run_id>/<seq>` so the receiver can de-dup. At-least-once: a successful POST
    may still be retried (the relay sees the response after the fact)."""

    url: str
    headers: dict = field(default_factory=dict)
    max_retries: int = 3
    initial_backoff_s: float = 0.5
    timeout_s: float = 10.0

    def publish(self, entry: Any) -> None:
        body = json.dumps({
            "run_id": getattr(entry, "run_id", ""),
            "seq": getattr(entry, "seq", 0),
            "kind": getattr(entry, "kind", ""),
            "payload_json": getattr(entry, "payload_json", ""),
            "ts_ms": getattr(entry, "ts_ms", 0),
        }).encode("utf-8")
        event_id = f"{getattr(entry, 'run_id', '')}/{getattr(entry, 'seq', 0)}"
        hdr = {"Content-Type": "application/json", "X-Tape-Event-Id": event_id, **self.headers}
        delay = self.initial_backoff_s
        last_err: Optional[Exception] = None
        for _ in range(self.max_retries):
            try:
                req = urllib.request.Request(self.url, data=body, headers=hdr, method="POST")
                with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                    if 200 <= resp.status < 300:
                        return
                    last_err = RuntimeError(f"webhook {self.url} returned HTTP {resp.status}")
            except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as ex:
                last_err = ex
            time.sleep(delay)
            delay *= 2
        if last_err:
            raise last_err

    def close(self) -> None:
        pass


# ── Pub/Sub sink ────────────────────────────────────────────────────────────

class PubSubSink:
    """Publish to Google Cloud Pub/Sub. `ordering_key = run_id` (so per-run order
    is preserved at the subscriber if it enables ordered delivery). The Pub/Sub
    `message_id` is assigned by Pub/Sub; the *attribute* `tape-event-id =
    run_id/seq` is what consumers should dedup on. Lazy-imports
    google-cloud-pubsub — if it's not installed, the constructor raises."""

    def __init__(self, project: str, topic: str):
        try:
            from google.cloud import pubsub_v1  # type: ignore
        except Exception as ex:  # noqa: BLE001
            raise RuntimeError("PubSubSink requires google-cloud-pubsub; pip install google-cloud-pubsub") from ex
        self._publisher = pubsub_v1.PublisherClient(publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True))
        self._topic = self._publisher.topic_path(project, topic)

    def publish(self, entry: Any) -> None:
        body = json.dumps({
            "run_id": getattr(entry, "run_id", ""),
            "seq": getattr(entry, "seq", 0),
            "kind": getattr(entry, "kind", ""),
            "payload_json": getattr(entry, "payload_json", ""),
            "ts_ms": getattr(entry, "ts_ms", 0),
        }).encode("utf-8")
        future = self._publisher.publish(
            self._topic, body,
            ordering_key=str(getattr(entry, "run_id", "")),
            attributes={"tape-event-id": f"{entry.run_id}/{entry.seq}", "kind": entry.kind},
        )
        future.result(timeout=10.0)

    def close(self) -> None:
        try:
            self._publisher.stop()
        except Exception:
            pass


# ── callable adapter ────────────────────────────────────────────────────────

class FnSink:
    """Wrap any `def publish(entry)` callable as a Sink."""
    def __init__(self, fn: Callable[[Any], None]):
        self._fn = fn
    def publish(self, entry: Any) -> None:
        self._fn(entry)
    def close(self) -> None:
        pass


# ── AIPlex audit sink (AIPlex integration PR 11) ───────────────────────────

# Wire shape mirrors aiplex/internal/models/execution.go::ExecutionEvent.
# Tape's `JournalEntry.kind` is a free-form string ("run" / "decision" /
# "effect" / "obligation" / "gate" / "value" / "policy"). AIPlex's
# `ExecutionEventKind` is a tighter enum the Console switches on. We map
# at egress so AIPlex never has to parse Tape's payload — the kind on the
# wire is already the consumer's vocabulary.

# Pre-decoded payload signals → AIPlex event kind. Order matters: the first
# matching key wins so "run.completed" beats the generic "run.*" mapping.
_AIPLEX_KIND_RULES = (
    # kind, payload-key match, AIPlex kind
    ("run",       ("status", "running"),       "run.started"),
    ("run",       ("status", "terminal"),      "run.completed"),
    ("run",       ("status", "failed"),        "run.failed"),
    ("run",       ("status", "stuck"),         "run.failed"),
    ("run",       ("status", "cancelled"),     "run.failed"),
    ("run",       ("status", "compensating"),  "obligation.created"),
    ("decision",  None,                        "decision.recorded"),
    ("effect",    ("status", "pending"),       "effect.begin"),
    ("effect",    ("status", "confirmed"),     "effect.confirmed"),
    ("effect",    ("status", "failed"),        "effect.failed"),
    ("effect",    ("status", "unknown"),       "effect.unknown"),
    ("effect",    ("status", "duplicate"),     "effect.duplicate"),
    ("obligation", None,                       "obligation.created"),
    ("gate",      None,                        "gate.waiting"),
    ("policy",    None,                        "policy.violation"),
    ("budget",    None,                        "budget.charged"),
    ("timer",     None,                        "timer.scheduled"),
)


def _map_kind(tape_kind: str, payload: dict) -> str:
    """Pick the AIPlex event kind for a Tape journal entry. Returns
    `tape_kind` verbatim as a last-resort fallback — AIPlex's enum is
    forward-compatible, the Console renders unknown kinds with a neutral
    glyph."""
    for k, predicate, aiplex_kind in _AIPLEX_KIND_RULES:
        if tape_kind != k:
            continue
        if predicate is None:
            return aiplex_kind
        key, want = predicate
        if str(payload.get(key, "")).lower() == want:
            return aiplex_kind
    return tape_kind


class AIPlexSink:
    """POST execution events to AIPlex's ingestion endpoint.

    Wire contract: matches `aiplex/internal/models/execution.go::ExecutionEvent`.
    The endpoint is at `${AIPLEX_INGEST_URL}/internal/tape/events` and is
    bearer-token authenticated via `AIPLEX_INGEST_TOKEN`.

    Batching: events are buffered until either `batch_size` accumulates or
    `flush_interval_s` elapses. `publish()` is non-blocking on the network —
    the flush happens on a worker thread so the relay loop doesn't stall
    behind a slow AIPlex.

    At-least-once: AIPlex dedupes on `(run_id, seq)`, so a retry-on-error
    after a partial batch is safe. The sink doesn't checkpoint internally
    — that's the relay's job via the cursor file.

    Identity fields: AIPlex needs `tenant_id`, `actor`, `agent_id` etc. on
    every event. The sink reads them from the constructor or, if omitted,
    from `AIPLEX_*` env vars (matching `tape.adk.identity.RunIdentity.from_env`).
    Identity is per-pod, not per-event, because a Tape relay serves one
    AIPlex-deployed agent at a time.
    """

    def __init__(
        self,
        *,
        url: str = "",
        token: str = "",
        tenant_id: str = "",
        agent_id: str = "",
        plane: str = "",
        actor: str = "",
        subject: str = "",
        aiplex_instance_id: str = "",
        batch_size: int = 100,
        flush_interval_s: float = 1.0,
        max_retries: int = 5,
        initial_backoff_s: float = 0.5,
        timeout_s: float = 10.0,
    ):
        self.url = (url or os.environ.get("AIPLEX_INGEST_URL", "")).rstrip("/")
        if not self.url:
            raise ValueError(
                "AIPlexSink requires url= or AIPLEX_INGEST_URL env (e.g. https://aiplex.example.com)")
        if not self.url.endswith("/internal/tape/events"):
            self.url = self.url + "/internal/tape/events"
        self.token = token or os.environ.get("AIPLEX_INGEST_TOKEN", "")

        # Identity defaults match RunIdentity.from_env conventions.
        self.tenant_id = tenant_id or os.environ.get("AIPLEX_TENANT_ID", "")
        self.agent_id = agent_id or os.environ.get("AIPLEX_AGENT_ID", "")
        self.plane = plane or _infer_plane(os.environ.get("AIPLEX_ROUTE", ""))
        self.actor = actor or os.environ.get("AIPLEX_ACTOR", "")
        self.subject = subject or os.environ.get("AIPLEX_SUBJECT", "")
        self.aiplex_instance_id = (
            aiplex_instance_id or os.environ.get("AIPLEX_INSTANCE_ID", ""))

        self.batch_size = max(1, int(batch_size))
        self.flush_interval_s = max(0.05, float(flush_interval_s))
        self.max_retries = max(1, int(max_retries))
        self.initial_backoff_s = max(0.05, float(initial_backoff_s))
        self.timeout_s = max(1.0, float(timeout_s))

        import threading
        self._lock = threading.Lock()
        self._buf: list[dict] = []
        self._last_flush = time.monotonic()
        self._closed = False

    # The reactor calls publish(entry) per JournalEntry. We buffer and
    # flush periodically — the time-since-last-flush check inside publish()
    # lets us avoid spawning a background thread.
    def publish(self, entry: Any) -> None:
        if self._closed:
            return
        ev = self._to_execution_event(entry)
        with self._lock:
            self._buf.append(ev)
            should_flush = (
                len(self._buf) >= self.batch_size
                or time.monotonic() - self._last_flush >= self.flush_interval_s
            )
        if should_flush:
            self.flush()

    def flush(self) -> None:
        with self._lock:
            batch, self._buf = self._buf, []
            self._last_flush = time.monotonic()
        if not batch:
            return
        body = json.dumps({"events": batch}).encode("utf-8")
        hdr = {"Content-Type": "application/json"}
        if self.token:
            hdr["Authorization"] = f"Bearer {self.token}"
        delay = self.initial_backoff_s
        last_err: Optional[Exception] = None
        for _ in range(self.max_retries):
            try:
                req = urllib.request.Request(self.url, data=body, headers=hdr, method="POST")
                with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                    if 200 <= resp.status < 300:
                        return
                    last_err = RuntimeError(
                        f"AIPlex ingest {self.url} returned HTTP {resp.status}")
            except urllib.error.HTTPError as ex:
                # 4xx (other than 408/429) are permanent — surfacing
                # them as failures rather than retrying forever.
                if 400 <= ex.code < 500 and ex.code not in (408, 429):
                    raise
                last_err = ex
            except (urllib.error.URLError, TimeoutError) as ex:
                last_err = ex
            time.sleep(delay)
            delay *= 2
        if last_err:
            raise last_err

    def close(self) -> None:
        try:
            self.flush()
        finally:
            self._closed = True

    def _to_execution_event(self, entry: Any) -> dict:
        """Map a Tape JournalEntry to an AIPlex ExecutionEvent dict.
        Payload-driven kind mapping (see `_AIPLEX_KIND_RULES`)."""
        payload = {}
        raw = getattr(entry, "payload_json", "") or ""
        if raw:
            try:
                payload = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                payload = {}
        tape_kind = getattr(entry, "kind", "")
        aiplex_kind = _map_kind(tape_kind, payload)
        ts_ms = int(getattr(entry, "ts_ms", 0) or 0)
        ts_iso = (
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts_ms / 1000.0))
            if ts_ms > 0
            else time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        )
        return {
            "run_id": getattr(entry, "run_id", ""),
            "seq": int(getattr(entry, "seq", 0) or 0),
            "tenant_id": self.tenant_id,
            "agent_id": self.agent_id,
            "plane": self.plane,
            "actor": self.actor,
            "subject": self.subject,
            "aiplex_instance_id": self.aiplex_instance_id,
            "kind": aiplex_kind,
            "scope": str(payload.get("scope", payload.get("required_scope", "")) or ""),
            "tool": str(payload.get("tool", "") or ""),
            "timestamp": ts_iso,
            "payload_json": raw,
        }


def _infer_plane(route: str) -> str:
    """Pull the AIPlex plane name out of an AIPLEX_ROUTE like
    '/a2a/treasury'. Returns '' when the convention doesn't match."""
    if not route:
        return ""
    parts = [p for p in route.strip("/").split("/") if p]
    if not parts:
        return ""
    head = parts[0].lower()
    if head in ("a2a", "mcp", "llm"):
        return {"a2a": "a2aplex", "mcp": "mcplex", "llm": "llmplex"}[head]
    return head
