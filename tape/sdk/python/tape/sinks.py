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
