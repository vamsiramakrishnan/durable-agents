"""PubSubConnector — publish each effect's intent as one Pub/Sub message.

This is **controlled, auditable delivery, not exactly-once at the final API**.
The downstream subscriber is the one talking to the non-idempotent upstream
and is responsible for the business-key lock (the message attributes give it
everything it needs to dedupe).

Why use this:
  * decouple agent processes from the upstream's quirks (rate limits, slow
    endpoints, IP allow-lists),
  * fan out one effect to multiple consumers (a payments worker AND a ledger
    audit sink),
  * survive a long upstream outage without holding agent state.

google-cloud-pubsub is a **lazy import** — the SDK does not declare it as a
dependency. Install it (`pip install google-cloud-pubsub`) where the outbox
reactor runs; agent processes don't need it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from .base import DispatchResult, ObservationResult, CompensationResult


@dataclass
class PubSubConnector:
    """Publish effect intents to a Pub/Sub topic, one message per effect.

      * `name`             — match key for `@tape.effect(connector=…)`.
      * `project`, `topic` — Pub/Sub destination.
      * `ordering_key_from` — callable `(effect) -> str` for the ordering key;
        defaults to `effect.run_id` so a run's effects publish in order.
      * `publisher`        — optional `pubsub_v1.PublisherClient` (test seam).

    `dispatch()` returns `confirmed` on publish ack (the *message landed in
    Pub/Sub*, which is what we promised); the downstream subscriber is
    responsible for the upstream business outcome. For the upstream answer,
    pair this connector with a reconciler `status_check` that asks the
    business system, or with a second connector whose `observe()` reads
    a business-side ledger.

    `observe()` here is a no-op (`absent`) — the connector knows about the
    Pub/Sub side, not the business side. Override in a subclass when the
    business system exposes a "did this happen?" endpoint.
    """
    name: str
    project: str
    topic: str
    ordering_key_from: Any = None
    publisher: Any = None       # pubsub_v1.PublisherClient

    def __post_init__(self) -> None:
        if self.ordering_key_from is None:
            self.ordering_key_from = lambda e: e.run_id

    def _client(self):
        if self.publisher is not None:
            return self.publisher
        from google.cloud import pubsub_v1  # type: ignore[import-not-found]
        from google.cloud.pubsub_v1.types import PublisherOptions  # type: ignore[import-not-found]
        # Ordering is required for ordering_key to be honoured.
        self.publisher = pubsub_v1.PublisherClient(
            publisher_options=PublisherOptions(enable_message_ordering=True))
        return self.publisher

    def _topic_path(self) -> str:
        return f"projects/{self.project}/topics/{self.topic}"

    # ── dispatch ────────────────────────────────────────────────────────────
    def dispatch(self, effect) -> DispatchResult:
        try:
            client = self._client()
            data = effect.request_json.encode("utf-8") if effect.request_json else b"{}"
            attrs = {
                "tape_run_id": effect.run_id,
                "tape_effect_key": effect.idempotency_key,
                "tape_business_key": effect.business_key or "",
                "tape_connector": self.name,
                "tape_tool": effect.tool_name,
                # The semantics tag lets the subscriber decide whether to
                # re-publish, dedupe by business key, or reject if it can't
                # handle non-idempotent.
                "tape_semantics": str(effect.semantics),
            }
            future = client.publish(
                self._topic_path(),
                data=data,
                ordering_key=str(self.ordering_key_from(effect)),
                **attrs,
            )
            mid = future.result(timeout=30)
            return DispatchResult(
                status="confirmed",
                external_ref=str(mid),
                response={"message_id": str(mid), "topic": self._topic_path()})
        except Exception as ex:
            # Publish failure: unknown if it might have crossed the wire,
            # else failed. PubSub publishes are at-least-once on success, but
            # a failure here is almost always "didn't land" — treat as failed
            # so the outbox loop retries.
            return DispatchResult(status="failed",
                                  error={"type": type(ex).__name__, "message": str(ex)})

    # ── observe (default: no-op) ────────────────────────────────────────────
    def observe(self, effect) -> ObservationResult:
        return ObservationResult(status="absent")

    # ── compensate (default: not supported) ─────────────────────────────────
    def compensate(self, obligation) -> CompensationResult:
        return CompensationResult(
            status="failed",
            error={"reason": "PubSubConnector does not implement compensate; "
                             "publish a compensation message via a separate connector or "
                             "wire it through the business system."})
