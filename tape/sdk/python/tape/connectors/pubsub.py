"""Pub/Sub connector — publish the intent as a message; the upstream is a
push/pull subscriber that does the actual work.

Pub/Sub dedupes within its dedup window on `message_id`, so we derive the
message_id from the idempotency key. For ordering, we use `run_id` as the
ordering_key.

Observation (`observe`) is delegated to a Tape value record (`namespace =
"outbox/<connector>"`, `key = idempotency_key`) — the subscriber writes the
result there via `tape.set_value` when it processes the message. The reactor
reads that record to resolve UNKNOWN.

Compensation publishes to `compensate_topic` if configured, otherwise marks
STUCK so a human-in-the-loop can take over.
"""

from __future__ import annotations

import json
from typing import Any, Optional

from .base import (
    Connector,
    DispatchResult,
    DispatchOutcome,
    ObservationResult,
    ObservationOutcome,
    CompensationResult,
    CompensationOutcome,
    EffectRecord,
    ObligationRecord,
)


class PubSubConnector:
    def __init__(
        self,
        *,
        project: str,
        topic: str,
        name: Optional[str] = None,
        compensate_topic: Optional[str] = None,
        tape_url: Optional[str] = None,
    ):
        self.name = name or f"pubsub:{topic}"
        self.project = project
        self.topic = topic
        self.compensate_topic = compensate_topic
        self.tape_url = tape_url

    def _publisher(self):
        try:
            from google.cloud import pubsub_v1
        except ImportError as ex:  # pragma: no cover
            raise RuntimeError(
                "PubSubConnector requires google-cloud-pubsub — "
                "`pip install google-cloud-pubsub`."
            ) from ex
        return pubsub_v1.PublisherClient()

    def _path(self, topic: str) -> str:
        from google.cloud import pubsub_v1
        return pubsub_v1.PublisherClient.topic_path(self.project, topic)

    async def dispatch(self, effect: EffectRecord) -> DispatchResult:
        try:
            client = self._publisher()
            future = client.publish(
                self._path(self.topic),
                data=json.dumps(effect.payload).encode("utf-8"),
                ordering_key=effect.run_id,
                tape_idempotency_key=effect.idempotency_key,
                tape_run_id=effect.run_id,
                tape_business_key=effect.business_key,
                tape_attempt=str(effect.attempt),
                tape_tool=effect.tool_name,
            )
            msg_id = future.result(timeout=30)
            return DispatchResult(outcome=DispatchOutcome.CONFIRMED,
                                  response={"message_id": msg_id},
                                  dispatch_id=msg_id)
        except Exception as ex:
            return DispatchResult(outcome=DispatchOutcome.UNKNOWN, error=str(ex))

    async def observe(self, effect: EffectRecord) -> ObservationResult:
        from ..client import TapeClient, DEFAULT_URL
        url = self.tape_url or DEFAULT_URL
        try:
            with TapeClient(url) as c:
                resp = c.get_value(namespace=f"outbox/{self.name}",
                                   key=effect.idempotency_key)
        except Exception as ex:
            return ObservationResult(outcome=ObservationOutcome.UNKNOWN, error=str(ex))
        if not resp.found:
            return ObservationResult(outcome=ObservationOutcome.ABSENT, count=0)
        try:
            body = json.loads(resp.value.value_json) if resp.value.value_json else {}
        except Exception:
            body = {}
        count = int(body.get("count", 1))
        if count == 0:
            return ObservationResult(outcome=ObservationOutcome.ABSENT, response=body, count=0)
        if count == 1:
            return ObservationResult(outcome=ObservationOutcome.CONFIRMED, response=body, count=1)
        return ObservationResult(outcome=ObservationOutcome.DUPLICATE, response=body, count=count)

    async def compensate(self, obligation: ObligationRecord) -> CompensationResult:
        if not self.compensate_topic:
            return CompensationResult(outcome=CompensationOutcome.STUCK,
                                      error="no compensate_topic configured")
        try:
            client = self._publisher()
            future = client.publish(
                self._path(self.compensate_topic),
                data=json.dumps(obligation.payload).encode("utf-8"),
                ordering_key=obligation.run_id,
                tape_obligation_kind=obligation.kind,
                tape_effect_key=obligation.effect_key,
                tape_run_id=obligation.run_id,
            )
            msg_id = future.result(timeout=30)
            return CompensationResult(outcome=CompensationOutcome.COMPENSATED,
                                      response={"message_id": msg_id})
        except Exception as ex:
            return CompensationResult(outcome=CompensationOutcome.PENDING, error=str(ex))
