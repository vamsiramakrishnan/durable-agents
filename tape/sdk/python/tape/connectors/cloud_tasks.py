"""Cloud Tasks connector — enqueue an HTTP target. Cloud Tasks owns retries,
backoff, and scheduling; the connector just creates the task.

The `task_name` is derived from the idempotency key so an at-most-once create
semantics holds for the duration the task name stays in Cloud Tasks history
(typically 1 hour after deletion).
"""

from __future__ import annotations

import json
import re
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


_TASK_ID_SAFE = re.compile(r"[^a-zA-Z0-9\-_]")


def _safe_task_id(key: str) -> str:
    return _TASK_ID_SAFE.sub("-", key)[:500]


class CloudTasksConnector:
    def __init__(
        self,
        *,
        project: str,
        location: str,
        queue: str,
        target_url: str,
        name: Optional[str] = None,
        service_account_email: Optional[str] = None,
        observe_url: Optional[str] = None,
        compensate_url: Optional[str] = None,
    ):
        self.name = name or f"tasks:{queue}"
        self.project = project
        self.location = location
        self.queue = queue
        self.target_url = target_url
        self.service_account_email = service_account_email
        self.observe_url = observe_url
        self.compensate_url = compensate_url

    def _client(self):
        try:
            from google.cloud import tasks_v2
        except ImportError as ex:  # pragma: no cover
            raise RuntimeError(
                "CloudTasksConnector requires google-cloud-tasks — "
                "`pip install google-cloud-tasks`."
            ) from ex
        return tasks_v2.CloudTasksClient()

    def _queue_path(self) -> str:
        from google.cloud import tasks_v2
        return tasks_v2.CloudTasksClient.queue_path(self.project, self.location, self.queue)

    async def dispatch(self, effect: EffectRecord) -> DispatchResult:
        try:
            from google.cloud import tasks_v2

            client = self._client()
            task = {
                "name": f"{self._queue_path()}/tasks/{_safe_task_id(effect.idempotency_key)}",
                "http_request": {
                    "http_method": tasks_v2.HttpMethod.POST,
                    "url": self.target_url,
                    "headers": {
                        "Content-Type": "application/json",
                        "X-Tape-Idempotency-Key": effect.idempotency_key,
                        "X-Tape-Run-Id": effect.run_id,
                        "X-Tape-Business-Key": effect.business_key,
                    },
                    "body": json.dumps(effect.payload).encode("utf-8"),
                },
            }
            if self.service_account_email:
                task["http_request"]["oidc_token"] = {
                    "service_account_email": self.service_account_email,
                    "audience": self.target_url,
                }
            created = client.create_task(parent=self._queue_path(), task=task)
            return DispatchResult(outcome=DispatchOutcome.PENDING,
                                  response={"name": created.name},
                                  dispatch_id=created.name)
        except Exception as ex:
            msg = str(ex)
            if "ALREADY_EXISTS" in msg:
                return DispatchResult(outcome=DispatchOutcome.CONFIRMED,
                                      response={"deduped": True})
            return DispatchResult(outcome=DispatchOutcome.UNKNOWN, error=msg)

    async def observe(self, effect: EffectRecord) -> ObservationResult:
        if self.observe_url:
            from .http import HttpConnector
            return await HttpConnector(
                url=self.observe_url,
                observe_url=self.observe_url,
            ).observe(effect)
        return ObservationResult(outcome=ObservationOutcome.UNKNOWN,
                                 error="no observe_url configured")

    async def compensate(self, obligation: ObligationRecord) -> CompensationResult:
        if self.compensate_url:
            from .http import HttpConnector
            return await HttpConnector(
                url=self.compensate_url,
                compensate_url=self.compensate_url,
            ).compensate(obligation)
        return CompensationResult(outcome=CompensationOutcome.STUCK,
                                  error="no compensate_url configured")
