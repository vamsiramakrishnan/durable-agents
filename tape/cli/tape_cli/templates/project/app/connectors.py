"""Capability connectors for the {{ name }} agent.

The outbox reactor looks up a connector by name when it needs to dispatch a
non-idempotent intent. Register them here at import time.

For local dev, the `LogConnector` is convenient — it journals every dispatch
to a file you can `tail`. For production, point at your real upstream via
`HttpConnector`, `PubSubConnector`, or `CloudTasksConnector`.
"""

from __future__ import annotations

import os

from tape.connectors import CONNECTORS, LogConnector


def _register_defaults() -> None:
    if "log" not in CONNECTORS:
        CONNECTORS.register("log", LogConnector(
            path=os.environ.get("TAPE_OUTBOX_LOG", "/tmp/{{ name }}-outbox.jsonl"),
        ))


_register_defaults()
