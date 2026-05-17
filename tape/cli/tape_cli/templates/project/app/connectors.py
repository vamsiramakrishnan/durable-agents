"""Capability connectors for the {{ name }} agent.

The outbox reactor looks up a connector by name when it needs to dispatch a
non-idempotent intent. Register them here at import time.

Built-in: `HTTPConnector` (POST + idempotency headers) and `PubSubConnector`
(publish to a topic). Implement the `EffectConnector` protocol for your own.

See `tape/docs/non-idempotent-upstreams.md` for the contract.
"""

from __future__ import annotations

import os

from tape import connectors
from tape.connectors.http import HTTPConnector


def _register_defaults() -> None:
    # Replace these with your real upstream connectors. For dev, point at a
    # local echo server — for prod, point at the bank / payments / etc.
    if not connectors.get("example.http"):
        connectors.register(HTTPConnector(
            name="example.http",
            endpoint=os.environ.get("EXAMPLE_HTTP_ENDPOINT",
                                    "http://localhost:8088/dispatch"),
        ))


_register_defaults()
