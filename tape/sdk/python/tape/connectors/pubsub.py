"""PubSub — publisher (`PubSubConnector`) and subscriber (`PubSubSubscriber`)
for Tape's outbox path.

The publisher side is the connector the outbox reactor invokes: each effect
becomes one Pub/Sub message, ordering-keyed on `run_id`, with the
`tape_run_id` / `tape_effect_key` / `tape_business_key` / `tape_connector` /
`tape_semantics` attributes set. That gives downstream workers everything
they need to dedupe and route.

The subscriber side is a small consumer helper for workers that *do* the
upstream call. It:
  * reads `tape_effect_key` / `tape_business_key` / `tape_run_id` /
    `tape_connector` from the message attributes,
  * dedupes on `tape_effect_key` (via an in-memory LRU; production workers
    typically also stamp a business-key lock in their own DB),
  * runs the worker's `handle(payload, attrs) -> DispatchResult` callable,
  * sends the result back to Tape via the gRPC `complete_effect` /
    `record_dispatch_attempt` / `record_external_observation` RPCs — closing
    the loop on the wire, exactly the way the local outbox reactor does.

This is **controlled, auditable delivery, not exactly-once at the final API**.
The downstream subscriber is the one talking to the non-idempotent upstream
and is responsible for the business-key lock; Tape gives it the keys.

`google-cloud-pubsub` is a **lazy import** — the SDK does not declare it as a
hard dependency. Install it (`pip install 'tape-py[gcp-pubsub]'`) where the
publisher or subscriber runs; agent processes don't need it.
"""

from __future__ import annotations

import logging
import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from .base import DispatchResult, ObservationResult, CompensationResult


log = logging.getLogger(__name__)


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


# ── subscriber side: the worker that does the real upstream call ────────────

@dataclass
class PubSubSubscriber:
    """Pull-subscriber for Tape outbox messages.

    The subscriber runs in its own process (Cloud Run service, GKE pod, a
    plain Python process — whatever fits). It reads messages off the Pub/Sub
    subscription, dedupes on `tape_effect_key`, calls the user's `handle`
    function, and reports the outcome back to Tape via gRPC.

      * `project`, `subscription` — Pub/Sub subscription to pull from.
      * `tape_url`                — gRPC URL of the Tape server.
      * `handle(payload, attrs)`  — the worker function; receives the
        message's JSON body (parsed) plus the attributes dict. Returns a
        `DispatchResult` whose `status` drives Tape's transition:
          - `confirmed` → `complete_effect(CONFIRMED)`
          - `failed`    → `record_dispatch_attempt(next_at_ms=now+backoff)`
          - `unknown`   → `record_dispatch_attempt(next_at_ms=0)`
                          (drives the effect to UNKNOWN; the reconciler
                          observes via the registered connector or
                          status_check)
      * `claimer`                  — identity recorded as `dispatch_claimed_by`.
      * `lease_ttl_ms`             — how long to hold the dispatch lease on
        Tape while the worker is running.
      * `lru_size`                 — in-memory dedupe cache; for stronger
        guarantees against deliveries that the cache forgot, the worker
        should also keep a business-key lock in its own DB.

    Wire it up::

        sub = PubSubSubscriber(
            project="my-project", subscription="tape-bank-wire-sub",
            tape_url="tapes://tape-server-xxxxx-uc.a.run.app",
            handle=do_wire,
        )
        sub.run()  # blocks; Ctrl-C to stop, or sub.stop() from another thread
    """
    project: str
    subscription: str
    tape_url: str
    handle: Callable[[dict, dict], DispatchResult]
    claimer: str = ""
    lease_ttl_ms: int = 60_000
    backoff_base_ms: int = 1_000
    backoff_max_ms: int = 60_000
    lru_size: int = 4096
    subscriber: Any = None         # pubsub_v1.SubscriberClient (test seam)
    tape_client_factory: Any = None  # () -> TapeClient (test seam)

    _seen: "OrderedDict[str, bool]" = field(default_factory=OrderedDict, init=False)
    _seen_lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    _stop: threading.Event = field(default_factory=threading.Event, init=False)
    _future: Any = field(default=None, init=False)
    _tape: Any = field(default=None, init=False)

    def __post_init__(self) -> None:
        if not self.claimer:
            import os, socket
            self.claimer = f"{socket.gethostname()}:{os.getpid()}/pubsub"

    def _subscription_path(self) -> str:
        return f"projects/{self.project}/subscriptions/{self.subscription}"

    def _client(self):
        if self.subscriber is not None:
            return self.subscriber
        from google.cloud import pubsub_v1  # type: ignore[import-not-found]
        self.subscriber = pubsub_v1.SubscriberClient()
        return self.subscriber

    def _tape_client(self):
        if self._tape is not None:
            return self._tape
        if self.tape_client_factory is not None:
            self._tape = self.tape_client_factory()
        else:
            from ..client import TapeClient as _TapeClient
            self._tape = _TapeClient(self.tape_url)
        return self._tape

    def _seen_lookup(self, effect_key: str) -> bool:
        with self._seen_lock:
            if effect_key in self._seen:
                self._seen.move_to_end(effect_key)
                return True
            self._seen[effect_key] = True
            while len(self._seen) > self.lru_size:
                self._seen.popitem(last=False)
            return False

    def _on_message(self, message) -> None:
        """The Pub/Sub library calls this for each delivered message. Always
        ack — even on errors — and report the outcome to Tape via gRPC. Acking
        is what tells Pub/Sub "we own this now"; the source of truth for the
        effect's lifecycle is Tape, not Pub/Sub redelivery."""
        attrs = dict(message.attributes or {})
        effect_key = attrs.get("tape_effect_key", "")
        run_id = attrs.get("tape_run_id", "")
        connector = attrs.get("tape_connector", "")
        if not effect_key or not run_id:
            log.warning("pubsub-sub: missing tape_effect_key/run_id; ack-and-drop")
            message.ack()
            return
        # In-memory dedupe — if Pub/Sub redelivers a message we already
        # processed, we don't run handle() again. The worker SHOULD also keep
        # a business-key lock in its own DB for stronger guarantees.
        if self._seen_lookup(effect_key):
            log.debug("pubsub-sub: %s already seen; ack-and-skip", effect_key)
            message.ack()
            return

        # Claim the dispatch lease on Tape — this is what makes a parallel
        # subscriber pulling the same message-redelivery safe even when our
        # local LRU has rotated the effect out.
        tape = self._tape_client()
        try:
            claim = tape.claim_effect_dispatch(
                run_id=run_id, idempotency_key=effect_key,
                claimer=self.claimer, lease_ttl_ms=self.lease_ttl_ms)
            if not claim.acquired:
                log.info("pubsub-sub: %s lease contended; ack and let the winner handle it",
                         effect_key)
                message.ack()
                return
        except Exception as ex:  # noqa: BLE001
            log.exception("pubsub-sub: claim_effect_dispatch failed; nack to retry: %s", ex)
            message.nack()
            return

        # Parse the body and call the handler.
        import json as _json
        try:
            data = message.data.decode("utf-8") if message.data else ""
            payload = _json.loads(data) if data else {}
        except Exception as ex:
            log.exception("pubsub-sub: bad message body; mark FAILED and ack: %s", ex)
            try:
                tape.record_dispatch_attempt(
                    run_id=run_id, idempotency_key=effect_key,
                    error=f"malformed body: {ex}", next_dispatch_at_ms=0)
            except Exception:
                pass
            message.ack()
            return

        try:
            result = self.handle(payload, attrs)
        except Exception as ex:  # noqa: BLE001
            log.exception("pubsub-sub: handler raised; report UNKNOWN: %s", ex)
            try:
                tape.record_dispatch_attempt(
                    run_id=run_id, idempotency_key=effect_key,
                    error=f"handler: {type(ex).__name__}: {ex}",
                    next_dispatch_at_ms=0)
            except Exception:
                pass
            message.ack()
            return

        try:
            self._report(tape, run_id=run_id, effect_key=effect_key,
                         connector=connector, result=result)
        except Exception as ex:  # noqa: BLE001
            log.exception("pubsub-sub: report to Tape failed; nack for redelivery: %s", ex)
            message.nack()
            return
        message.ack()

    def _report(self, tape, *, run_id: str, effect_key: str, connector: str,
                result: DispatchResult) -> None:
        """Translate a DispatchResult into the right gRPC call. Mirrors
        `dispatch_one` in tape.reactors.outbox so behaviour is consistent
        across the local-reactor and Pub/Sub paths."""
        import json as _json
        if result.status == "confirmed":
            tape.complete_effect(
                run_id=run_id, idempotency_key=effect_key,
                status=2,  # EFFECT_STATUS_CONFIRMED
                response_json=_json.dumps({
                    "external_ref": result.external_ref,
                    **(result.response or {}),
                }, default=str))
            return
        if result.status == "unknown":
            tape.record_dispatch_attempt(
                run_id=run_id, idempotency_key=effect_key,
                error=_json.dumps(result.error or {"reason": "ack lost"}, default=str),
                next_dispatch_at_ms=0)
            return
        # failed — schedule a retry; the connector's `retry_at_ms` overrides
        # the default backoff if set.
        import time as _t
        if result.retry_at_ms:
            next_at = result.retry_at_ms
        else:
            next_at = int(_t.time() * 1000) + self.backoff_base_ms
        tape.record_dispatch_attempt(
            run_id=run_id, idempotency_key=effect_key,
            error=_json.dumps(result.error or {}, default=str),
            next_dispatch_at_ms=next_at)

    def run(self, *, block: bool = True) -> None:
        """Start consuming. If `block=True`, runs until `stop()` is called
        from another thread or the stream errors. If `block=False`, returns
        the StreamingPullFuture so the caller can manage lifetime."""
        client = self._client()
        log.info("pubsub-sub: pulling from %s", self._subscription_path())
        self._future = client.subscribe(self._subscription_path(), callback=self._on_message)
        if not block:
            return self._future
        try:
            while not self._stop.is_set():
                try:
                    # `result()` blocks until the stream completes (an error
                    # or cancellation). We use a short timeout so the stop
                    # signal is responsive.
                    self._future.result(timeout=2.0)
                except Exception as ex:
                    if self._stop.is_set():
                        break
                    # TimeoutError-equivalent (futures.TimeoutError) is the
                    # normal "still healthy" exit; anything else, log and
                    # restart the subscription.
                    name = type(ex).__name__
                    if name not in ("TimeoutError", "_MultiThreadedRendezvous"):
                        log.exception("pubsub-sub: stream error; restarting: %s", ex)
                        try:
                            self._future.cancel()
                        except Exception:
                            pass
                        self._future = client.subscribe(self._subscription_path(),
                                                         callback=self._on_message)
        finally:
            self.stop()

    def stop(self) -> None:
        self._stop.set()
        try:
            if self._future is not None:
                self._future.cancel()
        except Exception:
            pass
        try:
            if self._tape is not None:
                self._tape.close()
        except Exception:
            pass


# ── CLI: `python -m tape.connectors.pubsub --project P --subscription S ...` ─

def _main(argv=None) -> int:
    """Tiny CLI for ad-hoc subscribers / smoke tests. For production, write a
    short script that constructs `PubSubSubscriber(handle=...)` with your
    domain-specific handler and runs `.run()` — the CLI uses a built-in
    "log-and-confirm" handler that's only useful for verifying delivery."""
    import argparse, importlib, sys
    p = argparse.ArgumentParser(prog="tape.connectors.pubsub",
        description="Run a Pub/Sub subscriber that reports outcomes back to Tape.")
    p.add_argument("--project", required=True)
    p.add_argument("--subscription", required=True)
    p.add_argument("--tape-url", required=True, help="e.g. tapes://tape-server-xxxxx-uc.a.run.app")
    p.add_argument("--handler", default="",
                   help="module:attr resolving to a handle(payload, attrs) -> DispatchResult callable. "
                        "Omit for a built-in log-and-confirm handler (smoke test only).")
    p.add_argument("--lease-ttl-ms", type=int, default=60_000)
    p.add_argument("--claimer", default="")
    args = p.parse_args(argv)

    if args.handler:
        mod_name, _, attr = args.handler.partition(":")
        if not attr:
            print("--handler must be module:attr", file=sys.stderr)
            return 2
        handle = getattr(importlib.import_module(mod_name), attr)
    else:
        def handle(payload, attrs):
            print(f"[pubsub] received {attrs.get('tape_effect_key')!r} payload={payload}")
            return DispatchResult(status="confirmed",
                                  external_ref=attrs.get("tape_effect_key", ""),
                                  response={"smoke": True})

    sub = PubSubSubscriber(
        project=args.project, subscription=args.subscription,
        tape_url=args.tape_url, handle=handle,
        lease_ttl_ms=args.lease_ttl_ms, claimer=args.claimer,
    )
    try:
        sub.run()
    except KeyboardInterrupt:
        sub.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
