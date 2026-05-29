"""AIPlexSink unit tests.

No tape-server, no real HTTP. We patch urlopen so the wire-shape +
batching + retry logic are observable.
"""

from __future__ import annotations

import io
import json
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any
from unittest import mock
from urllib.error import HTTPError, URLError

import pytest

from tape.sinks import AIPlexSink, _map_kind, _infer_plane


@dataclass
class FakeEntry:
    """Stand-in for tape._gen.tape_pb2.JournalEntry. The sink only
    reads attributes via getattr."""
    run_id: str = "r1"
    seq: int = 1
    kind: str = "effect"
    payload_json: str = ""
    ts_ms: int = 1_700_000_000_000


@contextmanager
def captured_posts(status: int = 200, raise_each: list | None = None):
    """Capture every urlopen POST. `raise_each` is consumed in order —
    a list of (exception or None); None means "respond with `status`."""
    raise_each = list(raise_each or [])
    calls: list[dict] = []

    def fake_urlopen(req, timeout=None):
        calls.append({
            "url": req.full_url,
            "headers": dict(req.headers),
            "body": json.loads(req.data.decode("utf-8")),
        })
        if raise_each:
            ex = raise_each.pop(0)
            if ex is not None:
                raise ex
        resp = mock.Mock()
        resp.status = status
        resp.__enter__ = lambda self: self
        resp.__exit__ = lambda *args: None
        return resp

    with mock.patch("tape.sinks.urllib.request.urlopen", side_effect=fake_urlopen), \
         mock.patch("tape.sinks.time.sleep") as sleeper:
        yield calls, sleeper


def _sink(**overrides) -> AIPlexSink:
    base = {
        "url": "https://aiplex.test",
        "token": "secret",
        "tenant_id": "acme",
        "agent_id": "treasury",
        "plane": "a2aplex",
        "actor": "spiffe://acme/treasury",
        "subject": "vamsi@example.com",
        "aiplex_instance_id": "treasury-abc",
        "batch_size": 1,
        "flush_interval_s": 0.05,
    }
    base.update(overrides)
    return AIPlexSink(**base)


# ── construction ──────────────────────────────────────────────────────────

def test_constructor_requires_url(monkeypatch):
    monkeypatch.delenv("AIPLEX_INGEST_URL", raising=False)
    with pytest.raises(ValueError, match="AIPLEX_INGEST_URL"):
        AIPlexSink()


def test_constructor_normalises_url():
    s = _sink(url="https://aiplex.test/")
    assert s.url == "https://aiplex.test/internal/tape/events"

    s2 = _sink(url="https://aiplex.test/internal/tape/events")
    assert s2.url == "https://aiplex.test/internal/tape/events"


def test_constructor_reads_env(monkeypatch):
    monkeypatch.setenv("AIPLEX_INGEST_URL", "https://aiplex.env")
    monkeypatch.setenv("AIPLEX_INGEST_TOKEN", "env-token")
    monkeypatch.setenv("AIPLEX_TENANT_ID", "envtenant")
    monkeypatch.setenv("AIPLEX_AGENT_ID", "envagent")
    monkeypatch.setenv("AIPLEX_ACTOR", "spiffe://env")
    monkeypatch.setenv("AIPLEX_SUBJECT", "env@example.com")
    monkeypatch.setenv("AIPLEX_INSTANCE_ID", "env-instance")
    monkeypatch.setenv("AIPLEX_ROUTE", "/a2a/foo")
    s = AIPlexSink(batch_size=1)
    assert s.url == "https://aiplex.env/internal/tape/events"
    assert s.token == "env-token"
    assert s.tenant_id == "envtenant"
    assert s.plane == "a2aplex"
    assert s.aiplex_instance_id == "env-instance"


# ── kind mapping ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("tape_kind,payload,expect", [
    ("effect",     {"status": "pending"},    "effect.begin"),
    ("effect",     {"status": "confirmed"},  "effect.confirmed"),
    ("effect",     {"status": "unknown"},    "effect.unknown"),
    ("effect",     {"status": "duplicate"},  "effect.duplicate"),
    ("run",        {"status": "running"},    "run.started"),
    ("run",        {"status": "terminal"},   "run.completed"),
    ("run",        {"status": "failed"},     "run.failed"),
    ("run",        {"status": "cancelled"},  "run.failed"),
    ("run",        {"status": "compensating"}, "obligation.created"),
    ("decision",   {},                       "decision.recorded"),
    ("obligation", {},                       "obligation.created"),
    ("policy",     {"required_scope": "x"},  "policy.violation"),
    ("budget",     {"usd_charged": 1.0},     "budget.charged"),
    ("gate",       {},                       "gate.waiting"),
    ("timer",      {},                       "timer.scheduled"),
])
def test_kind_mapping(tape_kind, payload, expect):
    assert _map_kind(tape_kind, payload) == expect


def test_unknown_kind_passes_through():
    assert _map_kind("custom_thing", {}) == "custom_thing"


@pytest.mark.parametrize("route,expect", [
    ("/a2a/treasury", "a2aplex"),
    ("/mcp/knowledge-base", "mcplex"),
    ("/llm/gemini-2.5-pro", "llmplex"),
    ("", ""),
])
def test_infer_plane(route, expect):
    assert _infer_plane(route) == expect


# ── wire shape ────────────────────────────────────────────────────────────

def test_publish_posts_single_event_in_batch_size_1():
    s = _sink(batch_size=1)
    entry = FakeEntry(run_id="r-x", seq=42, kind="effect",
                      payload_json='{"status":"confirmed","tool":"bank_wire","scope":"mcp:tools:bank_wire"}')
    with captured_posts() as (calls, _):
        s.publish(entry)
    assert len(calls) == 1
    body = calls[0]["body"]
    assert "events" in body
    assert len(body["events"]) == 1
    ev = body["events"][0]
    assert ev["run_id"] == "r-x"
    assert ev["seq"] == 42
    assert ev["kind"] == "effect.confirmed"
    assert ev["tool"] == "bank_wire"
    assert ev["scope"] == "mcp:tools:bank_wire"
    assert ev["tenant_id"] == "acme"
    assert ev["agent_id"] == "treasury"
    assert ev["plane"] == "a2aplex"
    assert ev["actor"] == "spiffe://acme/treasury"
    assert ev["subject"] == "vamsi@example.com"
    assert ev["aiplex_instance_id"] == "treasury-abc"
    # Timestamp present + ISO-8601 shape.
    assert ev["timestamp"].endswith("Z")


def test_authorization_header_set():
    s = _sink(token="my-token", batch_size=1)
    with captured_posts() as (calls, _):
        s.publish(FakeEntry())
    assert calls[0]["headers"]["Authorization"] == "Bearer my-token"


def test_no_auth_header_when_token_empty():
    s = _sink(token="", batch_size=1)
    with captured_posts() as (calls, _):
        s.publish(FakeEntry())
    assert "Authorization" not in calls[0]["headers"]


# ── batching ──────────────────────────────────────────────────────────────

def test_batch_size_triggers_flush():
    s = _sink(batch_size=3, flush_interval_s=999)
    with captured_posts() as (calls, _):
        s.publish(FakeEntry(seq=1))
        s.publish(FakeEntry(seq=2))
        assert len(calls) == 0  # under threshold
        s.publish(FakeEntry(seq=3))
        assert len(calls) == 1
        assert len(calls[0]["body"]["events"]) == 3


def test_close_flushes_buffered():
    s = _sink(batch_size=100, flush_interval_s=999)
    with captured_posts() as (calls, _):
        s.publish(FakeEntry(seq=1))
        s.publish(FakeEntry(seq=2))
        assert len(calls) == 0
        s.close()
        assert len(calls) == 1
        assert len(calls[0]["body"]["events"]) == 2


def test_publish_after_close_is_noop():
    s = _sink(batch_size=1)
    with captured_posts() as (calls, _):
        s.close()
        calls_before = len(calls)
        s.publish(FakeEntry())
        # No additional POST.
        assert len(calls) == calls_before


# ── retry / backoff ───────────────────────────────────────────────────────

def test_retries_transient_failures_then_succeeds():
    s = _sink(batch_size=1, max_retries=3, initial_backoff_s=0.01)
    with captured_posts(raise_each=[URLError("conn refused"), URLError("timeout"), None]) as (calls, sleeper):
        s.publish(FakeEntry())
    assert len(calls) == 3  # 2 failures + 1 success
    assert sleeper.call_count == 2  # backoff between attempts


def test_4xx_permanent_failure_raises_immediately():
    s = _sink(batch_size=1, max_retries=5)
    err = HTTPError(url="x", code=400, msg="Bad Request", hdrs=None, fp=io.BytesIO(b""))
    with captured_posts(raise_each=[err]) as (calls, _):
        with pytest.raises(HTTPError):
            s.publish(FakeEntry())
    # No retries on 400.
    assert len(calls) == 1


def test_429_retried():
    s = _sink(batch_size=1, max_retries=3, initial_backoff_s=0.01)
    err = HTTPError(url="x", code=429, msg="Too Many", hdrs=None, fp=io.BytesIO(b""))
    with captured_posts(raise_each=[err, None]) as (calls, _):
        s.publish(FakeEntry())
    assert len(calls) == 2


def test_max_retries_exhausted_raises():
    s = _sink(batch_size=1, max_retries=2, initial_backoff_s=0.01)
    with captured_posts(raise_each=[URLError("e1"), URLError("e2")]) as (calls, _):
        with pytest.raises(URLError):
            s.publish(FakeEntry())
    assert len(calls) == 2


# ── flush interval ────────────────────────────────────────────────────────

def test_time_based_flush(monkeypatch):
    # First publish goes in buffer; second arrives "later" and triggers
    # the time-based flush.
    fake = [100.0]
    monkeypatch.setattr("tape.sinks.time.monotonic", lambda: fake[0])
    s = _sink(batch_size=100, flush_interval_s=0.5)
    with captured_posts() as (calls, _):
        s.publish(FakeEntry(seq=1))
        assert len(calls) == 0
        fake[0] = 101.0  # 1 second later
        s.publish(FakeEntry(seq=2))
        assert len(calls) == 1
        assert len(calls[0]["body"]["events"]) == 2
