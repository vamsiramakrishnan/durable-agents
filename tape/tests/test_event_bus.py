"""Event-bus rebuild — end-to-end tests for the new surface.

Covers:
  * journal entries now carry `global_seq`, `subject`, `schema_version`;
  * subject-pattern matching is server-side (a non-matching write produces no
    task);
  * server-side CEL predicates filter matching entries further;
  * the in-proc dispatcher runs TASK reactions with the declared backpressure
    knobs (max_concurrency, rate_limit_per_s, debounce_ms);
  * the server enforces the retry / DLQ policy (`NackTask(permanent=true)`
    once `attempts >= dlq_after_n` flips the task to DLQ);
  * the outbox relay now uses a `last_global_seq` cursor (and migrates an old
    `from_ts_ms`/`last_run_id`/`last_seq` cursor on first read);
  * AGENT reactions create a `tape_run`, not a `tape_task`.

These tests need a real Tape server with the event-bus schema. The
`tape_server` fixture in `conftest.py` builds + boots the Rust server against
a temp SQLite store. If that binary isn't built yet, every test here skips —
the fixture itself emits the skip.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path

import grpc
import pytest

ROOT = Path(__file__).resolve().parents[2]
SDK_PY = ROOT / "tape" / "sdk" / "python"
import sys
sys.path.insert(0, str(SDK_PY))


def _query(db: str, sql: str, params=()):
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return con.execute(sql, params).fetchall()
    finally:
        con.close()


def _wait_for(predicate, timeout_s: float = 5.0, interval_s: float = 0.1):
    """Poll `predicate` until it returns a truthy value (returned to the
    caller) or the deadline expires (returns the last value, which may be
    falsy)."""
    deadline = time.time() + timeout_s
    val = None
    while time.time() < deadline:
        val = predicate()
        if val:
            return val
        time.sleep(interval_s)
    return val


def _drain_subject(client, subject_pattern: str, timeout_s: float = 1.0):
    """Read every entry currently available under `subject_pattern`. Stops on
    DEADLINE_EXCEEDED, which is how a subject stream signals "no more pending"."""
    out: list = []
    stream = client.subscribe_by_subject(subject_pattern=subject_pattern,
                                         from_global_seq=0, timeout=timeout_s)
    try:
        for entry in stream:
            out.append(entry)
    except grpc.RpcError as e:
        if e.code() not in (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.CANCELLED):
            raise
    finally:
        try:
            stream.cancel()
        except Exception:
            pass
    return out


# Each test clears the in-process reaction registry so decorators from one
# test don't bleed into the next.
@pytest.fixture(autouse=True)
def _clean_registry():
    from tape.reactions import _clear_registry
    _clear_registry()
    yield
    _clear_registry()


# ── 1. Journal entries carry the new fields ────────────────────────────────

def test_journal_entries_have_global_seq_and_subject(tape_server):
    """After a decision and a value write, the WAL (via `SubscribeBySubject`)
    yields entries whose `global_seq` is non-zero and increasing, whose
    `subject` is non-empty, and whose `schema_version == 1`."""
    from tape.client import TapeClient

    url = tape_server["url"]
    with TapeClient(url) as c:
        run = c.begin_run(app_name="t", user_id="u", session_id="s-journal",
                          invocation_id=f"inv-{uuid.uuid4().hex[:6]}",
                          lease_owner="test", lease_ttl_ms=60_000)
        c.record_decision(run_id=run.run_id, decision_index=0,
                          response_json='{"plan":1}')
        c.write_value(namespace="treasury", key="fx_rate",
                      value_json="1.10", writer="oracle")
        # let the server drain its in-memory bus_tick
        time.sleep(0.2)
        entries = _drain_subject(c, "/tape/**", timeout_s=1.0)

    assert entries, "expected at least one journal entry under /tape/**"
    seqs = [e.global_seq for e in entries]
    assert all(s > 0 for s in seqs), f"every global_seq must be > 0; got {seqs}"
    assert seqs == sorted(seqs), f"global_seq must be monotonic; got {seqs}"
    assert all(e.subject.startswith("/tape/") for e in entries), (
        f"every subject must start with /tape/; got {[e.subject for e in entries]}")
    assert all(e.schema_version == 1 for e in entries), (
        f"every schema_version must default to 1; got {[e.schema_version for e in entries]}")


# ── 2. Subject patterns are matched server-side ────────────────────────────

def test_subject_pattern_matching_server_side(tape_server):
    """A reaction with `subject_pattern=/tape/value/changed/treasury/**` should
    receive a task only when a value in the `treasury` namespace is written —
    NOT when a value in `other` is written."""
    from tape.client import (
        HANDLER_KIND_TASK, TASK_STATUS_PENDING, TapeClient,
    )

    url = tape_server["url"]
    with TapeClient(url) as c:
        r = c.register_reaction(
            name="treasury-only", subject_pattern="/tape/value/changed/treasury/**",
            handler_kind=HANDLER_KIND_TASK)
        rid = r.reaction_id
        assert rid

        # match: treasury/...
        c.write_value(namespace="treasury", key="fx_rate",
                      value_json="1.10", writer="t")
        # non-match: other/...
        c.write_value(namespace="other", key="x", value_json="1", writer="t")

        def _have_one():
            tasks = c.list_tasks(reaction_id=rid, limit=200)
            return tasks if tasks else None

        tasks = _wait_for(_have_one, timeout_s=4.0)
    assert tasks, "the matcher should have created one task for the treasury write"
    assert len(tasks) == 1, (
        f"expected exactly 1 task (other/... must NOT match); got {len(tasks)}: "
        f"{[t.subject for t in tasks]}")
    assert tasks[0].subject.startswith("/tape/value/changed/treasury/"), tasks[0].subject


# ── 3. CEL predicate filters matches further ───────────────────────────────

def test_cel_predicate_filters_matches(tape_server):
    """A reaction whose `predicate_cel` is non-trivial should only produce
    tasks for entries that satisfy the predicate.

    We register a reaction over `/tape/effect/confirmed/**` whose predicate
    insists `payload.tool == "execute_sweep"` (the effect-event payload carries
    a `tool` field — see the subjects::derive table). One sweep + one
    different-tool effect → exactly one task.
    """
    from tape.client import (
        EFFECT_STATUS_CONFIRMED, HANDLER_KIND_TASK, TapeClient,
    )

    url = tape_server["url"]
    with TapeClient(url) as c:
        r = c.register_reaction(
            name="sweep-only", subject_pattern="/tape/effect/confirmed/**",
            predicate_cel='payload.tool == "execute_sweep"',
            handler_kind=HANDLER_KIND_TASK)
        rid = r.reaction_id

        run = c.begin_run(app_name="t", user_id="u", session_id="s-cel",
                          invocation_id=f"inv-{uuid.uuid4().hex[:6]}",
                          lease_owner="test", lease_ttl_ms=60_000)
        c.record_decision(run_id=run.run_id, decision_index=0,
                          response_json='{"plan":1}')

        # match: tool_name=execute_sweep
        be1 = c.begin_effect(run_id=run.run_id, decision_index=0,
                             tool_name="execute_sweep", call_index=0,
                             request_json="{}")
        c.complete_effect(run_id=run.run_id, idempotency_key=be1.idempotency_key,
                          status=EFFECT_STATUS_CONFIRMED,
                          response_json='{"wire_id":"w1"}')

        # non-match: tool_name=some_other_tool
        be2 = c.begin_effect(run_id=run.run_id, decision_index=0,
                             tool_name="some_other_tool", call_index=1,
                             request_json="{}")
        c.complete_effect(run_id=run.run_id, idempotency_key=be2.idempotency_key,
                          status=EFFECT_STATUS_CONFIRMED,
                          response_json='{"ok":true}')

        def _one():
            tasks = c.list_tasks(reaction_id=rid, limit=200)
            return tasks if tasks else None

        tasks = _wait_for(_one, timeout_s=5.0) or []

    assert len(tasks) == 1, (
        f"CEL should let exactly one through; got {len(tasks)} subjects="
        f"{[t.subject for t in tasks]}")
    assert "execute_sweep" in tasks[0].subject


# ── 4. Dispatcher runs TASK handlers with backpressure ─────────────────────

def test_dispatcher_runs_task_handler_with_backpressure(tape_server):
    """Register a TASK reaction via the decorator, trigger it, run the
    dispatcher with `once=True`, verify the handler ran and the task is DONE."""
    import tape
    from tape.client import TASK_STATUS_DONE, TapeClient

    url = tape_server["url"]
    called: list[dict] = []

    @tape.on("/tape/value/changed/disp-test/**", max_concurrency=2)
    def handler(env):
        called.append(env)

    with TapeClient(url) as c:
        rs = tape.register_all(url, prefix=f"t{uuid.uuid4().hex[:4]}-")
        assert len(rs) == 1
        rid = rs[0].reaction_id

        c.write_value(namespace="disp-test", key="k1",
                      value_json='{"v":1}', writer="t")
        # wait for the matcher to create the task
        _wait_for(lambda: c.list_tasks(reaction_id=rid, limit=10),
                  timeout_s=4.0)

        # one dispatcher pass — claim + run + ack
        tape.run_dispatcher(url, once=True, register=False)
        # the handler may run in a worker thread; wait for it to land
        _wait_for(lambda: called, timeout_s=4.0)

        # task should be DONE
        def _done():
            tasks = c.list_tasks(reaction_id=rid, status=TASK_STATUS_DONE, limit=10)
            return tasks
        done_tasks = _wait_for(_done, timeout_s=4.0) or []

    assert called, "handler must have been invoked"
    assert any("disp-test" in env["subject"] for env in called)
    assert done_tasks, f"task must be DONE; got {done_tasks}"


# ── 5. retry → DLQ ─────────────────────────────────────────────────────────

def test_nack_with_retry_backoff_then_dlq(tape_server):
    """A handler that always raises should NACK; after `dlq_after_n` attempts,
    the task lands in DLQ."""
    import tape
    from tape.client import (
        TASK_STATUS_DLQ, TASK_STATUS_FAILED, TASK_STATUS_PENDING,
        TapeClient,
    )

    url = tape_server["url"]
    attempts: list[int] = []

    @tape.on("/tape/value/changed/dlq-test/**",
             retry_max=2, dlq_after_n=2, retry_backoff_ms=10)
    def boom(env):
        attempts.append(env["attempts"])
        raise RuntimeError("synthetic-failure")

    with TapeClient(url) as c:
        rs = tape.register_all(url, prefix=f"t{uuid.uuid4().hex[:4]}-")
        rid = rs[0].reaction_id
        c.write_value(namespace="dlq-test", key="k", value_json="0", writer="t")
        _wait_for(lambda: c.list_tasks(reaction_id=rid, limit=10),
                  timeout_s=4.0)

        # Run the dispatcher in a loop; the server NACKs schedule a re-attempt
        # via `next_attempt_at_ms`. Sleep + retry up to ~3s.
        deadline = time.time() + 6.0
        while time.time() < deadline:
            tape.run_dispatcher(url, once=True, register=False, poll_interval_s=0.05)
            time.sleep(0.2)
            dlq = c.list_tasks(reaction_id=rid, status=TASK_STATUS_DLQ, limit=10)
            if dlq:
                break

        dlq = c.list_tasks(reaction_id=rid, status=TASK_STATUS_DLQ, limit=10)

    assert attempts, "handler must have been invoked at least once"
    assert dlq, f"task should be in DLQ after {len(attempts)} attempts"


# ── 6. debounce coalesces ──────────────────────────────────────────────────

def test_debounce_coalesces_repeated_subjects(tape_server):
    """With `debounce_ms=500`, three rapid writes to the same key should yield
    at most one handler invocation inside the window."""
    import tape
    from tape.client import TapeClient

    url = tape_server["url"]
    fired: list[dict] = []

    @tape.on("/tape/value/changed/debounce-test/**", debounce_ms=500)
    def h(env):
        fired.append(env)

    with TapeClient(url) as c:
        rs = tape.register_all(url, prefix=f"t{uuid.uuid4().hex[:4]}-")
        rid = rs[0].reaction_id
        for v in (1, 2, 3):
            c.write_value(namespace="debounce-test", key="k",
                          value_json=str(v), writer="t")
        # Let the matcher enqueue all three tasks…
        _wait_for(lambda: len(c.list_tasks(reaction_id=rid, limit=10)) >= 3,
                  timeout_s=4.0)
        # …then dispatch them in one pass. The debouncer should fire the handler
        # at most once for the shared subject.
        tape.run_dispatcher(url, once=True, register=False, poll_interval_s=0.05)
        time.sleep(0.4)

    assert len(fired) <= 1, (
        f"debounce should coalesce repeated subjects; fired {len(fired)} times: {fired}")


# ── 7. Outbox relay uses the global_seq cursor ─────────────────────────────

def test_outbox_relay_uses_global_seq_cursor(tape_server, tmp_path):
    """Port of the legacy `test_outbox_relay_publishes_journal_entries_with_a_
    durable_cursor` test to the new cursor format. The cursor file must
    contain only `{"last_global_seq": N}` after a successful tick."""
    from tape.client import EFFECT_STATUS_CONFIRMED, TapeClient
    from tape.reactors import outbox_relay_tick
    from tape.sinks import LogSink

    url = tape_server["url"]
    cursor = tmp_path / "cursor.json"
    out = tmp_path / "out.jsonl"

    with TapeClient(url) as c:
        run = c.begin_run(app_name="a", user_id="u", session_id="s-outbox-gs",
                          invocation_id=f"inv-{uuid.uuid4().hex[:6]}",
                          lease_owner="t", lease_ttl_ms=60_000)
        rid = run.run_id
        c.record_decision(run_id=rid, decision_index=0, response_json='{"plan":1}')
        be = c.begin_effect(run_id=rid, decision_index=0,
                            tool_name="execute_sweep", call_index=0,
                            request_json="{}")
        c.complete_effect(run_id=rid, idempotency_key=be.idempotency_key,
                          status=EFFECT_STATUS_CONFIRMED,
                          response_json='{"wire_id":"w1"}')

    time.sleep(0.3)
    sink = LogSink(str(out))
    n1 = outbox_relay_tick(url, sink, cursor_path=str(cursor), idle_window_s=1.0)
    sink.close()
    assert n1 >= 3, f"expected at least 3 entries on first tick, got {n1}"

    # Cursor must be the new shape — single integer, no legacy keys.
    cursor_d = json.loads(cursor.read_text())
    assert set(cursor_d.keys()) == {"last_global_seq"}, (
        f"cursor must only contain last_global_seq; got {sorted(cursor_d)}")
    assert cursor_d["last_global_seq"] > 0

    # A re-tick with the cursor at the head publishes nothing.
    sink = LogSink(str(out))
    n2 = outbox_relay_tick(url, sink, cursor_path=str(cursor), idle_window_s=0.5)
    sink.close()
    assert n2 == 0, f"expected nothing new on re-tick; got {n2}"

    # One more entry → relay restart picks up just that one.
    with TapeClient(url) as c:
        c.register_compensation(run_id=rid, effect_key=be.idempotency_key,
                                kind="reverse_wire", payload_json="{}")
    time.sleep(0.3)
    sink = LogSink(str(out))
    n3 = outbox_relay_tick(url, sink, cursor_path=str(cursor), idle_window_s=1.0)
    sink.close()
    assert n3 == 1, f"expected exactly one new entry, got {n3}"


def test_outbox_relay_migrates_legacy_cursor(tape_server, tmp_path):
    """A cursor file in the old shape should be migrated transparently to
    `{"last_global_seq": 0}` on first read (logged as a warning)."""
    from tape.reactors import _read_cursor

    legacy = tmp_path / "cursor.json"
    legacy.write_text(json.dumps({"from_ts_ms": 12345, "last_run_id": "r1",
                                  "last_seq": 7}))
    val = _read_cursor(str(legacy))
    assert val == 0, f"legacy cursor must reset to 0; got {val}"
    after = json.loads(legacy.read_text())
    assert set(after.keys()) == {"last_global_seq"}
    assert after["last_global_seq"] == 0


# ── 8. AGENT reactions create runs, not tasks ──────────────────────────────

def test_agent_handler_creates_run_not_task(tape_server):
    """A reaction whose `handler_kind=AGENT` matches a journal entry should
    cause the server to create a new `tape_run` (with an `invocation_id`
    derived from the source `(reaction_id, global_seq)`), NOT a row in
    `tape_tasks`."""
    from tape.client import HANDLER_KIND_AGENT, TapeClient

    url = tape_server["url"]
    db = tape_server["db"]

    with TapeClient(url) as c:
        r = c.register_reaction(
            name="follow-up", subject_pattern="/tape/value/changed/agent-test/**",
            handler_kind=HANDLER_KIND_AGENT, agent_app="treasury-followup")
        rid = r.reaction_id
        c.write_value(namespace="agent-test", key="k",
                      value_json='{"v":1}', writer="t")

        def _run_appears():
            rows = _query(
                db,
                "SELECT invocation_id FROM tape_runs "
                "WHERE invocation_id LIKE 'react-%' OR app_name='treasury-followup'")
            return rows if rows else None

        runs = _wait_for(_run_appears, timeout_s=5.0) or []
        # No tasks for an AGENT reaction.
        tasks = c.list_tasks(reaction_id=rid, limit=10)

    assert runs, (
        "AGENT reactions should create a tape_run; none appeared. "
        "If the Rust server hasn't shipped the AGENT-kind matcher yet, this "
        "test will fail until it does.")
    invocation_ids = [r[0] for r in runs]
    assert any("react-" in inv for inv in invocation_ids), (
        f"expected an invocation_id starting with 'react-'; got {invocation_ids}")
    assert not tasks, (
        f"AGENT reactions must NOT create tape_tasks; got {len(tasks)} tasks")
