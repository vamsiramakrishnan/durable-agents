"""Reactive key-value store — the treatise's §IX ⑥ ("coordination through
journaled state, not messages") as a first-class primitive.

The headline scenario: write X=70, watch X, write X=90 — the watcher receives
the transition (prev=70, new=90)."""

from __future__ import annotations

import json
import threading
import time

import grpc

import tape


def test_write_get_delete_roundtrip(tape_server):
    url = tape_server["url"]

    r1 = tape.set_value("treasury", "fx_usd_eur", {"rate": 0.92}, writer="oracle", url=url)
    assert r1.version == 1
    assert not r1.deleted

    got = tape.get_value("treasury", "fx_usd_eur", url=url)
    assert got.found
    assert json.loads(got.value.value_json) == {"rate": 0.92}
    assert got.value.version == 1
    assert got.value.writer == "oracle"

    r2 = tape.set_value("treasury", "fx_usd_eur", {"rate": 0.93}, writer="oracle", url=url)
    assert r2.version == 2

    d = tape.delete_value("treasury", "fx_usd_eur", url=url)
    assert d.deleted
    assert d.version == 3
    got2 = tape.get_value("treasury", "fx_usd_eur", url=url)
    # `found=false` for a tombstoned row (the canonical "absent" signal) — but
    # the underlying ValueRecord IS returned with deleted=True so callers can
    # tell "never written" from "deleted at version N".
    assert not got2.found
    assert got2.value.deleted
    assert got2.value.version == 3


def test_cas_version_conflict(tape_server):
    url = tape_server["url"]
    tape.set_value("treasury", "limits/usd", {"cap": 50}, url=url)  # v=1

    # create-only (if_version=0) on an existing key must fail
    with pytest.raises(grpc.RpcError):
        tape.set_value("treasury", "limits/usd", {"cap": 60}, if_version=0, url=url)

    # exact-match CAS succeeds at the right version
    r = tape.set_value("treasury", "limits/usd", {"cap": 75}, if_version=1, url=url)
    assert r.version == 2

    # exact-match CAS at the WRONG version fails (we're at v=2 now)
    with pytest.raises(grpc.RpcError):
        tape.set_value("treasury", "limits/usd", {"cap": 90}, if_version=1, url=url)


def test_watch_value_X_70_to_90(tape_server):
    """The headline: X transitions from 70 to 90, the watcher sees both the
    snapshot AND the transition with the previous value attached."""
    url = tape_server["url"]

    # Seed X = 70 first so the watcher's initial snapshot has something.
    tape.set_value("counters", "X", 70, writer="seed", url=url)

    events: list = []
    stop = threading.Event()

    def reader():
        stream = tape.watch_value("counters", "X", from_version=0, url=url)
        try:
            for evt in stream:
                events.append(evt)
                if stop.is_set() or len(events) >= 2:
                    break
        except grpc.RpcError:
            pass
        finally:
            try:
                stream.cancel()
            except Exception:
                pass

    t = threading.Thread(target=reader, daemon=True)
    t.start()

    # Give the watcher one poll cycle to deliver the initial snapshot (250ms server poll).
    time.sleep(0.6)
    assert len(events) >= 1, "watcher should have received the initial snapshot of X=70"
    snap = events[0]
    assert json.loads(snap.value.value_json) == 70
    assert snap.value.version == 1
    # snapshot has no predecessor
    assert snap.prev_version == 0
    assert snap.prev_value_json == ""

    # Now the transition: X 70 → 90.
    tape.set_value("counters", "X", 90, writer="updater", url=url)

    # Wait for the watcher to deliver the second event.
    deadline = time.time() + 5.0
    while len(events) < 2 and time.time() < deadline:
        time.sleep(0.1)
    stop.set()
    t.join(timeout=2.0)

    assert len(events) >= 2, "watcher should have received the X 70 → 90 transition"
    trans = events[1]
    assert json.loads(trans.value.value_json) == 90
    assert trans.value.version == 2
    assert trans.value.writer == "updater"
    # The transition carries the PREVIOUS value — this is what "X (70 → 90)" means.
    assert trans.prev_version == 1
    assert json.loads(trans.prev_value_json) == 70


# pytest is needed for `raises`
import pytest  # noqa: E402
